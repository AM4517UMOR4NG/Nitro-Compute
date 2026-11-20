# Nitro Compute Architecture Specification

## 1. Problem Statement and Project Goal

Traditional distributed data processing frameworks (such as Hadoop MapReduce or Apache Spark) rely on centralized master nodes, heavyweight JVM/container runtimes, and high-bandwidth data movement into centralized cloud regions. At the network edge, these assumptions break down due to:

- High container cold-start latency (hundreds of milliseconds to seconds).
- Bandwidth bottlenecks when uploading raw edge telemetry to central clusters.
- Single-point-of-failure orchestration brokers.

**Nitro Compute** solves this by combining three technologies into a single `#![forbid(unsafe_code)]` Rust host workspace:

1. **WebAssembly (`wasm32-wasip1`)** as a portable, architecture-neutral binary format for stateless Map & Reduce compute kernels (~13 KB – 80 KB).
2. **Wasmtime (Cranelift JIT)** for sub-millisecond sandbox instantiation with **triple-layer resource protection** (deterministic fuel metering, `ResourceLimiter` linear memory/table ceilings, and preemptive wall-clock epoch interruption) plus SHA-256 compiled module caching.
3. **`libp2p` (Kademlia DHT + Gossipsub + mDNS + Identify + Ping)** for zero-config peer discovery, RTT monitoring, Zstd-compressed pub/sub framing, Ed25519 end-to-end payload attestation, and encrypted Noise + Yamux streams.

---

## 2. Module Architecture

### 2.1 `core` (`nitro_core` — `core/src/lib.rs`, `runtime.rs`, `network.rs`, `metrics.rs`)

The `core` library crate (`#![forbid(unsafe_code)]`, `lib name = "nitro_core"`) provides the foundational runtime, security, P2P, and observability primitives:

- **`NitroRuntime` (`core/src/runtime.rs`)**:
  - Wraps a `wasmtime::Engine` configured with `Strategy::Cranelift`, `OptLevel::Speed`, `config.consume_fuel(true)`, and `config.epoch_interruption(true)`.
  - Runs a managed background `EpochTicker` advancing the engine epoch counter every `10 ms` (cleanly joined on `Drop`) and sets `store.epoch_deadline_trap()` + `store.set_epoch_deadline(ticks)` for wall-clock deadline preemption.
  - Enforces bounded memory/table ceilings via a custom `ResourceLimiter` (`SandboxLimiter` tracking `max_memory_bytes` and `max_table_elements`).
  - Maintains a thread-safe **SHA-256 Content-Addressable Module Cache** (`RwLock<HashMap<[u8; 32], CachedModule>>`) so repeated task chunks reuse pre-compiled Cranelift `Module` instances.
  - Supports both **Scalar ABI** (`execute_with_timeout`: `(i32, i32) -> i32`) and **Variable-Length Byte-Buffer ABI** (`execute_bytes_with_options`: `(i32, i32) -> i64` packing `(out_ptr << 32) | out_len`, bounded by `max_output_bytes = 4 MiB`).
  - Surfaces deterministic `SandboxTrapError` variants (`OutOfFuel`, `MemoryLimitExceeded`, `TableLimitExceeded`, `EpochTimeout`).

- **`NitroBehavior` & Crypto Wire Layer (`core/src/network.rs`)**:
  - Combines `kademlia::Kademlia<MemoryStore>`, `gossipsub::Behaviour`, `Toggle<mdns::tokio::Behaviour>`, `identify::Behaviour`, and `ping::Behaviour` via `#[derive(NetworkBehavior)]`.
  - Raises Gossipsub `max_transmit_size` to `8 MiB` and wraps serialized payloads in transparent **Zstandard (`NZST` prefix)** compression with bounded `16 MiB` decompression limits.
  - Supports persistent protobuf-encoded Ed25519 node identities (`load_or_generate_keypair`) and end-to-end cryptographic payload attestation (`TaskPayload::sign`/`verify_signature` and `ResultPayload::sign`/`verify_signature` with anti-`PeerId`-spoofing checks).
  - Supports multi-phase execution modes (`JobPhase::MapScalar`, `JobPhase::MapBytes`, `JobPhase::ReduceBytes`) across `"task-alloc"`, `"task-results"`, and `"worker-telemetry"`.

- **Observability Endpoint (`core/src/metrics.rs`)**:
  - Lock-free `NitroTelemetryRegistry` exposed over an embedded, zero-dependency HTTP/1.1 server (`spawn_metrics_server`) serving Prometheus OpenMetrics (`GET /metrics`) and JSON health probes (`GET /healthz`, `GET /readyz`).

### 2.2 `coordinator` (`coordinator/src/main.rs`)

The Coordinator binary (`#![forbid(unsafe_code)]`) orchestrates two-phase MapReduce workflows across the mesh:

1. Streams a raw input file asynchronously using `tokio::io::BufReader` and fragments non-empty lines into chunks (`ingest_and_fragment_stream`).
2. Loads the compiled `.wasm` module (`load_wasm_binary`) and computes its `wasm_sha256` digest.
3. Waits for mesh worker readiness (`wait_for_mesh_workers`) via explicit `--peer` dials or LAN mDNS discovery.
4. Drives `JobScheduler`:
   - Signs every `TaskPayload` with its Ed25519 `Keypair`.
   - Optionally enables `--dedup-wasm` (omitting `wasm_binary` on chunks `1..N` with automatic cache-miss fallback).
   - Optionally enables **Google MapReduce §3.6 Speculative Straggler Execution** (`--speculative`), dispatching backup copies of slow tail chunks once `>= 75%` of chunks have finished and elapsed time exceeds `2x` median duration.
   - Retries transient errors or lease timeouts (`--task-timeout`) up to `--retries`, while immediately halting poisoned tasks that hit `SecurityTrap`.
   - Verifies worker Ed25519 signatures on inbound `ResultPayload`s (`--verify-worker-sigs`).
   - In `--mode kv-mapreduce`, runs **Stage 1 (`MapBytes`)** -> **Shuffle intermediate JSON maps** -> **Stage 2 (`ReduceBytes`)** across the mesh.

### 2.3 `worker` (`worker/src/main.rs`)

The Worker binary (`#![forbid(unsafe_code)]`) runs as a non-blocking, concurrency-bounded edge compute daemon:

1. Loads persistent or ephemeral Ed25519 identity (`--identity-key`), optionally enforces `--trusted-coordinator <PUBKEY_HEX>` / `--require-signed-tasks`, and optionally serves `/metrics` & `/healthz` (`--metrics-addr`).
2. Polls wire events without blocking on CPU-bound Wasm execution: incoming `TaskPayload`s are verified cryptographically and dispatched to `tokio::task::spawn_blocking` guarded by a `Semaphore` (`--max-concurrency`) and RAII `ActiveTaskGuard`.
3. Dispatches `JobPhase::MapScalar` or `JobPhase::MapBytes` / `JobPhase::ReduceBytes` into `NitroRuntime`, signs `ResultPayload` responses with its Ed25519 keypair, and publishes heartbeats onto `"worker-telemetry"`.

### 2.4 Guest WebAssembly Job Modules (`jobs/`)

- **`jobs/word-count`**: Scalar word counter exporting `alloc`, `dealloc`, and `map_line`.
- **`jobs/pattern-match`**: Multi-entrypoint log anomaly & metrics kernel exporting `map_line` (`ERROR`/`CRITICAL`/`FATAL`/`PANIC`), `count_lines`, and `count_chars`.
- **`jobs/kv-wordcount`**: Two-phase JSON key-value word-frequency kernel exporting `map_line`, `map_bytes`, and `reduce_bytes`.

---

## 3. Host-to-Guest Linear Memory ABIs

```text
Host (NitroRuntime)                              Guest Wasm Module (jobs/*)
-------------------                              --------------------------
1. Call guest `alloc(in_len)`         -------->  std::alloc::alloc(Layout) -> in_ptr
2. Write input bytes at `memory[in_ptr]` =====>  Linear Memory [in_ptr .. in_ptr + in_len]
3a. Scalar: `entry_fn(in_ptr, in_len)` <-------  return usize count (i32)
3b. Bytes:  `entry_fn(in_ptr, in_len)` ------->  allocate out_ptr, write JSON/KV bytes
                                       <-------  return packed ((out_ptr << 32) | out_len) (i64)
4. Bounds-checked `memory.read(out_ptr, out_len)` & `dealloc(...)` reclaim
```
