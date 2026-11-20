# Nitro Compute Architecture Specification

## 1. Problem Statement and Project Goal

Traditional distributed data processing frameworks (such as Hadoop MapReduce or Apache Spark) rely on centralized master nodes, heavyweight JVM/container runtimes, and high-bandwidth data movement into centralized cloud regions. At the network edge, these assumptions break down due to:

- High container cold-start latency (hundreds of milliseconds to seconds).
- Bandwidth bottlenecks when uploading raw edge telemetry to central clusters.
- Single-point-of-failure orchestration brokers.

**Nitro Compute** solves this by combining three technologies into a single Rust workspace:

1. **WebAssembly (`wasm32-wasip1`)** as a portable, architecture-neutral binary format for stateless compute kernels (~13 KB – 68 KB).
2. **Wasmtime (Cranelift JIT)** for sub-millisecond sandbox instantiation with deterministic instruction fuel metering, `ResourceLimiter` linear-memory bounds, and SHA-256 compiled module caching.
3. **`libp2p` (Kademlia DHT + Gossipsub + mDNS + Identify + Ping)** for zero-config peer discovery, RTT monitoring, Zstd-compressed pub/sub framing, and encrypted Noise + Yamux streams.

---

## 2. Module Architecture

### 2.1 `core` (`nitro_core` — `core/src/lib.rs`, `runtime.rs`, `network.rs`)

The `core` library crate (`lib name = "nitro_core"`) provides the foundational runtime and networking primitives shared by both `coordinator` and `worker`:

- **`NitroRuntime` (`core/src/runtime.rs`)**:
  - Wraps a `wasmtime::Engine` configured with `Strategy::Cranelift`, `OptLevel::Speed`, and `config.consume_fuel(true)`.
  - Enforces bounded memory/table ceilings via a custom `ResourceLimiter` (`SandboxLimiter` tracking `max_memory_bytes` and `max_table_elements`).
  - Maintains a thread-safe **SHA-256 Content-Addressable Module Cache** (`RwLock<HashMap<[u8; 32], CachedModule>>`) so repeated task chunks reuse pre-compiled Cranelift `Module` instances instead of recompiling per chunk.
  - Exposes both `execute_map(&self, wasm_bytes: &[u8], input_data: &str, fuel_limit: u64) -> anyhow::Result<usize>` and `execute_with_options(...) -> anyhow::Result<ExecutionMetrics>` supporting arbitrary entrypoints (`"map_line"`, `"count_lines"`, `"count_chars"`).
  - Surfaces deterministic `SandboxTrapError` variants (`OutOfFuel`, `MemoryLimitExceeded`, `TableLimitExceeded`).

- **`NitroBehavior` & P2P Wire Layer (`core/src/network.rs`)**:
  - Combines `kademlia::Kademlia<MemoryStore>`, `gossipsub::Behaviour`, `Toggle<mdns::tokio::Behaviour>`, `identify::Behaviour`, and `ping::Behaviour` via `#[derive(NetworkBehavior)]`.
  - Raises Gossipsub `max_transmit_size` to `8 MiB` and wraps serialized payloads in transparent **Zstandard (`NZST` prefix)** compression with bounded `16 MiB` decompression limits.
  - Defines Serde-serializable wire payloads across three Gossipsub channels:
    - `"task-alloc"` -> `TaskPayload { task_id, job_id, entry_fn, wasm_binary, wasm_sha256, data_chunk, fuel, max_memory_bytes, attempt }`
    - `"task-results"` -> `ResultPayload { task_id, job_id, worker_peer_id, computed_value, status: TaskOutcome, fuel_consumed, duration_us, cache_hit, attempt }`
    - `"worker-telemetry"` -> `WorkerTelemetry { worker_peer_id, active_tasks, completed_tasks, trapped_tasks, cached_modules, uptime_secs }`

### 2.2 `coordinator` (`coordinator/src/main.rs`)

The Coordinator binary acts as the fault-tolerant CLI network entry oracle and MapReduce reducer:

1. Streams a raw input file asynchronously using `tokio::io::BufReader` and groups non-empty lines into chunks (`ingest_and_fragment_stream`).
2. Loads the compiled `.wasm` module (`load_wasm_binary`) and computes its `wasm_sha256` digest.
3. Waits for mesh worker readiness (`wait_for_mesh_workers`) via explicit `--peer` dials or automatic LAN mDNS discovery.
4. Drives `JobScheduler`, dispatching `TaskPayload`s onto `"task-alloc"`, monitoring per-task leases (`--task-timeout`), retrying transient errors or timed-out tasks up to `--retries`, halting poisoned tasks that hit `SecurityTrap`, and aggregating `ResultPayload`s into a final unified sum with per-worker telemetry statistics.

### 2.3 `worker` (`worker/src/main.rs`)

The Worker binary runs as a non-blocking, concurrency-bounded edge compute daemon:

1. Initializes `NitroRuntime` (with `--max-memory-mb`) and the `libp2p` `Swarm` (with mDNS, Identify, Ping, Kademlia, and Gossipsub).
2. Polls wire events without blocking on CPU-bound Wasm execution: incoming `TaskPayload`s are dispatched to `tokio::task::spawn_blocking` guarded by a `tokio::sync::Semaphore` (`--max-concurrency`) and RAII `ActiveTaskGuard`.
3. Validates `wasm_binary` integrity against `wasm_sha256` (and supports worker-side artifact caching keyed by SHA-256).
4. Emits structured `ResultPayload` responses (including `TaskOutcome::SecurityTrap` when a guest triggers `OutOfFuel` or `MemoryLimitExceeded`) back onto `"task-results"`, and periodically publishes `WorkerTelemetry` heartbeats onto `"worker-telemetry"`.

### 2.4 Guest WebAssembly Job Modules (`jobs/`)

- **`jobs/word-count` (`jobs/word-count/src/lib.rs`)**: Counts whitespace-delimited words via `map_line(ptr, len)`.
- **`jobs/pattern-match` (`jobs/pattern-match/src/lib.rs`)**: Multi-entrypoint log anomaly and metrics kernel exporting `map_line` (counts `ERROR`/`CRITICAL`/`FATAL`/`PANIC` lines), `count_lines`, and `count_chars`.

---

## 3. Host-to-Guest Linear Memory ABI

Because WebAssembly `wasm32-wasip1` modules communicate with the host across a 32-bit numeric ABI boundary (`i32`), passing variable-length strings requires explicit linear memory orchestration:

```text
Host (NitroRuntime::execute_with_options)       Guest Wasm Module (jobs/*)
------------------------------------------       --------------------------
1. Call guest `alloc(wasm_len)`       -------->  std::alloc::alloc(Layout) -> ptr
2. Write UTF-8 bytes at `memory[ptr]` ========>  Linear Memory [ptr .. ptr + len]
3. Call `<entry_fn>(ptr, wasm_len)`   -------->  slice::from_raw_parts(ptr, len)
                                                 str::from_utf8(bytes)
                                      <--------  return computed usize count
4. Call guest `dealloc(ptr, wasm_len)`-------->  std::alloc::dealloc(ptr, Layout)
```
