# Nitro Compute Architecture Specification

## 1. Problem Statement and Project Goal

Traditional distributed data processing frameworks (such as Hadoop MapReduce or Apache Spark) rely on centralized master nodes, heavyweight JVM/container runtimes, and high-bandwidth data movement into centralized cloud regions. At the network edge, these assumptions break down due to:

- High container cold-start latency (hundreds of milliseconds to seconds).
- Bandwidth bottlenecks when uploading raw edge telemetry to central clusters.
- Single-point-of-failure orchestration brokers.

**Nitro Compute** solves this by combining three technologies into a single Rust workspace:

1. **WebAssembly (`wasm32-wasi`)** as a portable, architecture-neutral binary format for stateless compute kernels (~10 KB – 150 KB).
2. **Wasmtime (Cranelift JIT)** for sub-millisecond sandbox instantiation with deterministic instruction fuel metering.
3. **`libp2p` (Kademlia DHT + Gossipsub)** for decentralized peer discovery and encrypted pub/sub task orchestration over TCP, Noise, and Yamux.

---

## 2. Module Architecture

### 2.1 `core` (`core/src/lib.rs`, `runtime.rs`, `network.rs`)

The `core` library crate provides the foundational runtime and networking primitives shared by both `coordinator` and `worker`:

- **`NitroRuntime` (`core/src/runtime.rs`)**:
  - Wraps a `wasmtime::Engine` configured with `Strategy::Cranelift`, `OptLevel::Speed`, and `config.consume_fuel(true)`.
  - Exposes `execute_map(&self, wasm_bytes: &[u8], input_data: &str, fuel_limit: u64) -> anyhow::Result<usize>`.
  - Manages the full lifecycle of a single sandboxed Map execution:
    1. Builds an isolated WASI Preview 1 context (`WasiP1Ctx`).
    2. Initializes a `wasmtime::Store` and provisions execution fuel via `store.add_fuel(fuel_limit)?`.
    3. Compiles and instantiates the `.wasm` module with WASI imports linked via `preview1::add_to_linker_sync`.
    4. Allocates guest linear memory (calling the guest's `alloc` export or growing linear memory pages) and copies `input_data` into the sandbox using `Memory::write`.
    5. Invokes the exported `"map_line"` function `(i32, i32) -> i32`, catching `Trap::OutOfFuel` via `classify_execution_error`.
    6. Frees guest heap memory via `dealloc` and returns the computed count as `usize`.

- **`NitroBehavior` & P2P Swarm (`core/src/network.rs`)**:
  - Combines `kademlia::Kademlia<MemoryStore>` and `gossipsub::Behaviour` using `#[derive(NetworkBehavior)]`.
  - `create_swarm()` builds a Tokio-backed `libp2p::Swarm<NitroBehavior>` using TCP transport, Noise authenticated encryption, and Yamux multiplexing, subscribing automatically to `"task-alloc"` and `"task-results"`.
  - Defines the Serde-serializable wire protocol payloads:
    - `TaskPayload { task_id: String, wasm_binary: Vec<u8>, data_chunk: String, fuel: u64 }`
    - `ResultPayload { task_id: String, worker_peer_id: String, computed_value: usize }`

### 2.2 `coordinator` (`coordinator/src/main.rs`)

The Coordinator binary acts as the CLI network entry oracle and MapReduce reducer:

1. Streams a raw input file asynchronously using `tokio::io::BufReader` and groups non-empty lines into data chunks (`ingest_and_fragment_stream`).
2. Loads the compiled `.wasm` module from `jobs/word-count` (`load_wasm_binary`).
3. Packages each chunk into a `TaskPayload` with a unique `task_id` and fuel limit, publishing it onto the `"task-alloc"` Gossipsub topic (`broadcast_task_payloads`).
4. Subscribes to `"task-results"`, deduplicates incoming `ResultPayload` responses by `task_id`, and accumulates `computed_value` into the final unified MapReduce sum (`collect_and_aggregate_results`).

### 2.3 `worker` (`worker/src/main.rs`)

The Worker binary runs as a long-lived edge listener daemon:

1. Initializes `NitroRuntime` and the `libp2p` `Swarm`.
2. Listens on a configured TCP multiaddr and optionally dials bootstrap peers.
3. Polls incoming Gossipsub events via `run_event_loop_with_handler`.
4. Upon intercepting a `TaskPayload` on `"task-alloc"`, executes `runtime.execute_map(&task.wasm_binary, &task.data_chunk, task.fuel)`.
5. Packages the resulting `usize` alongside its local `PeerId` into a `ResultPayload` and publishes it back onto `"task-results"`.

### 2.4 `jobs/word-count` (`jobs/word-count/src/lib.rs`)

The reference guest compute module targeted for `wasm32-wasi`:

- Exports `alloc(len: usize) -> *mut u8` and `dealloc(ptr: *mut u8, len: usize)` backed by Rust's global allocator (`std::alloc`).
- Exports `map_line(ptr: *const u8, len: usize) -> usize`, reconstructing a UTF-8 slice from linear memory via `std::slice::from_raw_parts` and returning `line.split_whitespace().count()`.

---

## 3. Host-to-Guest Linear Memory ABI

Because WebAssembly `wasm32-wasi` modules communicate with the host across a 32-bit numeric ABI boundary (`i32` / `i64` / `f32` / `f64`), passing variable-length strings requires explicit linear memory orchestration:

```text
Host (NitroRuntime::execute_map)                 Guest Wasm Module (jobs/word-count)
--------------------------------                 -----------------------------------
1. Call guest `alloc(wasm_len)`       -------->  std::alloc::alloc(Layout) -> ptr
2. Write UTF-8 bytes at `memory[ptr]` ========>  Linear Memory [ptr .. ptr + len]
3. Call `map_line(ptr, wasm_len)`     -------->  slice::from_raw_parts(ptr, len)
                                                 str::from_utf8(bytes)
                                      <--------  return split_whitespace().count()
4. Call guest `dealloc(ptr, wasm_len)`-------->  std::alloc::dealloc(ptr, Layout)
```
