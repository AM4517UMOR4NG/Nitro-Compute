# Nitro Compute

Nitro Compute is a decentralized peer-to-peer serverless MapReduce engine built with Rust, `libp2p`, and `Wasmtime`. It compiles compute tasks into lightweight, sandboxed WebAssembly modules (`wasm32-wasi`) for low-latency distributed data processing across edge nodes.

Instead of transferring raw datasets to centralized cloud clusters or provisioning heavy container runtimes, Nitro Compute fragments input streams at the coordinator node, packages them alongside compiled `.wasm` kernels, and distributes workloads across an encrypted peer-to-peer mesh network.

## Overview

- **Sub-Millisecond Wasm Sandboxing**: Executes untrusted compute modules using the Wasmtime engine with the Cranelift compiler backend configured to `OptLevel::Speed` and WASI Preview 1 isolation.
- **Deterministic Fuel Metering**: Prevents infinite loops and resource exhaustion on worker nodes by enforcing instruction-level fuel budgets (`config.consume_fuel(true)` and `store.add_fuel(fuel_limit)`), trapping any execution that exceeds its quota (`Trap::OutOfFuel`).
- **Decentralized P2P Mesh Transport**: Uses `libp2p` over TCP with Noise cryptographic handshakes, Yamux stream multiplexing, Kademlia DHT peer routing, and signed Gossipsub broadcast channels.
- **Bidirectional MapReduce Pipeline**: Dispatches workloads on the `"task-alloc"` Gossipsub channel and aggregates computed worker outputs returned on the `"task-results"` channel.

## Architecture

```text
[ COMPUTER A: COORDINATOR ] <----------------------------------------+
  |                                                                   |
  +-- 1. Ingest Data Stream (Fragment large text stream into chunks)  | (Phase 6: Gossipsub)
  +-- 2. Load jobs/word-count.wasm (Compiled Wasm bytecode)           | "task-results" channel
  +-- 3. Pack Payload: TaskPayload { data_chunk, wasm, fuel_limit }   | carries ResultPayload
  |                                                                   |
  v  (Phase 5: Broadcast to "task-alloc" channel via Gossipsub)       |
========================== DECENTRALIZED P2P NETWORK ==========================
  |                                                                   |
  +--> Multi-routing & Peer Discovery (Kademlia DHT)                  |
  +--> Encrypted Transport (Noise Protocol + Yamux Multiplexing)      |
  |                                                                   |
===============================================================================
  |
  v  (Intercepted asynchronously by idle Worker nodes)
[ COMPUTER B: WORKER NODE ] -----------------------------------------+
  |
  +-- 4. Intercept network frame & decode binary TaskPayload
  +-- 5. Boot Sandbox: Instantiate Wasmtime instance (< 1ms)
  +-- 6. Fuel Metering: Inject `store.add_fuel(fuel_limit)` into Store
  |         +-- Within fuel budget -> Execute exported `map_line` via linear memory
  |         +-- Infinite loop / Exceeded -> Trap::OutOfFuel aborts execution safely
  |
  +-- 7. Pack Result: Wrap computed integer into `ResultPayload`
```

## Getting Started

### Prerequisites

- Rust 1.75+ (`rustc` and `cargo`, 2021 edition)
- WebAssembly WASI target installed via `rustup`:

```bash
rustup target add wasm32-wasi
```

### Build

1. Compile the `word-count` WebAssembly guest job:

```bash
cargo build --manifest-path jobs/word-count/Cargo.toml --target wasm32-wasi --release
```

2. Build the workspace binaries (`coordinator` and `worker`) in release mode:

```bash
cargo build --release
```

### Usage

1. Start one or more worker daemons on local or remote nodes:

```bash
RUST_LOG=info cargo run --release --bin worker -- --listen /ip4/0.0.0.0/tcp/4001
```

2. Run the coordinator oracle to fragment an input stream file, broadcast `.wasm` tasks to the mesh, and aggregate the returned counts:

```bash
RUST_LOG=info cargo run --release --bin coordinator -- \
  --input input.txt \
  --wasm target/wasm32-wasi/release/word_count.wasm \
  --peer /ip4/127.0.0.1/tcp/4001 \
  --chunk-lines 10 \
  --fuel 10000000 \
  --timeout 30
```

## Security and Isolation

Worker nodes execute arbitrary WebAssembly bytecode received over the network. [`core/src/runtime.rs`](./core/src/runtime.rs) enforces strict isolation boundaries for every task invocation:

1. **Memory Isolation**: Host input strings are written into guest linear memory using explicit guest heap allocation (`alloc` and `dealloc` exports in [`jobs/word-count/src/lib.rs`](./jobs/word-count/src/lib.rs)) and bounds-checked writes (`Memory::write`).
2. **WASI Capability Control**: Each invocation creates an isolated `Store<WasiP1Ctx>` without host filesystem mounts or network socket descriptors.
3. **Instruction Fuel Metering**: `NitroRuntime::new` enables `config.consume_fuel(true)`. Before invoking guest code, `NitroRuntime::execute_map` calls `store.add_fuel(fuel_limit)?`. If guest execution exhausts its allocated fuel, Wasmtime triggers `Trap::OutOfFuel`, which is intercepted and reported as a security resource exhaustion breach without terminating the worker process.

## Network Protocol

[`core/src/network.rs`](./core/src/network.rs) defines `NitroBehavior`, a composite `libp2p` `NetworkBehaviour` integrating Kademlia DHT (`kad::Behaviour<MemoryStore>`) and signed Gossipsub (`gossipsub::Behaviour`).

### Gossipsub Channels and Wire Formats

#### `"task-alloc"` (`TaskPayload`)

Published by the coordinator node for each fragmented data chunk:

```json
{
  "task_id": "task-0",
  "wasm_binary": [0, 97, 115, 109, 1, 0, 0, 0],
  "data_chunk": "distributed serverless mapreduce on webassembly edge nodes",
  "fuel": 10000000
}
```

#### `"task-results"` (`ResultPayload`)

Published by worker nodes upon completing `NitroRuntime::execute_map`:

```json
{
  "task_id": "task-0",
  "worker_peer_id": "12D3KooW...",
  "computed_value": 7
}
```

## CLI Reference

### `coordinator`

| Option | Short | Default | Description |
| :--- | :--- | :--- | :--- |
| `--input <PATH>` | `-i` | `input.txt` | Path to the input text file to stream and fragment. |
| `--wasm <PATH>` | `-w` | Auto-detected | Path to the compiled `.wasm` module binary. |
| `--listen <MULTIADDR>` | `-l` | `/ip4/0.0.0.0/tcp/0` | Local TCP multiaddr for the coordinator swarm. |
| `--peer <MULTIADDR>` | `-p` | None | Worker peer multiaddr to dial (repeatable). |
| `--chunk-lines <N>` | `-c` | `1` | Number of non-empty lines grouped per `TaskPayload`. |
| `--fuel <UNITS>` | `-f` | `10000000` | Wasmtime instruction fuel budget per task chunk. |
| `--timeout <SECS>` | `-t` | `30` | Timeout in seconds to wait for worker `ResultPayload` responses. |

### `worker`

| Option | Short | Default | Description |
| :--- | :--- | :--- | :--- |
| `--listen <MULTIADDR>` | `-l` | `/ip4/0.0.0.0/tcp/0` | Local TCP multiaddr for the worker daemon. |
| `--peer <MULTIADDR>` | `-p` | None | Bootstrap peer multiaddr to dial on startup (repeatable). |

## Project Structure

```text
nitro-compute/
├── Cargo.toml                  # Workspace manifest and shared dependency versions
├── Cargo.lock                  # Locked dependency graph
├── core/                       # Core runtime and P2P networking library
│   ├── Cargo.toml
│   └── src/
│       ├── lib.rs              # Public exports and tracing initialization
│       ├── runtime.rs          # NitroRuntime (Wasmtime, WASI Preview 1, Fuel Metering)
│       └── network.rs          # NitroBehavior (Kademlia, Gossipsub, Wire Payloads)
├── coordinator/                # Coordinator CLI binary
│   ├── Cargo.toml
│   └── src/
│       └── main.rs             # Stream chunker, task dispatcher, and result aggregator
├── worker/                     # Worker daemon binary
│   ├── Cargo.toml
│   └── src/
│       └── main.rs             # Task listener, Wasm executor, and result publisher
└── jobs/
    └── word-count/             # WebAssembly guest library (wasm32-wasi)
        ├── .cargo/config.toml
        ├── Cargo.toml
        └── src/
            └── lib.rs          # Exported alloc, dealloc, and map_line functions
```

## License

See [LICENSE](./LICENSE) for details.
