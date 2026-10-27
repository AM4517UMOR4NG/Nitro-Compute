# Changelog

All notable changes to **Nitro Compute** are documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [0.1.0] - 2026-10-27

### Added
- **Multi-Module Cargo Workspace**: Initialized workspace structure containing `core`, `coordinator`, `worker`, and `jobs/word-count`.
- **Sandboxed Wasmtime Execution Layer (`core::runtime`)**:
  - Implemented `NitroRuntime` powered by `wasmtime` v22.0 with the Cranelift compiler backend (`OptLevel::Speed`).
  - Added WASI Preview 1 isolation (`wasmtime_wasi::preview1::WasiP1Ctx`) and safe linear memory injection (`alloc`, `dealloc`, `Memory::write`).
  - Enabled deterministic instruction fuel metering (`config.consume_fuel(true)` and `store.add_fuel(fuel_limit)`) with `Trap::OutOfFuel` security breach detection.
- **Decentralized P2P Networking Layer (`core::network`)**:
  - Implemented `NitroBehavior` combining Kademlia DHT (`kad::Behaviour<MemoryStore>`) and signed Gossipsub (`gossipsub::Behaviour`) over TCP, Noise encryption, and Yamux stream multiplexing.
  - Defined Serde-serializable `TaskPayload` and `ResultPayload` wire formats across bidirectional Gossipsub topics (`"task-alloc"` and `"task-results"`).
- **Edge Worker Daemon (`worker`)**:
  - Built asynchronous background listener daemon that intercepts `"task-alloc"` payloads, executes `"map_line"` inside fuel-metered Wasmtime sandboxes, and publishes `ResultPayload`s to `"task-results"`.
- **Coordinator CLI Oracle (`coordinator`)**:
  - Built CLI tool for streaming and fragmenting input text files, loading compiled `.wasm` job modules, broadcasting `TaskPayload`s, and aggregating worker `ResultPayload` responses into a unified MapReduce sum.
- **WebAssembly Guest Job (`jobs/word-count`)**:
  - Implemented `wasm32-wasi` guest library exporting `alloc`, `dealloc`, and `map_line` for whitespace-delimited word counting.
- **Unified Telemetry**:
  - Standardized structured stdout logging across all modules via `tracing` and `tracing-subscriber`.
