# Changelog

All notable changes to **Nitro Compute** are documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [0.2.0] - 2026-11-20

### Added
- **Memory & Table ResourceLimiter (`core::runtime`)**: Added `SandboxLimiter` (`wasmtime::ResourceLimiter`) and `SandboxConfig` enforcing configurable linear memory (`64 MiB` default) and table element bounds (`SandboxTrapError::MemoryLimitExceeded`, `TableLimitExceeded`).
- **SHA-256 Compiled Module Cache (`core::runtime`)**: Implemented bounded in-memory `Module` caching keyed by SHA-256 digest (`sha2`) alongside `ExecutionMetrics` telemetry (`fuel_consumed`, `peak_memory_bytes`, `duration_us`, `cache_hit`).
- **Zstandard Wire Framing & Integrity (`core::network`)**: Implemented `NZST`-prefixed Zstd payload compression with `16 MiB` decompression-bomb limits, raised Gossipsub `max_transmit_size` to `8 MiB`, and added SHA-256 bytecode verification (`TaskPayload::verify_wasm_integrity`).
- **LAN mDNS Auto-Discovery, Identify & Ping (`core::network`)**: Upgraded `NitroBehavior` with `Toggle<mdns::tokio::Behaviour>`, `identify::Behaviour`, `ping::Behaviour`, and `"worker-telemetry"` heartbeats.
- **Non-Blocking Worker Execution Pool (`worker`)**: Offloaded Wasmtime execution onto `tokio::task::spawn_blocking` bounded by a `Semaphore` (`--max-concurrency`), added RAII `ActiveTaskGuard`, worker artifact caching, and `Ctrl+C` graceful shutdown.
- **Fault-Tolerant Job Scheduler (`coordinator`)**: Implemented `JobScheduler` with mDNS/dial readiness wait (`--min-peers`), per-task lease timeouts (`--task-timeout`), automatic retries (`--retries`), security-trap short-circuiting, and per-worker performance summaries.
- **Multi-Entrypoint Log Kernel (`jobs/pattern-match`)**: Added a second `wasm32-wasip1` compute kernel supporting `map_line`, `count_lines`, and `count_chars`, plus unit tests across both job crates.
- **End-to-End Integration Tests & Strict CI**: Added runtime unit tests (`wat`), a live 2-Swarm Noise+Yamux P2P integration test (`core/tests/p2p_mapreduce.rs`), and upgraded CI to enforce `rustfmt`, `clippy -D warnings`, `cargo test --workspace`, and `wasm32-wasip1` builds.

### Changed
- Updated WASI job targets from deprecated `wasm32-wasi` to `wasm32-wasip1` across `jobs/word-count` and `jobs/pattern-match`.

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
