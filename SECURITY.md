# Security Policy and Threat Model

Nitro Compute is designed to execute untrusted, network-delivered compute workloads across decentralized peer-to-peer nodes. Memory safety, cryptographic provenance, and deterministic resource isolation are core architectural invariants of the engine.

---

## Threat Model

In a decentralized P2P compute fabric, an adversary may attempt to:

1. **CPU Exhaustion (Infinite Loops & Slow Calls)**: Broadcast a `TaskPayload` containing a WebAssembly module with an infinite loop (`loop {}`) or long-running stall to exhaust worker CPU cores.
2. **Memory / Table Exhaustion (OOM Bombs)**: Broadcast a WebAssembly module declaring oversized linear memories, calling `memory.grow` / `table.grow` repeatedly, or returning forged oversized output lengths (`out_len`).
3. **Wire Decompression Bombs & Bytecode Tampering**: Send malformed or oversized Zstandard wire frames, tamper with `wasm_binary` bytes, or spoof another worker's `PeerId` in `ResultPayload`s.
4. **Unauthorized Task Injection**: Publish rogue `TaskPayload`s onto `"task-alloc"` without Coordinator authorization.
5. **Host Memory Corruption & Unauthorized OS Access**: Exploit host FFI memory bugs or access host files/sockets from inside guest code.

---

## Defense-in-Depth Controls

### 1. Zero `unsafe` Host Code (`#![forbid(unsafe_code)]`)

- All three host crates (`core`/`nitro_core`, `coordinator`, and `worker`) compile under `#![forbid(unsafe_code)]`.
- Only `wasm32-wasip1` guest kernels run inside Wasmtime's linear-memory sandbox, strictly isolated from host memory and OS calls.

### 2. Dual-Layer CPU & Wall-Clock Execution Guards (`core/src/runtime.rs`)

- **Deterministic Instruction Fuel Metering**: `NitroRuntime::with_config` enables `config.consume_fuel(true)` and provisions each `Store` via `store.add_fuel(fuel_limit)?`. Exhausting fuel triggers `Trap::OutOfFuel` (`SandboxTrapError::OutOfFuel`).
- **Preemptive Wall-Clock Epoch Interruption**: `NitroRuntime` enables `config.epoch_interruption(true)` with a background `EpochTicker` (`10 ms` default tick) and sets `store.epoch_deadline_trap()` + `store.set_epoch_deadline(ticks)`. Runaway modules exceeding `execution_timeout_ms` are preempted via `Trap::Interrupt` (`SandboxTrapError::EpochTimeout`).

### 3. Linear Memory, Table & Output Bounds (`core/src/runtime.rs`)

- Every `Store<SandboxState>` attaches `SandboxLimiter` (`wasmtime::ResourceLimiter`) enforcing `max_memory_bytes` (`64 MiB` default) and `max_table_elements` (`10,000` default). Exceeding either triggers `SandboxTrapError::MemoryLimitExceeded` or `TableLimitExceeded`.
- Variable-length byte returns (`execute_bytes_with_options`) enforce `out_len <= max_output_bytes` (`4 MiB` default) and use bounds-checked `Memory::read` before allocating host buffers.

### 4. Capability-Based WASI Sandboxing

- Each task runs in a fresh, isolated `Store<SandboxState>` with `WasiCtxBuilder::new()`.
- No host directories are preopened (`preopened_dir` is never called), preventing guest modules from reading or modifying the host filesystem.

### 5. End-to-End Ed25519 Attestation & Bounded Wire Framing (`core/src/network.rs`)

- **Encrypted P2P Transport**: Noise Protocol handshakes over TCP + Yamux multiplexing + signed Gossipsub (`ValidationMode::Strict`).
- **Ed25519 Task & Result Attestation**:
  - Coordinators sign `TaskPayload::canonical_signing_bytes()` (SHA-256 over task ID, phase, entrypoint, Wasm SHA-256, chunk SHA-256, fuel, limits, and attempt). Workers enforcing `--trusted-coordinator <HEX>` or `--require-signed-tasks` reject rogue or tampered tasks prior to Wasmtime instantiation (`TaskOutcome::SecurityTrap { kind: "InvalidSignature" }`).
  - Workers sign `ResultPayload::canonical_signing_bytes()`; `ResultPayload::verify_signature` validates both the Ed25519 signature and that the signer public key derives `worker_peer_id` (blocking PeerId spoofing).
- **SHA-256 Artifact Integrity & Bounded Zstd Decoding**: `verify_wasm_integrity()` verifies `wasm_binary` against `wasm_sha256`, and `decode_wire_frame` enforces a `16 MiB` read cap (`std::io::Read::take`) against Zstd decompression bombs.

---

## Reporting a Vulnerability

If you discover a security vulnerability in Nitro Compute (such as a sandbox escape, limiter bypass, signature forgery, or P2P deserialization flaw):

1. Do **not** open a public GitHub issue containing exploit details.
2. Report the finding privately via GitHub Security Advisories on the [Nitro-Compute repository](https://github.com/AM4517UMOR4NG/Nitro-Compute/security) or contact the maintainer directly (`aekmohop@gmail.com`).
3. Include a minimal reproduction (`TaskPayload` or `.wat` / `.wasm` snippet) and the affected component (`core::runtime`, `core::network`, `worker`, or `coordinator`).
