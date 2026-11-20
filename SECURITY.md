# Security Policy and Threat Model

Nitro Compute is designed to execute untrusted, network-delivered compute workloads across decentralized peer-to-peer nodes. Security isolation is a core architectural requirement of the engine.

---

## Threat Model

In a decentralized P2P compute fabric, an adversary may attempt to:

1. **CPU Exhaustion (Infinite Loops)**: Broadcast a `TaskPayload` containing a WebAssembly module with an intentional infinite loop (`loop {}`) to exhaust worker CPU cores.
2. **Memory / Table Exhaustion (OOM Bombs)**: Broadcast a WebAssembly module that declares oversized linear memories or invokes `memory.grow` / `table.grow` repeatedly to crash worker hosts.
3. **Wire Decompression Bombs & Bytecode Tampering**: Send malformed or oversized Zstandard wire frames, or tamper with `wasm_binary` bytes in transit.
4. **Sandbox Escape & Unauthorized Host Access**: Attempt out-of-bounds linear memory reads/writes, filesystem tampering, or unauthorized socket connections from inside guest code.

---

## Defense-in-Depth Controls

### 1. Deterministic Instruction Fuel Metering (`core/src/runtime.rs`)

Wall-clock timeouts are non-deterministic and vulnerable to thread scheduling jitter. Nitro Compute enforces instruction-level metering directly inside the Wasmtime Cranelift compiler:

- `NitroRuntime::with_config` enables `config.consume_fuel(true)`.
- Every execution store is provisioned with a strict fuel quota via `store.add_fuel(fuel_limit)?`.
- When fuel reaches `0`, execution immediately traps with `wasmtime::Trap::OutOfFuel`.
- `NitroRuntime::classify_execution_error` converts this into `SandboxTrapError::OutOfFuel` and surfaces `TaskOutcome::SecurityTrap` to the Coordinator, halting further retries on poisoned tasks.

### 2. Linear Memory & Table ResourceLimiter (`core/src/runtime.rs`)

- Each `Store<SandboxState>` attaches a custom `wasmtime::ResourceLimiter` (`SandboxLimiter`) configured with `max_memory_bytes` (default `64 MiB`) and `max_table_elements` (default `10,000`).
- Any `memory.grow` or `table.grow` exceeding configured bounds returns an immediate trap error (`SandboxTrapError::MemoryLimitExceeded` / `SandboxTrapError::TableLimitExceeded`), blocking memory-bomb DoS attacks before host allocation occurs.
- Wasmtime Cranelift (`OptLevel::Speed`) additionally enforces virtual memory guard pages and bounds-checked linear memory accesses (`Memory::write`, guest `alloc`/`dealloc`).

### 3. Capability-Based WASI Sandboxing

- Each task runs in a fresh, isolated `Store<SandboxState>` with `WasiCtxBuilder::new()`.
- No host directories are preopened (`preopened_dir` is never called), preventing guest modules from reading or modifying the host filesystem.

### 4. Wire Frame Integrity & Decompression-Bomb Protection (`core/src/network.rs`)

- **Encrypted Authenticated Transport**: All peer connections use **Noise Protocol** over TCP with **Yamux** stream multiplexing; Gossipsub messages require `MessageAuthenticity::Signed(keypair)` under `ValidationMode::Strict`.
- **SHA-256 Artifact Verification**: `TaskPayload::verify_wasm_integrity()` verifies embedded `wasm_binary` bytes against `wasm_sha256` before compilation or caching.
- **Bounded Zstd Decoding**: `decode_wire_frame` enforces a hard `MAX_WIRE_PAYLOAD_BYTES` (`16 MiB`) read cap using `std::io::Read::take`, defending against compressed payload expansion attacks.

---

## Reporting a Vulnerability

If you discover a security vulnerability in Nitro Compute (such as a sandbox escape, fuel/memory limiter bypass, or P2P deserialization flaw):

1. Do **not** open a public GitHub issue containing exploit details.
2. Report the finding privately via GitHub Security Advisories on the [Nitro-Compute repository](https://github.com/AM4517UMOR4NG/Nitro-Compute/security) or contact the maintainer directly (`aekmohop@gmail.com`).
3. Include a minimal reproduction (`TaskPayload` or `.wat` / `.wasm` snippet) and the affected component (`core::runtime`, `core::network`, `worker`, or `coordinator`).
