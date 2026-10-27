# Security Policy and Threat Model

Nitro Compute is designed to execute untrusted, network-delivered compute workloads across decentralized peer-to-peer nodes. Security isolation is a core architectural requirement of the engine.

---

## Threat Model

In a decentralized P2P compute fabric, an adversary may attempt to:

1. **Broadcast Malicious Bytecode (DoS / Infinite Loops)**: Publish a `TaskPayload` containing a WebAssembly module with an intentional infinite loop (`loop {}`) to exhaust worker CPU cores.
2. **Sandbox Escape / Memory Corruption**: Craft out-of-bounds linear memory reads or writes inside the Wasm guest to inspect or corrupt host worker memory.
3. **Unauthorized Host System Access**: Attempt to read local files, environment secrets, or open unauthorized network sockets from inside a guest MapReduce task.
4. **Network Spoofing / Man-in-the-Middle**: Intercept or tamper with task allocations or result payloads in transit across the P2P mesh.

---

## Defense-in-Depth Controls

### 1. Deterministic Instruction Fuel Metering (`core/src/runtime.rs`)

Wall-clock timeouts are non-deterministic and vulnerable to thread scheduling jitter. Nitro Compute enforces instruction-level metering directly inside the Wasmtime Cranelift compiler:

- `NitroRuntime::new` enables `config.consume_fuel(true)`.
- Every execution store is provisioned with a strict fuel quota via `store.add_fuel(fuel_limit)?`.
- Each executed Wasm instruction decrements the store's fuel counter. When fuel reaches `0`, execution immediately traps with `wasmtime::Trap::OutOfFuel`.
- `NitroRuntime::classify_execution_error` intercepts `Trap::OutOfFuel` across module instantiation, guest memory allocation, and `"map_line"` execution, aborting the task cleanly without crashing the worker daemon.

### 2. Linear Memory Isolation and Guard Pages

- Wasmtime compiles guest modules using Cranelift (`OptLevel::Speed`) with hardware virtual memory guard pages, ensuring all guest memory accesses are strictly bounds-checked to the module's linear memory address space.
- Host-to-guest chunk injection (`NitroRuntime::execute_map`) requests a dedicated heap region via the guest's exported `alloc` function (or grows linear memory pages at a non-zero offset), validates pointer bounds via `Memory::write`, and reclaims the buffer via `dealloc` after execution.

### 3. Capability-Based WASI Sandboxing

- Each task runs in a fresh, isolated `Store<WasiP1Ctx>` created via `WasiCtxBuilder::new()`.
- No host directories are preopened (`preopened_dir` is never called), preventing guest modules from reading or modifying the host filesystem.

### 4. Authenticated P2P Transport (`core/src/network.rs`)

- All peer-to-peer connections use **Noise Protocol** cryptographic handshakes over TCP with **Yamux** stream multiplexing.
- Gossipsub messages on `"task-alloc"` and `"task-results"` require cryptographic signatures (`MessageAuthenticity::Signed(keypair)`) and are validated under `ValidationMode::Strict`.

---

## Reporting a Vulnerability

If you discover a security vulnerability in Nitro Compute (such as a sandbox escape, fuel metering bypass, or P2P deserialization flaw):

1. Do **not** open a public GitHub issue containing exploit details.
2. Report the finding privately via GitHub Security Advisories on the [Nitro-Compute repository](https://github.com/AM4517UMOR4NG/Nitro-Compute/security) or contact the maintainer directly (`aekmohop@gmail.com`).
3. Include a minimal reproduction (`TaskPayload` or `.wat` / `.wasm` snippet) and the affected component (`core::runtime`, `core::network`, `worker`, or `coordinator`).
