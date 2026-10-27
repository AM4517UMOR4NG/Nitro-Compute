# Contributing to Nitro Compute

Thank you for your interest in contributing to **Nitro Compute**.

Nitro Compute is an open-source, decentralized peer-to-peer serverless MapReduce engine written in Rust. Its primary mission is to enable ultra-low latency distributed data processing across edge nodes by compiling compute tasks into sandboxed `wasm32-wasi` WebAssembly modules and orchestrating them over an encrypted `libp2p` mesh network.

---

## Core Design Principles

Every contribution to Nitro Compute should uphold four foundational guarantees:

1. **Zero-Trust Guest Isolation**: Worker nodes must never trust incoming WebAssembly bytecode. All guest modules must run inside capability-restricted WASI sandboxes with deterministic instruction fuel metering (`Trap::OutOfFuel`).
2. **Decentralized Mesh Resilience**: P2P discovery and task routing must operate without a single point of failure, leveraging Kademlia DHT and signed Gossipsub channels over Noise-encrypted TCP/Yamux streams.
3. **Sub-Millisecond Cold Starts**: Runtime initialization and Host-to-Guest linear memory transfers must remain lightweight and allocation-conscious.
4. **Zero-Warning Compilation**: All crates across the Cargo workspace must compile cleanly without compiler or Clippy warnings.

---

## Development Environment Setup

### 1. Prerequisites

- **Rust Toolchain**: Stable Rust (`rustc` and `cargo`, 2021 edition).
- **WASI Target**: Required for compiling guest MapReduce jobs under `jobs/`:
  ```bash
  rustup target add wasm32-wasi
  ```

### 2. Clone and Build

```bash
git clone https://github.com/AM4517UMOR4NG/Nitro-Compute.git
cd Nitro-Compute

# Build the WebAssembly guest job
cargo build --manifest-path jobs/word-count/Cargo.toml --target wasm32-wasi --release

# Build the workspace binaries (coordinator and worker)
cargo build --release
```

---

## How to Contribute

### 1. Authoring New WebAssembly MapReduce Jobs (`jobs/<job-name>`)

To add a new serverless compute kernel to the workspace:

1. Create a new library crate under `jobs/<job-name>` and add it to the root `Cargo.toml` `members` list.
2. Configure `[lib]` with `crate-type = ["cdylib", "rlib"]` and set `target = "wasm32-wasi"` in `jobs/<job-name>/.cargo/config.toml`.
3. Implement the guest linear memory ABI contract in `src/lib.rs`:

```rust
#[no_mangle]
pub extern "C" fn alloc(len: usize) -> *mut u8 { /* ... */ }

#[no_mangle]
pub extern "C" fn dealloc(ptr: *mut u8, len: usize) { /* ... */ }

#[no_mangle]
pub extern "C" fn map_line(ptr: *const u8, len: usize) -> usize { /* ... */ }
```

- `alloc` and `dealloc` allow `NitroRuntime::execute_map` to safely allocate and reclaim heap buffers inside the guest's linear memory without corrupting stack frames or static data.
- `map_line` receives the non-null pointer offset and byte length of the injected UTF-8 data chunk and returns the computed `usize` result.

### 2. Modifying the Core Runtime (`core/src/runtime.rs`)

- Preserve `Strategy::Cranelift`, `OptLevel::Speed`, and `config.consume_fuel(true)` in `NitroRuntime::new()`.
- Ensure any new host functions or Wasm invocations pass through `NitroRuntime::classify_execution_error` so out-of-fuel traps (`wasmtime::Trap::OutOfFuel`) are surfaced as explicit security resource exhaustion errors.

### 3. Modifying the P2P Network Layer (`core/src/network.rs`)

- Wire protocol structures (`TaskPayload`, `ResultPayload`) must derive `Debug`, `Clone`, `serde::Serialize`, and `serde::Deserialize`.
- Maintain strict Gossipsub message validation (`ValidationMode::Strict`) and signed message authenticity (`MessageAuthenticity::Signed`).

---

## Pull Request Checklist

Before opening a Pull Request:

1. Format all workspace crates:
   ```bash
   cargo fmt --all -- --check
   ```
2. Run Clippy across the workspace:
   ```bash
   cargo clippy --workspace --all-targets -- -D warnings
   ```
3. Verify release builds for both the WASI guest job and host binaries:
   ```bash
   cargo build --manifest-path jobs/word-count/Cargo.toml --target wasm32-wasi --release
   cargo build --release
   ```
4. Use [Conventional Commits](https://www.conventionalcommits.org/) for commit messages:
   - `feat(scope): ...` for new features
   - `fix(scope): ...` for bug fixes
   - `security(scope): ...` for sandbox or P2P security hardening
   - `refactor(scope): ...` for internal restructuring
   - `docs(scope): ...` for documentation updates
   - `chore(scope): ...` for build or tooling maintenance
