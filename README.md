# Nitro Compute

> **Decentralized P2P serverless MapReduce engine powered by Rust, libp2p, and Wasmtime.**  
> Compiles compute tasks into lightweight, sandboxed WebAssembly modules for ultra-low latency big data orchestration at the edge.

---

## End-to-End Execution Pipeline

```text
[ COMPUTER A: COORDINATOR ] ◄────────────────────────────────────────┐
  │                                                                   │
  ├── 1. Ingest Data Stream (File teks besar dipecah jadi chunk)      │ (Fase 6: Gossipsub)
  ├── 2. Load jobs/word-count.wasm (Bytecode terkompilasi)            │ "task-results" channel
  ├── 3. Pack Payload: TaskPayload { data_chunk, wasm, fuel_limit }   │ membawa ResultPayload
  │                                                                   │
  ▼  (Fase 5: Kirim ke "task-alloc" channel via libp2p Gossipsub)     │
========================== JARINGAN DECENTRALIZED P2P ==========================
  │                                                                   │
  ├──► Multi-routing & Peer Discovery (Kademlia DHT)                  │
  ├──► Transport Terenkripsi Aman (Noise Protocol + Yamux Multiplex)  │
  │                                                                   │
================================================================================
  │
  ▼  (Diterima Node Worker yang idle secara asinkronus)
[ COMPUTER B: WORKER NODE ] ──────────────────────────────────────────┘
  │
  ├── 4. Intersept Jaringan & Decode TaskPayload biner
  ├── 5. Booting Sandbox: Inisialisasi Instansi Wasmtime (< 1ms)
  ├── 6. FUEL METERING: Suntik `store.add_fuel(fuel_limit)` ke Sandbox
  │         ├── JIKA kode aman -> Eksekusi fungsi `map_line` via pointer memori
  │         └── JIKA kena infinite-loop -> Trap aktif, eksekusi diputus paksa (Aman!)
  │
  └── 7. Pack Result: Bungkus hasil angka ke `ResultPayload`
```

---

## Codebase Mapping per Pipeline Stage

| Tahap | Komponen | File & Fungsi Implementasi |
| :--- | :--- | :--- |
| **1. Ingest Data Stream** | Coordinator | `coordinator/src/main.rs` — `ingest_and_fragment_stream()` membaca stream file teks besar menggunakan `tokio::io::BufReader` dan memecahnya menjadi *chunk*. |
| **2. Load `.wasm` Bytecode** | Coordinator & Guest Job | `coordinator/src/main.rs` — `load_wasm_binary()` memuat hasil kompilasi dari `jobs/word-count/src/lib.rs` (`map_line`). |
| **3. Pack `TaskPayload` & Broadcast** | Coordinator → `"task-alloc"` | `core/src/network.rs` — `TaskPayload { task_id, wasm_binary, data_chunk, fuel }` dikirim lewat `publish_task_payload()` pada topik Gossipsub `"task-alloc"`. |
| **P2P Mesh Fabric** | `libp2p` Swarm | `core/src/network.rs` — `create_swarm()` menggabungkan `kademlia::Kademlia` (DHT) + `gossipsub::Behaviour` di atas TCP, `noise::Config`, dan `yamux::Config`. |
| **4. Intercept & Decode `TaskPayload`** | Worker Node | `worker/src/main.rs` & `core/src/network.rs` — `run_event_loop_with_handler()` menangkap pesan `"task-alloc"` dan melakukan deserialisasi `TaskPayload::from_bytes()`. |
| **5. Booting Wasmtime Sandbox** | Core Runtime | `core/src/runtime.rs` — `NitroRuntime::new()` mengonfigurasi compiler **Cranelift** (`OptLevel::Speed`) dan menginisialisasi WASI Preview 1 (`WasiP1Ctx`). |
| **6. Fuel Metering & `map_line` Execution** | Core Runtime | `core/src/runtime.rs` — `NitroRuntime::execute_map()` menyuntikkan `store.add_fuel(fuel_limit)?`, mengalokasikan memori linear Wasm, menyalin *chunk* data, memanggil `"map_line"`, dan menangkap `Trap::OutOfFuel` jika terjadi *infinite loop*. |
| **7. Pack `ResultPayload` & Return Loop** | Worker → `"task-results"` → Coordinator | `worker/src/main.rs` membungkus hasil ke `ResultPayload { task_id, worker_peer_id, computed_value }`, mempublikasikannya ke `"task-results"`, dan `coordinator/src/main.rs` (`collect_and_aggregate_results()`) menjumlahkan seluruh hasil ke *unified sum* akhir. |

---

## Workspace Structure

```text
nitro-compute/
├── Cargo.toml                  # Master Cargo Workspace manifest
├── core/
│   ├── Cargo.toml
│   └── src/
│       ├── lib.rs              # Exports & unified tracing logger (init_logging)
│       ├── runtime.rs          # NitroRuntime (Wasmtime + Cranelift + WASI + Fuel Metering)
│       └── network.rs          # NitroBehavior (libp2p Kademlia + Gossipsub + Wire Payloads)
├── coordinator/
│   ├── Cargo.toml
│   └── src/
│       └── main.rs             # CLI Oracle: stream chunking, "task-alloc" dispatch & result aggregation
├── worker/
│   ├── Cargo.toml
│   └── src/
│       └── main.rs             # Edge Worker Daemon: "task-alloc" listener, Wasm execution & "task-results" publisher
└── jobs/
    └── word-count/
        ├── .cargo/config.toml  # Target architecture: wasm32-wasi
        ├── Cargo.toml
        └── src/
            └── lib.rs          # #[no_mangle] extern "C" fn map_line, alloc, dealloc
```

---

## Quick Start

### 1. Build the WebAssembly Job (`jobs/word-count`)
```bash
cargo build --manifest-path jobs/word-count/Cargo.toml --target wasm32-wasi --release
```

### 2. Build the Workspace in Release Mode
```bash
cargo build --release
```

### 3. Run Computer B (Worker Node)
```bash
RUST_LOG=info cargo run --release --bin worker -- --listen /ip4/0.0.0.0/tcp/4001
```

### 4. Run Computer A (Coordinator Node)
```bash
RUST_LOG=info cargo run --release --bin coordinator -- \
  --input input.txt \
  --wasm target/wasm32-wasi/release/word_count.wasm \
  --peer /ip4/127.0.0.1/tcp/4001 \
  --chunk-lines 10 \
  --fuel 10000000
```
