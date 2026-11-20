//! Empirical latency, throughput, Zstd compression, and sandbox trap benchmark harness.
//!
//! Run with:
//! `cargo run --manifest-path core/Cargo.toml --example bench_sandbox --release`

use std::sync::Arc;
use std::time::Instant;

use nitro_core::network::TaskPayload;
use nitro_core::runtime::{NitroRuntime, SandboxConfig, DEFAULT_MAX_MEMORY_BYTES};

const WORD_COUNT_WAT: &str = r#"
    (module
        (memory (export "memory") 1)
        (func (export "map_line") (param $ptr i32) (param $len i32) (result i32)
            (local $i i32)
            (local $in_word i32)
            (local $count i32)
            (local $ch i32)
            (local.set $i (i32.const 0))
            (local.set $in_word (i32.const 0))
            (local.set $count (i32.const 0))
            (block $break
                (loop $loop
                    (br_if $break (i32.ge_u (local.get $i) (local.get $len)))
                    (local.set $ch (i32.load8_u (i32.add (local.get $ptr) (local.get $i))))
                    (if (i32.or
                            (i32.eq (local.get $ch) (i32.const 32))
                            (i32.or (i32.eq (local.get $ch) (i32.const 10)) (i32.eq (local.get $ch) (i32.const 9))))
                        (then
                            (local.set $in_word (i32.const 0))
                        )
                        (else
                            (if (i32.eqz (local.get $in_word))
                                (then
                                    (local.set $in_word (i32.const 1))
                                    (local.set $count (i32.add (local.get $count) (i32.const 1)))
                                )
                            )
                        )
                    )
                    (local.set $i (i32.add (local.get $i) (i32.const 1)))
                    (br $loop)
                )
            )
            (local.get $count)
        )
    )
"#;

const INFINITE_LOOP_WAT: &str = r#"
    (module
        (memory (export "memory") 1)
        (func (export "map_line") (param i32 i32) (result i32)
            (loop $inf
                (br $inf)
            )
            (i32.const 0)
        )
    )
"#;

fn percentile(sorted: &[u64], pct: f64) -> u64 {
    if sorted.is_empty() {
        return 0;
    }
    let idx = (((sorted.len() - 1) as f64) * pct).round() as usize;
    sorted[idx.min(sorted.len() - 1)]
}

fn main() -> anyhow::Result<()> {
    let valid_wasm = wat::parse_str(WORD_COUNT_WAT)?;
    let evil_wasm = wat::parse_str(INFINITE_LOOP_WAT)?;
    let sample_chunk =
        "nitro compute decentralized serverless mapreduce engine at the edge ".repeat(16);

    // 1. Cold-start (uncached JIT compile + instantiate + execute)
    let uncached_rt = NitroRuntime::with_config(SandboxConfig {
        max_cached_modules: 0,
        ..SandboxConfig::default()
    });
    let mut cold_us = Vec::with_capacity(20);
    for _ in 0..20 {
        let t0 = Instant::now();
        let res = uncached_rt.execute_map(&valid_wasm, &sample_chunk, 1_000_000)?;
        assert_eq!(res, 144);
        cold_us.push(t0.elapsed().as_micros() as u64);
    }
    cold_us.sort_unstable();

    // 2. Warm SHA-256 cached sandbox execution
    let cached_rt = Arc::new(NitroRuntime::new());
    let _ = cached_rt.execute_map(&valid_wasm, &sample_chunk, 1_000_000)?;
    let iterations = 500;
    let mut warm_us = Vec::with_capacity(iterations);
    let warm_t0 = Instant::now();
    for _ in 0..iterations {
        let t0 = Instant::now();
        let res = cached_rt.execute_with_options(
            &valid_wasm,
            &sample_chunk,
            1_000_000,
            DEFAULT_MAX_MEMORY_BYTES,
            "map_line",
        )?;
        assert!(res.cache_hit);
        assert_eq!(res.computed_value, 144);
        warm_us.push(t0.elapsed().as_micros() as u64);
    }
    let warm_total = warm_t0.elapsed();
    warm_us.sort_unstable();

    // 3. Multi-threaded parallel sandbox throughput
    let threads = std::thread::available_parallelism()
        .map(|n| n.get())
        .unwrap_or(2)
        .max(2);
    let per_thread = 250;
    let mt_t0 = Instant::now();
    std::thread::scope(|s| {
        for _ in 0..threads {
            let rt = Arc::clone(&cached_rt);
            let wasm = &valid_wasm;
            let chunk = &sample_chunk;
            s.spawn(move || {
                for _ in 0..per_thread {
                    let count = rt
                        .execute_map(wasm, chunk, 1_000_000)
                        .expect("parallel map execution");
                    assert_eq!(count, 144);
                }
            });
        }
    });
    let mt_elapsed = mt_t0.elapsed();
    let mt_tasks = threads * per_thread;

    // 4. OutOfFuel security trap latency
    let _ = cached_rt.execute_map(&evil_wasm, "x", 5_000);
    let mut trap_us = Vec::with_capacity(50);
    for _ in 0..50 {
        let t0 = Instant::now();
        let err = cached_rt
            .execute_map(&evil_wasm, "x", 5_000)
            .expect_err("infinite loop must trap");
        assert!(err.to_string().contains("OutOfFuel"));
        trap_us.push(t0.elapsed().as_micros() as u64);
    }
    trap_us.sort_unstable();

    // 5. Zstd wire compression ratio
    let synthetic_wasm = vec![0x61u8; 65_536];
    let payload = TaskPayload::new("bench-0", synthetic_wasm, &sample_chunk, 1_000_000);
    let raw_json_len = serde_json::to_vec(&payload)?.len();
    let compressed_len = payload.to_bytes()?.len();
    let savings_pct = (1.0 - (compressed_len as f64 / raw_json_len as f64)) * 100.0;

    println!("=== NITRO COMPUTE SANDBOX & WIRE BENCHMARK ===");
    println!(
        "Cold-Start (No Cache, n=20): p50={} us | p95={} us | p99={} us",
        percentile(&cold_us, 0.50),
        percentile(&cold_us, 0.95),
        percentile(&cold_us, 0.99)
    );
    println!(
        "Warm-Cache (SHA-256, n={iterations}): p50={} us | p95={} us | p99={} us | {:.1} tasks/sec",
        percentile(&warm_us, 0.50),
        percentile(&warm_us, 0.95),
        percentile(&warm_us, 0.99),
        iterations as f64 / warm_total.as_secs_f64()
    );
    println!(
        "Parallel Throughput ({threads} threads x {per_thread}): {mt_tasks} tasks in {:.3} s ({:.1} tasks/sec)",
        mt_elapsed.as_secs_f64(),
        mt_tasks as f64 / mt_elapsed.as_secs_f64()
    );
    println!(
        "Security Trap (OutOfFuel 5k fuel, n=50): p50={} us | p95={} us",
        percentile(&trap_us, 0.50),
        percentile(&trap_us, 0.95)
    );
    println!(
        "Zstd Wire Framing (64 KiB Wasm): raw_json={} B -> zstd={} B ({:.2}% reduction)",
        raw_json_len, compressed_len, savings_pct
    );

    Ok(())
}
