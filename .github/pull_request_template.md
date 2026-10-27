## Summary
Describe the changes introduced by this Pull Request and the motivation behind them.

## Affected Crates
- [ ] `core` (`runtime.rs` / `network.rs`)
- [ ] `coordinator`
- [ ] `worker`
- [ ] `jobs/word-count`
- [ ] Documentation / CI

## Verification Checklist
- [ ] `cargo fmt --all -- --check` passes
- [ ] `cargo clippy --workspace --all-targets -- -D warnings` passes
- [ ] `cargo build --release` compiles cleanly with zero warnings
- [ ] Wasmtime fuel metering (`store.add_fuel`) and sandbox isolation guarantees are preserved
