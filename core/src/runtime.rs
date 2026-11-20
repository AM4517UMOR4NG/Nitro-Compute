use std::collections::HashMap;
use std::fmt;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::RwLock;
use std::time::Instant;

use anyhow::{anyhow, Context};
use sha2::{Digest, Sha256};
use tracing::{debug, error, info};
use wasmtime::{Config, Engine, Linker, Module, OptLevel, ResourceLimiter, Store, Strategy, Trap};
use wasmtime_wasi::preview1::{self, WasiP1Ctx};
use wasmtime_wasi::WasiCtxBuilder;

/// Default maximum linear memory per Wasm sandbox invocation (64 MiB).
pub const DEFAULT_MAX_MEMORY_BYTES: usize = 64 * 1024 * 1024;

/// Default maximum table elements per Wasm sandbox invocation.
pub const DEFAULT_MAX_TABLE_ELEMENTS: usize = 10_000;

/// Default maximum number of compiled Cranelift modules held in the in-memory SHA-256 cache.
pub const DEFAULT_MAX_CACHED_MODULES: usize = 64;

/// Computes the lowercase hexadecimal SHA-256 digest of an arbitrary byte slice.
pub fn sha256_hex(bytes: &[u8]) -> String {
    let digest = Sha256::digest(bytes);
    let mut hex = String::with_capacity(digest.len() * 2);
    for byte in digest {
        use std::fmt::Write as _;
        let _ = write!(&mut hex, "{byte:02x}");
    }
    hex
}

fn sha256_digest(bytes: &[u8]) -> [u8; 32] {
    let digest = Sha256::digest(bytes);
    let mut out = [0u8; 32];
    out.copy_from_slice(&digest);
    out
}

/// Configuration parameters governing `NitroRuntime` sandbox isolation and module caching.
#[derive(Debug, Clone)]
pub struct SandboxConfig {
    pub max_memory_bytes: usize,
    pub max_table_elements: usize,
    pub max_cached_modules: usize,
}

impl Default for SandboxConfig {
    fn default() -> Self {
        Self {
            max_memory_bytes: DEFAULT_MAX_MEMORY_BYTES,
            max_table_elements: DEFAULT_MAX_TABLE_ELEMENTS,
            max_cached_modules: DEFAULT_MAX_CACHED_MODULES,
        }
    }
}

/// Telemetry returned after a successful fuel- and memory-bounded Wasm execution.
#[derive(Debug, Clone)]
pub struct ExecutionMetrics {
    pub computed_value: usize,
    pub fuel_consumed: u64,
    pub remaining_fuel: u64,
    pub peak_memory_bytes: usize,
    pub duration_us: u64,
    pub cache_hit: bool,
    pub wasm_sha256: String,
}

/// Typed security trap variants raised when untrusted guest bytecode violates resource limits.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum SandboxTrapError {
    OutOfFuel {
        fuel_limit: u64,
        stage: &'static str,
    },
    MemoryLimitExceeded {
        limit_bytes: usize,
        stage: &'static str,
    },
    TableLimitExceeded {
        limit_elements: usize,
        stage: &'static str,
    },
}

impl fmt::Display for SandboxTrapError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::OutOfFuel { fuel_limit, stage } => write!(
                f,
                "Security resource exhaustion breach during {stage}: untrusted WASM execution exceeded allocated fuel_limit of {fuel_limit} units (Trap::OutOfFuel)"
            ),
            Self::MemoryLimitExceeded { limit_bytes, stage } => write!(
                f,
                "Security resource exhaustion breach during {stage}: untrusted WASM execution exceeded linear memory limit of {limit_bytes} bytes"
            ),
            Self::TableLimitExceeded {
                limit_elements,
                stage,
            } => write!(
                f,
                "Security resource exhaustion breach during {stage}: untrusted WASM execution exceeded table limit of {limit_elements} elements"
            ),
        }
    }
}

impl std::error::Error for SandboxTrapError {}

/// Extension trait providing `add_fuel` on `wasmtime::Store` backed by Wasmtime's fuel metering API.
trait StoreFuelExt {
    fn add_fuel(&mut self, fuel: u64) -> anyhow::Result<()>;
}

impl<T> StoreFuelExt for Store<T> {
    fn add_fuel(&mut self, fuel: u64) -> anyhow::Result<()> {
        let existing = self.get_fuel().unwrap_or(0);
        self.set_fuel(existing.saturating_add(fuel))
            .context("Failed to allocate Wasmtime execution fuel on Store")
    }
}

struct SandboxLimiter {
    max_memory_bytes: usize,
    max_table_elements: usize,
    peak_memory_bytes: usize,
    memory_limit_exceeded: bool,
    table_limit_exceeded: bool,
}

impl SandboxLimiter {
    fn new(max_memory_bytes: usize, max_table_elements: usize) -> Self {
        Self {
            max_memory_bytes,
            max_table_elements,
            peak_memory_bytes: 0,
            memory_limit_exceeded: false,
            table_limit_exceeded: false,
        }
    }
}

impl ResourceLimiter for SandboxLimiter {
    fn memory_growing(
        &mut self,
        _current: usize,
        desired: usize,
        _maximum: Option<usize>,
    ) -> wasmtime::Result<bool> {
        if desired > self.max_memory_bytes {
            self.memory_limit_exceeded = true;
            return Err(anyhow!(
                "Linear memory growth denied: requested {} bytes > limit {} bytes",
                desired,
                self.max_memory_bytes
            ));
        }
        self.peak_memory_bytes = self.peak_memory_bytes.max(desired);
        Ok(true)
    }

    fn table_growing(
        &mut self,
        _current: usize,
        desired: usize,
        _maximum: Option<usize>,
    ) -> wasmtime::Result<bool> {
        if desired > self.max_table_elements {
            self.table_limit_exceeded = true;
            return Err(anyhow!(
                "Table growth denied: requested {} elements > limit {} elements",
                desired,
                self.max_table_elements
            ));
        }
        Ok(true)
    }
}

struct SandboxState {
    wasi: WasiP1Ctx,
    limiter: SandboxLimiter,
}

struct CachedModule {
    module: Module,
    last_used_seq: u64,
}

/// WebAssembly sandboxed isolation runtime powered by Wasmtime and Cranelift with fuel metering,
/// linear memory resource guards, and SHA-256 content-addressed module caching.
pub struct NitroRuntime {
    engine: Engine,
    config: SandboxConfig,
    module_cache: RwLock<HashMap<[u8; 32], CachedModule>>,
    cache_seq: AtomicU64,
}

impl NitroRuntime {
    /// Configures and initializes a `NitroRuntime` with `Cranelift` set to `OptLevel::Speed`,
    /// deterministic WASM fuel consumption enabled, and default sandbox resource limits.
    pub fn new() -> Self {
        Self::with_config(SandboxConfig::default())
    }

    /// Configures and initializes a `NitroRuntime` with custom `SandboxConfig` limits and cache sizing.
    pub fn with_config(sandbox_config: SandboxConfig) -> Self {
        let mut config = Config::new();
        config.strategy(Strategy::Cranelift);
        config.cranelift_opt_level(OptLevel::Speed);
        config.consume_fuel(true);

        let engine = Engine::new(&config).expect("Failed to initialize Wasmtime Engine");
        info!(
            max_memory_bytes = sandbox_config.max_memory_bytes,
            max_table_elements = sandbox_config.max_table_elements,
            max_cached_modules = sandbox_config.max_cached_modules,
            "Initialized NitroRuntime with Cranelift (OptLevel::Speed), fuel metering, and memory limiter"
        );
        Self {
            engine,
            config: sandbox_config,
            module_cache: RwLock::new(HashMap::new()),
            cache_seq: AtomicU64::new(0),
        }
    }

    /// Returns a reference to the underlying `wasmtime::Engine`.
    pub fn engine(&self) -> &Engine {
        &self.engine
    }

    /// Returns the number of compiled modules currently cached in memory.
    pub fn cached_module_count(&self) -> usize {
        self.module_cache.read().map(|c| c.len()).unwrap_or(0)
    }

    /// Returns the `SandboxConfig` active on this runtime.
    pub fn config(&self) -> &SandboxConfig {
        &self.config
    }

    /// Retrieves a compiled `Module` from the SHA-256 cache or compiles and caches it on miss.
    fn get_or_compile_module(&self, wasm_bytes: &[u8]) -> anyhow::Result<(Module, bool, String)> {
        let key = sha256_digest(wasm_bytes);
        let hex = sha256_hex(wasm_bytes);

        if self.config.max_cached_modules > 0 {
            if let Ok(mut cache) = self.module_cache.write() {
                if let Some(entry) = cache.get_mut(&key) {
                    entry.last_used_seq = self.cache_seq.fetch_add(1, Ordering::Relaxed) + 1;
                    return Ok((entry.module.clone(), true, hex));
                }
            }
        }

        let module = Module::new(&self.engine, wasm_bytes)
            .context("Failed to compile WebAssembly module")?;

        if self.config.max_cached_modules > 0 {
            if let Ok(mut cache) = self.module_cache.write() {
                if cache.len() >= self.config.max_cached_modules && !cache.contains_key(&key) {
                    if let Some(evict_key) = cache
                        .iter()
                        .min_by_key(|(_, entry)| entry.last_used_seq)
                        .map(|(k, _)| *k)
                    {
                        cache.remove(&evict_key);
                    }
                }
                let seq = self.cache_seq.fetch_add(1, Ordering::Relaxed) + 1;
                cache.insert(
                    key,
                    CachedModule {
                        module: module.clone(),
                        last_used_seq: seq,
                    },
                );
            }
        }

        Ok((module, false, hex))
    }

    /// Inspects a Wasmtime execution error to detect fuel or memory/table limit traps and surface
    /// an explicit `SandboxTrapError` security resource exhaustion error.
    fn classify_execution_error(
        err: anyhow::Error,
        store: &Store<SandboxState>,
        fuel_limit: u64,
        stage: &'static str,
    ) -> anyhow::Error {
        if store.data().limiter.memory_limit_exceeded {
            let trap = SandboxTrapError::MemoryLimitExceeded {
                limit_bytes: store.data().limiter.max_memory_bytes,
                stage,
            };
            error!(
                limit_bytes = store.data().limiter.max_memory_bytes,
                stage, "{trap}"
            );
            return anyhow::Error::new(trap);
        }

        if store.data().limiter.table_limit_exceeded {
            let trap = SandboxTrapError::TableLimitExceeded {
                limit_elements: store.data().limiter.max_table_elements,
                stage,
            };
            error!(
                limit_elements = store.data().limiter.max_table_elements,
                stage, "{trap}"
            );
            return anyhow::Error::new(trap);
        }

        let is_out_of_fuel = matches!(err.downcast_ref::<Trap>(), Some(Trap::OutOfFuel))
            || matches!(store.get_fuel(), Ok(0));

        if is_out_of_fuel {
            let trap = SandboxTrapError::OutOfFuel { fuel_limit, stage };
            error!(fuel_limit, stage, "{trap}");
            anyhow::Error::new(trap)
        } else {
            err.context(format!("Execution of '{stage}' trapped in Wasm sandbox"))
        }
    }

    /// Instantiates a Wasm module in a WASI-enabled sandbox constrained by `fuel_limit`,
    /// injects `input_data` into linear memory, invokes `"map_line"`, and returns the computed count.
    pub fn execute_map(
        &self,
        wasm_bytes: &[u8],
        input_data: &str,
        fuel_limit: u64,
    ) -> anyhow::Result<usize> {
        let metrics = self.execute_with_options(
            wasm_bytes,
            input_data,
            fuel_limit,
            self.config.max_memory_bytes,
            "map_line",
        )?;
        Ok(metrics.computed_value)
    }

    /// Executes an exported Wasm MapReduce function (`entry_fn`, e.g. `"map_line"`) inside an
    /// isolated WASI sandbox enforcing both instruction fuel metering and linear memory bounds,
    /// returning full execution telemetry.
    pub fn execute_with_options(
        &self,
        wasm_bytes: &[u8],
        input_data: &str,
        fuel_limit: u64,
        max_memory_bytes: usize,
        entry_fn: &str,
    ) -> anyhow::Result<ExecutionMetrics> {
        let started_at = Instant::now();

        debug!(
            wasm_size = wasm_bytes.len(),
            input_len = input_data.len(),
            fuel_limit,
            max_memory_bytes,
            entry_fn,
            "Starting fuel- and memory-metered Wasm sandbox execution"
        );

        if fuel_limit == 0 {
            return Err(anyhow::Error::new(SandboxTrapError::OutOfFuel {
                fuel_limit: 0,
                stage: "precheck",
            }));
        }

        let (module, cache_hit, wasm_sha256) = self.get_or_compile_module(wasm_bytes)?;

        let wasi_ctx: WasiP1Ctx = WasiCtxBuilder::new()
            .inherit_stdio()
            .inherit_args()
            .inherit_env()
            .build_p1();

        let effective_memory_limit = max_memory_bytes
            .min(self.config.max_memory_bytes)
            .max(65_536);
        let state = SandboxState {
            wasi: wasi_ctx,
            limiter: SandboxLimiter::new(effective_memory_limit, self.config.max_table_elements),
        };

        let mut store = Store::new(&self.engine, state);
        store.limiter(|s| &mut s.limiter);
        store.add_fuel(fuel_limit)?;

        let mut linker: Linker<SandboxState> = Linker::new(&self.engine);
        preview1::add_to_linker_sync(&mut linker, |state| &mut state.wasi)
            .context("Failed to link WASI preview1 imports")?;

        let instance = linker.instantiate(&mut store, &module).map_err(|err| {
            Self::classify_execution_error(err, &store, fuel_limit, "instantiate")
        })?;

        let memory = instance
            .get_memory(&mut store, "memory")
            .ok_or_else(|| anyhow!("WebAssembly module does not export linear 'memory'"))?;

        let input_bytes = input_data.as_bytes();
        let input_len = input_bytes.len();
        let wasm_len = i32::try_from(input_len)
            .context("Input data length exceeds 32-bit Wasm address space")?;

        // Allocate linear memory inside the Wasm sandbox via guest allocator if exported,
        // or by growing the module's linear memory pages.
        let (ptr_offset, used_guest_alloc) =
            if let Ok(alloc_fn) = instance.get_typed_func::<i32, i32>(&mut store, "alloc") {
                let ptr = alloc_fn.call(&mut store, wasm_len).map_err(|err| {
                    Self::classify_execution_error(err, &store, fuel_limit, "alloc")
                })?;
                if ptr <= 0 && wasm_len > 0 {
                    return Err(anyhow!("Guest 'alloc' returned a null or invalid pointer"));
                }
                (ptr, true)
            } else {
                const WASM_PAGE_SIZE: usize = 65_536;
                let pages_needed = input_len.max(1).div_ceil(WASM_PAGE_SIZE);
                let prev_pages = memory
                    .grow(&mut store, pages_needed as u64)
                    .map_err(|err| {
                        Self::classify_execution_error(err, &store, fuel_limit, "memory_grow")
                    })?;
                let base_offset = (prev_pages as usize) * WASM_PAGE_SIZE;
                let safe_offset = if base_offset == 0 { 1024 } else { base_offset };
                if safe_offset + input_len > memory.data_size(&store) {
                    let extra_pages = (safe_offset + input_len - memory.data_size(&store))
                        .div_ceil(WASM_PAGE_SIZE);
                    memory.grow(&mut store, extra_pages as u64).map_err(|err| {
                        Self::classify_execution_error(err, &store, fuel_limit, "memory_grow")
                    })?;
                }
                let ptr = i32::try_from(safe_offset)
                    .context("Allocated linear memory offset overflows i32")?;
                (ptr, false)
            };

        if input_len > 0 {
            memory
                .write(&mut store, ptr_offset as usize, input_bytes)
                .context("Failed to write input_data bytes into Wasm linear memory")?;
        }

        let target_fn = instance
            .get_typed_func::<(i32, i32), i32>(&mut store, entry_fn)
            .with_context(|| {
                format!("Failed to locate exported '{entry_fn}' function in Wasm module")
            })?;

        let result = target_fn
            .call(&mut store, (ptr_offset, wasm_len))
            .map_err(|err| Self::classify_execution_error(err, &store, fuel_limit, "map_line"))?;

        if used_guest_alloc {
            if let Ok(dealloc_fn) = instance.get_typed_func::<(i32, i32), ()>(&mut store, "dealloc")
            {
                let _ = dealloc_fn.call(&mut store, (ptr_offset, wasm_len));
            }
        }

        let remaining_fuel = store.get_fuel().unwrap_or(0);
        let consumed_fuel = fuel_limit.saturating_sub(remaining_fuel);
        let peak_memory_bytes = store
            .data()
            .limiter
            .peak_memory_bytes
            .max(memory.data_size(&store));
        let count = usize::try_from(result)
            .with_context(|| format!("Wasm '{entry_fn}' returned a negative count"))?;
        let duration_us = u64::try_from(started_at.elapsed().as_micros()).unwrap_or(u64::MAX);

        debug!(
            count,
            consumed_fuel,
            remaining_fuel,
            peak_memory_bytes,
            duration_us,
            cache_hit,
            wasm_sha256 = %wasm_sha256,
            "Completed fuel- and memory-metered Wasm sandbox execution"
        );

        Ok(ExecutionMetrics {
            computed_value: count,
            fuel_consumed: consumed_fuel,
            remaining_fuel,
            peak_memory_bytes,
            duration_us,
            cache_hit,
            wasm_sha256,
        })
    }
}

impl Default for NitroRuntime {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

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

    const MEMORY_BOMB_WAT: &str = r#"
        (module
            (memory (export "memory") 1)
            (func (export "map_line") (param i32 i32) (result i32)
                (drop (memory.grow (i32.const 512)))
                (i32.const 0)
            )
        )
    "#;

    #[test]
    fn executes_word_count_and_hits_module_cache() {
        let wasm = wat::parse_str(WORD_COUNT_WAT).expect("valid WAT");
        let runtime = NitroRuntime::new();

        let first = runtime
            .execute_with_options(
                &wasm,
                "nitro compute edge   wasm",
                1_000_000,
                DEFAULT_MAX_MEMORY_BYTES,
                "map_line",
            )
            .expect("first run succeeds");
        assert_eq!(first.computed_value, 4);
        assert!(!first.cache_hit);
        assert!(first.fuel_consumed > 0);
        assert_eq!(runtime.cached_module_count(), 1);

        let second = runtime
            .execute_with_options(
                &wasm,
                "one two three",
                1_000_000,
                DEFAULT_MAX_MEMORY_BYTES,
                "map_line",
            )
            .expect("second run succeeds");
        assert_eq!(second.computed_value, 3);
        assert!(second.cache_hit);
        assert_eq!(first.wasm_sha256, second.wasm_sha256);
    }

    #[test]
    fn traps_infinite_loop_with_out_of_fuel() {
        let wasm = wat::parse_str(INFINITE_LOOP_WAT).expect("valid WAT");
        let runtime = NitroRuntime::new();

        let err = runtime
            .execute_map(&wasm, "hello", 5_000)
            .expect_err("infinite loop must trap out of fuel");

        assert!(matches!(
            err.downcast_ref::<SandboxTrapError>(),
            Some(SandboxTrapError::OutOfFuel { .. })
        ));
    }

    #[test]
    fn blocks_memory_bomb_with_memory_limit_exceeded() {
        let wasm = wat::parse_str(MEMORY_BOMB_WAT).expect("valid WAT");
        let runtime = NitroRuntime::with_config(SandboxConfig {
            max_memory_bytes: 2 * 1024 * 1024, // 2 MiB ceiling
            ..SandboxConfig::default()
        });

        let err = runtime
            .execute_with_options(&wasm, "hello", 1_000_000, 2 * 1024 * 1024, "map_line")
            .expect_err("memory bomb must be blocked by ResourceLimiter");

        assert!(matches!(
            err.downcast_ref::<SandboxTrapError>(),
            Some(SandboxTrapError::MemoryLimitExceeded { .. })
        ));
    }
}
