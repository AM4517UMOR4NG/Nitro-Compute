use anyhow::{anyhow, Context};
use tracing::{debug, error, info};
use wasmtime::{Config, Engine, Linker, Module, OptLevel, Store, Strategy, Trap};
use wasmtime_wasi::preview1::{self, WasiP1Ctx};
use wasmtime_wasi::WasiCtxBuilder;

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

/// WebAssembly sandboxed isolation runtime powered by Wasmtime and Cranelift with fuel metering.
pub struct NitroRuntime {
    engine: Engine,
}

impl NitroRuntime {
    /// Configures and initializes a `NitroRuntime` with `Cranelift` set to `OptLevel::Speed`
    /// and deterministic WASM fuel consumption enabled (`config.consume_fuel(true)`).
    pub fn new() -> Self {
        let mut config = Config::new();
        config.strategy(Strategy::Cranelift);
        config.cranelift_opt_level(OptLevel::Speed);
        config.consume_fuel(true);

        let engine = Engine::new(&config).expect("Failed to initialize Wasmtime Engine");
        info!("Initialized NitroRuntime with Cranelift (OptLevel::Speed) and WASM fuel metering enabled");
        Self { engine }
    }

    /// Returns a reference to the underlying `wasmtime::Engine`.
    pub fn engine(&self) -> &Engine {
        &self.engine
    }

    /// Inspects a Wasmtime execution error to detect out-of-fuel traps and surface an explicit
    /// security resource exhaustion error.
    fn classify_execution_error(
        err: anyhow::Error,
        store: &Store<WasiP1Ctx>,
        fuel_limit: u64,
        stage: &'static str,
    ) -> anyhow::Error {
        let is_out_of_fuel = matches!(err.downcast_ref::<Trap>(), Some(Trap::OutOfFuel))
            || matches!(store.get_fuel(), Ok(0));

        if is_out_of_fuel {
            error!(
                fuel_limit,
                stage,
                "Security resource exhaustion breach: untrusted WASM bytecode exceeded fuel limit"
            );
            anyhow!(
                "Security resource exhaustion breach during {stage}: untrusted WASM execution exceeded allocated fuel_limit of {fuel_limit} units (Trap::OutOfFuel)"
            )
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
        debug!(
            wasm_size = wasm_bytes.len(),
            input_len = input_data.len(),
            fuel_limit,
            "Starting fuel-metered Wasm sandbox execution for 'map_line'"
        );

        let wasi_ctx: WasiP1Ctx = WasiCtxBuilder::new()
            .inherit_stdio()
            .inherit_args()
            .inherit_env()
            .build_p1();

        let mut store = Store::new(&self.engine, wasi_ctx);
        store.add_fuel(fuel_limit)?;

        if fuel_limit == 0 {
            return Err(anyhow!(
                "Security resource exhaustion breach: allocated fuel_limit is 0 units (Trap::OutOfFuel)"
            ));
        }

        let mut linker: Linker<WasiP1Ctx> = Linker::new(&self.engine);
        preview1::add_to_linker_sync(&mut linker, |ctx| ctx)
            .context("Failed to link WASI preview1 imports")?;

        let module = Module::new(&self.engine, wasm_bytes)
            .context("Failed to compile WebAssembly module")?;

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
                let pages_needed = (input_len.max(1) + WASM_PAGE_SIZE - 1) / WASM_PAGE_SIZE;
                let prev_pages = memory
                    .grow(&mut store, pages_needed as u64)
                    .context("Failed to grow Wasm linear memory for input injection")?;
                let base_offset = (prev_pages as usize) * WASM_PAGE_SIZE;
                let safe_offset = if base_offset == 0 { 1024 } else { base_offset };
                if safe_offset + input_len > memory.data_size(&store) {
                    let extra_pages =
                        ((safe_offset + input_len - memory.data_size(&store)) + WASM_PAGE_SIZE - 1)
                            / WASM_PAGE_SIZE;
                    memory
                        .grow(&mut store, extra_pages as u64)
                        .context("Failed to grow Wasm linear memory for safe offset")?;
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

        let map_line = instance
            .get_typed_func::<(i32, i32), i32>(&mut store, "map_line")
            .context("Failed to locate exported 'map_line' function in Wasm module")?;

        let result = map_line
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
        let count = usize::try_from(result).context("Wasm 'map_line' returned a negative count")?;

        debug!(
            count,
            consumed_fuel,
            remaining_fuel,
            "Completed fuel-metered Wasm sandbox execution for 'map_line'"
        );
        Ok(count)
    }
}

impl Default for NitroRuntime {
    fn default() -> Self {
        Self::new()
    }
}
