use anyhow::{anyhow, Context};
use wasmtime::{Config, Engine, Linker, Module, OptLevel, Store, Strategy};
use wasmtime_wasi::preview1::{self, WasiP1Ctx};
use wasmtime_wasi::WasiCtxBuilder;

/// WebAssembly sandboxed isolation runtime powered by Wasmtime and Cranelift.
pub struct NitroRuntime {
    engine: Engine,
}

impl NitroRuntime {
    /// Configures and initializes a `NitroRuntime` with `Cranelift` set to `OptLevel::Speed`.
    pub fn new() -> Self {
        let mut config = Config::new();
        config.strategy(Strategy::Cranelift);
        config.cranelift_opt_level(OptLevel::Speed);

        let engine = Engine::new(&config).expect("Failed to initialize Wasmtime Engine");
        Self { engine }
    }

    /// Returns a reference to the underlying `wasmtime::Engine`.
    pub fn engine(&self) -> &Engine {
        &self.engine
    }

    /// Instantiates a Wasm module in a WASI-enabled sandbox, injects `input_data` into its
    /// linear memory, invokes `"map_line"`, and returns the resulting count.
    pub fn execute_map(&self, wasm_bytes: &[u8], input_data: &str) -> anyhow::Result<usize> {
        let wasi_ctx: WasiP1Ctx = WasiCtxBuilder::new()
            .inherit_stdio()
            .inherit_args()
            .inherit_env()
            .build_p1();

        let mut store = Store::new(&self.engine, wasi_ctx);

        let mut linker: Linker<WasiP1Ctx> = Linker::new(&self.engine);
        preview1::add_to_linker_sync(&mut linker, |ctx| ctx)
            .context("Failed to link WASI preview1 imports")?;

        let module = Module::new(&self.engine, wasm_bytes)
            .context("Failed to compile WebAssembly module")?;

        let instance = linker
            .instantiate(&mut store, &module)
            .context("Failed to instantiate WebAssembly module")?;

        let memory = instance
            .get_memory(&mut store, "memory")
            .ok_or_else(|| anyhow!("WebAssembly module does not export linear 'memory'"))?;

        let input_bytes = input_data.as_bytes();
        let input_len = input_bytes.len();
        let wasm_len = i32::try_from(input_len)
            .context("Input data length exceeds 32-bit Wasm address space")?;

        let (ptr_offset, used_guest_alloc) =
            if let Ok(alloc_fn) = instance.get_typed_func::<i32, i32>(&mut store, "alloc") {
                let ptr = alloc_fn
                    .call(&mut store, wasm_len)
                    .context("Failed to allocate memory via guest 'alloc' function")?;
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
            .context("Execution of 'map_line' trapped in Wasm sandbox")?;

        if used_guest_alloc {
            if let Ok(dealloc_fn) = instance.get_typed_func::<(i32, i32), ()>(&mut store, "dealloc")
            {
                let _ = dealloc_fn.call(&mut store, (ptr_offset, wasm_len));
            }
        }

        usize::try_from(result).context("Wasm 'map_line' returned a negative count")
    }
}

impl Default for NitroRuntime {
    fn default() -> Self {
        Self::new()
    }
}
