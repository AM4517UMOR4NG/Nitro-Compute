use wasmtime::{Config, Engine, OptLevel, Strategy};

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
}

impl Default for NitroRuntime {
    fn default() -> Self {
        Self::new()
    }
}
