//! Core library module for `nitro-compute`.

pub mod network;
pub mod runtime;

pub use network::{
    create_swarm, publish_result_payload, publish_task_payload, run_event_loop,
    run_event_loop_with_handler, NitroBehavior, NitroBehaviorEvent, ResultPayload, TaskPayload,
    DEFAULT_FUEL_LIMIT, TASK_ALLOC_TOPIC, TASK_RESULTS_TOPIC,
};
pub use runtime::NitroRuntime;

/// Initializes unified system-wide structured logging to terminal stdout via `tracing_subscriber`.
pub fn init_logging() {
    let env_filter = tracing_subscriber::EnvFilter::try_from_default_env()
        .unwrap_or_else(|_| tracing_subscriber::EnvFilter::new("info"));

    let _ = tracing_subscriber::fmt()
        .with_env_filter(env_filter)
        .with_writer(std::io::stdout)
        .with_target(true)
        .try_init();
}
