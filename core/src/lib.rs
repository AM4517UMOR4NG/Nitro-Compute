//! Core library module for `nitro-compute`.

pub mod network;
pub mod runtime;

pub use network::{
    create_swarm, create_swarm_with_mdns, decode_wire_frame, encode_wire_frame,
    process_swarm_maintenance, publish_result_payload, publish_task_payload,
    publish_worker_telemetry, register_peer_address, run_event_loop, run_event_loop_with_handler,
    NitroBehavior, NitroBehaviorEvent, ResultPayload, TaskOutcome, TaskPayload, WorkerTelemetry,
    DEFAULT_FUEL_LIMIT, MAX_GOSSIPSUB_TRANSMIT_BYTES, TASK_ALLOC_TOPIC, TASK_RESULTS_TOPIC,
    WORKER_TELEMETRY_TOPIC,
};
pub use runtime::{
    sha256_hex, ExecutionMetrics, NitroRuntime, SandboxConfig, SandboxTrapError,
    DEFAULT_MAX_MEMORY_BYTES,
};

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
