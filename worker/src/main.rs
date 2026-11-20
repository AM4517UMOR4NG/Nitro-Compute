use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};
use std::sync::{Arc, RwLock};
use std::time::{Duration, Instant};

use anyhow::{anyhow, Context};
use libp2p::futures::StreamExt;
use libp2p::{gossipsub, Multiaddr};
use nitro_core::network::{
    create_swarm_with_mdns, process_swarm_maintenance, publish_result_payload,
    publish_worker_telemetry, ResultPayload, TaskOutcome, TaskPayload, WorkerTelemetry,
    TASK_ALLOC_TOPIC, TASK_RESULTS_TOPIC, WORKER_TELEMETRY_TOPIC,
};
use nitro_core::runtime::{
    sha256_hex, NitroRuntime, SandboxConfig, SandboxTrapError, DEFAULT_MAX_MEMORY_BYTES,
};
use tokio::sync::{mpsc, Semaphore};
use tracing::{error, info, warn};

/// CLI configuration for the edge worker daemon.
struct WorkerConfig {
    listen_addr: Multiaddr,
    bootstrap_peers: Vec<Multiaddr>,
    max_concurrency: usize,
    max_memory_bytes: usize,
    telemetry_interval_secs: u64,
    enable_mdns: bool,
}

impl WorkerConfig {
    fn from_args() -> anyhow::Result<Self> {
        let mut listen_addr: Multiaddr = "/ip4/0.0.0.0/tcp/0"
            .parse()
            .context("Invalid default listen Multiaddr")?;
        let mut bootstrap_peers: Vec<Multiaddr> = Vec::new();
        let default_parallelism = std::thread::available_parallelism()
            .map(|n| n.get())
            .unwrap_or(2)
            .max(1);
        let mut max_concurrency = default_parallelism;
        let mut max_memory_bytes = DEFAULT_MAX_MEMORY_BYTES;
        let mut telemetry_interval_secs = 10u64;
        let mut enable_mdns = true;

        let mut args = std::env::args().skip(1);
        while let Some(arg) = args.next() {
            match arg.as_str() {
                "--listen" | "-l" => {
                    if let Some(addr_str) = args.next() {
                        listen_addr = addr_str
                            .parse()
                            .with_context(|| format!("Invalid --listen multiaddr: {addr_str}"))?;
                    }
                }
                "--dial" | "--peer" | "-p" => {
                    if let Some(peer_str) = args.next() {
                        let addr: Multiaddr = peer_str
                            .parse()
                            .with_context(|| format!("Invalid peer multiaddr: {peer_str}"))?;
                        bootstrap_peers.push(addr);
                    }
                }
                "--max-concurrency" | "-j" => {
                    if let Some(val) = args.next() {
                        max_concurrency = val
                            .parse::<usize>()
                            .context("Invalid --max-concurrency value")?
                            .max(1);
                    }
                }
                "--max-memory-mb" | "-m" => {
                    if let Some(val) = args.next() {
                        let mb = val
                            .parse::<usize>()
                            .context("Invalid --max-memory-mb value")?
                            .max(1);
                        max_memory_bytes = mb.saturating_mul(1024 * 1024);
                    }
                }
                "--telemetry-interval" => {
                    if let Some(val) = args.next() {
                        telemetry_interval_secs = val
                            .parse::<u64>()
                            .context("Invalid --telemetry-interval value")?
                            .max(1);
                    }
                }
                "--no-mdns" => {
                    enable_mdns = false;
                }
                other => {
                    if let Ok(addr) = other.parse::<Multiaddr>() {
                        bootstrap_peers.push(addr);
                    }
                }
            }
        }

        Ok(Self {
            listen_addr,
            bootstrap_peers,
            max_concurrency,
            max_memory_bytes,
            telemetry_interval_secs,
            enable_mdns,
        })
    }
}

/// RAII guard tracking active worker tasks across blocking execution and panics.
struct ActiveTaskGuard {
    metrics: Arc<WorkerMetrics>,
}

impl ActiveTaskGuard {
    fn new(metrics: Arc<WorkerMetrics>) -> Self {
        metrics.active_tasks.fetch_add(1, Ordering::Relaxed);
        Self { metrics }
    }
}

impl Drop for ActiveTaskGuard {
    fn drop(&mut self) {
        self.metrics.active_tasks.fetch_sub(1, Ordering::Relaxed);
    }
}

/// Shared worker execution metrics and content-addressed Wasm bytecode cache.
struct WorkerMetrics {
    active_tasks: AtomicUsize,
    completed_tasks: AtomicU64,
    trapped_tasks: AtomicU64,
    artifact_cache: RwLock<HashMap<String, Vec<u8>>>,
}

impl WorkerMetrics {
    fn new() -> Self {
        Self {
            active_tasks: AtomicUsize::new(0),
            completed_tasks: AtomicU64::new(0),
            trapped_tasks: AtomicU64::new(0),
            artifact_cache: RwLock::new(HashMap::new()),
        }
    }

    fn resolve_or_store_wasm(&self, task: &TaskPayload) -> anyhow::Result<Vec<u8>> {
        task.verify_wasm_integrity()?;

        if !task.wasm_binary.is_empty() {
            let key = if task.wasm_sha256.is_empty() {
                sha256_hex(&task.wasm_binary)
            } else {
                task.wasm_sha256.clone()
            };
            if let Ok(mut cache) = self.artifact_cache.write() {
                cache.entry(key).or_insert_with(|| task.wasm_binary.clone());
            }
            return Ok(task.wasm_binary.clone());
        }

        if !task.wasm_sha256.is_empty() {
            if let Ok(cache) = self.artifact_cache.read() {
                if let Some(bytes) = cache.get(&task.wasm_sha256) {
                    return Ok(bytes.clone());
                }
            }
            return Err(anyhow!(
                "Wasm artifact '{}' not found in worker cache for cache-only task '{}'",
                task.wasm_sha256,
                task.task_id
            ));
        }

        Err(anyhow!(
            "Task '{}' contains neither wasm_binary nor wasm_sha256",
            task.task_id
        ))
    }
}

fn execute_task_in_sandbox(
    runtime: &NitroRuntime,
    metrics: &WorkerMetrics,
    worker_peer_id: &str,
    task: TaskPayload,
) -> ResultPayload {
    let wasm_bytes = match metrics.resolve_or_store_wasm(&task) {
        Ok(bytes) => bytes,
        Err(err) => {
            return ResultPayload::failure(
                task.task_id,
                task.job_id,
                worker_peer_id.to_string(),
                task.attempt,
                TaskOutcome::ExecutionError {
                    reason: format!("{err:#}"),
                },
            );
        }
    };

    match runtime.execute_with_options(
        &wasm_bytes,
        &task.data_chunk,
        task.fuel,
        task.max_memory_bytes,
        &task.entry_fn,
    ) {
        Ok(exec) => {
            metrics.completed_tasks.fetch_add(1, Ordering::Relaxed);
            ResultPayload::from_metrics(
                task.task_id,
                task.job_id,
                worker_peer_id.to_string(),
                task.attempt,
                &exec,
            )
        }
        Err(err) => {
            let status = match err.downcast_ref::<SandboxTrapError>() {
                Some(SandboxTrapError::OutOfFuel { .. }) => {
                    metrics.trapped_tasks.fetch_add(1, Ordering::Relaxed);
                    TaskOutcome::SecurityTrap {
                        kind: "OutOfFuel".to_string(),
                        reason: err.to_string(),
                    }
                }
                Some(SandboxTrapError::MemoryLimitExceeded { .. }) => {
                    metrics.trapped_tasks.fetch_add(1, Ordering::Relaxed);
                    TaskOutcome::SecurityTrap {
                        kind: "MemoryLimitExceeded".to_string(),
                        reason: err.to_string(),
                    }
                }
                Some(SandboxTrapError::TableLimitExceeded { .. }) => {
                    metrics.trapped_tasks.fetch_add(1, Ordering::Relaxed);
                    TaskOutcome::SecurityTrap {
                        kind: "TableLimitExceeded".to_string(),
                        reason: err.to_string(),
                    }
                }
                None => TaskOutcome::ExecutionError {
                    reason: format!("{err:#}"),
                },
            };

            ResultPayload::failure(
                task.task_id,
                task.job_id,
                worker_peer_id.to_string(),
                task.attempt,
                status,
            )
        }
    }
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    nitro_core::init_logging();

    let config = WorkerConfig::from_args()?;
    info!(
        max_concurrency = config.max_concurrency,
        max_memory_mb = config.max_memory_bytes / (1024 * 1024),
        enable_mdns = config.enable_mdns,
        "Starting nitro-compute worker daemon..."
    );

    let runtime = Arc::new(NitroRuntime::with_config(SandboxConfig {
        max_memory_bytes: config.max_memory_bytes,
        ..SandboxConfig::default()
    }));
    let metrics = Arc::new(WorkerMetrics::new());
    let semaphore = Arc::new(Semaphore::new(config.max_concurrency));

    let mut swarm = create_swarm_with_mdns(config.enable_mdns)
        .await
        .context("Failed to initialize worker libp2p Swarm")?;

    let worker_peer_id = swarm.local_peer_id().to_string();

    swarm
        .listen_on(config.listen_addr.clone())
        .with_context(|| format!("Worker failed to listen on {}", config.listen_addr))?;

    for peer_addr in &config.bootstrap_peers {
        info!(%peer_addr, "Dialing bootstrap mesh peer");
        if let Err(err) = swarm.dial(peer_addr.clone()) {
            error!(%peer_addr, error = %err, "Failed to dial bootstrap peer");
        }
    }

    info!(
        %worker_peer_id,
        alloc_topic = TASK_ALLOC_TOPIC,
        results_topic = TASK_RESULTS_TOPIC,
        telemetry_topic = WORKER_TELEMETRY_TOPIC,
        "Worker daemon ready with non-blocking Wasmtime pool"
    );

    let alloc_topic_hash = gossipsub::IdentTopic::new(TASK_ALLOC_TOPIC).hash();
    let (result_tx, mut result_rx) = mpsc::channel::<ResultPayload>(256);
    let mut telemetry_tick =
        tokio::time::interval(Duration::from_secs(config.telemetry_interval_secs));
    let started_at = Instant::now();

    loop {
        tokio::select! {
            _ = tokio::signal::ctrl_c() => {
                info!(
                    completed = metrics.completed_tasks.load(Ordering::Relaxed),
                    trapped = metrics.trapped_tasks.load(Ordering::Relaxed),
                    "Received shutdown signal; terminating worker daemon cleanly"
                );
                break;
            }
            _ = telemetry_tick.tick() => {
                let heartbeat = WorkerTelemetry {
                    worker_peer_id: worker_peer_id.clone(),
                    active_tasks: metrics.active_tasks.load(Ordering::Relaxed),
                    completed_tasks: metrics.completed_tasks.load(Ordering::Relaxed),
                    trapped_tasks: metrics.trapped_tasks.load(Ordering::Relaxed),
                    cached_modules: runtime.cached_module_count(),
                    uptime_secs: started_at.elapsed().as_secs(),
                };
                if let Err(err) = publish_worker_telemetry(&mut swarm, &heartbeat) {
                    warn!(error = %format!("{err:#}"), "Failed to publish worker heartbeat");
                }
            }
            Some(result_payload) = result_rx.recv() => {
                match publish_result_payload(&mut swarm, &result_payload) {
                    Ok(maybe_msg_id) => {
                        info!(
                            task_id = %result_payload.task_id,
                            job_id = %result_payload.job_id,
                            status = ?result_payload.status,
                            computed_value = result_payload.computed_value,
                            fuel_consumed = result_payload.fuel_consumed,
                            duration_us = result_payload.duration_us,
                            cache_hit = result_payload.cache_hit,
                            message_id = ?maybe_msg_id,
                            topic = TASK_RESULTS_TOPIC,
                            "Published ResultPayload back to coordinator mesh"
                        );
                    }
                    Err(err) => {
                        error!(
                            task_id = %result_payload.task_id,
                            error = %format!("{err:#}"),
                            "Failed to publish ResultPayload onto mesh"
                        );
                    }
                }
            }
            event = swarm.select_next_some() => {
                if let Some(gossipsub::Event::Message {
                    propagation_source,
                    message_id,
                    message,
                }) = process_swarm_maintenance(&mut swarm, event)
                {
                    if message.topic == alloc_topic_hash
                        || message.topic.as_str() == TASK_ALLOC_TOPIC
                    {
                        match TaskPayload::from_bytes(&message.data) {
                            Ok(task) => {
                                info!(
                                    task_id = %task.task_id,
                                    job_id = %task.job_id,
                                    attempt = task.attempt,
                                    %propagation_source,
                                    %message_id,
                                    wasm_bytes = task.wasm_binary.len(),
                                    chunk_len = task.data_chunk.len(),
                                    fuel = task.fuel,
                                    "Dispatching intercepted TaskPayload to non-blocking worker pool"
                                );

                                let sem = Arc::clone(&semaphore);
                                let rt = Arc::clone(&runtime);
                                let m = Arc::clone(&metrics);
                                let tx = result_tx.clone();
                                let peer_id = worker_peer_id.clone();

                                tokio::spawn(async move {
                                    let Ok(_permit) = sem.acquire_owned().await else {
                                        return;
                                    };
                                    let _active_guard = ActiveTaskGuard::new(Arc::clone(&m));
                                    let m_exec = Arc::clone(&m);
                                    let result = tokio::task::spawn_blocking(move || {
                                        execute_task_in_sandbox(&rt, &m_exec, &peer_id, task)
                                    })
                                    .await;
                                    match result {
                                        Ok(payload) => {
                                            if let Err(err) = tx.send(payload).await {
                                                warn!(error = %err, "Result receiver dropped before sending payload");
                                            }
                                        }
                                        Err(join_err) => {
                                            error!(error = %join_err, "Worker blocking task panicked");
                                        }
                                    }
                                });
                            }
                            Err(err) => {
                                warn!(
                                    error = %format!("{err:#}"),
                                    "Failed to decode incoming 'task-alloc' TaskPayload"
                                );
                            }
                        }
                    }
                }
            }
        }
    }

    Ok(())
}
