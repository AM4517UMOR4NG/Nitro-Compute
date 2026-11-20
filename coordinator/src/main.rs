use std::collections::{HashMap, HashSet};
use std::path::{Path, PathBuf};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use anyhow::{anyhow, Context};
use libp2p::futures::StreamExt;
use libp2p::{gossipsub, Multiaddr, PeerId, Swarm};
use nitro_core::network::{
    create_swarm_with_mdns, process_swarm_maintenance, publish_task_payload, NitroBehavior,
    ResultPayload, TaskOutcome, TaskPayload, WorkerTelemetry, DEFAULT_FUEL_LIMIT, TASK_ALLOC_TOPIC,
    TASK_RESULTS_TOPIC, WORKER_TELEMETRY_TOPIC,
};
use nitro_core::runtime::{sha256_hex, DEFAULT_MAX_MEMORY_BYTES};
use tokio::fs::File;
use tokio::io::{AsyncBufReadExt, BufReader};
use tracing::{debug, error, info, warn};

/// Candidate paths for the compiled `jobs/word-count` WebAssembly binary.
const DEFAULT_WASM_PATHS: &[&str] = &[
    "target/wasm32-wasip1/release/word_count.wasm",
    "target/wasm32-wasip1/debug/word_count.wasm",
    "jobs/word-count/target/wasm32-wasip1/release/word_count.wasm",
    "jobs/word-count/target/wasm32-wasip1/debug/word_count.wasm",
    "target/wasm32-wasi/release/word_count.wasm",
    "target/wasm32-wasi/debug/word_count.wasm",
    "jobs/word-count/target/wasm32-wasi/release/word_count.wasm",
    "jobs/word-count/target/wasm32-wasi/debug/word_count.wasm",
];

/// CLI configuration for the coordinator network entry oracle.
struct CoordinatorConfig {
    input_file: PathBuf,
    wasm_file: Option<PathBuf>,
    entry_fn: String,
    listen_addr: Multiaddr,
    peer_addrs: Vec<Multiaddr>,
    lines_per_chunk: usize,
    fuel_limit: u64,
    max_memory_bytes: usize,
    max_retries: u32,
    task_timeout_secs: u64,
    result_timeout_secs: u64,
    discovery_timeout_secs: u64,
    min_peers: usize,
    enable_mdns: bool,
}

impl CoordinatorConfig {
    fn from_args() -> anyhow::Result<Self> {
        let mut input_file: Option<PathBuf> = None;
        let mut wasm_file: Option<PathBuf> = None;
        let mut entry_fn = "map_line".to_string();
        let mut listen_addr: Multiaddr = "/ip4/0.0.0.0/tcp/0"
            .parse()
            .context("Invalid default listen Multiaddr")?;
        let mut peer_addrs: Vec<Multiaddr> = Vec::new();
        let mut lines_per_chunk: usize = 1;
        let mut fuel_limit: u64 = DEFAULT_FUEL_LIMIT;
        let mut max_memory_bytes: usize = DEFAULT_MAX_MEMORY_BYTES;
        let mut max_retries: u32 = 2;
        let mut task_timeout_secs: u64 = 10;
        let mut result_timeout_secs: u64 = 30;
        let mut discovery_timeout_secs: u64 = 10;
        let mut min_peers: usize = 1;
        let mut enable_mdns = true;

        let mut args = std::env::args().skip(1);
        while let Some(arg) = args.next() {
            match arg.as_str() {
                "--input" | "-i" => {
                    if let Some(val) = args.next() {
                        input_file = Some(PathBuf::from(val));
                    }
                }
                "--wasm" | "-w" => {
                    if let Some(val) = args.next() {
                        wasm_file = Some(PathBuf::from(val));
                    }
                }
                "--entry-fn" | "-e" => {
                    if let Some(val) = args.next() {
                        entry_fn = val;
                    }
                }
                "--listen" | "-l" => {
                    if let Some(val) = args.next() {
                        listen_addr = val
                            .parse()
                            .with_context(|| format!("Invalid --listen multiaddr: {val}"))?;
                    }
                }
                "--dial" | "--peer" | "-p" => {
                    if let Some(val) = args.next() {
                        let addr: Multiaddr = val
                            .parse()
                            .with_context(|| format!("Invalid peer multiaddr: {val}"))?;
                        peer_addrs.push(addr);
                    }
                }
                "--chunk-lines" | "-c" => {
                    if let Some(val) = args.next() {
                        lines_per_chunk = val
                            .parse::<usize>()
                            .context("Invalid --chunk-lines value")?
                            .max(1);
                    }
                }
                "--fuel" | "-f" => {
                    if let Some(val) = args.next() {
                        fuel_limit = val.parse::<u64>().context("Invalid --fuel value")?;
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
                "--retries" | "-r" => {
                    if let Some(val) = args.next() {
                        max_retries = val.parse::<u32>().context("Invalid --retries value")?;
                    }
                }
                "--task-timeout" => {
                    if let Some(val) = args.next() {
                        task_timeout_secs = val
                            .parse::<u64>()
                            .context("Invalid --task-timeout value")?
                            .max(1);
                    }
                }
                "--timeout" | "-t" => {
                    if let Some(val) = args.next() {
                        result_timeout_secs = val
                            .parse::<u64>()
                            .context("Invalid --timeout value in seconds")?
                            .max(1);
                    }
                }
                "--discovery-timeout" => {
                    if let Some(val) = args.next() {
                        discovery_timeout_secs = val
                            .parse::<u64>()
                            .context("Invalid --discovery-timeout value in seconds")?;
                    }
                }
                "--min-peers" => {
                    if let Some(val) = args.next() {
                        min_peers = val.parse::<usize>().context("Invalid --min-peers value")?;
                    }
                }
                "--no-mdns" => {
                    enable_mdns = false;
                }
                other => {
                    if input_file.is_none() {
                        input_file = Some(PathBuf::from(other));
                    } else if let Ok(addr) = other.parse::<Multiaddr>() {
                        peer_addrs.push(addr);
                    }
                }
            }
        }

        Ok(Self {
            input_file: input_file.unwrap_or_else(|| PathBuf::from("input.txt")),
            wasm_file,
            entry_fn,
            listen_addr,
            peer_addrs,
            lines_per_chunk,
            fuel_limit,
            max_memory_bytes,
            max_retries,
            task_timeout_secs,
            result_timeout_secs,
            discovery_timeout_secs,
            min_peers,
            enable_mdns,
        })
    }
}

enum TaskState {
    InFlight {
        chunk_index: usize,
        attempt: u32,
        dispatched_at: Instant,
    },
    Completed {
        worker_peer_id: String,
        computed_value: usize,
        fuel_consumed: u64,
        duration_us: u64,
        cache_hit: bool,
    },
    Trapped {
        worker_peer_id: String,
        kind: String,
        reason: String,
    },
    Exhausted {
        last_error: String,
    },
}

#[derive(Default)]
struct WorkerStats {
    tasks_completed: usize,
    total_fuel: u64,
    total_duration_us: u64,
    cache_hits: usize,
}

struct JobScheduler {
    job_id: String,
    wasm_binary: Vec<u8>,
    wasm_sha256: String,
    chunks: Vec<String>,
    tasks: HashMap<String, TaskState>,
    worker_stats: HashMap<String, WorkerStats>,
}

impl JobScheduler {
    fn new(job_id: String, wasm_binary: Vec<u8>, wasm_sha256: String, chunks: Vec<String>) -> Self {
        Self {
            job_id,
            wasm_binary,
            wasm_sha256,
            chunks,
            tasks: HashMap::new(),
            worker_stats: HashMap::new(),
        }
    }

    fn is_finished(&self) -> bool {
        !self.tasks.is_empty()
            && self
                .tasks
                .values()
                .all(|state| !matches!(state, TaskState::InFlight { .. }))
    }

    fn build_payload(
        &self,
        task_id: &str,
        chunk_index: usize,
        attempt: u32,
        cfg: &CoordinatorConfig,
    ) -> TaskPayload {
        TaskPayload::with_options(
            task_id.to_string(),
            self.job_id.clone(),
            cfg.entry_fn.clone(),
            self.wasm_binary.clone(),
            self.wasm_sha256.clone(),
            self.chunks[chunk_index].clone(),
            cfg.fuel_limit,
            cfg.max_memory_bytes,
            attempt,
        )
    }

    fn dispatch_initial(
        &mut self,
        swarm: &mut Swarm<NitroBehavior>,
        cfg: &CoordinatorConfig,
    ) -> anyhow::Result<()> {
        for index in 0..self.chunks.len() {
            let task_id = format!("{}-task-{index}", self.job_id);
            let payload = self.build_payload(&task_id, index, 1, cfg);
            let maybe_msg_id = publish_task_payload(swarm, &payload)?;

            info!(
                task_id = %payload.task_id,
                job_id = %self.job_id,
                attempt = 1,
                fuel = payload.fuel,
                chunk_bytes = payload.data_chunk.len(),
                message_id = ?maybe_msg_id,
                topic = TASK_ALLOC_TOPIC,
                "Broadcasted TaskPayload to Gossipsub mesh"
            );

            self.tasks.insert(
                task_id,
                TaskState::InFlight {
                    chunk_index: index,
                    attempt: 1,
                    dispatched_at: Instant::now(),
                },
            );
        }
        Ok(())
    }

    fn handle_result(
        &mut self,
        swarm: &mut Swarm<NitroBehavior>,
        result: ResultPayload,
        cfg: &CoordinatorConfig,
    ) -> anyhow::Result<()> {
        let Some(state) = self.tasks.get(&result.task_id) else {
            debug!(task_id = %result.task_id, "Ignoring result for unknown task");
            return Ok(());
        };

        let TaskState::InFlight {
            chunk_index,
            attempt,
            ..
        } = *state
        else {
            debug!(task_id = %result.task_id, "Ignoring duplicate ResultPayload");
            return Ok(());
        };

        match result.status {
            TaskOutcome::Success => {
                let stats = self
                    .worker_stats
                    .entry(result.worker_peer_id.clone())
                    .or_default();
                stats.tasks_completed += 1;
                stats.total_fuel = stats.total_fuel.saturating_add(result.fuel_consumed);
                stats.total_duration_us =
                    stats.total_duration_us.saturating_add(result.duration_us);
                if result.cache_hit {
                    stats.cache_hits += 1;
                }

                self.tasks.insert(
                    result.task_id.clone(),
                    TaskState::Completed {
                        worker_peer_id: result.worker_peer_id.clone(),
                        computed_value: result.computed_value,
                        fuel_consumed: result.fuel_consumed,
                        duration_us: result.duration_us,
                        cache_hit: result.cache_hit,
                    },
                );

                info!(
                    task_id = %result.task_id,
                    worker_peer_id = %result.worker_peer_id,
                    computed_value = result.computed_value,
                    fuel_consumed = result.fuel_consumed,
                    duration_us = result.duration_us,
                    cache_hit = result.cache_hit,
                    remaining = self.remaining_in_flight(),
                    "Completed MapReduce task chunk"
                );
            }
            TaskOutcome::SecurityTrap { kind, reason } => {
                error!(
                    task_id = %result.task_id,
                    worker_peer_id = %result.worker_peer_id,
                    trap_kind = %kind,
                    reason = %reason,
                    "Worker reported deterministic security trap; halting retries for poisoned task"
                );
                self.tasks.insert(
                    result.task_id,
                    TaskState::Trapped {
                        worker_peer_id: result.worker_peer_id,
                        kind,
                        reason,
                    },
                );
            }
            TaskOutcome::ExecutionError { reason } => {
                if attempt <= cfg.max_retries {
                    let next_attempt = attempt + 1;
                    warn!(
                        task_id = %result.task_id,
                        worker_peer_id = %result.worker_peer_id,
                        next_attempt,
                        reason = %reason,
                        "Worker returned execution error; re-dispatching task"
                    );
                    let payload =
                        self.build_payload(&result.task_id, chunk_index, next_attempt, cfg);
                    let _ = publish_task_payload(swarm, &payload)?;
                    self.tasks.insert(
                        result.task_id,
                        TaskState::InFlight {
                            chunk_index,
                            attempt: next_attempt,
                            dispatched_at: Instant::now(),
                        },
                    );
                } else {
                    error!(
                        task_id = %result.task_id,
                        attempts = attempt,
                        reason = %reason,
                        "Task exhausted max retries after worker execution error"
                    );
                    self.tasks
                        .insert(result.task_id, TaskState::Exhausted { last_error: reason });
                }
            }
        }

        Ok(())
    }

    fn retry_timed_out_tasks(
        &mut self,
        swarm: &mut Swarm<NitroBehavior>,
        cfg: &CoordinatorConfig,
    ) -> anyhow::Result<()> {
        let task_timeout = Duration::from_secs(cfg.task_timeout_secs);
        let timed_out: Vec<(String, usize, u32)> = self
            .tasks
            .iter()
            .filter_map(|(task_id, state)| match state {
                TaskState::InFlight {
                    chunk_index,
                    attempt,
                    dispatched_at,
                } if dispatched_at.elapsed() >= task_timeout => {
                    Some((task_id.clone(), *chunk_index, *attempt))
                }
                _ => None,
            })
            .collect();

        for (task_id, chunk_index, attempt) in timed_out {
            if attempt <= cfg.max_retries {
                let next_attempt = attempt + 1;
                warn!(
                    %task_id,
                    next_attempt,
                    timeout_secs = cfg.task_timeout_secs,
                    "Task lease expired without response; re-broadcasting to mesh"
                );
                let payload = self.build_payload(&task_id, chunk_index, next_attempt, cfg);
                let _ = publish_task_payload(swarm, &payload)?;
                self.tasks.insert(
                    task_id,
                    TaskState::InFlight {
                        chunk_index,
                        attempt: next_attempt,
                        dispatched_at: Instant::now(),
                    },
                );
            } else {
                error!(
                    %task_id,
                    attempts = attempt,
                    "Task exhausted retry budget after lease timeout"
                );
                self.tasks.insert(
                    task_id,
                    TaskState::Exhausted {
                        last_error: format!("Timed out after {} attempt(s)", attempt),
                    },
                );
            }
        }

        Ok(())
    }

    fn remaining_in_flight(&self) -> usize {
        self.tasks
            .values()
            .filter(|s| matches!(s, TaskState::InFlight { .. }))
            .count()
    }

    fn summarize(&self) -> (usize, usize, usize, usize, u64, u64, usize) {
        let mut unified_sum: usize = 0;
        let mut completed: usize = 0;
        let mut trapped: usize = 0;
        let mut failed: usize = 0;
        let mut total_fuel: u64 = 0;
        let mut total_duration_us: u64 = 0;
        let mut cache_hits: usize = 0;

        for state in self.tasks.values() {
            match state {
                TaskState::Completed {
                    computed_value,
                    fuel_consumed,
                    duration_us,
                    cache_hit,
                    ..
                } => {
                    completed += 1;
                    unified_sum = unified_sum.saturating_add(*computed_value);
                    total_fuel = total_fuel.saturating_add(*fuel_consumed);
                    total_duration_us = total_duration_us.saturating_add(*duration_us);
                    if *cache_hit {
                        cache_hits += 1;
                    }
                }
                TaskState::Trapped {
                    worker_peer_id,
                    kind,
                    reason,
                } => {
                    trapped += 1;
                    warn!(
                        %worker_peer_id,
                        trap_kind = %kind,
                        reason = %reason,
                        "Task ended with security trap"
                    );
                }
                TaskState::Exhausted { last_error } => {
                    failed += 1;
                    warn!(error = %last_error, "Task exhausted retries");
                }
                TaskState::InFlight { .. } => {
                    failed += 1;
                }
            }
        }

        (
            unified_sum,
            completed,
            trapped,
            failed,
            total_fuel,
            total_duration_us,
            cache_hits,
        )
    }
}

/// Reads the compiled `.wasm` bytecode from `--wasm` or standard `jobs/word-count` build output paths.
async fn load_wasm_binary(explicit_path: Option<&Path>) -> anyhow::Result<Vec<u8>> {
    if let Some(path) = explicit_path {
        info!(path = %path.display(), "Loading compiled Wasm job binary from explicit path");
        return tokio::fs::read(path)
            .await
            .with_context(|| format!("Failed to read Wasm binary at {}", path.display()));
    }

    for candidate in DEFAULT_WASM_PATHS {
        let path = Path::new(candidate);
        if path.exists() {
            info!(path = %path.display(), "Discovered compiled word-count Wasm binary");
            return tokio::fs::read(path).await.with_context(|| {
                format!(
                    "Failed to read discovered Wasm binary at {}",
                    path.display()
                )
            });
        }
    }

    Err(anyhow!(
        "Compiled word-count .wasm binary not found in standard target paths ({:?}). Build `jobs/word-count` with `--target wasm32-wasip1` or pass `--wasm <path>`.",
        DEFAULT_WASM_PATHS
    ))
}

/// Ingests a raw text stream pipeline file and fragments it into non-empty data chunks.
async fn ingest_and_fragment_stream(
    input_path: &Path,
    lines_per_chunk: usize,
) -> anyhow::Result<Vec<String>> {
    info!(
        path = %input_path.display(),
        lines_per_chunk,
        "Ingesting raw text stream pipeline file"
    );

    let file = File::open(input_path)
        .await
        .with_context(|| format!("Failed to open input stream file: {}", input_path.display()))?;
    let reader = BufReader::new(file);
    let mut lines = reader.lines();

    let mut chunks = Vec::new();
    let mut current_batch = Vec::with_capacity(lines_per_chunk);

    while let Some(line) = lines
        .next_line()
        .await
        .context("Failed while reading line from text stream")?
    {
        let trimmed = line.trim();
        if trimmed.is_empty() {
            continue;
        }
        current_batch.push(trimmed.to_string());
        if current_batch.len() >= lines_per_chunk {
            chunks.push(current_batch.join("\n"));
            current_batch.clear();
        }
    }

    if !current_batch.is_empty() {
        chunks.push(current_batch.join("\n"));
    }

    info!(
        total_chunks = chunks.len(),
        "Fragmented input text stream into MapReduce chunks"
    );
    Ok(chunks)
}

/// Waits until at least `min_peers` workers subscribe to `"task-alloc"` (via `--peer` dial or mDNS)
/// or until `discovery_timeout` expires.
async fn wait_for_mesh_workers(
    swarm: &mut Swarm<NitroBehavior>,
    min_peers: usize,
    discovery_timeout: Duration,
) {
    if min_peers == 0 || discovery_timeout.is_zero() {
        return;
    }

    let alloc_topic_hash = gossipsub::IdentTopic::new(TASK_ALLOC_TOPIC).hash();
    let mut subscribed_peers: HashSet<PeerId> = HashSet::new();
    let deadline = tokio::time::sleep(discovery_timeout);
    tokio::pin!(deadline);

    info!(
        min_peers,
        timeout_secs = discovery_timeout.as_secs(),
        "Waiting for mesh worker subscriptions (dial / mDNS)"
    );

    loop {
        tokio::select! {
            _ = &mut deadline => {
                if subscribed_peers.len() < min_peers {
                    warn!(
                        connected_subscribers = subscribed_peers.len(),
                        min_peers,
                        "Mesh discovery window elapsed before reaching target worker count; proceeding"
                    );
                }
                break;
            }
            event = swarm.select_next_some() => {
                if let Some(gossipsub::Event::Subscribed { peer_id, topic }) =
                    process_swarm_maintenance(swarm, event)
                {
                    if topic == alloc_topic_hash || topic.as_str() == TASK_ALLOC_TOPIC {
                        subscribed_peers.insert(peer_id);
                        info!(
                            %peer_id,
                            subscribers = subscribed_peers.len(),
                            min_peers,
                            "Worker peer subscribed to 'task-alloc'"
                        );
                        if subscribed_peers.len() >= min_peers {
                            break;
                        }
                    }
                }
            }
        }
    }
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    nitro_core::init_logging();

    let config = CoordinatorConfig::from_args()?;
    info!(
        fuel_limit = config.fuel_limit,
        max_memory_mb = config.max_memory_bytes / (1024 * 1024),
        entry_fn = %config.entry_fn,
        retries = config.max_retries,
        "Starting nitro-compute coordinator oracle..."
    );

    let wasm_binary = load_wasm_binary(config.wasm_file.as_deref()).await?;
    let wasm_sha256 = sha256_hex(&wasm_binary);
    let chunks = ingest_and_fragment_stream(&config.input_file, config.lines_per_chunk).await?;

    if chunks.is_empty() {
        info!("Input stream contained no non-empty lines; final sum = 0");
        return Ok(());
    }

    let epoch_ms = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_millis())
        .unwrap_or(0);
    let job_id = format!("job-{}-{}", &wasm_sha256[..8], epoch_ms % 100_000);

    let mut swarm = create_swarm_with_mdns(config.enable_mdns)
        .await
        .context("Failed to initialize coordinator libp2p Swarm")?;

    swarm
        .listen_on(config.listen_addr.clone())
        .with_context(|| format!("Coordinator failed to listen on {}", config.listen_addr))?;

    for peer_addr in &config.peer_addrs {
        info!(%peer_addr, "Dialing worker node on mesh fabric");
        swarm
            .dial(peer_addr.clone())
            .with_context(|| format!("Failed to dial worker peer {peer_addr}"))?;
    }

    wait_for_mesh_workers(
        &mut swarm,
        config.min_peers,
        Duration::from_secs(config.discovery_timeout_secs),
    )
    .await;

    let mut scheduler = JobScheduler::new(job_id.clone(), wasm_binary, wasm_sha256, chunks);
    scheduler.dispatch_initial(&mut swarm, &config)?;

    let results_topic_hash = gossipsub::IdentTopic::new(TASK_RESULTS_TOPIC).hash();
    let telemetry_topic_hash = gossipsub::IdentTopic::new(WORKER_TELEMETRY_TOPIC).hash();

    let total_deadline = tokio::time::sleep(Duration::from_secs(config.result_timeout_secs));
    tokio::pin!(total_deadline);
    let mut retry_tick = tokio::time::interval(Duration::from_secs(1));

    loop {
        tokio::select! {
            _ = &mut total_deadline => {
                warn!(
                    remaining_in_flight = scheduler.remaining_in_flight(),
                    "Global job timeout reached before all tasks completed"
                );
                break;
            }
            _ = retry_tick.tick() => {
                scheduler.retry_timed_out_tasks(&mut swarm, &config)?;
                if scheduler.is_finished() {
                    break;
                }
            }
            event = swarm.select_next_some() => {
                if let Some(gossipsub::Event::Message { message, .. }) =
                    process_swarm_maintenance(&mut swarm, event)
                {
                    if message.topic == results_topic_hash
                        || message.topic.as_str() == TASK_RESULTS_TOPIC
                    {
                        match ResultPayload::from_bytes(&message.data) {
                            Ok(result) => {
                                scheduler.handle_result(&mut swarm, result, &config)?;
                                if scheduler.is_finished() {
                                    break;
                                }
                            }
                            Err(err) => {
                                warn!(
                                    error = %format!("{err:#}"),
                                    "Failed to decode ResultPayload on '{}'",
                                    TASK_RESULTS_TOPIC
                                );
                            }
                        }
                    } else if message.topic == telemetry_topic_hash
                        || message.topic.as_str() == WORKER_TELEMETRY_TOPIC
                    {
                        if let Ok(hb) = WorkerTelemetry::from_bytes(&message.data) {
                            debug!(
                                worker_peer_id = %hb.worker_peer_id,
                                active = hb.active_tasks,
                                completed = hb.completed_tasks,
                                trapped = hb.trapped_tasks,
                                cached_modules = hb.cached_modules,
                                uptime_secs = hb.uptime_secs,
                                "Observed worker heartbeat"
                            );
                        }
                    }
                }
            }
        }
    }

    let (unified_sum, completed, trapped, failed, total_fuel, total_duration_us, cache_hits) =
        scheduler.summarize();

    let avg_duration_us = if completed > 0 {
        total_duration_us / (completed as u64)
    } else {
        0
    };

    for (peer_id, stats) in &scheduler.worker_stats {
        let worker_avg_us = if stats.tasks_completed > 0 {
            stats.total_duration_us / (stats.tasks_completed as u64)
        } else {
            0
        };
        info!(
            worker_peer_id = %peer_id,
            tasks_completed = stats.tasks_completed,
            total_fuel = stats.total_fuel,
            avg_duration_us = worker_avg_us,
            cache_hits = stats.cache_hits,
            "Worker contribution summary"
        );
    }

    info!(
        job_id = %job_id,
        total_chunks = scheduler.chunks.len(),
        completed,
        trapped,
        failed,
        unified_sum,
        total_fuel,
        avg_duration_us,
        cache_hits,
        "Final MapReduce execution summary -> unified_sum = {}",
        unified_sum
    );

    if trapped > 0 || failed > 0 {
        return Err(anyhow!(
            "MapReduce job '{}' finished with incomplete tasks (completed={}, trapped={}, failed={})",
            job_id,
            completed,
            trapped,
            failed
        ));
    }

    Ok(())
}
