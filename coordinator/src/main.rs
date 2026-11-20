#![forbid(unsafe_code)]

use std::collections::{HashMap, HashSet};
use std::net::SocketAddr;
use std::path::{Path, PathBuf};
use std::sync::atomic::Ordering;
use std::sync::Arc;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use anyhow::{anyhow, Context};
use libp2p::futures::StreamExt;
use libp2p::identity::Keypair;
use libp2p::{gossipsub, Multiaddr, PeerId, Swarm};
use nitro_core::metrics::{spawn_metrics_server, NitroTelemetryRegistry};
use nitro_core::network::{
    create_swarm_with_config, encode_hex, load_or_generate_keypair, process_swarm_maintenance,
    publish_task_payload, JobPhase, NitroBehavior, ResultPayload, TaskOutcome, TaskPayload,
    WorkerTelemetry, DEFAULT_FUEL_LIMIT, TASK_ALLOC_TOPIC, TASK_RESULTS_TOPIC,
    WORKER_TELEMETRY_TOPIC,
};
use nitro_core::runtime::{sha256_hex, DEFAULT_EXECUTION_TIMEOUT_MS, DEFAULT_MAX_MEMORY_BYTES};
use tokio::fs::File;
use tokio::io::{AsyncBufReadExt, BufReader};
use tracing::{debug, error, info, warn};

/// Candidate paths for compiled WebAssembly job binaries.
const DEFAULT_WASM_PATHS: &[&str] = &[
    "target/wasm32-wasip1/release/word_count.wasm",
    "target/wasm32-wasip1/debug/word_count.wasm",
    "jobs/word-count/target/wasm32-wasip1/release/word_count.wasm",
    "jobs/word-count/target/wasm32-wasip1/debug/word_count.wasm",
    "target/wasm32-wasip1/release/kv_wordcount.wasm",
    "target/wasm32-wasip1/release/pattern_match.wasm",
    "target/wasm32-wasi/release/word_count.wasm",
    "target/wasm32-wasi/debug/word_count.wasm",
];

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum CoordinatorMode {
    Scalar,
    KvMapReduce,
}

/// CLI configuration for the coordinator network entry oracle.
struct CoordinatorConfig {
    input_file: PathBuf,
    wasm_file: Option<PathBuf>,
    mode: CoordinatorMode,
    entry_fn: Option<String>,
    reduce_fn: String,
    listen_addr: Multiaddr,
    peer_addrs: Vec<Multiaddr>,
    identity_key_path: Option<PathBuf>,
    metrics_addr: Option<SocketAddr>,
    verify_worker_sigs: bool,
    dedup_wasm: bool,
    speculative: bool,
    lines_per_chunk: usize,
    fuel_limit: u64,
    max_memory_bytes: usize,
    execution_timeout_ms: u64,
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
        let mut mode = CoordinatorMode::Scalar;
        let mut entry_fn: Option<String> = None;
        let mut reduce_fn = "reduce_bytes".to_string();
        let mut listen_addr: Multiaddr = "/ip4/0.0.0.0/tcp/0"
            .parse()
            .context("Invalid default listen Multiaddr")?;
        let mut peer_addrs: Vec<Multiaddr> = Vec::new();
        let mut identity_key_path: Option<PathBuf> = None;
        let mut metrics_addr: Option<SocketAddr> = None;
        let mut verify_worker_sigs = false;
        let mut dedup_wasm = false;
        let mut speculative = false;
        let mut lines_per_chunk: usize = 1;
        let mut fuel_limit: u64 = DEFAULT_FUEL_LIMIT;
        let mut max_memory_bytes: usize = DEFAULT_MAX_MEMORY_BYTES;
        let mut execution_timeout_ms: u64 = DEFAULT_EXECUTION_TIMEOUT_MS;
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
                "--mode" => {
                    if let Some(val) = args.next() {
                        mode = match val.as_str() {
                            "scalar" => CoordinatorMode::Scalar,
                            "kv-mapreduce" | "mapreduce" | "kv" => CoordinatorMode::KvMapReduce,
                            _ => {
                                return Err(anyhow!(
                                    "Invalid --mode '{val}'; expected 'scalar' or 'kv-mapreduce'"
                                ))
                            }
                        };
                    }
                }
                "--entry-fn" | "-e" => {
                    if let Some(val) = args.next() {
                        entry_fn = Some(val);
                    }
                }
                "--reduce-fn" => {
                    if let Some(val) = args.next() {
                        reduce_fn = val;
                        mode = CoordinatorMode::KvMapReduce;
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
                "--identity-key" => {
                    if let Some(val) = args.next() {
                        identity_key_path = Some(PathBuf::from(val));
                    }
                }
                "--metrics-addr" => {
                    if let Some(val) = args.next() {
                        let addr: SocketAddr = val
                            .parse()
                            .with_context(|| format!("Invalid --metrics-addr SocketAddr: {val}"))?;
                        metrics_addr = Some(addr);
                    }
                }
                "--verify-worker-sigs" => {
                    verify_worker_sigs = true;
                }
                "--dedup-wasm" => {
                    dedup_wasm = true;
                }
                "--speculative" => {
                    speculative = true;
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
                "--execution-timeout-ms" => {
                    if let Some(val) = args.next() {
                        execution_timeout_ms = val
                            .parse::<u64>()
                            .context("Invalid --execution-timeout-ms value")?
                            .max(1);
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
            mode,
            entry_fn,
            reduce_fn,
            listen_addr,
            peer_addrs,
            identity_key_path,
            metrics_addr,
            verify_worker_sigs,
            dedup_wasm,
            speculative,
            lines_per_chunk,
            fuel_limit,
            max_memory_bytes,
            execution_timeout_ms,
            max_retries,
            task_timeout_secs,
            result_timeout_secs,
            discovery_timeout_secs,
            min_peers,
            enable_mdns,
        })
    }

    fn effective_map_entry(&self) -> &str {
        if let Some(ref custom) = self.entry_fn {
            custom.as_str()
        } else {
            match self.mode {
                CoordinatorMode::Scalar => "map_line",
                CoordinatorMode::KvMapReduce => "map_bytes",
            }
        }
    }
}

enum TaskState {
    InFlight {
        chunk_index: usize,
        attempt: u32,
        dispatched_at: Instant,
        speculative_sent: bool,
    },
    Completed {
        computed_value: usize,
        output_bytes: Vec<u8>,
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
    phase: JobPhase,
    entry_fn: String,
    wasm_binary: Vec<u8>,
    wasm_sha256: String,
    chunks: Vec<String>,
    tasks: HashMap<String, TaskState>,
    worker_stats: HashMap<String, WorkerStats>,
    keypair: Keypair,
}

impl JobScheduler {
    fn new(
        job_id: String,
        phase: JobPhase,
        entry_fn: String,
        wasm_binary: Vec<u8>,
        wasm_sha256: String,
        chunks: Vec<String>,
        keypair: Keypair,
    ) -> Self {
        Self {
            job_id,
            phase,
            entry_fn,
            wasm_binary,
            wasm_sha256,
            chunks,
            tasks: HashMap::new(),
            worker_stats: HashMap::new(),
            keypair,
        }
    }

    fn is_finished(&self) -> bool {
        !self.tasks.is_empty()
            && self
                .tasks
                .values()
                .all(|state| !matches!(state, TaskState::InFlight { .. }))
    }

    fn build_signed_payload(
        &self,
        task_id: &str,
        chunk_index: usize,
        attempt: u32,
        include_wasm: bool,
        cfg: &CoordinatorConfig,
    ) -> anyhow::Result<TaskPayload> {
        let embedded_wasm = if include_wasm {
            self.wasm_binary.clone()
        } else {
            Vec::new()
        };
        let mut payload = TaskPayload::with_options(
            task_id.to_string(),
            self.job_id.clone(),
            self.entry_fn.clone(),
            embedded_wasm,
            self.wasm_sha256.clone(),
            self.chunks[chunk_index].clone(),
            cfg.fuel_limit,
            cfg.max_memory_bytes,
            attempt,
        )
        .with_phase_and_timeout(self.phase, cfg.execution_timeout_ms);
        payload.sign(&self.keypair)?;
        Ok(payload)
    }

    fn dispatch_initial(
        &mut self,
        swarm: &mut Swarm<NitroBehavior>,
        cfg: &CoordinatorConfig,
    ) -> anyhow::Result<()> {
        for index in 0..self.chunks.len() {
            let task_id = format!("{}-task-{index}", self.job_id);
            let include_wasm = !cfg.dedup_wasm || index == 0;
            let payload = self.build_signed_payload(&task_id, index, 1, include_wasm, cfg)?;
            let maybe_msg_id = publish_task_payload(swarm, &payload)?;

            info!(
                task_id = %payload.task_id,
                job_id = %self.job_id,
                phase = ?self.phase,
                entry_fn = %self.entry_fn,
                attempt = 1,
                embedded_wasm = include_wasm,
                fuel = payload.fuel,
                chunk_bytes = payload.data_chunk.len(),
                message_id = ?maybe_msg_id,
                topic = TASK_ALLOC_TOPIC,
                "Broadcasted signed TaskPayload to Gossipsub mesh"
            );

            self.tasks.insert(
                task_id,
                TaskState::InFlight {
                    chunk_index: index,
                    attempt: 1,
                    dispatched_at: Instant::now(),
                    speculative_sent: false,
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
        registry: &NitroTelemetryRegistry,
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

        if let Err(sig_err) = result.verify_signature(cfg.verify_worker_sigs) {
            registry
                .signature_failures_total
                .fetch_add(1, Ordering::Relaxed);
            warn!(
                task_id = %result.task_id,
                worker_peer_id = %result.worker_peer_id,
                error = %format!("{sig_err:#}"),
                "Rejected ResultPayload due to Ed25519 signature verification failure"
            );
            return Ok(());
        }

        match result.status {
            TaskOutcome::Success => {
                registry.record_execution(
                    result.fuel_consumed,
                    result.duration_us,
                    result.cache_hit,
                );
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
                        computed_value: result.computed_value,
                        output_bytes: result.output_bytes,
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
                        "Worker returned execution error; re-dispatching with full Wasm binary"
                    );
                    let payload = self.build_signed_payload(
                        &result.task_id,
                        chunk_index,
                        next_attempt,
                        true,
                        cfg,
                    )?;
                    let _ = publish_task_payload(swarm, &payload)?;
                    self.tasks.insert(
                        result.task_id,
                        TaskState::InFlight {
                            chunk_index,
                            attempt: next_attempt,
                            dispatched_at: Instant::now(),
                            speculative_sent: false,
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

    /// Implements lease-timeout retries and Google MapReduce §3.6 speculative backup execution
    /// for straggler tasks when >= 75% of tasks have already completed.
    fn tick_maintenance(
        &mut self,
        swarm: &mut Swarm<NitroBehavior>,
        cfg: &CoordinatorConfig,
        registry: &NitroTelemetryRegistry,
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
                    ..
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
                    "Task lease expired without response; re-broadcasting with full Wasm binary"
                );
                let payload =
                    self.build_signed_payload(&task_id, chunk_index, next_attempt, true, cfg)?;
                let _ = publish_task_payload(swarm, &payload)?;
                self.tasks.insert(
                    task_id,
                    TaskState::InFlight {
                        chunk_index,
                        attempt: next_attempt,
                        dispatched_at: Instant::now(),
                        speculative_sent: false,
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

        if cfg.speculative {
            let mut completed_durations: Vec<u64> = self
                .tasks
                .values()
                .filter_map(|s| match s {
                    TaskState::Completed { duration_us, .. } => Some(*duration_us),
                    _ => None,
                })
                .collect();

            let total_tasks = self.tasks.len();
            if total_tasks >= 2 && completed_durations.len() * 4 >= total_tasks * 3 {
                completed_durations.sort_unstable();
                let median_us = completed_durations[completed_durations.len() / 2];
                let straggler_threshold = Duration::from_micros(median_us.saturating_mul(2))
                    .max(Duration::from_millis(1500));

                let stragglers: Vec<(String, usize, u32)> = self
                    .tasks
                    .iter()
                    .filter_map(|(task_id, state)| match state {
                        TaskState::InFlight {
                            chunk_index,
                            attempt,
                            dispatched_at,
                            speculative_sent: false,
                        } if dispatched_at.elapsed() >= straggler_threshold => {
                            Some((task_id.clone(), *chunk_index, *attempt))
                        }
                        _ => None,
                    })
                    .collect();

                for (task_id, chunk_index, attempt) in stragglers {
                    warn!(
                        %task_id,
                        threshold_ms = straggler_threshold.as_millis(),
                        "Speculative execution: dispatching backup copy for straggler chunk"
                    );
                    let payload =
                        self.build_signed_payload(&task_id, chunk_index, attempt, true, cfg)?;
                    let _ = publish_task_payload(swarm, &payload)?;
                    registry
                        .speculative_backups_total
                        .fetch_add(1, Ordering::Relaxed);
                    if let Some(TaskState::InFlight {
                        speculative_sent, ..
                    }) = self.tasks.get_mut(&task_id)
                    {
                        *speculative_sent = true;
                    }
                }
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

    fn collect_output_chunks_in_order(&self) -> Vec<Vec<u8>> {
        let mut outputs = Vec::with_capacity(self.chunks.len());
        for idx in 0..self.chunks.len() {
            let task_id = format!("{}-task-{idx}", self.job_id);
            if let Some(TaskState::Completed { output_bytes, .. }) = self.tasks.get(&task_id) {
                outputs.push(output_bytes.clone());
            }
        }
        outputs
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

/// Reads the compiled `.wasm` bytecode from `--wasm` or standard `jobs/*` build output paths.
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
            info!(path = %path.display(), "Discovered compiled Wasm binary");
            return tokio::fs::read(path).await.with_context(|| {
                format!(
                    "Failed to read discovered Wasm binary at {}",
                    path.display()
                )
            });
        }
    }

    Err(anyhow!(
        "Compiled .wasm binary not found in standard target paths ({:?}). Build a job crate with `--target wasm32-wasip1` or pass `--wasm <path>`.",
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

async fn run_scheduler_phase(
    swarm: &mut Swarm<NitroBehavior>,
    scheduler: &mut JobScheduler,
    config: &CoordinatorConfig,
    registry: &NitroTelemetryRegistry,
) -> anyhow::Result<()> {
    scheduler.dispatch_initial(swarm, config)?;

    let results_topic_hash = gossipsub::IdentTopic::new(TASK_RESULTS_TOPIC).hash();
    let telemetry_topic_hash = gossipsub::IdentTopic::new(WORKER_TELEMETRY_TOPIC).hash();

    let total_deadline = tokio::time::sleep(Duration::from_secs(config.result_timeout_secs));
    tokio::pin!(total_deadline);
    let mut retry_tick = tokio::time::interval(Duration::from_secs(1));

    loop {
        tokio::select! {
            _ = &mut total_deadline => {
                warn!(
                    job_id = %scheduler.job_id,
                    remaining_in_flight = scheduler.remaining_in_flight(),
                    "Global phase timeout reached before all tasks completed"
                );
                break;
            }
            _ = retry_tick.tick() => {
                registry
                    .connected_peers
                    .store(swarm.connected_peers().count(), Ordering::Relaxed);
                scheduler.tick_maintenance(swarm, config, registry)?;
                if scheduler.is_finished() {
                    break;
                }
            }
            event = swarm.select_next_some() => {
                if let Some(gossipsub::Event::Message { message, .. }) =
                    process_swarm_maintenance(swarm, event)
                {
                    registry
                        .connected_peers
                        .store(swarm.connected_peers().count(), Ordering::Relaxed);

                    if message.topic == results_topic_hash
                        || message.topic.as_str() == TASK_RESULTS_TOPIC
                    {
                        match ResultPayload::from_bytes(&message.data) {
                            Ok(result) => {
                                scheduler.handle_result(swarm, result, config, registry)?;
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

    Ok(())
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    nitro_core::init_logging();

    let config = CoordinatorConfig::from_args()?;
    let keypair = load_or_generate_keypair(config.identity_key_path.as_deref())?;
    let coordinator_pubkey_hex = encode_hex(&keypair.public().encode_protobuf());

    info!(
        mode = ?config.mode,
        map_entry_fn = config.effective_map_entry(),
        fuel_limit = config.fuel_limit,
        max_memory_mb = config.max_memory_bytes / (1024 * 1024),
        execution_timeout_ms = config.execution_timeout_ms,
        dedup_wasm = config.dedup_wasm,
        speculative = config.speculative,
        retries = config.max_retries,
        %coordinator_pubkey_hex,
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

    let registry = Arc::new(NitroTelemetryRegistry::new());
    let mut swarm = create_swarm_with_config(keypair.clone(), config.enable_mdns)
        .await
        .context("Failed to initialize coordinator libp2p Swarm")?;

    let local_peer_id = swarm.local_peer_id().to_string();
    if let Some(metrics_addr) = config.metrics_addr {
        let _ = spawn_metrics_server(
            metrics_addr,
            "coordinator",
            local_peer_id,
            Arc::clone(&registry),
        )
        .await?;
    }

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

    let map_phase = match config.mode {
        CoordinatorMode::Scalar => JobPhase::MapScalar,
        CoordinatorMode::KvMapReduce => JobPhase::MapBytes,
    };

    let mut map_scheduler = JobScheduler::new(
        job_id.clone(),
        map_phase,
        config.effective_map_entry().to_string(),
        wasm_binary.clone(),
        wasm_sha256.clone(),
        chunks,
        keypair.clone(),
    );

    run_scheduler_phase(&mut swarm, &mut map_scheduler, &config, &registry).await?;

    let (unified_sum, completed, trapped, failed, total_fuel, total_duration_us, cache_hits) =
        map_scheduler.summarize();

    let avg_duration_us = if completed > 0 {
        total_duration_us / (completed as u64)
    } else {
        0
    };

    for (peer_id, stats) in &map_scheduler.worker_stats {
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
        total_chunks = map_scheduler.chunks.len(),
        completed,
        trapped,
        failed,
        unified_sum,
        total_fuel,
        avg_duration_us,
        cache_hits,
        "Map phase execution summary"
    );

    if trapped > 0 || failed > 0 {
        return Err(anyhow!(
            "MapReduce job '{}' failed during Map phase (completed={}, trapped={}, failed={})",
            job_id,
            completed,
            trapped,
            failed
        ));
    }

    if config.mode == CoordinatorMode::KvMapReduce {
        let shuffled_lines: Vec<String> = map_scheduler
            .collect_output_chunks_in_order()
            .into_iter()
            .filter_map(|b| String::from_utf8(b).ok())
            .filter(|s| !s.trim().is_empty())
            .collect();
        let shuffled_input = shuffled_lines.join("\n");

        info!(
            intermediate_chunks = shuffled_lines.len(),
            shuffled_bytes = shuffled_input.len(),
            reduce_fn = %config.reduce_fn,
            "Stage 2: Dispatching shuffled Map outputs to Wasm Reduce phase"
        );

        let reduce_job_id = format!("{job_id}-reduce");
        let mut reduce_scheduler = JobScheduler::new(
            reduce_job_id.clone(),
            JobPhase::ReduceBytes,
            config.reduce_fn.clone(),
            wasm_binary,
            wasm_sha256,
            vec![shuffled_input],
            keypair,
        );

        run_scheduler_phase(&mut swarm, &mut reduce_scheduler, &config, &registry).await?;
        let (_, r_completed, r_trapped, r_failed, r_fuel, r_dur, _) = reduce_scheduler.summarize();
        if r_trapped > 0 || r_failed > 0 || r_completed == 0 {
            return Err(anyhow!(
                "Reduce phase '{}' failed (completed={}, trapped={}, failed={})",
                reduce_job_id,
                r_completed,
                r_trapped,
                r_failed
            ));
        }

        let reduced_outputs = reduce_scheduler.collect_output_chunks_in_order();
        let final_reduced = reduced_outputs
            .first()
            .map(|b| String::from_utf8_lossy(b).into_owned())
            .unwrap_or_else(|| "{}".to_string());

        info!(
            reduce_job_id = %reduce_job_id,
            reduce_fuel = r_fuel,
            reduce_duration_us = r_dur,
            reduced_output = %final_reduced,
            "Final Two-Phase MapReduce Reduced Output: {}",
            final_reduced
        );
    } else {
        info!(
            job_id = %job_id,
            final_unified_sum = unified_sum,
            "Final aggregated MapReduce word count sum: {}",
            unified_sum
        );
    }

    Ok(())
}
