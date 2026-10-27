use std::collections::HashSet;
use std::path::{Path, PathBuf};
use std::time::Duration;

use anyhow::{anyhow, Context};
use core::network::{
    create_swarm, publish_task_payload, NitroBehavior, NitroBehaviorEvent, ResultPayload,
    TaskPayload, DEFAULT_FUEL_LIMIT, TASK_ALLOC_TOPIC, TASK_RESULTS_TOPIC,
};
use libp2p::futures::StreamExt;
use libp2p::swarm::SwarmEvent;
use libp2p::{gossipsub, Multiaddr, Swarm};
use tokio::fs::File;
use tokio::io::{AsyncBufReadExt, BufReader};
use tracing::{debug, info, warn};

/// Candidate paths for the compiled `jobs/word-count` WebAssembly binary.
const DEFAULT_WASM_PATHS: &[&str] = &[
    "target/wasm32-wasi/release/word_count.wasm",
    "target/wasm32-wasi/debug/word_count.wasm",
    "jobs/word-count/target/wasm32-wasi/release/word_count.wasm",
    "jobs/word-count/target/wasm32-wasi/debug/word_count.wasm",
    "target/wasm32-wasip1/release/word_count.wasm",
    "target/wasm32-wasip1/debug/word_count.wasm",
];

/// CLI configuration for the coordinator network entry oracle.
struct CoordinatorConfig {
    input_file: PathBuf,
    wasm_file: Option<PathBuf>,
    listen_addr: Multiaddr,
    peer_addrs: Vec<Multiaddr>,
    lines_per_chunk: usize,
    fuel_limit: u64,
    result_timeout_secs: u64,
}

impl CoordinatorConfig {
    fn from_args() -> anyhow::Result<Self> {
        let mut input_file: Option<PathBuf> = None;
        let mut wasm_file: Option<PathBuf> = None;
        let mut listen_addr: Multiaddr = "/ip4/0.0.0.0/tcp/0"
            .parse()
            .context("Invalid default listen Multiaddr")?;
        let mut peer_addrs: Vec<Multiaddr> = Vec::new();
        let mut lines_per_chunk: usize = 1;
        let mut fuel_limit: u64 = DEFAULT_FUEL_LIMIT;
        let mut result_timeout_secs: u64 = 30;

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
                "--timeout" | "-t" => {
                    if let Some(val) = args.next() {
                        result_timeout_secs = val
                            .parse::<u64>()
                            .context("Invalid --timeout value in seconds")?;
                    }
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
            listen_addr,
            peer_addrs,
            lines_per_chunk,
            fuel_limit,
            result_timeout_secs,
        })
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
            return tokio::fs::read(path)
                .await
                .with_context(|| format!("Failed to read discovered Wasm binary at {}", path.display()));
        }
    }

    Err(anyhow!(
        "Compiled word-count .wasm binary not found in standard target paths ({:?}). Build `jobs/word-count` with `--target wasm32-wasi` or pass `--wasm <path>`.",
        DEFAULT_WASM_PATHS
    ))
}

/// Ingests a raw text stream pipeline file and fragments it into data chunks.
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

/// Wraps each data chunk into a `TaskPayload`, broadcasts it onto `"task-alloc"`,
/// and returns the set of dispatched `task_id`s.
fn broadcast_task_payloads(
    swarm: &mut Swarm<NitroBehavior>,
    wasm_binary: &[u8],
    chunks: &[String],
    fuel_limit: u64,
) -> anyhow::Result<HashSet<String>> {
    let mut pending_tasks = HashSet::with_capacity(chunks.len());

    for (index, chunk_data) in chunks.iter().enumerate() {
        let task_id = format!("task-{index}");
        let payload = TaskPayload::new(
            task_id.clone(),
            wasm_binary.to_vec(),
            chunk_data.clone(),
            fuel_limit,
        );

        let maybe_msg_id = publish_task_payload(swarm, &payload)?;
        info!(
            task_id = %payload.task_id,
            fuel = payload.fuel,
            chunk_bytes = payload.data_chunk.len(),
            message_id = ?maybe_msg_id,
            topic = TASK_ALLOC_TOPIC,
            "Broadcasted TaskPayload to Gossipsub mesh"
        );

        pending_tasks.insert(task_id);
    }

    Ok(pending_tasks)
}

/// Subscribes to `"task-results"`, intercepts inbound `ResultPayload` responses from workers,
/// and aggregates their `computed_value` fields into a final unified sum.
async fn collect_and_aggregate_results(
    swarm: &mut Swarm<NitroBehavior>,
    mut pending_tasks: HashSet<String>,
    timeout: Duration,
) -> anyhow::Result<usize> {
    if pending_tasks.is_empty() {
        return Ok(0);
    }

    let results_topic = gossipsub::IdentTopic::new(TASK_RESULTS_TOPIC);
    swarm
        .behaviour_mut()
        .gossipsub
        .subscribe(&results_topic)
        .map_err(|e| anyhow!("Failed to subscribe coordinator to '{}': {e}", TASK_RESULTS_TOPIC))?;

    let results_topic_hash = results_topic.hash();
    let mut unified_sum: usize = 0;

    let deadline = tokio::time::sleep(timeout);
    tokio::pin!(deadline);

    loop {
        tokio::select! {
            _ = &mut deadline => {
                warn!(
                    remaining_tasks = pending_tasks.len(),
                    current_sum = unified_sum,
                    "Result collection window elapsed before all tasks reported back"
                );
                break;
            }
            event = swarm.select_next_some() => {
                match event {
                    SwarmEvent::NewListenAddr { address, .. } => {
                        info!(%address, "Coordinator listening on local multiaddr");
                    }
                    SwarmEvent::Behaviour(NitroBehaviorEvent::Gossipsub(
                        gossipsub::Event::Message {
                            propagation_source,
                            message_id,
                            message,
                        },
                    )) => {
                        if message.topic == results_topic_hash
                            || message.topic.as_str() == TASK_RESULTS_TOPIC
                        {
                            match ResultPayload::from_bytes(&message.data) {
                                Ok(result) => {
                                    if pending_tasks.remove(&result.task_id) {
                                        unified_sum = unified_sum.saturating_add(result.computed_value);
                                        info!(
                                            task_id = %result.task_id,
                                            worker_peer_id = %result.worker_peer_id,
                                            %propagation_source,
                                            %message_id,
                                            computed_value = result.computed_value,
                                            unified_sum,
                                            remaining_tasks = pending_tasks.len(),
                                            "Received worker ResultPayload and updated aggregate sum"
                                        );

                                        if pending_tasks.is_empty() {
                                            info!(
                                                unified_sum,
                                                "All dispatched MapReduce tasks completed and aggregated"
                                            );
                                            break;
                                        }
                                    } else {
                                        debug!(
                                            task_id = %result.task_id,
                                            worker_peer_id = %result.worker_peer_id,
                                            "Ignoring duplicate or unknown ResultPayload"
                                        );
                                    }
                                }
                                Err(err) => {
                                    warn!(
                                        error = %format!("{:#}", err),
                                        "Failed to decode ResultPayload on '{}'",
                                        TASK_RESULTS_TOPIC
                                    );
                                }
                            }
                        }
                    }
                    SwarmEvent::ConnectionEstablished { peer_id, endpoint, .. } => {
                        info!(%peer_id, ?endpoint, "Coordinator connected to worker peer");
                        swarm.behaviour_mut().gossipsub.add_explicit_peer(&peer_id);
                        swarm
                            .behaviour_mut()
                            .kademlia
                            .add_address(&peer_id, endpoint.get_remote_address().clone());
                    }
                    other => {
                        debug!(?other, "Coordinator swarm event while awaiting results");
                    }
                }
            }
        }
    }

    Ok(unified_sum)
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    core::init_logging();

    let config = CoordinatorConfig::from_args()?;
    info!(
        fuel_limit = config.fuel_limit,
        "Starting nitro-compute coordinator oracle..."
    );

    let wasm_binary = load_wasm_binary(config.wasm_file.as_deref()).await?;
    let chunks = ingest_and_fragment_stream(&config.input_file, config.lines_per_chunk).await?;

    let mut swarm = create_swarm()
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

    // Allow connections and Gossipsub topic subscriptions to settle when peers are dialed.
    if !config.peer_addrs.is_empty() {
        let settle_deadline = tokio::time::sleep(Duration::from_secs(2));
        tokio::pin!(settle_deadline);
        loop {
            tokio::select! {
                _ = &mut settle_deadline => break,
                event = swarm.select_next_some() => {
                    match event {
                        SwarmEvent::NewListenAddr { address, .. } => {
                            info!(%address, "Coordinator listening on local multiaddr");
                        }
                        SwarmEvent::ConnectionEstablished { peer_id, endpoint, .. } => {
                            info!(%peer_id, ?endpoint, "Coordinator connected to worker peer");
                            swarm.behaviour_mut().gossipsub.add_explicit_peer(&peer_id);
                            swarm
                                .behaviour_mut()
                                .kademlia
                                .add_address(&peer_id, endpoint.get_remote_address().clone());
                        }
                        SwarmEvent::Behaviour(NitroBehaviorEvent::Gossipsub(
                            gossipsub::Event::Subscribed { peer_id, topic },
                        )) => {
                            info!(%peer_id, %topic, "Peer subscribed to Gossipsub topic");
                        }
                        other => {
                            debug!(?other, "Coordinator swarm event during handshake");
                        }
                    }
                }
            }
        }
    }

    let pending_tasks =
        broadcast_task_payloads(&mut swarm, &wasm_binary, &chunks, config.fuel_limit)?;

    let final_sum = collect_and_aggregate_results(
        &mut swarm,
        pending_tasks,
        Duration::from_secs(config.result_timeout_secs),
    )
    .await?;

    info!(
        total_chunks = chunks.len(),
        final_unified_sum = final_sum,
        "Final aggregated MapReduce word count sum: {}",
        final_sum
    );

    Ok(())
}
