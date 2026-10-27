use std::collections::HashSet;
use std::path::{Path, PathBuf};

use anyhow::{anyhow, Context};
use core::network::{
    create_swarm, publish_task_payload, NitroBehavior, TaskPayload, DEFAULT_FUEL_LIMIT,
    TASK_ALLOC_TOPIC,
};
use libp2p::{Multiaddr, Swarm};
use tokio::fs::File;
use tokio::io::{AsyncBufReadExt, BufReader};
use tracing::info;

/// Candidate paths for the compiled `jobs/word-count` WebAssembly binary.
const DEFAULT_WASM_PATHS: &[&str] = &[
    "target/wasm32-wasi/release/word_count.wasm",
    "target/wasm32-wasi/debug/word_count.wasm",
    "jobs/word-count/target/wasm32-wasi/release/word_count.wasm",
    "jobs/word-count/target/wasm32-wasi/debug/word_count.wasm",
    "target/wasm32-wasip1/release/word_count.wasm",
    "target/wasm32-wasip1/debug/word_count.wasm",
];

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
        "Compiled word-count .wasm binary not found in standard target paths ({:?}).",
        DEFAULT_WASM_PATHS
    ))
}

/// Ingests a raw text stream pipeline file and fragments it into data chunks.
async fn ingest_and_fragment_stream(
    input_path: &Path,
    lines_per_chunk: usize,
) -> anyhow::Result<Vec<String>> {
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

    Ok(chunks)
}

/// Wraps each data chunk into a `TaskPayload` and broadcasts it onto `"task-alloc"`.
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
            message_id = ?maybe_msg_id,
            topic = TASK_ALLOC_TOPIC,
            "Broadcasted TaskPayload to Gossipsub mesh"
        );
        pending_tasks.insert(task_id);
    }

    Ok(pending_tasks)
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    core::init_logging();
    let wasm_binary = load_wasm_binary(None).await?;
    let chunks = ingest_and_fragment_stream(&PathBuf::from("input.txt"), 1).await?;
    let mut swarm = create_swarm().await?;
    let listen_addr: Multiaddr = "/ip4/0.0.0.0/tcp/0".parse()?;
    swarm.listen_on(listen_addr)?;
    let _pending = broadcast_task_payloads(&mut swarm, &wasm_binary, &chunks, DEFAULT_FUEL_LIMIT)?;
    Ok(())
}
