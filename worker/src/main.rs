use anyhow::Context;
use libp2p::Multiaddr;
use nitro_core::network::{
    create_swarm, publish_result_payload, run_event_loop_with_handler, ResultPayload, TaskPayload,
    TASK_ALLOC_TOPIC, TASK_RESULTS_TOPIC,
};
use nitro_core::runtime::NitroRuntime;
use tracing::{error, info};

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    nitro_core::init_logging();

    let mut listen_addr: Multiaddr = "/ip4/0.0.0.0/tcp/0"
        .parse()
        .context("Invalid default listen Multiaddr")?;
    let mut bootstrap_peers: Vec<Multiaddr> = Vec::new();

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
            other => {
                if let Ok(addr) = other.parse::<Multiaddr>() {
                    bootstrap_peers.push(addr);
                }
            }
        }
    }

    info!("Starting nitro-compute worker daemon...");
    let runtime = NitroRuntime::new();
    let mut swarm = create_swarm()
        .await
        .context("Failed to initialize worker libp2p Swarm")?;

    let worker_peer_id = swarm.local_peer_id().to_string();

    swarm
        .listen_on(listen_addr.clone())
        .with_context(|| format!("Worker failed to listen on {listen_addr}"))?;

    for peer_addr in bootstrap_peers {
        info!(%peer_addr, "Dialing mesh peer");
        if let Err(err) = swarm.dial(peer_addr.clone()) {
            error!(%peer_addr, error = %err, "Failed to dial bootstrap peer");
        }
    }

    info!(
        %worker_peer_id,
        alloc_topic = TASK_ALLOC_TOPIC,
        results_topic = TASK_RESULTS_TOPIC,
        "Worker daemon ready and listening for MapReduce TaskPayloads"
    );

    run_event_loop_with_handler(&mut swarm, |swarm_ref, raw_payload| {
        let task = TaskPayload::from_bytes(&raw_payload)
            .context("Failed to decode incoming 'task-alloc' TaskPayload")?;

        info!(
            task_id = %task.task_id,
            wasm_bytes = task.wasm_binary.len(),
            chunk_len = task.data_chunk.len(),
            fuel = task.fuel,
            "Executing fuel-metered Wasm 'map_line' for TaskPayload"
        );

        let computed_value = runtime
            .execute_map(&task.wasm_binary, &task.data_chunk, task.fuel)
            .with_context(|| {
                format!(
                    "NitroRuntime::execute_map failed for task_id='{}'",
                    task.task_id
                )
            })?;

        let result_payload =
            ResultPayload::new(task.task_id.clone(), worker_peer_id.clone(), computed_value);

        let maybe_msg_id =
            publish_result_payload(swarm_ref, &result_payload).with_context(|| {
                format!(
                    "Failed to publish ResultPayload for task_id='{}' onto '{}'",
                    task.task_id, TASK_RESULTS_TOPIC
                )
            })?;

        info!(
            task_id = %result_payload.task_id,
            worker_peer_id = %result_payload.worker_peer_id,
            computed_value = result_payload.computed_value,
            message_id = ?maybe_msg_id,
            topic = TASK_RESULTS_TOPIC,
            "Published ResultPayload back to coordinator mesh"
        );

        Ok(())
    })
    .await
}
