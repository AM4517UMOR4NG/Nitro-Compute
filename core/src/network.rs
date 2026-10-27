use std::collections::hash_map::DefaultHasher;
use std::hash::{Hash, Hasher};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use anyhow::{anyhow, Context};
use libp2p::futures::StreamExt;
use libp2p::swarm::{NetworkBehaviour as NetworkBehavior, SwarmEvent};
use libp2p::{gossipsub, noise, tcp, yamux, Swarm, SwarmBuilder};
use serde::{Deserialize, Serialize};
use tracing::{debug, info, warn};

/// Kademlia DHT types re-exported for `NitroBehavior`.
pub mod kademlia {
    pub use libp2p::kad::store::MemoryStore;
    pub use libp2p::kad::*;

    /// Type alias for the Kademlia DHT network behaviour.
    pub type Kademlia<TStore = MemoryStore> = libp2p::kad::Behaviour<TStore>;
}

/// Topic 1: Gossipsub channel for the Coordinator to broadcast `TaskPayload` messages.
pub const TASK_ALLOC_TOPIC: &str = "task-alloc";

/// Topic 2: Gossipsub channel for Workers to broadcast back `ResultPayload` messages.
pub const TASK_RESULTS_TOPIC: &str = "task-results";

/// Default Wasmtime fuel budget allocated per MapReduce task chunk.
pub const DEFAULT_FUEL_LIMIT: u64 = 10_000_000;

/// Wire-protocol payload broadcast by the Coordinator on `"task-alloc"`.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TaskPayload {
    pub task_id: String,
    pub wasm_binary: Vec<u8>,
    pub data_chunk: String,
    pub fuel: u64,
}

impl TaskPayload {
    /// Constructs a new `TaskPayload`.
    pub fn new(
        task_id: impl Into<String>,
        wasm_binary: Vec<u8>,
        data_chunk: impl Into<String>,
        fuel: u64,
    ) -> Self {
        Self {
            task_id: task_id.into(),
            wasm_binary,
            data_chunk: data_chunk.into(),
            fuel,
        }
    }

    /// Serializes `TaskPayload` to a JSON byte vector for Gossipsub transmission.
    pub fn to_bytes(&self) -> anyhow::Result<Vec<u8>> {
        serde_json::to_vec(self).context("Failed to serialize TaskPayload")
    }

    /// Deserializes `TaskPayload` from a raw Gossipsub message slice.
    pub fn from_bytes(bytes: &[u8]) -> anyhow::Result<Self> {
        serde_json::from_slice::<Self>(bytes).context("Failed to deserialize TaskPayload")
    }
}

/// Wire-protocol payload broadcast by Workers on `"task-results"`.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ResultPayload {
    pub task_id: String,
    pub worker_peer_id: String,
    pub computed_value: usize,
}

impl ResultPayload {
    /// Constructs a new `ResultPayload`.
    pub fn new(
        task_id: impl Into<String>,
        worker_peer_id: impl Into<String>,
        computed_value: usize,
    ) -> Self {
        Self {
            task_id: task_id.into(),
            worker_peer_id: worker_peer_id.into(),
            computed_value,
        }
    }

    /// Serializes `ResultPayload` to a JSON byte vector for Gossipsub transmission.
    pub fn to_bytes(&self) -> anyhow::Result<Vec<u8>> {
        serde_json::to_vec(self).context("Failed to serialize ResultPayload")
    }

    /// Deserializes `ResultPayload` from a raw Gossipsub message slice.
    pub fn from_bytes(bytes: &[u8]) -> anyhow::Result<Self> {
        serde_json::from_slice::<Self>(bytes).context("Failed to deserialize ResultPayload")
    }
}

/// Custom P2P network behavior combining Kademlia DHT routing and Gossipsub broadcasting.
#[derive(NetworkBehavior)]
pub struct NitroBehavior {
    pub kademlia: kademlia::Kademlia<kademlia::MemoryStore>,
    pub gossipsub: gossipsub::Behaviour,
}

/// Builds and configures a `libp2p::Swarm<NitroBehavior>` over TCP with Noise encryption and Yamux multiplexing,
/// subscribed to both `"task-alloc"` and `"task-results"` Gossipsub channels.
pub async fn create_swarm() -> anyhow::Result<Swarm<NitroBehavior>> {
    let swarm = SwarmBuilder::with_new_identity()
        .with_tokio()
        .with_tcp(
            tcp::Config::default(),
            noise::Config::new,
            yamux::Config::default,
        )
        .context("Failed to configure TCP transport with Noise and Yamux")?
        .with_behaviour(|keypair| {
            let message_id_fn = |message: &gossipsub::Message| {
                let mut hasher = DefaultHasher::new();
                message.topic.hash(&mut hasher);
                message.data.hash(&mut hasher);
                if let Some(peer) = &message.source {
                    peer.hash(&mut hasher);
                }
                let nanos = SystemTime::now()
                    .duration_since(UNIX_EPOCH)
                    .map(|d| d.as_nanos())
                    .unwrap_or(0);
                nanos.hash(&mut hasher);
                gossipsub::MessageId::from(hasher.finish().to_string())
            };

            let gossipsub_config = gossipsub::ConfigBuilder::default()
                .heartbeat_interval(Duration::from_secs(1))
                .validation_mode(gossipsub::ValidationMode::Strict)
                .message_id_fn(message_id_fn)
                .build()
                .map_err(|e| std::io::Error::new(std::io::ErrorKind::Other, e))?;

            let mut gossipsub = gossipsub::Behaviour::new(
                gossipsub::MessageAuthenticity::Signed(keypair.clone()),
                gossipsub_config,
            )
            .map_err(|e| std::io::Error::new(std::io::ErrorKind::Other, e))?;

            // Topic 1: "task-alloc"
            let task_alloc_topic = gossipsub::IdentTopic::new(TASK_ALLOC_TOPIC);
            gossipsub
                .subscribe(&task_alloc_topic)
                .map_err(|e| std::io::Error::new(std::io::ErrorKind::Other, e))?;

            // Topic 2: "task-results"
            let task_results_topic = gossipsub::IdentTopic::new(TASK_RESULTS_TOPIC);
            gossipsub
                .subscribe(&task_results_topic)
                .map_err(|e| std::io::Error::new(std::io::ErrorKind::Other, e))?;

            let peer_id = keypair.public().to_peer_id();
            let store = kademlia::MemoryStore::new(peer_id);
            let kademlia = kademlia::Kademlia::new(peer_id, store);

            Ok(NitroBehavior {
                kademlia,
                gossipsub,
            })
        })
        .map_err(|e| anyhow!("Failed to initialize NitroBehavior in SwarmBuilder: {e}"))?
        .with_swarm_config(|cfg| cfg.with_idle_connection_timeout(Duration::from_secs(60)))
        .build();

    info!(
        local_peer_id = %swarm.local_peer_id(),
        alloc_topic = TASK_ALLOC_TOPIC,
        results_topic = TASK_RESULTS_TOPIC,
        "Initialized libp2p Swarm subscribed to bidirectional Gossipsub topics"
    );

    Ok(swarm)
}

/// Publishes a serialized `TaskPayload` onto the `"task-alloc"` Gossipsub topic.
pub fn publish_task_payload(
    swarm: &mut Swarm<NitroBehavior>,
    payload: &TaskPayload,
) -> anyhow::Result<Option<gossipsub::MessageId>> {
    let topic = gossipsub::IdentTopic::new(TASK_ALLOC_TOPIC);
    let wire_bytes = payload.to_bytes()?;

    match swarm.behaviour_mut().gossipsub.publish(topic, wire_bytes) {
        Ok(msg_id) => Ok(Some(msg_id)),
        Err(gossipsub::PublishError::InsufficientPeers) => {
            warn!(
                task_id = %payload.task_id,
                topic = TASK_ALLOC_TOPIC,
                "No mesh peers subscribed yet (InsufficientPeers) while publishing TaskPayload"
            );
            Ok(None)
        }
        Err(err) => Err(anyhow!(
            "Failed to publish TaskPayload '{}' to '{}': {}",
            payload.task_id,
            TASK_ALLOC_TOPIC,
            err
        )),
    }
}

/// Publishes a serialized `ResultPayload` onto the `"task-results"` Gossipsub topic.
pub fn publish_result_payload(
    swarm: &mut Swarm<NitroBehavior>,
    payload: &ResultPayload,
) -> anyhow::Result<Option<gossipsub::MessageId>> {
    let topic = gossipsub::IdentTopic::new(TASK_RESULTS_TOPIC);
    let wire_bytes = payload.to_bytes()?;

    match swarm.behaviour_mut().gossipsub.publish(topic, wire_bytes) {
        Ok(msg_id) => Ok(Some(msg_id)),
        Err(gossipsub::PublishError::InsufficientPeers) => {
            warn!(
                task_id = %payload.task_id,
                topic = TASK_RESULTS_TOPIC,
                "No mesh peers subscribed yet (InsufficientPeers) while publishing ResultPayload"
            );
            Ok(None)
        }
        Err(err) => Err(anyhow!(
            "Failed to publish ResultPayload for task '{}' to '{}': {}",
            payload.task_id,
            TASK_RESULTS_TOPIC,
            err
        )),
    }
}

/// Continuously polls incoming wire network events from the `Swarm`, intercepts Gossipsub messages
/// bound to `"task-alloc"`, and passes `&mut Swarm<NitroBehavior>` alongside the raw payload to `on_task_alloc`.
pub async fn run_event_loop_with_handler<F>(
    swarm: &mut Swarm<NitroBehavior>,
    mut on_task_alloc: F,
) -> anyhow::Result<()>
where
    F: FnMut(&mut Swarm<NitroBehavior>, Vec<u8>) -> anyhow::Result<()>,
{
    let task_alloc_topic_hash = gossipsub::IdentTopic::new(TASK_ALLOC_TOPIC).hash();

    loop {
        tokio::select! {
            event = swarm.select_next_some() => {
                match event {
                    SwarmEvent::NewListenAddr { address, .. } => {
                        info!(%address, "Swarm listening on local multiaddr");
                    }
                    SwarmEvent::Behaviour(NitroBehaviorEvent::Gossipsub(
                        gossipsub::Event::Message {
                            propagation_source,
                            message_id,
                            message,
                        },
                    )) => {
                        if message.topic == task_alloc_topic_hash
                            || message.topic.as_str() == TASK_ALLOC_TOPIC
                        {
                            info!(
                                %propagation_source,
                                %message_id,
                                payload_bytes = message.data.len(),
                                topic = TASK_ALLOC_TOPIC,
                                "Intercepted Gossipsub task allocation message"
                            );
                            let payload: Vec<u8> = message.data;
                            if let Err(err) = on_task_alloc(swarm, payload) {
                                warn!(
                                    topic = TASK_ALLOC_TOPIC,
                                    error = %format!("{:#}", err),
                                    "Failed to process intercepted task payload"
                                );
                            }
                        }
                    }
                    SwarmEvent::Behaviour(NitroBehaviorEvent::Kademlia(kad_event)) => {
                        debug!(?kad_event, "Received Kademlia DHT event");
                    }
                    SwarmEvent::ConnectionEstablished { peer_id, endpoint, .. } => {
                        info!(%peer_id, ?endpoint, "Established P2P connection with peer");
                        swarm
                            .behaviour_mut()
                            .gossipsub
                            .add_explicit_peer(&peer_id);
                        swarm
                            .behaviour_mut()
                            .kademlia
                            .add_address(&peer_id, endpoint.get_remote_address().clone());
                    }
                    SwarmEvent::OutgoingConnectionError { peer_id, error, .. } => {
                        warn!(?peer_id, %error, "Outgoing P2P connection error");
                    }
                    SwarmEvent::IncomingConnectionError { error, .. } => {
                        warn!(%error, "Incoming P2P connection error");
                    }
                    _ => {}
                }
            }
        }
    }
}

/// Continuously polls incoming wire network events and extracts payloads arriving on the `"task-alloc"` Gossipsub topic.
pub async fn run_event_loop(swarm: &mut Swarm<NitroBehavior>) -> anyhow::Result<()> {
    run_event_loop_with_handler(swarm, |_swarm, payload| {
        info!(
            payload_len = payload.len(),
            "Extracted raw payload from 'task-alloc' topic"
        );
        Ok(())
    })
    .await
}
