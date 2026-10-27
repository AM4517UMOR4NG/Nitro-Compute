use std::collections::hash_map::DefaultHasher;
use std::hash::{Hash, Hasher};
use std::time::Duration;

use anyhow::{anyhow, Context};
use libp2p::futures::StreamExt;
use libp2p::swarm::{NetworkBehaviour as NetworkBehavior, SwarmEvent};
use libp2p::{gossipsub, noise, tcp, yamux, Swarm, SwarmBuilder};

/// Kademlia DHT types re-exported for `NitroBehavior`.
pub mod kademlia {
    pub use libp2p::kad::store::MemoryStore;
    pub use libp2p::kad::*;

    /// Type alias for the Kademlia DHT network behaviour.
    pub type Kademlia<TStore = MemoryStore> = libp2p::kad::Behaviour<TStore>;
}

/// Gossipsub topic used for broadcasting MapReduce task allocations.
pub const TASK_ALLOC_TOPIC: &str = "task-alloc";

/// Custom P2P network behavior combining Kademlia DHT routing and Gossipsub broadcasting.
#[derive(NetworkBehavior)]
pub struct NitroBehavior {
    pub kademlia: kademlia::Kademlia<kademlia::MemoryStore>,
    pub gossipsub: gossipsub::Behaviour,
}

/// Builds and configures a `libp2p::Swarm<NitroBehavior>` over TCP with Noise encryption and Yamux multiplexing.
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
                message.data.hash(&mut hasher);
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

            let task_topic = gossipsub::IdentTopic::new(TASK_ALLOC_TOPIC);
            gossipsub
                .subscribe(&task_topic)
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

    Ok(swarm)
}

/// Continuously polls incoming wire network events and extracts payloads arriving on the `"task-alloc"` Gossipsub topic.
pub async fn run_event_loop(swarm: &mut Swarm<NitroBehavior>) -> anyhow::Result<()> {
    let task_alloc_topic_hash = gossipsub::IdentTopic::new(TASK_ALLOC_TOPIC).hash();

    loop {
        tokio::select! {
            event = swarm.select_next_some() => {
                if let SwarmEvent::Behaviour(NitroBehaviorEvent::Gossipsub(
                    gossipsub::Event::Message { message, .. },
                )) = event {
                    if message.topic == task_alloc_topic_hash
                        || message.topic.as_str() == TASK_ALLOC_TOPIC
                    {
                        let _payload: Vec<u8> = message.data;
                    }
                }
            }
        }
    }
}
