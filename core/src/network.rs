use libp2p::gossipsub;
use libp2p::swarm::NetworkBehaviour as NetworkBehavior;

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
