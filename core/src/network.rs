use std::collections::hash_map::DefaultHasher;
use std::hash::{Hash, Hasher};
use std::io::{Cursor, Read};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use anyhow::{anyhow, Context};
use libp2p::futures::StreamExt;
use libp2p::swarm::behaviour::toggle::Toggle;
#[allow(unused_imports)]
use libp2p::swarm::{NetworkBehaviour, NetworkBehaviour as NetworkBehavior, SwarmEvent};
use libp2p::{
    gossipsub, identify, mdns, noise, ping, tcp, yamux, Multiaddr, PeerId, Swarm, SwarmBuilder,
};
use serde::{Deserialize, Serialize};
use tracing::{debug, info, warn};

use crate::runtime::{sha256_hex, ExecutionMetrics, DEFAULT_MAX_MEMORY_BYTES};

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

/// Topic 3: Gossipsub channel for Workers to advertise capacity and cached Wasm artifacts.
pub const WORKER_TELEMETRY_TOPIC: &str = "worker-telemetry";

/// Default Wasmtime fuel budget allocated per MapReduce task chunk.
pub const DEFAULT_FUEL_LIMIT: u64 = 10_000_000;

/// Maximum Gossipsub message payload size (8 MiB) to accommodate compiled `.wasm` binaries.
pub const MAX_GOSSIPSUB_TRANSMIT_BYTES: usize = 8 * 1024 * 1024;

/// Maximum permitted decompressed wire payload size (16 MiB) to defend against decompression bombs.
pub const MAX_WIRE_PAYLOAD_BYTES: usize = 16 * 1024 * 1024;

/// Magic header prefix identifying Zstandard-compressed Nitro wire frames.
const ZSTD_FRAME_MAGIC: &[u8; 4] = b"NZST";

fn default_entry_fn() -> String {
    "map_line".to_string()
}

fn default_max_memory_bytes() -> usize {
    DEFAULT_MAX_MEMORY_BYTES
}

/// Compresses serialized JSON bytes with Zstandard when doing so reduces wire footprint.
pub fn encode_wire_frame<T: Serialize>(value: &T) -> anyhow::Result<Vec<u8>> {
    let raw = serde_json::to_vec(value).context("Failed to serialize wire payload")?;
    if raw.len() >= 128 {
        if let Ok(compressed) = zstd::bulk::compress(&raw, 3) {
            if compressed.len() + ZSTD_FRAME_MAGIC.len() < raw.len() {
                let mut frame = Vec::with_capacity(ZSTD_FRAME_MAGIC.len() + compressed.len());
                frame.extend_from_slice(ZSTD_FRAME_MAGIC);
                frame.extend_from_slice(&compressed);
                return Ok(frame);
            }
        }
    }
    Ok(raw)
}

/// Decodes a raw JSON or `NZST`-prefixed Zstandard-compressed wire frame with bounded memory expansion.
pub fn decode_wire_frame<T: for<'de> Deserialize<'de>>(bytes: &[u8]) -> anyhow::Result<T> {
    if bytes.starts_with(ZSTD_FRAME_MAGIC) {
        let cursor = Cursor::new(&bytes[ZSTD_FRAME_MAGIC.len()..]);
        let decoder = zstd::stream::read::Decoder::new(cursor)
            .context("Failed to initialize Zstd wire decoder")?;
        let mut decompressed = Vec::new();
        let mut bounded = decoder.take((MAX_WIRE_PAYLOAD_BYTES + 1) as u64);
        bounded
            .read_to_end(&mut decompressed)
            .context("Failed to decompress Zstd wire frame")?;

        if decompressed.len() > MAX_WIRE_PAYLOAD_BYTES {
            return Err(anyhow!(
                "Decompressed wire payload exceeded maximum allowed size of {} bytes",
                MAX_WIRE_PAYLOAD_BYTES
            ));
        }

        return serde_json::from_slice::<T>(&decompressed)
            .context("Failed to deserialize decompressed wire payload");
    }

    if bytes.len() > MAX_WIRE_PAYLOAD_BYTES {
        return Err(anyhow!(
            "Raw wire payload exceeded maximum allowed size of {} bytes",
            MAX_WIRE_PAYLOAD_BYTES
        ));
    }

    serde_json::from_slice::<T>(bytes).context("Failed to deserialize raw wire payload")
}

/// Wire-protocol payload broadcast by the Coordinator on `"task-alloc"`.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TaskPayload {
    pub task_id: String,
    #[serde(default)]
    pub job_id: String,
    #[serde(default = "default_entry_fn")]
    pub entry_fn: String,
    #[serde(default)]
    pub wasm_binary: Vec<u8>,
    #[serde(default)]
    pub wasm_sha256: String,
    pub data_chunk: String,
    pub fuel: u64,
    #[serde(default = "default_max_memory_bytes")]
    pub max_memory_bytes: usize,
    #[serde(default)]
    pub attempt: u32,
}

impl TaskPayload {
    /// Constructs a new `TaskPayload` with default job metadata and computed SHA-256 digest.
    pub fn new(
        task_id: impl Into<String>,
        wasm_binary: Vec<u8>,
        data_chunk: impl Into<String>,
        fuel: u64,
    ) -> Self {
        let wasm_sha256 = if wasm_binary.is_empty() {
            String::new()
        } else {
            sha256_hex(&wasm_binary)
        };
        Self {
            task_id: task_id.into(),
            job_id: "default".to_string(),
            entry_fn: default_entry_fn(),
            wasm_binary,
            wasm_sha256,
            data_chunk: data_chunk.into(),
            fuel,
            max_memory_bytes: DEFAULT_MAX_MEMORY_BYTES,
            attempt: 1,
        }
    }

    /// Constructs a fully parameterized `TaskPayload` for fault-tolerant scheduling and sandbox tuning.
    #[allow(clippy::too_many_arguments)]
    pub fn with_options(
        task_id: impl Into<String>,
        job_id: impl Into<String>,
        entry_fn: impl Into<String>,
        wasm_binary: Vec<u8>,
        wasm_sha256: impl Into<String>,
        data_chunk: impl Into<String>,
        fuel: u64,
        max_memory_bytes: usize,
        attempt: u32,
    ) -> Self {
        let sha = wasm_sha256.into();
        let resolved_sha = if sha.is_empty() && !wasm_binary.is_empty() {
            sha256_hex(&wasm_binary)
        } else {
            sha
        };
        Self {
            task_id: task_id.into(),
            job_id: job_id.into(),
            entry_fn: entry_fn.into(),
            wasm_binary,
            wasm_sha256: resolved_sha,
            data_chunk: data_chunk.into(),
            fuel,
            max_memory_bytes,
            attempt,
        }
    }

    /// Verifies that embedded `wasm_binary` matches `wasm_sha256` when both are provided.
    pub fn verify_wasm_integrity(&self) -> anyhow::Result<()> {
        if !self.wasm_binary.is_empty() && !self.wasm_sha256.is_empty() {
            let actual = sha256_hex(&self.wasm_binary);
            if actual != self.wasm_sha256 {
                return Err(anyhow!(
                    "Wasm integrity check failed for task '{}': expected sha256 '{}', got '{}'",
                    self.task_id,
                    self.wasm_sha256,
                    actual
                ));
            }
        }
        Ok(())
    }

    /// Serializes `TaskPayload` into a compact wire frame (Zstd-compressed when smaller).
    pub fn to_bytes(&self) -> anyhow::Result<Vec<u8>> {
        encode_wire_frame(self).context("Failed to encode TaskPayload")
    }

    /// Deserializes `TaskPayload` from a raw or Zstd-compressed Gossipsub message slice.
    pub fn from_bytes(bytes: &[u8]) -> anyhow::Result<Self> {
        decode_wire_frame(bytes).context("Failed to decode TaskPayload")
    }
}

/// Execution status returned by Workers inside `ResultPayload`.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, Default)]
pub enum TaskOutcome {
    #[default]
    Success,
    SecurityTrap {
        kind: String,
        reason: String,
    },
    ExecutionError {
        reason: String,
    },
}

/// Wire-protocol payload broadcast by Workers on `"task-results"`.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ResultPayload {
    pub task_id: String,
    #[serde(default)]
    pub job_id: String,
    pub worker_peer_id: String,
    pub computed_value: usize,
    #[serde(default)]
    pub status: TaskOutcome,
    #[serde(default)]
    pub fuel_consumed: u64,
    #[serde(default)]
    pub duration_us: u64,
    #[serde(default)]
    pub cache_hit: bool,
    #[serde(default)]
    pub attempt: u32,
}

impl ResultPayload {
    /// Constructs a successful `ResultPayload` preserving backward-compatible field defaults.
    pub fn new(
        task_id: impl Into<String>,
        worker_peer_id: impl Into<String>,
        computed_value: usize,
    ) -> Self {
        Self {
            task_id: task_id.into(),
            job_id: "default".to_string(),
            worker_peer_id: worker_peer_id.into(),
            computed_value,
            status: TaskOutcome::Success,
            fuel_consumed: 0,
            duration_us: 0,
            cache_hit: false,
            attempt: 1,
        }
    }

    /// Constructs a `ResultPayload` populated with sandbox execution telemetry.
    pub fn from_metrics(
        task_id: impl Into<String>,
        job_id: impl Into<String>,
        worker_peer_id: impl Into<String>,
        attempt: u32,
        metrics: &ExecutionMetrics,
    ) -> Self {
        Self {
            task_id: task_id.into(),
            job_id: job_id.into(),
            worker_peer_id: worker_peer_id.into(),
            computed_value: metrics.computed_value,
            status: TaskOutcome::Success,
            fuel_consumed: metrics.fuel_consumed,
            duration_us: metrics.duration_us,
            cache_hit: metrics.cache_hit,
            attempt,
        }
    }

    /// Constructs a failure `ResultPayload` reporting a security trap or execution error to the Coordinator.
    pub fn failure(
        task_id: impl Into<String>,
        job_id: impl Into<String>,
        worker_peer_id: impl Into<String>,
        attempt: u32,
        status: TaskOutcome,
    ) -> Self {
        Self {
            task_id: task_id.into(),
            job_id: job_id.into(),
            worker_peer_id: worker_peer_id.into(),
            computed_value: 0,
            status,
            fuel_consumed: 0,
            duration_us: 0,
            cache_hit: false,
            attempt,
        }
    }

    /// Serializes `ResultPayload` into a wire frame for Gossipsub transmission.
    pub fn to_bytes(&self) -> anyhow::Result<Vec<u8>> {
        encode_wire_frame(self).context("Failed to encode ResultPayload")
    }

    /// Deserializes `ResultPayload` from a raw or Zstd-compressed Gossipsub message slice.
    pub fn from_bytes(bytes: &[u8]) -> anyhow::Result<Self> {
        decode_wire_frame(bytes).context("Failed to decode ResultPayload")
    }
}

/// Telemetry heartbeat broadcast by Worker nodes on `"worker-telemetry"`.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct WorkerTelemetry {
    pub worker_peer_id: String,
    pub active_tasks: usize,
    pub completed_tasks: u64,
    pub trapped_tasks: u64,
    pub cached_modules: usize,
    pub uptime_secs: u64,
}

impl WorkerTelemetry {
    /// Serializes `WorkerTelemetry` into a wire frame.
    pub fn to_bytes(&self) -> anyhow::Result<Vec<u8>> {
        encode_wire_frame(self).context("Failed to encode WorkerTelemetry")
    }

    /// Deserializes `WorkerTelemetry` from a raw or Zstd-compressed Gossipsub message slice.
    pub fn from_bytes(bytes: &[u8]) -> anyhow::Result<Self> {
        decode_wire_frame(bytes).context("Failed to decode WorkerTelemetry")
    }
}

/// Custom P2P network behavior combining Kademlia DHT routing, Gossipsub pub/sub,
/// optional LAN mDNS discovery, Identify peer metadata exchange, and Ping RTT probing.
#[derive(NetworkBehavior)]
pub struct NitroBehavior {
    pub kademlia: kademlia::Kademlia<kademlia::MemoryStore>,
    pub gossipsub: gossipsub::Behaviour,
    pub mdns: Toggle<mdns::tokio::Behaviour>,
    pub identify: identify::Behaviour,
    pub ping: ping::Behaviour,
}

/// Builds and configures a `libp2p::Swarm<NitroBehavior>` over TCP with Noise encryption and Yamux multiplexing,
/// enabling LAN mDNS auto-discovery when supported by the host network interface.
pub async fn create_swarm() -> anyhow::Result<Swarm<NitroBehavior>> {
    create_swarm_with_mdns(true).await
}

/// Builds and configures a `libp2p::Swarm<NitroBehavior>` with configurable LAN mDNS auto-discovery.
pub async fn create_swarm_with_mdns(enable_mdns: bool) -> anyhow::Result<Swarm<NitroBehavior>> {
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
                .max_transmit_size(MAX_GOSSIPSUB_TRANSMIT_BYTES)
                .message_id_fn(message_id_fn)
                .build()
                .map_err(std::io::Error::other)?;

            let mut gossipsub = gossipsub::Behaviour::new(
                gossipsub::MessageAuthenticity::Signed(keypair.clone()),
                gossipsub_config,
            )
            .map_err(std::io::Error::other)?;

            for topic_name in [TASK_ALLOC_TOPIC, TASK_RESULTS_TOPIC, WORKER_TELEMETRY_TOPIC] {
                let topic = gossipsub::IdentTopic::new(topic_name);
                gossipsub.subscribe(&topic).map_err(std::io::Error::other)?;
            }

            let local_public_key = keypair.public();
            let peer_id = local_public_key.to_peer_id();
            let store = kademlia::MemoryStore::new(peer_id);
            let kademlia = kademlia::Kademlia::new(peer_id, store);

            let mdns_behaviour = if enable_mdns {
                match mdns::tokio::Behaviour::new(mdns::Config::default(), peer_id) {
                    Ok(behaviour) => Some(behaviour),
                    Err(err) => {
                        warn!(error = %err, "Failed to bind mDNS multicast socket; continuing without LAN mDNS discovery");
                        None
                    }
                }
            } else {
                None
            };

            let identify = identify::Behaviour::new(identify::Config::new(
                "/nitro-compute/id/1.0.0".to_string(),
                local_public_key,
            ));
            let ping = ping::Behaviour::default();

            Ok(NitroBehavior {
                kademlia,
                gossipsub,
                mdns: Toggle::from(mdns_behaviour),
                identify,
                ping,
            })
        })
        .map_err(|e| anyhow!("Failed to initialize NitroBehavior in SwarmBuilder: {e}"))?
        .with_swarm_config(|cfg| cfg.with_idle_connection_timeout(Duration::from_secs(60)))
        .build();

    info!(
        local_peer_id = %swarm.local_peer_id(),
        mdns_enabled = swarm.behaviour().mdns.is_enabled(),
        alloc_topic = TASK_ALLOC_TOPIC,
        results_topic = TASK_RESULTS_TOPIC,
        telemetry_topic = WORKER_TELEMETRY_TOPIC,
        max_transmit_bytes = MAX_GOSSIPSUB_TRANSMIT_BYTES,
        "Initialized libp2p Swarm (TCP + Noise + Yamux + Kademlia + Gossipsub + mDNS + Identify + Ping)"
    );

    Ok(swarm)
}

/// Registers a peer and its reachable address in both Gossipsub and Kademlia routing tables.
pub fn register_peer_address(swarm: &mut Swarm<NitroBehavior>, peer_id: &PeerId, addr: Multiaddr) {
    swarm.behaviour_mut().gossipsub.add_explicit_peer(peer_id);
    swarm.behaviour_mut().kademlia.add_address(peer_id, addr);
}

/// Handles transport, mDNS, Identify, Ping, and Kademlia swarm maintenance events uniformly,
/// returning `Some(gossipsub::Event)` when a Gossipsub event requires higher-level handling.
pub fn process_swarm_maintenance(
    swarm: &mut Swarm<NitroBehavior>,
    event: SwarmEvent<NitroBehaviorEvent>,
) -> Option<gossipsub::Event> {
    match event {
        SwarmEvent::NewListenAddr { address, .. } => {
            info!(%address, "Swarm listening on local multiaddr");
            None
        }
        SwarmEvent::ConnectionEstablished {
            peer_id, endpoint, ..
        } => {
            info!(%peer_id, ?endpoint, "Established encrypted P2P connection with peer");
            register_peer_address(swarm, &peer_id, endpoint.get_remote_address().clone());
            None
        }
        SwarmEvent::ConnectionClosed { peer_id, cause, .. } => {
            debug!(%peer_id, ?cause, "Closed P2P connection with peer");
            None
        }
        SwarmEvent::OutgoingConnectionError { peer_id, error, .. } => {
            warn!(?peer_id, %error, "Outgoing P2P connection error");
            None
        }
        SwarmEvent::IncomingConnectionError { error, .. } => {
            warn!(%error, "Incoming P2P connection error");
            None
        }
        SwarmEvent::Behaviour(NitroBehaviorEvent::Gossipsub(gs_event)) => Some(gs_event),
        SwarmEvent::Behaviour(NitroBehaviorEvent::Mdns(mdns::Event::Discovered(peers))) => {
            for (peer_id, addr) in peers {
                if peer_id == *swarm.local_peer_id() {
                    continue;
                }
                info!(%peer_id, %addr, "mDNS discovered local Nitro Compute peer");
                register_peer_address(swarm, &peer_id, addr.clone());
                if !swarm.is_connected(&peer_id) {
                    if let Err(err) = swarm.dial(addr) {
                        debug!(%peer_id, error = %err, "Skipping immediate mDNS dial");
                    }
                }
            }
            None
        }
        SwarmEvent::Behaviour(NitroBehaviorEvent::Mdns(mdns::Event::Expired(peers))) => {
            for (peer_id, addr) in peers {
                debug!(%peer_id, %addr, "mDNS peer announcement expired");
            }
            None
        }
        SwarmEvent::Behaviour(NitroBehaviorEvent::Identify(identify::Event::Received {
            peer_id,
            info,
        })) => {
            debug!(
                %peer_id,
                agent_version = %info.agent_version,
                listen_addrs = ?info.listen_addrs,
                "Identified remote P2P node"
            );
            for addr in info.listen_addrs {
                register_peer_address(swarm, &peer_id, addr);
            }
            None
        }
        SwarmEvent::Behaviour(NitroBehaviorEvent::Ping(ping::Event { peer, result, .. })) => {
            match result {
                Ok(rtt) => debug!(%peer, rtt_ms = rtt.as_secs_f64() * 1000.0, "Peer Ping RTT"),
                Err(err) => debug!(%peer, error = %err, "Peer Ping check failed"),
            }
            None
        }
        SwarmEvent::Behaviour(NitroBehaviorEvent::Kademlia(kad_event)) => {
            debug!(?kad_event, "Received Kademlia DHT event");
            None
        }
        _ => None,
    }
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

/// Publishes a `WorkerTelemetry` heartbeat onto the `"worker-telemetry"` Gossipsub topic.
pub fn publish_worker_telemetry(
    swarm: &mut Swarm<NitroBehavior>,
    telemetry: &WorkerTelemetry,
) -> anyhow::Result<Option<gossipsub::MessageId>> {
    let topic = gossipsub::IdentTopic::new(WORKER_TELEMETRY_TOPIC);
    let wire_bytes = telemetry.to_bytes()?;

    match swarm.behaviour_mut().gossipsub.publish(topic, wire_bytes) {
        Ok(msg_id) => Ok(Some(msg_id)),
        Err(gossipsub::PublishError::InsufficientPeers) => Ok(None),
        Err(err) => Err(anyhow!(
            "Failed to publish WorkerTelemetry onto '{}': {}",
            WORKER_TELEMETRY_TOPIC,
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
                if let Some(gossipsub::Event::Message {
                    propagation_source,
                    message_id,
                    message,
                }) = process_swarm_maintenance(swarm, event)
                {
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn zstd_wire_framing_compresses_and_roundtrips_task_payload() {
        let repeating_wasm = vec![0x61u8; 4096];
        let payload = TaskPayload::new(
            "task-42",
            repeating_wasm.clone(),
            "edge mapreduce over compressed wire frames",
            500_000,
        );

        let encoded = payload.to_bytes().expect("encode TaskPayload");
        assert!(
            encoded.starts_with(ZSTD_FRAME_MAGIC),
            "expected NZST compressed frame prefix"
        );
        assert!(encoded.len() < 512, "expected high zstd compression ratio");

        let decoded = TaskPayload::from_bytes(&encoded).expect("decode TaskPayload");
        assert_eq!(decoded.task_id, "task-42");
        assert_eq!(decoded.wasm_binary, repeating_wasm);
        assert_eq!(decoded.entry_fn, "map_line");
        decoded
            .verify_wasm_integrity()
            .expect("SHA-256 integrity must pass");
    }

    #[test]
    fn detects_corrupted_wasm_sha256() {
        let mut payload = TaskPayload::new("task-1", vec![1, 2, 3, 4], "chunk", 10_000);
        payload.wasm_binary[0] = 99;
        assert!(payload.verify_wasm_integrity().is_err());
    }

    #[test]
    fn roundtrips_result_payload_security_trap() {
        let failure = ResultPayload::failure(
            "task-evil",
            "job-1",
            "12D3KooWPeer",
            2,
            TaskOutcome::SecurityTrap {
                kind: "OutOfFuel".to_string(),
                reason: "Exceeded 1000 fuel".to_string(),
            },
        );
        let wire = failure.to_bytes().expect("encode ResultPayload");
        let decoded = ResultPayload::from_bytes(&wire).expect("decode ResultPayload");
        assert_eq!(decoded.task_id, "task-evil");
        assert_eq!(decoded.attempt, 2);
        assert!(matches!(decoded.status, TaskOutcome::SecurityTrap { .. }));
    }
}
