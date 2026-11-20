use std::fmt::Write as _;
use std::net::SocketAddr;
use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};

use anyhow::Context;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpListener;
use tracing::{info, warn};

/// Lock-free observability telemetry registry exposed in Prometheus/OpenMetrics exposition format.
#[derive(Debug)]
pub struct NitroTelemetryRegistry {
    started_at: Instant,
    pub tasks_active: AtomicUsize,
    pub tasks_completed_total: AtomicU64,
    pub tasks_trapped_out_of_fuel_total: AtomicU64,
    pub tasks_trapped_memory_limit_total: AtomicU64,
    pub tasks_trapped_table_limit_total: AtomicU64,
    pub tasks_trapped_epoch_timeout_total: AtomicU64,
    pub tasks_execution_error_total: AtomicU64,
    pub signature_failures_total: AtomicU64,
    pub module_cache_hits_total: AtomicU64,
    pub module_cache_misses_total: AtomicU64,
    pub fuel_consumed_total: AtomicU64,
    pub execution_duration_us_total: AtomicU64,
    pub wire_bytes_raw_total: AtomicU64,
    pub wire_bytes_compressed_total: AtomicU64,
    pub speculative_backups_total: AtomicU64,
    pub connected_peers: AtomicUsize,
}

impl Default for NitroTelemetryRegistry {
    fn default() -> Self {
        Self::new()
    }
}

impl NitroTelemetryRegistry {
    /// Initializes a zeroed telemetry registry anchored to the current monotonic timestamp.
    pub fn new() -> Self {
        Self {
            started_at: Instant::now(),
            tasks_active: AtomicUsize::new(0),
            tasks_completed_total: AtomicU64::new(0),
            tasks_trapped_out_of_fuel_total: AtomicU64::new(0),
            tasks_trapped_memory_limit_total: AtomicU64::new(0),
            tasks_trapped_table_limit_total: AtomicU64::new(0),
            tasks_trapped_epoch_timeout_total: AtomicU64::new(0),
            tasks_execution_error_total: AtomicU64::new(0),
            signature_failures_total: AtomicU64::new(0),
            module_cache_hits_total: AtomicU64::new(0),
            module_cache_misses_total: AtomicU64::new(0),
            fuel_consumed_total: AtomicU64::new(0),
            execution_duration_us_total: AtomicU64::new(0),
            wire_bytes_raw_total: AtomicU64::new(0),
            wire_bytes_compressed_total: AtomicU64::new(0),
            speculative_backups_total: AtomicU64::new(0),
            connected_peers: AtomicUsize::new(0),
        }
    }

    /// Records cache, fuel, and latency metrics from a completed Wasm execution.
    pub fn record_execution(&self, fuel_consumed: u64, duration_us: u64, cache_hit: bool) {
        self.tasks_completed_total.fetch_add(1, Ordering::Relaxed);
        self.fuel_consumed_total
            .fetch_add(fuel_consumed, Ordering::Relaxed);
        self.execution_duration_us_total
            .fetch_add(duration_us, Ordering::Relaxed);
        if cache_hit {
            self.module_cache_hits_total.fetch_add(1, Ordering::Relaxed);
        } else {
            self.module_cache_misses_total
                .fetch_add(1, Ordering::Relaxed);
        }
    }

    /// Renders all telemetry metrics in Prometheus v0.0.4 text exposition format.
    pub fn render_prometheus(&self, role: &str, peer_id: &str) -> String {
        let mut out = String::with_capacity(2048);
        let uptime_secs = self.started_at.elapsed().as_secs();

        let _ = writeln!(out, "# HELP nitro_uptime_seconds Node uptime in seconds.");
        let _ = writeln!(out, "# TYPE nitro_uptime_seconds gauge");
        let _ = writeln!(
            out,
            "nitro_uptime_seconds{{role=\"{role}\",peer_id=\"{peer_id}\"}} {uptime_secs}"
        );

        let _ = writeln!(
            out,
            "# HELP nitro_tasks_active Number of currently executing Wasm sandbox tasks."
        );
        let _ = writeln!(out, "# TYPE nitro_tasks_active gauge");
        let _ = writeln!(
            out,
            "nitro_tasks_active{{role=\"{role}\"}} {}",
            self.tasks_active.load(Ordering::Relaxed)
        );

        let _ = writeln!(
            out,
            "# HELP nitro_tasks_completed_total Total completed MapReduce tasks."
        );
        let _ = writeln!(out, "# TYPE nitro_tasks_completed_total counter");
        let _ = writeln!(
            out,
            "nitro_tasks_completed_total{{role=\"{role}\"}} {}",
            self.tasks_completed_total.load(Ordering::Relaxed)
        );

        let _ = writeln!(
            out,
            "# HELP nitro_tasks_trapped_total Total tasks terminated by sandbox security guards."
        );
        let _ = writeln!(out, "# TYPE nitro_tasks_trapped_total counter");
        let _ = writeln!(
            out,
            "nitro_tasks_trapped_total{{role=\"{role}\",kind=\"out_of_fuel\"}} {}",
            self.tasks_trapped_out_of_fuel_total.load(Ordering::Relaxed)
        );
        let _ = writeln!(
            out,
            "nitro_tasks_trapped_total{{role=\"{role}\",kind=\"memory_limit\"}} {}",
            self.tasks_trapped_memory_limit_total
                .load(Ordering::Relaxed)
        );
        let _ = writeln!(
            out,
            "nitro_tasks_trapped_total{{role=\"{role}\",kind=\"table_limit\"}} {}",
            self.tasks_trapped_table_limit_total.load(Ordering::Relaxed)
        );
        let _ = writeln!(
            out,
            "nitro_tasks_trapped_total{{role=\"{role}\",kind=\"epoch_timeout\"}} {}",
            self.tasks_trapped_epoch_timeout_total
                .load(Ordering::Relaxed)
        );

        let _ = writeln!(
            out,
            "# HELP nitro_tasks_execution_error_total Total non-trap task execution errors."
        );
        let _ = writeln!(out, "# TYPE nitro_tasks_execution_error_total counter");
        let _ = writeln!(
            out,
            "nitro_tasks_execution_error_total{{role=\"{role}\"}} {}",
            self.tasks_execution_error_total.load(Ordering::Relaxed)
        );

        let _ = writeln!(
            out,
            "# HELP nitro_signature_failures_total Total Ed25519 payload verification failures."
        );
        let _ = writeln!(out, "# TYPE nitro_signature_failures_total counter");
        let _ = writeln!(
            out,
            "nitro_signature_failures_total{{role=\"{role}\"}} {}",
            self.signature_failures_total.load(Ordering::Relaxed)
        );

        let _ = writeln!(
            out,
            "# HELP nitro_module_cache_hits_total SHA-256 compiled Cranelift module cache hits."
        );
        let _ = writeln!(out, "# TYPE nitro_module_cache_hits_total counter");
        let _ = writeln!(
            out,
            "nitro_module_cache_hits_total{{role=\"{role}\"}} {}",
            self.module_cache_hits_total.load(Ordering::Relaxed)
        );

        let _ = writeln!(
            out,
            "# HELP nitro_module_cache_misses_total SHA-256 compiled Cranelift module cache misses."
        );
        let _ = writeln!(out, "# TYPE nitro_module_cache_misses_total counter");
        let _ = writeln!(
            out,
            "nitro_module_cache_misses_total{{role=\"{role}\"}} {}",
            self.module_cache_misses_total.load(Ordering::Relaxed)
        );

        let _ = writeln!(
            out,
            "# HELP nitro_fuel_consumed_total Total deterministic Wasm instructions consumed."
        );
        let _ = writeln!(out, "# TYPE nitro_fuel_consumed_total counter");
        let _ = writeln!(
            out,
            "nitro_fuel_consumed_total{{role=\"{role}\"}} {}",
            self.fuel_consumed_total.load(Ordering::Relaxed)
        );

        let _ = writeln!(
            out,
            "# HELP nitro_execution_duration_microseconds_total Cumulative Wasm sandbox execution time in microseconds."
        );
        let _ = writeln!(
            out,
            "# TYPE nitro_execution_duration_microseconds_total counter"
        );
        let _ = writeln!(
            out,
            "nitro_execution_duration_microseconds_total{{role=\"{role}\"}} {}",
            self.execution_duration_us_total.load(Ordering::Relaxed)
        );

        let _ = writeln!(
            out,
            "# HELP nitro_speculative_backups_total Speculative straggler backup tasks dispatched."
        );
        let _ = writeln!(out, "# TYPE nitro_speculative_backups_total counter");
        let _ = writeln!(
            out,
            "nitro_speculative_backups_total{{role=\"{role}\"}} {}",
            self.speculative_backups_total.load(Ordering::Relaxed)
        );

        let _ = writeln!(
            out,
            "# HELP nitro_p2p_connected_peers Current connected libp2p mesh peers."
        );
        let _ = writeln!(out, "# TYPE nitro_p2p_connected_peers gauge");
        let _ = writeln!(
            out,
            "nitro_p2p_connected_peers{{role=\"{role}\"}} {}",
            self.connected_peers.load(Ordering::Relaxed)
        );

        out
    }

    /// Renders a lightweight JSON healthcheck document for `/healthz` and `/readyz`.
    pub fn render_healthz(&self, role: &str, peer_id: &str) -> String {
        format!(
            "{{\"status\":\"ok\",\"role\":\"{}\",\"peer_id\":\"{}\",\"uptime_secs\":{},\"active_tasks\":{},\"connected_peers\":{}}}",
            role,
            peer_id,
            self.started_at.elapsed().as_secs(),
            self.tasks_active.load(Ordering::Relaxed),
            self.connected_peers.load(Ordering::Relaxed),
        )
    }
}

/// Binds a non-blocking HTTP/1.1 observability server serving `/metrics` and `/healthz`
/// and returns the actual bound `SocketAddr`.
pub async fn spawn_metrics_server(
    bind_addr: SocketAddr,
    role: impl Into<String>,
    peer_id: impl Into<String>,
    registry: Arc<NitroTelemetryRegistry>,
) -> anyhow::Result<SocketAddr> {
    let listener = TcpListener::bind(bind_addr)
        .await
        .with_context(|| format!("Failed to bind observability HTTP server on {bind_addr}"))?;
    let local_addr = listener.local_addr()?;
    let role = role.into();
    let peer_id = peer_id.into();

    info!(%local_addr, %role, "Started Prometheus /metrics and /healthz observability endpoint");

    tokio::spawn(async move {
        loop {
            let Ok((mut socket, _remote)) = listener.accept().await else {
                break;
            };

            let reg = Arc::clone(&registry);
            let r = role.clone();
            let pid = peer_id.clone();

            tokio::spawn(async move {
                let mut buf = [0u8; 2048];
                let Ok(Ok(n)) =
                    tokio::time::timeout(Duration::from_secs(2), socket.read(&mut buf)).await
                else {
                    return;
                };
                if n == 0 {
                    return;
                }

                let req = String::from_utf8_lossy(&buf[..n]);
                let first_line = req.lines().next().unwrap_or("");
                let path = first_line.split_whitespace().nth(1).unwrap_or("/");

                let (status_line, content_type, body) = match path {
                    "/metrics" => (
                        "HTTP/1.1 200 OK",
                        "text/plain; version=0.0.4; charset=utf-8",
                        reg.render_prometheus(&r, &pid),
                    ),
                    "/healthz" | "/readyz" => (
                        "HTTP/1.1 200 OK",
                        "application/json",
                        reg.render_healthz(&r, &pid),
                    ),
                    _ => (
                        "HTTP/1.1 404 Not Found",
                        "text/plain; charset=utf-8",
                        "Not Found\n".to_string(),
                    ),
                };

                let response = format!(
                    "{status_line}\r\nContent-Type: {content_type}\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{body}",
                    body.len()
                );

                if let Err(err) = socket.write_all(response.as_bytes()).await {
                    warn!(error = %err, "Failed writing observability HTTP response");
                }
            });
        }
    });

    Ok(local_addr)
}

#[cfg(test)]
mod tests {
    use super::*;
    use tokio::net::TcpStream;

    #[tokio::test]
    async fn serves_prometheus_metrics_and_healthz_over_http() {
        let registry = Arc::new(NitroTelemetryRegistry::new());
        registry.record_execution(42_000, 350, true);
        registry
            .tasks_trapped_out_of_fuel_total
            .fetch_add(1, Ordering::Relaxed);

        let addr = spawn_metrics_server(
            "127.0.0.1:0".parse().expect("valid addr"),
            "worker",
            "12D3KooWTestPeer",
            Arc::clone(&registry),
        )
        .await
        .expect("spawn server");

        let mut stream = TcpStream::connect(addr).await.expect("connect metrics");
        stream
            .write_all(b"GET /metrics HTTP/1.1\r\nHost: localhost\r\n\r\n")
            .await
            .expect("send GET /metrics");
        let mut resp = String::new();
        stream.read_to_string(&mut resp).await.expect("read body");

        assert!(resp.contains("HTTP/1.1 200 OK"));
        assert!(resp.contains("nitro_tasks_completed_total{role=\"worker\"} 1"));
        assert!(resp.contains("nitro_fuel_consumed_total{role=\"worker\"} 42000"));
        assert!(resp.contains("nitro_tasks_trapped_total{role=\"worker\",kind=\"out_of_fuel\"} 1"));

        let mut health_stream = TcpStream::connect(addr).await.expect("connect healthz");
        health_stream
            .write_all(b"GET /healthz HTTP/1.1\r\nHost: localhost\r\n\r\n")
            .await
            .expect("send GET /healthz");
        let mut health_resp = String::new();
        health_stream
            .read_to_string(&mut health_resp)
            .await
            .expect("read healthz");
        assert!(health_resp.contains("\"status\":\"ok\""));
        assert!(health_resp.contains("\"peer_id\":\"12D3KooWTestPeer\""));
    }
}
