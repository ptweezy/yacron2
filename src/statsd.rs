//! Best-effort statsd metrics over UDP.
//!
//! A job emits a `start` gauge when it launches and `stop`/`success`/`duration`
//! when it finishes. Failures to send are the caller's to swallow — telemetry
//! must never crash the scheduler.

use std::time::Instant;

use tokio::net::UdpSocket;

use crate::config::Statsd;

/// Send a single statsd datagram to `host:port`.
async fn send(host: &str, port: u16, message: &str) -> std::io::Result<()> {
    let socket = UdpSocket::bind(("0.0.0.0", 0)).await?;
    socket.connect((host, port)).await?;
    socket.send(message.as_bytes()).await?;
    Ok(())
}

/// Emits per-job statsd metrics, tracking the job's wall-clock duration.
pub struct StatsdWriter {
    host: String,
    port: u16,
    prefix: String,
    start: Option<Instant>,
}

impl StatsdWriter {
    pub fn new(config: &Statsd) -> StatsdWriter {
        StatsdWriter {
            host: config.host.clone(),
            port: config.port,
            prefix: config.prefix.clone(),
            start: None,
        }
    }

    pub async fn job_started(&mut self) -> std::io::Result<()> {
        self.start = Some(Instant::now());
        send(
            &self.host,
            self.port,
            &format!("{}.start:1|g\n", self.prefix),
        )
        .await
    }

    pub async fn job_stopped(&mut self, failed: bool) -> std::io::Result<()> {
        let Some(start) = self.start else {
            return Ok(());
        };
        let duration_ms = (start.elapsed().as_secs_f64() * 1000.0).round() as i64;
        let success = if failed { 0 } else { 1 };
        let message = format!(
            "{p}.stop:1|g\n{p}.success:{success}|g\n{p}.duration:{duration_ms}|ms|@0.1\n",
            p = self.prefix,
        );
        send(&self.host, self.port, &message).await
    }
}
