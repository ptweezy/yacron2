//! The optional HTTP REST API: `GET /version`, `GET /status`, and
//! `POST /jobs/{name}/start`.
//!
//! Listens on any mix of `http://host:port` and `unix:///path` addresses, with
//! optional bearer-token authentication. It owns no scheduler state; status and
//! start requests are forwarded to the scheduler task over the message channel.

use std::convert::Infallible;
use std::sync::Arc;

use bytes::Bytes;
use http_body_util::Full;
use hyper::header::{ACCEPT, AUTHORIZATION, CONTENT_TYPE};
use hyper::service::service_fn;
use hyper::{Method, Request, Response, StatusCode};
use hyper_util::rt::TokioIo;
use tokio::net::TcpListener;
use tokio::sync::{mpsc, oneshot, watch};
use tokio::task::JoinSet;
use tracing::{debug, info, warn};

use crate::config::WebConfig;
use crate::cron::{natural_time, JobStatus, ScheduledIn, SchedulerMsg, StartJobResult, WebCommand};
use crate::error::{cfg_err, ConfigResult};
use crate::version::VERSION;

/// A running web server: a set of listener tasks and a shutdown switch.
pub struct WebServer {
    shutdown: watch::Sender<bool>,
    tasks: JoinSet<()>,
}

/// Shared, immutable state every request handler needs.
struct Handler {
    token: Option<String>,
    headers: Vec<(String, String)>,
    msg_tx: mpsc::UnboundedSender<SchedulerMsg>,
}

impl WebServer {
    /// Resolve auth, bind every listen address, and start serving. Bad
    /// addresses are skipped with a warning; an authToken that resolves to
    /// nothing is a hard error (fail closed).
    pub async fn start(
        config: &WebConfig,
        msg_tx: mpsc::UnboundedSender<SchedulerMsg>,
    ) -> ConfigResult<WebServer> {
        let token = resolve_web_token(config)?;
        if token.is_some() {
            info!("web: requiring bearer-token authentication");
        }

        let headers: Vec<(String, String)> = config
            .headers
            .clone()
            .unwrap_or_default()
            .into_iter()
            .collect();
        let handler = Arc::new(Handler {
            token,
            headers,
            msg_tx,
        });

        let (shutdown, shutdown_rx) = watch::channel(false);
        let mut tasks = JoinSet::new();

        for addr in &config.listen {
            match parse_listen(addr) {
                Some(ListenAddr::Tcp { host, port }) => {
                    match TcpListener::bind((host.as_str(), port)).await {
                        Ok(listener) => {
                            info!("web: started listening on {addr}");
                            spawn_tcp(&mut tasks, listener, handler.clone(), shutdown_rx.clone());
                        }
                        Err(err) => {
                            warn!("web: could not listen on {addr}: {err}");
                        }
                    }
                }
                #[cfg(unix)]
                Some(ListenAddr::Unix { path }) => {
                    match bind_unix(&path, config.socket_mode.as_deref()) {
                        Ok(listener) => {
                            info!("web: started listening on {addr}");
                            spawn_unix(&mut tasks, listener, handler.clone(), shutdown_rx.clone());
                        }
                        Err(err) => {
                            warn!("web: could not listen on {addr}: {err}");
                        }
                    }
                }
                #[cfg(not(unix))]
                Some(ListenAddr::Unix { .. }) => {
                    warn!("web: unix sockets are not supported on this platform: {addr}");
                }
                None => {
                    warn!("web: ignoring unusable listen url {addr}");
                }
            }
        }

        Ok(WebServer { shutdown, tasks })
    }

    /// Stop serving and wait for the listener tasks to finish.
    pub async fn stop(mut self) {
        let _ = self.shutdown.send(true);
        while self.tasks.join_next().await.is_some() {}
    }
}

enum ListenAddr {
    Tcp { host: String, port: u16 },
    Unix { path: String },
}

fn parse_listen(addr: &str) -> Option<ListenAddr> {
    if let Some(rest) = addr.strip_prefix("http://") {
        let hostport = rest.split('/').next().unwrap_or("");
        let (host, port) = hostport.rsplit_once(':')?;
        if host.is_empty() {
            return None;
        }
        let port: u16 = port.parse().ok()?;
        Some(ListenAddr::Tcp {
            host: host.to_string(),
            port,
        })
    } else if let Some(path) = addr.strip_prefix("unix://") {
        if path.is_empty() {
            return None;
        }
        Some(ListenAddr::Unix {
            path: path.to_string(),
        })
    } else {
        None
    }
}

#[cfg(unix)]
fn bind_unix(path: &str, socket_mode: Option<&str>) -> std::io::Result<tokio::net::UnixListener> {
    // Remove a stale socket so re-binding after a restart succeeds.
    let _ = std::fs::remove_file(path);
    let listener = tokio::net::UnixListener::bind(path)?;
    if let Some(mode) = socket_mode {
        use std::os::unix::fs::PermissionsExt;
        match u32::from_str_radix(mode, 8) {
            Ok(bits) => {
                if let Err(err) =
                    std::fs::set_permissions(path, std::fs::Permissions::from_mode(bits))
                {
                    warn!("web: could not set socketMode {mode:?} on {path}: {err}");
                }
            }
            Err(_) => warn!("web: invalid socketMode {mode:?}"),
        }
    }
    Ok(listener)
}

fn spawn_tcp(
    tasks: &mut JoinSet<()>,
    listener: TcpListener,
    handler: Arc<Handler>,
    mut shutdown: watch::Receiver<bool>,
) {
    tasks.spawn(async move {
        loop {
            tokio::select! {
                changed = shutdown.changed() => {
                    if changed.is_err() || *shutdown.borrow() { break; }
                }
                accepted = listener.accept() => {
                    if let Ok((stream, _)) = accepted {
                        let handler = handler.clone();
                        tokio::spawn(serve(stream, handler));
                    }
                }
            }
        }
    });
}

#[cfg(unix)]
fn spawn_unix(
    tasks: &mut JoinSet<()>,
    listener: tokio::net::UnixListener,
    handler: Arc<Handler>,
    mut shutdown: watch::Receiver<bool>,
) {
    tasks.spawn(async move {
        loop {
            tokio::select! {
                changed = shutdown.changed() => {
                    if changed.is_err() || *shutdown.borrow() { break; }
                }
                accepted = listener.accept() => {
                    if let Ok((stream, _)) = accepted {
                        let handler = handler.clone();
                        tokio::spawn(serve(stream, handler));
                    }
                }
            }
        }
    });
}

async fn serve<S>(stream: S, handler: Arc<Handler>)
where
    S: tokio::io::AsyncRead + tokio::io::AsyncWrite + Unpin + Send + 'static,
{
    let io = TokioIo::new(stream);
    let service = service_fn(move |req| {
        let handler = handler.clone();
        async move { handle_request(req, handler).await }
    });
    if let Err(err) = hyper::server::conn::http1::Builder::new()
        .serve_connection(io, service)
        .await
    {
        debug!("web: connection error: {err}");
    }
}

async fn handle_request(
    req: Request<hyper::body::Incoming>,
    handler: Arc<Handler>,
) -> Result<Response<Full<Bytes>>, Infallible> {
    if let Some(token) = &handler.token {
        let presented = req
            .headers()
            .get(AUTHORIZATION)
            .and_then(|v| v.to_str().ok())
            .unwrap_or("");
        if !check_bearer(presented, token) {
            return Ok(build_response(
                StatusCode::UNAUTHORIZED,
                "text/plain; charset=utf-8",
                "401 Unauthorized".into(),
                &handler,
            ));
        }
    }

    let accept_json =
        req.headers().get(ACCEPT).and_then(|v| v.to_str().ok()) == Some("application/json");

    let method = req.method().clone();
    let path = req.uri().path().to_string();

    let response = match (&method, path.as_str()) {
        (&Method::GET, "/version") => build_response(
            StatusCode::OK,
            "text/plain; charset=utf-8",
            VERSION.into(),
            &handler,
        ),
        (&Method::GET, "/status") => status_response(&handler, accept_json).await,
        (&Method::POST, p) if p.starts_with("/jobs/") && p.ends_with("/start") => {
            start_response(p, &handler).await
        }
        _ => build_response(
            StatusCode::NOT_FOUND,
            "text/plain; charset=utf-8",
            "404 Not Found".into(),
            &handler,
        ),
    };
    Ok(response)
}

async fn status_response(handler: &Handler, accept_json: bool) -> Response<Full<Bytes>> {
    let (tx, rx) = oneshot::channel();
    if handler
        .msg_tx
        .send(SchedulerMsg::Web(WebCommand::Status(tx)))
        .is_err()
    {
        return build_response(
            StatusCode::INTERNAL_SERVER_ERROR,
            "text/plain; charset=utf-8",
            "scheduler unavailable".into(),
            handler,
        );
    }
    let statuses = match rx.await {
        Ok(s) => s,
        Err(_) => {
            return build_response(
                StatusCode::INTERNAL_SERVER_ERROR,
                "text/plain; charset=utf-8",
                "scheduler unavailable".into(),
                handler,
            )
        }
    };

    if accept_json {
        let body =
            serde_json::to_string(&status_json(&statuses)).unwrap_or_else(|_| "[]".to_string());
        build_response(
            StatusCode::OK,
            "application/json; charset=utf-8",
            body,
            handler,
        )
    } else {
        build_response(
            StatusCode::OK,
            "text/plain; charset=utf-8",
            status_text(&statuses),
            handler,
        )
    }
}

fn status_json(statuses: &[JobStatus]) -> serde_json::Value {
    use serde_json::json;
    let items: Vec<serde_json::Value> = statuses
        .iter()
        .map(|s| match s {
            JobStatus::Running { name, pids } => json!({
                "job": name, "status": "running", "pid": pids,
            }),
            JobStatus::Disabled { name } => json!({
                "job": name, "status": "disabled",
            }),
            JobStatus::Scheduled {
                name,
                scheduled_in: ScheduledIn::Seconds(secs),
            } => json!({
                "job": name, "status": "scheduled", "scheduled_in": secs,
            }),
            JobStatus::Scheduled {
                name,
                scheduled_in: ScheduledIn::Reboot,
            } => json!({
                "job": name, "status": "scheduled", "scheduled_in": "@reboot",
            }),
        })
        .collect();
    serde_json::Value::Array(items)
}

fn status_text(statuses: &[JobStatus]) -> String {
    let mut lines = Vec::new();
    for status in statuses {
        let line = match status {
            JobStatus::Running { name, pids } => {
                let pid_list = pids
                    .iter()
                    .map(|p| p.to_string())
                    .collect::<Vec<_>>()
                    .join(", ");
                format!("{name}: running (pid: {pid_list})")
            }
            JobStatus::Disabled { name } => format!("{name}: disabled"),
            JobStatus::Scheduled {
                name,
                scheduled_in: ScheduledIn::Seconds(secs),
            } => format!("{name}: scheduled ({})", natural_time(*secs)),
            JobStatus::Scheduled {
                name,
                scheduled_in: ScheduledIn::Reboot,
            } => format!("{name}: scheduled (@reboot)"),
        };
        lines.push(line);
    }
    lines.join("\n")
}

async fn start_response(path: &str, handler: &Handler) -> Response<Full<Bytes>> {
    let name = path
        .trim_start_matches("/jobs/")
        .trim_end_matches("/start")
        .to_string();
    let (tx, rx) = oneshot::channel();
    if handler
        .msg_tx
        .send(SchedulerMsg::Web(WebCommand::StartJob { name, reply: tx }))
        .is_err()
    {
        return build_response(
            StatusCode::INTERNAL_SERVER_ERROR,
            "text/plain; charset=utf-8",
            "scheduler unavailable".into(),
            handler,
        );
    }
    match rx.await {
        Ok(StartJobResult::Started) => build_response(
            StatusCode::OK,
            "text/plain; charset=utf-8",
            String::new(),
            handler,
        ),
        Ok(StartJobResult::NotFound) => build_response(
            StatusCode::NOT_FOUND,
            "text/plain; charset=utf-8",
            "404 Not Found".into(),
            handler,
        ),
        Ok(StartJobResult::Disabled) => build_response(
            StatusCode::CONFLICT,
            "text/plain; charset=utf-8",
            "job is disabled".into(),
            handler,
        ),
        Err(_) => build_response(
            StatusCode::INTERNAL_SERVER_ERROR,
            "text/plain; charset=utf-8",
            "scheduler unavailable".into(),
            handler,
        ),
    }
}

fn build_response(
    status: StatusCode,
    content_type: &str,
    body: String,
    handler: &Handler,
) -> Response<Full<Bytes>> {
    let mut builder = Response::builder()
        .status(status)
        .header(CONTENT_TYPE, content_type);
    for (key, value) in &handler.headers {
        if let (Ok(name), Ok(val)) = (
            hyper::header::HeaderName::from_bytes(key.as_bytes()),
            hyper::header::HeaderValue::from_str(value),
        ) {
            builder = builder.header(name, val);
        }
    }
    builder
        .body(Full::new(Bytes::from(body)))
        .expect("valid response")
}

fn check_bearer(presented: &str, token: &str) -> bool {
    let (scheme, rest) = presented.split_once(' ').unwrap_or(("", ""));
    scheme.eq_ignore_ascii_case("bearer") && constant_time_eq(rest.as_bytes(), token.as_bytes())
}

fn constant_time_eq(a: &[u8], b: &[u8]) -> bool {
    if a.len() != b.len() {
        return false;
    }
    let mut diff = 0u8;
    for (x, y) in a.iter().zip(b) {
        diff |= x ^ y;
    }
    diff == 0
}

/// Resolve the web auth token, failing closed: an `authToken` block that
/// resolves to nothing is an error rather than silently-unauthenticated.
pub(crate) fn resolve_web_token(config: &WebConfig) -> ConfigResult<Option<String>> {
    let Some(auth) = &config.auth_token else {
        return Ok(None);
    };
    let token = if let Some(value) = auth.value.as_ref().filter(|v| !v.is_empty()) {
        value.clone()
    } else if let Some(file) = &auth.from_file {
        std::fs::read_to_string(file)
            .map_err(|e| cfg_err!("web.authToken.fromFile could not be read: {e}"))?
            .trim()
            .to_string()
    } else if let Some(var) = &auth.from_env_var {
        std::env::var(var).unwrap_or_default()
    } else {
        String::new()
    };

    if token.is_empty() {
        return Err(cfg_err!(
            "web.authToken is configured but resolved to an empty token; \
             refusing to start the web API without authentication"
        ));
    }
    Ok(Some(token))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parse_listen_urls() {
        assert!(matches!(
            parse_listen("http://127.0.0.1:8080"),
            Some(ListenAddr::Tcp { port: 8080, .. })
        ));
        assert!(matches!(
            parse_listen("unix:///tmp/y.sock"),
            Some(ListenAddr::Unix { .. })
        ));
        assert!(parse_listen("ftp://localhost:21").is_none());
        assert!(parse_listen("http://").is_none());
    }

    #[test]
    fn bearer_check() {
        assert!(check_bearer("Bearer secret", "secret"));
        assert!(check_bearer("bearer secret", "secret"));
        assert!(!check_bearer("Bearer wrong", "secret"));
        assert!(!check_bearer("", "secret"));
    }

    #[test]
    fn token_fails_closed() {
        let config = WebConfig {
            listen: vec![],
            headers: None,
            auth_token: Some(crate::config::SecretRef::default()),
            socket_mode: None,
        };
        assert!(resolve_web_token(&config).is_err());
    }
}
