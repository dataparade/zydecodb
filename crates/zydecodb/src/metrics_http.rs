//! Operability HTTP endpoint: Prometheus `/metrics`, plus `/healthz` (liveness)
//! and `/readyz` (readiness).
//!
//! This is intentionally tiny and read-only. It runs on its own loopback-by-
//! default address (see `[metrics] listen` in the config) so the data plane
//! (the binary protocol on the main port) and the operability plane never share
//! a socket. `tiny_http` parses the HTTP so we never hand-roll request parsing.
//!
//! Hardening: a non-loopback bind is refused unless `allow_remote = true`, and
//! remote binds require a bearer `token`. When a token is configured (loopback
//! or not), `/metrics` requires `Authorization: Bearer <token>`; `/healthz` and
//! `/readyz` stay unauthenticated so probes keep working.

use std::net::SocketAddr;
use std::sync::{Arc, Mutex};
use std::thread::{self, JoinHandle};
use std::time::Duration;
use tiny_http::{Header, Method, Request, Response, Server};
use tracing::{error, info};
use zydecodb_engine::metrics::Metrics;

/// Validate the metrics bind policy. Returns an error string suitable for
/// refusing startup.
pub fn check_bind_policy(
    addr: &SocketAddr,
    allow_remote: bool,
    token: Option<&str>,
) -> Result<(), String> {
    if addr.ip().is_loopback() {
        return Ok(());
    }
    if !allow_remote {
        return Err(format!(
            "metrics listen {addr} is not loopback — set [metrics] allow_remote = true \
             (and a token) to expose it, or bind 127.0.0.1"
        ));
    }
    match token {
        Some(t) if !t.is_empty() => Ok(()),
        _ => Err("metrics allow_remote = true requires a non-empty [metrics] token".to_string()),
    }
}

/// Spawn the metrics/health HTTP server. The thread polls a short receive
/// timeout so it observes the shared shutdown flag promptly.
pub fn spawn(
    addr: SocketAddr,
    metrics: Arc<Metrics>,
    shutdown: Arc<Mutex<bool>>,
    token: Option<String>,
) -> std::io::Result<JoinHandle<()>> {
    let server = Server::http(addr).map_err(|e| std::io::Error::other(e.to_string()))?;
    info!(%addr, auth = token.is_some(), "metrics endpoint listening (/metrics /healthz /readyz)");
    thread::Builder::new()
        .name("zydecodb-metrics-http".into())
        .spawn(move || loop {
            if *shutdown.lock().unwrap() {
                break;
            }
            match server.recv_timeout(Duration::from_millis(250)) {
                Ok(Some(req)) => handle(req, &metrics, token.as_deref()),
                Ok(None) => continue,
                Err(e) => {
                    error!(error = %e, "metrics http receive failed");
                    break;
                }
            }
        })
}

fn handle(req: Request, metrics: &Metrics, token: Option<&str>) {
    if *req.method() != Method::Get {
        let _ = req.respond(Response::from_string("method not allowed\n").with_status_code(405));
        return;
    }
    // Ignore any query string when routing.
    let path = req.url().split('?').next().unwrap_or("");
    let resp = match path {
        "/metrics" => {
            if let Some(expected) = token {
                if !bearer_ok(&req, expected) {
                    let _ =
                        req.respond(Response::from_string("unauthorized\n").with_status_code(401));
                    return;
                }
            }
            Response::from_string(metrics.render())
                .with_status_code(200)
                .with_header(content_type("text/plain; version=0.0.4"))
        }
        "/healthz" => Response::from_string("ok\n").with_status_code(200),
        "/readyz" => Response::from_string("ready\n").with_status_code(200),
        _ => Response::from_string("not found\n").with_status_code(404),
    };
    let _ = req.respond(resp);
}

fn bearer_ok(req: &Request, expected: &str) -> bool {
    let presented = req
        .headers()
        .iter()
        .find(|h| {
            h.field
                .as_str()
                .as_str()
                .eq_ignore_ascii_case("authorization")
        })
        .map(|h| h.value.as_str());
    let Some(value) = presented else {
        return false;
    };
    let Some(candidate) = value.strip_prefix("Bearer ") else {
        return false;
    };
    constant_time_eq(candidate.as_bytes(), expected.as_bytes())
}

fn constant_time_eq(a: &[u8], b: &[u8]) -> bool {
    if a.len() != b.len() {
        return false;
    }
    let mut diff = 0u8;
    for (x, y) in a.iter().zip(b.iter()) {
        diff |= x ^ y;
    }
    diff == 0
}

fn content_type(v: &str) -> Header {
    Header::from_bytes(&b"Content-Type"[..], v.as_bytes()).expect("static header")
}
