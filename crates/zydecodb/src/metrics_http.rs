//! Operability HTTP endpoint: Prometheus `/metrics`, plus `/healthz` (liveness)
//! and `/readyz` (readiness).
//!
//! This is intentionally tiny and read-only. It runs on its own loopback-by-
//! default address (see `[metrics] listen` in the config) so the data plane
//! (the binary protocol on the main port) and the operability plane never share
//! a socket. `tiny_http` parses the HTTP so we never hand-roll request parsing.

use std::net::SocketAddr;
use std::sync::{Arc, Mutex};
use std::thread::{self, JoinHandle};
use std::time::Duration;
use tiny_http::{Header, Method, Request, Response, Server};
use tracing::{error, info};
use zydecodb_engine::metrics::Metrics;

/// Spawn the metrics/health HTTP server. The thread polls a short receive
/// timeout so it observes the shared shutdown flag promptly.
pub fn spawn(
    addr: SocketAddr,
    metrics: Arc<Metrics>,
    shutdown: Arc<Mutex<bool>>,
) -> std::io::Result<JoinHandle<()>> {
    let server = Server::http(addr).map_err(|e| std::io::Error::other(e.to_string()))?;
    info!(%addr, "metrics endpoint listening (/metrics /healthz /readyz)");
    thread::Builder::new()
        .name("zydecodb-metrics-http".into())
        .spawn(move || loop {
            if *shutdown.lock().unwrap() {
                break;
            }
            match server.recv_timeout(Duration::from_millis(250)) {
                Ok(Some(req)) => handle(req, &metrics),
                Ok(None) => continue,
                Err(e) => {
                    error!(error = %e, "metrics http receive failed");
                    break;
                }
            }
        })
}

fn handle(req: Request, metrics: &Metrics) {
    if *req.method() != Method::Get {
        let _ = req.respond(Response::from_string("method not allowed\n").with_status_code(405));
        return;
    }
    // Ignore any query string when routing.
    let path = req.url().split('?').next().unwrap_or("");
    let resp = match path {
        "/metrics" => Response::from_string(metrics.render())
            .with_status_code(200)
            .with_header(content_type("text/plain; version=0.0.4")),
        "/healthz" => Response::from_string("ok\n").with_status_code(200),
        "/readyz" => Response::from_string("ready\n").with_status_code(200),
        _ => Response::from_string("not found\n").with_status_code(404),
    };
    let _ = req.respond(resp);
}

fn content_type(value: &str) -> Header {
    // Safe: static field name + a known-good value string.
    Header::from_bytes(b"Content-Type".as_slice(), value.as_bytes())
        .expect("valid content-type header")
}
