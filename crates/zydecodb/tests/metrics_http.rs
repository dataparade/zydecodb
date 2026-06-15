//! The operability HTTP endpoint serves Prometheus metrics and health probes.

use std::io::{Read, Write};
use std::net::{SocketAddr, TcpListener, TcpStream};
use std::thread;
use std::time::Duration;
use tempfile::TempDir;

fn free_addr() -> SocketAddr {
    let l = TcpListener::bind("127.0.0.1:0").unwrap();
    let a = l.local_addr().unwrap();
    drop(l);
    a
}

fn http_get(addr: SocketAddr, path: &str) -> (u16, String) {
    let mut s = TcpStream::connect(addr).unwrap();
    s.set_read_timeout(Some(Duration::from_secs(5))).unwrap();
    let req = format!("GET {path} HTTP/1.1\r\nHost: localhost\r\nConnection: close\r\n\r\n");
    s.write_all(req.as_bytes()).unwrap();
    let mut buf = String::new();
    s.read_to_string(&mut buf).unwrap();
    let code: u16 = buf
        .lines()
        .next()
        .and_then(|l| l.split_whitespace().nth(1))
        .and_then(|c| c.parse().ok())
        .expect("status line");
    (code, buf)
}

#[test]
fn metrics_health_and_readiness_endpoints() {
    let tmp = TempDir::new().unwrap();
    let data_dir = tmp.path().join("data");
    let wal_dir = tmp.path().join("wal");
    std::fs::create_dir_all(&data_dir).unwrap();
    std::fs::create_dir_all(&wal_dir).unwrap();

    let data_addr = free_addr();
    let metrics_addr = free_addr();

    let config = zydecodb::config::Config {
        listen: data_addr,
        data_dir,
        wal_dir,
        block_cache_mb: 64,
        max_open_readers: 32,
        poll_compaction_ms: 50,
        durability: Default::default(),
        fsync_interval_ms: 100,
        shipping: Default::default(),
        metrics: zydecodb::config::MetricsConfig {
            listen: Some(metrics_addr),
            per_tenant: false,
        },
        replica: Default::default(),
        security: zydecodb::config::SecurityConfig {
            require_auth: zydecodb::config::RequireAuth::False,
            ..Default::default()
        },
        tls: Default::default(),
        listen_unix: None,
        runtime: Default::default(),
    };

    let server = zydecodb::server::Server::new();
    let shutdown = server.shutdown_flag();
    let handle = thread::spawn(move || server.run(config).unwrap());

    // Wait for the metrics listener to come up.
    let mut up = false;
    for _ in 0..100 {
        if TcpStream::connect(metrics_addr).is_ok() {
            up = true;
            break;
        }
        thread::sleep(Duration::from_millis(20));
    }
    assert!(up, "metrics endpoint never started");

    let (code, body) = http_get(metrics_addr, "/healthz");
    assert_eq!(code, 200);
    assert!(body.contains("ok"));

    let (code, _) = http_get(metrics_addr, "/readyz");
    assert_eq!(code, 200);

    let (code, body) = http_get(metrics_addr, "/metrics");
    assert_eq!(code, 200);
    // Prometheus exposition format always emits HELP/TYPE comment lines.
    assert!(body.contains("# HELP") || body.contains("# TYPE"));

    let (code, _) = http_get(metrics_addr, "/nope");
    assert_eq!(code, 404);

    *shutdown.lock().unwrap() = true;
    let _ = handle.join();
}
