use std::net::{SocketAddr, TcpListener, TcpStream};
use std::path::PathBuf;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use std::thread;
use std::time::{Duration, Instant};
use tempfile::TempDir;

use zydecodb::config::{Config, SecurityConfig, RequireAuth};
use zydecodb_engine::frame::{Command, RequestEnvelope, ResponseEnvelope, ENVELOPE_HEADER_LEN};
use zydecodb_engine::errors::Status;
use zydecodb::security::keys::{KeyStore, KeyRecord, TenantRecord};
use std::io::{Read, Write};

fn free_addr() -> SocketAddr {
    let l = TcpListener::bind("127.0.0.1:0").unwrap();
    let a = l.local_addr().unwrap();
    drop(l);
    a
}

fn base_config(tmp: &TempDir, listen: SocketAddr) -> Config {
    let data_dir = tmp.path().join("data");
    let wal_dir = tmp.path().join("wal");
    let keys_file = tmp.path().join("keys.toml");
    std::fs::create_dir_all(&data_dir).unwrap();
    std::fs::create_dir_all(&wal_dir).unwrap();

    // Create a keys file with a tenant rate limit
    let key_record = KeyRecord {
        id: "test_key".to_string(),
        secret_hash: zydecodb::security::keys::hash_secret("dummy").unwrap(),
        secret_lookup: Some(zydecodb::security::keys::secret_lookup_hex("dummy")),
        tenant: "00000000000000000000000000000001".to_string(),
        allowed_prefixes: vec![],
        role: zydecodb::security::keys::KeyRole::ReadWrite,
    };
    let tenant_record = TenantRecord {
        tenant: "00000000000000000000000000000001".to_string(),
        max_bytes: None,
        rate_rps: Some(50),
    };
    
    let keys_toml = format!(
        "[[key]]\nid = \"{}\"\nsecret_hash = \"{}\"\nsecret_lookup = \"{}\"\ntenant = \"{}\"\n\n[[tenant]]\ntenant = \"{}\"\nrate_rps = 50\n",
        key_record.id, key_record.secret_hash, key_record.secret_lookup.unwrap(), key_record.tenant, tenant_record.tenant
    );
    std::fs::write(&keys_file, keys_toml).unwrap();

    Config {
        listen,
        data_dir,
        wal_dir,
        block_cache_mb: 64,
        max_open_readers: 32,
        poll_compaction_ms: 50,
        durability: Default::default(),
        fsync_interval_ms: 100,
        shipping: Default::default(),
        metrics: Default::default(),
        replica: Default::default(),
        security: SecurityConfig {
            require_auth: RequireAuth::True,
            keys_file,
            rate_limit_rps: 1000, // High per-connection limit
            max_connections: 1000,
            legacy_single_tenant: false,
            ..Default::default()
        },
        tls: Default::default(),
        listen_unix: None,
        runtime: Default::default(),
    }
}

fn write_request(stream: &mut TcpStream, req: &RequestEnvelope) {
    stream.write_all(&req.encode()).unwrap();
    stream.flush().unwrap();
}

fn read_response(stream: &mut TcpStream) -> ResponseEnvelope {
    let mut header = [0u8; ENVELOPE_HEADER_LEN];
    stream.read_exact(&mut header).unwrap();
    let (status, len) = ResponseEnvelope::parse_header(&header).unwrap();
    let mut payload = vec![0u8; len];
    if len > 0 {
        stream.read_exact(&mut payload).unwrap();
    }
    ResponseEnvelope::new(status, payload)
}

#[test]
fn test_rate_limit_evasion_connection_cycling() {
    let tmp = TempDir::new().unwrap();
    let addr = free_addr();
    let config = base_config(&tmp, addr);
    
    let server = zydecodb::server::Server::new();
    let shutdown = server.shutdown_flag();
    let handle = thread::spawn(move || server.run(config).unwrap());

    // Wait for server to start
    thread::sleep(Duration::from_millis(100));

    let successful_requests = Arc::new(AtomicUsize::new(0));
    let mut threads = vec![];

    let start_time = Instant::now();
    let test_duration = Duration::from_secs(1);

    for _ in 0..20 {
        let addr = addr;
        let successful_requests = successful_requests.clone();
        
        threads.push(thread::spawn(move || {
            let mut local_success = 0;
            while start_time.elapsed() < test_duration {
                if let Ok(mut stream) = TcpStream::connect(addr) {
                    stream.set_read_timeout(Some(Duration::from_millis(500))).unwrap();
                    stream.set_write_timeout(Some(Duration::from_millis(500))).unwrap();

                    // Authenticate
                    let auth_req = RequestEnvelope::new(Command::SessionInit, b"test_key:dummy".to_vec());
                    if stream.write_all(&auth_req.encode()).is_err() {
                        continue;
                    }
                    let mut header = [0u8; ENVELOPE_HEADER_LEN];
                    if stream.read_exact(&mut header).is_err() {
                        continue;
                    }
                    let (status, len) = ResponseEnvelope::parse_header(&header).unwrap();
                    if len > 0 {
                        let mut payload = vec![0u8; len];
                        let _ = stream.read_exact(&mut payload);
                    }
                    if status != Status::Ok {
                        continue;
                    }

                    // Send 5 pings
                    for _ in 0..5 {
                        let req = RequestEnvelope::new(Command::Ping, vec![]);
                        if stream.write_all(&req.encode()).is_err() {
                            break;
                        }
                        if stream.flush().is_err() {
                            break;
                        }

                        let mut header = [0u8; ENVELOPE_HEADER_LEN];
                        if stream.read_exact(&mut header).is_err() {
                            break;
                        }
                        let (status, _) = ResponseEnvelope::parse_header(&header).unwrap();
                        if status == Status::Ok {
                            local_success += 1;
                        } else if status == Status::EngineBusy {
                            // Rate limited
                            break;
                        }
                    }
                }
            }
            successful_requests.fetch_add(local_success, Ordering::Relaxed);
        }));
    }

    for t in threads {
        t.join().unwrap();
    }

    let total = successful_requests.load(Ordering::Relaxed);
    
    // 50 RPS limit * 2 seconds = 100 expected.
    // Allow some burst/timing variance, but it should be nowhere near 50*5*100 = 25000.
    assert!(
        total < 200,
        "VULNERABILITY SURFACED: Rate limit evaded via connection cycling! Total successful: {}",
        total
    );

    *shutdown.lock().unwrap() = true;
    handle.join().unwrap();
}