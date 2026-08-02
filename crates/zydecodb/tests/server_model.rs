//! Server-level model oracle over the wire protocol: spawns `zydecodb serve`
//! as a real subprocess with TLS + auth + quotas enabled and durability=sync,
//! drives a weighted random op loop (put / get / del / transaction), SIGKILLs
//! at random, and after every crash verifies the full keyspace against a
//! reference model. Where engine-model covers the engine in-process, this
//! extends the same differential oracle over the entire P2-hardened server
//! surface: TLS handshake path, SessionInit auth, quota accounting on
//! restart, transaction staging/atomicity, and crash recovery end to end.
//!
//! Ignored — run explicitly:
//!   SERVER_MODEL_STEPS=20000 SERVER_MODEL_SEED=1 \
//!     cargo test --release --test server_model -- --ignored --nocapture
//!
//! Ambiguity model: a write whose RESPONSE is lost to a crash may or may not
//! have applied (group commit acks after fsync, but the kill can land between
//! apply and reply). The model therefore tracks a plausible-value SET per
//! key; any served value must be a member, and a successful read collapses
//! the set. An ambiguous COMMIT is all-or-nothing across its staged keys —
//! partial application is a divergence.

use std::collections::BTreeMap;
use std::io::{Read, Write};
use std::net::{SocketAddr, TcpStream};
use std::path::{Path, PathBuf};
use std::process::{Child, Command as ProcCommand, Stdio};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use tempfile::TempDir;
use zydecodb::security::keys::{KeyRole, KeyStore};
use zydecodb_engine::errors::Status;
use zydecodb_engine::frame::{
    Command, KeyPayload, PutPayload, RequestEnvelope, ResponseEnvelope, ENVELOPE_HEADER_LEN,
};

// ---- deterministic RNG -----------------------------------------------------

struct Xor(u64);
impl Xor {
    fn next(&mut self) -> u64 {
        let mut x = self.0;
        x ^= x << 13;
        x ^= x >> 7;
        x ^= x << 17;
        self.0 = x;
        x
    }
    fn range(&mut self, lo: u64, hi: u64) -> u64 {
        lo + self.next() % (hi - lo + 1)
    }
}

// ---- wire client -----------------------------------------------------------

type TlsStream = rustls::StreamOwned<rustls::ClientConnection, TcpStream>;

struct Client {
    stream: TlsStream,
}

impl Client {
    fn connect(addr: SocketAddr, cert_path: &Path, secret: &str) -> std::io::Result<Self> {
        let tcp = TcpStream::connect(addr)?;
        tcp.set_read_timeout(Some(Duration::from_secs(10))).unwrap();
        tcp.set_write_timeout(Some(Duration::from_secs(10))).unwrap();
        let cert_der = rustls_pemfile::certs(&mut std::io::BufReader::new(
            std::fs::File::open(cert_path)?,
        ))
        .next()
        .unwrap()
        .unwrap();
        let mut roots = rustls::RootCertStore::empty();
        roots.add(cert_der).unwrap();
        let cfg = rustls::ClientConfig::builder()
            .with_root_certificates(roots)
            .with_no_client_auth();
        let name = rustls::pki_types::ServerName::try_from("localhost").unwrap();
        let conn = rustls::ClientConnection::new(Arc::new(cfg), name)
            .map_err(|e| std::io::Error::new(std::io::ErrorKind::Other, e))?;
        let mut stream = rustls::StreamOwned::new(conn, tcp);
        // Authenticate.
        let (st, _) = Self::roundtrip_raw(
            &mut stream,
            &RequestEnvelope::new(Command::SessionInit, secret.as_bytes().to_vec()),
        )?;
        if st != Status::Ok {
            return Err(std::io::Error::new(
                std::io::ErrorKind::PermissionDenied,
                format!("session init rejected: status {st:?}"),
            ));
        }
        Ok(Client { stream })
    }

    fn roundtrip_raw(
        stream: &mut TlsStream,
        req: &RequestEnvelope,
    ) -> std::io::Result<(Status, Vec<u8>)> {
        stream.write_all(&req.encode())?;
        stream.flush()?;
        let mut header = [0u8; ENVELOPE_HEADER_LEN];
        stream.read_exact(&mut header)?;
        let (status, len) = ResponseEnvelope::parse_header(&header)
            .map_err(|e| std::io::Error::new(std::io::ErrorKind::InvalidData, e))?;
        let mut payload = vec![0u8; len];
        if len > 0 {
            stream.read_exact(&mut payload)?;
        }
        Ok((status, payload))
    }

    fn roundtrip(&mut self, req: &RequestEnvelope) -> std::io::Result<(Status, Vec<u8>)> {
        Self::roundtrip_raw(&mut self.stream, req)
    }

    /// EngineBusy means the per-connection rate limiter rejected the request
    /// BEFORE dispatch — the op definitely did not apply, so a bounded retry
    /// is safe and exercises client backpressure.
    fn put(&mut self, key: &[u8], value: &[u8]) -> std::io::Result<Status> {
        let p = PutPayload {
            routing_key: [0u8; 16],
            txid: 0,
            expires_at: 0,
            key: key.to_vec(),
            value: value.to_vec(),
        };
        for _ in 0..100 {
            let st = self.roundtrip(&RequestEnvelope::new(Command::Put, p.encode()))?.0;
            if st != Status::EngineBusy {
                return Ok(st);
            }
            std::thread::sleep(Duration::from_millis(2));
        }
        Ok(Status::EngineBusy)
    }

    fn get(&mut self, key: &[u8]) -> std::io::Result<(Status, Vec<u8>)> {
        let p = KeyPayload {
            routing_key: [0u8; 16],
            snapshot_seq: 0,
            key: key.to_vec(),
        };
        for _ in 0..100 {
            let (st, body) = self.roundtrip(&RequestEnvelope::new(Command::Get, p.encode()))?;
            if st != Status::EngineBusy {
                return Ok((st, body));
            }
            std::thread::sleep(Duration::from_millis(2));
        }
        Ok((Status::EngineBusy, vec![]))
    }

    fn del(&mut self, key: &[u8]) -> std::io::Result<Status> {
        let p = KeyPayload {
            routing_key: [0u8; 16],
            snapshot_seq: 0,
            key: key.to_vec(),
        };
        for _ in 0..100 {
            let st = self.roundtrip(&RequestEnvelope::new(Command::Del, p.encode()))?.0;
            if st != Status::EngineBusy {
                return Ok(st);
            }
            std::thread::sleep(Duration::from_millis(2));
        }
        Ok(Status::EngineBusy)
    }

    fn simple(&mut self, cmd: Command) -> std::io::Result<(Status, Vec<u8>)> {
        self.roundtrip(&RequestEnvelope::new(cmd, vec![]))
    }
}

// ---- subprocess server -----------------------------------------------------

struct ServerChild {
    child: Child,
    addr: SocketAddr,
}

fn free_addr() -> SocketAddr {
    let l = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
    l.local_addr().unwrap()
}

fn write_config(root: &Path, addr: SocketAddr, keys_file: &Path, cert: &Path, key: &Path) -> PathBuf {
    let cfg_path = root.join("zydecodb.toml");
    let text = format!(
        r#"listen = "{addr}"
data_dir = "{data}"
wal_dir = "{wal}"
durability = "sync"
block_cache_mb = 64

[security]
require_auth = "true"
keys_file = "{keys}"
rate_limit_rps = 4000
legacy_single_tenant = true

[security.quotas]
max_bytes_per_tenant = 67108864

[tls]
enabled = true
cert = "{cert}"
key = "{key}"
"#,
        data = root.join("data").display(),
        wal = root.join("wal").display(),
        keys = keys_file.display(),
        cert = cert.display(),
        key = key.display(),
    );
    std::fs::write(&cfg_path, text).unwrap();
    cfg_path
}

fn spawn_server(root: &Path, generation: u32) -> (ServerChild, PathBuf) {
    let addr = free_addr(); // fresh port per generation: no TIME_WAIT rebind race
    let keys_file = root.join("keys.toml");
    let cert = root.join("tls.crt");
    let key = root.join("tls.key");
    let cfg = write_config(root, addr, &keys_file, &cert, &key);
    let stdout = std::fs::File::create(root.join(format!("server-{generation}.out"))).unwrap();
    let stderr = std::fs::File::create(root.join(format!("server-{generation}.err"))).unwrap();
    let child = ProcCommand::new(env!("CARGO_BIN_EXE_zydecodb"))
        .arg("serve")
        .arg("--config")
        .arg(&cfg)
        .stdout(Stdio::from(stdout))
        .stderr(Stdio::from(stderr))
        .spawn()
        .expect("spawn zydecodb serve");
    (ServerChild { child, addr }, cfg)
}

fn wait_ready(root: &Path, addr: SocketAddr, secret: &str, child: &mut Child) -> Client {
    let cert = root.join("tls.crt");
    let deadline = Instant::now() + Duration::from_secs(20);
    loop {
        if let Some(status) = child.try_wait().unwrap() {
            panic!("server exited during startup: {status}");
        }
        match Client::connect(addr, &cert, secret) {
            Ok(c) => return c,
            Err(_) => {
                assert!(Instant::now() < deadline, "server never became ready at {addr}");
                std::thread::sleep(Duration::from_millis(50));
            }
        }
    }
}

// ---- model -----------------------------------------------------------------

/// Plausible values for one key. `None` = deleted. Normally a singleton; a
/// write whose response was lost to a crash adds a candidate until a
/// successful read collapses the set.
type Plausible = Vec<Option<Vec<u8>>>;

struct Model {
    map: BTreeMap<Vec<u8>, Plausible>,
}

impl Model {
    fn new() -> Self {
        Model { map: BTreeMap::new() }
    }

    fn confirm_put(&mut self, key: &[u8], val: Vec<u8>) {
        self.map.insert(key.to_vec(), vec![Some(val)]);
    }
    fn confirm_del(&mut self, key: &[u8]) {
        self.map.insert(key.to_vec(), vec![None]);
    }
    fn maybe_put(&mut self, key: &[u8], val: Vec<u8>) {
        let e = self.map.entry(key.to_vec()).or_insert_with(|| vec![None]);
        if !e.contains(&Some(val.clone())) {
            e.push(Some(val));
        }
    }
    fn maybe_del(&mut self, key: &[u8]) {
        let e = self.map.entry(key.to_vec()).or_insert_with(|| vec![None]);
        if !e.contains(&None) {
            e.push(None);
        }
    }

    /// Check a served value against the plausible set, then collapse.
    fn check_and_collapse(&mut self, step: u64, key: &[u8], served: &Option<Vec<u8>>) {
        let set = self.map.entry(key.to_vec()).or_insert_with(|| vec![None]);
        assert!(
            set.contains(served),
            "step {step}: key {} serves {:?} — not in plausible set ({} candidates)",
            String::from_utf8_lossy(key),
            served.as_ref().map(|v| String::from_utf8_lossy(v)),
            set.len()
        );
        let s = served.clone();
        set.retain(|v| v == &s);
    }
}

fn key_of(id: u64) -> Vec<u8> {
    format!("smkey-{id:05}").into_bytes()
}

// ---- adversarial clients ---------------------------------------------------
//
// These run concurrently with the correctness oracle. They must never crash,
// hang, or contaminate the well-behaved client; the oracle's post-crash
// full-keyspace verify is the assertion that they didn't. Bad-credential
// floods are deliberately NOT here: the auth burst limiter is per-IP, all
// clients share 127.0.0.1, and tripping it would block the oracle client too
// (that axis is covered by rate_limit_evasion.rs).

fn adversary_garbage(stop: Arc<AtomicBool>, addr: Arc<Mutex<SocketAddr>>, seed: u64) {
    let mut rng = Xor(seed);
    while !stop.load(Ordering::Relaxed) {
        let addr = *addr.lock().unwrap();
        if let Ok(mut s) = TcpStream::connect(addr) {
            s.set_write_timeout(Some(Duration::from_millis(500))).ok();
            let mode = rng.range(0, 2);
            match mode {
                // Raw garbage where a TLS ClientHello belongs.
                0 => {
                    let n = rng.range(1, 200) as usize;
                    let mut buf = vec![0u8; n];
                    for b in buf.iter_mut() {
                        *b = rng.next() as u8;
                    }
                    s.write_all(&buf).ok();
                }
                // First byte of a TLS handshake, then silence and close.
                1 => {
                    s.write_all(&[0x16, 0x03, 0x01]).ok();
                    std::thread::sleep(Duration::from_millis(rng.range(5, 100)));
                }
                // A plaintext wire frame (valid shape, no TLS) with a huge
                // declared payload — pre-auth cap must reject it.
                _ => {
                    let mut f = vec![0x01u8, 0x02];
                    f.extend_from_slice(&u32::MAX.to_be_bytes());
                    s.write_all(&f).ok();
                }
            }
            std::thread::sleep(Duration::from_millis(rng.range(0, 10)));
        } else {
            std::thread::sleep(Duration::from_millis(50));
        }
    }
}

fn adversary_flooder(
    stop: Arc<AtomicBool>,
    addr: Arc<Mutex<SocketAddr>>,
    root: PathBuf,
    secret: String,
    seed: u64,
) {
    let mut rng = Xor(seed);
    let cert = root.join("tls.crt");
    while !stop.load(Ordering::Relaxed) {
        let a = *addr.lock().unwrap();
        let Ok(mut c) = Client::connect(a, &cert, &secret) else {
            std::thread::sleep(Duration::from_millis(50));
            continue;
        };
        // Blast until the per-connection limiter pushes back or the server
        // dies under us; junk keys live outside the model keyspace.
        for i in 0..2000u32 {
            let k = format!("advk-{:06}", rng.range(0, 10_000)).into_bytes();
            match c.put(&k, b"x") {
                Ok(Status::Ok) | Ok(Status::EngineBusy) => {}
                Ok(_) | Err(_) => break,
            }
            if i % 64 == 0 && stop.load(Ordering::Relaxed) {
                break;
            }
        }
    }
}

fn adversary_postauth_malformed(
    stop: Arc<AtomicBool>,
    addr: Arc<Mutex<SocketAddr>>,
    root: PathBuf,
    secret: String,
    seed: u64,
) {
    let mut rng = Xor(seed);
    let cert = root.join("tls.crt");
    while !stop.load(Ordering::Relaxed) {
        let a = *addr.lock().unwrap();
        let Ok(mut c) = Client::connect(a, &cert, &secret) else {
            std::thread::sleep(Duration::from_millis(50));
            continue;
        };
        match rng.range(0, 2) {
            // Unknown opcode: server answers ProtocolError, stays connected.
            0 => {
                let mut raw = vec![0x01u8, 0x77];
                raw.extend_from_slice(&3u32.to_be_bytes());
                raw.extend_from_slice(b"abc");
                if c.stream.write_all(&raw).is_ok() {
                    let mut header = [0u8; ENVELOPE_HEADER_LEN];
                    c.stream.read_exact(&mut header).ok();
                }
            }
            // Bad protocol version byte.
            1 => {
                let mut raw = vec![0xEEu8, 0x02];
                raw.extend_from_slice(&0u32.to_be_bytes());
                c.stream.write_all(&raw).ok();
            }
            // Valid header, truncated payload, immediate close.
            _ => {
                let mut raw = vec![0x01u8, 0x02];
                raw.extend_from_slice(&500u32.to_be_bytes());
                raw.extend_from_slice(b"short");
                c.stream.write_all(&raw).ok();
            }
        }
        std::thread::sleep(Duration::from_millis(rng.range(0, 5)));
    }
}

fn describe(o: &Option<Vec<u8>>) -> String {
    match o {
        None => "<absent>".into(),
        Some(v) => String::from_utf8_lossy(v).into_owned(),
    }
}

// ---- the test --------------------------------------------------------------

#[test]
#[ignore]
fn server_model_wire_oracle() {
    let steps: u64 = std::env::var("SERVER_MODEL_STEPS")
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or(8_000);
    let seed: u64 = std::env::var("SERVER_MODEL_SEED")
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or(0x5EED);
    let keyspace: u64 = 256;
    let mut rng = Xor(seed.max(1));

    let _ = rustls::crypto::aws_lc_rs::default_provider().install_default();

    let tmp = TempDir::new().unwrap();
    let root = tmp.path().to_path_buf();
    if std::env::var("SERVER_MODEL_KEEP").is_ok() {
        eprintln!("server-model keeping artifacts at {}", root.display());
        std::mem::forget(tmp);
    }
    std::fs::create_dir_all(root.join("data")).unwrap();
    std::fs::create_dir_all(root.join("wal")).unwrap();

    // API key (tenant zero, full access).
    let keys_file = root.join("keys.toml");
    let secret = KeyStore::create_key(
        &keys_file,
        "model",
        KeyRole::ReadWrite,
        "00000000000000000000000000000000",
        vec![],
    )
    .unwrap();

    // Self-signed TLS identity.
    let key_pair = rcgen::KeyPair::generate().unwrap();
    let mut params = rcgen::CertificateParams::new(vec!["localhost".to_string()]).unwrap();
    params.distinguished_name.push(
        rcgen::DnType::CommonName,
        rcgen::DnValue::Utf8String("localhost".into()),
    );
    let cert = params.self_signed(&key_pair).unwrap();
    std::fs::write(root.join("tls.crt"), cert.pem()).unwrap();
    std::fs::write(root.join("tls.key"), key_pair.serialize_pem()).unwrap();

    let mut model = Model::new();
    let mut generation = 0u32;
    let (mut srv, _) = spawn_server(&root, generation);
    let mut client = wait_ready(&root, srv.addr, &secret, &mut srv.child);

    // Adversarial pressure concurrent with the oracle: malformed pre-auth
    // bytes, TLS dribble, per-connection flooding, post-auth protocol abuse.
    let adv_stop = Arc::new(AtomicBool::new(false));
    let shared_addr = Arc::new(Mutex::new(srv.addr));
    let adversaries_on = std::env::var("SERVER_MODEL_NO_ADV").is_err();
    let mut adv_handles = Vec::new();
    if adversaries_on {
        let (a, b, c) = (adv_stop.clone(), shared_addr.clone(), root.clone());
        adv_handles.push(std::thread::spawn(move || adversary_garbage(a, b, seed ^ 0xA1)));
        adv_handles.push(std::thread::spawn({
            let (a, b, r, s) = (adv_stop.clone(), shared_addr.clone(), root.clone(), secret.clone());
            move || adversary_flooder(a, b, r, s, seed ^ 0xF10D)
        }));
        adv_handles.push(std::thread::spawn({
            let (a, b, r, s) = (adv_stop.clone(), shared_addr.clone(), root.clone(), secret.clone());
            move || adversary_postauth_malformed(a, b, r, s, seed ^ 0xBAAD)
        }));
        let _ = c;
    }

    let mut op_counter = 0u64;
    let mut crashes = 0u64;
    let mut txns = 0u64;
    let started = Instant::now();

    let make_val = |op: u64, rng: &mut Xor| format!("sm-{op:08}-{:016x}", rng.next()).into_bytes();

    for step in 0..steps {
        let roll = rng.range(0, 999);
        if roll < 4 {
            // -- Crash: SIGKILL, respawn on the same dirs, verify everything.
            srv.child.kill().unwrap();
            srv.child.wait().unwrap();
            crashes += 1;
            generation += 1;
            let (mut next, _) = spawn_server(&root, generation);
            client = wait_ready(&root, next.addr, &secret, &mut next.child);
            *shared_addr.lock().unwrap() = next.addr;
            srv = next;
            for id in 0..keyspace {
                let k = key_of(id);
                let (st, body) = client
                    .get(&k)
                    .expect("post-crash get must succeed transport-wise");
                let served = match st {
                    Status::Ok => Some(body),
                    Status::NotFound => None,
                    other => panic!("step {step}: get status {other:?}"),
                };
                model.check_and_collapse(step, &k, &served);
            }
            continue;
        }

        match rng.range(0, 99) {
            // Put (40%)
            0..=39 => {
                let id = rng.next() % keyspace;
                let k = key_of(id);
                let v = make_val(op_counter, &mut rng);
                op_counter += 1;
                match client.put(&k, &v) {
                    Ok(Status::Ok) => model.confirm_put(&k, v),
                    Ok(Status::EngineBusy) => {} // rejected pre-dispatch: not applied
                    Ok(st) => panic!("step {step}: put status {st:?}"),
                    Err(_) => model.maybe_put(&k, v), // response lost; maybe applied
                }
            }
            // Get (35%)
            40..=74 => {
                let id = rng.next() % keyspace;
                let k = key_of(id);
                match client.get(&k) {
                    Ok((Status::Ok, body)) => model.check_and_collapse(step, &k, &Some(body)),
                    Ok((Status::NotFound, _)) => model.check_and_collapse(step, &k, &None),
                    Ok((Status::EngineBusy, _)) => {} // rejected pre-dispatch
                    Ok((st, _)) => panic!("step {step}: get status {st:?}"),
                    Err(_) => {} // transport error: no information gained
                }
            }
            // Del (10%)
            75..=84 => {
                let id = rng.next() % keyspace;
                let k = key_of(id);
                match client.del(&k) {
                    Ok(Status::Ok) => model.confirm_del(&k),
                    Ok(Status::EngineBusy) => {} // rejected pre-dispatch
                    Ok(st) => panic!("step {step}: del status {st:?}"),
                    Err(_) => model.maybe_del(&k),
                }
            }
            // Transaction (15%): begin, stage 2-4 ops, read-your-writes probe,
            // then commit (90%) or rollback (10%).
            _ => {
                txns += 1;
                let (st, _) = client.simple(Command::Begin).expect("begin transport");
                assert_eq!(st, Status::Ok, "step {step}: begin rejected");

                let n = rng.range(2, 4) as usize;
                let mut staged: Vec<(Vec<u8>, Option<Vec<u8>>)> = Vec::new();
                let mut ids: Vec<u64> = Vec::new();
                while ids.len() < n {
                    let id = rng.next() % keyspace;
                    if !ids.contains(&id) {
                        ids.push(id);
                    }
                }
                let mut staging_failed = false;
                for &id in &ids {
                    let k = key_of(id);
                    if rng.range(0, 99) < 75 {
                        let v = make_val(op_counter, &mut rng);
                        op_counter += 1;
                        let st = client.put(&k, &v).expect("staged put transport");
                        if st == Status::EngineBusy {
                            staging_failed = true;
                            break;
                        }
                        assert_eq!(st, Status::Ok, "step {step}: staged put rejected");
                        staged.push((k, Some(v)));
                    } else {
                        let st = client.del(&k).expect("staged del transport");
                        if st == Status::EngineBusy {
                            staging_failed = true;
                            break;
                        }
                        assert_eq!(st, Status::Ok, "step {step}: staged del rejected");
                        staged.push((k, None));
                    }
                }
                if staging_failed {
                    // Rate-limited mid-staging: abandon cleanly.
                    let (st, _) = client.simple(Command::Rollback).expect("rollback transport");
                    assert_eq!(st, Status::Ok);
                    continue;
                }
                // Read-your-writes: first staged key must serve the staged value.
                let (probe_k, probe_v) = staged[0].clone();
                let (st, body) = client.get(&probe_k).expect("staged get transport");
                let served = match st {
                    Status::Ok => Some(body),
                    Status::NotFound => None,
                    other => panic!("step {step}: staged get status {other:?}"),
                };
                assert_eq!(
                    served,
                    probe_v,
                    "step {step}: read-your-writes broken inside txn",
                );

                if rng.range(0, 99) < 90 {
                    match client.simple(Command::Commit) {
                        Ok((Status::Ok, _)) => {
                            for (k, v) in staged {
                                match v {
                                    Some(v) => model.confirm_put(&k, v),
                                    None => model.confirm_del(&k),
                                }
                            }
                        }
                        Ok((st, body)) => panic!(
                            "step {step}: commit status {st:?}: {}",
                            String::from_utf8_lossy(&body)
                        ),
                        Err(_) => {
                            // Commit response lost: all-or-nothing ambiguity.
                            // Resolve on the live connection (server is up).
                            let mut all_new = true;
                            let mut all_old = true;
                            for (k, v) in &staged {
                                let (gst, body) = client.get(k).expect("resolve get");
                                let served = match gst {
                                    Status::Ok => Some(body),
                                    Status::NotFound => None,
                                    other => panic!("step {step}: resolve status {other:?}"),
                                };
                                if &served == v {
                                    all_old = false;
                                } else {
                                    all_new = false;
                                }
                            }
                            assert!(
                                all_new || all_old,
                                "step {step}: PARTIAL TRANSACTION APPLICATION — atomicity broken"
                            );
                            for (k, v) in staged {
                                if all_new {
                                    match v {
                                        Some(v) => model.confirm_put(&k, v),
                                        None => model.confirm_del(&k),
                                    }
                                } else {
                                    model.check_and_collapse(step, &k, &model.map[&k][0].clone());
                                }
                            }
                        }
                    }
                } else {
                    let (st, _) = client.simple(Command::Rollback).expect("rollback transport");
                    assert_eq!(st, Status::Ok, "step {step}: rollback rejected");
                    // Staged ops must be invisible now.
                    for (k, _) in &staged {
                        let (gst, body) = client.get(k).expect("post-rollback get");
                        let served = match gst {
                            Status::Ok => Some(body),
                            Status::NotFound => None,
                            other => panic!("step {step}: post-rollback status {other:?}"),
                        };
                        model.check_and_collapse(step, k, &served);
                    }
                }
            }
        }
    }

    // Stop adversaries, then the last server generation.
    adv_stop.store(true, Ordering::Relaxed);
    for h in adv_handles {
        h.join().unwrap();
    }
    srv.child.kill().ok();
    srv.child.wait().ok();

    eprintln!(
        "server-model done: steps={} crashes={} txns={} ops={} keys={} elapsed={:?}",
        steps,
        crashes,
        txns,
        op_counter,
        model.map.len(),
        started.elapsed()
    );
}
