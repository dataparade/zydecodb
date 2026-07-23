use crate::admin_dispatch::{handle_admin_drop_tenant, is_admin_command};
use crate::commit::CommitCoordinator;
use crate::config::Config;
use crate::dispatch::{handle_request, write_response};
use crate::docdispatch::handle_document;
use crate::security::ratelimit::RateLimiter;
use crate::security::tls::{accept as tls_accept, load_server_config};
use crate::security::{SecurityRuntime, SessionState};
use crate::shared::{SharedCatalog, SharedEngine};
use std::io::{ErrorKind, Read, Write};
use std::net::{IpAddr, Ipv4Addr, Shutdown, TcpListener, TcpStream};
use std::os::unix::net::{UnixListener, UnixStream};
use std::sync::{Arc, Condvar, Mutex, RwLock};
use std::thread::{self, JoinHandle};
use std::time::{Duration, Instant};
use tracing::{error, info, warn};
use zydecodb_document::catalog::Catalog;
use zydecodb_engine::engine::{Engine, EngineConfig};
use zydecodb_engine::engine_handle::EngineHandle;
use zydecodb_engine::frame::Command;

/// Optional per-tenant request metrics, registered on the shared Prometheus
/// registry when `[metrics].per_tenant` is set. A single counter vector keeps
/// cardinality bounded to (tenants x commands x statuses).
struct TenantMetrics {
    requests: prometheus::IntCounterVec,
}

impl TenantMetrics {
    fn register(registry: &prometheus::Registry) -> Option<Arc<Self>> {
        let requests = prometheus::IntCounterVec::new(
            prometheus::Opts::new(
                "zydecodb_tenant_requests_total",
                "Requests handled, labeled by tenant, command, and result status.",
            ),
            &["tenant", "command", "status"],
        )
        .ok()?;
        registry.register(Box::new(requests.clone())).ok()?;
        Some(Arc::new(TenantMetrics { requests }))
    }

    fn record(&self, tenant: &[u8; 16], command: &str, status: &str) {
        let tenant_hex: String = tenant.iter().map(|b| format!("{b:02x}")).collect();
        self.requests
            .with_label_values(&[&tenant_hex, command, status])
            .inc();
    }
}

/// Poller key identifying readiness on the TCP listener.
const TCP_KEY: usize = 0;
/// Poller key identifying readiness on the optional Unix-domain-socket listener.
const UDS_KEY: usize = 1;

/// Sleep up to `dur` on a background timer thread, returning early the moment
/// shutdown is signaled. Returns `true` if the thread should stop. Replaces the
/// old `thread::sleep` busy-poll so idle pods wake on shutdown instead of on the
/// next tick (and tests that set the flag still observe it within `dur`).
fn wait_or_shutdown(shutdown: &Mutex<bool>, wake: &Condvar, dur: Duration) -> bool {
    let guard = shutdown.lock().unwrap();
    if *guard {
        return true;
    }
    let (guard, _timeout) = wake.wait_timeout(guard, dur).unwrap();
    *guard
}

/// Acquire a connection slot and spawn a `zydecodb-conn` thread to serve a freshly
/// accepted TCP stream. Releases the slot if the limit is hit or the spawn fails.
#[allow(clippy::too_many_arguments)] // Full shared server context for one connection.
fn spawn_tcp_conn(
    engine: &SharedEngine,
    catalog: &SharedCatalog,
    commit: &Arc<CommitCoordinator>,
    security: &Arc<SecurityRuntime>,
    shutdown: &Arc<Mutex<bool>>,
    tls_config: &Option<Arc<rustls::ServerConfig>>,
    tenant_metrics: &Option<Arc<TenantMetrics>>,
    conns: &mut Vec<JoinHandle<()>>,
    stream: TcpStream,
    peer_ip: std::net::IpAddr,
) {
    if !security.try_acquire_connection() {
        warn!(%peer_ip, "max connections reached, dropping");
        return;
    }
    let engine = Arc::clone(engine);
    let catalog = Arc::clone(catalog);
    let commit = Arc::clone(commit);
    let conn_security = Arc::clone(security);
    let shutdown = Arc::clone(shutdown);
    let tls_config = tls_config.clone();
    let tenant_metrics = tenant_metrics.clone();
    let spawned = thread::Builder::new()
        .name("zydecodb-conn".into())
        .spawn(move || {
            if let Err(e) = serve_tcp_connection(
                &engine,
                &catalog,
                &commit,
                stream,
                peer_ip,
                &conn_security,
                &shutdown,
                tls_config,
                &tenant_metrics,
            ) {
                if !e.to_string().contains("timed out") {
                    error!(error = %e, "connection error");
                }
            }
            conn_security.release_connection();
        });
    match spawned {
        Ok(handle) => conns.push(handle),
        Err(e) => {
            error!(error = %e, "failed to spawn connection thread");
            // The slot was acquired above; release it since no thread will.
            security.release_connection();
        }
    }
}

/// Like [`spawn_tcp_conn`] but for a Unix-domain-socket connection. There is no
/// TLS over UDS; the peer is reported as loopback for the IP-based auth-burst
/// limiter (all local connections share one bucket).
#[allow(clippy::too_many_arguments)] // Full shared server context for one connection.
fn spawn_uds_conn(
    engine: &SharedEngine,
    catalog: &SharedCatalog,
    commit: &Arc<CommitCoordinator>,
    security: &Arc<SecurityRuntime>,
    shutdown: &Arc<Mutex<bool>>,
    tenant_metrics: &Option<Arc<TenantMetrics>>,
    conns: &mut Vec<JoinHandle<()>>,
    stream: UnixStream,
) {
    if !security.try_acquire_connection() {
        warn!("max connections reached, dropping unix-socket connection");
        return;
    }
    let engine = Arc::clone(engine);
    let catalog = Arc::clone(catalog);
    let commit = Arc::clone(commit);
    let conn_security = Arc::clone(security);
    let shutdown = Arc::clone(shutdown);
    let tenant_metrics = tenant_metrics.clone();
    let spawned = thread::Builder::new()
        .name("zydecodb-conn".into())
        .spawn(move || {
            if let Err(e) = serve_uds_connection(
                &engine,
                &catalog,
                &commit,
                stream,
                &conn_security,
                &shutdown,
                &tenant_metrics,
            ) {
                if !e.to_string().contains("timed out") {
                    error!(error = %e, "unix-socket connection error");
                }
            }
            conn_security.release_connection();
        });
    match spawned {
        Ok(handle) => conns.push(handle),
        Err(e) => {
            error!(error = %e, "failed to spawn unix-socket connection thread");
            security.release_connection();
        }
    }
}

pub struct Server {
    shutdown: Arc<Mutex<bool>>,
    /// Paired with `shutdown` so background timer threads can block on
    /// `wait_timeout` and wake instantly when shutdown is signaled (instead of
    /// busy-polling a boolean). Notified on the signal-driven shutdown path.
    wake: Arc<Condvar>,
}

impl Default for Server {
    fn default() -> Self {
        Self::new()
    }
}

impl Server {
    pub fn new() -> Self {
        Server {
            shutdown: Arc::new(Mutex::new(false)),
            wake: Arc::new(Condvar::new()),
        }
    }

    pub fn shutdown_flag(&self) -> Arc<Mutex<bool>> {
        Arc::clone(&self.shutdown)
    }

    pub fn run(&self, config: Config) -> Result<(), Box<dyn std::error::Error>> {
        if std::env::var_os("ZYDECODB_BOOTSTRAP_KEY").is_some() && !config.listen.ip().is_loopback()
        {
            return Err(
                "ZYDECODB_BOOTSTRAP_KEY is set but listen is not loopback — refuse to start \
                 (bootstrap auth is loopback/dev-only; use a keys file for networked binds)"
                    .into(),
            );
        }

        let security = Arc::new(SecurityRuntime::from_config(&config)?);
        if security.require_auth {
            info!("authentication required for this listen address");
        }

        // Fail closed: auth required but nothing can ever authenticate. A
        // server in that state looks healthy while rejecting every client —
        // or worse, tempts the operator into flipping require_auth off.
        {
            let keys = security.keys.load();
            if security.require_auth && keys.record_count() == 0 && !keys.has_bootstrap() {
                return Err(format!(
                    "refusing to start: authentication is required but the keys file {} has no \
                     keys — create one with `zydecodb admin keys create --keys-file {}`",
                    config.security.keys_file.display(),
                    config.security.keys_file.display(),
                )
                .into());
            }

            // legacy_single_tenant collapses the all-zero tenant onto the
            // un-prefixed key layout. Mixing that with real (non-zero) tenant
            // keys silently splits the keyspace into two layouts; refuse the
            // ambiguous configuration instead.
            if config.security.legacy_single_tenant {
                let zero = "00000000000000000000000000000000";
                if let Some(bad) = keys.records().iter().find(|r| r.tenant != zero) {
                    return Err(format!(
                        "refusing to start: legacy_single_tenant = true but key '{}' has \
                         non-zero tenant {} — set legacy_single_tenant = false for \
                         multi-tenant deployments (or keep all keys on the zero tenant)",
                        bad.id, bad.tenant,
                    )
                    .into());
                }
            }
        }

        let engine_cfg = config.to_engine_config();
        if engine_cfg.fair.enabled {
            info!(
                tenants = engine_cfg.fair.tenant_count,
                delta_steady_ms = engine_cfg.fair.delta_steady.as_millis() as u64,
                "δ-fair multi-tenant isolation enabled"
            );
        }

        // Read-replica mode: ingest verified WAL segments shipped by a primary
        // into our WAL dir before opening, so the initial replay already
        // reflects everything delivered so far. A poll thread (spawned below)
        // keeps catching up. The shipped manifest must be HMAC-authenticated:
        // a replica ingesting an unauthenticated stream would trust whatever
        // an attacker with write access to the ship path put there.
        let mut replica = match config.replica.from.clone() {
            Some(from) => {
                let key_file = config.replica.hmac_key_file.as_ref().ok_or(
                    "replica.from is set but replica.hmac_key_file is missing — the shipped \
                     stream must be HMAC-authenticated (share the primary's shipping key)",
                )?;
                let key = crate::config::load_hmac_key(key_file)?;
                info!(dir = %from.display(), "starting as READ REPLICA (read-only)");
                let mut rep = crate::replica::Replica::new(from, config.wal_dir.clone())
                    .with_hmac_key(Some(key));
                let out = rep.sync()?;
                info!(
                    installed = out.installed.len(),
                    max_seq = out.max_seq,
                    "initial replica sync complete"
                );
                Some(rep)
            }
            None => None,
        };

        let mut engine = Engine::open(engine_cfg.clone())?;

        // Install the byte-cap write policy when there is a global per-tenant cap
        // or any per-tenant override in the keys file. The effective cap per
        // tenant is its override, else the global default (0 = unlimited).
        let global_tenant_cap = config.security.quotas.max_bytes_per_tenant;
        if global_tenant_cap > 0 || security.tenant_limits.any_byte_cap() {
            engine =
                engine.with_write_policy(Arc::new(crate::security::quota::TenantQuotaPolicy::new(
                    global_tenant_cap,
                    Arc::clone(&security.tenant_limits),
                )));
        }

        // WAL shipping: sealed segments are transported into ship_dir for an
        // off-box sidecar / read replica (see Phase 5 --replica-from).
        if let Some(ship_dir) = config.shipping.ship_dir.clone() {
            // Cooperative fence: if this stream already carries a higher epoch
            // than ours, a newer primary was promoted past us. Refuse to start
            // rather than risk two primaries writing the same shipped stream.
            // (Best-effort, shared-stream only; hard fencing is the operator's.)
            if config.replica.from.is_none() {
                let node_epoch = crate::replica::read_epoch(&config.data_dir);
                if let Some(fence_epoch) = crate::replica::read_fence(&ship_dir) {
                    if fence_epoch > node_epoch {
                        return Err(format!(
                            "refusing to start: shipped stream fence epoch {} is newer than this node's epoch {} (a replica was promoted past this primary)",
                            fence_epoch, node_epoch
                        )
                        .into());
                    }
                }
                crate::replica::write_fence(&ship_dir, node_epoch)?;
                info!(epoch = node_epoch, dir = %ship_dir.display(), "stamped shipping fence epoch");
            }
            let key_file = config.shipping.hmac_key_file.as_ref().ok_or(
                "shipping.ship_dir is set but shipping.hmac_key_file is missing — shipped \
                 manifests must be HMAC-authenticated (generate a key: \
                 `head -c 32 /dev/urandom > ship.hmac && chmod 600 ship.hmac`)",
            )?;
            let hmac_key = crate::config::load_hmac_key(key_file)?;
            let mode =
                zydecodb_engine::shipping::ShipMode::from_str_or_default(&config.shipping.mode);
            engine = engine
                .with_shipping(Some(ship_dir.clone()), mode)
                .with_shipping_hmac_key(Some(hmac_key));
            info!(dir = %ship_dir.display(), ?mode, "WAL shipping enabled (HMAC-authenticated)");
        }

        // Always construct and attach the metrics registry so it is populated
        // even when the HTTP endpoint is off; the endpoint (if any) renders it.
        let metrics = zydecodb_engine::metrics::Metrics::new();
        engine = engine.with_metrics(Arc::clone(&metrics));

        // Optional per-tenant request metrics on the shared registry.
        let tenant_metrics: Option<Arc<TenantMetrics>> = if config.metrics.per_tenant {
            let m = TenantMetrics::register(&metrics.registry);
            if m.is_some() {
                info!("per-tenant request metrics enabled");
            }
            m
        } else {
            None
        };

        // Load the document catalog before sharing the engine; reads of it
        // afterward never need the engine lock.
        let catalog: SharedCatalog = Arc::new(RwLock::new(
            Catalog::load(&engine).map_err(|e| e.to_string())?,
        ));

        let engine: SharedEngine = EngineHandle::new(engine);

        // Durability: the engine buffers WAL appends; the commit coordinator owns
        // the fsync so acknowledged writes are durable by default (or on a bounded
        // interval in periodic mode). See crate::commit.
        let durability = config.commit_durability();
        let commit = CommitCoordinator::new(&engine, durability);
        let commit_thread = commit.spawn()?;
        info!(?durability, "commit coordinator started");

        let tls_config = if config.tls.enabled {
            let cert = config
                .tls
                .cert
                .as_ref()
                .ok_or("tls.enabled requires tls.cert")?;
            let key = config
                .tls
                .key
                .as_ref()
                .ok_or("tls.enabled requires tls.key")?;
            Some(load_server_config(cert, key)?)
        } else {
            None
        };

        // Operability HTTP endpoint (Prometheus /metrics, /healthz, /readyz) on
        // its own address, separate from the data-plane socket. Non-loopback
        // binds are refused unless explicitly allowed with a token.
        let metrics_http = match config.metrics.listen {
            Some(addr) => {
                crate::metrics_http::check_bind_policy(
                    &addr,
                    config.metrics.allow_remote,
                    config.metrics.token.as_deref(),
                )?;
                Some(crate::metrics_http::spawn(
                    addr,
                    Arc::clone(&metrics),
                    Arc::clone(&self.shutdown),
                    config.metrics.token.clone(),
                )?)
            }
            None => None,
        };

        let listener = TcpListener::bind(config.listen)?;
        listener.set_nonblocking(true)?;
        info!(addr = %config.listen, tls = config.tls.enabled, "ZydecoDB listening");

        // Optional Unix-domain socket for local control-plane traffic. Removing a
        // stale socket from a prior run is required because bind() fails if the
        // path already exists. The file's permissions are the trust boundary;
        // TLS does not apply (API-key auth still does). We chmod to 0600
        // explicitly so the process umask can never leave the socket
        // world-connectable.
        let uds_listener: Option<(UnixListener, std::path::PathBuf)> =
            match config.listen_unix.clone() {
                Some(path) => {
                    let _ = std::fs::remove_file(&path);
                    let l = UnixListener::bind(&path)?;
                    {
                        use std::os::unix::fs::PermissionsExt;
                        std::fs::set_permissions(&path, std::fs::Permissions::from_mode(0o600))?;
                    }
                    l.set_nonblocking(true)?;
                    info!(path = %path.display(), "ZydecoDB listening on unix socket (mode 0600)");
                    Some((l, path))
                }
                None => None,
            };

        // Dedicated maintenance thread: drives background compaction apply on a
        // fixed cadence, independent of connection activity, so a busy client
        // can never starve catalog maintenance.
        let maintenance = {
            let engine = Arc::clone(&engine);
            let shutdown = Arc::clone(&self.shutdown);
            let wake = Arc::clone(&self.wake);
            let poll_interval = Duration::from_millis(config.poll_compaction_ms.max(1));
            thread::Builder::new()
                .name("zydecodb-maintenance".into())
                .spawn(move || loop {
                    if *shutdown.lock().unwrap() {
                        break;
                    }
                    if let Ok(mut e) = engine.try_write() {
                        let _ = e.poll_compaction();
                    }
                    if wait_or_shutdown(&shutdown, &wake, poll_interval) {
                        break;
                    }
                })?
        };

        // Primary heartbeat: refresh a liveness marker in the shipped stream on a
        // fixed cadence (even while idle) so a replica can tell a quiet primary
        // from a dead one. Only a primary (not a replica) heartbeats.
        let heartbeat_thread: Option<JoinHandle<()>> = match config.shipping.ship_dir.clone() {
            Some(ship_dir) if config.shipping.heartbeat_ms > 0 && config.replica.from.is_none() => {
                let engine = Arc::clone(&engine);
                let shutdown = Arc::clone(&self.shutdown);
                let wake = Arc::clone(&self.wake);
                let interval = Duration::from_millis(config.shipping.heartbeat_ms);
                info!(dir = %ship_dir.display(), every_ms = config.shipping.heartbeat_ms, "primary heartbeat enabled");
                Some(
                    thread::Builder::new()
                        .name("zydecodb-heartbeat".into())
                        .spawn(move || loop {
                            if *shutdown.lock().unwrap() {
                                break;
                            }
                            let seq = engine.read().current_seq();
                            let now = std::time::SystemTime::now()
                                .duration_since(std::time::UNIX_EPOCH)
                                .map(|d| d.as_millis() as u64)
                                .unwrap_or(0);
                            if let Err(e) =
                                zydecodb_engine::shipping::write_heartbeat(&ship_dir, now, seq)
                            {
                                warn!(error = %e, "heartbeat write failed");
                            }
                            // Roll a (time, seq) sample for best-effort PITR --to-time.
                            let _ =
                                zydecodb_engine::shipping::append_timeindex(&ship_dir, now, seq);
                            if wait_or_shutdown(&shutdown, &wake, interval) {
                                break;
                            }
                        })?,
                )
            }
            _ => None,
        };

        // Lazy TTL sweep: expire engine-level `expires_at` entries on a cadence
        // so document TTL (and raw-KV TTL) become unreachable without waiting
        // for a natural read.
        let sweep_thread = {
            let engine = Arc::clone(&engine);
            let shutdown = Arc::clone(&self.shutdown);
            let wake = Arc::clone(&self.wake);
            let interval = Duration::from_secs(30);
            thread::Builder::new()
                .name("zydecodb-ttl-sweep".into())
                .spawn(move || loop {
                    if wait_or_shutdown(&shutdown, &wake, interval) {
                        break;
                    }
                    if let Ok(mut e) = engine.try_write() {
                        match e.sweep_expired() {
                            Ok(n) if n > 0 => info!(expired = n, "TTL sweep removed entries"),
                            Ok(_) => {}
                            Err(err) => warn!(error = %err, "TTL sweep failed"),
                        }
                    }
                })?
        };

        // Replica catch-up: periodically ingest newly shipped segments, then
        // apply them into the live engine (incremental) or reopen on failure.
        let replica_thread: Option<JoinHandle<()>> = match replica.take() {
            Some(mut rep) => {
                let engine = Arc::clone(&engine);
                let catalog = Arc::clone(&catalog);
                let metrics = Arc::clone(&metrics);
                let shutdown = Arc::clone(&self.shutdown);
                let wake = Arc::clone(&self.wake);
                let engine_cfg = engine_cfg.clone();
                let poll = Duration::from_millis(config.replica.poll_ms.max(50));
                let from = config.replica.from.clone().unwrap_or_default();
                let wal_dir = config.wal_dir.clone();
                // Live replica observability, registered into the shared registry
                // so the /metrics endpoint renders it alongside core counters.
                let lag_gauge = prometheus::IntGauge::with_opts(prometheus::Opts::new(
                    "zydecodb_replica_lag_seqs",
                    "Replica lag behind the primary, in write sequences.",
                ))
                .expect("valid gauge opts");
                let hb_age_gauge = prometheus::IntGauge::with_opts(prometheus::Opts::new(
                    "zydecodb_replica_heartbeat_age_seconds",
                    "Seconds since the primary's last shipped heartbeat (-1 if none).",
                ))
                .expect("valid gauge opts");
                let _ = metrics.registry.register(Box::new(lag_gauge.clone()));
                let _ = metrics.registry.register(Box::new(hb_age_gauge.clone()));
                Some(
                    thread::Builder::new()
                        .name("zydecodb-replica".into())
                        .spawn(move || loop {
                            if wait_or_shutdown(&shutdown, &wake, poll) {
                                break;
                            }
                            match rep.sync() {
                                Ok(out) if out.made_progress() => {
                                    if let Err(e) = catch_up_replica(
                                        &engine,
                                        &catalog,
                                        &metrics,
                                        &engine_cfg,
                                        &out.installed,
                                    ) {
                                        error!(error = %e, "replica catch-up failed");
                                    } else {
                                        info!(
                                            installed = ?out.installed,
                                            max_seq = out.max_seq,
                                            "replica caught up"
                                        );
                                    }
                                }
                                Ok(_) => {}
                                Err(e) => error!(error = %e, "replica sync failed"),
                            }
                            // Refresh observability each pass (cheap file reads).
                            if let Ok(report) = crate::replica::status(&from, &wal_dir, u64::MAX) {
                                lag_gauge.set(report.seq_lag as i64);
                                hb_age_gauge
                                    .set(report.heartbeat_age_secs.map(|a| a as i64).unwrap_or(-1));
                            }
                        })?,
                )
            }
            None => None,
        };

        let mut conns: Vec<JoinHandle<()>> = Vec::new();

        // Event-driven accept: block in epoll/kqueue until a connection arrives,
        // the signal thread wakes us, or a short fallback timeout elapses. This
        // replaces the old ~100/sec nonblocking-accept spin so an idle pod stays
        // quiet while still picking up new connections instantly.
        let poller = Arc::new(polling::Poller::new()?);
        // SAFETY: `listener` outlives this registration; we `delete` it during
        // teardown below before `listener` is dropped.
        unsafe {
            poller.add(&listener, polling::Event::readable(TCP_KEY))?;
            if let Some((ref l, _)) = uds_listener {
                poller.add(l, polling::Event::readable(UDS_KEY))?;
            }
        }

        // Signal handling on a dedicated thread:
        //   - SIGTERM/SIGINT: flip the shutdown flag and wake the timer threads
        //     (condvar) and accept loop (poller), then exit.
        //   - SIGHUP: reload per-tenant limits from the keys file in place, so
        //     limit changes apply without restarting the pod (which would evict
        //     every tenant in it).
        // The teardown path closes the handle so this thread also exits when
        // shutdown was triggered another way (e.g. a test setting the flag).
        let mut signals = signal_hook::iterator::Signals::new([
            signal_hook::consts::SIGTERM,
            signal_hook::consts::SIGINT,
            signal_hook::consts::SIGHUP,
        ])?;
        let signal_handle = signals.handle();
        let signal_thread = {
            let shutdown = Arc::clone(&self.shutdown);
            let wake = Arc::clone(&self.wake);
            let poller = Arc::clone(&poller);
            let tenant_limits = Arc::clone(&security.tenant_limits);
            let security_keys = Arc::clone(&security.keys);
            let keys_file = config.security.keys_file.clone();
            thread::Builder::new()
                .name("zydecodb-signal".into())
                .spawn(move || {
                    for sig in signals.forever() {
                        if sig == signal_hook::consts::SIGHUP {
                            match crate::security::keys::KeyStore::load(&keys_file) {
                                Ok(store) => {
                                    tenant_limits.reload(store.tenant_records());
                                    security_keys.store(Arc::new(store));
                                    info!("reloaded keys and per-tenant limits on SIGHUP");
                                }
                                Err(e) => warn!(error = %e, "SIGHUP reload failed"),
                            }
                            continue;
                        }
                        info!(
                            signal = sig,
                            "shutdown signal received; stopping gracefully"
                        );
                        *shutdown.lock().unwrap() = true;
                        wake.notify_all();
                        let _ = poller.notify();
                        break;
                    }
                })?
        };

        let mut events = polling::Events::new();
        loop {
            if *self.shutdown.lock().unwrap() {
                break;
            }

            // Reap finished connection threads so the vec doesn't grow without
            // bound over a long-running server.
            conns.retain(|h| !h.is_finished());

            events.clear();
            // 250ms fallback bounds how long a bare flag set (no notify) goes
            // unnoticed; real connections and signals wake the loop immediately.
            match poller.wait(&mut events, Some(Duration::from_millis(250))) {
                Ok(_) => {}
                Err(e) if e.kind() == ErrorKind::Interrupted => continue,
                Err(e) => {
                    error!(error = %e, "poller wait failed");
                    break;
                }
            }

            let tcp_ready = events.iter().any(|ev| ev.key == TCP_KEY);
            if tcp_ready {
                // Oneshot: drain every pending connection, then re-arm interest.
                loop {
                    match listener.accept() {
                        Ok((stream, peer)) => {
                            spawn_tcp_conn(
                                &engine,
                                &catalog,
                                &commit,
                                &security,
                                &self.shutdown,
                                &tls_config,
                                &tenant_metrics,
                                &mut conns,
                                stream,
                                peer.ip(),
                            );
                        }
                        Err(e) if e.kind() == ErrorKind::WouldBlock => break,
                        Err(e) => {
                            error!(error = %e, "accept failed");
                            break;
                        }
                    }
                }
                poller.modify(&listener, polling::Event::readable(TCP_KEY))?;
            }

            if let Some((ref l, _)) = uds_listener {
                if events.iter().any(|ev| ev.key == UDS_KEY) {
                    loop {
                        match l.accept() {
                            Ok((stream, _addr)) => {
                                spawn_uds_conn(
                                    &engine,
                                    &catalog,
                                    &commit,
                                    &security,
                                    &self.shutdown,
                                    &tenant_metrics,
                                    &mut conns,
                                    stream,
                                );
                            }
                            Err(e) if e.kind() == ErrorKind::WouldBlock => break,
                            Err(e) => {
                                error!(error = %e, "uds accept failed");
                                break;
                            }
                        }
                    }
                    poller.modify(l, polling::Event::readable(UDS_KEY))?;
                }
            }
        }

        // Stop accepting; tear down the poller registration and the signal thread
        // (closing the handle unblocks it if no signal ever arrived), then wake any
        // connection threads blocked on durability so they can observe shutdown and
        // exit, drain them and the background threads, and perform the final flush.
        // `Engine::shutdown` performs the final fsync, so writes that a waiter
        // unblocked from on shutdown are still made durable before the process exits.
        let _ = poller.delete(&listener);
        if let Some((ref l, ref path)) = uds_listener {
            let _ = poller.delete(l);
            let _ = std::fs::remove_file(path);
        }
        signal_handle.close();
        let _ = signal_thread.join();
        // Wake the timer threads now so they exit immediately rather than at their
        // next tick, regardless of how shutdown was triggered.
        self.wake.notify_all();
        commit.stop();
        for h in conns {
            let _ = h.join();
        }
        let _ = maintenance.join();
        let _ = commit_thread.join();
        if let Some(h) = replica_thread {
            let _ = h.join();
        }
        if let Some(h) = heartbeat_thread {
            let _ = h.join();
        }
        let _ = sweep_thread.join();
        if let Some(h) = metrics_http {
            let _ = h.join();
        }

        engine.write().shutdown()?;
        Ok(())
    }
}

/// Apply newly installed WAL segments into the live engine (happy path).
/// Falls back to full reopen if incremental apply fails.
///
/// Incremental apply holds the engine lock only for flush+replay of new
/// segments (no second `Engine::open` on the happy path). Full reopen remains
/// for corruption / apply failures: shutdown under the lock, open replacement,
/// swap, reload catalog.
fn catch_up_replica(
    engine: &SharedEngine,
    catalog: &SharedCatalog,
    metrics: &Arc<zydecodb_engine::metrics::Metrics>,
    engine_cfg: &EngineConfig,
    installed: &[u64],
) -> Result<(), Box<dyn std::error::Error>> {
    let started = std::time::Instant::now();
    {
        let mut guard = engine.write();
        for &segment_id in installed {
            if let Err(e) = guard.apply_installed_wal_segment(segment_id) {
                warn!(
                    error = %e,
                    segment_id,
                    "incremental replica apply failed; falling back to reopen"
                );
                drop(guard);
                return reopen_replica(engine, catalog, metrics, engine_cfg);
            }
        }
        let cat = Catalog::load(&*guard).map_err(|e| e.to_string())?;
        drop(guard);
        *catalog.write().unwrap() = cat;
    }
    info!(
        elapsed_ms = started.elapsed().as_millis() as u64,
        segments = installed.len(),
        "replica catch-up (incremental apply)"
    );
    Ok(())
}

/// Full engine reopen fallback for replica catch-up.
fn reopen_replica(
    engine: &SharedEngine,
    catalog: &SharedCatalog,
    metrics: &Arc<zydecodb_engine::metrics::Metrics>,
    engine_cfg: &EngineConfig,
) -> Result<(), Box<dyn std::error::Error>> {
    let started = std::time::Instant::now();
    let mut guard = engine.write();
    guard.shutdown()?;
    let fresh = Engine::open(engine_cfg.clone())?.with_metrics(Arc::clone(metrics));
    let cat = Catalog::load(&fresh).map_err(|e| e.to_string())?;
    *guard = fresh;
    drop(guard);
    *catalog.write().unwrap() = cat;
    warn!(
        elapsed_ms = started.elapsed().as_millis() as u64,
        "replica catch-up (full reopen fallback)"
    );
    Ok(())
}

#[allow(clippy::too_many_arguments)] // a connection needs the full shared server context
fn serve_tcp_connection(
    engine: &SharedEngine,
    catalog: &SharedCatalog,
    commit: &CommitCoordinator,
    mut stream: TcpStream,
    peer_ip: std::net::IpAddr,
    security: &SecurityRuntime,
    shutdown: &Arc<Mutex<bool>>,
    tls_config: Option<Arc<rustls::ServerConfig>>,
    tenant_metrics: &Option<Arc<TenantMetrics>>,
) -> Result<(), Box<dyn std::error::Error>> {
    stream.set_read_timeout(Some(Duration::from_millis(200)))?;
    stream.set_write_timeout(Some(Duration::from_secs(30)))?;

    if let Some(config) = tls_config {
        let mut tls = tls_accept(stream, &config)?;
        serve_stream(
            engine,
            catalog,
            commit,
            &mut tls,
            peer_ip,
            security,
            shutdown,
            tenant_metrics,
        )?;
        return Ok(());
    }

    serve_stream(
        engine,
        catalog,
        commit,
        &mut stream,
        peer_ip,
        security,
        shutdown,
        tenant_metrics,
    )?;
    let _ = stream.shutdown(Shutdown::Both);
    Ok(())
}

/// Serve one Unix-domain-socket connection (plain framing; no TLS). Mirrors
/// [`serve_tcp_connection`] but over a [`UnixStream`], reporting a loopback peer.
fn serve_uds_connection(
    engine: &SharedEngine,
    catalog: &SharedCatalog,
    commit: &CommitCoordinator,
    mut stream: UnixStream,
    security: &SecurityRuntime,
    shutdown: &Arc<Mutex<bool>>,
    tenant_metrics: &Option<Arc<TenantMetrics>>,
) -> Result<(), Box<dyn std::error::Error>> {
    stream.set_read_timeout(Some(Duration::from_millis(200)))?;
    stream.set_write_timeout(Some(Duration::from_secs(30)))?;
    let peer_ip = IpAddr::V4(Ipv4Addr::LOCALHOST);
    serve_stream(
        engine,
        catalog,
        commit,
        &mut stream,
        peer_ip,
        security,
        shutdown,
        tenant_metrics,
    )?;
    let _ = stream.shutdown(Shutdown::Both);
    Ok(())
}

/// Commands that mutate state (writes + DDL). Rejected on a read replica.
fn is_write_command(cmd: Command) -> bool {
    matches!(
        cmd,
        Command::Put
            | Command::Del
            | Command::DocPut
            | Command::DocDel
            | Command::Update
            | Command::Delete
            | Command::IndexDef
            | Command::AdminDropTenant
    )
}

#[allow(clippy::too_many_arguments)] // a connection needs the full shared server context
fn serve_stream<S: Read + Write>(
    engine: &SharedEngine,
    catalog: &SharedCatalog,
    commit: &CommitCoordinator,
    stream: &mut S,
    peer_ip: std::net::IpAddr,
    security: &SecurityRuntime,
    shutdown: &Arc<Mutex<bool>>,
    tenant_metrics: &Option<Arc<TenantMetrics>>,
) -> Result<(), Box<dyn std::error::Error>> {
    let mut session = SessionState::anonymous();
    let mut rate_limiter = RateLimiter::new(security.rate_limit_rps);
    let mut last_activity = Instant::now();

    loop {
        if *shutdown.lock().unwrap() {
            break;
        }

        let req = match read_message(stream, shutdown, security.idle_timeout) {
            ReadOutcome::Request(r) => {
                last_activity = Instant::now();
                r
            }
            // Idle (no request started): keep the connection warm for pooled
            // clients, but enforce the configurable idle cap so dead peers are
            // eventually reclaimed.
            ReadOutcome::Idle => {
                if let Some(limit) = security.idle_timeout {
                    if last_activity.elapsed() >= limit {
                        break;
                    }
                }
                continue;
            }
            ReadOutcome::Closed => break,
            ReadOutcome::Error(e) => return Err(e.into()),
        };

        if !rate_limiter.allow() {
            let resp = zydecodb_engine::frame::ResponseEnvelope::error(
                zydecodb_engine::errors::Status::EngineBusy,
                "rate limit exceeded",
            );
            write_response(stream, &resp)?;
            stream.flush()?;
            continue;
        }

        // Re-validate session against current KeyStore (handles SIGHUP revocations)
        if session.authenticated {
            let store = security.keys.load();
            if !store.is_session_valid(&session) {
                tracing::warn!("Session revoked during re-validation!");
                session = SessionState::anonymous();
                if security.require_auth && req.command != Command::SessionInit {
                    let resp = zydecodb_engine::frame::ResponseEnvelope::error(
                        zydecodb_engine::errors::Status::Unauthorized,
                        "session revoked",
                    );
                    write_response(stream, &resp)?;
                    stream.flush()?;
                    continue;
                }
            } else {
                // tracing::info!("Session is still valid");
            }
        }

        // Per-tenant rate ceiling: shared across all of an authenticated tenant's
        // connections (the per-connection limiter above is unaware of tenancy).
        if session.authenticated && !security.tenant_limits.allow(&session.tenant) {
            let resp = zydecodb_engine::frame::ResponseEnvelope::error(
                zydecodb_engine::errors::Status::EngineBusy,
                "tenant rate limit exceeded",
            );
            write_response(stream, &resp)?;
            stream.flush()?;
            continue;
        }

        // Read replicas reject writes/DDL before any engine work happens.
        if security.read_only && is_write_command(req.command) {
            let resp = zydecodb_engine::frame::ResponseEnvelope::error(
                zydecodb_engine::errors::Status::Forbidden,
                "read replica is read-only",
            );
            write_response(stream, &resp)?;
            stream.flush()?;
            continue;
        }

        if req.command == Command::SessionInit
            && security.require_auth
            && !session.authenticated
            && security.auth_burst.is_blocked(peer_ip)
        {
            let resp = zydecodb_engine::frame::ResponseEnvelope::error(
                zydecodb_engine::errors::Status::EngineBusy,
                "too many auth failures",
            );
            write_response(stream, &resp)?;
            stream.flush()?;
            break;
        }

        // Capture the command for metrics before `req` is moved into dispatch.
        let req_command = req.command;

        // Document commands have their own dispatch (and their own lock scoping,
        // including two-phase Query); they never mutate the session. Raw-KV
        // commands go through handle_request, which scopes the engine lock per
        // command (no lock for control commands, a brief snapshot capture for
        // Get/Stats, the write under the lock for Put/Del) -- never across
        // socket I/O.
        let response = if is_admin_command(req.command) {
            handle_admin_drop_tenant(engine, catalog, &req, &session, security)
        } else if req.command.is_document_command() {
            handle_document(engine, catalog, commit, &req, &session, security)
        } else {
            let outcome = handle_request(engine, req, session, security);
            session = outcome.session;
            if outcome.response.status == zydecodb_engine::errors::Status::Unauthorized
                && !session.authenticated
            {
                security.auth_burst.record_failure(peer_ip);
            }
            // Make a raw-KV write durable before acknowledging. The engine lock
            // was released above; the coordinator batches this fsync with any
            // other writers' (real group commit).
            if let Some(seq) = outcome.commit_seq {
                commit.commit(seq, false);
            }
            outcome.response
        };

        if let Some(tm) = tenant_metrics {
            tm.record(
                &session.tenant,
                &format!("{:?}", req_command),
                &format!("{:?}", response.status),
            );
        }

        write_response(stream, &response)?;
        stream.flush()?;
    }

    Ok(())
}

/// Time budget to finish reading a message once its first byte has arrived. A
/// client that starts a frame and then stalls cannot pin a connection thread
/// indefinitely.
const MESSAGE_READ_TIMEOUT: Duration = Duration::from_secs(30);

/// Result of attempting to read one request frame.
enum ReadOutcome {
    Request(zydecodb_engine::frame::RequestEnvelope),
    /// No request was in flight when the socket's read poll timed out. The
    /// connection is healthy and idle.
    Idle,
    /// The peer closed the connection (clean EOF) or shutdown was signaled.
    Closed,
    Error(zydecodb_engine::errors::EngineError),
}

enum Fill {
    Done,
    /// Nothing had been read yet when the read poll timed out (only returned
    /// when `allow_idle` is set, i.e. between frames).
    Idle,
    /// Clean EOF / shutdown.
    Closed,
}

/// Read exactly `buf.len()` bytes, tolerating the socket's short read-timeout.
/// Between frames (`allow_idle`) a timeout with nothing read yields `Idle` so the
/// caller can keep an idle connection alive; mid-frame it keeps waiting up to
/// [`MESSAGE_READ_TIMEOUT`] so a partially-read frame is never abandoned (which
/// would desynchronize the stream).
fn fill<S: Read>(
    stream: &mut S,
    buf: &mut [u8],
    shutdown: &Arc<Mutex<bool>>,
    allow_idle: bool,
    idle_timeout: Option<Duration>,
) -> Result<Fill, zydecodb_engine::errors::EngineError> {
    let mut filled = 0;
    let started = Instant::now();
    let mut last_byte = Instant::now();
    let timeout = idle_timeout
        .unwrap_or(MESSAGE_READ_TIMEOUT)
        .min(MESSAGE_READ_TIMEOUT);

    while filled < buf.len() {
        if *shutdown.lock().unwrap() {
            return Ok(Fill::Closed);
        }
        match stream.read(&mut buf[filled..]) {
            Ok(0) => return Ok(Fill::Closed),
            Ok(n) => {
                filled += n;
                last_byte = Instant::now();
            }
            Err(ref e) if e.kind() == ErrorKind::Interrupted => continue,
            Err(ref e) if is_idle_io(e) => {
                if filled == 0 && allow_idle {
                    return Ok(Fill::Idle);
                }
                if last_byte.elapsed() > timeout {
                    return Err(zydecodb_engine::errors::EngineError::Io(
                        "read timed out mid-message".into(),
                    ));
                }
                if started.elapsed() > MESSAGE_READ_TIMEOUT {
                    return Err(zydecodb_engine::errors::EngineError::Io(
                        "message read exceeded absolute time limit".into(),
                    ));
                }
                continue;
            }
            Err(ref e) if is_eof_io(e) => return Ok(Fill::Closed),
            Err(e) => return Err(zydecodb_engine::errors::EngineError::from(e)),
        }
    }
    Ok(Fill::Done)
}

/// Read one request frame, distinguishing "idle between frames" from "client
/// closed" from "stream error".
fn read_message<S: Read>(
    stream: &mut S,
    shutdown: &Arc<Mutex<bool>>,
    idle_timeout: Option<Duration>,
) -> ReadOutcome {
    use zydecodb_engine::frame::{RequestEnvelope, ENVELOPE_HEADER_LEN};

    let mut header = [0u8; ENVELOPE_HEADER_LEN];
    match fill(stream, &mut header, shutdown, true, idle_timeout) {
        Ok(Fill::Done) => {}
        Ok(Fill::Idle) => return ReadOutcome::Idle,
        Ok(Fill::Closed) => return ReadOutcome::Closed,
        Err(e) => return ReadOutcome::Error(e),
    }

    let (command, len) = match RequestEnvelope::parse_header(&header) {
        Ok(v) => v,
        Err(e) => return ReadOutcome::Error(e),
    };
    if len > zydecodb_engine::keys::MAX_VALUE_BYTES + 4096 {
        return ReadOutcome::Error(zydecodb_engine::errors::EngineError::Protocol(
            "payload too large".into(),
        ));
    }

    let mut payload = vec![0u8; len];
    if len > 0 {
        match fill(stream, &mut payload, shutdown, false, idle_timeout) {
            Ok(Fill::Done) => {}
            // EOF/shutdown after the header started a frame is a desync, not a
            // clean close.
            Ok(Fill::Closed) => {
                return ReadOutcome::Error(zydecodb_engine::errors::EngineError::Protocol(
                    "connection closed mid-message".into(),
                ))
            }
            Ok(Fill::Idle) => unreachable!("allow_idle is false for the payload read"),
            Err(e) => return ReadOutcome::Error(e),
        }
    }

    ReadOutcome::Request(RequestEnvelope { command, payload })
}

/// A read poll timed out / would block (no data yet), not a fatal error.
fn is_idle_io(e: &std::io::Error) -> bool {
    if matches!(e.kind(), ErrorKind::WouldBlock | ErrorKind::TimedOut) {
        return true;
    }
    let m = e.to_string();
    m.contains("timed out") || m.contains("would block")
}

/// The peer closed the connection (covers rustls/TLS close variants too).
fn is_eof_io(e: &std::io::Error) -> bool {
    if matches!(
        e.kind(),
        ErrorKind::UnexpectedEof
            | ErrorKind::ConnectionReset
            | ErrorKind::ConnectionAborted
            | ErrorKind::BrokenPipe
    ) {
        return true;
    }
    let m = e.to_string();
    m.contains("unexpected end of file")
        || m.contains("early eof")
        || m.contains("connection closed")
        || m.contains("closed connection")
}
