use crate::config::Config;
use crate::security::keys::{parse_tenant_hex, KeyError, KeyRole, KeyStore};
use std::path::{Path, PathBuf};
use zydecodb_document::catalog::Catalog;
use zydecodb_engine::engine::{Engine, EngineConfig};
use zydecodb_engine::keys::KS_USER;

/// Build an [`EngineConfig`] from a server [`Config`] for offline admin commands.
fn engine_cfg_from(cfg: &Config) -> EngineConfig {
    cfg.to_engine_config()
}

pub fn keys_create(
    keys_file: PathBuf,
    id: String,
    role: KeyRole,
    tenant: String,
    prefixes: Vec<String>,
) -> Result<(), KeyError> {
    let secret = KeyStore::create_key(&keys_file, &id, role, &tenant, prefixes)?;
    println!("API key created (save this — it will not be shown again):");
    println!("{secret}");
    Ok(())
}

pub fn keys_list(keys_file: PathBuf) -> Result<(), KeyError> {
    let store = KeyStore::load(&keys_file)?;
    for id in store.list_ids() {
        println!("{id}");
    }
    Ok(())
}

pub fn keys_revoke(keys_file: PathBuf, id: String) -> Result<(), KeyError> {
    KeyStore::revoke_key(&keys_file, &id)?;
    println!("revoked key {id}");
    Ok(())
}

/// Set or update a tenant's resource limits in the keys file. Omitting a limit
/// leaves it unchanged. A running server applies the change on `SIGHUP`.
pub fn tenant_set_limit(
    keys_file: PathBuf,
    tenant: String,
    max_bytes: Option<u64>,
    rate_rps: Option<u32>,
) -> Result<(), KeyError> {
    KeyStore::set_tenant_limit(&keys_file, &tenant, max_bytes, rate_rps)?;
    println!("set limits for tenant {tenant} (send SIGHUP to a running server to apply)");
    Ok(())
}

/// Hardlink `src` to `dst`, falling back to a full copy across filesystems.
fn link_or_copy(src: &Path, dst: &Path) -> std::io::Result<()> {
    match std::fs::hard_link(src, dst) {
        Ok(()) => Ok(()),
        Err(e) if e.kind() == std::io::ErrorKind::AlreadyExists => Ok(()),
        Err(_) => std::fs::copy(src, dst).map(|_| ()),
    }
}

/// Restore a database into `out` from a base snapshot plus shipped WAL, stopping
/// at a point in time. `to_seq` is exact; `to_time` (unix millis) is best-effort,
/// resolved via the shipped `timeindex.log` to the greatest seq at or before that
/// time. With neither, the entire shipped WAL is replayed.
///
/// Lays the base SSTables + MANIFEST into `out`, ingests (sha256-verified) shipped
/// WAL segments into `out/wal`, then opens the engine with a replay ceiling and
/// shuts it down cleanly so `out` is immediately ready to serve.
pub fn restore(
    base: &Path,
    wal: &Path,
    to_seq: Option<u64>,
    to_time: Option<u64>,
    out: &Path,
) -> Result<(), String> {
    let out_wal = out.join("wal");
    std::fs::create_dir_all(out).map_err(|e| e.to_string())?;
    std::fs::create_dir_all(&out_wal).map_err(|e| e.to_string())?;

    // 1. Lay down the base snapshot: hardlink SSTables (immutable), copy MANIFEST.
    for entry in std::fs::read_dir(base).map_err(|e| e.to_string())? {
        let entry = entry.map_err(|e| e.to_string())?;
        let name = entry.file_name();
        let name = name.to_string_lossy();
        let dst = out.join(&*name);
        if name.ends_with(".sst") {
            link_or_copy(&entry.path(), &dst).map_err(|e| e.to_string())?;
        } else if name == "MANIFEST" {
            std::fs::copy(entry.path(), &dst).map_err(|e| e.to_string())?;
        }
    }

    // 2. Ingest shipped WAL into the restore wal dir (verifies sha256).
    let mut rep = crate::replica::Replica::new(wal.to_path_buf(), out_wal.clone());
    rep.sync().map_err(|e| e.to_string())?;

    // 3. Resolve the replay ceiling: exact seq wins; else map time via the index.
    let ceiling = match (to_seq, to_time) {
        (Some(s), _) => Some(s),
        (None, Some(t)) => Some(
            zydecodb_engine::shipping::resolve_seq_at_or_before(wal, t)
                .map_err(|e| e.to_string())?
                .ok_or_else(|| {
                    format!("no time-index sample at or before {t} in {}", wal.display())
                })?,
        ),
        (None, None) => None,
    };

    // 4. Open at the ceiling, then shut down to flush replayed data + mark clean.
    let cfg = EngineConfig {
        data_dir: out.to_path_buf(),
        wal_dir: out_wal,
        wal_replay_max_seq: ceiling,
        ..Default::default()
    };
    let mut engine = Engine::open(cfg).map_err(|e| e.to_string())?;
    let restored_seq = engine.current_seq();
    engine.shutdown().map_err(|e| e.to_string())?;
    println!(
        "restored {} to seq {restored_seq} (ceiling {ceiling:?})",
        out.display()
    );
    Ok(())
}

/// Capture a base snapshot of the database into `out`. Runs offline against a
/// stopped node (the engine lock enforces this) or against a replica's data_dir.
/// The snapshot directory holds the live SSTables (hardlinked), a copy of the
/// MANIFEST, and a SNAPMETA recording the snapshot sequence.
pub fn snapshot(config: &Path, out: &Path) -> Result<(), String> {
    let cfg = Config::from_file(config).map_err(|e| e.to_string())?;
    let mut engine = Engine::open(engine_cfg_from(&cfg)).map_err(|e| e.to_string())?;
    let seq = engine.snapshot_to(out).map_err(|e| e.to_string())?;
    engine.shutdown().map_err(|e| e.to_string())?;
    println!("snapshot written to {} at seq {seq}", out.display());
    Ok(())
}

/// Count live SSTables by on-disk format, reading each footer offline.
/// Returns `(current, legacy)`. Files that don't parse as SSTables (partials,
/// stray files) are skipped.
fn count_sstable_versions(data_dir: &Path) -> Result<(usize, usize), String> {
    use std::io::{Read, Seek, SeekFrom};
    use zydecodb_engine::sstable;

    let mut current = 0usize;
    let mut legacy = 0usize;
    for entry in std::fs::read_dir(data_dir).map_err(|e| e.to_string())? {
        let entry = entry.map_err(|e| e.to_string())?;
        let name = entry.file_name();
        if !name.to_string_lossy().ends_with(".sst") {
            continue;
        }
        let mut f = std::fs::File::open(entry.path()).map_err(|e| e.to_string())?;
        let len = f.seek(SeekFrom::End(0)).map_err(|e| e.to_string())?;
        if (len as usize) < sstable::FOOTER_LEN {
            continue;
        }
        f.seek(SeekFrom::End(-(sstable::FOOTER_LEN as i64)))
            .map_err(|e| e.to_string())?;
        let mut footer = vec![0u8; sstable::FOOTER_LEN];
        f.read_exact(&mut footer).map_err(|e| e.to_string())?;
        match sstable::parse_footer(&footer) {
            Ok(ft) if ft.version >= sstable::FORMAT_VERSION => current += 1,
            Ok(_) => legacy += 1,
            Err(_) => {}
        }
    }
    Ok((current, legacy))
}

/// Rewrite on-disk SSTables to the current format by forcing a full compaction
/// (offline). Legacy-format files are readable regardless; this just accelerates
/// the rewrite that background compaction performs over time. Reports how many
/// files remain in a legacy format afterward (some settled, non-overlapping
/// files may not be picked by the planner and are rewritten later organically).
pub fn upgrade(config: &Path) -> Result<(), String> {
    let cfg = Config::from_file(config).map_err(|e| e.to_string())?;
    let data_dir = cfg.data_dir.clone();
    let mut engine = Engine::open(engine_cfg_from(&cfg)).map_err(|e| e.to_string())?;
    engine.compact_all().map_err(|e| e.to_string())?;
    engine.shutdown().map_err(|e| e.to_string())?;

    let (current, legacy) = count_sstable_versions(&data_dir)?;
    println!(
        "upgrade complete: {current} SSTable(s) at current format v{}, {legacy} legacy",
        zydecodb_engine::sstable::FORMAT_VERSION
    );
    if legacy > 0 {
        println!(
            "note: {legacy} legacy file(s) remain and are fully readable; \
             they are rewritten as background compaction touches them"
        );
    }
    Ok(())
}

/// List the configured per-tenant limits.
pub fn tenant_list(keys_file: PathBuf) -> Result<(), KeyError> {
    let store = KeyStore::load(&keys_file)?;
    for (tenant, max_bytes, rate_rps) in store.list_tenant_limits() {
        let mb = max_bytes
            .map(|v| v.to_string())
            .unwrap_or_else(|| "-".into());
        let rr = rate_rps
            .map(|v| v.to_string())
            .unwrap_or_else(|| "-".into());
        println!("{tenant} max_bytes={mb} rate_rps={rr}");
    }
    Ok(())
}

/// Result of dropping a tenant's user-keyspace prefix and catalog entries.
#[derive(Debug, Clone, Copy)]
pub struct DropTenantResult {
    pub deleted_keys: u64,
    pub removed_collections: usize,
}

/// Core offboard logic shared by offline CLI and live `AdminDropTenant`.
///
/// Deletes all keys under `KS_USER | tenant` and removes matching catalog
/// collections. With `compact`, runs `compact_all` before returning (slow).
/// Refuses the all-zero tenant (legacy single-tenant layout).
pub fn drop_tenant_on_engine(
    engine: &mut Engine,
    catalog: &mut Catalog,
    tenant: &[u8; 16],
    compact: bool,
) -> Result<DropTenantResult, String> {
    if *tenant == [0u8; 16] {
        return Err("refusing to drop the all-zero tenant (reserved for legacy \
                    single-tenant data); pass a real 32-hex tenant id"
            .into());
    }

    let mut prefix = Vec::with_capacity(1 + 16);
    prefix.push(KS_USER);
    prefix.extend_from_slice(tenant);

    let deleted = engine
        .delete_prefix(prefix.clone())
        .map_err(|e| e.to_string())?;

    let removed = catalog.remove_collections_with_prefix(&prefix);
    catalog.persist(engine).map_err(|e| e.to_string())?;

    if compact {
        engine.compact_all().map_err(|e| e.to_string())?;
    }

    Ok(DropTenantResult {
        deleted_keys: deleted,
        removed_collections: removed,
    })
}

/// Offline offboard against a stopped node (exclusive `data_dir` lock).
pub fn drop_tenant(config: &Path, tenant_hex: &str, compact: bool) -> Result<(), String> {
    let tenant = parse_tenant_hex(tenant_hex).map_err(|e| e.to_string())?;
    let cfg = Config::from_file(config).map_err(|e| e.to_string())?;
    let mut engine = Engine::open(engine_cfg_from(&cfg)).map_err(|e| e.to_string())?;
    let mut catalog = Catalog::load(&engine).map_err(|e| e.to_string())?;

    let result = drop_tenant_on_engine(&mut engine, &mut catalog, &tenant, compact)?;
    engine.shutdown().map_err(|e| e.to_string())?;

    println!(
        "dropped tenant {tenant_hex}: {} key(s) deleted, {} collection(s) removed{}",
        result.deleted_keys,
        result.removed_collections,
        if compact { ", space reclaimed" } else { "" }
    );
    Ok(())
}

/// Live offboard: connect to a running server and issue `AdminDropTenant`.
/// Prefers `listen_unix` from the config when set; otherwise TCP `listen`.
/// Requires `ZYDECODB_API_KEY` (admin role) in the environment.
pub fn drop_tenant_live(config: &Path, tenant_hex: &str, compact: bool) -> Result<(), String> {
    use std::io::{Read, Write};
    use std::net::TcpStream;
    use std::os::unix::net::UnixStream;
    use zydecodb_engine::errors::Status;
    use zydecodb_engine::frame::{Command, RequestEnvelope};

    let tenant = parse_tenant_hex(tenant_hex).map_err(|e| e.to_string())?;
    let cfg = Config::from_file(config).map_err(|e| e.to_string())?;
    let admin_key = std::env::var("ZYDECODB_API_KEY").map_err(|_| {
        "live drop-tenant requires ZYDECODB_API_KEY (admin role) in the environment".to_string()
    })?;

    let mut payload = Vec::with_capacity(17);
    payload.extend_from_slice(&tenant);
    payload.push(if compact { 1 } else { 0 });

    enum Conn {
        Tcp(TcpStream),
        Unix(UnixStream),
    }
    impl Read for Conn {
        fn read(&mut self, buf: &mut [u8]) -> std::io::Result<usize> {
            match self {
                Conn::Tcp(s) => s.read(buf),
                Conn::Unix(s) => s.read(buf),
            }
        }
    }
    impl Write for Conn {
        fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
            match self {
                Conn::Tcp(s) => s.write(buf),
                Conn::Unix(s) => s.write(buf),
            }
        }
        fn flush(&mut self) -> std::io::Result<()> {
            match self {
                Conn::Tcp(s) => s.flush(),
                Conn::Unix(s) => s.flush(),
            }
        }
    }

    let mut stream = if let Some(ref uds) = cfg.listen_unix {
        Conn::Unix(
            UnixStream::connect(uds).map_err(|e| format!("connect unix {}: {e}", uds.display()))?,
        )
    } else {
        Conn::Tcp(
            TcpStream::connect(cfg.listen).map_err(|e| format!("connect {}: {e}", cfg.listen))?,
        )
    };

    let init = RequestEnvelope::new(Command::SessionInit, admin_key.into_bytes());
    stream
        .write_all(&init.encode())
        .map_err(|e| e.to_string())?;
    let (init_status, _) = read_response_status(&mut stream)?;
    if init_status != Status::Ok {
        return Err(format!("SessionInit failed: {init_status:?}"));
    }

    let drop_req = RequestEnvelope::new(Command::AdminDropTenant, payload);
    stream
        .write_all(&drop_req.encode())
        .map_err(|e| e.to_string())?;
    let (drop_status, msg) = read_response_status(&mut stream)?;
    if drop_status != Status::Ok {
        return Err(format!("AdminDropTenant failed: {drop_status:?} {msg}"));
    }

    println!(
        "live-dropped tenant {tenant_hex}{}",
        if compact { " (compact requested)" } else { "" }
    );
    Ok(())
}

fn read_response_status(
    stream: &mut dyn std::io::Read,
) -> Result<(zydecodb_engine::errors::Status, String), String> {
    use zydecodb_engine::errors::Status;
    use zydecodb_engine::frame::{ENVELOPE_HEADER_LEN, PROTO_VERSION};
    let mut header = [0u8; ENVELOPE_HEADER_LEN];
    stream.read_exact(&mut header).map_err(|e| e.to_string())?;
    if header[0] != PROTO_VERSION {
        return Err("bad protocol version".into());
    }
    let status = Status::from_u8(header[1]).ok_or_else(|| "unknown status".to_string())?;
    let len = u32::from_be_bytes(header[2..6].try_into().unwrap()) as usize;
    let mut payload = vec![0u8; len];
    if len > 0 {
        stream.read_exact(&mut payload).map_err(|e| e.to_string())?;
    }
    Ok((status, String::from_utf8_lossy(&payload).into_owned()))
}
