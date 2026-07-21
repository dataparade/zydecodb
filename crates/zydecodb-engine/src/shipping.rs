//! WAL shipping: filesystem-first off-box durability.
//!
//! When a WAL segment seals, the engine drops a byte-identical copy into a
//! configured `ship_dir` and appends a line to `shipped.log` describing it. An
//! operator-supplied sidecar (rsync, s5cmd, AWS DataSync, ...) watches that
//! directory and transports the bytes elsewhere. The engine itself does no
//! network I/O and owns no object-store client — that is deliberately out of
//! scope. The contract is simply: "the file in `ship_dir` is exactly the sealed
//! segment, and `shipped.log` records the order."
//!
//! `shipped.log` line format (append-only, one per shipped segment):
//! ```text
//! <segment_id> <seal_seq> <sha256_hex> [<hmac_hex>]\n
//! ```
//!
//! When an HMAC key is configured, each line carries a fourth field:
//! `HMAC-SHA256(key, "<segment_id> <seal_seq> <sha256_hex>")` in lower hex.
//! The HMAC authenticates the manifest entry, so an attacker who can write the
//! ship directory cannot forge both the segment bytes and a matching log line.
//! Verification with a key requires the HMAC field; legacy 3-field lines are
//! accepted only when no key is configured (dev/back-compat).

use crate::errors::{EngineError, EngineResult};
use sha2::{Digest, Sha256};
use std::io::Write;
use std::path::Path;

/// How a sealed segment reaches `ship_dir`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ShipMode {
    /// Hardlink the sealed segment (atomic, no copy cost). Requires `ship_dir`
    /// to be on the same filesystem as the WAL.
    Hardlink,
    /// Copy the bytes. Works across filesystems; costs a full read+write.
    Copy,
}

impl ShipMode {
    /// Parse from config (`"hardlink"` | `"copy"`). Unknown -> hardlink.
    pub fn from_str_or_default(s: &str) -> ShipMode {
        match s.trim().to_ascii_lowercase().as_str() {
            "copy" => ShipMode::Copy,
            _ => ShipMode::Hardlink,
        }
    }
}

pub const SHIPPED_LOG: &str = "shipped.log";

/// Liveness marker the primary refreshes on a fixed cadence, even while idle, so
/// a replica can tell a quiet primary from a dead one. Travels in `ship_dir`
/// alongside the segments the sidecar transports.
pub const HEARTBEAT: &str = "shipped.heartbeat";

/// One parsed heartbeat: when the primary last wrote it and its write position.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Heartbeat {
    /// Wall-clock time of the heartbeat, milliseconds since the Unix epoch.
    pub unix_millis: u64,
    /// The primary's highest assigned write sequence at heartbeat time.
    pub last_seal_seq: u64,
}

/// Atomically (temp file + rename) refresh the heartbeat in `ship_dir`.
pub fn write_heartbeat(ship_dir: &Path, unix_millis: u64, last_seal_seq: u64) -> EngineResult<()> {
    std::fs::create_dir_all(ship_dir)?;
    let tmp = ship_dir.join("shipped.heartbeat.tmp");
    let dst = ship_dir.join(HEARTBEAT);
    {
        let mut f = std::fs::File::create(&tmp)?;
        writeln!(f, "{} {}", unix_millis, last_seal_seq)
            .map_err(|e| EngineError::Io(format!("heartbeat write: {}", e)))?;
        f.sync_all()?;
    }
    std::fs::rename(&tmp, &dst).map_err(|e| EngineError::Io(format!("heartbeat rename: {}", e)))?;
    Ok(())
}

/// Rolling time index filename in a shipped-stream directory: append-only
/// `<unix_millis> <seq>` lines, written next to each heartbeat so a point-in-time
/// restore can map a wall-clock target to a write sequence.
pub const TIMEINDEX: &str = "timeindex.log";

/// Append a `(unix_millis, seq)` sample to the rolling time index in `ship_dir`.
/// Best-effort and at heartbeat granularity; the file grows over time and may be
/// rotated by the operator/sidecar.
pub fn append_timeindex(ship_dir: &Path, unix_millis: u64, seq: u64) -> EngineResult<()> {
    std::fs::create_dir_all(ship_dir)?;
    let path = ship_dir.join(TIMEINDEX);
    let mut f = std::fs::OpenOptions::new()
        .create(true)
        .append(true)
        .open(&path)?;
    writeln!(f, "{} {}", unix_millis, seq)
        .map_err(|e| EngineError::Io(format!("timeindex write: {}", e)))?;
    Ok(())
}

/// Resolve the greatest sequence whose time-index sample is `<= target_millis`.
/// Returns `None` if the index is missing or every sample is newer than target.
pub fn resolve_seq_at_or_before(dir: &Path, target_millis: u64) -> EngineResult<Option<u64>> {
    let path = dir.join(TIMEINDEX);
    let text = match std::fs::read_to_string(&path) {
        Ok(t) => t,
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => return Ok(None),
        Err(e) => return Err(EngineError::Io(format!("read timeindex: {}", e))),
    };
    let mut best: Option<u64> = None;
    for line in text.lines() {
        let parts: Vec<&str> = line.split_whitespace().collect();
        if parts.len() != 2 {
            continue;
        }
        let (Ok(t), Ok(seq)) = (parts[0].parse::<u64>(), parts[1].parse::<u64>()) else {
            continue;
        };
        if t <= target_millis {
            best = Some(best.map_or(seq, |b| b.max(seq)));
        }
    }
    Ok(best)
}

/// Read the heartbeat from a shipped-stream directory. A missing file is `None`
/// (no heartbeat shipped yet); a malformed file is an error rather than a
/// silent "primary looks dead".
pub fn read_heartbeat(dir: &Path) -> EngineResult<Option<Heartbeat>> {
    let path = dir.join(HEARTBEAT);
    let text = match std::fs::read_to_string(&path) {
        Ok(t) => t,
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => return Ok(None),
        Err(e) => return Err(EngineError::Io(format!("read heartbeat: {}", e))),
    };
    let line = text.trim();
    let parts: Vec<&str> = line.split_whitespace().collect();
    if parts.len() != 2 {
        return Err(EngineError::Io(
            "heartbeat: expected '<unix_millis> <seq>'".into(),
        ));
    }
    let unix_millis = parts[0]
        .parse::<u64>()
        .map_err(|_| EngineError::Io("heartbeat: bad timestamp".into()))?;
    let last_seal_seq = parts[1]
        .parse::<u64>()
        .map_err(|_| EngineError::Io("heartbeat: bad seq".into()))?;
    Ok(Some(Heartbeat {
        unix_millis,
        last_seal_seq,
    }))
}

/// Ship one sealed segment into `ship_dir` and append a `shipped.log` entry.
/// Idempotent on the destination file name (overwrites a same-named stale ship).
/// With `hmac_key`, the log line carries an HMAC authenticating the entry.
pub fn ship_segment(
    src: &Path,
    ship_dir: &Path,
    segment_id: u64,
    seal_seq: u64,
    mode: ShipMode,
    hmac_key: Option<&[u8]>,
) -> EngineResult<()> {
    std::fs::create_dir_all(ship_dir)?;
    let file_name = src
        .file_name()
        .ok_or_else(|| EngineError::Io("ship: source has no file name".into()))?;
    let dst = ship_dir.join(file_name);

    // Hash the source so the sidecar (and our restore path) can verify integrity.
    let bytes = std::fs::read(src)?;
    let mut hasher = Sha256::new();
    hasher.update(&bytes);
    let digest = hasher.finalize();
    let hex = hex_encode(&digest);

    // Remove any stale destination, then link or copy.
    let _ = std::fs::remove_file(&dst);
    match mode {
        ShipMode::Hardlink => {
            if let Err(e) = std::fs::hard_link(src, &dst) {
                // Cross-device or unsupported -> fall back to a copy so shipping
                // still succeeds rather than silently dropping durability.
                if e.raw_os_error() == Some(libc_exdev()) {
                    std::fs::write(&dst, &bytes)?;
                } else {
                    return Err(EngineError::Io(format!("ship hardlink: {}", e)));
                }
            }
        }
        ShipMode::Copy => {
            std::fs::write(&dst, &bytes)?;
        }
    }

    let hmac_hex = hmac_key.map(|k| entry_hmac_hex(k, segment_id, seal_seq, &hex));
    append_shipped_log(ship_dir, segment_id, seal_seq, &hex, hmac_hex.as_deref())?;
    Ok(())
}

fn append_shipped_log(
    ship_dir: &Path,
    segment_id: u64,
    seal_seq: u64,
    sha256_hex: &str,
    hmac_hex: Option<&str>,
) -> EngineResult<()> {
    let log_path = ship_dir.join(SHIPPED_LOG);
    let mut f = std::fs::OpenOptions::new()
        .create(true)
        .append(true)
        .open(&log_path)?;
    match hmac_hex {
        Some(mac) => writeln!(f, "{} {} {} {}", segment_id, seal_seq, sha256_hex, mac),
        None => writeln!(f, "{} {} {}", segment_id, seal_seq, sha256_hex),
    }
    .map_err(|e| EngineError::Io(format!("shipped.log write: {}", e)))?;
    f.sync_all()?;
    Ok(())
}

/// One parsed line of `shipped.log`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ShippedEntry {
    pub segment_id: u64,
    pub seal_seq: u64,
    pub sha256_hex: String,
    /// Present on lines written with an HMAC key (4-field format).
    pub hmac_hex: Option<String>,
}

/// Read and parse `shipped.log` from a ship directory, in append order.
/// A missing log is treated as "nothing shipped yet" (empty list). Malformed
/// lines are rejected so a corrupt manifest cannot silently skip data.
pub fn read_shipped_log(ship_dir: &Path) -> EngineResult<Vec<ShippedEntry>> {
    let log_path = ship_dir.join(SHIPPED_LOG);
    let text = match std::fs::read_to_string(&log_path) {
        Ok(t) => t,
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => return Ok(Vec::new()),
        Err(e) => return Err(EngineError::Io(format!("read shipped.log: {}", e))),
    };
    let mut out: Vec<ShippedEntry> = Vec::new();
    for (lineno, line) in text.lines().enumerate() {
        let line = line.trim();
        if line.is_empty() {
            continue;
        }
        let parts: Vec<&str> = line.split_whitespace().collect();
        if parts.len() != 3 && parts.len() != 4 {
            return Err(EngineError::Io(format!(
                "shipped.log line {}: expected '<id> <seq> <sha256> [<hmac>]'",
                lineno + 1
            )));
        }
        let segment_id = parts[0]
            .parse::<u64>()
            .map_err(|_| EngineError::Io(format!("shipped.log line {}: bad id", lineno + 1)))?;
        let seal_seq = parts[1]
            .parse::<u64>()
            .map_err(|_| EngineError::Io(format!("shipped.log line {}: bad seq", lineno + 1)))?;
        let sha256_hex = parts[2].to_ascii_lowercase();
        if sha256_hex.len() != 64 || !sha256_hex.bytes().all(|b| b.is_ascii_hexdigit()) {
            return Err(EngineError::Io(format!(
                "shipped.log line {}: bad sha256",
                lineno + 1
            )));
        }
        let hmac_hex = if parts.len() == 4 {
            let mac = parts[3].to_ascii_lowercase();
            if mac.len() != 64 || !mac.bytes().all(|b| b.is_ascii_hexdigit()) {
                return Err(EngineError::Io(format!(
                    "shipped.log line {}: bad hmac",
                    lineno + 1
                )));
            }
            Some(mac)
        } else {
            None
        };
        
        if let Some(last) = out.last() {
            if segment_id <= last.segment_id {
                return Err(EngineError::Io(format!(
                    "shipped.log line {}: out of order segment_id {} (<= {})",
                    lineno + 1, segment_id, last.segment_id
                )));
            }
        }
        
        out.push(ShippedEntry {
            segment_id,
            seal_seq,
            sha256_hex,
            hmac_hex,
        });
    }
    Ok(out)
}

/// SHA-256 of a file, lower-case hex.
pub fn sha256_file(path: &Path) -> EngineResult<String> {
    let bytes = std::fs::read(path)?;
    let mut hasher = Sha256::new();
    hasher.update(&bytes);
    Ok(hex_encode(&hasher.finalize()))
}

/// Verify a shipped segment's bytes match the sha256 recorded in `shipped.log`.
pub fn verify_segment(path: &Path, expected_sha256_hex: &str) -> EngineResult<bool> {
    Ok(sha256_file(path)?.eq_ignore_ascii_case(expected_sha256_hex))
}

/// Verify a shipped-log entry against the segment bytes and, when a key is
/// configured, its HMAC. With a key, a missing or wrong HMAC fails — a legacy
/// 3-field line is not acceptable on an authenticated stream.
pub fn verify_entry(path: &Path, entry: &ShippedEntry, hmac_key: Option<&[u8]>) -> EngineResult<bool> {
    if !verify_segment(path, &entry.sha256_hex)? {
        return Err(EngineError::Io(format!("segment {} hash mismatch or corrupt", entry.segment_id)));
    }
    let Some(key) = hmac_key else {
        return Ok(true);
    };
    let Some(ref presented) = entry.hmac_hex else {
        return Err(EngineError::Io(format!("segment {} missing hmac", entry.segment_id)));
    };
    let expected = entry_hmac_hex(key, entry.segment_id, entry.seal_seq, &entry.sha256_hex);
    if !constant_time_eq_str(presented, &expected) {
        return Err(EngineError::Io(format!("segment {} hmac mismatch", entry.segment_id)));
    }
    Ok(true)
}

/// HMAC-SHA256 over the canonical entry string `"<id> <seq> <sha256_hex>"`.
pub fn entry_hmac_hex(key: &[u8], segment_id: u64, seal_seq: u64, sha256_hex: &str) -> String {
    let msg = format!("{} {} {}", segment_id, seal_seq, sha256_hex);
    hex_encode(&hmac_sha256(key, msg.as_bytes()))
}

/// RFC 2104 HMAC-SHA256. Implemented on top of the `sha2` crate we already
/// depend on (no extra crypto dependency for one construction).
fn hmac_sha256(key: &[u8], msg: &[u8]) -> [u8; 32] {
    const BLOCK: usize = 64;
    let mut k = [0u8; BLOCK];
    if key.len() > BLOCK {
        let mut h = Sha256::new();
        h.update(key);
        k[..32].copy_from_slice(&h.finalize());
    } else {
        k[..key.len()].copy_from_slice(key);
    }
    let mut ipad = [0x36u8; BLOCK];
    let mut opad = [0x5cu8; BLOCK];
    for i in 0..BLOCK {
        ipad[i] ^= k[i];
        opad[i] ^= k[i];
    }
    let mut inner = Sha256::new();
    inner.update(ipad);
    inner.update(msg);
    let inner_digest = inner.finalize();
    let mut outer = Sha256::new();
    outer.update(opad);
    outer.update(inner_digest);
    outer.finalize().into()
}

fn constant_time_eq_str(a: &str, b: &str) -> bool {
    let (a, b) = (a.as_bytes(), b.as_bytes());
    if a.len() != b.len() {
        return false;
    }
    let mut diff = 0u8;
    for (x, y) in a.iter().zip(b.iter()) {
        diff |= x ^ y;
    }
    diff == 0
}

/// EXDEV ("cross-device link") errno. Avoids a libc dependency for one constant.
fn libc_exdev() -> i32 {
    18
}

fn hex_encode(bytes: &[u8]) -> String {
    use std::fmt::Write;
    let mut s = String::with_capacity(bytes.len() * 2);
    for b in bytes {
        let _ = write!(s, "{:02x}", b);
    }
    s
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn hardlink_ships_byte_identical_and_logs() {
        let dir = tempfile::TempDir::new().unwrap();
        let wal = dir.path().join("wal");
        let ship = dir.path().join("ship");
        std::fs::create_dir_all(&wal).unwrap();
        let src = wal.join("wal-00000001.log");
        std::fs::write(&src, b"hello-wal-bytes").unwrap();

        ship_segment(&src, &ship, 1, 42, ShipMode::Hardlink, None).unwrap();

        let shipped = ship.join("wal-00000001.log");
        assert!(shipped.exists());
        assert_eq!(std::fs::read(&shipped).unwrap(), b"hello-wal-bytes");

        let log = std::fs::read_to_string(ship.join(SHIPPED_LOG)).unwrap();
        assert!(log.starts_with("1 42 "), "log line was: {}", log);
        // sha256("hello-wal-bytes") suffix present and 64 hex chars.
        let hex = log.trim().rsplit(' ').next().unwrap();
        assert_eq!(hex.len(), 64);
    }

    #[test]
    fn copy_mode_ships_independent_bytes() {
        let dir = tempfile::TempDir::new().unwrap();
        let wal = dir.path().join("wal");
        let ship = dir.path().join("ship");
        std::fs::create_dir_all(&wal).unwrap();
        let src = wal.join("wal-00000007.log");
        std::fs::write(&src, b"copy-me").unwrap();

        ship_segment(&src, &ship, 7, 100, ShipMode::Copy, None).unwrap();
        assert_eq!(
            std::fs::read(ship.join("wal-00000007.log")).unwrap(),
            b"copy-me"
        );
    }

    #[test]
    fn heartbeat_round_trips() {
        let dir = tempfile::TempDir::new().unwrap();
        assert_eq!(read_heartbeat(dir.path()).unwrap(), None);
        write_heartbeat(dir.path(), 1_700_000_000_000, 42).unwrap();
        let hb = read_heartbeat(dir.path()).unwrap().unwrap();
        assert_eq!(hb.unix_millis, 1_700_000_000_000);
        assert_eq!(hb.last_seal_seq, 42);
        // A later write overwrites in place.
        write_heartbeat(dir.path(), 1_700_000_005_000, 99).unwrap();
        let hb2 = read_heartbeat(dir.path()).unwrap().unwrap();
        assert_eq!(hb2.last_seal_seq, 99);
    }

    #[test]
    fn ship_mode_parses() {
        assert_eq!(ShipMode::from_str_or_default("copy"), ShipMode::Copy);
        assert_eq!(ShipMode::from_str_or_default("COPY"), ShipMode::Copy);
        assert_eq!(
            ShipMode::from_str_or_default("hardlink"),
            ShipMode::Hardlink
        );
        assert_eq!(ShipMode::from_str_or_default("garbage"), ShipMode::Hardlink);
    }
}
