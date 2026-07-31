//! Reopen + WAL CRC integrity probe used by `scripts/crash-soak.sh`.
//!
//! Usage: crash-soak-integrity --data-dir PATH --wal-dir PATH

use std::path::PathBuf;
use zydecodb_engine::engine::{Engine, EngineConfig};
use zydecodb_engine::keys::KS_USER;
use zydecodb_engine::wal::{self, ReplayOutcome};

fn uk(k: &[u8]) -> Vec<u8> {
    let mut v = vec![KS_USER];
    v.extend_from_slice(k);
    v
}

fn main() {
    let mut data_dir: Option<PathBuf> = None;
    let mut wal_dir: Option<PathBuf> = None;
    let mut args = std::env::args().skip(1);
    while let Some(a) = args.next() {
        match a.as_str() {
            "--data-dir" => data_dir = Some(PathBuf::from(args.next().expect("--data-dir value"))),
            "--wal-dir" => wal_dir = Some(PathBuf::from(args.next().expect("--wal-dir value"))),
            other => panic!("unknown arg: {other}"),
        }
    }
    let data_dir = data_dir.expect("--data-dir is required");
    let wal_dir = wal_dir.unwrap_or_else(|| data_dir.join("wal"));

    let mut e = Engine::open(EngineConfig {
        data_dir: data_dir.clone(),
        wal_dir: wal_dir.clone(),
        ..Default::default()
    })
    .expect("reopen");

    let segs = wal::list_segments(&wal_dir).expect("list wal");
    let max_id = segs.iter().map(|(i, _)| *i).max();
    for (id, path) in &segs {
        let bytes = std::fs::read(path).expect("read seg");
        if bytes.len() < wal::SEGMENT_HEADER_LEN {
            continue;
        }
        let body = &bytes[wal::SEGMENT_HEADER_LEN..];
        let (_entries, outcome) = wal::replay_segment_body(body);
        if matches!(outcome, ReplayOutcome::Corruption) && Some(*id) != max_id {
            panic!("sealed segment {id} corruption");
        }
    }

    let key = uk(b"__crash_soak_probe__");
    e.put(key.clone(), b"ok".to_vec(), 0).expect("put");
    e.sync_wal().expect("sync");
    assert_eq!(e.get(&key).expect("get").as_deref(), Some(&b"ok"[..]));
    e.force_flush().expect("flush");
    e.shutdown().expect("shutdown");
}
