//! TTL compaction alongside change-log retention: reclaiming expired SST values
//! must not drop archive segments still needed by a resume token inside the
//! retention window.

use tempfile::TempDir;
use zydecodb_engine::change_log::{self, ChangeLogConfig, ResumeToken};
use zydecodb_engine::engine::{Engine, EngineConfig};
use zydecodb_engine::keys::KS_USER;

fn uk(k: &[u8]) -> Vec<u8> {
    let mut v = vec![KS_USER];
    v.extend_from_slice(k);
    v
}

fn open(dir: &TempDir, retention_secs: u64, retention_bytes: u64) -> Engine {
    let archive = dir.path().join("change_log");
    std::fs::create_dir_all(&archive).unwrap();
    Engine::open(EngineConfig {
        data_dir: dir.path().join("data"),
        wal_dir: dir.path().join("data/wal"),
        ..Default::default()
    })
    .unwrap()
    .with_change_log(ChangeLogConfig {
        archive_dir: archive,
        retention_secs,
        retention_bytes,
    })
    .unwrap()
}

#[test]
fn ttl_compaction_does_not_drop_retained_changelog_segments() {
    let dir = TempDir::new().unwrap();
    let mut e = open(&dir, 3600, 64 * 1024 * 1024);

    // Durable user writes, then seal so they land in the change-log archive.
    e.put(uk(b"keep"), b"v".to_vec(), 0).unwrap();
    e.sync_wal().unwrap();
    e.force_roll_wal_for_test().unwrap();

    let token_seq = e.current_seq();
    let db_id = e.database_id_for_change_log();
    let token = ResumeToken {
        database_id: db_id,
        tenant_prefix: vec![KS_USER],
        collection_id: 0,
        seq: token_seq,
        op_ordinal: 0,
    };

    // Expired entries + flush + compaction reclaim SST space.
    e.put(uk(b"exp1"), b"gone".to_vec(), 1).unwrap();
    e.put(uk(b"exp2"), b"gone".to_vec(), 1).unwrap();
    e.sync_wal().unwrap();
    e.force_flush().unwrap();
    let _ = e.compact_once();
    e.drain_compaction().unwrap();
    e.force_roll_wal_for_test().unwrap();

    let cfg = e.change_log_config().unwrap().clone();
    let mut manifest = e.change_log_manifest().unwrap().clone();
    let earliest_before = manifest.earliest_seq().unwrap();
    assert!(
        earliest_before <= token.seq,
        "resume token must still be inside the retention window"
    );

    // Prune under a large retention window must keep the token's segment.
    let removed = change_log::prune(&cfg, &mut manifest).unwrap();
    assert_eq!(removed, 0, "in-window prune must not drop segments");
    assert!(
        manifest.earliest_seq().unwrap() <= token.seq,
        "token seq must remain covered after prune"
    );

    // Reopen and confirm the archived segment file still exists on disk.
    drop(e);
    let e = open(&dir, 3600, 64 * 1024 * 1024);
    let manifest = e.change_log_manifest().unwrap();
    assert!(!manifest.segments.is_empty());
    for seg in &manifest.segments {
        let path = e
            .change_log_config()
            .unwrap()
            .archive_dir
            .join(&seg.file_name);
        assert!(
            path.is_file(),
            "archive segment {} missing after TTL compaction",
            seg.file_name
        );
    }
    assert!(manifest.earliest_seq().unwrap() <= token.seq);
}
