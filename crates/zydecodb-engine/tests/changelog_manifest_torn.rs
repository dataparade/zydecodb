//! Change-log `manifest.json` torn-write: mid-rename / partial tmp must never
//! leave a partial JSON that opens as a valid wrong state.
//!
//! Reopen sees either the previous complete manifest or the new complete one.

#![cfg(feature = "failpoints")]

use std::sync::Mutex;
use tempfile::TempDir;
use zydecodb_engine::change_log::{
    self, ArchiveSegment, ChangeLogConfig, ChangeLogManifest, MANIFEST_NAME,
};
use zydecodb_engine::failpoints::{
    CHANGELOG_AFTER_MANIFEST_RENAME, CHANGELOG_BEFORE_MANIFEST_RENAME,
};

fn fail_lock() -> &'static Mutex<()> {
    static LOCK: Mutex<()> = Mutex::new(());
    &LOCK
}

fn archive_cfg(dir: &TempDir) -> ChangeLogConfig {
    let archive = dir.path().join("archive");
    std::fs::create_dir_all(&archive).unwrap();
    ChangeLogConfig {
        archive_dir: archive,
        retention_secs: 3600,
        retention_bytes: 64 * 1024 * 1024,
    }
}

fn seg(id: u64, min: u64, max: u64) -> ArchiveSegment {
    ArchiveSegment {
        segment_id: id,
        min_seq: min,
        max_seq: max,
        sealed_unix_ms: 1_700_000_000_000,
        size_bytes: 100,
        file_name: format!("wal-{:06}.log", id),
    }
}

#[test]
fn kill_before_manifest_rename_keeps_old_manifest() {
    let _g = fail_lock().lock().unwrap_or_else(|p| p.into_inner());
    let _scenario = fail::FailScenario::setup();
    let dir = TempDir::new().unwrap();
    let cfg = archive_cfg(&dir);
    let db_id = [7u8; 16];

    let mut old = ChangeLogManifest::new(db_id);
    old.segments.push(seg(1, 1, 10));
    change_log::persist_manifest(&cfg, &old).unwrap();

    let mut next = ChangeLogManifest::new(db_id);
    next.segments.push(seg(1, 1, 10));
    next.segments.push(seg(2, 11, 20));

    fail::cfg(CHANGELOG_BEFORE_MANIFEST_RENAME, "1*return").unwrap();
    let err = change_log::persist_manifest(&cfg, &next);
    assert!(err.is_err(), "before-rename failpoint must surface");
    fail::remove(CHANGELOG_BEFORE_MANIFEST_RENAME);

    let reopened = change_log::open_manifest(&cfg, db_id).unwrap();
    assert_eq!(
        reopened.segments.len(),
        1,
        "must keep previous complete manifest"
    );
    assert_eq!(reopened.segments[0].segment_id, 1);
    // tmp may exist; must not be the published name with partial content.
    let published = cfg.archive_dir.join(MANIFEST_NAME);
    let text = std::fs::read_to_string(&published).unwrap();
    let parsed: ChangeLogManifest = serde_json::from_str(&text).unwrap();
    assert_eq!(parsed.segments.len(), 1);
}

#[test]
fn kill_after_manifest_rename_sees_new_manifest() {
    let _g = fail_lock().lock().unwrap_or_else(|p| p.into_inner());
    let _scenario = fail::FailScenario::setup();
    let dir = TempDir::new().unwrap();
    let cfg = archive_cfg(&dir);
    let db_id = [8u8; 16];

    let mut old = ChangeLogManifest::new(db_id);
    old.segments.push(seg(1, 1, 10));
    change_log::persist_manifest(&cfg, &old).unwrap();

    let mut next = ChangeLogManifest::new(db_id);
    next.segments.push(seg(1, 1, 10));
    next.segments.push(seg(2, 11, 20));

    fail::cfg(CHANGELOG_AFTER_MANIFEST_RENAME, "1*return").unwrap();
    let err = change_log::persist_manifest(&cfg, &next);
    assert!(err.is_err(), "after-rename failpoint must surface");
    fail::remove(CHANGELOG_AFTER_MANIFEST_RENAME);

    let reopened = change_log::open_manifest(&cfg, db_id).unwrap();
    assert_eq!(
        reopened.segments.len(),
        2,
        "after rename the new complete manifest is visible"
    );
}

#[test]
fn failed_persist_never_claims_segment_and_retry_repairs() {
    let _g = fail_lock().lock().unwrap_or_else(|p| p.into_inner());
    let _scenario = fail::FailScenario::setup();
    let dir = TempDir::new().unwrap();
    let cfg = archive_cfg(&dir);
    let db_id = [10u8; 16];
    let wal_dir = dir.path().join("wal");
    std::fs::create_dir_all(&wal_dir).unwrap();

    // A real sealed WAL segment with two records.
    let seg_path = wal_dir.join(zydecodb_engine::wal::segment_filename(1));
    {
        use std::io::Write;
        let mut f = std::fs::File::create(&seg_path).unwrap();
        f.write_all(&1u64.to_be_bytes()).unwrap();
        f.write_all(&[zydecodb_engine::wal::WAL_FORMAT_VERSION]).unwrap();
        for rec in [
            zydecodb_engine::wal::WalRecord::put(1, 0, b"k1".to_vec(), b"v1".to_vec()),
            zydecodb_engine::wal::WalRecord::put(2, 0, b"k2".to_vec(), b"v2".to_vec()),
        ] {
            f.write_all(&rec.encode()).unwrap();
        }
        f.sync_all().unwrap();
    }

    let mut manifest = change_log::open_manifest(&cfg, db_id).unwrap();

    // Crash the manifest publish: the archive must fail WITHOUT the in-memory
    // manifest claiming the segment (the unlink gate trusts that state).
    fail::cfg(CHANGELOG_BEFORE_MANIFEST_RENAME, "1*return").unwrap();
    let err = change_log::archive_segment(&cfg, &mut manifest, &wal_dir, 1, 2);
    assert!(err.is_err(), "publish failure must surface: {err:?}");
    fail::remove(CHANGELOG_BEFORE_MANIFEST_RENAME);
    assert!(
        !change_log::is_archived(&manifest, 1),
        "in-memory manifest must not claim a segment the disk never published"
    );

    // Retry: the stale destination from the failed attempt is re-archived
    // idempotently, and the manifest on disk now matches memory.
    change_log::archive_segment(&cfg, &mut manifest, &wal_dir, 1, 2).unwrap();
    assert!(change_log::is_archived(&manifest, 1));
    let reopened = change_log::open_manifest(&cfg, db_id).unwrap();
    assert!(
        reopened.contains_segment(1),
        "durable manifest must list the segment after successful retry"
    );
}

#[test]
fn partial_tmp_never_opens_as_published_manifest() {
    let dir = TempDir::new().unwrap();
    let cfg = archive_cfg(&dir);
    let db_id = [9u8; 16];

    let mut good = ChangeLogManifest::new(db_id);
    good.segments.push(seg(1, 1, 5));
    change_log::persist_manifest(&cfg, &good).unwrap();

    // Simulate a torn write of the tmp file left beside a good published manifest.
    let tmp = cfg.archive_dir.join("manifest.json.tmp");
    std::fs::write(&tmp, b"{\"database_id_hex\":\"incomplete").unwrap();

    let reopened = change_log::open_manifest(&cfg, db_id).unwrap();
    assert_eq!(reopened.segments.len(), 1);
    assert_eq!(reopened.segments[0].max_seq, 5);

    // If only a torn tmp exists (no published manifest), open must not invent state.
    let dir2 = TempDir::new().unwrap();
    let cfg2 = archive_cfg(&dir2);
    std::fs::write(
        cfg2.archive_dir.join("manifest.json.tmp"),
        b"{\"database_id_hex\":\"zz\",",
    )
    .unwrap();
    // No published manifest → open creates a fresh empty one (never parses tmp).
    let fresh = change_log::open_manifest(&cfg2, db_id).unwrap();
    assert!(fresh.segments.is_empty());
    let published = std::fs::read_to_string(cfg2.archive_dir.join(MANIFEST_NAME)).unwrap();
    assert!(
        serde_json::from_str::<ChangeLogManifest>(&published).is_ok(),
        "published manifest must be complete JSON"
    );
}
