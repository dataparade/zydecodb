//! Regression: a memtable submitted to the flush worker left the read set at
//! submit time, so a snapshot taken while a flush was in flight missed that
//! memtable's entire contents until the SSTable apply landed (and, before the
//! fix, forever when the apply ordering lost the window). The memtable now
//! stays in the read set until the apply publishes the SSTable.
//!
//! Both the plain `put` path and `write_batch_with_sys` are covered: the bug
//! was timing-dependent (fast writers outran the flush apply), so the two
//! write shapes are both worth pinning.

use tempfile::TempDir;
use zydecodb_engine::engine::{BatchOp, Engine, EngineConfig};
use zydecodb_engine::keys::KS_USER;

const N: u32 = 200_000;

fn open(dir: &TempDir) -> Engine {
    Engine::open(EngineConfig {
        data_dir: dir.path().join("data"),
        wal_dir: dir.path().join("data/wal"),
        block_cache_bytes: 64 * 1024 * 1024,
        // Small memtable: the load spans many freeze/flush cycles, so the
        // final snapshot lands mid-flush with high probability.
        memtable_flush_threshold: 4 * 1024 * 1024,
        ..Default::default()
    })
    .unwrap()
    .with_group_commit(false)
}

fn uk(i: u32) -> Vec<u8> {
    let mut v = vec![KS_USER];
    v.extend_from_slice(format!("f{i:07}").as_bytes());
    v
}

fn val(i: u32) -> Vec<u8> {
    format!("v{i}:{}", "x".repeat(150)).into_bytes()
}

fn count_scan(e: &Engine) -> usize {
    let snap = e.snapshot_owned();
    let lo = vec![KS_USER];
    let mut hi = vec![KS_USER];
    hi.extend_from_slice(&[0xff; 16]);
    let mut n = 0;
    for item in snap.scan(lo, hi).unwrap() {
        item.unwrap();
        n += 1;
    }
    n
}

fn load(e: &mut Engine, with_sys: bool) {
    let sys_val = vec![b'c'; 350];
    for i in 0..N {
        loop {
            let r = if with_sys {
                e.write_batch_with_sys(
                    vec![BatchOp::Put {
                        key: uk(i),
                        value: val(i),
                        expires_at: 0,
                    }],
                    vec![(
                        vec![zydecodb_engine::keys::KS_SYSTEM, b'c'],
                        sys_val.clone(),
                    )],
                )
            } else {
                e.put(uk(i), val(i), 0)
            };
            match r {
                Ok(_) => break,
                Err(zydecodb_engine::errors::EngineError::EngineBusy(_)) => {
                    e.drain_background_work().unwrap();
                }
                Err(e) => panic!("write {i} failed: {e}"),
            }
        }
    }
    e.sync_wal().unwrap();
}

#[cfg_attr(miri, ignore = "200k puts; Miri cannot finish this probe")]
#[test]
fn live_reads_cover_in_flight_flush_plain_puts() {
    let dir = TempDir::new().unwrap();
    let mut e = open(&dir);
    load(&mut e, false);
    // Snapshot immediately after the load: a flush is almost certainly in
    // flight, and every written key must still be visible.
    assert_eq!(count_scan(&e), N as usize);
}

#[cfg_attr(miri, ignore = "200k puts; Miri cannot finish this probe")]
#[test]
fn live_reads_cover_in_flight_flush_batch_with_sys() {
    let dir = TempDir::new().unwrap();
    let mut e = open(&dir);
    load(&mut e, true);
    assert_eq!(count_scan(&e), N as usize);
}
