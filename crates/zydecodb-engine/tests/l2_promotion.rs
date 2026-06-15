//! L1→L2 overlap promotion packs outputs by target_file_bytes.

// Tests build CompactionConfig::default() then tweak a couple of fields.
#![allow(clippy::field_reassign_with_default)]

use std::path::Path;
use std::sync::atomic::AtomicU64;
use tempfile::TempDir;
use zydecodb_engine::block_cache::BlockCache;
use zydecodb_engine::compaction::{CompactionConfig, CompactionJob};
use zydecodb_engine::compaction_worker::execute_compaction;
use zydecodb_engine::entry::Entry;
use zydecodb_engine::keys::{EntryKind, InternalKey, KS_USER};
use zydecodb_engine::manifest::SstableMeta;
use zydecodb_engine::reader_cache::ReaderCache;
use zydecodb_engine::sstable;

fn uk(s: &[u8]) -> Vec<u8> {
    let mut v = vec![KS_USER];
    v.extend_from_slice(s);
    v
}

fn write_sst(data_dir: &Path, id: u64, level: u8, keys: &[&[u8]], value_len: usize) -> SstableMeta {
    let value = vec![0xCDu8; value_len];
    let pairs: Vec<(InternalKey, Entry)> = keys
        .iter()
        .enumerate()
        .map(|(i, k)| {
            (
                InternalKey::new(uk(k), id + i as u64, EntryKind::Value),
                Entry::value(value.clone(), None),
            )
        })
        .collect();
    let sst = sstable::build(&pairs, true);
    let path = data_dir.join(format!("{:08}.sst", id));
    std::fs::write(&path, &sst.bytes).unwrap();
    SstableMeta {
        id,
        level,
        min_key: pairs.first().unwrap().0.user_key.clone(),
        max_key: pairs.last().unwrap().0.user_key.clone(),
        min_seq: id,
        max_seq: id + keys.len() as u64,
        size_bytes: sst.bytes.len() as u64,
    }
}

#[test]
fn l1_to_l2_overlap_promotion_packs_outputs() {
    let dir = TempDir::new().unwrap();
    let data_dir = dir.path().join("data");
    std::fs::create_dir_all(&data_dir).unwrap();

    let mut cfg = CompactionConfig::default();
    cfg.target_file_bytes = 8 * 1024;

    let l1 = write_sst(&data_dir, 10, 1, &[b"a", b"b", b"c", b"d"], 512);
    let l2 = write_sst(&data_dir, 20, 2, &[b"a", b"b", b"c", b"d", b"e", b"f"], 512);
    let total_bytes = l1.size_bytes + l2.size_bytes;
    let max_outputs = total_bytes.div_ceil(cfg.target_file_bytes) as usize;

    let job = CompactionJob {
        inputs: vec![l1.id, l2.id],
        input_level: 1,
        output_level: 2,
        priority_score: 0.0,
    };
    let cache = BlockCache::new(4 * 1024 * 1024);
    let reader_cache = ReaderCache::new(0);
    let next_id = AtomicU64::new(100);
    let result = execute_compaction(
        &job,
        &[l1, l2],
        &data_dir,
        &cfg,
        &cache,
        &reader_cache,
        &next_id,
        0,
        false,
    )
    .expect("execute_compaction");

    assert!(
        result.new_metas.len() <= max_outputs,
        "expected <= {max_outputs} outputs, got {}",
        result.new_metas.len()
    );
    assert!(!result.new_metas.is_empty());

    let out_path = data_dir.join(format!("{:08}.sst", result.new_metas[0].id));
    let reader =
        sstable::SstableReader::open_from_path(&out_path, result.new_metas[0].id, cache.clone())
            .expect("open L2 output");
    assert!(
        !reader.has_bloom(),
        "L2 output skips bloom when optimize_filters_for_hits is enabled"
    );
}
