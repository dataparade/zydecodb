//! STATS (0xF1) payload builder. Returns a JSON snapshot of engine state.

use serde::Serialize;

#[derive(Debug, Serialize)]
pub struct SstableStat {
    pub id: u64,
    pub size_bytes: u64,
    pub min_seq: u64,
    pub max_seq: u64,
    pub entries: u64,
}

#[derive(Debug, Serialize)]
pub struct WalSegmentStat {
    pub id: u64,
    pub first_seq: u64,
    pub max_seq: u64,
    pub size_bytes: u64,
    pub active: bool,
}

#[derive(Debug, Serialize)]
pub struct Stats {
    pub uptime_s: u64,
    pub last_durable_seq: u64,
    pub memtable_bytes: u64,
    pub memtable_entries: u64,
    pub immutable_memtables: u64,
    pub sstables: Vec<SstableStat>,
    pub wal_segments: Vec<WalSegmentStat>,
}

impl Stats {
    pub fn to_json(&self) -> Vec<u8> {
        serde_json::to_vec(self).unwrap_or_else(|_| b"{}".to_vec())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn empty_stats_serialize() {
        let s = Stats {
            uptime_s: 0,
            last_durable_seq: 0,
            memtable_bytes: 0,
            memtable_entries: 0,
            immutable_memtables: 0,
            sstables: vec![],
            wal_segments: vec![],
        };
        let json = s.to_json();
        let v: serde_json::Value = serde_json::from_slice(&json).unwrap();
        assert_eq!(v["last_durable_seq"], 0);
        assert!(v["sstables"].is_array());
    }
}
