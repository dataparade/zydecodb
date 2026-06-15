//! LSM compaction: planning + supporting types.
//!
//! Leveled compaction: L0 → L1 → L2 with background execution on a dedicated
//! worker thread. Level-N promotions gather overlapping files at level N+1 and
//! k-way merge into packed outputs at `target_file_bytes`. The planner uses
//! RocksDB-style per-level scores (highest score >= 1.0 wins) and dynamic level
//! byte targets sized from the actual bottom-level footprint. The max level is a
//! compaction destination only — it never self-schedules.

use crate::manifest::SstableMeta;

/// Per-engine compaction configuration. Lives on [`crate::engine::EngineConfig`]
/// and is read by [`CompactionPlanner`] at every check.
#[derive(Debug, Clone, Copy)]
pub struct CompactionConfig {
    /// Number of L0 SSTables that triggers an L0->L1 compaction.
    pub l0_trigger: usize,
    /// Size ratio between adjacent levels. L_n target = l1_target * multiplier^(n-1).
    pub level_size_multiplier: u64,
    /// Target total bytes for L1. L2 target = L1 * multiplier, etc.
    pub l1_target_bytes: u64,
    /// Target bytes per output file. Compaction splits its output at this
    /// boundary so non-L0 levels stay non-overlapping with bounded file size.
    pub target_file_bytes: u64,
    /// Highest level the engine compacts into. Files at the highest level
    /// stay there; no L_{max+1} promotion. Three-level configuration in v1.
    pub max_level: u8,
    /// Estimated compaction debt above which writes are micro-delayed.
    pub soft_pending_compaction_bytes: u64,
    /// Estimated compaction debt above which writes are refused.
    pub hard_pending_compaction_bytes: u64,
    /// Minimum estimated byte reclaim before an L2-only GC job is scheduled.
    pub l2_gc_min_reclaim_bytes: u64,
    /// When true, skip bloom filters on output files at `max_level` (RocksDB
    /// `optimize_filters_for_hits`). Bottom-level blooms rarely help hot GETs.
    pub optimize_filters_for_hits: bool,
}

impl Default for CompactionConfig {
    fn default() -> Self {
        CompactionConfig {
            l0_trigger: 4,
            level_size_multiplier: 10,
            l1_target_bytes: 256 * 1024 * 1024,  // 256 MB
            target_file_bytes: 64 * 1024 * 1024, // 64 MB per output file
            max_level: 2,
            soft_pending_compaction_bytes: 256 * 1024 * 1024,
            hard_pending_compaction_bytes: 512 * 1024 * 1024,
            l2_gc_min_reclaim_bytes: 4 * 1024 * 1024,
            optimize_filters_for_hits: true,
        }
    }
}

/// A scheduled compaction. The planner computes this; the engine executes it.
#[derive(Debug, Clone)]
pub struct CompactionJob {
    pub inputs: Vec<u64>,
    /// Level of the primary input file(s) being compacted (0 for L0 ingest).
    pub input_level: u8,
    pub output_level: u8,
    /// Planner score for the level that produced this job (submit coalescing).
    pub priority_score: f64,
}

/// Same-level compaction cannot reduce file count when every input is already
/// >= target_file_bytes/2 and pairwise non-overlapping.
pub fn same_level_compaction_would_make_progress(
    input_metas: &[SstableMeta],
    target_file_bytes: u64,
) -> bool {
    if input_metas.len() < 2 {
        return false;
    }
    let half = target_file_bytes / 2;
    if input_metas.iter().any(|m| m.size_bytes < half) {
        return true;
    }
    for i in 0..input_metas.len() {
        for j in (i + 1)..input_metas.len() {
            let a = &input_metas[i];
            let b = &input_metas[j];
            if overlaps(&a.min_key, &a.max_key, &b.min_key, &b.max_key) {
                return true;
            }
        }
    }
    false
}

/// Plans compactions over a snapshot of the SSTable catalog. Pure; takes a
/// borrowed slice of metas and returns a job (or None). The engine takes the
/// job and executes it.
pub struct CompactionPlanner<'a> {
    metas: &'a [SstableMeta],
    cfg: &'a CompactionConfig,
}

const SCORE_THRESHOLD: f64 = 1.0;

impl<'a> CompactionPlanner<'a> {
    pub fn new(metas: &'a [SstableMeta], cfg: &'a CompactionConfig) -> Self {
        CompactionPlanner { metas, cfg }
    }

    /// Score for a level (0.0 when idle). Public for submit coalescing.
    pub fn level_score(&self, level: u8) -> f64 {
        self.compute_level_scores()
            .into_iter()
            .find(|(l, _)| *l == level)
            .map(|(_, s)| s)
            .unwrap_or(0.0)
    }

    /// Estimate of bytes waiting to be compacted (for backpressure + metrics).
    ///
    /// Uses **unweighted** per-level byte excess against dynamic targets plus
    /// L0 file-trigger debt. Fanout-weighted excess blew past 1 GB on small
    /// databases (dynamic L1 targets tiny vs live bytes) and tripped the hard
    /// backpressure limit during otherwise healthy soaks.
    pub fn estimate_pending_bytes(&self) -> u64 {
        let max = self.cfg.max_level;
        let mut pending = 0u64;
        for level in 1..=max {
            let target = self.effective_level_target(level);
            if target == 0 {
                continue;
            }
            pending = pending.saturating_add(self.level_bytes(level).saturating_sub(target));
        }
        // L0 file-trigger debt (RocksDB counts size(L0)+size(base) at trigger).
        let l0_files = self.level_file_count(0);
        if l0_files >= self.cfg.l0_trigger {
            pending = pending.saturating_add(self.level_bytes(0));
            pending = pending.saturating_add(self.level_bytes(self.base_level()));
        } else if l0_files >= 2 {
            pending = pending.saturating_add(self.level_bytes(0));
        }
        pending
    }

    /// Compute one compaction job, or `None` if no level needs work.
    pub fn plan(&self) -> Option<CompactionJob> {
        for level in self.levels_by_score() {
            if let Some(mut job) = self.plan_for_level(level) {
                job.priority_score = self.level_score(level);
                return Some(job);
            }
        }
        self.plan_l2_gc()
    }

    fn levels_by_score(&self) -> Vec<u8> {
        let mut scored: Vec<(u8, f64)> = self
            .compute_level_scores()
            .into_iter()
            .filter(|(_, score)| *score >= SCORE_THRESHOLD)
            .collect();
        scored.sort_by(|(la, sa), (lb, sb)| {
            match sb.partial_cmp(sa).unwrap_or(std::cmp::Ordering::Equal) {
                std::cmp::Ordering::Equal => la.cmp(lb), // lower level wins at equal score
                ord => ord,
            }
        });
        scored.into_iter().map(|(level, _)| level).collect()
    }

    fn plan_for_level(&self, level: u8) -> Option<CompactionJob> {
        match level {
            0 => self.plan_l0(),
            l => self.plan_level(l),
        }
    }

    fn compute_level_scores(&self) -> Vec<(u8, f64)> {
        let mut scores = Vec::new();

        let l0_files = self.level_file_count(0);
        let l0_bytes = self.level_bytes(0);
        let l0_file_score = l0_files as f64 / self.cfg.l0_trigger.max(1) as f64;
        let l0_byte_score = if self.cfg.l1_target_bytes > 0 {
            l0_bytes as f64 / self.cfg.l1_target_bytes as f64
        } else {
            0.0
        };
        scores.push((0, l0_file_score.max(l0_byte_score)));

        // Max level is a destination only — not a compaction source.
        for level in 1..self.cfg.max_level {
            let target = self.effective_level_target(level);
            let score = if target > 0 {
                self.level_bytes(level) as f64 / target as f64
            } else {
                0.0
            };
            scores.push((level, score));
        }

        scores
    }

    fn level_bytes(&self, level: u8) -> u64 {
        self.metas
            .iter()
            .filter(|m| m.level == level)
            .map(|m| m.size_bytes)
            .sum()
    }

    fn level_file_count(&self, level: u8) -> usize {
        self.metas.iter().filter(|m| m.level == level).count()
    }

    fn min_level_bytes(&self) -> u64 {
        self.cfg.l1_target_bytes / self.cfg.level_size_multiplier.max(1)
    }

    fn nominal_max_level_target(&self) -> u64 {
        let mut t = self.cfg.l1_target_bytes;
        for _ in 1..self.cfg.max_level {
            t = t.saturating_mul(self.cfg.level_size_multiplier);
        }
        t
    }

    fn effective_level_targets(&self) -> Vec<u64> {
        let max = self.cfg.max_level;
        let min_level = self.min_level_bytes();
        let mut targets = vec![0u64; (max + 1) as usize];

        let max_actual = self.level_bytes(max);
        targets[max as usize] = if max_actual > 0 {
            max_actual
        } else {
            self.nominal_max_level_target()
        };

        for lvl in (1..max).rev() {
            let next = targets[(lvl + 1) as usize];
            let computed = next / self.cfg.level_size_multiplier.max(1);
            targets[lvl as usize] = if computed >= min_level { computed } else { 0 };
        }
        targets
    }

    fn effective_level_target(&self, level: u8) -> u64 {
        if level == 0 {
            return 0;
        }
        self.effective_level_targets()[level as usize]
    }

    fn base_level(&self) -> u8 {
        let max = self.cfg.max_level;
        for lvl in 1..max {
            if self.effective_level_target(lvl) > 0 {
                return lvl;
            }
        }
        max
    }

    fn plan_l0(&self) -> Option<CompactionJob> {
        let l0: Vec<&SstableMeta> = self.metas.iter().filter(|m| m.level == 0).collect();
        if l0.len() < self.cfg.l0_trigger {
            return None;
        }
        let base = self.base_level();
        let (min_key, max_key) = key_span(l0.iter().copied()).expect("non-empty L0");
        let mut inputs: Vec<u64> = l0.iter().map(|m| m.id).collect();
        for m in self.metas.iter().filter(|m| m.level == base) {
            if overlaps(&m.min_key, &m.max_key, &min_key, &max_key) {
                inputs.push(m.id);
            }
        }
        Some(CompactionJob {
            inputs,
            input_level: 0,
            output_level: base,
            priority_score: 0.0,
        })
    }

    fn plan_level(&self, level: u8) -> Option<CompactionJob> {
        let lvl: Vec<&SstableMeta> = self.metas.iter().filter(|m| m.level == level).collect();
        let target = self.effective_level_target(level);
        if target == 0 {
            return None;
        }
        let total: u64 = lvl.iter().map(|m| m.size_bytes).sum();
        if total <= target {
            return None;
        }
        let oldest = lvl.iter().min_by_key(|m| m.id)?;
        let (min_key, max_key) = (&oldest.min_key, &oldest.max_key);
        let mut inputs = vec![oldest.id];
        let next_level = level + 1;
        for m in self.metas.iter().filter(|m| m.level == next_level) {
            if overlaps(&m.min_key, &m.max_key, min_key, max_key) {
                inputs.push(m.id);
            }
        }
        Some(CompactionJob {
            inputs,
            input_level: level,
            output_level: next_level,
            priority_score: 0.0,
        })
    }

    /// Bottommost L2 GC when leveled promotion is idle. Merges overlapping L2
    /// files in-place to drop tombstones and superseded versions.
    fn plan_l2_gc(&self) -> Option<CompactionJob> {
        let max = self.cfg.max_level;
        let l2: Vec<&SstableMeta> = self.metas.iter().filter(|m| m.level == max).collect();
        if l2.is_empty() {
            return None;
        }
        let oldest = l2.iter().min_by_key(|m| m.id)?;
        let oldest_id = oldest.id;
        let min_key = oldest.min_key.clone();
        let max_key = oldest.max_key.clone();
        let mut inputs = vec![oldest_id];
        for m in l2 {
            if m.id != oldest_id && overlaps(&m.min_key, &m.max_key, &min_key, &max_key) {
                inputs.push(m.id);
            }
        }
        if inputs.len() < 2 {
            return None;
        }
        let input_bytes: u64 = self
            .metas
            .iter()
            .filter(|m| inputs.contains(&m.id))
            .map(|m| m.size_bytes)
            .sum();
        let estimated_output = input_bytes / 2;
        let reclaim = input_bytes.saturating_sub(estimated_output);
        if reclaim < self.cfg.l2_gc_min_reclaim_bytes {
            return None;
        }
        Some(CompactionJob {
            inputs,
            input_level: max,
            output_level: max,
            priority_score: 0.0,
        })
    }
}

/// Min/max user-key span across a set of metas.
fn key_span<'a, I: IntoIterator<Item = &'a SstableMeta>>(iter: I) -> Option<(Vec<u8>, Vec<u8>)> {
    let mut it = iter.into_iter();
    let first = it.next()?;
    let mut min = first.min_key.clone();
    let mut max = first.max_key.clone();
    for m in it {
        if m.min_key < min {
            min = m.min_key.clone();
        }
        if m.max_key > max {
            max = m.max_key.clone();
        }
    }
    Some((min, max))
}

fn overlaps(a_min: &[u8], a_max: &[u8], b_min: &[u8], b_max: &[u8]) -> bool {
    !(a_max < b_min || b_max < a_min)
}

#[cfg(test)]
// Tests build CompactionConfig::default() then tweak a couple of fields; the
// reassign-after-default pattern reads more clearly here than a full struct literal.
#[allow(clippy::field_reassign_with_default)]
mod tests {
    use super::*;

    fn meta(id: u64, level: u8, min: &[u8], max: &[u8], size: u64) -> SstableMeta {
        SstableMeta {
            id,
            level,
            min_key: min.to_vec(),
            max_key: max.to_vec(),
            min_seq: id,
            max_seq: id,
            size_bytes: size,
        }
    }

    #[test]
    fn no_l0_pressure_returns_none() {
        let cfg = CompactionConfig::default();
        let metas = vec![meta(1, 0, b"a", b"z", 1000)];
        assert!(CompactionPlanner::new(&metas, &cfg).plan().is_none());
    }

    #[test]
    fn l0_trigger_picks_all_l0_plus_overlapping_l1() {
        let cfg = CompactionConfig::default();
        let metas = vec![
            meta(1, 0, b"a", b"m", 100),
            meta(2, 0, b"b", b"n", 100),
            meta(3, 0, b"c", b"o", 100),
            meta(4, 0, b"d", b"p", 100),
            meta(5, 1, b"a", b"e", 100), // overlaps
            meta(6, 1, b"u", b"z", 100), // does NOT overlap
        ];
        let job = CompactionPlanner::new(&metas, &cfg).plan().unwrap();
        assert_eq!(job.output_level, 1);
        let mut ids = job.inputs.clone();
        ids.sort();
        assert_eq!(ids, vec![1, 2, 3, 4, 5]);
    }

    #[test]
    fn level_pressure_promotes_oldest_into_next_level() {
        let mut cfg = CompactionConfig::default();
        cfg.l0_trigger = 999; // disable L0 trigger
        cfg.l1_target_bytes = 100; // tiny L1 target so any L1 content trips it
        let metas = vec![
            meta(10, 1, b"a", b"m", 100),
            meta(11, 1, b"n", b"r", 100),
            meta(12, 1, b"s", b"z", 100),
        ];
        let job = CompactionPlanner::new(&metas, &cfg).plan().unwrap();
        assert_eq!(job.output_level, 2);
        assert_eq!(job.inputs, vec![10]);
    }

    #[test]
    fn level_pressure_includes_overlapping_next_level_files() {
        let mut cfg = CompactionConfig::default();
        cfg.l0_trigger = 999;
        cfg.l1_target_bytes = 100;
        let metas = vec![
            meta(10, 1, b"a", b"m", 100),
            meta(11, 1, b"n", b"r", 100),
            meta(12, 1, b"s", b"z", 100),
            meta(20, 2, b"b", b"f", 100),
            meta(21, 2, b"u", b"z", 100),
        ];
        let job = CompactionPlanner::new(&metas, &cfg).plan().unwrap();
        assert_eq!(job.output_level, 2);
        let mut ids = job.inputs.clone();
        ids.sort();
        assert_eq!(ids, vec![10, 20]);
    }

    #[test]
    fn l2_gc_plans_overlapping_files_when_idle() {
        let mut cfg = CompactionConfig::default();
        cfg.l0_trigger = 999;
        cfg.l1_target_bytes = 10_000_000;
        cfg.l2_gc_min_reclaim_bytes = 1024;
        let metas = vec![
            meta(30, 2, b"a", b"m", 8 * 1024),
            meta(31, 2, b"l", b"z", 8 * 1024),
            meta(32, 2, b"m", b"r", 8 * 1024),
        ];
        let job = CompactionPlanner::new(&metas, &cfg).plan().expect("l2 gc");
        assert_eq!(job.input_level, 2);
        assert_eq!(job.output_level, 2);
        assert!(job.inputs.contains(&30));
        assert!(job.inputs.contains(&32));
    }

    #[test]
    fn max_level_only_catalog_plans_nothing() {
        let mut cfg = CompactionConfig::default();
        cfg.l0_trigger = 999;
        cfg.l1_target_bytes = 10;
        let metas = vec![
            meta(30, 2, b"a", b"m", 100),
            meta(31, 2, b"f", b"r", 100),
            meta(32, 2, b"s", b"z", 100),
        ];
        assert!(CompactionPlanner::new(&metas, &cfg).plan().is_none());
    }

    #[test]
    fn l0_wins_when_pressured_over_large_l2_catalog() {
        let cfg = CompactionConfig::default();
        let metas = vec![
            meta(1, 0, b"a", b"d", 100),
            meta(2, 0, b"e", b"h", 100),
            meta(3, 0, b"i", b"l", 100),
            meta(4, 0, b"m", b"p", 100),
            meta(10, 2, b"a", b"b", 100),
            meta(11, 2, b"c", b"d", 100),
            meta(12, 2, b"e", b"f", 100),
            meta(13, 2, b"g", b"h", 100),
            meta(14, 2, b"i", b"j", 100),
            meta(15, 2, b"k", b"l", 100),
            meta(16, 2, b"m", b"n", 100),
            meta(17, 2, b"o", b"p", 100),
            meta(18, 2, b"q", b"r", 100),
        ];
        let job = CompactionPlanner::new(&metas, &cfg).plan().unwrap();
        assert_eq!(job.input_level, 0);
        assert_ne!(job.input_level, job.output_level);
        assert!(job.inputs.iter().any(|&id| (1..=4).contains(&id)));
    }

    #[test]
    fn score_picks_highest_level() {
        let mut cfg = CompactionConfig::default();
        cfg.l0_trigger = 4;
        cfg.l1_target_bytes = 100;
        let metas = vec![
            meta(1, 0, b"a", b"d", 100),
            meta(2, 0, b"e", b"h", 100),
            meta(3, 0, b"i", b"l", 100),
            meta(4, 0, b"m", b"p", 100),
            meta(10, 1, b"a", b"m", 200),
            meta(11, 1, b"n", b"r", 200),
            meta(12, 1, b"s", b"z", 200),
        ];
        let planner = CompactionPlanner::new(&metas, &cfg);
        let l1_score = planner.level_score(1);
        let l0_score = planner.level_score(0);
        assert!(l1_score > l0_score);
        let job = planner.plan().unwrap();
        assert_eq!(job.output_level, 2);
        assert_eq!(job.inputs, vec![10]);
    }

    #[test]
    fn pending_bytes_counts_l0_file_pressure_before_byte_target() {
        let cfg = CompactionConfig::default();
        let metas = vec![
            meta(1, 0, b"a", b"c", 64 * 1024 * 1024),
            meta(2, 0, b"d", b"f", 64 * 1024 * 1024),
            meta(3, 0, b"g", b"i", 64 * 1024 * 1024),
        ];
        let pending = CompactionPlanner::new(&metas, &cfg).estimate_pending_bytes();
        assert_eq!(pending, 3 * 64 * 1024 * 1024);
    }

    #[test]
    fn pending_bytes_at_l0_trigger_includes_base_level() {
        let cfg = CompactionConfig::default();
        let metas = vec![
            meta(1, 0, b"a", b"b", 50),
            meta(2, 0, b"c", b"d", 50),
            meta(3, 0, b"e", b"f", 50),
            meta(4, 0, b"g", b"h", 50),
            meta(10, 1, b"a", b"h", 100),
        ];
        let pending = CompactionPlanner::new(&metas, &cfg).estimate_pending_bytes();
        assert_eq!(pending, 200 + 100);
    }

    #[test]
    fn pending_bytes_stays_bounded_with_pressured_l1() {
        let cfg = CompactionConfig::default();
        let metas = vec![
            meta(1, 0, b"a", b"b", 64 * 1024 * 1024),
            meta(2, 0, b"c", b"d", 64 * 1024 * 1024),
            meta(3, 0, b"e", b"f", 64 * 1024 * 1024),
            meta(10, 1, b"a", b"m", 64 * 1024 * 1024),
            meta(11, 1, b"n", b"z", 64 * 1024 * 1024),
            meta(12, 1, b"o", b"z", 64 * 1024 * 1024),
            meta(13, 1, b"p", b"z", 64 * 1024 * 1024),
            meta(20, 2, b"a", b"m", 64 * 1024 * 1024),
        ];
        let pending = CompactionPlanner::new(&metas, &cfg).estimate_pending_bytes();
        assert!(
            pending < cfg.hard_pending_compaction_bytes,
            "pending {pending} must stay below hard limit {}",
            cfg.hard_pending_compaction_bytes
        );
    }

    #[test]
    fn disjoint_l2_no_steady_repack_below_ceiling() {
        let mut cfg = CompactionConfig::default();
        cfg.l0_trigger = 999;
        let tfb = cfg.target_file_bytes;
        let metas: Vec<SstableMeta> = (10..=15u64)
            .map(|id| {
                let b = b'a' + (id - 10) as u8;
                meta(id, 2, &[b], &[b], tfb)
            })
            .collect();
        let job = CompactionPlanner::new(&metas, &cfg).plan();
        assert!(job.is_none());
    }

    #[test]
    fn eight_target_sized_disjoint_l2_no_job() {
        let mut cfg = CompactionConfig::default();
        cfg.l0_trigger = 999;
        let tfb = cfg.target_file_bytes;
        let metas: Vec<SstableMeta> = (10..=17u64)
            .map(|id| {
                let b = b'a' + (id - 10) as u8;
                meta(id, 2, &[b], &[b], tfb)
            })
            .collect();
        assert!(CompactionPlanner::new(&metas, &cfg).plan().is_none());
    }

    #[test]
    fn max_level_not_in_compaction_scores() {
        let mut cfg = CompactionConfig::default();
        cfg.l0_trigger = 999;
        let tfb = cfg.target_file_bytes;
        let metas: Vec<SstableMeta> = (10..=17u64)
            .map(|id| {
                let b = b'a' + (id - 10) as u8;
                meta(id, 2, &[b], &[b], tfb)
            })
            .collect();
        let planner = CompactionPlanner::new(&metas, &cfg);
        assert_eq!(planner.level_score(cfg.max_level), 0.0);
        assert!(planner.plan().is_none());
    }

    #[test]
    fn same_level_no_progress_guard_rejects_eight_target_files() {
        let tfb = CompactionConfig::default().target_file_bytes;
        let metas: Vec<SstableMeta> = (10..=17u64)
            .map(|id| {
                let b = b'a' + (id - 10) as u8;
                meta(id, 2, &[b], &[b], tfb)
            })
            .collect();
        assert!(!same_level_compaction_would_make_progress(&metas, tfb));
    }

    #[test]
    fn l1_promotion_includes_overlapping_l2_when_both_pressured() {
        let mut cfg = CompactionConfig::default();
        cfg.l0_trigger = 999;
        cfg.l1_target_bytes = 100;
        let metas = vec![
            meta(10, 1, b"a", b"m", 100),
            meta(11, 1, b"n", b"r", 100),
            meta(12, 1, b"s", b"z", 100),
            meta(20, 2, b"b", b"f", 100),
        ];
        let job = CompactionPlanner::new(&metas, &cfg).plan().unwrap();
        assert_eq!(job.output_level, 2);
        assert!(job.inputs.contains(&10));
        assert!(job.inputs.contains(&20));
    }

    #[test]
    fn no_idle_trap_with_l0_pressure_and_l2_present() {
        let mut cfg = CompactionConfig::default();
        cfg.l1_target_bytes = 256 * 1024 * 1024;
        let small_l2 = cfg.l1_target_bytes / cfg.level_size_multiplier / 2;
        let metas = vec![
            meta(1, 0, b"a", b"d", 100),
            meta(2, 0, b"e", b"h", 100),
            meta(3, 0, b"i", b"l", 100),
            meta(4, 0, b"m", b"p", 100),
            meta(10, 2, b"a", b"z", small_l2),
        ];
        let job = CompactionPlanner::new(&metas, &cfg).plan().unwrap();
        assert_eq!(job.output_level, 2);
        assert_ne!(job.input_level, job.output_level);
    }

    #[test]
    fn dynamic_l0_compacts_to_base_level() {
        let mut cfg = CompactionConfig::default();
        cfg.l1_target_bytes = 256 * 1024 * 1024;
        let small_l2 = cfg.l1_target_bytes / cfg.level_size_multiplier / 2;
        let metas = vec![
            meta(1, 0, b"a", b"m", 100),
            meta(2, 0, b"n", b"z", 100),
            meta(3, 0, b"o", b"p", 100),
            meta(4, 0, b"q", b"r", 100),
            meta(10, 2, b"a", b"z", small_l2),
        ];
        let planner = CompactionPlanner::new(&metas, &cfg);
        assert_eq!(planner.effective_level_target(1), 0);
        assert_eq!(planner.base_level(), 2);
        let job = planner.plan().unwrap();
        assert_eq!(job.output_level, 2);
    }

    #[test]
    fn dynamic_targets_shrink_when_db_small() {
        let cfg = CompactionConfig::default();
        let small_l2 = cfg.l1_target_bytes / cfg.level_size_multiplier / 2;
        let metas = vec![meta(10, 2, b"a", b"z", small_l2)];
        let planner = CompactionPlanner::new(&metas, &cfg);
        assert_eq!(planner.effective_level_target(1), 0);
        assert_eq!(planner.base_level(), 2);
    }

    #[test]
    fn key_span_handles_unsorted_inputs() {
        let metas = [
            meta(1, 0, b"m", b"q", 1),
            meta(2, 0, b"a", b"d", 1),
            meta(3, 0, b"x", b"z", 1),
        ];
        let (lo, hi) = key_span(metas.iter()).unwrap();
        assert_eq!(lo, b"a");
        assert_eq!(hi, b"z");
    }

    #[test]
    fn overlap_logic() {
        assert!(overlaps(b"a", b"m", b"f", b"r"));
        assert!(overlaps(b"a", b"z", b"m", b"n"));
        assert!(!overlaps(b"a", b"c", b"d", b"f"));
        assert!(!overlaps(b"x", b"z", b"a", b"c"));
        assert!(overlaps(b"a", b"m", b"m", b"z"));
    }
}
