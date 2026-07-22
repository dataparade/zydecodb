//! Two-tenant adversarial isolation bench (Phase 4 baseline / Phase 5 gate).
//!
//! Layers:
//! 1. Always-on mechanism tests (memtable pools, stall attribution).
//! 2. Ignored concurrent latency benches (fair off baseline / fair on interim δ).
//!
//! Manual latency run:
//! `cargo test -p zydecodb-engine --test tenant_isolation_bench -- --ignored --nocapture`

use std::sync::{Arc, Barrier, Mutex};
use std::thread;
use std::time::{Duration, Instant};
use tempfile::TempDir;
use zydecodb_engine::engine::{Engine, EngineConfig};
use zydecodb_engine::engine_handle::EngineHandle;
use zydecodb_engine::keys::KS_USER;
use zydecodb_engine::tenant_fair::FairConfig;

fn tenant_key(tenant: u8, i: u64) -> Vec<u8> {
    let mut k = Vec::with_capacity(1 + 16 + 8);
    k.push(KS_USER);
    k.extend_from_slice(&[tenant; 16]);
    k.extend_from_slice(&i.to_be_bytes());
    k
}

fn p99_ms(samples: &mut [u128]) -> f64 {
    samples.sort_unstable();
    let idx = ((samples.len() as f64) * 0.99).floor() as usize;
    samples[idx.min(samples.len() - 1)] as f64 / 1000.0
}

fn fair_cfg_enabled() -> FairConfig {
    let mut fair = FairConfig::default();
    fair.enabled = true;
    fair.tenant_count = 2;
    // Keep fair budget well below the flush threshold so the bench stresses
    // memtable pools, not the global immutable flush-queue stall.
    fair.memtable_total_bytes = 2 * 1024 * 1024;
    fair.delta_buffer = Duration::from_millis(350);
    fair.delta_cache = Duration::from_millis(250);
    fair.delta_steady = Duration::from_millis(50);
    fair.ramp_up_k = 2;
    fair.flush_bandwidth_bytes_per_sec = 64 * 1024 * 1024;
    fair
}

/// Always-on unit check: fair memtable pools admit victim while rejecting a
/// noisy tenant that exhausted the global pool (Phase 5a mechanism).
#[test]
fn fair_memtable_pool_isolates_admission() {
    let tmp = TempDir::new().unwrap();
    let mut fair_cfg = FairConfig::default();
    fair_cfg.enabled = true;
    fair_cfg.tenant_count = 2;
    fair_cfg.memtable_total_bytes = 512_000;
    fair_cfg.delta_buffer = Duration::from_millis(0);
    fair_cfg.flush_bandwidth_bytes_per_sec = 1;
    let mut engine = Engine::open(EngineConfig {
        data_dir: tmp.path().join("data"),
        wal_dir: tmp.path().join("wal"),
        memtable_flush_threshold: 512_000,
        fair: fair_cfg,
        ..Default::default()
    })
    .unwrap();

    let chunk = 32_768usize;
    let mut i = 0u64;
    loop {
        match engine.put(tenant_key(2, i), vec![0u8; chunk], 0) {
            Ok(_) => i += 1,
            Err(_) => break,
        }
        if i > 100 {
            break;
        }
    }
    assert!(i > 0, "noisy tenant should have admitted some writes");
    let err = engine
        .put(tenant_key(2, i + 1), vec![0u8; chunk], 0)
        .unwrap_err();
    let msg = format!("{err}");
    assert!(
        msg.contains("EngineBusy") || msg.contains("fair memtable"),
        "expected fair memtable busy, got {msg}"
    );
    engine.put(tenant_key(1, 0), vec![0u8; 1024], 0).unwrap();
}

/// Always-on: under L0 write-stall pressure, fair-off rejects both tenants;
/// fair-on rejects only the noisy tenant (over reserve / token debt).
#[test]
fn fair_stall_attribution_spares_well_behaved_tenant() {
    let tmp = TempDir::new().unwrap();
    let high_l0_trigger = {
        let mut c = zydecodb_engine::compaction::CompactionConfig::default();
        // Avoid compacting L0 away while we accumulate stall pressure.
        c.l0_trigger = 64;
        c
    };

    // --- fair OFF: both tenants stall once L0 threshold is hit ---
    {
        let mut engine = Engine::open(EngineConfig {
            data_dir: tmp.path().join("off-data"),
            wal_dir: tmp.path().join("off-wal"),
            memtable_flush_threshold: 8 * 1024,
            l0_write_stall_threshold: Some(2),
            fair: FairConfig::default(),
            compaction: high_l0_trigger.clone(),
            ..Default::default()
        })
        .unwrap();

        for i in 0..8u64 {
            if engine
                .put(tenant_key(2, i), vec![0u8; 4 * 1024], 0)
                .is_err()
            {
                break;
            }
            engine.force_flush().unwrap();
        }
        assert!(
            engine.put(tenant_key(1, 0), vec![0u8; 64], 0).is_err(),
            "fair-off: victim must stall under L0 pressure"
        );
        assert!(
            engine.put(tenant_key(2, 99), vec![0u8; 64], 0).is_err(),
            "fair-off: noisy tenant must also stall"
        );
    }

    // --- fair ON: well-behaved victim can create L0 pressure; noisy absorbs stall ---
    {
        let mut fair = fair_cfg_enabled();
        fair.memtable_total_bytes = 8 * 1024 * 1024;
        // High budget so victim L0 flush credits do not self-attribute during setup.
        fair.l0_token_budget = 1_000_000_000;
        let mut engine = Engine::open(EngineConfig {
            data_dir: tmp.path().join("on-data"),
            wal_dir: tmp.path().join("on-wal"),
            memtable_flush_threshold: 8 * 1024,
            l0_write_stall_threshold: Some(2),
            fair,
            compaction: high_l0_trigger,
            ..Default::default()
        })
        .unwrap();

        let noisy = [2u8; 16];
        let victim = [1u8; 16];
        // Victim builds L0 while still well-behaved (skips stall under fair-on).
        for i in 0..6u64 {
            engine
                .put(tenant_key(1, i), vec![0u8; 4 * 1024], 0)
                .expect("victim setup put");
            engine.force_flush().unwrap();
        }
        // Mark only the noisy tenant over token budget.
        engine.fair_share().charge_l0_tokens(noisy, 2_000_000_000);
        assert!(engine.fair_share().should_attribute_stall(noisy));
        assert!(!engine.fair_share().should_attribute_stall(victim));

        let noisy_err = engine.put(tenant_key(2, 999), vec![0u8; 64], 0);
        assert!(
            noisy_err.is_err(),
            "fair-on: noisy tenant must absorb L0 stall, got {noisy_err:?}"
        );
        engine
            .put(tenant_key(1, 999), vec![0u8; 64], 0)
            .expect("fair-on: well-behaved victim must proceed under L0 stall");
    }
}

/// Always-on: fair-on spares a well-behaved tenant when the immutable flush
/// queue is at the soft cap; noisy (over fair share / tokens) is rejected.
#[test]
fn fair_flush_queue_stall_spares_well_behaved_tenant() {
    let tmp = TempDir::new().unwrap();
    let mut fair = fair_cfg_enabled();
    // Large fair budget so admission is not the reject path.
    fair.memtable_total_bytes = 64 * 1024 * 1024;
    fair.l0_token_budget = 1_000_000_000;
    let mut engine = Engine::open(EngineConfig {
        data_dir: tmp.path().join("data"),
        wal_dir: tmp.path().join("wal"),
        // Tiny threshold: each noisy put freezes; without poll_flush the
        // immutable deque fills while the worker is busy.
        memtable_flush_threshold: 1024,
        max_immutable_memtables: 2,
        fair,
        ..Default::default()
    })
    .unwrap();

    let noisy = [2u8; 16];
    let victim = [1u8; 16];
    engine.fair_share().charge_l0_tokens(noisy, 2_000_000_000);
    assert!(engine.fair_share().should_attribute_stall(noisy));
    assert!(!engine.fair_share().should_attribute_stall(victim));

    let mut saw_noisy_busy = false;
    for i in 0..128u64 {
        match engine.put(tenant_key(2, i), vec![0u8; 8 * 1024], 0) {
            Ok(_) => {}
            Err(e) => {
                let msg = format!("{e}");
                if msg.contains("flush queue") {
                    saw_noisy_busy = true;
                    break;
                }
            }
        }
    }
    assert!(
        saw_noisy_busy,
        "expected noisy tenant to hit flush queue busy under fair-on"
    );
    engine
        .put(tenant_key(1, 42), vec![0u8; 64], 0)
        .expect("fair-on: well-behaved victim must proceed under soft flush-queue stall");
}

/// Returns (solo_p99_ms, under_noise_p99_ms, victim_successes).
fn run_concurrent_victim_p99(fair: FairConfig, rounds: u64) -> (f64, f64, usize) {
    let tmp = TempDir::new().unwrap();
    // Flush threshold >> fair memtable budget: isolate pool/lock effects from
    // global immutable-queue EngineBusy (still shared-fate today).
    let handle = EngineHandle::new(
        Engine::open(EngineConfig {
            data_dir: tmp.path().join("data"),
            wal_dir: tmp.path().join("wal"),
            memtable_flush_threshold: 64 * 1024 * 1024,
            max_immutable_memtables: 16,
            block_cache_bytes: 4 * 1024 * 1024,
            fair,
            ..Default::default()
        })
        .unwrap(),
    );

    let mut solo = Vec::with_capacity(rounds as usize);
    for i in 0..rounds {
        let t0 = Instant::now();
        handle
            .write()
            .put(tenant_key(1, i), vec![0u8; 4096], 0)
            .unwrap();
        solo.push(t0.elapsed().as_micros());
    }
    let solo_p99 = p99_ms(&mut solo);

    let barrier = Arc::new(Barrier::new(2));
    let samples = Arc::new(Mutex::new(Vec::with_capacity(rounds as usize)));
    let successes = Arc::new(Mutex::new(0usize));
    let h_n = Arc::clone(&handle);
    let b_n = Arc::clone(&barrier);
    let noisy = thread::spawn(move || {
        b_n.wait();
        for i in 0..(rounds * 20) {
            let _ = h_n.write().put(tenant_key(2, i), vec![0u8; 8192], 0);
            // Yield so the victim can acquire the write mutex.
            thread::yield_now();
        }
    });

    let h_v = Arc::clone(&handle);
    let b_v = Arc::clone(&barrier);
    let samples_v = Arc::clone(&samples);
    let succ_v = Arc::clone(&successes);
    let victim = thread::spawn(move || {
        b_v.wait();
        let mut local = Vec::with_capacity(rounds as usize);
        let mut ok = 0usize;
        for i in 0..rounds {
            let t0 = Instant::now();
            let mut succeeded = false;
            // Short retry window — do not inflate p99 with a 500ms timeout ceiling.
            while t0.elapsed() < Duration::from_millis(50) {
                match h_v
                    .write()
                    .put(tenant_key(1, 10_000 + i), vec![0u8; 4096], 0)
                {
                    Ok(_) => {
                        succeeded = true;
                        break;
                    }
                    Err(_) => thread::sleep(Duration::from_micros(100)),
                }
            }
            if succeeded {
                ok += 1;
            }
            local.push(t0.elapsed().as_micros());
        }
        *succ_v.lock().unwrap() = ok;
        *samples_v.lock().unwrap() = local;
    });

    noisy.join().unwrap();
    victim.join().unwrap();
    let mut under = samples.lock().unwrap().clone();
    let noisy_p99 = p99_ms(&mut under);
    let ok = *successes.lock().unwrap();
    (solo_p99, noisy_p99, ok)
}

/// Fair off: concurrent noisy neighbor must inflate victim p99 (shared fate).
#[test]
#[ignore = "adversarial latency; run manually / nightly"]
fn concurrent_noisy_neighbor_inflates_victim_p99_without_fair_share() {
    let (solo, noisy, ok) = run_concurrent_victim_p99(FairConfig::default(), 80);
    let delta = noisy - solo;
    eprintln!(
        "fair=OFF solo_p99={solo:.2}ms under_noise_p99={noisy:.2}ms delta={delta:.2}ms successes={ok}/80"
    );
    assert!(
        delta > 0.0,
        "expected shared-fate latency inflation, delta={delta}"
    );
}

/// Fair on: interim gate — victim keeps completing and p99 delta stays within
/// paper-like buffer δ (350 ms). Tighten to 50 ms after soak.
#[test]
#[ignore = "adversarial latency; prefer scripts/tenant-isolation-soak.sh for ship δ"]
fn concurrent_fair_share_bounds_victim_p99_delta() {
    let (solo, noisy, ok) = run_concurrent_victim_p99(fair_cfg_enabled(), 80);
    let delta = noisy - solo;
    let interim_delta_ms = 350.0;
    eprintln!(
        "fair=ON solo_p99={solo:.2}ms under_noise_p99={noisy:.2}ms delta={delta:.2}ms interim_δ={interim_delta_ms} successes={ok}/80"
    );
    assert!(
        ok >= 70,
        "fair-on victim must mostly succeed under noise (got {ok}/80)"
    );
    assert!(
        delta <= interim_delta_ms,
        "fair-on victim p99 delta {delta:.2}ms exceeds interim δ={interim_delta_ms}ms (solo={solo:.2} noisy={noisy:.2})"
    );
}

/// Short realistic soak pointer — prefer the binary for measured δ:
/// `scripts/tenant-isolation-soak.sh` (e2e Busy retries + flush poller).
#[test]
#[ignore = "simulated pods soak; run scripts/tenant-isolation-soak.sh"]
fn simulated_pods_soak_fair_on_beats_fair_off() {
    // Kept as a discoverable ignored test; the binary is the source of truth.
    eprintln!("run: ./scripts/tenant-isolation-soak.sh");
}
