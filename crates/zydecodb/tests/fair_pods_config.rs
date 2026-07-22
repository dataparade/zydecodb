//! Pods operator path: `[fair]` in TOML enables δ-fair on the same EngineConfig
//! path `serve` and offline admin use (`Config::to_engine_config`).

use tempfile::TempDir;
use zydecodb::config::Config;
use zydecodb_engine::engine::Engine;
use zydecodb_engine::keys::KS_USER;

fn tenant_key(tenant: u8, i: u64) -> Vec<u8> {
    let mut k = Vec::with_capacity(1 + 16 + 8);
    k.push(KS_USER);
    k.extend_from_slice(&[tenant; 16]);
    k.extend_from_slice(&i.to_be_bytes());
    k
}

#[test]
fn pods_toml_fair_section_enables_memtable_isolation() {
    let tmp = TempDir::new().unwrap();
    let data = tmp.path().join("data");
    let wal = tmp.path().join("wal");
    let config_path = tmp.path().join("zydeco.toml");

    let toml = format!(
        r#"
listen = "127.0.0.1:0"
data_dir = {data:?}
wal_dir = {wal:?}
block_cache_mb = 8
[security]
require_auth = "false"
legacy_single_tenant = false
[fair]
enabled = true
tenant_count = 2
delta_steady_ms = 50
delta_buffer_ms = 0
memtable_total_mb = 1
"#
    );
    std::fs::write(&config_path, toml).unwrap();

    let cfg = Config::from_file(&config_path).unwrap();
    assert!(cfg.fair.enabled);
    assert!(!cfg.security.legacy_single_tenant);

    let mut eng_cfg = cfg.to_engine_config();
    // Match tiny budget so open() does not inflate pools past TOML.
    eng_cfg.memtable_flush_threshold = 1024 * 1024;
    eng_cfg.fair.memtable_total_bytes = 1024 * 1024;
    eng_cfg.fair.flush_bandwidth_bytes_per_sec = 1;
    eng_cfg.fair.delta_buffer = std::time::Duration::from_millis(0);

    let mut engine = Engine::open(eng_cfg).unwrap();
    assert!(engine.fair_share().config().enabled);

    let chunk = 32_768usize;
    let mut i = 0u64;
    loop {
        match engine.put(tenant_key(2, i), vec![0u8; chunk], 0) {
            Ok(_) => i += 1,
            Err(_) => break,
        }
        if i > 80 {
            break;
        }
    }
    assert!(i > 0, "noisy tenant should admit some writes");
    let err = engine
        .put(tenant_key(2, i + 1), vec![0u8; chunk], 0)
        .unwrap_err();
    assert!(
        format!("{err}").contains("EngineBusy") || format!("{err}").contains("fair memtable"),
        "expected fair busy via pods config path, got {err}"
    );
    engine
        .put(tenant_key(1, 0), vec![0u8; 1024], 0)
        .expect("victim must still admit under pods fair config");
}
