#![no_main]

use libfuzzer_sys::fuzz_target;
use zydecodb_engine::manifest;

fuzz_target!(|data: &[u8]| {
    if data.is_empty() {
        return;
    }
    let dir = tempfile::tempdir().expect("tempdir");
    let path = dir.path().join("MANIFEST");
    if std::fs::write(&path, data).is_err() {
        return;
    }
    let _ = manifest::load(&path);
});
