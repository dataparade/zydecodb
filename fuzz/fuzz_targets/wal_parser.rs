#![no_main]
use libfuzzer_sys::fuzz_target;

// The WAL replay routine must never panic on arbitrary bytes. It must either
// parse cleanly, report a structured error, or detect a torn tail.
fuzz_target!(|data: &[u8]| {
    let _ = zydecodb_engine::wal::replay_segment_body(data);
    let _ = zydecodb_engine::wal::WalRecord::decode_one(data);
});
