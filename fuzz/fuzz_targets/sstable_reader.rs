#![no_main]
use libfuzzer_sys::fuzz_target;

// Opening and scanning an arbitrary byte buffer as an SSTable must never panic.
fuzz_target!(|data: &[u8]| {
    if let Ok(reader) = zydecodb_engine::sstable::SstableReader::open(data.to_vec()) {
        let _ = reader.get_latest(b"\x01probe");
        let _ = reader.scan_all();
    }
    let _ = zydecodb_engine::sstable::parse_footer(data);
});
