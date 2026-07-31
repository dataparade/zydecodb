#![no_main]
use libfuzzer_sys::fuzz_target;

fuzz_target!(|data: &[u8]| {
    let _ = zydecodb_document::filter::Filter::parse_bytes(data);
});
