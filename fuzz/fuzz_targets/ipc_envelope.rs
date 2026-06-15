#![no_main]
use libfuzzer_sys::fuzz_target;

// The IPC envelope parser and payload codecs must never panic on hostile input.
fuzz_target!(|data: &[u8]| {
    let _ = zydecodb_engine::frame::RequestEnvelope::decode(data);
    let _ = zydecodb_engine::frame::PutPayload::decode(data);
    let _ = zydecodb_engine::frame::KeyPayload::decode(data);
});
