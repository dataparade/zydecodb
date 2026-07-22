#![no_main]

use libfuzzer_sys::fuzz_target;
use zydecodb_engine::frame::RequestEnvelope;

fuzz_target!(|data: &[u8]| {
    if data.len() < 6 {
        return;
    }

    // Try to parse the header
    let mut header = [0u8; 6];
    header.copy_from_slice(&data[0..6]);

    if let Ok((_cmd, len)) = RequestEnvelope::parse_header(&header) {
        // If the header is valid, and we have enough data for the payload, try to decode it
        if data.len() >= 6 + len {
            let _ = RequestEnvelope::decode(&data[0..6 + len]);
        }
    }
});
