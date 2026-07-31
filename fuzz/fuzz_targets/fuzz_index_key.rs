#![no_main]
use libfuzzer_sys::fuzz_target;
use serde_json::Value;
use zydecodb_document::encoding::{encode_fields, try_decode_fields};
use zydecodb_document::keys::{index_key, try_parse_index_key};

fuzz_target!(|data: &[u8]| {
    // Encode path: treat input as UTF-8-ish scalars for field values.
    let values: Vec<Value> = data
        .chunks(8)
        .take(4)
        .map(|c| {
            if c.is_empty() {
                Value::Null
            } else if c[0] & 1 == 0 {
                Value::Bool(c[0] & 2 != 0)
            } else if c[0] & 4 == 0 {
                Value::Number((c[0] as i64).into())
            } else {
                Value::String(String::from_utf8_lossy(c).into_owned())
            }
        })
        .collect();
    let encoded = encode_fields(&values);
    let _ = try_decode_fields(&encoded);

    let prefix = b"\x01";
    let key = index_key(prefix, 1, 2, &encoded, data.get(..16).unwrap_or(b"doc"));
    let _ = try_parse_index_key(prefix.len(), &key);

    // Hostile raw key bytes must never panic the parse helper.
    let _ = try_parse_index_key(1, data);
    let _ = try_decode_fields(data);
});
