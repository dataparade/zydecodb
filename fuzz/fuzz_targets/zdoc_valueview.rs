#![no_main]

//! Fuzzes the ZDoc zero-copy decode surface — the P2 depth-guard path
//! (`ValueView::to_value` → `to_value_at_depth`, MAX_ZDOC_DEPTH 256).
//! Arbitrary bytes must never panic or exhaust the stack: deep nesting must
//! return Err, garbage must return None/Err from accessors.

use libfuzzer_sys::fuzz_target;
use zydecodb_document::binary::ValueView;

fuzz_target!(|data: &[u8]| {
    let view = ValueView::new(data);
    // Full recursive decode (depth-guarded).
    let _ = view.to_value();
    // Path navigation walks the same recursive structure.
    let _ = view.get_path("a.b.c");
    let _ = view.get_path("deep.nested.field.path");
    // Scalar/container accessors on arbitrary bytes.
    let _ = view.as_bool();
    let _ = view.as_i64();
    let _ = view.as_f64();
    let _ = view.as_str();
    if let Some(obj) = view.as_object() {
        for i in 0..obj.len().min(8) {
            if let Some((_k, v)) = obj.get_at(i) {
                let _ = v.to_value();
            }
        }
    }
    if let Some(arr) = view.as_array() {
        for i in 0..arr.len().min(8) {
            if let Some(v) = arr.get(i) {
                let _ = v.to_value();
            }
        }
    }
});
