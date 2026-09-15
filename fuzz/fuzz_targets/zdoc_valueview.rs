#![no_main]

//! Fuzzes the ZDoc zero-copy decode surface — the P2 depth-guard path
//! (`ValueView::to_value` → `to_value_at_depth`, MAX_ZDOC_DEPTH 256) and the
//! bounds-checked container accessors. Arbitrary bytes must never panic or
//! exhaust the stack: deep nesting must return Err, garbage must return
//! None/Err from accessors. The server builds with `panic = "abort"`, so any
//! panic here is a remote crash of the process.

use libfuzzer_sys::fuzz_target;
use zydecodb_document::binary::ValueView;

fuzz_target!(|data: &[u8]| {
    let view = ValueView::new(data);
    // Full recursive decode (depth-guarded).
    let _ = view.to_value();
    // Path navigation walks the same recursive structure.
    let _ = view.get_path("a.b.c");
    let _ = view.get_path("deep.nested.field.path");
    let _ = view.get_path("");
    // Scalar/container accessors on arbitrary bytes.
    let _ = view.len();
    let _ = view.as_bool();
    let _ = view.as_i64();
    let _ = view.as_f64();
    let _ = view.as_str();
    if let Some(obj) = view.as_object() {
        let _ = obj.checked_len();
        // Keyed binary search over fuzzer-controlled offset tables.
        for key in ["", "a", "id", "name", "zz", "\u{ffff}"] {
            if let Some(v) = obj.get(key) {
                let _ = v.to_value();
            }
        }
        // Use the first key the buffer itself claims, so the search path
        // that hits `Ordering::Equal` is exercised too.
        if let Some((k, _)) = obj.get_at(0) {
            let _ = obj.get(k);
        }
        for i in 0..obj.len().min(8) {
            if let Some((_k, v)) = obj.get_at(i) {
                let _ = v.to_value();
                let _ = v.as_str();
            }
        }
    }
    if let Some(arr) = view.as_array() {
        let _ = arr.checked_len();
        for i in 0..arr.len().min(8) {
            if let Some(v) = arr.get(i) {
                let _ = v.to_value();
                if let Some(inner) = v.as_object() {
                    let _ = inner.get("x");
                }
            }
        }
    }
});
