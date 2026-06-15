//! Order-preserving encoding of JSON scalars for secondary-index keys.
//!
//! The contract: for any two scalars `a` and `b`, `encode(a) <= encode(b)`
//! (lexicographic byte comparison) iff `a <= b` in logical order. This is what
//! lets a plain byte-range scan over the LSM return rows in field order.
//!
//! Encodings are **prefix-free** (no encoding is a prefix of another), so
//! concatenating per-field encodings for a composite index preserves order, and
//! appending a doc-id suffix to an index key never disturbs field ordering.
//!
//! Cross-type order is fixed by the leading tag byte: null < bool < number <
//! string. This is an arbitrary-but-stable total order for mixed-type indexes.

use serde_json::Value;

const TAG_NULL: u8 = 0x00;
const TAG_BOOL: u8 = 0x01;
const TAG_NUM: u8 = 0x02;
const TAG_STR: u8 = 0x03;

/// Encode one JSON scalar into `out`. Non-scalars (objects, arrays) are not
/// indexable and sort as `null`.
pub fn encode_value(v: &Value, out: &mut Vec<u8>) {
    match v {
        Value::Null => out.push(TAG_NULL),
        Value::Bool(b) => {
            out.push(TAG_BOOL);
            out.push(if *b { 1 } else { 0 });
        }
        Value::Number(n) => {
            out.push(TAG_NUM);
            out.extend_from_slice(&encode_f64_total_order(n.as_f64().unwrap_or(0.0)));
        }
        Value::String(s) => {
            out.push(TAG_STR);
            encode_str(s, out);
        }
        Value::Array(_) | Value::Object(_) => out.push(TAG_NULL),
    }
}

/// Encode an ordered list of field values into one composite key fragment.
pub fn encode_fields(values: &[Value]) -> Vec<u8> {
    let mut out = Vec::new();
    for v in values {
        encode_value(v, &mut out);
    }
    out
}

/// Total order over JSON values matching index order (null < bool < number <
/// string; arrays/objects sort as null). Used for in-memory `sort`.
pub fn cmp_values(a: &Value, b: &Value) -> std::cmp::Ordering {
    let mut ea = Vec::new();
    let mut eb = Vec::new();
    encode_value(a, &mut ea);
    encode_value(b, &mut eb);
    ea.cmp(&eb)
}

/// Total-order transform for IEEE-754 doubles: flip the sign bit for positives
/// and flip all bits for negatives, so big-endian unsigned comparison of the
/// result matches numeric order. (JSON has no NaN, so NaN ordering is moot.)
fn encode_f64_total_order(f: f64) -> [u8; 8] {
    let bits = f.to_bits();
    let x = if bits & (1u64 << 63) != 0 {
        !bits
    } else {
        bits | (1u64 << 63)
    };
    x.to_be_bytes()
}

/// Encode a string: escape `0x00 -> 0x00 0xFF`, terminate with `0x00 0x00`.
/// The terminator is strictly less than any escaped or literal continuation
/// byte at the same position, so a shorter string sorts before a longer one
/// sharing its prefix, and embedded NULs never break ordering or framing.
fn encode_str(s: &str, out: &mut Vec<u8>) {
    for &b in s.as_bytes() {
        if b == 0x00 {
            out.push(0x00);
            out.push(0xFF);
        } else {
            out.push(b);
        }
    }
    out.push(0x00);
    out.push(0x00);
}

/// Extract the (scalar) value at a dotted path (e.g. `"address.city"`) from a
/// JSON document. Missing fields and non-object intermediates yield `Null`.
pub fn extract_path(doc: &Value, path: &str) -> Value {
    let mut cur = doc;
    for seg in path.split('.') {
        match cur {
            Value::Object(map) => match map.get(seg) {
                Some(v) => cur = v,
                None => return Value::Null,
            },
            _ => return Value::Null,
        }
    }
    cur.clone()
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;
    use std::cmp::Ordering;

    fn enc(v: &Value) -> Vec<u8> {
        let mut out = Vec::new();
        encode_value(v, &mut out);
        out
    }

    /// A reference total order matching our intended semantics.
    fn reference_cmp(a: &Value, b: &Value) -> Ordering {
        fn rank(v: &Value) -> u8 {
            match v {
                Value::Null | Value::Array(_) | Value::Object(_) => 0,
                Value::Bool(_) => 1,
                Value::Number(_) => 2,
                Value::String(_) => 3,
            }
        }
        let (ra, rb) = (rank(a), rank(b));
        if ra != rb {
            return ra.cmp(&rb);
        }
        match (a, b) {
            (Value::Bool(x), Value::Bool(y)) => x.cmp(y),
            (Value::Number(x), Value::Number(y)) => x
                .as_f64()
                .unwrap()
                .partial_cmp(&y.as_f64().unwrap())
                .unwrap(),
            (Value::String(x), Value::String(y)) => x.cmp(y),
            _ => Ordering::Equal,
        }
    }

    #[test]
    fn encoding_preserves_order_across_mixed_types() {
        let mut values = vec![
            json!(null),
            json!(false),
            json!(true),
            json!(-1e300),
            json!(-42.5),
            json!(-1),
            json!(0),
            json!(1),
            json!(42),
            json!(42.5),
            json!(1e300),
            json!(""),
            json!("a"),
            json!("ab"),
            json!("b"),
            json!("z"),
            json!("\u{0000}embedded"),
        ];
        // Shuffle deterministically by sorting on encoded bytes, then assert the
        // encoded order matches the reference comparator for every pair.
        values.sort_by_key(enc);
        for w in values.windows(2) {
            let by_bytes = enc(&w[0]).cmp(&enc(&w[1]));
            let by_ref = reference_cmp(&w[0], &w[1]);
            assert_ne!(
                by_bytes,
                Ordering::Greater,
                "byte order must be non-decreasing after sort"
            );
            // Equal-encoding only allowed when reference says equal.
            if by_bytes == Ordering::Equal {
                assert_eq!(by_ref, Ordering::Equal, "{:?} vs {:?}", w[0], w[1]);
            } else {
                assert_eq!(by_bytes, by_ref, "{:?} vs {:?}", w[0], w[1]);
            }
        }
    }

    #[test]
    fn no_encoding_is_a_prefix_of_another() {
        let values = [
            json!("a"),
            json!("ab"),
            json!("abc"),
            json!(1),
            json!(2),
            json!(true),
            json!(null),
        ];
        for (i, a) in values.iter().enumerate() {
            for (j, b) in values.iter().enumerate() {
                if i == j {
                    continue;
                }
                let (ea, eb) = (enc(a), enc(b));
                assert!(!eb.starts_with(&ea), "{:?} is a prefix of {:?}", a, b);
            }
        }
    }

    #[test]
    fn extract_dotted_path() {
        let doc = json!({"a": {"b": {"c": 7}}, "name": "x"});
        assert_eq!(extract_path(&doc, "a.b.c"), json!(7));
        assert_eq!(extract_path(&doc, "name"), json!("x"));
        assert_eq!(extract_path(&doc, "a.b.missing"), Value::Null);
        assert_eq!(extract_path(&doc, "a.b"), json!({"c": 7}));
    }
}
