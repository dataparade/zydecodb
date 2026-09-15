use crate::error::{DocError, DocResult};
use serde_json::Value;
use std::cmp::Ordering;
pub const TYPE_NULL: u8 = 0;
pub const TYPE_BOOL_FALSE: u8 = 1;
pub const TYPE_BOOL_TRUE: u8 = 2;
pub const TYPE_I64: u8 = 3;
pub const TYPE_F64: u8 = 4;
pub const TYPE_STRING: u8 = 5;
pub const TYPE_ARRAY: u8 = 6;
pub const TYPE_OBJECT: u8 = 7;

/// Compiles a `serde_json::Value` into a ZDoc binary byte vector.
pub struct ZDocBuilder;

impl ZDocBuilder {
    pub fn from_value(val: &Value) -> Vec<u8> {
        let mut out = Vec::new();
        Self::write_value(val, &mut out);
        out
    }

    fn write_value(val: &Value, out: &mut Vec<u8>) {
        let start = out.len();
        match val {
            Value::Null => {
                out.push(TYPE_NULL);
            }
            Value::Bool(b) => {
                out.push(if *b { TYPE_BOOL_TRUE } else { TYPE_BOOL_FALSE });
            }
            Value::Number(n) => {
                if let Some(i) = n.as_i64() {
                    out.push(TYPE_I64);
                    out.extend_from_slice(&i.to_le_bytes());
                } else if let Some(f) = n.as_f64() {
                    out.push(TYPE_F64);
                    out.extend_from_slice(&f.to_le_bytes());
                } else {
                    // Fallback for arbitrarily large/precision numbers, store as f64 for now
                    out.push(TYPE_F64);
                    out.extend_from_slice(&n.as_f64().unwrap_or(0.0).to_le_bytes());
                }
            }
            Value::String(s) => {
                out.push(TYPE_STRING);
                let bytes = s.as_bytes();
                out.extend_from_slice(&(bytes.len() as u32).to_le_bytes());
                out.extend_from_slice(bytes);
            }
            Value::Array(arr) => {
                out.push(TYPE_ARRAY);
                out.extend_from_slice(&0u32.to_le_bytes()); // placeholder for total len
                out.extend_from_slice(&(arr.len() as u32).to_le_bytes());

                let offsets_start = out.len();
                out.resize(out.len() + arr.len() * 4, 0); // placeholders for value offsets

                let mut offsets = Vec::with_capacity(arr.len());
                for item in arr {
                    offsets.push((out.len() - start) as u32);
                    Self::write_value(item, out);
                }

                // Backfill offsets
                for (i, offset) in offsets.into_iter().enumerate() {
                    let pos = offsets_start + i * 4;
                    out[pos..pos + 4].copy_from_slice(&offset.to_le_bytes());
                }

                // Backfill total length
                let total_len = (out.len() - start) as u32;
                out[start + 1..start + 5].copy_from_slice(&total_len.to_le_bytes());
            }
            Value::Object(obj) => {
                out.push(TYPE_OBJECT);
                out.extend_from_slice(&0u32.to_le_bytes()); // placeholder for total len
                out.extend_from_slice(&(obj.len() as u32).to_le_bytes());

                // Sort keys for O(log N) lookup
                let mut sorted_keys: Vec<(&String, &Value)> = obj.iter().collect();
                sorted_keys.sort_by(|a, b| a.0.cmp(b.0));

                let keys_offsets_start = out.len();
                out.resize(out.len() + obj.len() * 4, 0); // placeholder for key offsets
                let vals_offsets_start = out.len();
                out.resize(out.len() + obj.len() * 4, 0); // placeholder for value offsets

                let mut key_offsets = Vec::with_capacity(obj.len());
                let mut val_offsets = Vec::with_capacity(obj.len());

                for (k, v) in sorted_keys {
                    // Write key (raw utf8 bytes with length prefix)
                    key_offsets.push((out.len() - start) as u32);
                    let k_bytes = k.as_bytes();
                    out.extend_from_slice(&(k_bytes.len() as u32).to_le_bytes());
                    out.extend_from_slice(k_bytes);

                    // Write value
                    val_offsets.push((out.len() - start) as u32);
                    Self::write_value(v, out);
                }

                // Backfill offsets
                for (i, offset) in key_offsets.into_iter().enumerate() {
                    let pos = keys_offsets_start + i * 4;
                    out[pos..pos + 4].copy_from_slice(&offset.to_le_bytes());
                }
                for (i, offset) in val_offsets.into_iter().enumerate() {
                    let pos = vals_offsets_start + i * 4;
                    out[pos..pos + 4].copy_from_slice(&offset.to_le_bytes());
                }

                // Backfill total length
                let total_len = (out.len() - start) as u32;
                out[start + 1..start + 5].copy_from_slice(&total_len.to_le_bytes());
            }
        }
    }
}

/// Read a little-endian `u32` at `pos`, or `None` if it does not fit.
fn read_u32(data: &[u8], pos: usize) -> Option<u32> {
    let bytes: [u8; 4] = data.get(pos..pos.checked_add(4)?)?.try_into().ok()?;
    Some(u32::from_le_bytes(bytes))
}

/// Read 8 little-endian bytes at `pos`, or `None` if they do not fit.
fn read_8(data: &[u8], pos: usize) -> Option<[u8; 8]> {
    data.get(pos..pos.checked_add(8)?)?.try_into().ok()
}

/// A child value must start strictly inside the parent buffer; an offset at
/// or past the end is a corrupt pointer, not an empty value.
fn child_view(data: &[u8], offset: usize) -> Option<ValueView<'_>> {
    if offset >= data.len() {
        return None;
    }
    Some(ValueView::new(&data[offset..]))
}

fn corrupt(what: &str) -> DocError {
    DocError::Corrupt(format!("malformed ZDoc: {what}"))
}

/// Zero-copy view over one ZDoc value. Every accessor is total over arbitrary
/// bytes: a truncated or inconsistent buffer yields `None` / `Err(Corrupt)`,
/// never a panic. Stored bodies can be reached by paths that bypass the
/// builder (corruption, legacy bytes), and the server aborts on panic.
#[derive(Debug, Clone, Copy)]
pub struct ValueView<'a> {
    pub data: &'a [u8],
}

impl<'a> ValueView<'a> {
    pub fn new(data: &'a [u8]) -> Self {
        Self { data }
    }

    pub fn type_byte(&self) -> u8 {
        self.data.first().copied().unwrap_or(TYPE_NULL)
    }

    /// Encoded length this value claims. For containers and strings the
    /// claim comes from the header and may exceed `data.len()` on a corrupt
    /// buffer; callers slicing by it must bounds-check.
    pub fn len(&self) -> usize {
        match self.type_byte() {
            TYPE_NULL | TYPE_BOOL_FALSE | TYPE_BOOL_TRUE => 1,
            TYPE_I64 | TYPE_F64 => 9,
            TYPE_STRING => match read_u32(self.data, 1) {
                Some(slen) => 5usize.saturating_add(slen as usize),
                None => self.data.len(),
            },
            TYPE_ARRAY | TYPE_OBJECT => match read_u32(self.data, 1) {
                Some(total) => total as usize,
                None => self.data.len(),
            },
            _ => 1,
        }
    }

    pub fn is_empty(&self) -> bool {
        self.data.is_empty()
    }

    pub fn is_null(&self) -> bool {
        self.type_byte() == TYPE_NULL
    }
    pub fn as_bool(&self) -> Option<bool> {
        match self.type_byte() {
            TYPE_BOOL_FALSE => Some(false),
            TYPE_BOOL_TRUE => Some(true),
            _ => None,
        }
    }
    pub fn as_i64(&self) -> Option<i64> {
        if self.type_byte() == TYPE_I64 {
            read_8(self.data, 1).map(i64::from_le_bytes)
        } else {
            None
        }
    }
    pub fn as_f64(&self) -> Option<f64> {
        if self.type_byte() == TYPE_F64 {
            read_8(self.data, 1).map(f64::from_le_bytes)
        } else {
            self.as_i64().map(|i| i as f64)
        }
    }
    pub fn as_str(&self) -> Option<&'a str> {
        if self.type_byte() != TYPE_STRING {
            return None;
        }
        let len = read_u32(self.data, 1)? as usize;
        let bytes = self.data.get(5..5usize.checked_add(len)?)?;
        std::str::from_utf8(bytes).ok()
    }

    pub fn as_object(&self) -> Option<ObjectView<'a>> {
        if self.type_byte() == TYPE_OBJECT {
            Some(ObjectView { data: self.data })
        } else {
            None
        }
    }

    pub fn as_array(&self) -> Option<ArrayView<'a>> {
        if self.type_byte() == TYPE_ARRAY {
            Some(ArrayView { data: self.data })
        } else {
            None
        }
    }

    pub fn get_path(&self, path: &str) -> Option<ValueView<'a>> {
        let mut current = *self;
        for part in path.split('.') {
            if let Some(obj) = current.as_object() {
                if let Some(child) = obj.get(part) {
                    current = child;
                } else {
                    return None;
                }
            } else {
                return None;
            }
        }
        Some(current)
    }

    /// Maximum nesting depth when materializing a stored ZDoc document back
    /// into a `serde_json::Value`. Legitimate document writes pass through
    /// serde_json's own 128-level parse cap (plus a small wrapper margin), so
    /// 256 never rejects real data — it exists to stop stack exhaustion from
    /// bytes that reached the store without a parse cap (raw-KV overlap,
    /// legacy data, corruption).
    pub fn to_value(&self) -> DocResult<Value> {
        self.to_value_at_depth(0)
    }

    fn to_value_at_depth(&self, depth: usize) -> DocResult<Value> {
        if depth >= MAX_ZDOC_DEPTH {
            return Err(DocError::Protocol(format!(
                "document nesting exceeds {MAX_ZDOC_DEPTH} levels"
            )));
        }
        Ok(match self.type_byte() {
            TYPE_NULL => Value::Null,
            TYPE_BOOL_FALSE => Value::Bool(false),
            TYPE_BOOL_TRUE => Value::Bool(true),
            TYPE_I64 => Value::Number(serde_json::Number::from(
                self.as_i64().ok_or_else(|| corrupt("truncated i64"))?,
            )),
            TYPE_F64 => {
                let f = self.as_f64().ok_or_else(|| corrupt("truncated f64"))?;
                match serde_json::Number::from_f64(f) {
                    Some(n) => Value::Number(n),
                    None => Value::Null,
                }
            }
            TYPE_STRING => Value::String(
                self.as_str()
                    .ok_or_else(|| corrupt("truncated or non-UTF-8 string"))?
                    .to_string(),
            ),
            TYPE_ARRAY => {
                let arr = self.as_array().ok_or_else(|| corrupt("array header"))?;
                let n = arr
                    .checked_len()
                    .ok_or_else(|| corrupt("array offset table exceeds buffer"))?;
                let mut v = Vec::with_capacity(n);
                for i in 0..n {
                    let item = arr
                        .get(i)
                        .ok_or_else(|| corrupt("array element offset out of range"))?;
                    v.push(item.to_value_at_depth(depth + 1)?);
                }
                Value::Array(v)
            }
            TYPE_OBJECT => {
                let obj = self.as_object().ok_or_else(|| corrupt("object header"))?;
                let n = obj
                    .checked_len()
                    .ok_or_else(|| corrupt("object offset table exceeds buffer"))?;
                let mut map = serde_json::Map::new();
                for i in 0..n {
                    let (k, v) = obj
                        .get_at(i)
                        .ok_or_else(|| corrupt("object entry offset out of range"))?;
                    map.insert(k.to_string(), v.to_value_at_depth(depth + 1)?);
                }
                Value::Object(map)
            }
            other => return Err(corrupt(&format!("unknown type tag 0x{other:02x}"))),
        })
    }
}

/// Depth bound for [`ValueView::to_value`]; also the ceiling the update path
/// enforces on grafted documents so a stored body can always be read back.
pub const MAX_ZDOC_DEPTH: usize = 256;

/// Layout: `[tag][total_len u32][count u32][key_off u32 x count][val_off u32 x count][entries]`.
pub struct ObjectView<'a> {
    data: &'a [u8],
}

impl<'a> ObjectView<'a> {
    /// Entry count from the header, or `None` when the header or the two
    /// offset tables it implies do not fit inside the buffer. A count that
    /// passes this check is bounded by `data.len() / 8`, so it is safe to
    /// size allocations by.
    pub fn checked_len(&self) -> Option<usize> {
        let count = read_u32(self.data, 5)? as usize;
        let table_end = 9usize.checked_add(count.checked_mul(8)?)?;
        (table_end <= self.data.len()).then_some(count)
    }

    /// Entry count; `0` for a corrupt header. Callers that must tell "empty"
    /// from "corrupt" use [`ObjectView::checked_len`].
    pub fn len(&self) -> usize {
        self.checked_len().unwrap_or(0)
    }

    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    fn key_offset(&self, index: usize) -> Option<usize> {
        read_u32(self.data, 9usize.checked_add(index.checked_mul(4)?)?).map(|o| o as usize)
    }

    fn val_offset(&self, index: usize, count: usize) -> Option<usize> {
        let pos = 9usize
            .checked_add(count.checked_mul(4)?)?
            .checked_add(index.checked_mul(4)?)?;
        read_u32(self.data, pos).map(|o| o as usize)
    }

    fn key_str(&self, off: usize) -> Option<&'a str> {
        let len = read_u32(self.data, off)? as usize;
        let start = off.checked_add(4)?;
        let bytes = self.data.get(start..start.checked_add(len)?)?;
        std::str::from_utf8(bytes).ok()
    }

    /// Entry `index` as `(key, value)`. `None` past the end or when any
    /// offset points outside the buffer.
    pub fn get_at(&self, index: usize) -> Option<(&'a str, ValueView<'a>)> {
        let count = self.checked_len()?;
        if index >= count {
            return None;
        }
        let k = self.key_str(self.key_offset(index)?)?;
        let v = child_view(self.data, self.val_offset(index, count)?)?;
        Some((k, v))
    }

    /// Binary search by key (the builder writes keys sorted). A corrupt
    /// entry encountered during the search ends it with `None`.
    pub fn get(&self, key: &str) -> Option<ValueView<'a>> {
        let count = self.checked_len()?;
        if count == 0 {
            return None;
        }

        let mut low = 0usize;
        let mut high = count - 1;

        loop {
            let mid = low + (high - low) / 2;
            let mid_key = self.key_str(self.key_offset(mid)?)?;

            match mid_key.cmp(key) {
                Ordering::Equal => {
                    return child_view(self.data, self.val_offset(mid, count)?);
                }
                Ordering::Less => low = mid + 1,
                Ordering::Greater => {
                    if mid == 0 {
                        return None;
                    }
                    high = mid - 1;
                }
            }
            if low > high {
                return None;
            }
        }
    }
}

/// Layout: `[tag][total_len u32][count u32][val_off u32 x count][elements]`.
pub struct ArrayView<'a> {
    data: &'a [u8],
}

impl<'a> ArrayView<'a> {
    /// Element count from the header, or `None` when the offset table it
    /// implies does not fit inside the buffer (bounded by `data.len() / 4`).
    pub fn checked_len(&self) -> Option<usize> {
        let count = read_u32(self.data, 5)? as usize;
        let table_end = 9usize.checked_add(count.checked_mul(4)?)?;
        (table_end <= self.data.len()).then_some(count)
    }

    /// Element count; `0` for a corrupt header.
    pub fn len(&self) -> usize {
        self.checked_len().unwrap_or(0)
    }

    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    /// Element `index`; `None` past the end or when its offset points
    /// outside the buffer.
    pub fn get(&self, index: usize) -> Option<ValueView<'a>> {
        if index >= self.checked_len()? {
            return None;
        }
        let off = read_u32(self.data, 9usize.checked_add(index.checked_mul(4)?)?)? as usize;
        child_view(self.data, off)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Build a nested Value `n` levels deep programmatically — bypassing
    /// serde_json's 128-level parse cap, the same way a document can reach
    /// the store uncapped (raw-KV overlap, legacy bytes).
    fn deep_value(n: usize) -> Value {
        let mut v = Value::from(1);
        for _ in 0..n {
            let mut m = serde_json::Map::new();
            m.insert("d".to_string(), v);
            v = Value::Object(m);
        }
        v
    }

    #[test]
    fn to_value_errors_beyond_depth_cap_instead_of_recursing() {
        let bytes = ZDocBuilder::from_value(&deep_value(MAX_ZDOC_DEPTH + 50));
        let err = ValueView::new(&bytes).to_value().unwrap_err();
        assert!(matches!(err, DocError::Protocol(_)), "got {err:?}");

        // Comfortably below the cap (and above serde's 128 parse cap, so
        // legitimate documents are unaffected): still materializes.
        let ok = ZDocBuilder::from_value(&deep_value(200));
        assert!(ValueView::new(&ok).to_value().is_ok());
    }

    fn is_corrupt(r: DocResult<Value>) -> bool {
        matches!(r, Err(DocError::Corrupt(_)))
    }

    #[test]
    fn truncated_scalars_are_corrupt_not_panics() {
        // A lone i64 tag with no payload: previously as_i64().unwrap() aborted.
        assert!(is_corrupt(ValueView::new(&[TYPE_I64]).to_value()));
        assert!(is_corrupt(ValueView::new(&[TYPE_I64, 1, 2, 3]).to_value()));
        assert!(is_corrupt(ValueView::new(&[TYPE_F64, 0, 0]).to_value()));
        // String header claims 100 bytes; only 3 follow.
        let mut s = vec![TYPE_STRING];
        s.extend_from_slice(&100u32.to_le_bytes());
        s.extend_from_slice(b"abc");
        assert!(is_corrupt(ValueView::new(&s).to_value()));
        assert_eq!(ValueView::new(&s).as_str(), None);
        // String header itself truncated.
        assert!(is_corrupt(ValueView::new(&[TYPE_STRING, 5]).to_value()));
        // Invalid UTF-8 with a correct length.
        let mut bad = vec![TYPE_STRING];
        bad.extend_from_slice(&2u32.to_le_bytes());
        bad.extend_from_slice(&[0xff, 0xfe]);
        assert!(is_corrupt(ValueView::new(&bad).to_value()));
        // Unknown tag.
        assert!(is_corrupt(ValueView::new(&[0x7f]).to_value()));
    }

    #[test]
    fn container_header_overclaim_is_corrupt() {
        // Object claiming u32::MAX entries: the offset tables cannot fit, so
        // len() is 0, checked_len() is None, and to_value is Corrupt (never an
        // allocation of 4 billion slots).
        let mut obj = vec![TYPE_OBJECT];
        obj.extend_from_slice(&9u32.to_le_bytes());
        obj.extend_from_slice(&u32::MAX.to_le_bytes());
        let view = ValueView::new(&obj);
        let o = view.as_object().unwrap();
        assert_eq!(o.checked_len(), None);
        assert_eq!(o.len(), 0);
        assert!(o.get("x").is_none());
        assert_eq!(o.get_at(0).map(|(k, _)| k), None);
        assert!(is_corrupt(view.to_value()));

        let mut arr = vec![TYPE_ARRAY];
        arr.extend_from_slice(&9u32.to_le_bytes());
        arr.extend_from_slice(&u32::MAX.to_le_bytes());
        let view = ValueView::new(&arr);
        assert_eq!(view.as_array().unwrap().checked_len(), None);
        assert!(view.as_array().unwrap().get(0).is_none());
        assert!(is_corrupt(view.to_value()));

        // Header shorter than 9 bytes.
        assert!(is_corrupt(ValueView::new(&[TYPE_OBJECT, 1, 2]).to_value()));
        assert!(is_corrupt(
            ValueView::new(&[TYPE_ARRAY, 1, 2, 3, 4, 5, 6]).to_value()
        ));
    }

    #[test]
    fn container_offsets_out_of_range_are_corrupt() {
        // One-entry object whose key and value offsets point past the end.
        let mut obj = vec![TYPE_OBJECT];
        obj.extend_from_slice(&17u32.to_le_bytes());
        obj.extend_from_slice(&1u32.to_le_bytes());
        obj.extend_from_slice(&500u32.to_le_bytes()); // key offset
        obj.extend_from_slice(&600u32.to_le_bytes()); // value offset
        let view = ValueView::new(&obj);
        assert_eq!(view.as_object().unwrap().checked_len(), Some(1));
        assert!(view.as_object().unwrap().get_at(0).is_none());
        assert!(view.as_object().unwrap().get("k").is_none());
        assert!(is_corrupt(view.to_value()));

        // Valid key, value offset exactly at data.len() (empty tail): corrupt.
        let key = b"k";
        let mut obj = vec![TYPE_OBJECT];
        obj.extend_from_slice(&0u32.to_le_bytes());
        obj.extend_from_slice(&1u32.to_le_bytes());
        let key_off = 17u32;
        let val_off = key_off + 4 + key.len() as u32;
        obj.extend_from_slice(&key_off.to_le_bytes());
        obj.extend_from_slice(&val_off.to_le_bytes());
        obj.extend_from_slice(&(key.len() as u32).to_le_bytes());
        obj.extend_from_slice(key);
        assert_eq!(obj.len() as u32, val_off);
        let view = ValueView::new(&obj);
        assert!(view.as_object().unwrap().get("k").is_none());
        assert!(is_corrupt(view.to_value()));

        // Array element offset out of range.
        let mut arr = vec![TYPE_ARRAY];
        arr.extend_from_slice(&13u32.to_le_bytes());
        arr.extend_from_slice(&1u32.to_le_bytes());
        arr.extend_from_slice(&999u32.to_le_bytes());
        let view = ValueView::new(&arr);
        assert!(view.as_array().unwrap().get(0).is_none());
        assert!(is_corrupt(view.to_value()));
    }

    #[test]
    fn every_prefix_and_byte_flip_of_a_real_doc_is_total() {
        let doc = serde_json::json!({
            "a": 1, "b": 2.5, "c": "str", "d": [1, "x", null, {"n": true}],
            "e": {"z": [], "y": {}}, "f": false
        });
        let bytes = ZDocBuilder::from_value(&doc);
        assert_eq!(ValueView::new(&bytes).to_value().unwrap(), doc);

        // Every truncation.
        for n in 0..bytes.len() {
            let v = ValueView::new(&bytes[..n]);
            let _ = v.to_value();
            let _ = v.get_path("d");
            let _ = v.get_path("e.y");
            if let Some(o) = v.as_object() {
                let _ = o.get("c");
                for i in 0..o.len() {
                    let _ = o.get_at(i);
                }
            }
        }
        // Every single-byte flip (offset tables and lengths included).
        for i in 0..bytes.len() {
            for flip in [0xff, 0x80, 0x01] {
                let mut m = bytes.clone();
                m[i] ^= flip;
                let v = ValueView::new(&m);
                let _ = v.to_value();
                let _ = v.get_path("d");
                let _ = v.get_path("e.y.q");
                if let Some(o) = v.as_object() {
                    let _ = o.get("f");
                    for j in 0..o.len() {
                        let _ = o.get_at(j);
                    }
                }
                if let Some(a) = v.get_path("d").and_then(|d| d.as_array()) {
                    for j in 0..a.len() {
                        let _ = a.get(j);
                    }
                }
            }
        }
    }

    #[test]
    fn valid_lookups_still_work() {
        let doc =
            serde_json::json!({"alpha": 1, "beta": "b", "gamma": [1, 2], "delta": {"x": null}});
        let bytes = ZDocBuilder::from_value(&doc);
        let v = ValueView::new(&bytes);
        let o = v.as_object().unwrap();
        assert_eq!(o.checked_len(), Some(4));
        assert_eq!(o.get("alpha").unwrap().as_i64(), Some(1));
        assert_eq!(o.get("beta").unwrap().as_str(), Some("b"));
        assert_eq!(o.get("gamma").unwrap().as_array().unwrap().len(), 2);
        assert!(o.get("delta").unwrap().get_path("x").unwrap().is_null());
        assert!(o.get("aaaa").is_none());
        assert!(o.get("zzzz").is_none());
        assert!(o.get("").is_none());
        let (k, _) = o.get_at(0).unwrap();
        assert_eq!(k, "alpha");
        assert!(o.get_at(4).is_none());
        let empty = ZDocBuilder::from_value(&serde_json::json!({}));
        assert!(ValueView::new(&empty)
            .as_object()
            .unwrap()
            .get("a")
            .is_none());
    }
}
