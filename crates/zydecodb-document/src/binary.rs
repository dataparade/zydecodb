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
                    out[pos..pos+4].copy_from_slice(&offset.to_le_bytes());
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
                    out[pos..pos+4].copy_from_slice(&offset.to_le_bytes());
                }
                for (i, offset) in val_offsets.into_iter().enumerate() {
                    let pos = vals_offsets_start + i * 4;
                    out[pos..pos+4].copy_from_slice(&offset.to_le_bytes());
                }
                
                // Backfill total length
                let total_len = (out.len() - start) as u32;
                out[start + 1..start + 5].copy_from_slice(&total_len.to_le_bytes());
            }
        }
    }
}

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
    
    pub fn len(&self) -> usize {
        match self.type_byte() {
            TYPE_NULL | TYPE_BOOL_FALSE | TYPE_BOOL_TRUE => 1,
            TYPE_I64 | TYPE_F64 => 9,
            TYPE_STRING => {
                if self.data.len() < 5 { return self.data.len(); }
                let slen = u32::from_le_bytes(self.data[1..5].try_into().unwrap()) as usize;
                5 + slen
            }
            TYPE_ARRAY | TYPE_OBJECT => {
                if self.data.len() < 5 { return self.data.len(); }
                u32::from_le_bytes(self.data[1..5].try_into().unwrap()) as usize
            }
            _ => 1,
        }
    }

    pub fn is_null(&self) -> bool { self.type_byte() == TYPE_NULL }
    pub fn as_bool(&self) -> Option<bool> {
        match self.type_byte() {
            TYPE_BOOL_FALSE => Some(false),
            TYPE_BOOL_TRUE => Some(true),
            _ => None,
        }
    }
    pub fn as_i64(&self) -> Option<i64> {
        if self.type_byte() == TYPE_I64 && self.data.len() >= 9 {
            Some(i64::from_le_bytes(self.data[1..9].try_into().unwrap()))
        } else {
            None
        }
    }
    pub fn as_f64(&self) -> Option<f64> {
        if self.type_byte() == TYPE_F64 && self.data.len() >= 9 {
            Some(f64::from_le_bytes(self.data[1..9].try_into().unwrap()))
        } else if let Some(i) = self.as_i64() {
            Some(i as f64)
        } else {
            None
        }
    }
    pub fn as_str(&self) -> Option<&'a str> {
        if self.type_byte() == TYPE_STRING && self.data.len() >= 5 {
            let len = u32::from_le_bytes(self.data[1..5].try_into().unwrap()) as usize;
            if self.data.len() >= 5 + len {
                return std::str::from_utf8(&self.data[5..5+len]).ok();
            }
        }
        None
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

    pub fn to_value(&self) -> Value {
        match self.type_byte() {
            TYPE_NULL => Value::Null,
            TYPE_BOOL_FALSE => Value::Bool(false),
            TYPE_BOOL_TRUE => Value::Bool(true),
            TYPE_I64 => Value::Number(serde_json::Number::from(self.as_i64().unwrap())),
            TYPE_F64 => {
                if let Some(n) = serde_json::Number::from_f64(self.as_f64().unwrap()) {
                    Value::Number(n)
                } else {
                    Value::Null
                }
            }
            TYPE_STRING => Value::String(self.as_str().unwrap().to_string()),
            TYPE_ARRAY => {
                let arr = self.as_array().unwrap();
                let mut v = Vec::with_capacity(arr.len());
                for i in 0..arr.len() {
                    v.push(arr.get(i).unwrap().to_value());
                }
                Value::Array(v)
            }
            TYPE_OBJECT => {
                let obj = self.as_object().unwrap();
                let mut map = serde_json::Map::new();
                for i in 0..obj.len() {
                    let (k, v) = obj.get_at(i).unwrap();
                    map.insert(k.to_string(), v.to_value());
                }
                Value::Object(map)
            }
            _ => Value::Null,
        }
    }
}

pub struct ObjectView<'a> {
    data: &'a [u8],
}

impl<'a> ObjectView<'a> {
    pub fn len(&self) -> usize {
        if self.data.len() < 9 { return 0; }
        u32::from_le_bytes(self.data[5..9].try_into().unwrap()) as usize
    }

    fn key_offset(&self, index: usize) -> u32 {
        let pos = 9 + index * 4;
        u32::from_le_bytes(self.data[pos..pos+4].try_into().unwrap())
    }

    fn val_offset(&self, index: usize) -> u32 {
        let count = self.len();
        let pos = 9 + count * 4 + index * 4;
        u32::from_le_bytes(self.data[pos..pos+4].try_into().unwrap())
    }

    fn key_str(&self, offset: u32) -> &'a str {
        let off = offset as usize;
        let len = u32::from_le_bytes(self.data[off..off+4].try_into().unwrap()) as usize;
        std::str::from_utf8(&self.data[off+4..off+4+len]).unwrap_or("")
    }

    pub fn get_at(&self, index: usize) -> Option<(&'a str, ValueView<'a>)> {
        if index >= self.len() { return None; }
        let k_off = self.key_offset(index);
        let v_off = self.val_offset(index);
        let k = self.key_str(k_off);
        let v = ValueView::new(&self.data[v_off as usize..]);
        Some((k, v))
    }

    pub fn get(&self, key: &str) -> Option<ValueView<'a>> {
        let count = self.len();
        if count == 0 { return None; }
        
        let mut low = 0;
        let mut high = count as isize - 1;
        
        while low <= high {
            let mid = low + (high - low) / 2;
            let mid_idx = mid as usize;
            let k_off = self.key_offset(mid_idx);
            let mid_key = self.key_str(k_off);
            
            match mid_key.cmp(key) {
                Ordering::Equal => {
                    let v_off = self.val_offset(mid_idx);
                    return Some(ValueView::new(&self.data[v_off as usize..]));
                }
                Ordering::Less => low = mid + 1,
                Ordering::Greater => high = mid - 1,
            }
        }
        None
    }
}

pub struct ArrayView<'a> {
    data: &'a [u8],
}

impl<'a> ArrayView<'a> {
    pub fn len(&self) -> usize {
        if self.data.len() < 9 { return 0; }
        u32::from_le_bytes(self.data[5..9].try_into().unwrap()) as usize
    }

    pub fn get(&self, index: usize) -> Option<ValueView<'a>> {
        if index >= self.len() { return None; }
        let pos = 9 + index * 4;
        let v_off = u32::from_le_bytes(self.data[pos..pos+4].try_into().unwrap());
        Some(ValueView::new(&self.data[v_off as usize..]))
    }
}
