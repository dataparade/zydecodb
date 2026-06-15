//! The value payload stored against an `InternalKey`.

/// A stored entry. `value` is `None` for tombstones. `expires_at` is a Unix-ms
/// timestamp, `None` (encoded as 0 on the wire) means no expiry.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Entry {
    pub value: Option<Vec<u8>>,
    pub expires_at: Option<u64>,
}

impl Entry {
    pub fn value(value: Vec<u8>, expires_at: Option<u64>) -> Self {
        Entry {
            value: Some(value),
            expires_at,
        }
    }

    pub fn tombstone() -> Self {
        Entry {
            value: None,
            expires_at: None,
        }
    }

    pub fn is_tombstone(&self) -> bool {
        self.value.is_none()
    }

    /// Approximate heap footprint of the value payload, for memtable accounting.
    pub fn value_len(&self) -> usize {
        self.value.as_ref().map(|v| v.len()).unwrap_or(0)
    }

    /// Whether this entry is expired relative to `now_ms`.
    pub fn is_expired(&self, now_ms: u64) -> bool {
        match self.expires_at {
            Some(ts) if ts != 0 => now_ms > ts,
            _ => false,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn tombstone_has_no_value() {
        let t = Entry::tombstone();
        assert!(t.is_tombstone());
        assert_eq!(t.value_len(), 0);
    }

    #[test]
    fn value_entry_reports_len() {
        let e = Entry::value(b"hello".to_vec(), None);
        assert!(!e.is_tombstone());
        assert_eq!(e.value_len(), 5);
    }

    #[test]
    fn expiry_logic() {
        let e = Entry::value(b"x".to_vec(), Some(100));
        assert!(!e.is_expired(50));
        assert!(!e.is_expired(100));
        assert!(e.is_expired(101));

        let no_expiry = Entry::value(b"x".to_vec(), None);
        assert!(!no_expiry.is_expired(u64::MAX));
    }
}
