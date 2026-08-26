//! Collection/index catalog.
//!
//! The whole catalog is persisted as ONE JSON blob under a single `KS_SYSTEM`
//! key and cached in memory behind an `RwLock`. A single blob (read-modify-
//! write) avoids needing a system-keyspace range scan, which the engine does
//! not expose; the catalog is small (metadata only).
//!
//! Collections are identified by `(prefix, name)` so each tenant's namespace is
//! isolated by its storage prefix while ids stay globally unique.

use crate::error::{DocError, DocResult};
use serde::{Deserialize, Serialize};
use std::sync::{Arc, RwLock};
use zydecodb_engine::engine::Engine;

/// System key for the catalog blob: `KS_SYSTEM` (0x00) + `"doc/catalog"`.
pub const CATALOG_SYS_KEY: &[u8] = b"\x00doc/catalog";

fn directions_all_ascending(dirs: &[bool]) -> bool {
    dirs.iter().all(|&d| d)
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct IndexMeta {
    pub id: u32,
    pub name: String,
    /// Dotted JSON paths whose values form the (composite) index key, in order.
    pub fields: Vec<String>,
    /// Per-field ascending flags (same length as `fields`). Empty/`[]` or missing
    /// in old catalog JSON means all ascending.
    #[serde(default, skip_serializing_if = "directions_all_ascending")]
    pub directions: Vec<bool>,
    pub unique: bool,
    /// When set, this is a TTL index: body `expires_at` is derived as
    /// `field_unix_millis + expire_after_seconds * 1000`. At most one per collection.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub expire_after_seconds: Option<u64>,
    /// Exact count of live index entries. Maintained in the same engine-lock
    /// critical section as document writes; `0` in old catalog blobs.
    #[serde(default)]
    pub entry_count: u64,
}

impl IndexMeta {
    /// Normalized per-field ascending flags (length == `fields.len()`).
    pub fn ascending(&self) -> Vec<bool> {
        if self.directions.is_empty() {
            vec![true; self.fields.len()]
        } else if self.directions.len() == self.fields.len() {
            self.directions.clone()
        } else {
            let mut d = self.directions.clone();
            d.resize(self.fields.len(), true);
            d
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct CollectionMeta {
    pub id: u32,
    /// Storage prefix (`KS_USER` + optional tenant) this collection lives under.
    pub prefix: Vec<u8>,
    pub name: String,
    pub indexes: Vec<IndexMeta>,
    /// Exact count of live documents. Maintained in the same engine-lock
    /// critical section as document writes; `0` in old catalog blobs.
    #[serde(default)]
    pub doc_count: u64,
}

impl CollectionMeta {
    /// The collection's TTL index, if any.
    pub fn ttl_index(&self) -> Option<&IndexMeta> {
        self.indexes
            .iter()
            .find(|i| i.expire_after_seconds.is_some())
    }
}

#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct Catalog {
    next_collection_id: u32,
    next_index_id: u32,
    collections: Vec<CollectionMeta>,
}

/// In-memory catalog shared across connection threads. Mostly-read; a write
/// lock is taken only for DDL.
pub type SharedCatalog = Arc<RwLock<Catalog>>;

impl Catalog {
    /// Load the catalog from the engine, or return an empty catalog if none has
    /// been written yet.
    pub fn load(engine: &Engine) -> DocResult<Self> {
        match engine.sys_get(CATALOG_SYS_KEY)? {
            Some(bytes) => {
                serde_json::from_slice(&bytes).map_err(|e| DocError::Corrupt(e.to_string()))
            }
            None => Ok(Catalog::default()),
        }
    }

    /// Persist the catalog as the single system blob.
    pub fn persist(&self, engine: &mut Engine) -> DocResult<()> {
        let bytes = serde_json::to_vec(self).map_err(|e| DocError::Corrupt(e.to_string()))?;
        engine.sys_put(CATALOG_SYS_KEY.to_vec(), bytes)?;
        Ok(())
    }

    /// Remove every collection (and its indexes) stored under `prefix`, returning
    /// the number removed. Used for tenant offboarding: the caller deletes the
    /// underlying document/index keys separately and then persists the catalog.
    pub fn remove_collections_with_prefix(&mut self, prefix: &[u8]) -> usize {
        let before = self.collections.len();
        self.collections.retain(|c| c.prefix != prefix);
        before - self.collections.len()
    }

    /// Look up a collection by `(prefix, name)`.
    pub fn collection(&self, prefix: &[u8], name: &str) -> Option<&CollectionMeta> {
        self.collections
            .iter()
            .find(|c| c.prefix == prefix && c.name == name)
    }

    /// All collections (used by the TTL sweep to classify expired keys).
    pub fn collections(&self) -> &[CollectionMeta] {
        &self.collections
    }

    /// Apply a document-count delta to a collection. Every index's
    /// `entry_count` moves by the same delta: each document contributes
    /// exactly one entry per index. Saturates at zero rather than wrapping —
    /// exactness is enforced by the drift tests, not by panicking in prod.
    pub fn add_doc_count(&mut self, collection_id: u32, delta: i64) {
        if let Some(c) = self.collections.iter_mut().find(|c| c.id == collection_id) {
            c.doc_count = c.doc_count.saturating_add_signed(delta);
            for idx in &mut c.indexes {
                idx.entry_count = idx.entry_count.saturating_add_signed(delta);
            }
        }
    }

    /// Apply a document-count delta WITHOUT touching index entry counts.
    /// Used by the TTL sweep, where a doc key and its index keys are
    /// tombstoned together and counted independently.
    pub fn add_doc_count_only(&mut self, collection_id: u32, delta: i64) {
        if let Some(c) = self.collections.iter_mut().find(|c| c.id == collection_id) {
            c.doc_count = c.doc_count.saturating_add_signed(delta);
        }
    }

    /// Apply an entry-count delta to one index (ids are globally unique).
    /// Used by the TTL sweep, which tombstones index keys directly.
    pub fn add_index_entry_count(&mut self, index_id: u32, delta: i64) {
        for c in &mut self.collections {
            if let Some(idx) = c.indexes.iter_mut().find(|i| i.id == index_id) {
                idx.entry_count = idx.entry_count.saturating_add_signed(delta);
                return;
            }
        }
    }

    /// Set an index's entry count outright (backfill).
    pub fn set_index_entry_count(&mut self, index_id: u32, count: u64) {
        for c in &mut self.collections {
            if let Some(idx) = c.indexes.iter_mut().find(|i| i.id == index_id) {
                idx.entry_count = count;
                return;
            }
        }
    }

    /// Create the collection if it does not exist, returning its id.
    pub fn ensure_collection(&mut self, prefix: &[u8], name: &str) -> u32 {
        if let Some(c) = self.collection(prefix, name) {
            return c.id;
        }
        let id = self.next_collection_id;
        self.next_collection_id += 1;
        self.collections.push(CollectionMeta {
            id,
            prefix: prefix.to_vec(),
            name: name.to_string(),
            indexes: Vec::new(),
            doc_count: 0,
        });
        id
    }

    /// Define a new index on a collection (creating the collection if needed).
    /// Returns the new index's metadata. Errors if an index of that name
    /// already exists on the collection.
    pub fn add_index(
        &mut self,
        prefix: &[u8],
        collection: &str,
        name: &str,
        fields: Vec<String>,
        unique: bool,
        expire_after_seconds: Option<u64>,
    ) -> DocResult<IndexMeta> {
        self.add_index_directed(
            prefix,
            collection,
            name,
            fields,
            Vec::new(),
            unique,
            expire_after_seconds,
        )
    }

    /// Like [`add_index`] with explicit per-field ascending flags. Empty
    /// `directions` means all ascending. Length must match `fields` when non-empty.
    pub fn add_index_directed(
        &mut self,
        prefix: &[u8],
        collection: &str,
        name: &str,
        fields: Vec<String>,
        directions: Vec<bool>,
        unique: bool,
        expire_after_seconds: Option<u64>,
    ) -> DocResult<IndexMeta> {
        if fields.is_empty() {
            return Err(DocError::Protocol(
                "index must have at least one field".into(),
            ));
        }
        if !directions.is_empty() && directions.len() != fields.len() {
            return Err(DocError::Protocol(
                "index directions length must match fields".into(),
            ));
        }
        if expire_after_seconds.is_some() && fields.len() != 1 {
            return Err(DocError::Protocol(
                "TTL index must have exactly one field (unix millis)".into(),
            ));
        }
        // Persist empty when all-ascending so serde omit/default round-trips cleanly.
        let directions = if directions.is_empty() || directions.iter().all(|&d| d) {
            Vec::new()
        } else {
            directions
        };
        let collection_id = self.ensure_collection(prefix, collection);
        let index_id = self.next_index_id;
        let meta = IndexMeta {
            id: index_id,
            name: name.to_string(),
            fields,
            directions,
            unique,
            expire_after_seconds,
            entry_count: 0,
        };
        let c = self
            .collections
            .iter_mut()
            .find(|c| c.id == collection_id)
            .expect("collection just ensured");
        if c.indexes.iter().any(|i| i.name == name) {
            return Err(DocError::AlreadyExists(format!("index '{name}'")));
        }
        if meta.expire_after_seconds.is_some() && c.ttl_index().is_some() {
            return Err(DocError::Protocol(
                "collection already has a TTL index".into(),
            ));
        }
        c.indexes.push(meta.clone());
        self.next_index_id += 1;
        Ok(meta)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn add_index_creates_collection_and_assigns_ids() {
        let mut cat = Catalog::default();
        let m = cat
            .add_index(b"\x01", "users", "by_age", vec!["age".into()], false, None)
            .unwrap();
        assert_eq!(m.id, 0);
        let c = cat.collection(b"\x01", "users").unwrap();
        assert_eq!(c.id, 0);
        assert_eq!(c.indexes.len(), 1);
    }

    #[test]
    fn duplicate_index_name_rejected() {
        let mut cat = Catalog::default();
        cat.add_index(b"\x01", "users", "by_age", vec!["age".into()], false, None)
            .unwrap();
        let err = cat
            .add_index(b"\x01", "users", "by_age", vec!["age".into()], false, None)
            .unwrap_err();
        assert!(matches!(err, DocError::AlreadyExists(_)));
    }

    #[test]
    fn same_name_isolated_by_prefix() {
        let mut cat = Catalog::default();
        cat.add_index(b"\x01a", "users", "by_age", vec!["age".into()], false, None)
            .unwrap();
        cat.add_index(b"\x01b", "users", "by_age", vec!["age".into()], false, None)
            .unwrap();
        assert_ne!(
            cat.collection(b"\x01a", "users").unwrap().id,
            cat.collection(b"\x01b", "users").unwrap().id
        );
    }

    #[test]
    fn round_trips_through_serde() {
        let mut cat = Catalog::default();
        cat.add_index(b"\x01", "users", "by_age", vec!["age".into()], true, None)
            .unwrap();
        cat.add_index(
            b"\x01",
            "users",
            "by_exp",
            vec!["exp".into()],
            false,
            Some(3600),
        )
        .unwrap();
        let bytes = serde_json::to_vec(&cat).unwrap();
        let back: Catalog = serde_json::from_slice(&bytes).unwrap();
        assert_eq!(cat, back);
        assert_eq!(
            back.collection(b"\x01", "users")
                .unwrap()
                .ttl_index()
                .unwrap()
                .expire_after_seconds,
            Some(3600)
        );
    }

    #[test]
    fn second_ttl_index_rejected() {
        let mut cat = Catalog::default();
        cat.add_index(
            b"\x01",
            "users",
            "ttl1",
            vec!["exp".into()],
            false,
            Some(60),
        )
        .unwrap();
        let err = cat
            .add_index(
                b"\x01",
                "users",
                "ttl2",
                vec!["other".into()],
                false,
                Some(60),
            )
            .unwrap_err();
        assert!(matches!(err, DocError::Protocol(_)));
    }
}
