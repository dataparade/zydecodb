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

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct IndexMeta {
    pub id: u32,
    pub name: String,
    /// Dotted JSON paths whose values form the (composite) index key, in order.
    pub fields: Vec<String>,
    pub unique: bool,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct CollectionMeta {
    pub id: u32,
    /// Storage prefix (`KS_USER` + optional tenant) this collection lives under.
    pub prefix: Vec<u8>,
    pub name: String,
    pub indexes: Vec<IndexMeta>,
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
    ) -> DocResult<IndexMeta> {
        if fields.is_empty() {
            return Err(DocError::Protocol(
                "index must have at least one field".into(),
            ));
        }
        let collection_id = self.ensure_collection(prefix, collection);
        let index_id = self.next_index_id;
        let meta = IndexMeta {
            id: index_id,
            name: name.to_string(),
            fields,
            unique,
        };
        let c = self
            .collections
            .iter_mut()
            .find(|c| c.id == collection_id)
            .expect("collection just ensured");
        if c.indexes.iter().any(|i| i.name == name) {
            return Err(DocError::AlreadyExists(format!("index '{name}'")));
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
            .add_index(b"\x01", "users", "by_age", vec!["age".into()], false)
            .unwrap();
        assert_eq!(m.id, 0);
        let c = cat.collection(b"\x01", "users").unwrap();
        assert_eq!(c.id, 0);
        assert_eq!(c.indexes.len(), 1);
    }

    #[test]
    fn duplicate_index_name_rejected() {
        let mut cat = Catalog::default();
        cat.add_index(b"\x01", "users", "by_age", vec!["age".into()], false)
            .unwrap();
        let err = cat
            .add_index(b"\x01", "users", "by_age", vec!["age".into()], false)
            .unwrap_err();
        assert!(matches!(err, DocError::AlreadyExists(_)));
    }

    #[test]
    fn same_name_isolated_by_prefix() {
        let mut cat = Catalog::default();
        cat.add_index(b"\x01a", "users", "by_age", vec!["age".into()], false)
            .unwrap();
        cat.add_index(b"\x01b", "users", "by_age", vec!["age".into()], false)
            .unwrap();
        assert_ne!(
            cat.collection(b"\x01a", "users").unwrap().id,
            cat.collection(b"\x01b", "users").unwrap().id
        );
    }

    #[test]
    fn round_trips_through_serde() {
        let mut cat = Catalog::default();
        cat.add_index(b"\x01", "users", "by_age", vec!["age".into()], true)
            .unwrap();
        let bytes = serde_json::to_vec(&cat).unwrap();
        let back: Catalog = serde_json::from_slice(&bytes).unwrap();
        assert_eq!(cat, back);
    }
}
