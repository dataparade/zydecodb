//! Collection/index catalog.
//!
//! The schema (collections, indexes, id allocators) is persisted as ONE JSON
//! blob under a single `KS_SYSTEM` key and cached in memory behind an
//! `RwLock`. A single blob (read-modify-write) avoids needing a system-keyspace
//! range scan, which the engine does not expose; the schema is small and only
//! changes on DDL.
//!
//! Live counters (`doc_count`, `entry_count`) change on every document write,
//! so they do NOT live in the blob: each collection has its own small binary
//! counter record under [`COUNTER_SYS_PREFIX`], written in the same WAL record
//! as the document ops that move it (see `store::write_counted_chunk`). A
//! document write therefore costs one ~30-byte system put instead of a rewrite
//! of the whole catalog. The blob is rewritten on DDL only, and every DDL
//! rewrite also writes every collection's counter record, which is what seeds
//! counters for a catalog written by an older version (those blobs still carry
//! counts, and [`Catalog::load`] falls back to them when a counter record is
//! missing).
//!
//! Collections are identified by `(prefix, name)` so each tenant's namespace is
//! isolated by its storage prefix while ids stay globally unique.

use crate::error::{DocError, DocResult};
use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;
use std::sync::{Arc, RwLock};
use zydecodb_engine::engine::Engine;
use zydecodb_engine::keys::MAX_BATCH_KEYS;

/// System key for the catalog blob: `KS_SYSTEM` (0x00) + `"doc/catalog"`.
pub const CATALOG_SYS_KEY: &[u8] = b"\x00doc/catalog";

/// System key prefix for per-collection counter records: `KS_SYSTEM` (0x00) +
/// `"doc/cnt/"` + `collection_id` (u32 BE).
pub const COUNTER_SYS_PREFIX: &[u8] = b"\x00doc/cnt/";

/// Counter record key for `collection_id`.
pub fn counter_sys_key(collection_id: u32) -> Vec<u8> {
    let mut k = Vec::with_capacity(COUNTER_SYS_PREFIX.len() + 4);
    k.extend_from_slice(COUNTER_SYS_PREFIX);
    k.extend_from_slice(&collection_id.to_be_bytes());
    k
}

/// One collection's live counters, the value of its counter record.
///
/// Fixed little-endian layout: `doc_count: u64`, `n: u32`, then `n` pairs of
/// `(index_id: u32, entry_count: u64)`.
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct CollectionCounters {
    pub doc_count: u64,
    /// `(index_id, entry_count)` per index, in catalog order.
    pub indexes: Vec<(u32, u64)>,
}

impl CollectionCounters {
    pub fn encode(&self) -> Vec<u8> {
        let mut out = Vec::with_capacity(12 + self.indexes.len() * 12);
        out.extend_from_slice(&self.doc_count.to_le_bytes());
        out.extend_from_slice(&(self.indexes.len() as u32).to_le_bytes());
        for (id, n) in &self.indexes {
            out.extend_from_slice(&id.to_le_bytes());
            out.extend_from_slice(&n.to_le_bytes());
        }
        out
    }

    pub fn decode(bytes: &[u8]) -> DocResult<Self> {
        let corrupt = || DocError::Corrupt("truncated collection counter record".into());
        let u64_at = |off: usize| -> DocResult<u64> {
            bytes
                .get(off..off + 8)
                .and_then(|s| s.try_into().ok())
                .map(u64::from_le_bytes)
                .ok_or_else(corrupt)
        };
        let u32_at = |off: usize| -> DocResult<u32> {
            bytes
                .get(off..off + 4)
                .and_then(|s| s.try_into().ok())
                .map(u32::from_le_bytes)
                .ok_or_else(corrupt)
        };
        let doc_count = u64_at(0)?;
        let n = u32_at(8)? as usize;
        if bytes.len() != 12 + n * 12 {
            return Err(DocError::Corrupt(format!(
                "collection counter record length {} does not match {} index entries",
                bytes.len(),
                n
            )));
        }
        let mut indexes = Vec::with_capacity(n);
        for i in 0..n {
            let off = 12 + i * 12;
            indexes.push((u32_at(off)?, u64_at(off + 4)?));
        }
        Ok(CollectionCounters { doc_count, indexes })
    }
}

/// A counter change to commit atomically with a batch of document ops.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CounterDelta {
    /// A document was inserted (`+1`) or removed (`-1`); every index on the
    /// collection moves by the same amount (one entry per document per index).
    Doc { collection_id: u32, delta: i64 },
    /// Document count only. Used by the TTL sweep, where a doc key and its
    /// index keys are tombstoned and counted independently.
    DocOnly { collection_id: u32, delta: i64 },
    /// One index's entry count only (TTL sweep). Index ids are globally
    /// unique, so the owning collection is looked up from the catalog.
    Index { index_id: u32, delta: i64 },
}

/// Post-write counters for the collections a batch touches. Built from the
/// live catalog before the write ([`Catalog::counter_update`]) and folded back
/// in only after the batch commits ([`Catalog::apply_counter_update`]), so a
/// failed write leaves the in-memory counters untouched without cloning the
/// catalog.
#[derive(Debug, Default)]
pub struct CounterUpdate {
    touched: BTreeMap<u32, CollectionCounters>,
}

impl CounterUpdate {
    /// Number of counter records the update writes (one per touched collection).
    pub fn len(&self) -> usize {
        self.touched.len()
    }

    pub fn is_empty(&self) -> bool {
        self.touched.is_empty()
    }

    /// System puts to include in the batch, one per touched collection.
    pub fn sys_puts(&self) -> Vec<(Vec<u8>, Vec<u8>)> {
        self.touched
            .iter()
            .map(|(id, c)| (counter_sys_key(*id), c.encode()))
            .collect()
    }
}

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
    /// critical section as document writes and persisted in the collection's
    /// counter record, never in the catalog blob (`default` still accepts the
    /// count from blobs written by older versions).
    #[serde(default, skip_serializing)]
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
    /// critical section as document writes and persisted in the collection's
    /// counter record, never in the catalog blob (`default` still accepts the
    /// count from blobs written by older versions).
    #[serde(default, skip_serializing)]
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
    /// been written yet. Read-only: the schema comes from the blob and each
    /// collection's counters from its counter record. A collection without a
    /// counter record (blob written by an older version, before its first
    /// counted write) keeps the counts carried in the blob.
    pub fn load(engine: &Engine) -> DocResult<Self> {
        let mut cat: Catalog = match engine.sys_get(CATALOG_SYS_KEY)? {
            Some(bytes) => {
                serde_json::from_slice(&bytes).map_err(|e| DocError::Corrupt(e.to_string()))?
            }
            None => return Ok(Catalog::default()),
        };
        for c in &mut cat.collections {
            let Some(bytes) = engine.sys_get(&counter_sys_key(c.id))? else {
                continue;
            };
            let counters = CollectionCounters::decode(&bytes)?;
            c.doc_count = counters.doc_count;
            for idx in &mut c.indexes {
                if let Some((_, n)) = counters.indexes.iter().find(|(id, _)| *id == idx.id) {
                    idx.entry_count = *n;
                }
            }
        }
        Ok(cat)
    }

    /// Persist the schema blob and every collection's counter record. DDL only:
    /// document writes never call this (they update counter records through
    /// [`Catalog::counter_update`]). The blob and the first
    /// `MAX_BATCH_KEYS - 1` counters commit in one WAL record; any further
    /// counters follow in their own records (rewriting an unchanged counter is
    /// idempotent, so a crash between records loses nothing). Fsyncs before
    /// returning, matching the durability of the old single system put.
    pub fn persist(&self, engine: &mut Engine) -> DocResult<()> {
        let bytes = serde_json::to_vec(self).map_err(|e| DocError::Corrupt(e.to_string()))?;
        let mut puts: Vec<(Vec<u8>, Vec<u8>)> = Vec::with_capacity(1 + self.collections.len());
        puts.push((CATALOG_SYS_KEY.to_vec(), bytes));
        for c in &self.collections {
            puts.push((counter_sys_key(c.id), Self::counters_of(c).encode()));
        }
        for chunk in puts.chunks(MAX_BATCH_KEYS) {
            engine.write_batch_with_sys(Vec::new(), chunk.to_vec())?;
        }
        engine.sync_wal()?;
        Ok(())
    }

    fn counters_of(c: &CollectionMeta) -> CollectionCounters {
        CollectionCounters {
            doc_count: c.doc_count,
            indexes: c.indexes.iter().map(|i| (i.id, i.entry_count)).collect(),
        }
    }

    fn collection_by_id(&self, collection_id: u32) -> Option<&CollectionMeta> {
        self.collections.iter().find(|c| c.id == collection_id)
    }

    /// Owning collection of an index (ids are globally unique).
    pub fn collection_id_for_index(&self, index_id: u32) -> Option<u32> {
        self.collections
            .iter()
            .find(|c| c.indexes.iter().any(|i| i.id == index_id))
            .map(|c| c.id)
    }

    /// Compute the counter records a batch must write for `deltas`, without
    /// mutating the catalog. Zero deltas and deltas for unknown collections or
    /// indexes touch nothing. Counts saturate at zero rather than wrapping;
    /// exactness is enforced by the drift tests, not by panicking in prod.
    pub fn counter_update(&self, deltas: &[CounterDelta]) -> CounterUpdate {
        let mut upd = CounterUpdate::default();
        for d in deltas {
            let (collection_id, delta) = match *d {
                CounterDelta::Doc {
                    collection_id,
                    delta,
                }
                | CounterDelta::DocOnly {
                    collection_id,
                    delta,
                } => (collection_id, delta),
                CounterDelta::Index { index_id, delta } => {
                    match self.collection_id_for_index(index_id) {
                        Some(c) => (c, delta),
                        None => continue,
                    }
                }
            };
            if delta == 0 {
                continue;
            }
            let Some(coll) = self.collection_by_id(collection_id) else {
                continue;
            };
            let entry = upd
                .touched
                .entry(collection_id)
                .or_insert_with(|| Self::counters_of(coll));
            match *d {
                CounterDelta::Doc { .. } => {
                    entry.doc_count = entry.doc_count.saturating_add_signed(delta);
                    for (_, n) in &mut entry.indexes {
                        *n = n.saturating_add_signed(delta);
                    }
                }
                CounterDelta::DocOnly { .. } => {
                    entry.doc_count = entry.doc_count.saturating_add_signed(delta);
                }
                CounterDelta::Index { index_id, .. } => {
                    if let Some((_, n)) = entry.indexes.iter_mut().find(|(id, _)| *id == index_id) {
                        *n = n.saturating_add_signed(delta);
                    }
                }
            }
        }
        upd
    }

    /// Fold a committed [`CounterUpdate`] into the in-memory counters.
    pub fn apply_counter_update(&mut self, upd: &CounterUpdate) {
        for (collection_id, counters) in &upd.touched {
            let Some(c) = self.collections.iter_mut().find(|c| c.id == *collection_id) else {
                continue;
            };
            c.doc_count = counters.doc_count;
            for idx in &mut c.indexes {
                if let Some((_, n)) = counters.indexes.iter().find(|(id, _)| *id == idx.id) {
                    idx.entry_count = *n;
                }
            }
        }
    }

    /// Remove every collection (and its indexes) stored under `prefix`, returning
    /// the ids removed. Used for tenant offboarding: the caller deletes the
    /// underlying document/index keys and the removed collections' counter
    /// records separately and then persists the catalog.
    pub fn remove_collections_with_prefix(&mut self, prefix: &[u8]) -> Vec<u32> {
        let removed: Vec<u32> = self
            .collections
            .iter()
            .filter(|c| c.prefix == prefix)
            .map(|c| c.id)
            .collect();
        self.collections.retain(|c| c.prefix != prefix);
        removed
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

    /// Set an index's entry count outright (backfill). DDL only: the caller
    /// persists the catalog (blob + counter records) afterwards.
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
