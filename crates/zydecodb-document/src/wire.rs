//! Wire payload codecs for the document commands.
//!
//! These sit on top of the engine's envelope (version/command/length header in
//! [`zydecodb_engine::frame`]); only the per-command payload bodies are defined
//! here. All variable fields are length-prefixed with a `u32` big-endian length
//! so payloads are self-describing and bounded.

use crate::error::{DocError, DocResult};
use crate::query::QueryPage;

/// Query mode discriminator (first payload byte).
const MODE_BY_ID: u8 = 0x00;
const MODE_INDEX_RANGE: u8 = 0x01;

/// Bit 0 of the optional trailing flags byte on write payloads: when set, the
/// write is acknowledged without waiting for the durability fsync (`relaxed`).
const FLAG_RELAXED: u8 = 0x01;

/// Cursor reader over a payload buffer with bounds-checked primitives.
struct Reader<'a> {
    buf: &'a [u8],
    pos: usize,
}

impl<'a> Reader<'a> {
    fn new(buf: &'a [u8]) -> Self {
        Reader { buf, pos: 0 }
    }

    fn take(&mut self, n: usize) -> DocResult<&'a [u8]> {
        let end = self
            .pos
            .checked_add(n)
            .ok_or_else(|| DocError::Protocol("length overflow".into()))?;
        if end > self.buf.len() {
            return Err(DocError::Protocol("payload truncated".into()));
        }
        let s = &self.buf[self.pos..end];
        self.pos = end;
        Ok(s)
    }

    fn u8(&mut self) -> DocResult<u8> {
        Ok(self.take(1)?[0])
    }

    /// Read a trailing flag byte if present, else default to 0. Used for
    /// optional, append-only fields so older encoders stay wire-compatible.
    fn opt_u8(&mut self) -> u8 {
        if self.pos < self.buf.len() {
            let b = self.buf[self.pos];
            self.pos += 1;
            b
        } else {
            0
        }
    }

    fn u32(&mut self) -> DocResult<usize> {
        let b = self.take(4)?;
        Ok(u32::from_be_bytes([b[0], b[1], b[2], b[3]]) as usize)
    }

    /// Length-prefixed byte field.
    fn lp(&mut self) -> DocResult<&'a [u8]> {
        let n = self.u32()?;
        self.take(n)
    }

    fn lp_string(&mut self) -> DocResult<String> {
        let b = self.lp()?;
        String::from_utf8(b.to_vec()).map_err(|_| DocError::Protocol("invalid utf-8".into()))
    }
}

fn put_lp(out: &mut Vec<u8>, bytes: &[u8]) {
    out.extend_from_slice(&(bytes.len() as u32).to_be_bytes());
    out.extend_from_slice(bytes);
}

// ---- DocPut: [collection][doc_id][body] ----

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DocPutPayload {
    pub collection: String,
    pub doc_id: Vec<u8>,
    pub body: Vec<u8>,
    /// Acknowledge without waiting for the durability fsync. Optional on the
    /// wire (a missing trailing flags byte decodes as `false`).
    pub relaxed: bool,
}

impl DocPutPayload {
    pub fn encode(&self) -> Vec<u8> {
        let mut out = Vec::new();
        put_lp(&mut out, self.collection.as_bytes());
        put_lp(&mut out, &self.doc_id);
        put_lp(&mut out, &self.body);
        out.push(if self.relaxed { FLAG_RELAXED } else { 0 });
        out
    }

    pub fn decode(p: &[u8]) -> DocResult<DocPutPayload> {
        let mut r = Reader::new(p);
        let collection = r.lp_string()?;
        let doc_id = r.lp()?.to_vec();
        let body = r.lp()?.to_vec();
        let relaxed = r.opt_u8() & FLAG_RELAXED != 0;
        Ok(DocPutPayload {
            collection,
            doc_id,
            body,
            relaxed,
        })
    }
}

// ---- DocDel: [collection][doc_id] ----

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DocDelPayload {
    pub collection: String,
    pub doc_id: Vec<u8>,
}

impl DocDelPayload {
    pub fn encode(&self) -> Vec<u8> {
        let mut out = Vec::new();
        put_lp(&mut out, self.collection.as_bytes());
        put_lp(&mut out, &self.doc_id);
        out
    }

    pub fn decode(p: &[u8]) -> DocResult<DocDelPayload> {
        let mut r = Reader::new(p);
        let collection = r.lp_string()?;
        let doc_id = r.lp()?.to_vec();
        Ok(DocDelPayload { collection, doc_id })
    }
}

// ---- IndexDef: [collection][index_name][unique u8][field_count u32]{[field]} ----

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct IndexDefPayload {
    pub collection: String,
    pub index_name: String,
    pub fields: Vec<String>,
    pub unique: bool,
}

impl IndexDefPayload {
    pub fn encode(&self) -> Vec<u8> {
        let mut out = Vec::new();
        put_lp(&mut out, self.collection.as_bytes());
        put_lp(&mut out, self.index_name.as_bytes());
        out.push(if self.unique { 1 } else { 0 });
        out.extend_from_slice(&(self.fields.len() as u32).to_be_bytes());
        for f in &self.fields {
            put_lp(&mut out, f.as_bytes());
        }
        out
    }

    pub fn decode(p: &[u8]) -> DocResult<IndexDefPayload> {
        let mut r = Reader::new(p);
        let collection = r.lp_string()?;
        let index_name = r.lp_string()?;
        let unique = r.u8()? != 0;
        let count = r.u32()?;
        let mut fields = Vec::with_capacity(count.min(256));
        for _ in 0..count {
            fields.push(r.lp_string()?);
        }
        Ok(IndexDefPayload {
            collection,
            index_name,
            fields,
            unique,
        })
    }
}

// ---- Query: [mode] then mode-specific body ----

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum QueryPayload {
    ById {
        collection: String,
        doc_id: Vec<u8>,
    },
    IndexRange {
        collection: String,
        index_name: String,
        /// JSON-array lower bound (empty = unbounded).
        lo: Vec<u8>,
        /// JSON-array upper bound (empty = unbounded).
        hi: Vec<u8>,
        /// Opaque cursor from a prior page (empty = first page).
        cursor: Vec<u8>,
        limit: u32,
    },
}

impl QueryPayload {
    pub fn encode(&self) -> Vec<u8> {
        let mut out = Vec::new();
        match self {
            QueryPayload::ById { collection, doc_id } => {
                out.push(MODE_BY_ID);
                put_lp(&mut out, collection.as_bytes());
                put_lp(&mut out, doc_id);
            }
            QueryPayload::IndexRange {
                collection,
                index_name,
                lo,
                hi,
                cursor,
                limit,
            } => {
                out.push(MODE_INDEX_RANGE);
                put_lp(&mut out, collection.as_bytes());
                put_lp(&mut out, index_name.as_bytes());
                out.extend_from_slice(&limit.to_be_bytes());
                put_lp(&mut out, lo);
                put_lp(&mut out, hi);
                put_lp(&mut out, cursor);
            }
        }
        out
    }

    pub fn decode(p: &[u8]) -> DocResult<QueryPayload> {
        let mut r = Reader::new(p);
        match r.u8()? {
            MODE_BY_ID => {
                let collection = r.lp_string()?;
                let doc_id = r.lp()?.to_vec();
                Ok(QueryPayload::ById { collection, doc_id })
            }
            MODE_INDEX_RANGE => {
                let collection = r.lp_string()?;
                let index_name = r.lp_string()?;
                let limit = {
                    let b = r.take(4)?;
                    u32::from_be_bytes([b[0], b[1], b[2], b[3]])
                };
                let lo = r.lp()?.to_vec();
                let hi = r.lp()?.to_vec();
                let cursor = r.lp()?.to_vec();
                Ok(QueryPayload::IndexRange {
                    collection,
                    index_name,
                    lo,
                    hi,
                    cursor,
                    limit,
                })
            }
            m => Err(DocError::Protocol(format!("unknown query mode 0x{m:02x}"))),
        }
    }
}

// ---- Find: filter + sort + projection + paging ----

/// Projection request: include or exclude a set of dotted field paths.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum WireProjection {
    None,
    Include(Vec<String>),
    Exclude(Vec<String>),
}

const PROJ_NONE: u8 = 0x00;
const PROJ_INCLUDE: u8 = 0x01;
const PROJ_EXCLUDE: u8 = 0x02;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct FindPayload {
    pub collection: String,
    /// Raw JSON filter document (empty = match all).
    pub filter: Vec<u8>,
    /// Sort keys: `(dotted_path, ascending)`.
    pub sort: Vec<(String, bool)>,
    pub projection: WireProjection,
    pub skip: u32,
    pub limit: u32,
    /// Opaque cursor from a prior page (empty = first page).
    pub cursor: Vec<u8>,
}

impl FindPayload {
    pub fn encode(&self) -> Vec<u8> {
        let mut out = Vec::new();
        put_lp(&mut out, self.collection.as_bytes());
        put_lp(&mut out, &self.filter);
        out.extend_from_slice(&(self.sort.len() as u32).to_be_bytes());
        for (field, asc) in &self.sort {
            put_lp(&mut out, field.as_bytes());
            out.push(if *asc { 1 } else { 0 });
        }
        match &self.projection {
            WireProjection::None => out.push(PROJ_NONE),
            WireProjection::Include(fs) => {
                out.push(PROJ_INCLUDE);
                put_field_list(&mut out, fs);
            }
            WireProjection::Exclude(fs) => {
                out.push(PROJ_EXCLUDE);
                put_field_list(&mut out, fs);
            }
        }
        out.extend_from_slice(&self.skip.to_be_bytes());
        out.extend_from_slice(&self.limit.to_be_bytes());
        put_lp(&mut out, &self.cursor);
        out
    }

    pub fn decode(p: &[u8]) -> DocResult<FindPayload> {
        let mut r = Reader::new(p);
        let collection = r.lp_string()?;
        let filter = r.lp()?.to_vec();
        let sort_count = r.u32()?;
        let mut sort = Vec::with_capacity(sort_count.min(64));
        for _ in 0..sort_count {
            let field = r.lp_string()?;
            let asc = r.u8()? != 0;
            sort.push((field, asc));
        }
        let projection = match r.u8()? {
            PROJ_NONE => WireProjection::None,
            PROJ_INCLUDE => WireProjection::Include(take_field_list(&mut r)?),
            PROJ_EXCLUDE => WireProjection::Exclude(take_field_list(&mut r)?),
            m => {
                return Err(DocError::Protocol(format!(
                    "unknown projection mode 0x{m:02x}"
                )))
            }
        };
        let skip = r.u32()? as u32;
        let limit = r.u32()? as u32;
        let cursor = r.lp()?.to_vec();
        Ok(FindPayload {
            collection,
            filter,
            sort,
            projection,
            skip,
            limit,
            cursor,
        })
    }
}

fn put_field_list(out: &mut Vec<u8>, fields: &[String]) {
    out.extend_from_slice(&(fields.len() as u32).to_be_bytes());
    for f in fields {
        put_lp(out, f.as_bytes());
    }
}

fn take_field_list(r: &mut Reader<'_>) -> DocResult<Vec<String>> {
    let n = r.u32()?;
    let mut v = Vec::with_capacity(n.min(256));
    for _ in 0..n {
        v.push(r.lp_string()?);
    }
    Ok(v)
}

// ---- Update: [collection][filter][update][multi u8] ----

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct UpdatePayload {
    pub collection: String,
    pub filter: Vec<u8>,
    pub update: Vec<u8>,
    /// false = update_one (first match); true = update_many.
    pub multi: bool,
    /// Acknowledge without waiting for the durability fsync (optional on wire).
    pub relaxed: bool,
}

impl UpdatePayload {
    pub fn encode(&self) -> Vec<u8> {
        let mut out = Vec::new();
        put_lp(&mut out, self.collection.as_bytes());
        put_lp(&mut out, &self.filter);
        put_lp(&mut out, &self.update);
        out.push(if self.multi { 1 } else { 0 });
        out.push(if self.relaxed { FLAG_RELAXED } else { 0 });
        out
    }

    pub fn decode(p: &[u8]) -> DocResult<UpdatePayload> {
        let mut r = Reader::new(p);
        let collection = r.lp_string()?;
        let filter = r.lp()?.to_vec();
        let update = r.lp()?.to_vec();
        let multi = r.u8()? != 0;
        let relaxed = r.opt_u8() & FLAG_RELAXED != 0;
        Ok(UpdatePayload {
            collection,
            filter,
            update,
            multi,
            relaxed,
        })
    }
}

// ---- Delete (filter-based): [collection][filter][multi u8] ----

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DeletePayload {
    pub collection: String,
    pub filter: Vec<u8>,
    /// false = delete_one (first match); true = delete_many.
    pub multi: bool,
    /// Acknowledge without waiting for the durability fsync (optional on wire).
    pub relaxed: bool,
}

impl DeletePayload {
    pub fn encode(&self) -> Vec<u8> {
        let mut out = Vec::new();
        put_lp(&mut out, self.collection.as_bytes());
        put_lp(&mut out, &self.filter);
        out.push(if self.multi { 1 } else { 0 });
        out.push(if self.relaxed { FLAG_RELAXED } else { 0 });
        out
    }

    pub fn decode(p: &[u8]) -> DocResult<DeletePayload> {
        let mut r = Reader::new(p);
        let collection = r.lp_string()?;
        let filter = r.lp()?.to_vec();
        let multi = r.u8()? != 0;
        let relaxed = r.opt_u8() & FLAG_RELAXED != 0;
        Ok(DeletePayload {
            collection,
            filter,
            multi,
            relaxed,
        })
    }
}

// ---- Count / Distinct: [mode u8][collection][filter][field] ----

const COUNT_MODE_COUNT: u8 = 0x00;
const COUNT_MODE_DISTINCT: u8 = 0x01;

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum CountPayload {
    Count {
        collection: String,
        filter: Vec<u8>,
    },
    Distinct {
        collection: String,
        filter: Vec<u8>,
        field: String,
    },
}

impl CountPayload {
    pub fn encode(&self) -> Vec<u8> {
        let mut out = Vec::new();
        match self {
            CountPayload::Count { collection, filter } => {
                out.push(COUNT_MODE_COUNT);
                put_lp(&mut out, collection.as_bytes());
                put_lp(&mut out, filter);
            }
            CountPayload::Distinct {
                collection,
                filter,
                field,
            } => {
                out.push(COUNT_MODE_DISTINCT);
                put_lp(&mut out, collection.as_bytes());
                put_lp(&mut out, filter);
                put_lp(&mut out, field.as_bytes());
            }
        }
        out
    }

    pub fn decode(p: &[u8]) -> DocResult<CountPayload> {
        let mut r = Reader::new(p);
        match r.u8()? {
            COUNT_MODE_COUNT => Ok(CountPayload::Count {
                collection: r.lp_string()?,
                filter: r.lp()?.to_vec(),
            }),
            COUNT_MODE_DISTINCT => Ok(CountPayload::Distinct {
                collection: r.lp_string()?,
                filter: r.lp()?.to_vec(),
                field: r.lp_string()?,
            }),
            m => Err(DocError::Protocol(format!("unknown count mode 0x{m:02x}"))),
        }
    }
}

/// Encode an index-range response page:
/// `[row_count u32]{[doc_id][body]}* [cursor]` (cursor empty = end of results).
pub fn encode_query_page(page: &QueryPage) -> Vec<u8> {
    let mut out = Vec::new();
    out.extend_from_slice(&(page.rows.len() as u32).to_be_bytes());
    for row in &page.rows {
        put_lp(&mut out, &row.doc_id);
        put_lp(&mut out, row.body.as_deref().unwrap_or(&[]));
    }
    put_lp(&mut out, page.next_cursor.as_deref().unwrap_or(&[]));
    out
}

/// One decoded row from a query response page.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DecodedRow {
    pub doc_id: Vec<u8>,
    pub body: Vec<u8>,
}

/// Decode an index-range response page produced by [`encode_query_page`].
/// Returns the rows and an optional next-page cursor (empty cursor = end).
pub fn decode_query_page(p: &[u8]) -> DocResult<(Vec<DecodedRow>, Option<Vec<u8>>)> {
    let mut r = Reader::new(p);
    let count = r.u32()?;
    let mut rows = Vec::with_capacity(count.min(4096));
    for _ in 0..count {
        let doc_id = r.lp()?.to_vec();
        let body = r.lp()?.to_vec();
        rows.push(DecodedRow { doc_id, body });
    }
    let cursor = r.lp()?.to_vec();
    let next = if cursor.is_empty() {
        None
    } else {
        Some(cursor)
    };
    Ok((rows, next))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn doc_put_round_trips() {
        let p = DocPutPayload {
            collection: "users".into(),
            doc_id: b"u1".to_vec(),
            body: br#"{"age":30}"#.to_vec(),
            relaxed: false,
        };
        assert_eq!(DocPutPayload::decode(&p.encode()).unwrap(), p);

        // A payload without the trailing flags byte (older encoder) decodes as
        // relaxed = false.
        let mut legacy = Vec::new();
        put_lp(&mut legacy, b"users");
        put_lp(&mut legacy, b"u1");
        put_lp(&mut legacy, br#"{"age":30}"#);
        assert_eq!(DocPutPayload::decode(&legacy).unwrap(), p);

        let relaxed = DocPutPayload {
            relaxed: true,
            ..p.clone()
        };
        assert_eq!(DocPutPayload::decode(&relaxed.encode()).unwrap(), relaxed);
    }

    #[test]
    fn index_def_round_trips() {
        let p = IndexDefPayload {
            collection: "users".into(),
            index_name: "by_age".into(),
            fields: vec!["age".into(), "name".into()],
            unique: true,
        };
        assert_eq!(IndexDefPayload::decode(&p.encode()).unwrap(), p);
    }

    #[test]
    fn query_round_trips() {
        let p = QueryPayload::IndexRange {
            collection: "users".into(),
            index_name: "by_age".into(),
            lo: b"[18]".to_vec(),
            hi: b"[65]".to_vec(),
            cursor: vec![],
            limit: 50,
        };
        assert_eq!(QueryPayload::decode(&p.encode()).unwrap(), p);

        let by_id = QueryPayload::ById {
            collection: "users".into(),
            doc_id: b"u1".to_vec(),
        };
        assert_eq!(QueryPayload::decode(&by_id.encode()).unwrap(), by_id);
    }

    #[test]
    fn truncated_payload_errors() {
        assert!(DocPutPayload::decode(&[0, 0, 0, 5, b'a']).is_err());
    }

    #[test]
    fn find_round_trips() {
        let p = FindPayload {
            collection: "users".into(),
            filter: br#"{"age":{"$gte":18}}"#.to_vec(),
            sort: vec![("age".into(), true), ("name".into(), false)],
            projection: WireProjection::Include(vec!["name".into(), "age".into()]),
            skip: 5,
            limit: 50,
            cursor: vec![1, 2, 3],
        };
        assert_eq!(FindPayload::decode(&p.encode()).unwrap(), p);

        let p2 = FindPayload {
            collection: "c".into(),
            filter: vec![],
            sort: vec![],
            projection: WireProjection::None,
            skip: 0,
            limit: 1,
            cursor: vec![],
        };
        assert_eq!(FindPayload::decode(&p2.encode()).unwrap(), p2);
    }

    #[test]
    fn update_delete_round_trip() {
        let u = UpdatePayload {
            collection: "users".into(),
            filter: br#"{"_id":"u1"}"#.to_vec(),
            update: br#"{"$set":{"name":"x"}}"#.to_vec(),
            multi: true,
            relaxed: true,
        };
        assert_eq!(UpdatePayload::decode(&u.encode()).unwrap(), u);

        let d = DeletePayload {
            collection: "users".into(),
            filter: br#"{"age":{"$lt":0}}"#.to_vec(),
            multi: false,
            relaxed: false,
        };
        assert_eq!(DeletePayload::decode(&d.encode()).unwrap(), d);
    }

    #[test]
    fn count_distinct_round_trip() {
        let c = CountPayload::Count {
            collection: "users".into(),
            filter: br#"{"active":true}"#.to_vec(),
        };
        assert_eq!(CountPayload::decode(&c.encode()).unwrap(), c);

        let d = CountPayload::Distinct {
            collection: "users".into(),
            filter: vec![],
            field: "city".into(),
        };
        assert_eq!(CountPayload::decode(&d.encode()).unwrap(), d);
    }

    #[test]
    fn query_page_round_trips() {
        let page = QueryPage {
            rows: vec![
                crate::query::QueryRow {
                    doc_id: b"u1".to_vec(),
                    body: Some(b"{}".to_vec()),
                },
                crate::query::QueryRow {
                    doc_id: b"u2".to_vec(),
                    body: None,
                },
            ],
            next_cursor: Some(b"cursor-bytes".to_vec()),
        };
        let (rows, cursor) = decode_query_page(&encode_query_page(&page)).unwrap();
        assert_eq!(rows.len(), 2);
        assert_eq!(rows[0].doc_id, b"u1");
        assert_eq!(rows[0].body, b"{}");
        assert_eq!(rows[1].doc_id, b"u2");
        assert_eq!(rows[1].body, b"");
        assert_eq!(cursor, Some(b"cursor-bytes".to_vec()));
    }
}
