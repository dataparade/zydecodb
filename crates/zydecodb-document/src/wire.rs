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

/// Bounds on client-supplied count fields in payload decoders. These counts
/// drive loop iterations and `vec![true; count]`-style allocations — without
/// a cap, one malicious u32 forces a multi-GB allocation before the first
/// bounds check can fire.
const MAX_INDEX_FIELDS: usize = 64;
const MAX_SORT_FIELDS: usize = 64;
const MAX_FIELD_LIST: usize = 256;

/// Bit 0 of the optional trailing flags byte on write payloads: when set, the
/// write is acknowledged without waiting for the durability fsync (`relaxed`).
const FLAG_RELAXED: u8 = 0x01;
/// Bit 1: filter upsert — insert one document if the update matches nothing.
const FLAG_UPSERT: u8 = 0x02;

/// Reject unused write-flag bits. Documented contract: unknown bits must be zero.
fn check_write_flags(flags: u8, allowed: u8) -> DocResult<()> {
    let unknown = flags & !allowed;
    if unknown != 0 {
        return Err(DocError::Protocol(format!(
            "unused write flag bits set: 0x{unknown:02x}"
        )));
    }
    Ok(())
}

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

    fn remaining(&self) -> usize {
        self.buf.len().saturating_sub(self.pos)
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
    /// Absolute expiry time (unix millis). `0` = never. Optional on the wire:
    /// after the flags byte, an 8-byte big-endian `expires_at` may follow.
    pub expires_at: u64,
}

impl DocPutPayload {
    pub fn encode(&self) -> Vec<u8> {
        let mut out = Vec::new();
        put_lp(&mut out, self.collection.as_bytes());
        put_lp(&mut out, &self.doc_id);
        put_lp(&mut out, &self.body);
        out.push(if self.relaxed { FLAG_RELAXED } else { 0 });
        if self.expires_at != 0 {
            out.extend_from_slice(&self.expires_at.to_be_bytes());
        }
        out
    }

    pub fn decode(p: &[u8]) -> DocResult<DocPutPayload> {
        let mut r = Reader::new(p);
        let collection = r.lp_string()?;
        let doc_id = r.lp()?.to_vec();
        let body = r.lp()?.to_vec();
        let flags = r.opt_u8();
        check_write_flags(flags, FLAG_RELAXED)?;
        let relaxed = flags & FLAG_RELAXED != 0;
        let expires_at = if r.remaining() >= 8 {
            let mut buf = [0u8; 8];
            buf.copy_from_slice(r.take(8)?);
            u64::from_be_bytes(buf)
        } else {
            0
        };
        Ok(DocPutPayload {
            collection,
            doc_id,
            body,
            relaxed,
            expires_at,
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

// ---- IndexDef: [collection][index_name][unique u8][field_count u32]{[field]}
//      [optional ttl u64][optional 0x02 + N direction bytes] ----

/// Trailer tag for per-field ascending flags (1=ASC, 0=DESC).
pub const INDEX_DIR_TAG: u8 = 0x02;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct IndexDefPayload {
    pub collection: String,
    pub index_name: String,
    pub fields: Vec<String>,
    pub unique: bool,
    /// TTL duration in seconds. `0` = not a TTL index (trailer omitted on wire
    /// unless directions are present).
    pub expire_after_seconds: u64,
    /// Per-field ascending flags. Empty means all ascending (wire omits DIR_TAG).
    pub directions: Vec<bool>,
}

impl IndexDefPayload {
    fn any_descending(&self) -> bool {
        self.directions.iter().any(|a| !*a)
    }

    pub fn encode(&self) -> Vec<u8> {
        let mut out = Vec::new();
        put_lp(&mut out, self.collection.as_bytes());
        put_lp(&mut out, self.index_name.as_bytes());
        out.push(if self.unique { 1 } else { 0 });
        out.extend_from_slice(&(self.fields.len() as u32).to_be_bytes());
        for f in &self.fields {
            put_lp(&mut out, f.as_bytes());
        }
        let write_dirs = self.any_descending() && self.directions.len() == self.fields.len();
        if self.expire_after_seconds != 0 || write_dirs {
            out.extend_from_slice(&self.expire_after_seconds.to_be_bytes());
        }
        if write_dirs {
            out.push(INDEX_DIR_TAG);
            for d in &self.directions {
                out.push(if *d { 1 } else { 0 });
            }
        }
        out
    }

    pub fn decode(p: &[u8]) -> DocResult<IndexDefPayload> {
        let mut r = Reader::new(p);
        let collection = r.lp_string()?;
        let index_name = r.lp_string()?;
        let unique = r.u8()? != 0;
        let count = r.u32()?;
        if count > MAX_INDEX_FIELDS {
            return Err(DocError::Protocol(format!(
                "index field count {count} exceeds max {MAX_INDEX_FIELDS}"
            )));
        }
        let mut fields = Vec::with_capacity(count);
        for _ in 0..count {
            fields.push(r.lp_string()?);
        }
        let expire_after_seconds = if r.remaining() >= 8 {
            let mut buf = [0u8; 8];
            buf.copy_from_slice(r.take(8)?);
            u64::from_be_bytes(buf)
        } else {
            0
        };
        let directions =
            if r.remaining() > count && r.buf.get(r.pos).copied() == Some(INDEX_DIR_TAG) {
                let _tag = r.u8()?;
                let mut dirs = Vec::with_capacity(count);
                for _ in 0..count {
                    dirs.push(r.u8()? != 0);
                }
                dirs
            } else {
                vec![true; count]
            };
        Ok(IndexDefPayload {
            collection,
            index_name,
            fields,
            unique,
            expire_after_seconds,
            directions,
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
        /// When `false`, return doc ids only (skip body point-gets). Encoded as
        /// an optional trailing u8 (`0` = false, `1`/omitted = true) so older
        /// clients keep receiving bodies.
        include_bodies: bool,
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
                include_bodies,
            } => {
                out.push(MODE_INDEX_RANGE);
                put_lp(&mut out, collection.as_bytes());
                put_lp(&mut out, index_name.as_bytes());
                out.extend_from_slice(&limit.to_be_bytes());
                put_lp(&mut out, lo);
                put_lp(&mut out, hi);
                put_lp(&mut out, cursor);
                // Append-only trailer: omit when true so legacy vectors match.
                if !*include_bodies {
                    out.push(0);
                }
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
                // Optional trailing u8: 0 = ids only; absent/nonzero = bodies.
                let include_bodies = match r.remaining() {
                    0 => true,
                    _ => r.u8()? != 0,
                };
                Ok(QueryPayload::IndexRange {
                    collection,
                    index_name,
                    lo,
                    hi,
                    cursor,
                    limit,
                    include_bodies,
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
        if sort_count > MAX_SORT_FIELDS {
            return Err(DocError::Protocol(format!(
                "sort field count {sort_count} exceeds max {MAX_SORT_FIELDS}"
            )));
        }
        let mut sort = Vec::with_capacity(sort_count);
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
    if n > MAX_FIELD_LIST {
        return Err(DocError::Protocol(format!(
            "field list count {n} exceeds max {MAX_FIELD_LIST}"
        )));
    }
    let mut v = Vec::with_capacity(n);
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
    /// Insert one document when the update matches nothing (optional on wire).
    pub upsert: bool,
}

impl UpdatePayload {
    pub fn encode(&self) -> Vec<u8> {
        let mut out = Vec::new();
        put_lp(&mut out, self.collection.as_bytes());
        put_lp(&mut out, &self.filter);
        put_lp(&mut out, &self.update);
        out.push(if self.multi { 1 } else { 0 });
        let mut flags = 0u8;
        if self.relaxed {
            flags |= FLAG_RELAXED;
        }
        if self.upsert {
            flags |= FLAG_UPSERT;
        }
        out.push(flags);
        out
    }

    pub fn decode(p: &[u8]) -> DocResult<UpdatePayload> {
        let mut r = Reader::new(p);
        let collection = r.lp_string()?;
        let filter = r.lp()?.to_vec();
        let update = r.lp()?.to_vec();
        let multi = r.u8()? != 0;
        let flags = r.opt_u8();
        check_write_flags(flags, FLAG_RELAXED | FLAG_UPSERT)?;
        Ok(UpdatePayload {
            collection,
            filter,
            update,
            multi,
            relaxed: flags & FLAG_RELAXED != 0,
            upsert: flags & FLAG_UPSERT != 0,
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
        let flags = r.opt_u8();
        check_write_flags(flags, FLAG_RELAXED)?;
        Ok(DeletePayload {
            collection,
            filter,
            multi,
            relaxed: flags & FLAG_RELAXED != 0,
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

// ---- Aggregate: [collection][pipeline_json] ----
// Response: [row_count u32 BE]{[row_json lp]}*

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AggregatePayload {
    pub collection: String,
    /// Full pipeline JSON array bytes (opaque on the wire).
    pub pipeline: Vec<u8>,
}

impl AggregatePayload {
    pub fn encode(&self) -> Vec<u8> {
        let mut out = Vec::new();
        put_lp(&mut out, self.collection.as_bytes());
        put_lp(&mut out, &self.pipeline);
        out
    }

    pub fn decode(p: &[u8]) -> DocResult<AggregatePayload> {
        let mut r = Reader::new(p);
        Ok(AggregatePayload {
            collection: r.lp_string()?,
            pipeline: r.lp()?.to_vec(),
        })
    }
}

/// Encode an Aggregate response while enforcing `max_result_bytes` as each row
/// is appended. Exceeding the budget returns [`DocError::BadFilter`] without
/// allocating an oversized buffer.
pub fn encode_aggregate_response(
    rows: &[serde_json::Value],
    max_result_bytes: usize,
) -> DocResult<Vec<u8>> {
    if 4 > max_result_bytes {
        return Err(DocError::BadFilter(format!(
            "aggregation: result exceeds {max_result_bytes} bytes"
        )));
    }
    let mut out = Vec::with_capacity(4);
    out.extend_from_slice(&(rows.len() as u32).to_be_bytes());
    for row in rows {
        let json = serde_json::to_vec(row).map_err(|e| DocError::InvalidJson(e.to_string()))?;
        let added = 4usize
            .checked_add(json.len())
            .ok_or_else(|| DocError::BadFilter("aggregation: result size overflow".into()))?;
        let next = out
            .len()
            .checked_add(added)
            .ok_or_else(|| DocError::BadFilter("aggregation: result size overflow".into()))?;
        if next > max_result_bytes {
            return Err(DocError::BadFilter(format!(
                "aggregation: result exceeds {max_result_bytes} bytes"
            )));
        }
        put_lp(&mut out, &json);
    }
    Ok(out)
}

/// Decode an Aggregate response produced by [`encode_aggregate_response`].
pub fn decode_aggregate_response(p: &[u8]) -> DocResult<Vec<Vec<u8>>> {
    let mut r = Reader::new(p);
    let count = r.u32()?;
    let mut rows = Vec::with_capacity(count.min(4096));
    for _ in 0..count {
        rows.push(r.lp()?.to_vec());
    }
    Ok(rows)
}

// ---- Watch: [collection][resume_token lp] ----

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct WatchPayload {
    pub collection: String,
    /// Opaque resume token (empty = start after current durable watermark).
    pub resume_token: Vec<u8>,
}

impl WatchPayload {
    pub fn encode(&self) -> Vec<u8> {
        let mut out = Vec::new();
        put_lp(&mut out, self.collection.as_bytes());
        put_lp(&mut out, &self.resume_token);
        out
    }

    pub fn decode(p: &[u8]) -> DocResult<WatchPayload> {
        let mut r = Reader::new(p);
        Ok(WatchPayload {
            collection: r.lp_string()?,
            resume_token: r.lp()?.to_vec(),
        })
    }
}

/// Watch stream frame kinds (first byte of Ok payloads after subscription open).
pub const WATCH_FRAME_ACK: u8 = 0x01;
pub const WATCH_FRAME_EVENT: u8 = 0x02;
pub const WATCH_FRAME_HEARTBEAT: u8 = 0x03;

/// Upsert event op byte.
pub const WATCH_OP_UPSERT: u8 = 0x01;
/// Delete event op byte.
pub const WATCH_OP_DELETE: u8 = 0x02;

/// Encode a Watch ACK: `[WATCH_FRAME_ACK][resume_token lp]`.
pub fn encode_watch_ack(resume_token: &[u8]) -> Vec<u8> {
    let mut out = vec![WATCH_FRAME_ACK];
    put_lp(&mut out, resume_token);
    out
}

/// Encode a Watch EVENT:
/// `[WATCH_FRAME_EVENT][resume_token lp][op u8][doc_id lp][body lp]`.
pub fn encode_watch_event(resume_token: &[u8], op: u8, doc_id: &[u8], body: &[u8]) -> Vec<u8> {
    let mut out = vec![WATCH_FRAME_EVENT];
    put_lp(&mut out, resume_token);
    out.push(op);
    put_lp(&mut out, doc_id);
    put_lp(&mut out, body);
    out
}

/// Encode a Watch HEARTBEAT: `[WATCH_FRAME_HEARTBEAT][resume_token lp]`.
pub fn encode_watch_heartbeat(resume_token: &[u8]) -> Vec<u8> {
    let mut out = vec![WATCH_FRAME_HEARTBEAT];
    put_lp(&mut out, resume_token);
    out
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum WatchFrame {
    Ack {
        resume_token: Vec<u8>,
    },
    Event {
        resume_token: Vec<u8>,
        op: u8,
        doc_id: Vec<u8>,
        body: Vec<u8>,
    },
    Heartbeat {
        resume_token: Vec<u8>,
    },
}

pub fn decode_watch_frame(p: &[u8]) -> DocResult<WatchFrame> {
    let mut r = Reader::new(p);
    match r.u8()? {
        WATCH_FRAME_ACK => Ok(WatchFrame::Ack {
            resume_token: r.lp()?.to_vec(),
        }),
        WATCH_FRAME_EVENT => Ok(WatchFrame::Event {
            resume_token: r.lp()?.to_vec(),
            op: r.u8()?,
            doc_id: r.lp()?.to_vec(),
            body: r.lp()?.to_vec(),
        }),
        WATCH_FRAME_HEARTBEAT => Ok(WatchFrame::Heartbeat {
            resume_token: r.lp()?.to_vec(),
        }),
        m => Err(DocError::Protocol(format!("unknown watch frame 0x{m:02x}"))),
    }
}

/// Encode an index-range response page:
/// `[row_count u32]{[doc_id][body]}* [cursor]` (cursor empty = end of results).
pub fn encode_query_page(page: &QueryPage) -> Vec<u8> {
    encode_query_page_inner(page, false)
}

/// Encode a FindRev page: each row appends an 8-byte big-endian revision.
pub fn encode_query_page_with_revision(page: &QueryPage) -> Vec<u8> {
    encode_query_page_inner(page, true)
}

fn encode_query_page_inner(page: &QueryPage, with_revision: bool) -> Vec<u8> {
    let mut out = Vec::new();
    out.extend_from_slice(&(page.rows.len() as u32).to_be_bytes());
    for row in &page.rows {
        put_lp(&mut out, &row.doc_id);
        put_lp(&mut out, row.body.as_deref().unwrap_or(&[]));
        if with_revision {
            out.extend_from_slice(&row.revision.unwrap_or(0).to_be_bytes());
        }
    }
    put_lp(&mut out, page.next_cursor.as_deref().unwrap_or(&[]));
    out
}

/// One decoded row from a query response page.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DecodedRow {
    pub doc_id: Vec<u8>,
    pub body: Vec<u8>,
    pub revision: Option<u64>,
}

/// Decode an index-range response page produced by [`encode_query_page`].
/// Returns the rows and an optional next-page cursor (empty cursor = end).
pub fn decode_query_page(p: &[u8]) -> DocResult<(Vec<DecodedRow>, Option<Vec<u8>>)> {
    decode_query_page_inner(p, false)
}

/// Decode a FindRev page produced by [`encode_query_page_with_revision`].
pub fn decode_query_page_with_revision(p: &[u8]) -> DocResult<(Vec<DecodedRow>, Option<Vec<u8>>)> {
    decode_query_page_inner(p, true)
}

fn decode_query_page_inner(
    p: &[u8],
    with_revision: bool,
) -> DocResult<(Vec<DecodedRow>, Option<Vec<u8>>)> {
    let mut r = Reader::new(p);
    let count = r.u32()?;
    let mut rows = Vec::with_capacity(count.min(4096));
    for _ in 0..count {
        let doc_id = r.lp()?.to_vec();
        let body = r.lp()?.to_vec();
        let revision = if with_revision {
            let mut buf = [0u8; 8];
            buf.copy_from_slice(r.take(8)?);
            Some(u64::from_be_bytes(buf))
        } else {
            None
        };
        rows.push(DecodedRow {
            doc_id,
            body,
            revision,
        });
    }
    let cursor = r.lp()?.to_vec();
    let next = if cursor.is_empty() {
        None
    } else {
        Some(cursor)
    };
    Ok((rows, next))
}

// ---- DocGetRev request: same framing as Query ById ----
// Response: [body lp][revision u64 BE]

/// Encode a DocGetRev / Query-ById-shaped request body.
pub fn encode_doc_get_rev(collection: &str, doc_id: &[u8]) -> Vec<u8> {
    QueryPayload::ById {
        collection: collection.to_string(),
        doc_id: doc_id.to_vec(),
    }
    .encode()
}

pub fn decode_doc_get_rev(p: &[u8]) -> DocResult<(String, Vec<u8>)> {
    match QueryPayload::decode(p)? {
        QueryPayload::ById { collection, doc_id } => Ok((collection, doc_id)),
        _ => Err(DocError::Protocol(
            "DocGetRev expects ById query payload".into(),
        )),
    }
}

pub fn encode_doc_get_rev_response(body: &[u8], revision: u64) -> Vec<u8> {
    let mut out = Vec::new();
    put_lp(&mut out, body);
    out.extend_from_slice(&revision.to_be_bytes());
    out
}

pub fn decode_doc_get_rev_response(p: &[u8]) -> DocResult<(Vec<u8>, u64)> {
    let mut r = Reader::new(p);
    let body = r.lp()?.to_vec();
    let mut buf = [0u8; 8];
    buf.copy_from_slice(r.take(8)?);
    Ok((body, u64::from_be_bytes(buf)))
}

// ---- DocPutIfMatch: [collection][doc_id][body][flags][if_match u64][optional expires_at] ----

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DocPutIfMatchPayload {
    pub collection: String,
    pub doc_id: Vec<u8>,
    pub body: Vec<u8>,
    pub relaxed: bool,
    pub if_match: u64,
    pub expires_at: u64,
}

impl DocPutIfMatchPayload {
    pub fn encode(&self) -> Vec<u8> {
        let mut out = Vec::new();
        put_lp(&mut out, self.collection.as_bytes());
        put_lp(&mut out, &self.doc_id);
        put_lp(&mut out, &self.body);
        out.push(if self.relaxed { FLAG_RELAXED } else { 0 });
        out.extend_from_slice(&self.if_match.to_be_bytes());
        if self.expires_at != 0 {
            out.extend_from_slice(&self.expires_at.to_be_bytes());
        }
        out
    }

    pub fn decode(p: &[u8]) -> DocResult<DocPutIfMatchPayload> {
        let mut r = Reader::new(p);
        let collection = r.lp_string()?;
        let doc_id = r.lp()?.to_vec();
        let body = r.lp()?.to_vec();
        let flags = r.u8()?;
        check_write_flags(flags, FLAG_RELAXED)?;
        let relaxed = flags & FLAG_RELAXED != 0;
        let mut buf = [0u8; 8];
        buf.copy_from_slice(r.take(8)?);
        let if_match = u64::from_be_bytes(buf);
        let expires_at = if r.remaining() >= 8 {
            buf.copy_from_slice(r.take(8)?);
            u64::from_be_bytes(buf)
        } else {
            0
        };
        Ok(DocPutIfMatchPayload {
            collection,
            doc_id,
            body,
            relaxed,
            if_match,
            expires_at,
        })
    }
}

// ---- DocUpdateIfMatch: [collection][doc_id][update][flags][if_match u64] ----

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DocUpdateIfMatchPayload {
    pub collection: String,
    pub doc_id: Vec<u8>,
    pub update: Vec<u8>,
    pub relaxed: bool,
    pub if_match: u64,
}

impl DocUpdateIfMatchPayload {
    pub fn encode(&self) -> Vec<u8> {
        let mut out = Vec::new();
        put_lp(&mut out, self.collection.as_bytes());
        put_lp(&mut out, &self.doc_id);
        put_lp(&mut out, &self.update);
        out.push(if self.relaxed { FLAG_RELAXED } else { 0 });
        out.extend_from_slice(&self.if_match.to_be_bytes());
        out
    }

    pub fn decode(p: &[u8]) -> DocResult<DocUpdateIfMatchPayload> {
        let mut r = Reader::new(p);
        let collection = r.lp_string()?;
        let doc_id = r.lp()?.to_vec();
        let update = r.lp()?.to_vec();
        let flags = r.u8()?;
        check_write_flags(flags, FLAG_RELAXED)?;
        let relaxed = flags & FLAG_RELAXED != 0;
        let mut buf = [0u8; 8];
        buf.copy_from_slice(r.take(8)?);
        let if_match = u64::from_be_bytes(buf);
        Ok(DocUpdateIfMatchPayload {
            collection,
            doc_id,
            update,
            relaxed,
            if_match,
        })
    }
}

// ---- Transaction control responses ----

/// Begin response: `[tx_id u64 BE][snapshot_seq u64 BE]`.
pub fn encode_begin_response(tx_id: u64, snapshot_seq: u64) -> Vec<u8> {
    let mut out = Vec::with_capacity(16);
    out.extend_from_slice(&tx_id.to_be_bytes());
    out.extend_from_slice(&snapshot_seq.to_be_bytes());
    out
}

pub fn decode_begin_response(p: &[u8]) -> DocResult<(u64, u64)> {
    if p.len() != 16 {
        return Err(DocError::Protocol("Begin response must be 16 bytes".into()));
    }
    let mut a = [0u8; 8];
    a.copy_from_slice(&p[..8]);
    let mut b = [0u8; 8];
    b.copy_from_slice(&p[8..]);
    Ok((u64::from_be_bytes(a), u64::from_be_bytes(b)))
}

/// Commit response: `[seq u64 BE]`.
pub fn encode_commit_response(seq: u64) -> Vec<u8> {
    seq.to_be_bytes().to_vec()
}

pub fn decode_commit_response(p: &[u8]) -> DocResult<u64> {
    if p.len() != 8 {
        return Err(DocError::Protocol("Commit response must be 8 bytes".into()));
    }
    let mut a = [0u8; 8];
    a.copy_from_slice(p);
    Ok(u64::from_be_bytes(a))
}

/// Stage acknowledgement: `[logical_ops u32 BE][estimated_keys u32 BE]`.
pub fn encode_stage_ack(logical_ops: u32, estimated_keys: u32) -> Vec<u8> {
    let mut out = Vec::with_capacity(8);
    out.extend_from_slice(&logical_ops.to_be_bytes());
    out.extend_from_slice(&estimated_keys.to_be_bytes());
    out
}

pub fn decode_stage_ack(p: &[u8]) -> DocResult<(u32, u32)> {
    if p.len() != 8 {
        return Err(DocError::Protocol("stage ack must be 8 bytes".into()));
    }
    let mut a = [0u8; 4];
    a.copy_from_slice(&p[..4]);
    let mut b = [0u8; 4];
    b.copy_from_slice(&p[4..]);
    Ok((u32::from_be_bytes(a), u32::from_be_bytes(b)))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn oversized_count_fields_rejected_without_huge_allocation() {
        // IndexDefPayload: collection, index name, unique, then count = u32::MAX.
        // Before the cap, the direction-default path did `vec![true; count]`
        // (multi-GB) even though every other path bounds the loop by bytes.
        let mut p = Vec::new();
        put_lp(&mut p, b"users");
        put_lp(&mut p, b"idx");
        p.push(0u8); // unique = false
        p.extend_from_slice(&u32::MAX.to_be_bytes());
        assert!(matches!(
            IndexDefPayload::decode(&p),
            Err(DocError::Protocol(_))
        ));

        // FindPayload: collection, filter, then sort_count = u32::MAX.
        let mut f = Vec::new();
        put_lp(&mut f, b"users");
        put_lp(&mut f, br"{}");
        f.extend_from_slice(&u32::MAX.to_be_bytes());
        assert!(matches!(
            FindPayload::decode(&f),
            Err(DocError::Protocol(_))
        ));

        // Projection field list: valid prefix, then count = u32::MAX.
        let mut fl = Vec::new();
        put_lp(&mut fl, b"users");
        put_lp(&mut fl, br"{}");
        fl.extend_from_slice(&0u32.to_be_bytes()); // no sort
        fl.push(PROJ_INCLUDE);
        fl.extend_from_slice(&u32::MAX.to_be_bytes());
        assert!(matches!(
            FindPayload::decode(&fl),
            Err(DocError::Protocol(_))
        ));

        // Sane counts still decode.
        let ok = IndexDefPayload {
            collection: "users".into(),
            index_name: "by_age".into(),
            fields: vec!["age".into()],
            unique: false,
            expire_after_seconds: 0,
            directions: vec![true],
        };
        let decoded = IndexDefPayload::decode(&ok.encode()).unwrap();
        assert_eq!(decoded.fields, vec!["age".to_string()]);
    }

    #[test]
    fn unused_write_flag_bits_rejected() {
        let mut put = DocPutPayload {
            collection: "users".into(),
            doc_id: b"u1".to_vec(),
            body: br#"{}"#.to_vec(),
            relaxed: false,
            expires_at: 0,
        }
        .encode();
        *put.last_mut().unwrap() |= 0x80;
        assert!(matches!(
            DocPutPayload::decode(&put),
            Err(DocError::Protocol(_))
        ));

        let mut upd = UpdatePayload {
            collection: "users".into(),
            filter: br#"{}"#.to_vec(),
            update: br#"{"$set":{"a":1}}"#.to_vec(),
            multi: false,
            relaxed: false,
            upsert: false,
        }
        .encode();
        *upd.last_mut().unwrap() |= 0x04;
        assert!(matches!(
            UpdatePayload::decode(&upd),
            Err(DocError::Protocol(_))
        ));
    }

    #[test]
    fn doc_put_round_trips() {
        let p = DocPutPayload {
            collection: "users".into(),
            doc_id: b"u1".to_vec(),
            body: br#"{"age":30}"#.to_vec(),
            relaxed: false,
            expires_at: 0,
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
            expire_after_seconds: 0,
            directions: vec![true, true],
        };
        assert_eq!(IndexDefPayload::decode(&p.encode()).unwrap(), p);
        // All-ASC omit direction trailer — wire matches pre-direction encoders.
        let legacy = {
            let mut out = Vec::new();
            put_lp(&mut out, b"users");
            put_lp(&mut out, b"by_age");
            out.push(1);
            out.extend_from_slice(&2u32.to_be_bytes());
            put_lp(&mut out, b"age");
            put_lp(&mut out, b"name");
            out
        };
        assert_eq!(IndexDefPayload::decode(&legacy).unwrap(), p);

        let ttl = IndexDefPayload {
            collection: "sess".into(),
            index_name: "by_exp".into(),
            fields: vec!["exp".into()],
            unique: false,
            expire_after_seconds: 3600,
            directions: vec![true],
        };
        assert_eq!(IndexDefPayload::decode(&ttl.encode()).unwrap(), ttl);

        let desc = IndexDefPayload {
            collection: "events".into(),
            index_name: "by_owner_ts".into(),
            fields: vec!["ownerId".into(), "updatedAt".into()],
            unique: false,
            expire_after_seconds: 0,
            directions: vec![true, false],
        };
        let round = IndexDefPayload::decode(&desc.encode()).unwrap();
        assert_eq!(round.directions, vec![true, false]);
        assert_eq!(round.expire_after_seconds, 0);
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
            include_bodies: true,
        };
        assert_eq!(QueryPayload::decode(&p.encode()).unwrap(), p);
        let ids_only = QueryPayload::IndexRange {
            collection: "users".into(),
            index_name: "by_age".into(),
            lo: vec![],
            hi: vec![],
            cursor: vec![],
            limit: 10,
            include_bodies: false,
        };
        assert_eq!(QueryPayload::decode(&ids_only.encode()).unwrap(), ids_only);

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
            upsert: false,
        };
        assert_eq!(UpdatePayload::decode(&u.encode()).unwrap(), u);

        let u2 = UpdatePayload {
            collection: "users".into(),
            filter: br#"{"email":"a@b.c"}"#.to_vec(),
            update: br#"{"$set":{"email":"a@b.c"}}"#.to_vec(),
            multi: false,
            relaxed: false,
            upsert: true,
        };
        assert_eq!(UpdatePayload::decode(&u2.encode()).unwrap(), u2);

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
    fn aggregate_round_trip_and_result_budget() {
        let p = AggregatePayload {
            collection: "sales".into(),
            pipeline: br#"[{"$group":{"_id":"$team","n":{"$count":{}}}}]"#.to_vec(),
        };
        assert_eq!(AggregatePayload::decode(&p.encode()).unwrap(), p);

        let rows = vec![
            serde_json::json!({"_id":"a","n":1}),
            serde_json::json!({"_id":"b","n":2}),
        ];
        let encoded = encode_aggregate_response(&rows, 4 * 1024 * 1024).unwrap();
        let decoded = decode_aggregate_response(&encoded).unwrap();
        assert_eq!(decoded.len(), 2);
        assert_eq!(
            serde_json::from_slice::<serde_json::Value>(&decoded[0]).unwrap(),
            rows[0]
        );
        assert!(encode_aggregate_response(&rows, 4).is_err());
    }

    #[test]
    fn query_page_round_trips() {
        let page = QueryPage {
            rows: vec![
                crate::query::QueryRow {
                    doc_id: b"u1".to_vec(),
                    body: Some(b"{}".to_vec()),
                    revision: None,
                },
                crate::query::QueryRow {
                    doc_id: b"u2".to_vec(),
                    body: None,
                    revision: None,
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

        let with_rev = QueryPage {
            rows: vec![crate::query::QueryRow {
                doc_id: b"u1".to_vec(),
                body: Some(b"{}".to_vec()),
                revision: Some(42),
            }],
            next_cursor: None,
        };
        let (rows, _) =
            decode_query_page_with_revision(&encode_query_page_with_revision(&with_rev)).unwrap();
        assert_eq!(rows[0].revision, Some(42));

        let put = DocPutIfMatchPayload {
            collection: "users".into(),
            doc_id: b"u1".to_vec(),
            body: br#"{"age":30}"#.to_vec(),
            relaxed: false,
            if_match: 7,
            expires_at: 0,
        };
        assert_eq!(DocPutIfMatchPayload::decode(&put.encode()).unwrap(), put);

        let upd = DocUpdateIfMatchPayload {
            collection: "users".into(),
            doc_id: b"u1".to_vec(),
            update: br#"{"$inc":{"age":1}}"#.to_vec(),
            relaxed: true,
            if_match: 9,
        };
        assert_eq!(DocUpdateIfMatchPayload::decode(&upd.encode()).unwrap(), upd);

        let (body, rev) =
            decode_doc_get_rev_response(&encode_doc_get_rev_response(br#"{"a":1}"#, 11)).unwrap();
        assert_eq!(body, br#"{"a":1}"#);
        assert_eq!(rev, 11);

        let begin = encode_begin_response(7, 42);
        let (txid, snap) = decode_begin_response(&begin).unwrap();
        assert_eq!((txid, snap), (7, 42));
        let commit = encode_commit_response(99);
        assert_eq!(decode_commit_response(&commit).unwrap(), 99);
        let stage = encode_stage_ack(3, 12);
        assert_eq!(decode_stage_ack(&stage).unwrap(), (3, 12));
    }
}
