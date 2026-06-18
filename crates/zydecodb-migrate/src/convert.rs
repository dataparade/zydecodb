//! Faithful type conversion and document assembly.
//!
//! Two jobs:
//!
//! 1. **Scalar conversion** ([`convert_scalar`]) turns a Postgres text value into
//!    a JSON value the engine can store and index. The non-obvious choices, all
//!    driven by the engine's index encoding:
//!    - `numeric(p,s)` / `money` -> a whole number of minor units (cents), so
//!      money stays *exact* and still sorts/range-queries as a number (the index
//!      coerces JSON numbers through `f64`, which is lossy for large decimals).
//!    - timestamps/dates -> epoch milliseconds (a sortable integer).
//!    - json/jsonb -> parsed through; arrays -> a JSON array; enums -> strings.
//!
//! 2. **Document assembly** ([`build_collection_docs`]) materializes each
//!    collection's documents: one object per surviving row, with owned children
//!    embedded, shared entities snapshotted into those children, and dissolved
//!    join tables turned into id-list fields. The `_id` rule is applied here and
//!    is deterministic so reruns upsert the same documents.

use crate::classify::{CollectionPlan, EmbedPlan, IdStrategy, JoinDissolve, SnapshotSource};
use crate::error::MigrateResult;
use crate::pgdump::{Dump, Table};
use serde_json::{Map, Value};
use std::collections::HashMap;

/// A document ready to upsert.
#[derive(Debug, Clone)]
pub struct ConvertedDoc {
    pub id: String,
    pub body: Vec<u8>,
}

/// Build all documents for one collection.
pub fn build_collection_docs(
    dump: &Dump,
    coll: &CollectionPlan,
) -> MigrateResult<Vec<ConvertedDoc>> {
    let table = match dump.table(&coll.name) {
        Some(t) => t,
        None => return Ok(Vec::new()),
    };

    let mut docs = Vec::with_capacity(table.rows.len());
    for row in &table.rows {
        let mut obj = row_to_object(table, row);

        for embed in &coll.embeds {
            let value = build_embed(dump, table, row, embed)?;
            obj.insert(embed.field.clone(), value);
        }
        for dissolve in &coll.join_dissolves {
            let ids = build_join_ids(dump, table, row, dissolve);
            obj.insert(dissolve.field.clone(), Value::Array(ids));
        }

        let id = document_id(table, row, &coll.id_strategy);
        obj.insert("_id".to_string(), Value::String(id.clone()));

        let body = serde_json::to_vec(&Value::Object(obj))
            .map_err(|e| crate::error::MigrateError::Parse(e.to_string()))?;
        docs.push(ConvertedDoc { id, body });
    }
    Ok(docs)
}

/// Convert a row into a JSON object keyed by column name.
fn row_to_object(table: &Table, row: &[Option<String>]) -> Map<String, Value> {
    let mut obj = Map::new();
    for (i, col_name) in table.copy_columns.iter().enumerate() {
        let raw = row.get(i).and_then(|v| v.as_ref());
        let sql_type = table
            .column(col_name)
            .map(|c| c.sql_type.as_str())
            .unwrap_or("text");
        let value = match raw {
            Some(s) => convert_scalar(sql_type, s),
            None => Value::Null,
        };
        obj.insert(col_name.clone(), value);
    }
    obj
}

/// Build the embedded value (object for 1:1, array for 1:N) for one parent row.
fn build_embed(
    dump: &Dump,
    parent: &Table,
    parent_row: &[Option<String>],
    embed: &EmbedPlan,
) -> MigrateResult<Value> {
    let child = match dump.table(&embed.child_table) {
        Some(c) => c,
        None => return Ok(Value::Null),
    };
    let parent_key = key_values(parent, parent_row, &embed.parent_columns);
    let parent_key = match parent_key {
        Some(k) => k,
        None => {
            return Ok(if embed.one_to_one {
                Value::Null
            } else {
                Value::Array(Vec::new())
            })
        }
    };

    let child_idx = match column_indexes(child, &embed.fk_columns) {
        Some(v) => v,
        None => return Ok(Value::Array(Vec::new())),
    };

    let mut matches = Vec::new();
    for crow in &child.rows {
        if row_key_at(crow, &child_idx) == Some(parent_key.clone()) {
            let mut cobj = row_to_object(child, crow);
            for snap in &embed.snapshots {
                if let Some(snap_val) = build_snapshot(dump, child, crow, snap) {
                    cobj.insert(snap.field.clone(), snap_val);
                }
            }
            matches.push(Value::Object(cobj));
        }
    }

    if embed.one_to_one {
        Ok(matches.into_iter().next().unwrap_or(Value::Null))
    } else {
        Ok(Value::Array(matches))
    }
}

/// Snapshot a shared entity's scalar columns into an embedded child, as a
/// nested object. Captures point-in-time truth (and saves a read-time lookup).
fn build_snapshot(
    dump: &Dump,
    child: &Table,
    child_row: &[Option<String>],
    snap: &SnapshotSource,
) -> Option<Value> {
    let ref_table = dump.table(&snap.ref_table)?;
    let child_idx = column_indexes(child, &snap.fk_columns)?;
    let fk_value = row_key_at(child_row, &child_idx)?;

    let ref_idx = column_indexes(ref_table, &snap.ref_columns)?;
    for rrow in &ref_table.rows {
        if row_key_at(rrow, &ref_idx) == Some(fk_value.clone()) {
            return Some(Value::Object(row_to_object(ref_table, rrow)));
        }
    }
    None
}

/// Collect the ids on the "other" side of a dissolved join table for one host.
fn build_join_ids(
    dump: &Dump,
    host: &Table,
    host_row: &[Option<String>],
    dissolve: &JoinDissolve,
) -> Vec<Value> {
    let join = match dump.table(&dissolve.join_table) {
        Some(j) => j,
        None => return Vec::new(),
    };
    // Host PK values, matched against the join table's host-side FK columns.
    let host_pk = match key_values(host, host_row, &host.primary_key) {
        Some(k) => k,
        None => return Vec::new(),
    };
    let host_fk_idx = match column_indexes(join, &dissolve.host_fk_columns) {
        Some(v) => v,
        None => return Vec::new(),
    };
    let other_idx = match column_indexes(join, &dissolve.other_fk_columns) {
        Some(v) => v,
        None => return Vec::new(),
    };
    let mut out = Vec::new();
    for jrow in &join.rows {
        if row_key_at(jrow, &host_fk_idx) == Some(host_pk.clone()) {
            if let Some(vals) = row_key_at(jrow, &other_idx) {
                // Single-column FK is the common case; join the parts otherwise.
                out.push(Value::String(vals.join(":")));
            }
        }
    }
    out
}

/// Derive a deterministic document id so reruns upsert (never duplicate).
fn document_id(table: &Table, row: &[Option<String>], strategy: &IdStrategy) -> String {
    match strategy {
        IdStrategy::PrimaryKey(col) => {
            if let Some(idx) = table.copy_columns.iter().position(|c| c == col) {
                if let Some(Some(v)) = row.get(idx) {
                    return v.clone();
                }
            }
            stable_id(row)
        }
        IdStrategy::Generated => {
            // Composite PK -> join its values (stable + unique). Else hash row.
            if !table.primary_key.is_empty() {
                if let Some(idx) = column_indexes(table, &table.primary_key) {
                    if let Some(vals) = row_key_at(row, &idx) {
                        return vals.join(":");
                    }
                }
            }
            stable_id(row)
        }
    }
}

/// FNV-1a hash of all column values, hex-encoded. Stable across reruns of the
/// same dump, so id-less rows still upsert idempotently.
fn stable_id(row: &[Option<String>]) -> String {
    let mut hash: u64 = 0xcbf29ce484222325;
    for cell in row {
        let bytes = match cell {
            Some(s) => s.as_bytes(),
            None => b"\x00NULL",
        };
        for &b in bytes {
            hash ^= b as u64;
            hash = hash.wrapping_mul(0x100000001b3);
        }
        hash ^= 0xff;
        hash = hash.wrapping_mul(0x100000001b3);
    }
    format!("{hash:016x}")
}

// ---- scalar conversion ----

/// Convert one Postgres text value to JSON, honoring its declared SQL type.
pub fn convert_scalar(sql_type: &str, raw: &str) -> Value {
    let t = sql_type.trim();
    if let Some(elem) = t.strip_suffix("[]") {
        return convert_array(elem, raw);
    }
    if t.starts_with("int")
        || t.starts_with("bigint")
        || t.starts_with("smallint")
        || t.starts_with("serial")
        || t.starts_with("bigserial")
    {
        if let Ok(n) = raw.parse::<i64>() {
            return Value::Number(n.into());
        }
        return Value::String(raw.to_string());
    }
    if t.starts_with("numeric") || t.starts_with("decimal") || t.starts_with("money") {
        return convert_decimal(t, raw);
    }
    if t.starts_with("double") || t.starts_with("real") || t.starts_with("float") {
        if let Ok(f) = raw.parse::<f64>() {
            if let Some(n) = serde_json::Number::from_f64(f) {
                return Value::Number(n);
            }
        }
        return Value::String(raw.to_string());
    }
    if t.starts_with("bool") {
        return match raw {
            "t" | "true" | "TRUE" | "1" => Value::Bool(true),
            "f" | "false" | "FALSE" | "0" => Value::Bool(false),
            _ => Value::String(raw.to_string()),
        };
    }
    if t.starts_with("timestamp") || t == "date" {
        if let Some(ms) = parse_timestamp_millis(raw) {
            return Value::Number(ms.into());
        }
        return Value::String(raw.to_string());
    }
    if t.starts_with("json") {
        if let Ok(v) = serde_json::from_str::<Value>(raw) {
            return v;
        }
        return Value::String(raw.to_string());
    }
    // text, varchar, char, uuid, enums (USER-DEFINED), and anything else.
    Value::String(raw.to_string())
}

/// `numeric(p,s)` / `money` -> integer minor units (exact). When no scale is
/// declared (bare `numeric`) the value is kept as a string to avoid silent
/// rounding; large values that overflow `i64` likewise fall back to a string.
fn convert_decimal(sql_type: &str, raw: &str) -> Value {
    let scale = if sql_type.starts_with("money") {
        Some(2)
    } else {
        decimal_scale(sql_type)
    };
    let scale = match scale {
        Some(s) => s,
        None => return Value::String(raw.to_string()),
    };
    match decimal_to_minor(raw, scale) {
        Some(n) if i64::try_from(n).is_ok() => Value::Number((n as i64).into()),
        _ => Value::String(raw.to_string()),
    }
}

/// Extract `s` from `numeric(p,s)`; `numeric(p)` -> scale 0; bare -> None.
fn decimal_scale(sql_type: &str) -> Option<u32> {
    let open = sql_type.find('(')?;
    let close = sql_type.find(')')?;
    let inner = &sql_type[open + 1..close];
    let mut parts = inner.split(',');
    let _precision = parts.next();
    match parts.next() {
        Some(s) => s.trim().parse::<u32>().ok(),
        None => Some(0),
    }
}

/// Convert a decimal string to an integer number of minor units at `scale`,
/// without floating point. Extra fractional digits are truncated.
fn decimal_to_minor(raw: &str, scale: u32) -> Option<i128> {
    let raw = raw.trim();
    let (neg, digits) = match raw.strip_prefix('-') {
        Some(r) => (true, r),
        None => (false, raw.strip_prefix('+').unwrap_or(raw)),
    };
    let (int_part, frac_part) = match digits.split_once('.') {
        Some((i, f)) => (i, f),
        None => (digits, ""),
    };
    if !int_part.chars().all(|c| c.is_ascii_digit()) || int_part.is_empty() && frac_part.is_empty()
    {
        return None;
    }
    if !frac_part.chars().all(|c| c.is_ascii_digit()) {
        return None;
    }
    let scale = scale as usize;
    let mut frac = String::with_capacity(scale);
    for i in 0..scale {
        frac.push(frac_part.as_bytes().get(i).copied().unwrap_or(b'0') as char);
    }
    let combined = format!("{}{}", int_part, frac);
    let mut value: i128 = combined.parse::<i128>().ok()?;
    if neg {
        value = -value;
    }
    Some(value)
}

/// Parse a Postgres array literal into a JSON array, honoring the text format's
/// quoting rules: elements containing commas/braces/spaces are double-quoted,
/// `\"` and `\\` are escapes inside a quoted element, an unquoted `NULL` is the
/// SQL null (the quoted string `"NULL"` is just text), and nested `{...}` are
/// multidimensional arrays. Falls back to the raw string if it is not an array
/// literal at all. Element typing is best-effort.
fn convert_array(elem_type: &str, raw: &str) -> Value {
    match parse_array_literal(elem_type, raw.trim()) {
        Some(v) => v,
        None => Value::String(raw.to_string()),
    }
}

/// Parse a `{...}` array body (recursively for nested arrays).
fn parse_array_literal(elem_type: &str, s: &str) -> Option<Value> {
    let inner = s.strip_prefix('{')?.strip_suffix('}')?;
    let mut out = Vec::new();
    if inner.trim().is_empty() {
        return Some(Value::Array(out));
    }
    for tok in split_array_tokens(inner) {
        out.push(array_token_to_value(elem_type, &tok));
    }
    Some(Value::Array(out))
}

/// Split an array body into top-level element tokens, respecting double quotes
/// (with `\` escaping) and nested-brace depth. Tokens keep their quotes/braces.
fn split_array_tokens(inner: &str) -> Vec<String> {
    let mut tokens = Vec::new();
    let mut cur = String::new();
    let mut in_quotes = false;
    let mut depth = 0i32;
    let mut chars = inner.chars();
    while let Some(c) = chars.next() {
        if in_quotes {
            cur.push(c);
            if c == '\\' {
                if let Some(next) = chars.next() {
                    cur.push(next); // escaped char stays inside the token
                }
            } else if c == '"' {
                in_quotes = false;
            }
            continue;
        }
        match c {
            '"' => {
                in_quotes = true;
                cur.push(c);
            }
            '{' => {
                depth += 1;
                cur.push(c);
            }
            '}' => {
                depth -= 1;
                cur.push(c);
            }
            ',' if depth == 0 => tokens.push(std::mem::take(&mut cur)),
            _ => cur.push(c),
        }
    }
    tokens.push(cur);
    tokens
}

/// Interpret one array element token.
fn array_token_to_value(elem_type: &str, tok: &str) -> Value {
    let t = tok.trim();
    if t.starts_with('{') {
        if let Some(v) = parse_array_literal(elem_type, t) {
            return v;
        }
    }
    if t.len() >= 2 && t.starts_with('"') && t.ends_with('"') {
        let unescaped = unescape_array_element(&t[1..t.len() - 1]);
        return convert_scalar(elem_type, &unescaped);
    }
    if t.eq_ignore_ascii_case("null") {
        return Value::Null;
    }
    convert_scalar(elem_type, t)
}

/// Undo `\"` / `\\` escaping inside a quoted array element.
fn unescape_array_element(s: &str) -> String {
    let mut out = String::with_capacity(s.len());
    let mut chars = s.chars();
    while let Some(c) = chars.next() {
        if c == '\\' {
            if let Some(next) = chars.next() {
                out.push(next);
            }
        } else {
            out.push(c);
        }
    }
    out
}

/// Parse `YYYY-MM-DD[ HH:MM:SS[.fff]]` (optionally `T`-separated, optional tz)
/// into epoch milliseconds (UTC). Returns None if it does not match.
fn parse_timestamp_millis(raw: &str) -> Option<i64> {
    let s = raw.trim();
    let (date_part, time_part) = if let Some((d, t)) = s.split_once(['T', ' ']) {
        (d, Some(t))
    } else {
        (s, None)
    };
    let mut dp = date_part.split('-');
    let year: i64 = dp.next()?.parse().ok()?;
    let month: i64 = dp.next()?.parse().ok()?;
    let day: i64 = dp.next()?.parse().ok()?;

    let (mut hour, mut min, mut sec, mut millis) = (0i64, 0i64, 0i64, 0i64);
    if let Some(tp) = time_part {
        // Drop a trailing timezone (Z or +/-offset); we treat values as UTC.
        let tp = tp.trim_end_matches('Z');
        let tp = strip_tz_offset(tp);
        let mut hms = tp.split(':');
        hour = hms.next().unwrap_or("0").parse().ok()?;
        min = hms.next().unwrap_or("0").parse().ok()?;
        if let Some(sec_str) = hms.next() {
            if let Some((s_int, frac)) = sec_str.split_once('.') {
                sec = s_int.parse().ok()?;
                let mut f = frac.to_string();
                f.truncate(3);
                while f.len() < 3 {
                    f.push('0');
                }
                millis = f.parse().unwrap_or(0);
            } else {
                sec = sec_str.parse().ok()?;
            }
        }
    }

    let days = days_from_civil(year, month, day);
    let total_secs = days * 86_400 + hour * 3_600 + min * 60 + sec;
    Some(total_secs * 1_000 + millis)
}

/// Remove a trailing `+HH[:MM]` / `-HH[:MM]` timezone offset from a time string.
fn strip_tz_offset(tp: &str) -> &str {
    // Find a sign that is not at position 0 (offsets follow the seconds field).
    if let Some(pos) = tp.rfind(['+', '-']).filter(|&p| p > 0) {
        &tp[..pos]
    } else {
        tp
    }
}

/// Days since the Unix epoch for a civil date (Howard Hinnant's algorithm).
fn days_from_civil(y: i64, m: i64, d: i64) -> i64 {
    let y = if m <= 2 { y - 1 } else { y };
    let era = if y >= 0 { y } else { y - 399 } / 400;
    let yoe = y - era * 400;
    let doy = (153 * (if m > 2 { m - 3 } else { m + 9 }) + 2) / 5 + d - 1;
    let doe = yoe * 365 + yoe / 4 - yoe / 100 + doy;
    era * 146_097 + doe - 719_468
}

// ---- key helpers ----

fn column_indexes(table: &Table, names: &[String]) -> Option<Vec<usize>> {
    let mut out = Vec::with_capacity(names.len());
    for n in names {
        out.push(table.copy_columns.iter().position(|c| c == n)?);
    }
    Some(out)
}

fn key_values(table: &Table, row: &[Option<String>], cols: &[String]) -> Option<Vec<String>> {
    let idx = column_indexes(table, cols)?;
    row_key_at(row, &idx)
}

/// Read the values at `idx`, returning None if any is NULL.
fn row_key_at(row: &[Option<String>], idx: &[usize]) -> Option<Vec<String>> {
    let mut out = Vec::with_capacity(idx.len());
    for &i in idx {
        out.push(row.get(i)?.clone()?);
    }
    Some(out)
}

#[allow(dead_code)]
fn build_row_index(table: &Table, cols: &[String]) -> HashMap<Vec<String>, Vec<usize>> {
    let mut map: HashMap<Vec<String>, Vec<usize>> = HashMap::new();
    if let Some(idx) = column_indexes(table, cols) {
        for (ri, row) in table.rows.iter().enumerate() {
            if let Some(key) = row_key_at(row, &idx) {
                map.entry(key).or_default().push(ri);
            }
        }
    }
    map
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{classify, graph, pgdump};
    use serde_json::json;

    #[test]
    fn money_becomes_minor_units() {
        assert_eq!(convert_scalar("numeric(20,4)", "19.9900"), json!(199900));
        assert_eq!(convert_scalar("numeric(10,2)", "19.99"), json!(1999));
        assert_eq!(convert_scalar("numeric(10,2)", "-5"), json!(-500));
        assert_eq!(convert_scalar("money", "10.5"), json!(1050));
    }

    #[test]
    fn bare_numeric_preserved_as_string() {
        assert_eq!(convert_scalar("numeric", "3.14159"), json!("3.14159"));
    }

    #[test]
    fn integers_and_bools() {
        assert_eq!(convert_scalar("integer", "42"), json!(42));
        assert_eq!(convert_scalar("boolean", "t"), json!(true));
        assert_eq!(convert_scalar("boolean", "f"), json!(false));
    }

    #[test]
    fn timestamp_to_epoch_millis() {
        // 2021-01-01T00:00:00Z = 1609459200000 ms.
        assert_eq!(
            convert_scalar("timestamp without time zone", "2021-01-01 00:00:00"),
            json!(1_609_459_200_000i64)
        );
        // Unix epoch.
        assert_eq!(convert_scalar("date", "1970-01-01"), json!(0));
    }

    #[test]
    fn json_passthrough() {
        assert_eq!(convert_scalar("jsonb", r#"{"a":1}"#), json!({"a": 1}));
    }

    #[test]
    fn array_literal() {
        assert_eq!(convert_scalar("integer[]", "{1,2,3}"), json!([1, 2, 3]));
    }

    const SHOP: &str = r#"
CREATE TABLE public.products (
    id integer NOT NULL,
    name text NOT NULL,
    price numeric(10,2) NOT NULL
);
CREATE TABLE public.orders (
    id integer NOT NULL,
    customer_id integer NOT NULL
);
CREATE TABLE public.line_items (
    id integer NOT NULL,
    order_id integer NOT NULL,
    product_id integer NOT NULL,
    qty integer NOT NULL
);
COPY public.products (id, name, price) FROM stdin;
50	Widget	9.99
\.
COPY public.orders (id, customer_id) FROM stdin;
1000	1
\.
COPY public.line_items (id, order_id, product_id, qty) FROM stdin;
1	1000	50	2
\.
ALTER TABLE ONLY public.products ADD CONSTRAINT products_pkey PRIMARY KEY (id);
ALTER TABLE ONLY public.orders ADD CONSTRAINT orders_pkey PRIMARY KEY (id);
ALTER TABLE ONLY public.line_items ADD CONSTRAINT line_items_pkey PRIMARY KEY (id);
ALTER TABLE ONLY public.line_items ADD CONSTRAINT li_order_fkey FOREIGN KEY (order_id) REFERENCES public.orders(id);
ALTER TABLE ONLY public.line_items ADD CONSTRAINT li_product_fkey FOREIGN KEY (product_id) REFERENCES public.products(id);
"#;

    #[test]
    fn order_doc_embeds_items_with_product_snapshot() {
        let dump = pgdump::parse(SHOP).unwrap();
        let g = graph::build(&dump);
        let plan = classify::classify(&dump, &g);
        let orders = plan.collection("orders").unwrap();
        let docs = build_collection_docs(&dump, orders).unwrap();
        assert_eq!(docs.len(), 1);
        let v: Value = serde_json::from_slice(&docs[0].body).unwrap();

        assert_eq!(v["_id"], json!("1000"));
        let items = v["line_items"].as_array().unwrap();
        assert_eq!(items.len(), 1);
        assert_eq!(items[0]["qty"], json!(2));
        // Snapshotted product (price in minor units).
        assert_eq!(items[0]["products"]["name"], json!("Widget"));
        assert_eq!(items[0]["products"]["price"], json!(999));
    }
}
