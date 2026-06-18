//! Parser for plain `pg_dump` output (the default text format with `COPY`
//! data blocks).
//!
//! A plain dump is not self-contained SQL we can hand to a driver: it
//! interleaves `CREATE TABLE`, `COPY ... FROM stdin` data blocks, and trailing
//! `ALTER TABLE ... ADD CONSTRAINT` / `CREATE INDEX` statements. We parse it in
//! a single line-oriented pass that understands those four shapes and ignores
//! everything else (SET, sequences, comments, ...).
//!
//! Constraints live in the trailing `ALTER TABLE` block, *after* the data, so
//! the parser fills each table's columns and rows first and then back-fills the
//! primary key, foreign keys, unique constraints, and indexes onto the
//! already-seen table. That is the "two pass over the file shape" the plan
//! calls for, expressed as one streaming pass with deferred constraint attach.

use crate::error::{MigrateError, MigrateResult};
use std::collections::HashMap;

/// One column of a table.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Column {
    pub name: String,
    /// Lowercased SQL type as written in the dump, e.g. `numeric(20,4)`,
    /// `timestamp without time zone`, `character varying(255)`. The converter
    /// interprets this; the parser keeps it verbatim.
    pub sql_type: String,
    pub not_null: bool,
}

/// A foreign-key constraint: `columns` in this table reference `ref_columns`
/// in `ref_table`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ForeignKey {
    pub columns: Vec<String>,
    pub ref_table: String,
    pub ref_columns: Vec<String>,
}

/// A parsed table: schema, constraints, and its data rows.
#[derive(Debug, Clone, Default)]
pub struct Table {
    pub name: String,
    pub columns: Vec<Column>,
    pub primary_key: Vec<String>,
    /// Each entry is the column set of one UNIQUE constraint.
    pub unique: Vec<Vec<String>>,
    pub foreign_keys: Vec<ForeignKey>,
    /// Column sets that Postgres had a (non-unique or unique) index on. The
    /// strongest available signal for "the app queries by these columns".
    pub indexed_columns: Vec<Vec<String>>,
    /// Number of CHECK constraints (counted for the dropped-constraints report).
    pub check_constraints: usize,
    /// Column order used by this table's `COPY` block.
    pub copy_columns: Vec<String>,
    /// Data rows, aligned to `copy_columns`. `None` = SQL NULL.
    pub rows: Vec<Vec<Option<String>>>,
}

impl Table {
    pub fn column(&self, name: &str) -> Option<&Column> {
        self.columns.iter().find(|c| c.name == name)
    }
}

/// The whole parsed dump.
#[derive(Debug, Clone, Default)]
pub struct Dump {
    pub tables: Vec<Table>,
}

impl Dump {
    pub fn table(&self, name: &str) -> Option<&Table> {
        self.tables.iter().find(|t| t.name == name)
    }
}

/// Parse a plain `pg_dump` from its full text contents.
pub fn parse(contents: &str) -> MigrateResult<Dump> {
    let lines: Vec<&str> = contents.lines().collect();
    let mut tables: Vec<Table> = Vec::new();
    let mut index_of: HashMap<String, usize> = HashMap::new();

    let mut i = 0;
    while i < lines.len() {
        let raw = lines[i];
        let line = raw.trim_start();

        if let Some(rest) = strip_kw(line, "COPY ") {
            // COPY <table> [(cols)] FROM stdin;  then data rows until "\.".
            let (table, cols) = parse_copy_header(rest)?;
            let mut rows: Vec<Vec<Option<String>>> = Vec::new();
            i += 1;
            while i < lines.len() {
                let data = lines[i];
                if data == "\\." {
                    break;
                }
                rows.push(parse_copy_row(data));
                i += 1;
            }
            if let Some(&idx) = index_of.get(&table) {
                let t: &mut Table = &mut tables[idx];
                t.copy_columns = if cols.is_empty() {
                    t.columns.iter().map(|c| c.name.clone()).collect()
                } else {
                    cols
                };
                t.rows = rows;
            }
            i += 1;
            continue;
        }

        // `pg_dump --inserts` / `--column-inserts` emit data as INSERT
        // statements instead of COPY. We only understand COPY, and silently
        // dropping the data would produce an empty migration, so refuse loudly.
        if is_kw(line, "INSERT INTO ") {
            return Err(MigrateError::Parse(
                "this dump uses INSERT statements for data (pg_dump --inserts); \
                 only the default plain COPY format is supported. Re-run pg_dump \
                 without --inserts/--column-inserts."
                    .to_string(),
            ));
        }

        if is_kw(line, "CREATE TABLE ") {
            let (stmt, next) = take_statement(&lines, i);
            i = next;
            // Partitioned/inherited tables don't reshape to a single faithful
            // collection (their data lives in or is split across other tables),
            // so refuse rather than emit a garbage table from the mangled DDL.
            if let Some(feature) = unsupported_table_feature(&stmt) {
                return Err(MigrateError::Parse(format!(
                    "unsupported table feature ({feature}); partitioned and \
                     inherited tables cannot be migrated automatically"
                )));
            }
            if let Some(table) = parse_create_table(&stmt)? {
                // Two tables that normalize to the same bare name (e.g. the same
                // table in different schemas) would clobber each other and
                // mis-route COPY data. We strip schemas, so this is ambiguous;
                // refuse rather than corrupt.
                if index_of.contains_key(&table.name) {
                    return Err(MigrateError::Parse(format!(
                        "table name collision: '{}' is defined more than once \
                         (likely the same name under different schemas); \
                         multi-schema dumps with duplicate table names are not \
                         supported",
                        table.name
                    )));
                }
                index_of.insert(table.name.clone(), tables.len());
                tables.push(table);
            }
            continue;
        }

        if is_kw(line, "ALTER TABLE ") {
            let (stmt, next) = take_statement(&lines, i);
            i = next;
            apply_alter_table(&stmt, &mut tables, &index_of);
            continue;
        }

        if is_kw(line, "CREATE INDEX ") || is_kw(line, "CREATE UNIQUE INDEX ") {
            let (stmt, next) = take_statement(&lines, i);
            i = next;
            apply_create_index(&stmt, &mut tables, &index_of);
            continue;
        }

        i += 1;
    }

    Ok(Dump { tables })
}

// ---- statement assembly ----

/// Collect lines starting at `start` until the statement terminates. A
/// statement ends at a `;` that is **outside** any quoted string, so a `;`
/// living inside a single- or double-quoted literal (even one that ends a
/// physical line) does not truncate the statement. Returns the joined statement
/// and the index of the line after the terminator.
fn take_statement(lines: &[&str], start: usize) -> (String, usize) {
    let mut buf = String::new();
    let mut i = start;
    while i < lines.len() {
        if !buf.is_empty() {
            buf.push(' ');
        }
        buf.push_str(lines[i].trim());
        i += 1;
        if statement_complete(&buf) {
            break;
        }
    }
    (buf, i)
}

/// True when `buf`'s last non-space char is `;` and that `;` sits outside any
/// quoted string.
fn statement_complete(buf: &str) -> bool {
    let trimmed = buf.trim_end();
    trimmed.ends_with(';') && !ends_inside_quote(trimmed)
}

/// Scan `s` tracking single- and double-quote state (with SQL `''` / `""`
/// escaping) and report whether the end of `s` lies inside a string literal.
fn ends_inside_quote(s: &str) -> bool {
    let bytes = s.as_bytes();
    let mut in_single = false;
    let mut in_double = false;
    let mut i = 0;
    while i < bytes.len() {
        let c = bytes[i];
        if in_single {
            if c == b'\'' {
                if bytes.get(i + 1) == Some(&b'\'') {
                    i += 2; // escaped quote ''
                    continue;
                }
                in_single = false;
            }
        } else if in_double {
            if c == b'"' {
                if bytes.get(i + 1) == Some(&b'"') {
                    i += 2; // escaped quote ""
                    continue;
                }
                in_double = false;
            }
        } else {
            match c {
                b'\'' => in_single = true,
                b'"' => in_double = true,
                _ => {}
            }
        }
        i += 1;
    }
    in_single || in_double
}

// ---- CREATE TABLE ----

/// Detect a `CREATE TABLE` shape we deliberately do not support, returning the
/// construct's name for the error message. Matched on word boundaries so a mere
/// column named `partition_key` does not trip it.
fn unsupported_table_feature(stmt: &str) -> Option<&'static str> {
    let upper = stmt.to_ascii_uppercase();
    if upper.contains(" PARTITION OF ") {
        Some("PARTITION OF")
    } else if upper.contains(" PARTITION BY ") {
        Some("PARTITION BY")
    } else if upper.contains(" INHERITS ") || upper.contains(" INHERITS(") {
        Some("INHERITS")
    } else {
        None
    }
}

fn parse_create_table(stmt: &str) -> MigrateResult<Option<Table>> {
    // CREATE TABLE <name> ( <col defs> );
    let open = match stmt.find('(') {
        Some(p) => p,
        None => return Ok(None),
    };
    let name_part = &stmt["CREATE TABLE".len()..open];
    let name = norm_ident(name_part);
    if name.is_empty() {
        return Ok(None);
    }
    let close = match stmt.rfind(')') {
        Some(p) if p > open => p,
        _ => return Err(MigrateError::Parse(format!("unterminated CREATE TABLE {name}"))),
    };
    let body = &stmt[open + 1..close];

    let mut table = Table {
        name,
        ..Default::default()
    };
    for part in split_top_level(body) {
        let def = part.trim();
        if def.is_empty() {
            continue;
        }
        // Table-level constraints can appear inline; record what we care about
        // and skip the rest. Most plain dumps emit these as ALTER TABLE instead.
        let upper = def.to_ascii_uppercase();
        if upper.starts_with("CONSTRAINT")
            || upper.starts_with("PRIMARY KEY")
            || upper.starts_with("FOREIGN KEY")
            || upper.starts_with("UNIQUE")
            || upper.starts_with("CHECK")
        {
            if upper.contains("CHECK") {
                table.check_constraints += 1;
            }
            if let Some(cols) = upper.find("PRIMARY KEY").and(extract_paren_cols(def)) {
                table.primary_key = cols;
            }
            continue;
        }
        if let Some(col) = parse_column_def(def) {
            table.columns.push(col);
        }
    }
    Ok(Some(table))
}

/// Parse `id integer NOT NULL DEFAULT ...` into a column.
fn parse_column_def(def: &str) -> Option<Column> {
    let mut it = def.splitn(2, char::is_whitespace);
    let name = norm_ident(it.next()?);
    if name.is_empty() {
        return None;
    }
    let rest = it.next().unwrap_or("").trim();
    let not_null = rest.to_ascii_uppercase().contains("NOT NULL");
    let sql_type = extract_type(rest);
    Some(Column {
        name,
        sql_type,
        not_null,
    })
}

/// Pull the type out of a column definition's trailing text, stopping at the
/// first modifier keyword (NOT NULL, DEFAULT, ...). Keeps parenthesized size /
/// precision like `numeric(20,4)` or `character varying(255)`.
fn extract_type(rest: &str) -> String {
    let upper = rest.to_ascii_uppercase();
    let mut cut = rest.len();
    for kw in [" NOT NULL", " DEFAULT", " PRIMARY KEY", " UNIQUE", " REFERENCES", " CHECK", " COLLATE", " GENERATED"] {
        if let Some(p) = upper.find(kw) {
            cut = cut.min(p);
        }
    }
    rest[..cut].trim().to_ascii_lowercase()
}

// ---- COPY ----

/// Parse `public.users (id, name) FROM stdin;` -> (table, [id, name]).
fn parse_copy_header(rest: &str) -> MigrateResult<(String, Vec<String>)> {
    // Strip the trailing "FROM stdin;" (case-insensitive).
    let upper = rest.to_ascii_uppercase();
    let end = upper
        .find("FROM STDIN")
        .ok_or_else(|| MigrateError::Parse(format!("unsupported COPY: {rest}")))?;
    let head = rest[..end].trim();
    // head is `<table>` or `<table> (cols)`.
    if let Some(open) = head.find('(') {
        let table = norm_ident(&head[..open]);
        let close = head
            .rfind(')')
            .ok_or_else(|| MigrateError::Parse(format!("bad COPY column list: {head}")))?;
        let cols = head[open + 1..close]
            .split(',')
            .map(norm_ident)
            .filter(|s| !s.is_empty())
            .collect();
        Ok((table, cols))
    } else {
        Ok((norm_ident(head), Vec::new()))
    }
}

/// Decode one tab-separated COPY data line into column values. `\N` -> NULL.
fn parse_copy_row(line: &str) -> Vec<Option<String>> {
    line.split('\t').map(decode_copy_field).collect()
}

/// Decode a single COPY field, applying the text-format backslash escapes.
fn decode_copy_field(field: &str) -> Option<String> {
    if field == "\\N" {
        return None;
    }
    if !field.contains('\\') {
        return Some(field.to_string());
    }
    let mut out = String::with_capacity(field.len());
    let mut chars = field.chars();
    while let Some(c) = chars.next() {
        if c != '\\' {
            out.push(c);
            continue;
        }
        match chars.next() {
            Some('t') => out.push('\t'),
            Some('n') => out.push('\n'),
            Some('r') => out.push('\r'),
            Some('b') => out.push('\u{0008}'),
            Some('f') => out.push('\u{000C}'),
            Some('v') => out.push('\u{000B}'),
            Some('\\') => out.push('\\'),
            Some(other) => out.push(other),
            None => out.push('\\'),
        }
    }
    Some(out)
}

// ---- ALTER TABLE ... ADD CONSTRAINT ----

fn apply_alter_table(stmt: &str, tables: &mut [Table], index_of: &HashMap<String, usize>) {
    let upper = stmt.to_ascii_uppercase();
    let add = match upper.find("ADD CONSTRAINT") {
        Some(p) => p,
        None => return,
    };
    // Table name sits between "ALTER TABLE [ONLY]" and "ADD CONSTRAINT".
    let mut head = &stmt["ALTER TABLE".len()..add];
    if let Some(rest) = strip_kw(head.trim_start(), "ONLY ") {
        head = rest;
    }
    let table_name = norm_ident(head);
    let idx = match index_of.get(&table_name) {
        Some(&i) => i,
        None => return,
    };
    let t = &mut tables[idx];

    if let Some(p) = upper.find("PRIMARY KEY") {
        if let Some(cols) = extract_paren_cols(&stmt[p..]) {
            t.primary_key = cols;
        }
    } else if let Some(p) = upper.find("FOREIGN KEY") {
        if let Some(fk) = parse_foreign_key(&stmt[p..]) {
            t.foreign_keys.push(fk);
        }
    } else if let Some(p) = upper.find("UNIQUE") {
        if let Some(cols) = extract_paren_cols(&stmt[p..]) {
            t.unique.push(cols);
        }
    } else if upper[add..].contains("CHECK") {
        t.check_constraints += 1;
    }
}

/// Parse `FOREIGN KEY (a, b) REFERENCES public.other(x, y)`.
fn parse_foreign_key(s: &str) -> Option<ForeignKey> {
    let columns = extract_paren_cols(s)?;
    let upper = s.to_ascii_uppercase();
    let refpos = upper.find("REFERENCES")?;
    let after = &s[refpos + "REFERENCES".len()..];
    let open = after.find('(')?;
    let ref_table = norm_ident(&after[..open]);
    let ref_columns = extract_paren_cols(after).unwrap_or_default();
    Some(ForeignKey {
        columns,
        ref_table,
        ref_columns,
    })
}

// ---- CREATE INDEX ----

fn apply_create_index(stmt: &str, tables: &mut [Table], index_of: &HashMap<String, usize>) {
    // CREATE [UNIQUE] INDEX <name> ON <table> [USING ...] (<cols>);
    let upper = stmt.to_ascii_uppercase();
    let on = match upper.find(" ON ") {
        Some(p) => p,
        None => return,
    };
    let after_on = &stmt[on + 4..];
    // Table name runs until "USING" or the opening paren.
    let after_upper = after_on.to_ascii_uppercase();
    let stop = after_upper
        .find(" USING ")
        .or_else(|| after_on.find('('))
        .unwrap_or(after_on.len());
    let table_name = norm_ident(&after_on[..stop]);
    let idx = match index_of.get(&table_name) {
        Some(&i) => i,
        None => return,
    };
    // Only record plain-column indexes; expression indexes (containing a call)
    // are not a usable field signal.
    if let Some(cols) = extract_paren_cols(after_on) {
        if !cols.is_empty() && cols.iter().all(|c| is_plain_ident(c)) {
            tables[idx].indexed_columns.push(cols);
        }
    }
}

// ---- shared helpers ----

/// Case-insensitive keyword check at the start of a line.
fn is_kw(line: &str, kw: &str) -> bool {
    line.len() >= kw.len() && line[..kw.len()].eq_ignore_ascii_case(kw)
}

/// If `line` starts with `kw` (case-insensitive), return the remainder.
fn strip_kw<'a>(line: &'a str, kw: &str) -> Option<&'a str> {
    if is_kw(line, kw) {
        Some(&line[kw.len()..])
    } else {
        None
    }
}

/// Normalize an identifier: trim, drop trailing punctuation, strip a schema
/// qualifier, and remove surrounding double quotes. The schema split happens on
/// the last `.` that is **outside** double quotes, so a quoted identifier like
/// `public."My.Table"` resolves to `My.Table`, not `Table`.
fn norm_ident(s: &str) -> String {
    let mut s = s.trim();
    s = s.trim_end_matches([';', ',', '(']);
    s = s.trim();
    unquote_ident(last_unquoted_segment(s))
}

/// The segment after the last unquoted `.` (the table part of a qualified name).
fn last_unquoted_segment(s: &str) -> &str {
    let bytes = s.as_bytes();
    let mut in_quotes = false;
    let mut last_dot: Option<usize> = None;
    let mut i = 0;
    while i < bytes.len() {
        match bytes[i] {
            b'"' => {
                if in_quotes && bytes.get(i + 1) == Some(&b'"') {
                    i += 2; // escaped "" inside a quoted identifier
                    continue;
                }
                in_quotes = !in_quotes;
            }
            b'.' if !in_quotes => last_dot = Some(i),
            _ => {}
        }
        i += 1;
    }
    match last_dot {
        Some(p) => &s[p + 1..],
        None => s,
    }
}

/// Strip surrounding double quotes from one identifier segment, collapsing the
/// `""` escape to a single quote.
fn unquote_ident(s: &str) -> String {
    let s = s.trim();
    if s.len() >= 2 && s.starts_with('"') && s.ends_with('"') {
        s[1..s.len() - 1].replace("\"\"", "\"")
    } else {
        s.to_string()
    }
}

/// True if `s` looks like a bare column identifier (no function call / operator).
fn is_plain_ident(s: &str) -> bool {
    !s.is_empty()
        && s.chars()
            .all(|c| c.is_alphanumeric() || c == '_' || c == '"')
}

/// Extract the comma-separated identifiers from the first parenthesized group.
fn extract_paren_cols(s: &str) -> Option<Vec<String>> {
    let open = s.find('(')?;
    let close = matching_paren(s, open)?;
    let inner = &s[open + 1..close];
    let cols: Vec<String> = inner
        .split(',')
        .map(|c| {
            // Drop a trailing sort qualifier like "col DESC".
            let c = c.trim();
            let token = c.split_whitespace().next().unwrap_or(c);
            norm_ident(token)
        })
        .filter(|c| !c.is_empty())
        .collect();
    if cols.is_empty() {
        None
    } else {
        Some(cols)
    }
}

/// Index of the `)` matching the `(` at `open`.
fn matching_paren(s: &str, open: usize) -> Option<usize> {
    let bytes = s.as_bytes();
    let mut depth = 0i32;
    for (i, &b) in bytes.iter().enumerate().skip(open) {
        match b {
            b'(' => depth += 1,
            b')' => {
                depth -= 1;
                if depth == 0 {
                    return Some(i);
                }
            }
            _ => {}
        }
    }
    None
}

/// Split a comma-separated list, ignoring commas nested inside parentheses
/// (so `numeric(20,4)` stays one part).
fn split_top_level(s: &str) -> Vec<String> {
    let mut parts = Vec::new();
    let mut depth = 0i32;
    let mut cur = String::new();
    for c in s.chars() {
        match c {
            '(' => {
                depth += 1;
                cur.push(c);
            }
            ')' => {
                depth -= 1;
                cur.push(c);
            }
            ',' if depth == 0 => {
                parts.push(std::mem::take(&mut cur));
            }
            _ => cur.push(c),
        }
    }
    if !cur.trim().is_empty() {
        parts.push(cur);
    }
    parts
}

#[cfg(test)]
mod tests {
    use super::*;

    const DUMP: &str = r#"
SET statement_timeout = 0;

CREATE TABLE public.customers (
    id integer NOT NULL,
    name text NOT NULL,
    city character varying(255)
);

CREATE TABLE public.orders (
    id integer NOT NULL,
    customer_id integer NOT NULL,
    total numeric(20,4) NOT NULL,
    placed_at timestamp without time zone
);

COPY public.customers (id, name, city) FROM stdin;
1	Ada	London
2	Bo	\N
\.

COPY public.orders (id, customer_id, total, placed_at) FROM stdin;
10	1	19.9900	2021-01-01 00:00:00
11	1	5.0000	2021-02-01 00:00:00
12	2	100.0000	\N
\.

ALTER TABLE ONLY public.customers
    ADD CONSTRAINT customers_pkey PRIMARY KEY (id);

ALTER TABLE ONLY public.orders
    ADD CONSTRAINT orders_pkey PRIMARY KEY (id);

ALTER TABLE ONLY public.orders
    ADD CONSTRAINT orders_customer_id_fkey FOREIGN KEY (customer_id) REFERENCES public.customers(id);

CREATE INDEX idx_orders_customer ON public.orders USING btree (customer_id);
"#;

    #[test]
    fn parses_tables_columns_and_types() {
        let dump = parse(DUMP).unwrap();
        assert_eq!(dump.tables.len(), 2);
        let orders = dump.table("orders").unwrap();
        assert_eq!(orders.columns.len(), 4);
        assert_eq!(orders.column("total").unwrap().sql_type, "numeric(20,4)");
        assert!(orders.column("customer_id").unwrap().not_null);
        assert_eq!(
            orders.column("placed_at").unwrap().sql_type,
            "timestamp without time zone"
        );
    }

    #[test]
    fn parses_copy_rows_with_nulls() {
        let dump = parse(DUMP).unwrap();
        let customers = dump.table("customers").unwrap();
        assert_eq!(customers.rows.len(), 2);
        assert_eq!(customers.rows[0][1], Some("Ada".to_string()));
        assert_eq!(customers.rows[1][2], None); // \N
        let orders = dump.table("orders").unwrap();
        assert_eq!(orders.rows.len(), 3);
        assert_eq!(orders.rows[0][2], Some("19.9900".to_string()));
    }

    #[test]
    fn back_fills_constraints_and_indexes() {
        let dump = parse(DUMP).unwrap();
        let orders = dump.table("orders").unwrap();
        assert_eq!(orders.primary_key, vec!["id".to_string()]);
        assert_eq!(orders.foreign_keys.len(), 1);
        let fk = &orders.foreign_keys[0];
        assert_eq!(fk.columns, vec!["customer_id".to_string()]);
        assert_eq!(fk.ref_table, "customers");
        assert_eq!(fk.ref_columns, vec!["id".to_string()]);
        assert_eq!(orders.indexed_columns, vec![vec!["customer_id".to_string()]]);
    }

    #[test]
    fn copy_escapes_decoded() {
        assert_eq!(decode_copy_field("\\N"), None);
        assert_eq!(decode_copy_field("a\\tb"), Some("a\tb".to_string()));
        assert_eq!(decode_copy_field("line\\nbreak"), Some("line\nbreak".to_string()));
        assert_eq!(decode_copy_field("c:\\\\path"), Some("c:\\path".to_string()));
    }
}
