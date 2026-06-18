//! Adversarial parser fixtures: malformed-for-us-but-valid `pg_dump` shapes that
//! the happy-path tests never exercise. Each targets one concrete weakness.
//!
//! Policy (chosen with the operator): constructs we genuinely cannot represent
//! fail loudly, naming the offending construct, instead of silently producing
//! wrong documents. Constructs we *can* parse correctly (quoted semicolons) are
//! parsed correctly.

use serde_json::json;
use zydecodb_migrate::{classify, convert, graph, pgdump};

/// A dump produced with `pg_dump --inserts` carries data as `INSERT INTO`
/// statements, not `COPY`. The old parser ignored them and emitted zero rows —
/// a silently empty migration. It must now refuse loudly.
#[test]
fn insert_style_dump_is_rejected_loudly() {
    let dump = r#"
CREATE TABLE public.users (
    id integer NOT NULL,
    name text NOT NULL
);
INSERT INTO public.users (id, name) VALUES (1, 'Ada');
INSERT INTO public.users (id, name) VALUES (2, 'Bo');
ALTER TABLE ONLY public.users ADD CONSTRAINT users_pkey PRIMARY KEY (id);
"#;
    let err = pgdump::parse(dump).expect_err("must reject --inserts dumps");
    let msg = err.to_string().to_lowercase();
    assert!(msg.contains("insert"), "error should name INSERT: {msg}");
    assert!(
        msg.contains("copy"),
        "error should point at COPY format: {msg}"
    );
}

/// Two tables with the same name in different schemas both normalize to the bare
/// name and would clobber each other (and mis-route their COPY data). That is
/// silent corruption, so it must be refused with the colliding name.
#[test]
fn cross_schema_name_collision_is_rejected_loudly() {
    let dump = r#"
CREATE TABLE public.users (
    id integer NOT NULL,
    email text NOT NULL
);
CREATE TABLE billing.users (
    id integer NOT NULL,
    plan text NOT NULL
);
"#;
    let err = pgdump::parse(dump).expect_err("must reject cross-schema collisions");
    let msg = err.to_string().to_lowercase();
    assert!(msg.contains("users"), "error should name the table: {msg}");
    assert!(
        msg.contains("schema") || msg.contains("collision"),
        "error should explain the collision: {msg}"
    );
}

/// Distinct table names across schemas are fine — only true collisions fail.
#[test]
fn distinct_tables_across_schemas_are_accepted() {
    let dump = r#"
CREATE TABLE public.users (
    id integer NOT NULL,
    email text NOT NULL
);
CREATE TABLE billing.invoices (
    id integer NOT NULL,
    amount integer NOT NULL
);
"#;
    let parsed = pgdump::parse(dump).expect("distinct names parse");
    assert!(parsed.table("users").is_some());
    assert!(parsed.table("invoices").is_some());
}

/// A column default whose string literal ends a physical line with `;` used to
/// truncate the statement at the wrong place. A quote-aware scanner must treat
/// that `;` as data and keep reading to the real terminator.
#[test]
fn semicolon_inside_string_default_does_not_truncate() {
    let dump = r#"
CREATE TABLE public.notes (
    id integer NOT NULL,
    tmpl text DEFAULT 'line one;
second line' NOT NULL,
    author text NOT NULL
);
ALTER TABLE ONLY public.notes ADD CONSTRAINT notes_pkey PRIMARY KEY (id);
"#;
    let parsed = pgdump::parse(dump).expect("multi-line string default parses");
    let notes = parsed.table("notes").expect("notes table parsed");
    // All three columns must survive — truncation would drop `author`.
    assert!(notes.column("id").is_some());
    assert!(notes.column("tmpl").is_some());
    assert!(notes.column("author").is_some());
    assert_eq!(notes.primary_key, vec!["id".to_string()]);
}

/// A semicolon living inside a CHECK constraint's string literal (single line)
/// must not confuse the terminator either.
#[test]
fn semicolon_inside_check_literal_is_fine() {
    let dump = r#"
CREATE TABLE public.t (
    id integer NOT NULL,
    sep text NOT NULL
);
ALTER TABLE ONLY public.t ADD CONSTRAINT t_sep_chk CHECK (sep <> ';');
ALTER TABLE ONLY public.t ADD CONSTRAINT t_pkey PRIMARY KEY (id);
"#;
    let parsed = pgdump::parse(dump).expect("check with quoted semicolon parses");
    let t = parsed.table("t").expect("table t parsed");
    assert_eq!(t.primary_key, vec!["id".to_string()]);
    assert_eq!(t.check_constraints, 1);
}

// ---------------------------------------------------------------------------
// Hole 1: array values containing quoted commas / escapes / NULLs.
// `convert_array` split naively on every comma, shredding quoted elements.
// ---------------------------------------------------------------------------

#[test]
fn array_elements_with_quoted_commas_stay_intact() {
    // Postgres quotes elements that contain commas/spaces; the comma is data.
    assert_eq!(
        convert::convert_scalar("text[]", r#"{"a,b","c"}"#),
        json!(["a,b", "c"])
    );
}

#[test]
fn array_elements_with_escaped_quotes_and_backslashes() {
    // Inside a quoted array element, `\"` is a literal quote and `\\` a literal
    // backslash.
    assert_eq!(
        convert::convert_scalar("text[]", r#"{"say \"hi\"","c:\\tmp"}"#),
        json!(["say \"hi\"", "c:\\tmp"])
    );
}

#[test]
fn array_unquoted_null_element_is_json_null() {
    // Unquoted NULL is the SQL null; the quoted string "NULL" is just text.
    assert_eq!(
        convert::convert_scalar("text[]", r#"{a,NULL,"NULL"}"#),
        json!(["a", null, "NULL"])
    );
}

#[test]
fn empty_and_numeric_arrays() {
    assert_eq!(convert::convert_scalar("integer[]", "{}"), json!([]));
    assert_eq!(
        convert::convert_scalar("integer[]", "{1,2,3}"),
        json!([1, 2, 3])
    );
}

// ---------------------------------------------------------------------------
// Hole 2: partitioned / inherited tables. We cannot faithfully reshape these,
// so refuse loudly naming the construct rather than emit a garbage table.
// ---------------------------------------------------------------------------

#[test]
fn partitioned_parent_table_is_rejected() {
    let dump = r#"
CREATE TABLE public.measurement (
    id integer NOT NULL,
    logdate date NOT NULL
) PARTITION BY RANGE (logdate);
"#;
    let err = pgdump::parse(dump).expect_err("partitioned parent must be rejected");
    assert!(
        err.to_string().to_lowercase().contains("partition"),
        "error should name partitioning: {err}"
    );
}

#[test]
fn partition_child_table_is_rejected() {
    let dump = r#"
CREATE TABLE public.measurement_y2020 PARTITION OF public.measurement
    FOR VALUES FROM ('2020-01-01') TO ('2021-01-01');
"#;
    let err = pgdump::parse(dump).expect_err("partition child must be rejected");
    assert!(
        err.to_string().to_lowercase().contains("partition"),
        "error should name partitioning: {err}"
    );
}

#[test]
fn inherited_table_is_rejected() {
    let dump = r#"
CREATE TABLE public.capitals (
    state char(2)
) INHERITS (public.cities);
"#;
    let err = pgdump::parse(dump).expect_err("table inheritance must be rejected");
    assert!(
        err.to_string().to_lowercase().contains("inherit"),
        "error should name inheritance: {err}"
    );
}

// ---------------------------------------------------------------------------
// Hole 3: quoted identifiers containing dots. `norm_ident` split on every dot,
// so `public."My.Table"` became `Table` (and would never match its COPY/FKs).
// ---------------------------------------------------------------------------

#[test]
fn quoted_identifier_with_dot_is_preserved_and_matched() {
    let dump = r#"
CREATE TABLE public."My.Table" (
    id integer NOT NULL,
    name text NOT NULL
);
COPY public."My.Table" (id, name) FROM stdin;
1	Ada
\.
ALTER TABLE ONLY public."My.Table" ADD CONSTRAINT mt_pkey PRIMARY KEY (id);
"#;
    let parsed = pgdump::parse(dump).expect("quoted dotted identifier parses");
    let t = parsed
        .table("My.Table")
        .expect("table keyed by its real name");
    assert_eq!(t.columns.len(), 2);
    // COPY data routed to the same table (name matched).
    assert_eq!(t.rows.len(), 1);
    assert_eq!(t.primary_key, vec!["id".to_string()]);
}

// ---------------------------------------------------------------------------
// Hole 4: generated/identity columns, ON DELETE tails, self/circular FKs.
// These were assumed-fine; prove it (and don't hang on cycles).
// ---------------------------------------------------------------------------

#[test]
fn generated_identity_and_stored_columns_parse() {
    let dump = r#"
CREATE TABLE public.invoices (
    id integer GENERATED ALWAYS AS IDENTITY NOT NULL,
    qty integer NOT NULL,
    price numeric(10,2) NOT NULL,
    total numeric(10,2) GENERATED ALWAYS AS ((qty * price)) STORED
);
"#;
    let parsed = pgdump::parse(dump).expect("generated columns parse");
    let inv = parsed.table("invoices").expect("invoices parsed");
    // Type is extracted, modifiers stripped.
    assert_eq!(inv.column("id").unwrap().sql_type, "integer");
    assert!(inv.column("id").unwrap().not_null);
    assert_eq!(inv.column("total").unwrap().sql_type, "numeric(10,2)");
    // All four columns survive the expression parens in `total`.
    assert_eq!(inv.columns.len(), 4);
}

#[test]
fn foreign_key_with_on_delete_clause_parses_cleanly() {
    let dump = r#"
CREATE TABLE public.customers ( id integer NOT NULL );
CREATE TABLE public.orders (
    id integer NOT NULL,
    customer_id integer NOT NULL
);
ALTER TABLE ONLY public.customers ADD CONSTRAINT customers_pkey PRIMARY KEY (id);
ALTER TABLE ONLY public.orders ADD CONSTRAINT orders_customer_fkey FOREIGN KEY (customer_id) REFERENCES public.customers(id) ON DELETE CASCADE ON UPDATE RESTRICT;
"#;
    let parsed = pgdump::parse(dump).expect("fk with action clauses parses");
    let orders = parsed.table("orders").unwrap();
    assert_eq!(orders.foreign_keys.len(), 1);
    let fk = &orders.foreign_keys[0];
    assert_eq!(fk.columns, vec!["customer_id".to_string()]);
    assert_eq!(fk.ref_table, "customers");
    assert_eq!(fk.ref_columns, vec!["id".to_string()]);
}

#[test]
fn self_referential_fk_stays_a_collection() {
    let dump = r#"
CREATE TABLE public.employees (
    id integer NOT NULL,
    manager_id integer
);
COPY public.employees (id, manager_id) FROM stdin;
1	\N
2	1
3	1
\.
ALTER TABLE ONLY public.employees ADD CONSTRAINT employees_pkey PRIMARY KEY (id);
ALTER TABLE ONLY public.employees ADD CONSTRAINT emp_mgr_fkey FOREIGN KEY (manager_id) REFERENCES public.employees(id);
"#;
    let parsed = pgdump::parse(dump).unwrap();
    let g = graph::build(&parsed);
    let plan = classify::classify(&parsed, &g);
    // A self-referencing table cannot embed into itself; it survives.
    let emp = plan.collection("employees").expect("employees survives");
    assert!(emp
        .indexes
        .iter()
        .any(|i| i.fields == vec!["manager_id".to_string()]));
}

#[test]
fn circular_foreign_keys_terminate_with_both_collections() {
    let dump = r#"
CREATE TABLE public.a (
    id integer NOT NULL,
    b_id integer NOT NULL
);
CREATE TABLE public.b (
    id integer NOT NULL,
    a_id integer NOT NULL
);
ALTER TABLE ONLY public.a ADD CONSTRAINT a_pkey PRIMARY KEY (id);
ALTER TABLE ONLY public.b ADD CONSTRAINT b_pkey PRIMARY KEY (id);
ALTER TABLE ONLY public.a ADD CONSTRAINT a_b_fkey FOREIGN KEY (b_id) REFERENCES public.b(id);
ALTER TABLE ONLY public.b ADD CONSTRAINT b_a_fkey FOREIGN KEY (a_id) REFERENCES public.a(id);
"#;
    let parsed = pgdump::parse(dump).unwrap();
    let g = graph::build(&parsed);
    let plan = classify::classify(&parsed, &g); // must not loop forever
    assert!(plan.collection("a").is_some());
    assert!(plan.collection("b").is_some());
}
