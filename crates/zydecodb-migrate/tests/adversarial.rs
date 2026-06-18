//! Adversarial parser fixtures: malformed-for-us-but-valid `pg_dump` shapes that
//! the happy-path tests never exercise. Each targets one concrete weakness.
//!
//! Policy (chosen with the operator): constructs we genuinely cannot represent
//! fail loudly, naming the offending construct, instead of silently producing
//! wrong documents. Constructs we *can* parse correctly (quoted semicolons) are
//! parsed correctly.

use zydecodb_migrate::pgdump;

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
    assert!(msg.contains("copy"), "error should point at COPY format: {msg}");
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
