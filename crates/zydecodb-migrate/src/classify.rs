//! Classify every table into a document role and assemble the migration [`Plan`].
//!
//! The single rule, applied to the annotated graph:
//!
//! > Embed a child into its parent when it is owned by exactly one parent, the
//! > per-parent count is bounded and small, and nothing queries the child by its
//! > own fields. Reference (keep separate + index) when it is shared, unbounded,
//! > or independently queried.
//!
//! Two engine realities sharpen the rule:
//!
//! - There are no joins, so a "reference" the application reads through becomes a
//!   client round trip. We lean toward embedding owned/bounded children and, when
//!   an embedded child also points at a *shared* entity, we **snapshot** that
//!   entity's scalar columns into the child (point-in-time truth, and one fewer
//!   lookup at read time).
//! - Embedded arrays are not indexable, so a child the app queries by its own
//!   fields must stay a separate, indexed collection — embedding would make those
//!   queries unserviceable.
//!
//! When a child has several foreign keys we pick the *owning* parent with the
//! data, not the schema: the owner is the relationship with the smallest fan-out
//! (few children per parent), while large fan-out marks a shared entity to
//! reference/snapshot. This is the cardinality-driven judgment the whole tool
//! turns on.

use crate::graph::Graph;
use crate::pgdump::{Dump, Table};

/// Children counts at or below this per-parent maximum are considered bounded
/// and eligible to embed; anything larger stays a referenced collection.
pub const MAX_EMBED_FANOUT: u64 = 1000;

/// How a collection assigns each document's `_id`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum IdStrategy {
    /// Single simple primary-key column, stringified into `_id`.
    PrimaryKey(String),
    /// Composite or absent PK: generate a time-ordered `_id`; the PK columns are
    /// kept as fields and indexed so lookups still work.
    Generated,
}

/// A shared entity whose scalar columns are copied (snapshotted) into an
/// embedded child at migration time, under a nested object `field`.
#[derive(Debug, Clone)]
pub struct SnapshotSource {
    pub fk_columns: Vec<String>,
    pub ref_table: String,
    pub ref_columns: Vec<String>,
    pub field: String,
}

/// A child table folded into its parent document.
#[derive(Debug, Clone)]
pub struct EmbedPlan {
    /// Field name on the parent document holding the child(ren).
    pub field: String,
    pub child_table: String,
    pub fk_columns: Vec<String>,
    pub parent_columns: Vec<String>,
    /// One-to-one embeds as a single object; one-to-many as an array.
    pub one_to_one: bool,
    /// Shared entities snapshotted into each embedded child.
    pub snapshots: Vec<SnapshotSource>,
}

/// A many-to-many join table dissolved into an id-list field on the bounded
/// side entity.
#[derive(Debug, Clone)]
pub struct JoinDissolve {
    pub join_table: String,
    pub host_fk_columns: Vec<String>,
    pub other_table: String,
    pub other_fk_columns: Vec<String>,
    /// Field on the host document holding the array of related ids.
    pub field: String,
}

/// An index to create on a collection (also how the collection is created).
#[derive(Debug, Clone)]
pub struct IndexPlan {
    pub name: String,
    pub fields: Vec<String>,
    pub unique: bool,
}

/// A surviving collection and everything folded into it.
#[derive(Debug, Clone)]
pub struct CollectionPlan {
    pub name: String,
    pub id_strategy: IdStrategy,
    pub embeds: Vec<EmbedPlan>,
    pub join_dissolves: Vec<JoinDissolve>,
    pub indexes: Vec<IndexPlan>,
    /// Best-effort dependency rank; referenced parents sort before referrers.
    pub load_order: usize,
}

/// What Postgres was enforcing and what becomes of it.
#[derive(Debug, Clone, Default)]
pub struct DroppedReport {
    /// Unique constraints recreated as server-side unique indexes.
    pub preserved_unique: Vec<(String, Vec<String>)>,
    /// Unique constraints that could not be recreated (on embedded/dissolved
    /// tables, or composite spanning non-top-level fields) — now app's job.
    pub dropped_unique: Vec<(String, Vec<String>)>,
    /// NOT NULL columns (now the application's responsibility).
    pub not_null: Vec<(String, String)>,
    /// CHECK constraints dropped.
    pub check: usize,
    /// Foreign-key constraints (enforcement is now the application's job).
    pub foreign_keys: usize,
}

/// The full transformation plan.
#[derive(Debug, Clone, Default)]
pub struct Plan {
    pub collections: Vec<CollectionPlan>,
    pub embedded_tables: Vec<String>,
    pub join_tables: Vec<String>,
    pub dropped: DroppedReport,
    /// Human-readable decisions, surfaced in the pre-run preview.
    pub notes: Vec<String>,
}

impl Plan {
    pub fn collection(&self, name: &str) -> Option<&CollectionPlan> {
        self.collections.iter().find(|c| c.name == name)
    }
}

/// Resolved role of each table (internal to classification).
#[derive(Debug, Clone)]
enum Role {
    /// Survives as a collection.
    Collection,
    /// Embedded into `parent` via the owning relationship.
    Embedded { parent: String },
    /// Dissolved many-to-many connector.
    Join,
}

/// Classify the dump + graph into a [`Plan`].
pub fn classify(dump: &Dump, graph: &Graph) -> Plan {
    let mut plan = Plan::default();

    // --- Decide each table's role ---
    let mut roles: Vec<(String, Role)> = Vec::new();
    for t in &dump.tables {
        let role = decide_role(t, dump, graph);
        roles.push((t.name.clone(), role));
    }
    let role_of =
        |name: &str| -> Option<&Role> { roles.iter().find(|(n, _)| n == name).map(|(_, r)| r) };

    // --- Build collections for surviving tables ---
    for t in &dump.tables {
        match role_of(&t.name) {
            Some(Role::Embedded { parent }) => {
                plan.embedded_tables.push(t.name.clone());
                plan.notes.push(format!(
                    "embed: {} -> {} ({})",
                    t.name,
                    parent,
                    embed_shape(t, graph, parent)
                ));
            }
            Some(Role::Join) => {
                plan.join_tables.push(t.name.clone());
            }
            _ => {}
        }
    }

    for t in &dump.tables {
        if !matches!(role_of(&t.name), Some(Role::Collection)) {
            continue;
        }
        let mut coll = CollectionPlan {
            name: t.name.clone(),
            id_strategy: id_strategy(t),
            embeds: Vec::new(),
            join_dissolves: Vec::new(),
            indexes: Vec::new(),
            load_order: 0,
        };

        // Embeds: children whose owning parent is this table and were embedded.
        for rel in graph.children_of(&t.name) {
            if let Some(Role::Embedded { parent }) = role_of(&rel.child) {
                if parent == &t.name {
                    let child = match dump.table(&rel.child) {
                        Some(c) => c,
                        None => continue,
                    };
                    coll.embeds.push(EmbedPlan {
                        field: rel.child.clone(),
                        child_table: rel.child.clone(),
                        fk_columns: rel.fk_columns.clone(),
                        parent_columns: rel.parent_columns.clone(),
                        one_to_one: rel.one_to_one,
                        snapshots: snapshot_sources(child, graph, &rel.parent),
                    });
                }
            }
        }

        // Reference relationships: children kept separate need an FK index here.
        for rel in &graph.relationships {
            if rel.child == t.name {
                let child_kept = matches!(role_of(&rel.child), Some(Role::Collection));
                let owns = is_owning_relationship(dump, graph, &rel.child, &rel.parent);
                if child_kept && owns {
                    coll.indexes.push(IndexPlan {
                        name: index_name(&rel.fk_columns),
                        fields: rel.fk_columns.clone(),
                        unique: false,
                    });
                }
            }
        }

        // Recreate Postgres indexes (the "queried by these columns" signal).
        for cols in &t.indexed_columns {
            if all_top_level(t, cols) {
                push_unique_index(&mut coll.indexes, index_name(cols), cols.clone(), false);
            }
        }

        // Unique constraints -> unique indexes when all columns survive as
        // top-level scalar fields; otherwise report them as dropped.
        for u in &t.unique {
            if all_top_level(t, u) {
                push_unique_index(&mut coll.indexes, unique_index_name(u), u.clone(), true);
                plan.dropped
                    .preserved_unique
                    .push((t.name.clone(), u.clone()));
            } else {
                plan.dropped
                    .dropped_unique
                    .push((t.name.clone(), u.clone()));
            }
        }

        // Join-table dissolution: attach an id-list field to the bounded host.
        for jt in &dump.tables {
            if !matches!(role_of(&jt.name), Some(Role::Join)) {
                continue;
            }
            if let Some(dissolve) = join_dissolve_for_host(jt, &t.name, graph) {
                coll.join_dissolves.push(dissolve);
            }
        }

        plan.collections.push(coll);
    }

    ensure_creatable_indexes(&mut plan);
    assign_load_order(&mut plan, graph);
    fill_dropped_report(&mut plan, dump);
    plan
}

/// Decide the role of one table.
fn decide_role(t: &Table, _dump: &Dump, graph: &Graph) -> Role {
    if graph
        .table_stats(&t.name)
        .map(|s| s.is_join_table)
        .unwrap_or(false)
    {
        return Role::Join;
    }
    // The owning relationship (smallest fan-out among this table's FKs).
    let owner = owning_relationship(graph, &t.name);
    let owner = match owner {
        Some(o) => o,
        None => return Role::Collection, // a root: nothing it is owned by
    };

    // Owned by exactly one parent (the others, if any, are shared references).
    // Nobody may reference this child, or embedding would orphan those refs.
    if referenced_by_count(graph, &t.name) > 0 {
        return Role::Collection;
    }
    // Bounded per-parent count (one-to-one always qualifies).
    let bounded = owner.one_to_one || owner.max_fanout <= MAX_EMBED_FANOUT;
    if !bounded {
        return Role::Collection;
    }
    // Not independently queried by its own fields.
    if has_independent_index(t) {
        return Role::Collection;
    }
    Role::Embedded {
        parent: owner.parent.clone(),
    }
}

/// The owning relationship for `child`: among its foreign keys (to in-dump
/// tables), the one with the smallest observed fan-out. Few-children-per-parent
/// marks ownership; large fan-out marks a shared entity.
fn owning_relationship<'a>(
    graph: &'a Graph,
    child: &str,
) -> Option<&'a crate::graph::Relationship> {
    graph
        .relationships
        .iter()
        .filter(|r| r.child == child)
        .min_by(|a, b| {
            a.max_fanout.cmp(&b.max_fanout).then(
                a.avg_fanout
                    .partial_cmp(&b.avg_fanout)
                    .unwrap_or(std::cmp::Ordering::Equal),
            )
        })
}

fn is_owning_relationship(_dump: &Dump, graph: &Graph, child: &str, parent: &str) -> bool {
    owning_relationship(graph, child)
        .map(|o| o.parent == parent)
        .unwrap_or(false)
}

/// Shared entities (non-owning FKs) to snapshot into an embedded child.
fn snapshot_sources(child: &Table, graph: &Graph, owning_parent: &str) -> Vec<SnapshotSource> {
    let mut out = Vec::new();
    for fk in &child.foreign_keys {
        if fk.ref_table == owning_parent {
            continue; // the owning parent, not a snapshot source
        }
        // Only snapshot from tables that survive as collections (shared refs).
        let is_owner = owning_relationship(graph, &child.name)
            .map(|o| o.parent == fk.ref_table)
            .unwrap_or(false);
        if is_owner {
            continue;
        }
        out.push(SnapshotSource {
            fk_columns: fk.columns.clone(),
            ref_table: fk.ref_table.clone(),
            ref_columns: fk.ref_columns.clone(),
            field: fk.ref_table.clone(),
        });
    }
    out
}

/// Number of distinct tables that reference `table`.
fn referenced_by_count(graph: &Graph, table: &str) -> usize {
    let mut seen: Vec<&str> = Vec::new();
    for r in &graph.relationships {
        if r.parent == table && !seen.contains(&r.child.as_str()) {
            seen.push(&r.child);
        }
    }
    seen.len()
}

/// True if the table has a Postgres index on a column set that is neither a
/// foreign key nor its primary key — i.e. the application queries it by its own
/// fields, so it must stay separately queryable.
fn has_independent_index(t: &Table) -> bool {
    for cols in &t.indexed_columns {
        let is_pk = sorted(cols) == sorted(&t.primary_key);
        let is_fk = t
            .foreign_keys
            .iter()
            .any(|fk| sorted(&fk.columns) == sorted(cols));
        if !is_pk && !is_fk {
            return true;
        }
    }
    false
}

/// Choose `_id` strategy from the primary key.
fn id_strategy(t: &Table) -> IdStrategy {
    if t.primary_key.len() == 1 {
        IdStrategy::PrimaryKey(t.primary_key[0].clone())
    } else {
        IdStrategy::Generated
    }
}

/// Build a join-table dissolve if `host` is the bounded side of `jt`.
fn join_dissolve_for_host(jt: &Table, host: &str, graph: &Graph) -> Option<JoinDissolve> {
    if jt.foreign_keys.len() != 2 {
        return None;
    }
    let fk_host = jt.foreign_keys.iter().find(|f| f.ref_table == host)?;
    let fk_other = jt.foreign_keys.iter().find(|f| f.ref_table != host)?;

    // Host is the bounded side: smaller fan-out (fewer links per host row).
    let host_fanout = rel_fanout(graph, &jt.name, host);
    let other_fanout = rel_fanout(graph, &jt.name, &fk_other.ref_table);
    if host_fanout > other_fanout {
        return None; // the other side is the bounded host
    }
    Some(JoinDissolve {
        join_table: jt.name.clone(),
        host_fk_columns: fk_host.columns.clone(),
        other_table: fk_other.ref_table.clone(),
        other_fk_columns: fk_other.columns.clone(),
        field: format!("{}_ids", singular(&fk_other.ref_table)),
    })
}

fn rel_fanout(graph: &Graph, child: &str, parent: &str) -> u64 {
    graph
        .relationships
        .iter()
        .find(|r| r.child == child && r.parent == parent)
        .map(|r| r.max_fanout)
        .unwrap_or(0)
}

fn embed_shape(t: &Table, graph: &Graph, parent: &str) -> &'static str {
    let one_to_one = graph
        .relationships
        .iter()
        .find(|r| r.child == t.name && r.parent == parent)
        .map(|r| r.one_to_one)
        .unwrap_or(false);
    if one_to_one {
        "object"
    } else {
        "array"
    }
}

/// Make sure every collection has at least one index, since `IndexDef` is the
/// only command that creates a collection server-side. Falls back to `_id`.
fn ensure_creatable_indexes(plan: &mut Plan) {
    for coll in &mut plan.collections {
        if coll.indexes.is_empty() {
            coll.indexes.push(IndexPlan {
                name: "by__id".to_string(),
                fields: vec!["_id".to_string()],
                unique: false,
            });
        }
    }
}

/// Best-effort topological-ish rank: a collection that references another sorts
/// after it. Cycles fall back to declaration order.
fn assign_load_order(plan: &mut Plan, graph: &Graph) {
    let names: Vec<String> = plan.collections.iter().map(|c| c.name.clone()).collect();
    for coll in &mut plan.collections {
        // Rank = number of distinct surviving parents this collection references.
        let mut parents: Vec<&str> = Vec::new();
        for r in &graph.relationships {
            if r.child == coll.name
                && names.contains(&r.parent)
                && !parents.contains(&r.parent.as_str())
            {
                parents.push(&r.parent);
            }
        }
        coll.load_order = parents.len();
    }
    plan.collections
        .sort_by(|a, b| a.load_order.cmp(&b.load_order).then(a.name.cmp(&b.name)));
}

/// Gather the not-null / check / fk facts for the dropped-constraints report.
fn fill_dropped_report(plan: &mut Plan, dump: &Dump) {
    let mut fk = 0usize;
    let mut check = 0usize;
    for t in &dump.tables {
        fk += t.foreign_keys.len();
        check += t.check_constraints;
        // Only report not-null for columns that survive as queryable fields,
        // i.e. on tables that became collections.
        if plan.collection(&t.name).is_some() {
            for c in &t.columns {
                if c.not_null && !t.primary_key.contains(&c.name) {
                    plan.dropped.not_null.push((t.name.clone(), c.name.clone()));
                }
            }
        }
    }
    plan.dropped.foreign_keys = fk;
    plan.dropped.check = check;
}

// ---- small helpers ----

fn all_top_level(t: &Table, cols: &[String]) -> bool {
    cols.iter().all(|c| t.column(c).is_some())
}

fn push_unique_index(into: &mut Vec<IndexPlan>, name: String, fields: Vec<String>, unique: bool) {
    if into.iter().any(|i| i.fields == fields) {
        // Upgrade an existing non-unique index to unique if needed.
        if unique {
            if let Some(existing) = into.iter_mut().find(|i| i.fields == fields) {
                existing.unique = true;
            }
        }
        return;
    }
    into.push(IndexPlan {
        name,
        fields,
        unique,
    });
}

fn index_name(cols: &[String]) -> String {
    format!("by_{}", cols.join("_"))
}

fn unique_index_name(cols: &[String]) -> String {
    format!("uniq_{}", cols.join("_"))
}

fn singular(name: &str) -> String {
    name.strip_suffix('s').unwrap_or(name).to_string()
}

fn sorted(v: &[String]) -> Vec<String> {
    let mut v = v.to_vec();
    v.sort();
    v
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{graph, pgdump};

    const SHOP: &str = r#"
CREATE TABLE public.customers (
    id integer NOT NULL,
    email text NOT NULL,
    name text NOT NULL
);
CREATE TABLE public.products (
    id integer NOT NULL,
    name text NOT NULL,
    price numeric(10,2) NOT NULL
);
CREATE TABLE public.orders (
    id integer NOT NULL,
    customer_id integer NOT NULL,
    placed_at timestamp without time zone
);
CREATE TABLE public.line_items (
    id integer NOT NULL,
    order_id integer NOT NULL,
    product_id integer NOT NULL,
    qty integer NOT NULL
);

COPY public.customers (id, email, name) FROM stdin;
1	ada@x.io	Ada
2	bo@x.io	Bo
\.
COPY public.products (id, name, price) FROM stdin;
50	Widget	9.99
51	Gadget	19.99
\.
COPY public.orders (id, customer_id, placed_at) FROM stdin;
1000	1	2021-01-01 00:00:00
1001	1	2021-02-01 00:00:00
1002	2	2021-03-01 00:00:00
\.
COPY public.line_items (id, order_id, product_id, qty) FROM stdin;
1	1000	50	2
2	1000	51	1
3	1001	50	5
4	1002	51	3
\.

ALTER TABLE ONLY public.customers ADD CONSTRAINT customers_pkey PRIMARY KEY (id);
ALTER TABLE ONLY public.customers ADD CONSTRAINT customers_email_key UNIQUE (email);
ALTER TABLE ONLY public.products ADD CONSTRAINT products_pkey PRIMARY KEY (id);
ALTER TABLE ONLY public.orders ADD CONSTRAINT orders_pkey PRIMARY KEY (id);
ALTER TABLE ONLY public.line_items ADD CONSTRAINT line_items_pkey PRIMARY KEY (id);
ALTER TABLE ONLY public.orders ADD CONSTRAINT orders_customer_fkey FOREIGN KEY (customer_id) REFERENCES public.customers(id);
ALTER TABLE ONLY public.line_items ADD CONSTRAINT li_order_fkey FOREIGN KEY (order_id) REFERENCES public.orders(id);
ALTER TABLE ONLY public.line_items ADD CONSTRAINT li_product_fkey FOREIGN KEY (product_id) REFERENCES public.products(id);
"#;

    fn plan() -> Plan {
        let dump = pgdump::parse(SHOP).unwrap();
        let g = graph::build(&dump);
        classify(&dump, &g)
    }

    #[test]
    fn line_items_embed_into_orders() {
        let p = plan();
        assert!(p.embedded_tables.contains(&"line_items".to_string()));
        let orders = p.collection("orders").expect("orders survives");
        assert_eq!(orders.embeds.len(), 1);
        let embed = &orders.embeds[0];
        assert_eq!(embed.child_table, "line_items");
        assert!(!embed.one_to_one);
        // line_items references products (shared) -> snapshot it.
        assert_eq!(embed.snapshots.len(), 1);
        assert_eq!(embed.snapshots[0].ref_table, "products");
    }

    #[test]
    fn customers_and_products_survive_as_collections() {
        let p = plan();
        assert!(p.collection("customers").is_some());
        assert!(p.collection("products").is_some());
        assert!(p.collection("orders").is_some());
        // line_items folded away.
        assert!(p.collection("line_items").is_none());
    }

    #[test]
    fn orders_indexed_on_customer_fk() {
        let p = plan();
        let orders = p.collection("orders").unwrap();
        assert!(orders
            .indexes
            .iter()
            .any(|i| i.fields == vec!["customer_id".to_string()] && !i.unique));
    }

    #[test]
    fn unique_email_preserved_as_unique_index() {
        let p = plan();
        let customers = p.collection("customers").unwrap();
        assert!(customers
            .indexes
            .iter()
            .any(|i| i.fields == vec!["email".to_string()] && i.unique));
        assert!(p
            .dropped
            .preserved_unique
            .iter()
            .any(|(t, c)| t == "customers" && c == &vec!["email".to_string()]));
    }

    #[test]
    fn id_strategy_uses_single_pk() {
        let p = plan();
        let orders = p.collection("orders").unwrap();
        assert_eq!(orders.id_strategy, IdStrategy::PrimaryKey("id".to_string()));
    }
}
