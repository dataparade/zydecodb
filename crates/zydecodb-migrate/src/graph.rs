//! Foreign-key graph annotated with real cardinalities sampled from the data.
//!
//! The schema tells us the *shape* of a relationship (which columns reference
//! which, and whether a unique constraint makes it one-to-one). The data tells
//! us its *magnitude* (how many children a parent actually has, on average and
//! at the worst case). The classifier needs both: a relationship that is
//! one-to-many by schema but caps at three children per parent embeds happily,
//! while one that fans out to tens of thousands must stay referenced. This
//! module produces that annotated graph and computes nothing about documents.

use crate::pgdump::{Dump, Table};
use std::collections::HashMap;

/// One referencing relationship: `child.fk_columns` -> `parent.parent_columns`.
#[derive(Debug, Clone)]
pub struct Relationship {
    pub child: String,
    pub parent: String,
    pub fk_columns: Vec<String>,
    pub parent_columns: Vec<String>,
    /// Mean children per parent (over parents that have at least one child).
    pub avg_fanout: f64,
    /// Largest number of children seen for a single parent.
    pub max_fanout: u64,
    /// The FK column set is unique in the child (PK or UNIQUE) -> one-to-one.
    pub one_to_one: bool,
    /// Every FK column is NOT NULL -> the child always has a parent.
    pub fk_not_null: bool,
}

/// Per-table facts the classifier consults alongside the relationships.
#[derive(Debug, Clone)]
pub struct TableStats {
    pub name: String,
    pub row_count: usize,
    pub fk_count: usize,
    /// A pure many-to-many connector (exactly two FKs, no real payload).
    pub is_join_table: bool,
}

/// The annotated graph.
#[derive(Debug, Clone, Default)]
pub struct Graph {
    pub relationships: Vec<Relationship>,
    pub tables: Vec<TableStats>,
}

impl Graph {
    pub fn table_stats(&self, name: &str) -> Option<&TableStats> {
        self.tables.iter().find(|t| t.name == name)
    }

    /// Relationships whose parent is `table` (i.e. `table`'s children).
    pub fn children_of<'a>(&'a self, table: &str) -> Vec<&'a Relationship> {
        self.relationships
            .iter()
            .filter(|r| r.parent == table)
            .collect()
    }
}

/// Build the annotated graph from a parsed dump.
pub fn build(dump: &Dump) -> Graph {
    let mut tables = Vec::with_capacity(dump.tables.len());
    for t in &dump.tables {
        tables.push(TableStats {
            name: t.name.clone(),
            row_count: t.rows.len(),
            fk_count: t.foreign_keys.len(),
            is_join_table: is_join_table(t),
        });
    }

    let mut relationships = Vec::new();
    for child in &dump.tables {
        for fk in &child.foreign_keys {
            // Skip dangling FKs whose target table isn't in the dump.
            if dump.table(&fk.ref_table).is_none() {
                continue;
            }
            let (avg, max) = fanout(child, &fk.columns);
            let one_to_one = is_unique_set(child, &fk.columns);
            let fk_not_null = fk
                .columns
                .iter()
                .all(|c| child.column(c).map(|col| col.not_null).unwrap_or(false));
            relationships.push(Relationship {
                child: child.name.clone(),
                parent: fk.ref_table.clone(),
                fk_columns: fk.columns.clone(),
                parent_columns: fk.ref_columns.clone(),
                avg_fanout: avg,
                max_fanout: max,
                one_to_one,
                fk_not_null,
            });
        }
    }

    Graph {
        relationships,
        tables,
    }
}

/// Mean and max children per parent, sampled from the child's rows. Rows with a
/// NULL in any FK column are ignored (they reference no parent).
fn fanout(child: &Table, fk_columns: &[String]) -> (f64, u64) {
    let idxs: Vec<usize> = match column_indexes(child, fk_columns) {
        Some(v) => v,
        None => return (0.0, 0),
    };
    let mut groups: HashMap<Vec<String>, u64> = HashMap::new();
    for row in &child.rows {
        let mut key = Vec::with_capacity(idxs.len());
        let mut has_null = false;
        for &i in &idxs {
            match row.get(i).and_then(|v| v.as_ref()) {
                Some(v) => key.push(v.clone()),
                None => {
                    has_null = true;
                    break;
                }
            }
        }
        if has_null {
            continue;
        }
        *groups.entry(key).or_insert(0) += 1;
    }
    if groups.is_empty() {
        return (0.0, 0);
    }
    let total: u64 = groups.values().sum();
    let max = groups.values().copied().max().unwrap_or(0);
    let avg = total as f64 / groups.len() as f64;
    (avg, max)
}

/// Column positions within the child's COPY column order.
fn column_indexes(table: &Table, names: &[String]) -> Option<Vec<usize>> {
    let mut out = Vec::with_capacity(names.len());
    for n in names {
        let pos = table.copy_columns.iter().position(|c| c == n)?;
        out.push(pos);
    }
    Some(out)
}

/// True if `columns` (as a set) is the primary key or a declared UNIQUE set.
fn is_unique_set(table: &Table, columns: &[String]) -> bool {
    let target = sorted(columns);
    if !table.primary_key.is_empty() && sorted(&table.primary_key) == target {
        return true;
    }
    table.unique.iter().any(|u| sorted(u) == target)
}

/// A join table is a pure connector: exactly two foreign keys and essentially
/// no payload of its own (at most one non-FK column, e.g. a surrogate id or a
/// created_at timestamp).
fn is_join_table(table: &Table) -> bool {
    if table.foreign_keys.len() != 2 {
        return false;
    }
    let mut fk_cols: Vec<&String> = Vec::new();
    for fk in &table.foreign_keys {
        for c in &fk.columns {
            fk_cols.push(c);
        }
    }
    let non_fk = table
        .columns
        .iter()
        .filter(|c| !fk_cols.contains(&&c.name))
        .count();
    non_fk <= 1
}

fn sorted(v: &[String]) -> Vec<String> {
    let mut v = v.to_vec();
    v.sort();
    v
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::pgdump;

    const DUMP: &str = r#"
CREATE TABLE public.customers (
    id integer NOT NULL,
    name text NOT NULL
);
CREATE TABLE public.orders (
    id integer NOT NULL,
    customer_id integer NOT NULL
);
CREATE TABLE public.profiles (
    id integer NOT NULL,
    customer_id integer NOT NULL
);
CREATE TABLE public.enrollment (
    student_id integer NOT NULL,
    course_id integer NOT NULL
);
CREATE TABLE public.students ( id integer NOT NULL );
CREATE TABLE public.courses ( id integer NOT NULL );

COPY public.customers (id, name) FROM stdin;
1	Ada
2	Bo
\.
COPY public.orders (id, customer_id) FROM stdin;
10	1
11	1
12	1
13	2
\.
COPY public.profiles (id, customer_id) FROM stdin;
100	1
101	2
\.
COPY public.enrollment (student_id, course_id) FROM stdin;
1	5
1	6
2	5
\.
COPY public.students (id) FROM stdin;
1
2
\.
COPY public.courses (id) FROM stdin;
5
6
\.

ALTER TABLE ONLY public.customers ADD CONSTRAINT customers_pkey PRIMARY KEY (id);
ALTER TABLE ONLY public.profiles ADD CONSTRAINT profiles_customer_key UNIQUE (customer_id);
ALTER TABLE ONLY public.orders ADD CONSTRAINT orders_customer_fkey FOREIGN KEY (customer_id) REFERENCES public.customers(id);
ALTER TABLE ONLY public.profiles ADD CONSTRAINT profiles_customer_fkey FOREIGN KEY (customer_id) REFERENCES public.customers(id);
ALTER TABLE ONLY public.enrollment ADD CONSTRAINT enrollment_student_fkey FOREIGN KEY (student_id) REFERENCES public.students(id);
ALTER TABLE ONLY public.enrollment ADD CONSTRAINT enrollment_course_fkey FOREIGN KEY (course_id) REFERENCES public.courses(id);
"#;

    fn graph() -> Graph {
        build(&pgdump::parse(DUMP).unwrap())
    }

    #[test]
    fn computes_fanout() {
        let g = graph();
        let orders = g
            .relationships
            .iter()
            .find(|r| r.child == "orders")
            .unwrap();
        // customer 1 has 3 orders, customer 2 has 1 -> max 3, avg 2.0.
        assert_eq!(orders.max_fanout, 3);
        assert!((orders.avg_fanout - 2.0).abs() < 1e-9);
        assert!(!orders.one_to_one);
        assert!(orders.fk_not_null);
    }

    #[test]
    fn detects_one_to_one_from_unique() {
        let g = graph();
        let profiles = g
            .relationships
            .iter()
            .find(|r| r.child == "profiles")
            .unwrap();
        assert!(profiles.one_to_one);
    }

    #[test]
    fn detects_join_table() {
        let g = graph();
        assert!(g.table_stats("enrollment").unwrap().is_join_table);
        assert!(!g.table_stats("orders").unwrap().is_join_table);
    }
}
