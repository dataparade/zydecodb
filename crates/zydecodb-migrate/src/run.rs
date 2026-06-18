//! End-to-end orchestration for `zydecodb migrate`.
//!
//! Everything before the prompt is offline analysis against the dump file: no
//! socket is opened until the operator confirms. The executor assumes a fresh,
//! empty target (the documented requirement) and guards it: if any target
//! collection already holds rows, the run aborts before writing anything. A
//! failed run is recoverable by wiping the empty database and rerunning, and
//! every write is an idempotent upsert by a deterministic `_id`, so even a
//! partial run replays cleanly.

use crate::classify::{self, CollectionPlan, Plan};
use crate::client::Client;
use crate::convert;
use crate::error::{MigrateError, MigrateResult};
use crate::{graph, pgdump};
use std::io::Write;
use std::path::PathBuf;

/// Options for a migration run (assembled from CLI flags by the binary).
pub struct MigrateOptions {
    pub file: PathBuf,
    pub url: String,
    pub api_key: Option<String>,
    /// Skip the interactive confirmation (non-interactive runs).
    pub assume_yes: bool,
}

/// Parse, classify, preview, confirm, then load into the target server.
pub fn run(opts: MigrateOptions) -> MigrateResult<()> {
    let contents = std::fs::read_to_string(&opts.file)
        .map_err(|e| MigrateError::Io(format!("reading {}: {e}", opts.file.display())))?;

    let dump = pgdump::parse(&contents)?;
    if dump.tables.is_empty() {
        return Err(MigrateError::Parse(
            "no tables found in dump (is this a plain pg_dump with COPY data?)".into(),
        ));
    }
    let graph = graph::build(&dump);
    let plan = classify::classify(&dump, &graph);

    print_preview(&dump, &plan);

    if !opts.assume_yes {
        print!("\nPress enter to start the migration (Ctrl-C to abort): ");
        std::io::stdout().flush().ok();
        let mut line = String::new();
        std::io::stdin()
            .read_line(&mut line)
            .map_err(|e| MigrateError::Io(e.to_string()))?;
    }

    println!("\nConnecting to {} ...", opts.url);
    let mut client = Client::connect(&opts.url, opts.api_key.as_deref())?;
    client.ping()?;

    guard_empty(&mut client, &plan)?;
    execute(&mut client, &dump, &plan)?;

    print_summary(&plan);
    Ok(())
}

/// Abort if any target collection already holds documents.
fn guard_empty(client: &mut Client, plan: &Plan) -> MigrateResult<()> {
    for coll in &plan.collections {
        if client.collection_has_rows(&coll.name)? {
            return Err(MigrateError::NotEmpty(format!(
                "collection '{}' already has documents; the migrator only loads a fresh database",
                coll.name
            )));
        }
    }
    Ok(())
}

/// Create indexes (which creates the collection) then load documents, in the
/// plan's dependency order.
fn execute(client: &mut Client, dump: &pgdump::Dump, plan: &Plan) -> MigrateResult<()> {
    for coll in &plan.collections {
        create_indexes(client, coll)?;

        let docs = convert::build_collection_docs(dump, coll)?;
        let total = docs.len();
        for (i, doc) in docs.iter().enumerate() {
            client.put_document(&coll.name, &doc.id, &doc.body)?;
            if total >= 1000 && (i + 1) % 1000 == 0 {
                println!("  {} : {}/{}", coll.name, i + 1, total);
            }
        }
        println!("  {} : {} document(s)", coll.name, total);
    }
    Ok(())
}

/// Define every index for a collection. The first `IndexDef` also creates the
/// collection server-side (the only command that does).
fn create_indexes(client: &mut Client, coll: &CollectionPlan) -> MigrateResult<()> {
    for idx in &coll.indexes {
        client.define_index(&coll.name, &idx.name, &idx.fields, idx.unique)?;
    }
    Ok(())
}

// ---- preview / summary ----

fn print_preview(dump: &pgdump::Dump, plan: &Plan) {
    let total_rows: usize = dump.tables.iter().map(|t| t.rows.len()).sum();
    println!("ZydecoDB migration plan");
    println!("=======================");
    println!(
        "Parsed {} table(s), {} row(s) total.",
        dump.tables.len(),
        total_rows
    );
    println!();
    println!(
        "Target shape: {} collection(s), {} embedded table(s), {} join table(s) dissolved.",
        plan.collections.len(),
        plan.embedded_tables.len(),
        plan.join_tables.len()
    );
    println!();

    println!("Collections:");
    for coll in &plan.collections {
        let rows = dump.table(&coll.name).map(|t| t.rows.len()).unwrap_or(0);
        let id = match &coll.id_strategy {
            classify::IdStrategy::PrimaryKey(c) => format!("_id from {c}"),
            classify::IdStrategy::Generated => "_id generated".to_string(),
        };
        println!("  - {} ({} doc(s), {})", coll.name, rows, id);
        for embed in &coll.embeds {
            let shape = if embed.one_to_one { "object" } else { "array" };
            let snap = if embed.snapshots.is_empty() {
                String::new()
            } else {
                let names: Vec<&str> = embed
                    .snapshots
                    .iter()
                    .map(|s| s.ref_table.as_str())
                    .collect();
                format!(", snapshot {}", names.join("+"))
            };
            println!("      embed {} as {}{}", embed.child_table, shape, snap);
        }
        for d in &coll.join_dissolves {
            println!("      dissolve {} -> {}", d.join_table, d.field);
        }
        if !coll.indexes.is_empty() {
            let names: Vec<String> = coll
                .indexes
                .iter()
                .map(|i| {
                    let u = if i.unique { " (unique)" } else { "" };
                    format!("{}{}", i.fields.join("+"), u)
                })
                .collect();
            println!("      index {}", names.join(", "));
        }
    }
}

fn print_summary(plan: &Plan) {
    let d = &plan.dropped;
    println!();
    println!("Migration complete.");
    println!();
    println!("Constraint report (what Postgres enforced):");
    println!(
        "  preserved: {} unique constraint(s) recreated as server-side unique index(es)",
        d.preserved_unique.len()
    );
    println!("  now your application's responsibility:");
    if !d.dropped_unique.is_empty() {
        println!(
            "    - {} unique constraint(s) not recreatable as a single-field index",
            d.dropped_unique.len()
        );
    }
    println!(
        "    - {} foreign-key constraint(s) (referential integrity is no longer enforced)",
        d.foreign_keys
    );
    println!(
        "    - {} NOT NULL column(s) (presence is no longer enforced)",
        d.not_null.len()
    );
    println!(
        "    - {} CHECK constraint(s) (value rules are no longer enforced)",
        d.check
    );
}
