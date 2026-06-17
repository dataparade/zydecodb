//! ZydecoDB migration tool: turn a plain `pg_dump` into read-optimized document
//! collections and load them into a fresh ZydecoDB server.
//!
//! The pipeline is staged so each step is independently testable and nothing
//! touches the network until the operator confirms:
//!
//! 1. [`pgdump`] — parse the dump into tables, columns, constraints, and rows.
//! 2. [`graph`] — build the foreign-key graph and annotate it with the real
//!    fan-out sampled from the data.
//! 3. [`classify`] — apply the embed/reference/snapshot rule to produce a [`Plan`].
//! 4. [`convert`] — materialize documents (faithful type conversion, `_id` rule).
//! 5. [`run`] — preview stats, prompt, then write via the [`client`] wire client.
//!
//! This crate is a *client*. It depends on `zydecodb-engine` only for the wire
//! envelope and on `zydecodb-document` only for the payload codecs; it never
//! links the storage engine.

pub mod classify;
pub mod client;
pub mod convert;
pub mod error;
pub mod graph;
pub mod pgdump;
pub mod run;

pub use classify::Plan;
pub use error::{MigrateError, MigrateResult};
pub use run::{run, MigrateOptions};
