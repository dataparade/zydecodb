//! Error taxonomy for the migration tool.
//!
//! Migrations touch three failure domains — reading the dump file, parsing its
//! SQL, and talking to a live server — so the variants keep those separate to
//! make operator-facing messages precise about *where* a run stopped.

use std::fmt;

#[derive(Debug)]
pub enum MigrateError {
    /// Reading the dump file from disk failed.
    Io(String),
    /// The dump could not be parsed (unsupported statement, malformed COPY, ...).
    Parse(String),
    /// Talking to the server failed at the transport layer.
    Connection(String),
    /// The server answered a command with a non-OK status.
    Server(String),
    /// The target database is not empty (the migrator only loads fresh nodes).
    NotEmpty(String),
    /// The migration was aborted before any writes (e.g. user declined).
    Aborted(String),
}

impl fmt::Display for MigrateError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            MigrateError::Io(m) => write!(f, "io error: {m}"),
            MigrateError::Parse(m) => write!(f, "parse error: {m}"),
            MigrateError::Connection(m) => write!(f, "connection error: {m}"),
            MigrateError::Server(m) => write!(f, "server error: {m}"),
            MigrateError::NotEmpty(m) => write!(f, "target not empty: {m}"),
            MigrateError::Aborted(m) => write!(f, "aborted: {m}"),
        }
    }
}

impl std::error::Error for MigrateError {}

impl From<std::io::Error> for MigrateError {
    fn from(e: std::io::Error) -> Self {
        MigrateError::Io(e.to_string())
    }
}

pub type MigrateResult<T> = Result<T, MigrateError>;
