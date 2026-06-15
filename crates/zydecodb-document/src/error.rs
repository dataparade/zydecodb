//! Document-layer error taxonomy, mapped onto the engine's frozen wire
//! [`Status`] codes so the server can return a single response envelope.

use std::fmt;
use zydecodb_engine::errors::{EngineError, Status};

#[derive(Debug)]
pub enum DocError {
    /// The named collection has not been defined.
    CollectionNotFound(String),
    /// The named index does not exist on the collection.
    IndexNotFound(String),
    /// A collection or index with this name already exists.
    AlreadyExists(String),
    /// A write would violate a unique index constraint.
    DuplicateKey(String),
    /// The document body (or a query bound) was not valid JSON.
    InvalidJson(String),
    /// A single document touched more index keys than one atomic batch allows.
    BatchTooLarge(usize),
    /// On-disk catalog blob could not be (de)serialized.
    Corrupt(String),
    /// A malformed wire payload.
    Protocol(String),
    /// A malformed query filter document.
    BadFilter(String),
    /// A malformed update document.
    BadUpdate(String),
    /// An underlying engine error; carries its own wire status.
    Engine(EngineError),
}

pub type DocResult<T> = Result<T, DocError>;

impl From<EngineError> for DocError {
    fn from(e: EngineError) -> Self {
        DocError::Engine(e)
    }
}

impl DocError {
    /// The frozen wire status this error maps to.
    pub fn status(&self) -> Status {
        match self {
            DocError::CollectionNotFound(_) | DocError::IndexNotFound(_) => Status::NotFound,
            DocError::AlreadyExists(_) | DocError::DuplicateKey(_) => Status::Conflict,
            DocError::InvalidJson(_) | DocError::Protocol(_) => Status::ProtocolError,
            DocError::BadFilter(_) | DocError::BadUpdate(_) => Status::InvalidValue,
            DocError::BatchTooLarge(_) => Status::InvalidValue,
            DocError::Corrupt(_) => Status::IoError,
            DocError::Engine(e) => e.status(),
        }
    }
}

impl fmt::Display for DocError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            DocError::CollectionNotFound(c) => write!(f, "collection not found: {c}"),
            DocError::IndexNotFound(i) => write!(f, "index not found: {i}"),
            DocError::AlreadyExists(x) => write!(f, "already exists: {x}"),
            DocError::DuplicateKey(x) => write!(f, "duplicate key: {x}"),
            DocError::InvalidJson(e) => write!(f, "invalid json: {e}"),
            DocError::BatchTooLarge(n) => {
                write!(
                    f,
                    "document touches {n} keys, exceeding the atomic batch limit"
                )
            }
            DocError::Corrupt(e) => write!(f, "corrupt catalog: {e}"),
            DocError::Protocol(e) => write!(f, "protocol error: {e}"),
            DocError::BadFilter(e) => write!(f, "bad filter: {e}"),
            DocError::BadUpdate(e) => write!(f, "bad update: {e}"),
            DocError::Engine(e) => write!(f, "{e}"),
        }
    }
}
