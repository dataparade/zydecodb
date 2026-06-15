//! Structured error taxonomy. Status code bytes are FROZEN at v1 — new codes
//! append, existing codes never renumber.
//!
//! Byte 0x09 is the generic `PolicyRejected` slot. Embedders that layer their
//! own write-time policies (see [`crate::policy::WritePolicy`]) return
//! [`EngineError::PolicyRejected`] with a human-readable reason; the caller's
//! front end is responsible for mapping that string onto its own user-facing
//! taxonomy (usage limit, schema violation, etc.).

use thiserror::Error;

/// Wire status byte returned in every response envelope.
#[repr(u8)]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Status {
    Ok = 0x00,
    NotFound = 0x01,
    Error = 0x02,
    Conflict = 0x03,
    IoError = 0x04,
    InvalidKey = 0x05,
    InvalidValue = 0x06,
    EngineBusy = 0x07,
    ProtocolError = 0x08,
    /// A write was rejected by a caller-installed [`crate::policy::WritePolicy`].
    /// The accompanying message (in `EngineError::PolicyRejected`) describes why.
    PolicyRejected = 0x09,
    /// The engine encountered an on-disk artifact (WAL segment, SSTable
    /// footer, manifest record) whose format version it does not know how to
    /// read. The engine refuses to proceed rather than silently misparse.
    /// Distinct from [`Status::IoError`] (which covers torn writes /
    /// truncation): an `UnsupportedFormat` means the data on disk is intact
    /// but written by a different (usually newer) engine version.
    UnsupportedFormat = 0x0A,
    /// Missing or invalid API key, or command sent before SessionInit.
    Unauthorized = 0x0B,
    /// Authenticated but operation not permitted (read-only key, prefix ACL, etc.).
    Forbidden = 0x0C,
}

impl Status {
    pub fn as_u8(self) -> u8 {
        self as u8
    }

    pub fn from_u8(b: u8) -> Option<Status> {
        Some(match b {
            0x00 => Status::Ok,
            0x01 => Status::NotFound,
            0x02 => Status::Error,
            0x03 => Status::Conflict,
            0x04 => Status::IoError,
            0x05 => Status::InvalidKey,
            0x06 => Status::InvalidValue,
            0x07 => Status::EngineBusy,
            0x08 => Status::ProtocolError,
            0x09 => Status::PolicyRejected,
            0x0A => Status::UnsupportedFormat,
            0x0B => Status::Unauthorized,
            0x0C => Status::Forbidden,
            _ => return None,
        })
    }
}

/// Engine-internal error type. Each variant maps to exactly one wire [`Status`].
#[derive(Debug, Error)]
pub enum EngineError {
    #[error("not found")]
    NotFound,

    #[error("invalid key: {0}")]
    InvalidKey(String),

    #[error("invalid value: {0}")]
    InvalidValue(String),

    #[error("engine busy: {0}")]
    EngineBusy(String),

    #[error("protocol error: {0}")]
    Protocol(String),

    #[error("conflict")]
    Conflict,

    #[error("io error: {0}")]
    Io(String),

    #[error("policy rejected: {0}")]
    PolicyRejected(String),

    /// The engine read a well-formed but version-incompatible on-disk artifact
    /// (e.g. a manifest record type the running build does not recognize, or a
    /// WAL/SSTable header tagged with a future format version). Opening the
    /// engine MUST fail loudly rather than silently truncate or misparse.
    #[error("unsupported on-disk format: {0}")]
    UnsupportedFormat(String),

    #[error("unauthorized: {0}")]
    Unauthorized(String),

    #[error("forbidden: {0}")]
    Forbidden(String),

    /// The `data_dir` is already locked by another live engine instance. Raised
    /// only at [`crate::engine::Engine::open`] time (never crosses the wire) to
    /// stop two processes from corrupting the same directory.
    #[error("data_dir locked: {0}")]
    Locked(String),

    #[error("{0}")]
    Other(String),
}

impl EngineError {
    /// Map an engine error onto its frozen wire status code.
    pub fn status(&self) -> Status {
        match self {
            EngineError::NotFound => Status::NotFound,
            EngineError::InvalidKey(_) => Status::InvalidKey,
            EngineError::InvalidValue(_) => Status::InvalidValue,
            EngineError::EngineBusy(_) => Status::EngineBusy,
            EngineError::Protocol(_) => Status::ProtocolError,
            EngineError::Conflict => Status::Conflict,
            EngineError::Io(_) => Status::IoError,
            EngineError::PolicyRejected(_) => Status::PolicyRejected,
            EngineError::UnsupportedFormat(_) => Status::UnsupportedFormat,
            EngineError::Unauthorized(_) => Status::Unauthorized,
            EngineError::Forbidden(_) => Status::Forbidden,
            // A failed data_dir lock is a local startup failure, surfaced as IO.
            EngineError::Locked(_) => Status::IoError,
            EngineError::Other(_) => Status::Error,
        }
    }
}

impl From<std::io::Error> for EngineError {
    fn from(e: std::io::Error) -> Self {
        EngineError::Io(e.to_string())
    }
}

pub type EngineResult<T> = Result<T, EngineError>;

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn status_round_trips() {
        for b in 0x00u8..=0x0C {
            let s = Status::from_u8(b).expect("known code");
            assert_eq!(s.as_u8(), b);
        }
        assert!(Status::from_u8(0x0D).is_none());
        assert!(Status::from_u8(0xFF).is_none());
    }

    #[test]
    fn error_maps_to_frozen_status() {
        assert_eq!(EngineError::NotFound.status(), Status::NotFound);
        assert_eq!(
            EngineError::InvalidKey("x".into()).status(),
            Status::InvalidKey
        );
        assert_eq!(
            EngineError::InvalidValue("x".into()).status(),
            Status::InvalidValue
        );
        assert_eq!(
            EngineError::EngineBusy("x".into()).status(),
            Status::EngineBusy
        );
        assert_eq!(
            EngineError::Protocol("x".into()).status(),
            Status::ProtocolError
        );
        assert_eq!(EngineError::Conflict.status(), Status::Conflict);
        assert_eq!(EngineError::Io("x".into()).status(), Status::IoError);
        assert_eq!(
            EngineError::PolicyRejected("x".into()).status(),
            Status::PolicyRejected
        );
        assert_eq!(
            EngineError::UnsupportedFormat("x".into()).status(),
            Status::UnsupportedFormat
        );
        assert_eq!(
            EngineError::Unauthorized("x".into()).status(),
            Status::Unauthorized
        );
        assert_eq!(
            EngineError::Forbidden("x".into()).status(),
            Status::Forbidden
        );
        assert_eq!(EngineError::Other("x".into()).status(), Status::Error);
    }
}
