//! Versioned wire envelope codec.
//!
//! ```text
//! [1] protocol version (0x01)
//! [1] command code
//! [4] payload length (u32 BE)
//! [N] payload
//! ```
//!
//! The engine owns the envelope byte layout and the command/status enumerations.
//! Document opcodes (`0x20`–`0x26`, `0x30`) are interpreted by the server document
//! layer; session/admin opcodes (`0x40`–`0x42`) by the server security/admin path.
//! Reserved slots (`Begin`/`Commit`/`Rollback`, `SchemaDef`) parse but reject with
//! `ProtocolError` until a future minor line assigns semantics.

use crate::errors::{EngineError, Status};

pub const PROTO_VERSION: u8 = 0x01;
pub const ENVELOPE_HEADER_LEN: usize = 6;

/// Command codes for the 0.9 wire. Implemented opcodes and this numbering are
/// frozen for 0.9.x; reserved slots may gain semantics later without renumbering.
#[repr(u8)]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Command {
    Put = 0x01,
    Get = 0x02,
    Del = 0x03,
    Begin = 0x10,
    Commit = 0x11,
    Rollback = 0x12,
    Query = 0x20,
    /// Document upsert. Caller-defined semantics (the document layer), the
    /// engine only round-trips the bytes.
    DocPut = 0x21,
    /// Document delete.
    DocDel = 0x22,
    /// Filter-based find with sort/projection/pagination (document layer).
    Find = 0x23,
    /// Filter-based partial update (document layer).
    Update = 0x24,
    /// Filter-based delete (document layer).
    Delete = 0x25,
    /// Filter-based count / distinct (document layer).
    Count = 0x26,
    IndexDef = 0x30,
    SchemaDef = 0x31,
    /// Reserved byte for caller-defined session establishment. The engine parses
    /// but does not interpret it.
    SessionInit = 0x40,
    /// Reserved byte for caller-defined per-connection routing context. The
    /// engine parses but does not interpret it.
    SetContext = 0x41,
    /// Admin: drop a tenant's data on a live server (prefix delete + catalog).
    AdminDropTenant = 0x42,
    Ping = 0xF0,
    Stats = 0xF1,
}

impl Command {
    pub fn as_u8(self) -> u8 {
        self as u8
    }

    pub fn from_u8(b: u8) -> Option<Command> {
        Some(match b {
            0x01 => Command::Put,
            0x02 => Command::Get,
            0x03 => Command::Del,
            0x10 => Command::Begin,
            0x11 => Command::Commit,
            0x12 => Command::Rollback,
            0x20 => Command::Query,
            0x21 => Command::DocPut,
            0x22 => Command::DocDel,
            0x23 => Command::Find,
            0x24 => Command::Update,
            0x25 => Command::Delete,
            0x26 => Command::Count,
            0x30 => Command::IndexDef,
            0x31 => Command::SchemaDef,
            0x40 => Command::SessionInit,
            0x41 => Command::SetContext,
            0x42 => Command::AdminDropTenant,
            0xF0 => Command::Ping,
            0xF1 => Command::Stats,
            _ => return None,
        })
    }

    /// Raw key-value and session/control commands handled by the KV dispatcher
    /// (not the document layer).
    pub fn is_kv_command(self) -> bool {
        matches!(
            self,
            Command::Put
                | Command::Get
                | Command::Del
                | Command::SessionInit
                | Command::SetContext
                | Command::AdminDropTenant
                | Command::Ping
                | Command::Stats
        )
    }

    /// Document-layer commands (routed via docdispatch).
    pub fn is_document_command(self) -> bool {
        matches!(
            self,
            Command::Query
                | Command::DocPut
                | Command::DocDel
                | Command::Find
                | Command::Update
                | Command::Delete
                | Command::Count
                | Command::IndexDef
        )
    }
}

/// A parsed request envelope.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RequestEnvelope {
    pub command: Command,
    pub payload: Vec<u8>,
}

impl RequestEnvelope {
    pub fn new(command: Command, payload: Vec<u8>) -> Self {
        RequestEnvelope { command, payload }
    }

    /// Encode to the wire. Returns the full byte buffer.
    pub fn encode(&self) -> Vec<u8> {
        let mut buf = Vec::with_capacity(ENVELOPE_HEADER_LEN + self.payload.len());
        buf.push(PROTO_VERSION);
        buf.push(self.command.as_u8());
        buf.extend_from_slice(&(self.payload.len() as u32).to_be_bytes());
        buf.extend_from_slice(&self.payload);
        buf
    }

    /// Parse a header (6 bytes). Returns `(command, payload_len)`.
    pub fn parse_header(header: &[u8]) -> Result<(Command, usize), EngineError> {
        if header.len() < ENVELOPE_HEADER_LEN {
            return Err(EngineError::Protocol("short header".into()));
        }
        if header[0] != PROTO_VERSION {
            return Err(EngineError::Protocol(format!(
                "unsupported protocol version 0x{:02x}",
                header[0]
            )));
        }
        let command = Command::from_u8(header[1])
            .ok_or_else(|| EngineError::Protocol(format!("unknown command 0x{:02x}", header[1])))?;
        let len = u32::from_be_bytes([header[2], header[3], header[4], header[5]]) as usize;
        Ok((command, len))
    }

    /// Decode a complete in-memory buffer (header + payload).
    pub fn decode(buf: &[u8]) -> Result<RequestEnvelope, EngineError> {
        let (command, len) = Self::parse_header(buf)?;
        let body = &buf[ENVELOPE_HEADER_LEN..];
        if body.len() < len {
            return Err(EngineError::Protocol("truncated payload".into()));
        }
        Ok(RequestEnvelope {
            command,
            payload: body[..len].to_vec(),
        })
    }
}

/// A response envelope.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ResponseEnvelope {
    pub status: Status,
    pub payload: Vec<u8>,
}

impl ResponseEnvelope {
    pub fn new(status: Status, payload: Vec<u8>) -> Self {
        ResponseEnvelope { status, payload }
    }

    pub fn ok(payload: Vec<u8>) -> Self {
        ResponseEnvelope::new(Status::Ok, payload)
    }

    pub fn not_found() -> Self {
        ResponseEnvelope::new(Status::NotFound, Vec::new())
    }

    pub fn error(status: Status, msg: &str) -> Self {
        ResponseEnvelope::new(status, msg.as_bytes().to_vec())
    }

    pub fn encode(&self) -> Vec<u8> {
        let mut buf = Vec::with_capacity(ENVELOPE_HEADER_LEN + self.payload.len());
        buf.push(PROTO_VERSION);
        buf.push(self.status.as_u8());
        buf.extend_from_slice(&(self.payload.len() as u32).to_be_bytes());
        buf.extend_from_slice(&self.payload);
        buf
    }

    pub fn parse_header(header: &[u8]) -> Result<(Status, usize), EngineError> {
        if header.len() < ENVELOPE_HEADER_LEN {
            return Err(EngineError::Protocol("short response header".into()));
        }
        if header[0] != PROTO_VERSION {
            return Err(EngineError::Protocol("bad response version".into()));
        }
        let status = Status::from_u8(header[1])
            .ok_or_else(|| EngineError::Protocol("unknown status".into()))?;
        let len = u32::from_be_bytes([header[2], header[3], header[4], header[5]]) as usize;
        Ok((status, len))
    }
}

// ---- Payload codecs for v1 commands ----

/// Decoded PUT payload.
///
/// `routing_key` is a 16-byte opaque slot the engine never interprets. Callers
/// that need a 16-byte routing/identity prefix put it here; the engine just
/// round-trips the bytes.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PutPayload {
    pub routing_key: [u8; 16],
    pub txid: u64,       // reserved, zero in v1
    pub expires_at: u64, // 0 = no expiry
    pub key: Vec<u8>,
    pub value: Vec<u8>,
}

impl PutPayload {
    pub fn encode(&self) -> Vec<u8> {
        let mut buf = Vec::with_capacity(16 + 8 + 8 + 4 + 4 + self.key.len() + self.value.len());
        buf.extend_from_slice(&self.routing_key);
        buf.extend_from_slice(&self.txid.to_be_bytes());
        buf.extend_from_slice(&self.expires_at.to_be_bytes());
        buf.extend_from_slice(&(self.key.len() as u32).to_be_bytes());
        buf.extend_from_slice(&(self.value.len() as u32).to_be_bytes());
        buf.extend_from_slice(&self.key);
        buf.extend_from_slice(&self.value);
        buf
    }

    pub fn decode(p: &[u8]) -> Result<PutPayload, EngineError> {
        const FIXED: usize = 16 + 8 + 8 + 4 + 4;
        if p.len() < FIXED {
            return Err(EngineError::Protocol("PUT payload too short".into()));
        }
        let mut routing_key = [0u8; 16];
        routing_key.copy_from_slice(&p[0..16]);
        let txid = u64::from_be_bytes(p[16..24].try_into().unwrap());
        let expires_at = u64::from_be_bytes(p[24..32].try_into().unwrap());
        let key_len = u32::from_be_bytes(p[32..36].try_into().unwrap()) as usize;
        let value_len = u32::from_be_bytes(p[36..40].try_into().unwrap()) as usize;
        if p.len() != FIXED + key_len + value_len {
            return Err(EngineError::Protocol("PUT payload length mismatch".into()));
        }
        let key = p[FIXED..FIXED + key_len].to_vec();
        let value = p[FIXED + key_len..].to_vec();
        Ok(PutPayload {
            routing_key,
            txid,
            expires_at,
            key,
            value,
        })
    }
}

/// Decoded GET/DEL payload (same shape).
///
/// `routing_key` is an opaque 16-byte caller-defined slot (see [`PutPayload`]).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct KeyPayload {
    pub routing_key: [u8; 16],
    pub snapshot_seq: u64, // reserved, zero in v1
    pub key: Vec<u8>,
}

impl KeyPayload {
    pub fn encode(&self) -> Vec<u8> {
        let mut buf = Vec::with_capacity(16 + 8 + 4 + self.key.len());
        buf.extend_from_slice(&self.routing_key);
        buf.extend_from_slice(&self.snapshot_seq.to_be_bytes());
        buf.extend_from_slice(&(self.key.len() as u32).to_be_bytes());
        buf.extend_from_slice(&self.key);
        buf
    }

    pub fn decode(p: &[u8]) -> Result<KeyPayload, EngineError> {
        const FIXED: usize = 16 + 8 + 4;
        if p.len() < FIXED {
            return Err(EngineError::Protocol("key payload too short".into()));
        }
        let mut routing_key = [0u8; 16];
        routing_key.copy_from_slice(&p[0..16]);
        let snapshot_seq = u64::from_be_bytes(p[16..24].try_into().unwrap());
        let key_len = u32::from_be_bytes(p[24..28].try_into().unwrap()) as usize;
        if p.len() != FIXED + key_len {
            return Err(EngineError::Protocol("key payload length mismatch".into()));
        }
        let key = p[FIXED..].to_vec();
        Ok(KeyPayload {
            routing_key,
            snapshot_seq,
            key,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn command_round_trips() {
        for b in [
            0x01, 0x02, 0x03, 0x10, 0x11, 0x12, 0x20, 0x21, 0x22, 0x23, 0x24, 0x25, 0x26, 0x30,
            0x31, 0x40, 0x41, 0xF0, 0xF1,
        ] {
            let c = Command::from_u8(b).unwrap();
            assert_eq!(c.as_u8(), b);
        }
        assert!(Command::from_u8(0x99).is_none());
    }

    #[test]
    fn only_expected_commands_are_v1() {
        assert!(Command::Put.is_kv_command());
        assert!(Command::Get.is_kv_command());
        assert!(Command::Del.is_kv_command());
        assert!(Command::Ping.is_kv_command());
        assert!(Command::Stats.is_kv_command());
        assert!(Command::SessionInit.is_kv_command());
        assert!(Command::SetContext.is_kv_command());
        assert!(Command::AdminDropTenant.is_kv_command());
        assert!(!Command::Begin.is_kv_command());
        assert!(!Command::Query.is_kv_command());
        assert!(Command::Query.is_document_command());
        assert!(Command::Find.is_document_command());
    }

    #[test]
    fn request_envelope_round_trips() {
        let req = RequestEnvelope::new(Command::Put, vec![1, 2, 3, 4]);
        let bytes = req.encode();
        assert_eq!(bytes.len(), ENVELOPE_HEADER_LEN + 4);
        let back = RequestEnvelope::decode(&bytes).unwrap();
        assert_eq!(req, back);
    }

    #[test]
    fn response_envelope_round_trips() {
        let envelope = ResponseEnvelope::ok(b"world".to_vec());
        let bytes = envelope.encode();
        let (status, len) = ResponseEnvelope::parse_header(&bytes).unwrap();
        assert_eq!(status, Status::Ok);
        assert_eq!(len, 5);
        assert_eq!(&bytes[ENVELOPE_HEADER_LEN..], b"world");
    }

    #[test]
    fn bad_version_rejected() {
        let mut bytes = RequestEnvelope::new(Command::Get, vec![]).encode();
        bytes[0] = 0x02;
        assert!(RequestEnvelope::decode(&bytes).is_err());
    }

    #[test]
    fn unknown_command_rejected() {
        let bytes = vec![PROTO_VERSION, 0x99, 0, 0, 0, 0];
        assert!(RequestEnvelope::decode(&bytes).is_err());
    }

    #[test]
    fn truncated_payload_rejected() {
        let mut bytes = RequestEnvelope::new(Command::Put, vec![1, 2, 3, 4]).encode();
        bytes.truncate(ENVELOPE_HEADER_LEN + 2);
        assert!(RequestEnvelope::decode(&bytes).is_err());
    }

    #[test]
    fn put_payload_round_trips() {
        let p = PutPayload {
            routing_key: [0u8; 16],
            txid: 0,
            expires_at: 12345,
            key: b"\x01mykey".to_vec(),
            value: b"myvalue".to_vec(),
        };
        let bytes = p.encode();
        let back = PutPayload::decode(&bytes).unwrap();
        assert_eq!(p, back);
    }

    #[test]
    fn key_payload_round_trips() {
        let p = KeyPayload {
            routing_key: [7u8; 16],
            snapshot_seq: 0,
            key: b"\x01abc".to_vec(),
        };
        let bytes = p.encode();
        let back = KeyPayload::decode(&bytes).unwrap();
        assert_eq!(p, back);
    }

    #[test]
    fn put_payload_length_mismatch_rejected() {
        let p = PutPayload {
            routing_key: [0u8; 16],
            txid: 0,
            expires_at: 0,
            key: b"k".to_vec(),
            value: b"v".to_vec(),
        };
        let mut bytes = p.encode();
        bytes.push(0xFF); // extra trailing byte
        assert!(PutPayload::decode(&bytes).is_err());
    }
}
