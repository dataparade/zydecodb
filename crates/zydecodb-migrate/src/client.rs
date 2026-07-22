//! Minimal synchronous wire client for the migrator.
//!
//! This is deliberately tiny: the migrator only needs to authenticate, define
//! indexes (which also creates collections server-side), upsert documents, and
//! peek at a collection to confirm the target is empty. It reuses the engine's
//! envelope codec and the document layer's payload codecs so the wire format is
//! defined in exactly one place.

use crate::error::{MigrateError, MigrateResult};
use std::io::{Read, Write};
use std::net::TcpStream;
use std::time::Duration;
use zydecodb_document::wire::{DocPutPayload, FindPayload, IndexDefPayload, WireProjection};
use zydecodb_engine::errors::Status;
use zydecodb_engine::frame::{
    Command, RequestEnvelope, ResponseEnvelope, ENVELOPE_HEADER_LEN, PROTO_VERSION,
};

/// A single connection to a ZydecoDB server. Not thread-safe; the migrator is
/// single-threaded by design (one ordered write stream into an empty database).
pub struct Client {
    stream: TcpStream,
}

impl Client {
    /// Connect to `addr` (`host:port`) and, if `api_key` is set, authenticate.
    pub fn connect(addr: &str, api_key: Option<&str>) -> MigrateResult<Client> {
        let stream = TcpStream::connect(addr)
            .map_err(|e| MigrateError::Connection(format!("connect to {addr} failed: {e}")))?;
        stream
            .set_read_timeout(Some(Duration::from_secs(30)))
            .map_err(|e| MigrateError::Connection(e.to_string()))?;
        stream
            .set_write_timeout(Some(Duration::from_secs(30)))
            .map_err(|e| MigrateError::Connection(e.to_string()))?;
        // Requests are small; disable Nagle for latency like the official driver.
        let _ = stream.set_nodelay(true);
        let mut client = Client { stream };
        if let Some(key) = api_key {
            client.session_init(key)?;
        }
        Ok(client)
    }

    /// Authenticate the connection with a raw API key (sent as UTF-8 bytes).
    pub fn session_init(&mut self, api_key: &str) -> MigrateResult<()> {
        let (status, body) = self.request(Command::SessionInit, api_key.as_bytes().to_vec())?;
        expect_ok(status, &body, "SessionInit")
    }

    /// Liveness check.
    pub fn ping(&mut self) -> MigrateResult<()> {
        let (status, body) = self.request(Command::Ping, Vec::new())?;
        expect_ok(status, &body, "Ping")
    }

    /// Define an index. This is also how a collection is first created on the
    /// server (the catalog's `ensure_collection` runs inside `IndexDef`).
    /// Treats an existing index of the same name as success (idempotent reruns).
    pub fn define_index(
        &mut self,
        collection: &str,
        index_name: &str,
        fields: &[String],
        unique: bool,
    ) -> MigrateResult<()> {
        let payload = IndexDefPayload {
            collection: collection.to_string(),
            index_name: index_name.to_string(),
            fields: fields.to_vec(),
            unique,
        }
        .encode();
        let (status, body) = self.request_retrying(Command::IndexDef, payload)?;
        if status == Status::Conflict {
            // Index already exists — a rerun against the same fresh DB. Fine.
            return Ok(());
        }
        expect_ok(status, &body, "IndexDef")
    }

    /// Upsert a document by id (idempotent; safe to replay on a rerun).
    pub fn put_document(
        &mut self,
        collection: &str,
        doc_id: &str,
        body_json: &[u8],
    ) -> MigrateResult<()> {
        let payload = DocPutPayload {
            collection: collection.to_string(),
            doc_id: doc_id.as_bytes().to_vec(),
            body: body_json.to_vec(),
            relaxed: false,
            expires_at: 0,
        }
        .encode();
        let (status, resp) = self.request_retrying(Command::DocPut, payload)?;
        expect_ok(status, &resp, "DocPut")
    }

    /// Return true if `collection` already holds at least one document. Used as
    /// the empty-database guard before any writes. A collection that does not
    /// exist yet (NotFound) counts as empty.
    pub fn collection_has_rows(&mut self, collection: &str) -> MigrateResult<bool> {
        let payload = FindPayload {
            collection: collection.to_string(),
            filter: Vec::new(),
            sort: Vec::new(),
            projection: WireProjection::None,
            skip: 0,
            limit: 1,
            cursor: Vec::new(),
        }
        .encode();
        let (status, body) = self.request(Command::Find, payload)?;
        match status {
            // No such collection -> nothing stored -> empty.
            Status::NotFound => Ok(false),
            Status::Ok => {
                let (rows, _) = zydecodb_document::wire::decode_query_page(&body)
                    .map_err(|e| MigrateError::Server(format!("Find decode: {e}")))?;
                Ok(!rows.is_empty())
            }
            other => Err(server_error("Find", other, &body)),
        }
    }

    /// Like [`Self::request`] but retries on `EngineBusy`, the server's
    /// transient "rate limit exceeded" signal. The per-connection token bucket
    /// refills at the configured rps, so a short backoff always recovers; this
    /// paces the bulk loader to the server's rate instead of failing the run.
    fn request_retrying(
        &mut self,
        command: Command,
        payload: Vec<u8>,
    ) -> MigrateResult<(Status, Vec<u8>)> {
        let mut backoff = Duration::from_millis(20);
        const MAX_ATTEMPTS: u32 = 14;
        for attempt in 0..MAX_ATTEMPTS {
            let (status, body) = self.request(command, payload.clone())?;
            if status != Status::EngineBusy || attempt + 1 == MAX_ATTEMPTS {
                return Ok((status, body));
            }
            std::thread::sleep(backoff);
            backoff = (backoff * 2).min(Duration::from_millis(500));
        }
        unreachable!("loop returns on the final attempt")
    }

    /// Send one framed request and read the framed response.
    fn request(&mut self, command: Command, payload: Vec<u8>) -> MigrateResult<(Status, Vec<u8>)> {
        let bytes = RequestEnvelope::new(command, payload).encode();
        self.stream
            .write_all(&bytes)
            .map_err(|e| MigrateError::Connection(format!("write failed: {e}")))?;
        self.read_response()
    }

    fn read_response(&mut self) -> MigrateResult<(Status, Vec<u8>)> {
        let mut header = [0u8; ENVELOPE_HEADER_LEN];
        self.stream
            .read_exact(&mut header)
            .map_err(|e| MigrateError::Connection(format!("read header failed: {e}")))?;
        if header[0] != PROTO_VERSION {
            return Err(MigrateError::Connection(format!(
                "unexpected protocol version 0x{:02x}",
                header[0]
            )));
        }
        let (status, len) = ResponseEnvelope::parse_header(&header)
            .map_err(|e| MigrateError::Connection(format!("bad response header: {e}")))?;
        let mut body = vec![0u8; len];
        if len > 0 {
            self.stream
                .read_exact(&mut body)
                .map_err(|e| MigrateError::Connection(format!("read body failed: {e}")))?;
        }
        Ok((status, body))
    }
}

/// Turn a non-OK status into a descriptive error, decoding the message body
/// (the server returns the human-readable reason as the payload on errors).
fn server_error(op: &str, status: Status, body: &[u8]) -> MigrateError {
    let msg = String::from_utf8_lossy(body);
    if msg.is_empty() {
        MigrateError::Server(format!("{op}: status {:?}", status))
    } else {
        MigrateError::Server(format!("{op}: {:?}: {msg}", status))
    }
}

fn expect_ok(status: Status, body: &[u8], op: &str) -> MigrateResult<()> {
    if status == Status::Ok {
        Ok(())
    } else {
        Err(server_error(op, status, body))
    }
}
