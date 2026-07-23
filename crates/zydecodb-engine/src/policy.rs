//! Write-time policy extension point.
//!
//! The engine stores opaque byte keys and never inspects who a write belongs to.
//! Any caller-side policy — usage limits, schema gates, validation hooks — is
//! injected through this trait so the engine can enforce a policy it does not
//! itself understand.
//!
//! A policy is consulted around every user-initiated PUT/DEL:
//!
//! - [`WritePolicy::pre_write`] runs BEFORE any WAL/memtable mutation. Returning
//!   an error (conventionally [`crate::errors::EngineError::PolicyRejected`])
//!   rejects the write with nothing persisted.
//! - [`WritePolicy::post_write`] runs AFTER the memtable insert, on the same
//!   commit path, so any bookkeeping it persists in the system keyspace joins
//!   the same group-commit fsync and recovers for free.
//!
//! Both hooks receive `&mut Engine` so an implementation can read and write the
//! system keyspace (via [`crate::engine::Engine::sys_get`] /
//! [`crate::engine::Engine::sys_put_policy`]) without taking a second borrow.
//!
//! The default is [`NoopWritePolicy`], which allows every write and records
//! nothing — an embedder gets a plain KV engine with no SaaS opinions.

use crate::engine::Engine;
use crate::errors::EngineResult;

/// Hook invoked by the engine around user writes. Implementations live outside
/// the engine crate (the engine ships only [`NoopWritePolicy`]).
///
/// `Send + Sync` so the policy can be held in an `Arc` shared across the
/// executor alongside the engine handle.
pub trait WritePolicy: Send + Sync {
    /// When `true`, the engine performs a full LSM point-get before each write
    /// so `existing_value_len` is accurate. Default `false` (noop policies and
    /// single-tenant embeds skip the get). Quota policies that credit freed
    /// bytes on overwrite/delete must return `true`.
    fn needs_existing_len(&self) -> bool {
        false
    }

    /// Called before a user PUT/DEL is persisted. `key` is the fully composed
    /// stored key. `value_len` is the new value length (0 for a delete).
    /// `existing_value_len` is `Some(len)` when the key already exists (an
    /// overwrite/delete of a live key) or `None` for a brand-new key — or
    /// always `None` when [`WritePolicy::needs_existing_len`] is `false`.
    ///
    /// Return `Ok(())` to allow the write, or an error to reject it before any
    /// mutation. Conventionally [`crate::errors::EngineError::PolicyRejected`].
    fn pre_write(
        &self,
        engine: &mut Engine,
        key: &[u8],
        value_len: usize,
        existing_value_len: Option<usize>,
        is_delete: bool,
    ) -> EngineResult<()>;

    /// Called after a successful memtable insert to record resource changes.
    /// Runs on the same commit path as the user write, so anything persisted
    /// here is durable in the same batch. Side-effecting; failures are the
    /// implementation's responsibility to log (it must not unwind the write).
    fn post_write(
        &self,
        engine: &mut Engine,
        key: &[u8],
        value_len: usize,
        existing_value_len: Option<usize>,
        is_delete: bool,
    );
}

/// The default policy: always allows, records nothing. This is what ships with
/// the open-source engine and what an embedder gets unless they install one.
pub struct NoopWritePolicy;

impl WritePolicy for NoopWritePolicy {
    fn pre_write(
        &self,
        _engine: &mut Engine,
        _key: &[u8],
        _value_len: usize,
        _existing_value_len: Option<usize>,
        _is_delete: bool,
    ) -> EngineResult<()> {
        Ok(())
    }

    fn post_write(
        &self,
        _engine: &mut Engine,
        _key: &[u8],
        _value_len: usize,
        _existing_value_len: Option<usize>,
        _is_delete: bool,
    ) {
    }
}
