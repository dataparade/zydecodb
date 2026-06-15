# ADR-0003 — Glommio (thread-per-core, io_uring) for the engine runtime

Status: Accepted

## Context
The engine is I/O-bound (WAL fsync, SSTable reads/writes) and must extract
maximum throughput from modern NVMe + multi-core hardware. The runtime choice
shapes the whole concurrency model.

## Decision
Use **Glommio**: a thread-per-core, share-nothing async runtime built natively
on Linux `io_uring`. Each core owns its executor; the engine state lives behind
`Rc<RefCell<…>>` within a single executor, avoiding cross-thread locking on the
hot path. I/O is submitted through io_uring rather than the thread-pool blocking
model.

## Alternatives considered
- **Tokio:** ubiquitous and battle-tested, but its work-stealing, multi-threaded
  model forces `Arc<Mutex<…>>` around engine state and uses a blocking thread
  pool for file I/O — both of which cap throughput for this workload.
- **Raw io_uring (e.g. `io-uring` crate):** maximum control, but we'd be
  rebuilding an async runtime; Glommio already provides the right abstractions.
- **std threads + blocking I/O:** simplest, but leaves io_uring's batched
  submission/completion performance on the table.

## Consequences
- **Linux-only.** io_uring is not portable; acceptable for a server engine.
- Verified at Day 0 that glommio 0.9 builds and runs on the target kernel (6.8).
- Thread-per-core means sharding/state-partitioning becomes the scaling lever;
  cross-core coordination (if ever needed) must be explicit message passing, not
  shared locks. This is a deliberate constraint that keeps the hot path
  lock-free.
- The RESP2 front end is served in-process on the same glommio executor (see
  `src/server.rs`), so client requests are direct engine calls. An out-of-process
  control plane is no longer part of v1; if one returns later it would speak the
  retained Unix-socket IPC protocol, keeping the runtime choice encapsulated.
