# ADR-0002 — MVCC design

Status: Accepted (foundation built in v1; transactions deferred)

## Context
Multi-document ACID transactions with snapshot isolation are a headline
relational pillar. Even though v1 does not expose transactions, the storage
layout must support snapshot reads later **without** a format migration.

## Decision
Every record is keyed by an `InternalKey = (user_key, seq, kind)`:
- `user_key` sorts ascending.
- `seq` (a strictly monotonic 64-bit sequence) sorts **descending**, so the
  newest version of a key is encountered first during a forward scan.
- `kind` distinguishes `Value` from `Tombstone`.

A read for `user_key` walks versions newest-first and stops at the first version
whose `seq <= snapshot_seq`. In v1 `snapshot_seq` is always "latest", which
collapses to ordinary last-write-wins — but the machinery for snapshot reads is
already present in the key ordering and the IPC `KeyPayload` (which reserves a
`snapshot_seq` field).

Sequence numbers are allocated by a single `SeqAllocator` (atomic, verified
monotonic under contention) and checkpointed into the manifest so they never go
backwards across a restart.

## Alternatives considered
- **Lock-based / single-version store:** simplest, but no path to snapshot
  isolation without a rewrite of both the key layout and the read path.
- **Per-key version chains in a separate structure:** more complex recovery and
  compaction story than folding versions into the LSM keyspace.

## Consequences
- Compaction must be MVCC-aware (drop versions below the oldest live snapshot,
  collapse tombstones) — deferred to BACKLOG P2.
- The transaction manager (BEGIN/COMMIT/ROLLBACK, IPC `0x10`–`0x12`) layers on
  top of this foundation; it does not require changing on-disk formats.
