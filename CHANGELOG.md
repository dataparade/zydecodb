# Changelog

All notable changes to the ZydecoDB server and official drivers are recorded
here. Version numbers are unified across artifacts; see
[`docs/COMPATIBILITY.md`](docs/COMPATIBILITY.md#releases-and-tagging).

## [Unreleased]

### Compatibility

- Wire `proto_version = 1` freeze language updated for the upcoming 1.x line:
  unknown opcodes return `ProtocolError` without closing the connection; unused
  write-flag bits are rejected. See [`docs/PROTOCOL.md`](docs/PROTOCOL.md)
  and [`docs/COMPATIBILITY.md`](docs/COMPATIBILITY.md).
- Compatibility policy moves from lockstep driver/server minors to a wire-v1
  matrix (any 1.x driver ↔ any 1.x server) while keeping unified release tags.
- Go wire codecs moved to `clients/go/internal/proto` (not a public API).
- Conformance vectors extended with admin/Stats/SchemaDef, Query
  `include_bodies=false`, and golden status envelopes for every status byte.

## [0.11.0] - 2026-07-27

### Server

- `zydecodb update`: in-process self-update from GitHub Releases using the same
  asset contract as `scripts/install.sh` (sha256-verified tarball, atomic
  replace). Flags: `--check`, `--version`, `--force`, `--yes`. Binary only —
  does not update drivers or data dirs. See `docs/COMPATIBILITY.md#updating-the-server-binary`.

### Compatibility

- Requires server `0.11.x`, wire `proto_version = 1` (append-only opcodes)

## [0.10.0] - 2026-07-27

### Server

- Bounded per-connection transactions (`Begin`/`Commit`/`Rollback`): stage
  by-ID document and raw-KV ops, validate revisions/uniques at commit, persist
  one durable WAL batch (≤1024 keys). Not general MVCC.
- Filtered positional array `$set`: paths like `items.$[skuId=ABC].qty` update
  exactly one matching element (0 or >1 matches → `BadUpdate`). `$set` only;
  Mongo `$` / `$[]` / `arrayFilters` rejected. No new wire opcodes.
- Directional indexes and reverse index scans: per-field ASC/DESC on `IndexDef`
  (optional `0x02` direction trailer), DESC key encoding, engine `scan_rev`,
  planner streams `{ownerId}` + `updatedAt DESC` from a matching index without
  the sort buffer.
- TTL compaction reclamation: Tidewalker drops wall-clock-expired SST entries
  (and older versions of that key) during merge; metrics
  `compaction_expired_dropped_total` / `ttl_sweep_tombstones_total`. Memtable
  sweeper remains non-WAL hygiene. Document upserts rewrite index-key expiry
  when body `expires_at` changes.
- Minimal aggregation (`Aggregate = 0x2B`): optional `$match` + one `$group`
  with `$sum`/`$count`, deterministic ordering, and `[aggregation]` resource
  limits. Joins/`$lookup`/`$unwind` remain unsupported. See `docs/PROTOCOL.md#aggregation`.
- Change streams (`Watch = 0x2C`): primary-only, collection-scoped, dedicated
  connection streaming of fsynced upsert/delete events with durable resume
  tokens backed by a retained WAL archive (`[change_streams]`, off by default).
  At-least-once delivery; not raw WAL replication. See `docs/PROTOCOL.md#change-streams`.

### Go / Python / TypeScript drivers

- Pinned-connection transaction APIs (`WithTransaction` / `transaction()` /
  `withTransaction`); no retries inside an open transaction; commit transport
  failure surfaces as unknown commit result
- Filtered positional `$set` works through existing update APIs (opaque update
  JSON); no driver codec changes
- Directional `create_index` / `CreateIndexFields` (ASC default preserved for
  string-only field lists)
- `aggregate(...)` APIs for bounded pipelines
- Dedicated-connection `watch(...)` / `ChangeStream` APIs with opaque base64
  resume tokens (Python / Go / TypeScript)

### Compatibility

- Requires server `0.10.x`, wire `proto_version = 1` (append-only opcodes)
- Existing unconditional replace/update/find/get opcodes are unchanged
- New opcodes (`Begin`, `Aggregate`, `Watch`, conditional writes) fail with
  `ProtocolError` against older servers (never silently degrade)

## [0.9.0] - 2026-07-26

### Server

- Opaque document revisions (`InternalKey.seq`) exposed on revision-aware reads
- Conditional replace and by-ID update opcodes (`DocPutIfMatch`, `DocUpdateIfMatch`)
- Revision-aware get/find opcodes that return an 8-byte revision per document

### Go / Python / TypeScript drivers

- Additive APIs: `GetWithRevision`, revision-aware find, `ReplaceOneIfMatch`,
  `UpdateByIDIfMatch` (names vary slightly by language)
- Go module tags use the nested form `clients/go/vX.Y.Z`

### Compatibility

- Requires server `0.9.x`, wire `proto_version = 1`
- Existing unconditional replace/update/find/get opcodes are unchanged
- Conditional methods fail with `ProtocolError` against older servers (never
  silently degrade to unconditional writes)

## [0.9.0-beta.7] - 2026-07-22

### Server

- Field-based TTL indexes (`expireAfterSeconds` on IndexDef)
- Per-document DocPut `expires_at`

### Drivers

- Expose DocPut `expires_at` and typed policy/format errors in Py/Go/TS
- Pods ops path documented

### Compatibility

- Requires server `0.9.x`, wire `proto_version = 1`

## [0.9.0-beta.6] - 2026-07-22

### Server

- `$setOnInsert` on filter upsert
- δ-fair pods path and 0.9 wire freeze documentation

### Compatibility

- Requires server `0.9.x`, wire `proto_version = 1`

## [0.9.0-beta.4] - 2026-07-20

### Server

- Version bump for the beta.4 release train

### Compatibility

- Requires server `0.9.x`, wire `proto_version = 1`

## [0.9.0-beta.3] - 2026-07-20

### Packaging

- Remove third-party database product names from public package copy

### Compatibility

- Requires server `0.9.x`, wire `proto_version = 1`

## [0.9.0-beta.2] - 2026-07-20

### Server

- Fix release-tree compile without unfinished ACL work

### Compatibility

- Requires server `0.9.x`, wire `proto_version = 1`

## [0.9.0-beta.1] - 2026-07-20

### Server

- Zero-config `serve`, install script, and registry publish pipeline
- Document store + KV core with official Python, Go, and TypeScript drivers

### Compatibility

- Requires server `0.9.x`, wire `proto_version = 1`
