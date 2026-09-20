# Changelog

All notable changes to the ZydecoDB server and official drivers are recorded
here. Version numbers are unified across artifacts; see
[`docs/COMPATIBILITY.md`](docs/COMPATIBILITY.md#releases-and-tagging).

## [Unreleased]

## [1.3.0] - 2026-09-19

- Bench nightly p99 compare floor is 1000µs so a laptop 5µs baseline cannot
  fail shared GHA runners (measured 471µs). Soak `megabyte-values` runs at
  5 ops/s; 2000 ops/s of 1–4MiB puts was compaction-backlog EngineBusy,
  not a crash.
- `$lookup` gains an optional `filter` key: a normal filter object applied
  to inner documents before they are attached. Rejected documents do not
  count toward `max_matches_per_outer` or `max_hash_bytes`; the
  `max_scan_docs` hash-build gate still applies. Strategy selection is
  unchanged.
- A `$match` directly after `$lookup` is now legal (at most one) and
  filters the joined documents — outer plus the `as` array. Pipeline
  shapes grow from six to ten; `MAX_PIPELINE_STAGES` is now 4. Rejected
  documents are dropped before the `max_memory_bytes` charge. Filter paths
  keep the shipped `find` semantics and do not walk arrays — use
  `$elemMatch`, `{"as": []}`, or `{"as": {"$ne": []}}` on joined children.
- New `$size` accumulator (`{"$size": "$path"}`): adds the length of each
  array found at the path. `$sum` and `$size` paths now walk into arrays
  at every segment, including the last, so `[$lookup, $group]` can
  aggregate over joined children (`{"$sum": "$orders.total"}`).
  **Behavior change:** `{"$sum": "$amounts"}` with `amounts: [1,2,3]`
  returned `0` in 1.0–1.2; it now returns `6`.
- Servers that predate these shapes reject them with `InvalidValue`
  (`InvalidRequestError` in the drivers); the connection stays open. No
  wire format or driver changes.

## [1.2.0] - 2026-09-16

- Miri is a required CI job (90-minute timeout). Tests Miri cannot
  execute are ignored: OS-thread / latency windows, the 200k-put
  write-loss probe, and `snapshot_is_a_consistent_readable_base`
  (`copy_file_range`). `proptest_codecs` runs 16 cases under Miri
  (1024 on native). Those are not memory-model tests.
- Fuzz nightly no longer dies on the first target timeout: each input is
  capped at 10s and the remaining targets still run. `fuzz_dispatch` no
  longer Sync-fsyncs (this harness never spawned the commit thread, so
  Sync `commit()` waited forever) and skips Watch / Find / Count /
  Aggregate.

- rustls `0.23.40` → `0.23.45` (`RUSTSEC-2026-0285`: TLS 1.3 handshake
  messages accepted across encryption-level boundaries).
- `drain_flush` no longer resubmits a failed flush in the same poll, and
  both flush and compaction drains abort on worker failure / 30s timeout.
  The apply worker now catches failpoint panics and marks the apply failed
  instead of dying and leaving `finish_pending_applies` blocked. Those two
  loops are what burned the 6h CI rust job. The rust job now has a
  45-minute timeout (15 minutes on the failpoint step).

Correctness, security and availability fixes from the tier-1 audit. No wire
changes; conformance vectors are unchanged.

### Server

- `zydecodb --agent [TOPIC]` prints a versioned, topic-split usage contract
  (install, official drivers, query/update limits, hard no's) embedded in the
  binary so agents do not need the git tree.
- Catalog counters (`doc_count` / `entry_count`) moved out of the catalog
  blob into one small per-collection system record (`\x00doc/cnt/` +
  collection id), written in the same WAL record as the document ops. The
  schema blob is now rewritten on DDL only, not on every document write.
  Blobs written by older versions still carry inline counts; they load as
  before and each collection's counter record is created on its first counted
  write or the next DDL. `admin drop-tenant` removes the dropped collections'
  counter records.
- Raw-KV `Put` / `Del` (auto-commit and in-transaction) reject client keys
  starting with `d` or `i` followed by `0x00` with `InvalidKey`: that is the
  only key shape that aliases the document/index keyspace. Natural keys such
  as `device:1` are unaffected. Reads are unrestricted.
- ZDoc reader is total over arbitrary bytes: `ObjectView` / `ArrayView` /
  `ValueView` bounds-check every offset and length; malformed or truncated
  stored bodies surface as `Corrupt` errors from `Find`, `Get`, updates and
  deletes instead of aborting the server. Covered by unit tests, an
  integration test that plants garbage bodies, and the `zdoc_valueview` fuzz
  target (now also exercises keyed lookups).
- The nightly fuzz workflow had failed identically on every run since
  2026-08-07 (`rust-toolchain.toml` pins 1.91; the job never selected
  nightly, so cargo-fuzz's `-Z` flags hit stable rustc before any target ran).
  It now forces nightly and builds with `--sanitizer none` like the CI
  fuzz-smoke gate.
- Unique indexes are enforced in the two paths that skipped them:
  `UpdateMany` fails with `Conflict` (and changes nothing) when two documents
  in the same batch would claim the same unique value; defining a unique index
  over pre-existing duplicates fails with `Conflict`, leaves the catalog
  unchanged and removes the partially written index range (TTL indexes are
  not stamped onto documents until uniqueness is confirmed).
- `replica promote` requires `[replica].hmac_key_file` and verifies each
  shipped segment's HMAC before draining; sha256 alone no longer suffices.
  Promote without a key file is an error.
- WAL fsync failure fails closed instead of hanging `Sync` writers: the commit
  coordinator records the failure, wakes all waiters, rejects further writes
  with `IoError` ("WAL fsync failed; write not durable; writes refused until
  restart"), keeps serving reads, and `/readyz` returns `503`. Under
  `--features failpoints` the path is covered end to end.
- In-transaction `DocGetRev` applies the collection-prefix ACL like every
  other document command (`Forbidden` on a disallowed collection).

### Drivers

- Watch streams no longer die under the default request timeout. The dedicated
  Watch connection uses its own idle timeout (default 45s, must exceed the
  server's `change_streams.heartbeat_ms`): Python `Client(watch_idle_timeout=)`,
  Go `WithWatchIdleTimeout`, TypeScript `watchIdleTimeoutMs`. Driver CI runs
  the live suites with change streams enabled and an idle-watch test.
- Python `zydecodb.__version__` is `1.2.0`, matching `pyproject.toml`; a test
  keeps them in sync.

## [1.1.0] - 2026-08-26

Bounded `$lookup` on the existing `Aggregate` opcode. Not Mongo aggregation
compatibility — equality left-outer, one stage, two physical operators, hard
caps. See [`docs/DESIGN-joins.md`](docs/DESIGN-joins.md) and
[`docs/PROTOCOL.md`](docs/PROTOCOL.md#aggregation).

### Server

- `$lookup` as an aggregation stage (`from` / `localField` / `foreignField` /
  `as`). Legal pipelines: `[$group]`, `[$match, $group]`, `[$lookup]`,
  `[$lookup, $group]`, `[$match, $lookup]`, `[$match, $lookup, $group]`.
- Physical operators: indexed nested-loop when the inner `foreignField` is
  `_id` or a leading-field index; bounded hash join when there is no index
  and `inner.doc_count <= max_scan_docs`. Otherwise a named error — never a
  per-outer collection scan.
- Crash-atomic catalog `doc_count` / `entry_count` (catalog blob in the same
  WAL record as the data). Durable TTL sweep keeps counters exact.
- New `[aggregation]` knobs: `max_matches_per_outer` (default 1000),
  `max_hash_bytes` (default 16 MiB). `MAX_PIPELINE_STAGES` is 3.
- Flush visibility: an in-flight memtable flush no longer drops that
  memtable from the live read set before its SSTable is published
  (`flush_in_flight`).

### Compatibility

- Wire `Aggregate = 0x2B` is unchanged. Older 1.x drivers already send
  pipeline bytes; `$lookup` is a server-side stage. 1.0 servers reject
  `$lookup` pipelines as before.
- `$unwind`, multi-`$lookup`, and `pipeline:` / `let:` remain unsupported.

## [1.0.0] - 2026-08-18

First stable 1.0. The compatibility promise is in
[`docs/COMPATIBILITY.md`](docs/COMPATIBILITY.md): wire `proto_version = 1` is
frozen (additive opcodes and status bytes only); on-disk SSTable/WAL/manifest
v2 formats, official driver APIs, config keys, CLI flags, and metric names do
not break without a major version bump. Any 1.x driver works against any 1.x
server on wire v1.

### Since 1.0.0-rc.1

- Existing sessions are dropped when a key is revoked and the keystore reloads
  (`SIGHUP` / `Server::reload_keys`).
- Python `Transaction.put_document` no longer JSON-encodes the body twice.
- CI on `main` is green (rust, drivers, wire conformance, audit, fuzz-smoke).

## [1.0.0-rc.1] - 2026-08-07

First release candidate for 1.0. The 1.x compatibility promise is stated in
[`docs/COMPATIBILITY.md`](docs/COMPATIBILITY.md): wire `proto_version = 1` is
frozen (additive opcodes/status bytes only); on-disk SSTable/WAL/manifest v2
formats and the official driver APIs do not break without a major bump.

### Server

- Durability and DoS hardening from the P0–P2 audit (WAL/manifest fsync
  ordering, quota seed-on-open, pre-auth frame caps, TLS handshake deadlines,
  ZDoc depth guards, rate-limit bounds).
- Compaction: close stale-catalog submission windows that doubled write amp.
- SSTable reads: block-boundary walk-back so newest versions (and tombstones)
  are not missed when a key straddles blocks.
- Owned snapshots: memtable tombstones shadow older SST values; bounded
  ceilings walk visible versions inside a table instead of skipping it.
- Explicit `rustls` CryptoProvider install in the server binary (TLS no longer
  aborts at startup under feature unification).

### Testing / release proof

- Model-based differential tester (`engine-model`) + nightly seed sweep.
- Expanded fuzz budget and ZDoc `ValueView` target; crash-soak fault axes;
  weekly soak variants; server wire oracle with adversarial clients.
- RC soak archives under `docs/soak-baselines/rc/0.11.0/`: 24h uncapped
  (~33k ops/s) and 72h paced (stability all pass, amp ~2.89).

### Compatibility

- Wire `proto_version = 1` freeze language updated for the 1.x line: unknown
  opcodes return `ProtocolError` without closing the connection; unused
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
