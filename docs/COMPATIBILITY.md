# Compatibility (1.x)

This document is the semver contract for ZydecoDB 1.x. Tagging `1.0.0` means
the surfaces below do not break without a major version bump.

Related: wire details in [`PROTOCOL.md`](PROTOCOL.md#wire-protocol),
on-disk upgrades in [`GUIDE.md`](GUIDE.md#upgrading), releases and tagging in
[Releases and tagging](#releases-and-tagging).

## Compatibility matrix (wire-v1)

After 1.0, **any 1.x official driver works against any 1.x server** on
`proto_version = 1`. Versions stay **unified for packaging** (one `X.Y.Z` across
server + Python + npm + Go tags), but runtime compatibility is defined by the
wire — not by matching minor numbers.

| Rule | Behavior |
|------|----------|
| Older driver → newer server | Works for opcodes the driver sends |
| Newer driver → older server | New opcodes fail closed with `ProtocolError` (`0x08`); connection stays open |
| Unknown opcode byte | Server returns `ProtocolError` and keeps the connection |
| Reserved unimplemented (e.g. `SchemaDef`) | Same: `ProtocolError`, connection stays open |

Pre-1.0 (`0.9`–`0.11`) releases used lockstep “driver minor == server minor”
language. That policy ends at 1.0; prefer this wire-v1 matrix going forward.
Applications should still pin explicit versions (not `@latest`).

## What is frozen vs additive

### Breaking (requires 2.0)

- Changing the meaning or byte layout of an existing opcode, status, flag, or
  payload discriminator
- Removing or renumbering a public driver method / error type that was part of
  the 1.0 surface
- Dropping SSTable N/N−1 read support without a major bump
- Renaming published Prometheus metric names that were present at 1.0
- Removing a documented config key or CLI flag without the deprecation window
  below (removals only in 2.0)

### Additive (allowed in 1.x minors)

- New opcodes and status bytes (append-only)
- New optional trailers on existing payloads (older encoders omit; decoders
  default safely)
- New driver methods and options
- New config keys with safe defaults
- New metrics and CLI subcommands
- New on-disk format versions that preserve the N/N−1 SSTable policy

### Not part of the driver contract

- Go `internal/proto` wire codecs and opcode constants — applications must not
  import them
- Python `_protocol` / TypeScript non-exported helpers
- Change-log archive file layout beyond the resume-token opacity guarantee
  (tokens remain decodable across 1.0.x patches while within retention)

## Driver API map

Language-idiomatic naming (snake_case / PascalCase / camelCase) is intentional.
The logical surface is shared:

| Concern | Python | Go | TypeScript |
|---------|--------|-----|------------|
| Client | `Client(...)` | `NewClient(addr, opts...)` | `new Client(address, opts)` |
| Collection | `collection(name)` | `Collection(name)` | `collection(name)` |
| Insert | `insert_one` / `insert_many` | `InsertOne` / `InsertMany` | `insertOne` / `insertMany` |
| Replace | `replace_one` / `replace_one_if_match` | `ReplaceOne` / `ReplaceOneIfMatch` | `replaceOne` / `replaceOneIfMatch` |
| Update | `update_one` / `update_many` / `update_by_id_if_match` | `UpdateOne` / `UpdateMany` / `UpdateByIDIfMatch` | same camelCase |
| Find | `find` / `find_one` / `find_with_revision` | `Find` / `FindOne` / `FindWithRevision` | same |
| Get | `get` / `get_with_revision` | `Get` / `GetWithRevision` | same |
| Aggregate | `aggregate` | `Aggregate` | `aggregate` |
| Watch | `watch` → iterator | `Watch` → `Next` | `watch` → async iterable |
| Transaction | `transaction()` CM | `BeginTx` / `WithTransaction` | `withTransaction(fn)` |
| ID helper | `generate_id` | `GenerateID` | `generateId` |

### Resume tokens

- **In:** `watch(resume=...)` takes raw bytes / `[]byte` / `Buffer`
- **Out:** change events expose a **base64** string (`resume_token` /
  `ResumeToken` / `resumeToken`)
- Decode before passing back into `watch`

### Error taxonomy (status → driver)

| Status | Byte | Python / TypeScript | Go |
|--------|------|---------------------|----|
| Ok | `0x00` | success | success |
| NotFound | `0x01` | absence (`None` / `null`), not an exception | `(nil, nil)` / empty |
| Error | `0x02` | `ServerError` | `ServerError` (no `Is*`) |
| Conflict | `0x03` | `ConflictError` | `IsConflict` |
| IoError | `0x04` | `ServerError` | `ServerError` (no `Is*`) |
| InvalidKey / InvalidValue / ProtocolError | `0x05`/`0x06`/`0x08` | `InvalidRequestError` | `IsInvalidRequest` |
| EngineBusy | `0x07` | `ServerBusyError` | `IsBusy` |
| PolicyRejected | `0x09` | `PolicyError` | `IsPolicyRejected` |
| UnsupportedFormat | `0x0A` | `UnsupportedFormatError` | `IsUnsupportedFormat` |
| Unauthorized / Forbidden | `0x0B`/`0x0C` | `AuthError` | `IsAuth` |
| Unknown commit result | — | `UnknownCommitError` | `ErrUnknownCommitResult` |

Go transport failures use `ConnError` (named differently from Py/TS
`ConnectionError` by design; same role).

The conformance suite (`clients/conformance/vectors.json`) is the byte-level
driver contract for encode/decode.

## Deprecation policy

Inside 1.x:

1. Mark the surface deprecated in docs and changelog for **at least one minor**
   release.
2. Keep the old behavior working through that window.
3. Removals happen only in **2.0**.

## Support window (once 2.x exists)

Security fixes and data-corruption fixes are backported to the latest 1.x for
**12 months** after the first 2.0 release. Feature work stays on the current
major.

## Explicit non-goals (1.x product scope)

- No consensus / Raft; assisted WAL shipping + fenced manual promote is the HA
  model
- No general MVCC; transactions remain bounded per-connection staging
  (≤1024 keys)
- No `$lookup`, joins, or `$unwind`
- No MongoDB compatibility
- Single-writer primary

## Releases and tagging

How ZydecoDB versions and publishes the server binary and official drivers.

### Unified semver

One release commit carries one logical version `X.Y.Z` (optionally with a
pre-release suffix such as `-beta.7`). Artifacts at that commit:

| Artifact | Version identity |
|----------|------------------|
| Server binary | Root git tag `vX.Y.Z` (GitHub Release) |
| Python driver | `clients/python/pyproject.toml` `version` (must match the tag under PEP 440) |
| TypeScript driver | `clients/typescript/package.json` `version` (must match the tag) |
| Go driver | Nested git tag `clients/go/vX.Y.Z` at the **same commit** as `vX.Y.Z` |

Go modules in a subdirectory do not inherit root `v*` tags. Without
`clients/go/vX.Y.Z`, `go get` falls back to a pseudo-version
(`v0.0.0-<timestamp>-<hash>`).

### Compatibility

**After 1.0:** any **1.x** official driver works against any **1.x** server on
`proto_version = 1`. Packaging stays unified (one `X.Y.Z` for server + drivers),
but runtime compatibility is the wire — not lockstep minors. Full policy:
[Compatibility matrix (wire-v1)](#compatibility-matrix-wire-v1).

| Line | Drivers | Wire |
|------|---------|------|
| `1.x` (upcoming) | Python / npm / Go `1.x*` | `proto_version = 1` |
| `0.11.x` (current) | Python / npm / Go `0.11.x*` | `proto_version = 1` |
| `0.10.x` | Python / npm / Go `0.10.x*` | `proto_version = 1` |
| `0.9.x` | Python / npm / Go `0.9.x*` | `proto_version = 1` |

- `0.9`–`0.11` used lockstep “driver minor == server minor” language; that ends
  at 1.0 in favor of the wire-v1 matrix above.
- Applications should pin an explicit driver version, not `@latest`.
- Append-only opcodes and status bytes may appear in later 1.x minors. Older
  drivers never send unknown opcodes. New opcodes fail closed (`ProtocolError`)
  on older servers rather than degrading silently.

## Cutting a release

**Pre-tag gates** (mandatory before any RC / 1.0 tag) — see
[`GUIDE.md` Release checklist](GUIDE.md#release-checklist-pre-tag):

- [ ] 90m paced soak + `analyze-soak.py --mode stability` exit 0
      (or green `.github/workflows/release-soak.yml`)
- [ ] `MODE=both` tenant-isolation soak exit 0 (same workflow / nightly on RC commit)
- [ ] (RC) 24h uncapped VPS soak archived under `docs/soak-baselines/`

Bump versions in `Cargo.toml`, `clients/python/pyproject.toml`, and
`clients/typescript/package.json` on the release commit. Update
[`CHANGELOG.md`](../CHANGELOG.md). Then tag **both** the root and Go module
tags at that commit and push them together:

```bash
ver=1.0.0   # or 0.11.0 / 1.0.0-rc.1
git tag "v${ver}"
git tag "clients/go/v${ver}" "$(git rev-parse "v${ver}^{commit}")"
git push origin "v${ver}" "clients/go/v${ver}"
```

Pushing only the root tag will fail the release workflow’s Go tag gate.
Pushing only `clients/go/v*` does nothing (the workflow triggers on root `v*`).

The release workflow then:

1. Verifies `clients/go/v${ver}` exists at the same commit as `v${ver}`
2. Verifies `clients/go/go.mod` module path
3. Resolves `github.com/dataparade/zydecodb/clients/go@v${ver}` with
   `GOPROXY=direct`
4. Publishes the server binary, PyPI package, and npm package

## Pinning

```bash
# Go — pin the module version (not @latest)
go get github.com/dataparade/zydecodb/clients/go@v0.11.0   # current
# go get github.com/dataparade/zydecodb/clients/go@v1.0.0  # after 1.0

# Python
pip install zydecodb==0.11.0

# TypeScript
npm install zydecodb@0.11.0
```

Unified releases still ship matching `X.Y.Z` tags for convenience. After 1.0,
any 1.x driver may talk to any 1.x server on wire v1 (see
[`COMPATIBILITY.md`](COMPATIBILITY.md)).

## Updating the server binary

After the first install (`scripts/install.sh` or a release tarball), upgrade the
**server binary only** with:

```bash
zydecodb update --check    # print current vs available; exit 1 if newer exists
zydecodb update            # sha256 + gh attestation verify, then replace binary
zydecodb update --yes      # non-interactive
zydecodb update --version v0.11.0
zydecodb update --force    # reinstall same version, or allow a major jump
zydecodb update --skip-attestation   # airgap / no gh CLI; SHA-256 still required
```

`update` uses the same GitHub Release assets as `install.sh`
(`zydecodb-${tag}-${target}.tar.gz` + `.sha256`). After checksum verify it runs
`gh attestation verify` by default and **refuses to install** on failure or if
`gh` is missing — use `--skip-attestation` only when you intentionally opt out.
It does **not** update data directories, config, or language drivers (still pip /
npm / `go get`). Restart any running `zydecodb serve` after a successful update —
the old process keeps the previous inode until it exits.

## Prometheus metric names (1.0 freeze)

Published series names below are part of the 1.0 contract. **Renames are
breaking** and require a major version bump. New series may be added in 1.x
minors. Source of truth: `crates/zydecodb-engine/src/metrics.rs` plus replica
gauges registered in `crates/zydecodb/src/server.rs`.

### Engine registry (`Metrics`)

- `zydecodb_wal_bytes_written_total`
- `zydecodb_wal_group_commit_syncs_total`
- `zydecodb_wal_fsync_duration_seconds`
- `zydecodb_wal_segment_count`
- `zydecodb_wal_unshipped_bytes`
- `zydecodb_sstable_flushes_total`
- `zydecodb_sstable_get_duration_seconds`
- `zydecodb_bloom_false_positives_total`
- `zydecodb_memtable_size_bytes`
- `zydecodb_immutable_memtable_count`
- `zydecodb_live_sstable_count`
- `zydecodb_live_sstables_by_level` (label `level`)
- `zydecodb_compaction_jobs_total`
- `zydecodb_compaction_bytes_read_total`
- `zydecodb_compaction_bytes_written_total`
- `zydecodb_compaction_duration_seconds`
- `zydecodb_compaction_queue_depth`
- `zydecodb_compaction_worker_busy`
- `zydecodb_compaction_versions_dropped_total`
- `zydecodb_compaction_tombstones_dropped_total`
- `zydecodb_compaction_expired_dropped_total`
- `zydecodb_ttl_sweep_tombstones_total`
- `zydecodb_compaction_repack_total`
- `zydecodb_compaction_rejected_no_progress_total`
- `zydecodb_compaction_jobs_by_input_level_total` (label `input_level`)
- `zydecodb_compaction_apply_duration_seconds`
- `zydecodb_manifest_syncs_total`
- `zydecodb_manifest_sync_duration_seconds`
- `zydecodb_pending_compaction_bytes`
- `zydecodb_user_bytes_written_total`
- `zydecodb_block_cache_hits_total`
- `zydecodb_block_cache_misses_total`
- `zydecodb_block_cache_compaction_reads_total`
- `zydecodb_block_cache_evictions_total`
- `zydecodb_block_cache_resident_bytes`
- `zydecodb_block_cache_resident_entries`
- `zydecodb_result_cache_hits_total` (registered when result cache enabled)
- `zydecodb_result_cache_misses_total` (registered when result cache enabled)
- `zydecodb_result_cache_evictions_total` (registered when result cache enabled)
- `zydecodb_result_cache_resident_bytes` (registered when result cache enabled)
- `zydecodb_disk_bytes_total`
- `zydecodb_logical_live_bytes`
- `zydecodb_space_amplification`
- `zydecodb_last_durable_seq`
- `zydecodb_last_shutdown_clean`
- `zydecodb_change_stream_subscriptions`
- `zydecodb_change_stream_events_total`
- `zydecodb_change_stream_heartbeats_total`
- `zydecodb_change_stream_disconnects_total` (label `reason`)
- `zydecodb_change_stream_consumer_lag_seqs`
- `zydecodb_change_log_archive_segments`
- `zydecodb_change_log_archive_bytes`
- `zydecodb_change_log_earliest_seq`
- `zydecodb_change_log_latest_seq`
- `zydecodb_snapshot_duration_seconds`
- `zydecodb_restore_duration_seconds`
- `zydecodb_tx_begin_total`
- `zydecodb_tx_commit_total`
- `zydecodb_tx_abort_total`
- `zydecodb_tx_timeout_total`
- `zydecodb_errors_total` (label `code`)

### Server-registered replica gauges

Registered into the shared registry when `[replica].from` is set:

- `zydecodb_replica_lag_seqs`
- `zydecodb_replica_heartbeat_age_seconds`
- `zydecodb_replica_lag_seconds`

### Optional per-tenant series

When `[metrics].per_tenant = true`, the server also registers:

- `zydecodb_tenant_requests_total` (labels `tenant`, `command`, `status`)
