# Changelog

All notable changes to the ZydecoDB server and official drivers are recorded
here. Version numbers are unified across artifacts; see
[`docs/RELEASES.md`](docs/RELEASES.md).

## [Unreleased]

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
