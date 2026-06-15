# ADR-0001 — FlatBuffers for typed document values

Status: Accepted (v1 reserves the design; encoding not yet emitted)

## Context
The relational pillars (schema enforcement, secondary indexes, query AST) need
to read fields out of stored values without deserializing the whole document.
v1 stores opaque byte values, but the on-disk value format must not paint us
into a corner that forces a migration when typed documents arrive.

## Decision
Adopt **FlatBuffers** as the wire/storage format for typed document values when
they land. FlatBuffers gives zero-copy field access: the engine can validate a
schema and read indexed fields directly from the stored buffer without
allocating or parsing.

To keep v1 forward-compatible, the value payload carries no format assumptions
today — values are opaque bytes. When typed documents arrive, a value-kind tag
distinguishes `Raw` from `FlatBuffer` so old and new values coexist without a
rewrite.

**Placement (decided):** `value_kind` is the **first byte of the value
payload**, owned by the document layer — NOT a field in the WAL/SSTable/`Entry`
header. The engine continues to treat values as opaque bytes, so adding typed
documents requires no on-disk format migration (no WAL or SSTable version bump).
Reserved values: `0x00 = Raw`, `0x01 = FlatBuffer`. Putting the tag in the
engine entry header was considered and rejected: it would force a coordinated
WAL + SSTable + `Entry` format change for zero benefit, since only the document
layer ever interprets the byte.

## Alternatives considered
- **Protobuf:** requires full deserialization to read a field; no zero-copy.
- **Cap'n Proto:** zero-copy and excellent, but FlatBuffers has the more mature
  Rust tooling and a clearer schema-evolution story for stored values.
- **Custom layout:** maximum control, but reinventing schema evolution and
  cross-language codegen is not worth it.

## Consequences
- A `value_kind` discriminator must exist before the first typed document is
  written. Because it lives in the value payload (first byte), it can be added
  by the document layer with no engine change and no on-disk migration.
- Schema registry and codegen become part of the toolchain when pillars land.
