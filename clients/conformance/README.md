# ZydecoDB wire conformance vectors

`vectors.json` is the **single source of truth** for the binary wire protocol.
It is generated from the real server encoders — not hand-written — so every
client implementation (Python, Go, TypeScript, …) can prove byte-for-byte that
its codec matches the server and can never silently drift.

## Regenerating

The authority is the Rust code in
[`crates/zydecodb-document/src/wire.rs`](../../crates/zydecodb-document/src/wire.rs)
(per-command payload bodies) and
[`crates/zydecodb-engine/src/frame.rs`](../../crates/zydecodb-engine/src/frame.rs)
(the envelope header). To regenerate after a protocol change:

```bash
cargo run -p zydecodb-document --bin gen_conformance
```

Commit the resulting `vectors.json`. CI job **`wire-conformance`** (see
[`.github/workflows/ci.yml`](../../.github/workflows/ci.yml)):

1. Regenerates vectors and fails if `vectors.json` is stale.
2. Runs Python, Go, and TypeScript codec suites against that file.

Python is the hand-maintained reference client; Go and TypeScript must match
the same bytes. A protocol change that breaks any client fails the PR.

## File shape

```jsonc
{
  "proto_version": 1,
  "envelope_header_len": 6,
  "commands":  { "DocPut": 33, ... },   // command name -> byte code
  "statuses":  { "Ok": 0, ... },        // status name  -> byte code
  "requests":  [ /* encode vectors */ ],
  "responses": [ /* decode vectors */ ]
}
```

### Envelope

Every request is `[version u8][command u8][payload_len u32 BE][payload]`.
`version` is `proto_version`; `payload_len` is the byte length of `payload`.
A request vector's `envelope_hex` is exactly that frame; `payload_hex` is just
the payload body.

### `requests[]` — encode vectors

A client maps `kind` to its codec function, feeds it `input`, and asserts the
result equals `payload_hex` (and the framed bytes equal `envelope_hex`).

| field          | meaning                                                        |
| -------------- | -------------------------------------------------------------- |
| `name`         | stable identifier for the vector                               |
| `kind`         | logical command (`DocPut`, `Find`, `QueryIndexRange`, …)       |
| `command`      | envelope command byte                                          |
| `input`        | language-neutral arguments (see below)                         |
| `payload_hex`  | authoritative payload bytes, hex-encoded                       |
| `envelope_hex` | authoritative full frame (header + payload), hex-encoded       |

**JSON bodies are opaque bytes.** Fields suffixed `_json` (`body_json`,
`filter_json`, `update_json`, `lo_json`, `hi_json`) carry *already-serialized*
JSON as a UTF-8 string. The conformance contract is about framing, not about any
one language's JSON serializer (key ordering, whitespace, number formatting),
so a client's **codec layer must accept these bytes verbatim**. Serializing an
object into those bytes is the job of the higher-level client layer and is out
of scope here. Fields suffixed `_hex` (`cursor_hex`) carry raw bytes as hex.

`input` shapes by `kind`:

- **DocPut** — `collection`, `doc_id` (UTF-8 → bytes), `body_json`, `relaxed`.
  Optional wire trailer (not yet in all driver APIs): after the flags byte, an
  8-byte big-endian `expires_at` (unix millis) when non-zero. Vectors today
  cover `relaxed` only; server encode/decode of `expires_at` is in
  `DocPutPayload`.
- **DocDel** — `collection`, `doc_id`
- **IndexDef** — `collection`, `index_name`, `fields` (string[]), `unique`
- **QueryById** — `collection`, `doc_id`
- **QueryIndexRange** — `collection`, `index_name`, `lo_json`, `hi_json`,
  `cursor_hex`, `limit`
- **Find** — `collection`, `filter_json`, `sort` (`[field, ascending][]`),
  `projection` (`{mode: none|include|exclude, fields: string[]}`), `skip`,
  `limit`, `cursor_hex`
- **Update** — `collection`, `filter_json`, `update_json`, `multi`, `relaxed`,
  `upsert` (bool; sets `FLAG_UPSERT=0x02` on the trailing flags byte)
- **Delete** — `collection`, `filter_json`, `multi`, `relaxed`
- **Count** — `collection`, `filter_json`
- **Distinct** — `collection`, `filter_json`, `field`
- **SessionInit** — `api_key` (UTF-8 → payload bytes)
- **Ping** — `{}` (empty payload)

### `responses[]` — decode vectors

A client decodes `bytes_hex` and asserts the result equals `decoded`.

| field       | meaning                                              |
| ----------- | ---------------------------------------------------- |
| `name`      | stable identifier                                    |
| `kind`      | `QueryPage`                                          |
| `bytes_hex` | server-produced response page, hex-encoded           |
| `decoded`   | expected decode result                               |

`QueryPage.decoded` is `{ rows: [{doc_id, body_json}], next_cursor_hex }` where
a missing/empty body decodes to `""` and end-of-results decodes
`next_cursor_hex` to `null`.
