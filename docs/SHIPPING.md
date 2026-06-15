# WAL Shipping (off-box durability)

ZydecoDB writes a byte-identical copy of every **sealed** WAL segment into a
configured directory the moment it rolls. An operator-supplied **sidecar** moves
those files off the box. The engine does no network I/O and ships nothing to an
object store itself — that is deliberately out of scope. The engine's only
promise is:

> The file in `wal_ship_dir` is exactly the sealed segment, and `shipped.log`
> records the segments in seal order with their SHA-256.

This is the cheapest credible answer to "lose the NVMe, lose the data": pair it
with a one-line `rsync`/`s5cmd`/AWS DataSync watcher and you have off-box copies
of the write-ahead log without coupling the engine to any cloud.

## Configuration

```rust
use zydecodb_engine::engine::{Engine, EngineConfig};
use zydecodb_engine::shipping::ShipMode;
use std::path::PathBuf;

let mut engine = Engine::open(EngineConfig {
    data_dir: "/var/lib/zydecodb/data".into(),
    wal_dir: "/var/lib/zydecodb/data/wal".into(),
    ..Default::default()
})?
.with_shipping(
    Some(PathBuf::from("/var/lib/zydecodb/ship")),
    ShipMode::Hardlink,  // or ShipMode::Copy
);
```

Pass `None` for `ship_dir` to disable shipping.

- **hardlink** (default): atomic and free — no bytes are copied, the directory
  entry just points at the same inode. Requires `wal_ship_dir` to be on the
  **same filesystem** as `wal_dir`. If it is not, the engine automatically falls
  back to a copy for that segment (cross-device link is impossible).
- **copy**: always copies the bytes. Use this when `wal_ship_dir` is a different
  mount (e.g. a separate volume the sidecar owns).

## What gets shipped, and when

- A segment is shipped the instant it **seals** — i.e. when a write rolls the
  WAL to a new segment. The sealed segment is fsynced first, so the shipped file
  is complete and durable.
- `Engine::shutdown()` (graceful SIGTERM/SIGINT) also syncs and ships the
  currently-active segment, so a clean stop leaves nothing un-shipped.
- The **active** (not-yet-sealed) segment is *not* shipped on every write. The
  bytes sitting in it are your recovery-point-objective (RPO) exposure.

## `shipped.log`

Append-only, one line per shipped segment, written into `wal_ship_dir`:

```text
<segment_id> <seal_seq> <sha256_hex>
```

- `segment_id` — the WAL segment number (matches `wal-XXXXXXXX.log`).
- `seal_seq` — the highest durable sequence number at seal time.
- `sha256_hex` — SHA-256 of the shipped file, for end-to-end integrity checks.

The sidecar should transport files in `segment_id` order and may use the hash to
verify each upload.

## The sidecar contract

You own transport. A minimal example that mirrors the ship dir to S3:

```bash
# Runs on the same box; watches the ship dir and syncs new segments.
while true; do
  s5cmd sync /var/lib/zydecodb/ship/  s3://my-bucket/zydecodb-wal/
  sleep 5
done
```

Rules the sidecar must follow:

1. **Append-only consumption.** Never delete or mutate files the engine wrote
   until they are safely transported. Deleting locally is fine *after* upload.
2. **Order by `shipped.log`.** Restore correctness depends on applying segments
   in seal order.
3. **Verify with the hash.** Compare the uploaded object's SHA-256 against the
   `shipped.log` entry.

## Recovery / restore

To restore on a fresh box after losing the local disk:

1. Stop the engine (if running).
2. Pull the shipped segments from your remote into a clean `wal_dir`, in
   `segment_id` order.
3. Start the engine. Normal WAL replay (seq-ordered, CRC-checked, torn-tail
   tolerant) reconstructs the memtable from the segments. Records already
   covered by an existing SSTable are skipped.

For a faster restore — or to roll back to a specific point in time — pair the
shipped WAL with a base snapshot instead of replaying the full WAL history:

```bash
# Capture a base snapshot (offline, or against a replica's data_dir).
zydecodb admin snapshot --config zydecodb.toml --out /backups/snap-2026-06-14

# Restore base + shipped WAL up to an exact sequence (or best-effort time).
zydecodb admin restore \
  --base /backups/snap-2026-06-14 \
  --wal  /var/lib/zydecodb/ship \
  --to-seq 12840 \
  --out  /var/lib/zydecodb/restored
```

`--to-time <unix_millis>` resolves to a sequence via the shipped time index
(`timeindex.log`, written at heartbeat granularity), so it is coarse; use
`--to-seq` for precise control. See [`docs/REPLICATION.md`](REPLICATION.md) for
the shipped-stream layout.

The RPO is bounded by the bytes still in the active segment at the moment of
loss — exported as the Prometheus gauge:

```text
zydecodb_wal_unshipped_bytes
```

Alert on this if it grows beyond your tolerance (it grows until the next segment
seal, then drops). A graceful shutdown drives it to zero.

## What this does NOT do (scope)

- No object-store client, no async uploader, no encryption — the sidecar owns
  all of that.
- SSTables are not shipped (only the WAL). Full base backups are produced
  on demand by `admin snapshot` (hardlinked SSTables + manifest), and
  point-in-time restore is `admin snapshot` + `admin restore` over the shipped
  WAL — see [Recovery / restore](#recovery--restore).
