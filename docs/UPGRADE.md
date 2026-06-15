# Upgrading ZydecoDB

This is the operator runbook for moving a `data_dir` across ZydecoDB versions
when the on-disk format changes. It documents the format-version policy and the
exact upgrade procedure.

## On-disk formats and versioning

ZydecoDB has three independently versioned on-disk surfaces:

| Surface  | Version constant            | Current | Backward read support              |
| -------- | --------------------------- | ------- | ---------------------------------- |
| SSTable  | `sstable::FORMAT_VERSION`   | `v2`    | reads `v1` and `v2`                |
| WAL      | `wal::WAL_FORMAT_VERSION`   | `v2`    | current only (WAL is transient)    |
| Manifest | per-record-type tag         | —       | refuses unknown record types       |

### Supported-range policy

- **SSTable** readers accept the **current version and the immediately prior
  version** (`N` and `N-1`). New tables are always written at the current
  version `N`. Older tables are rewritten forward by background compaction (or
  on demand via `admin upgrade`). Before a future `v3` bump, the migration
  window guarantees every reachable `v1` file has been rewritten to `v2`, so a
  reader only ever spans one version gap.
- **WAL** segments are validated against the current version only. The WAL is a
  transient recovery log, not long-term storage: a clean shutdown (or
  `admin upgrade`, which flushes) drains it into SSTables, so there is nothing
  to migrate. Never copy a WAL across a format boundary — drain it first.
- **Manifest** records carry a type tag. An older binary that meets a
  record type written by a newer binary refuses to open loudly
  (`UnsupportedFormat`) rather than silently truncating catalog state. This is a
  forward-compatibility *guard*, not a migration path: do not downgrade.

### Integrity (v2 SSTables)

`v2` adds a per-block CRC32 trailer to every data, index, and bloom block,
verified on read. Silent bit-rot at rest surfaces as an `Io` error
(`sstable: ... block checksum mismatch`) instead of being served as a correct
value or panicking on decode. `v1` files have no trailers and are read without
verification — another reason to migrate them forward.

## Upgrade procedure

ZydecoDB upgrades are **in-place and backward-read-compatible**: a newer binary
opens an older `data_dir` directly. The steps below are the safe sequence.

1. **Back up first.** Capture a base snapshot (offline or against a replica):

   ```bash
   zydecodb admin snapshot --config /etc/zydecodb/config.toml --out /backups/$(date +%F)
   ```

   Keep shipped WAL alongside it if you rely on point-in-time restore (see
   [`SHIPPING.md`](SHIPPING.md) / [`REPLICATION.md`](REPLICATION.md)).

2. **Stop the old server.** A graceful stop writes the clean-shutdown marker and
   flushes the WAL into SSTables.

3. **Swap the binary** and start the new version against the same `data_dir`.
   At startup the engine logs the on-disk SSTable format mix. If any
   legacy-format files remain you will see:

   ```
   WARN on-disk SSTables include legacy-format files (readable; run `admin upgrade` to rewrite)
   ```

   The server is fully operational in this state — legacy files are read
   transparently.

4. **(Optional) Rewrite legacy files now.** To force the migration instead of
   waiting for background compaction to reach every file, stop the server and
   run:

   ```bash
   zydecodb admin upgrade --config /etc/zydecodb/config.toml
   ```

   This forces a full compaction (offline, takes the `data_dir` lock) and then
   reports how many SSTables are at the current format vs. still legacy:

   ```
   upgrade complete: 42 SSTable(s) at current format v2, 0 legacy
   ```

   A small number of settled, non-overlapping files may not be picked by the
   compaction planner and are reported as legacy; they remain readable and are
   rewritten organically as future writes touch their key ranges.

## Downgrade

Downgrading is **not supported**. A newer binary may write a newer SSTable
format (and newer manifest record types) that the older binary refuses to read.
If you must roll back, restore from the snapshot taken in step 1 with the old
binary.

## Quick reference

```bash
# Inspect format mix without changing anything: just start the server and read
# the startup log line (INFO/WARN about SSTable format).

# Force-rewrite legacy SSTables forward (offline):
zydecodb admin upgrade --config /etc/zydecodb/config.toml

# Take a backup before any upgrade:
zydecodb admin snapshot --config /etc/zydecodb/config.toml --out /backups/pre-upgrade
```
