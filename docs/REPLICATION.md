# Replication & failover

ZydecoDB replicates with **filesystem-first WAL shipping**: the primary copies
each sealed WAL segment off-box, and a read replica replays those segments to
stay caught up. The database does no network I/O itself — an operator-supplied
sidecar (rsync, s5cmd, AWS DataSync, ...) moves bytes between hosts. This keeps
the data path simple and auditable.

```
 primary ──ship──> ship_dir ──sidecar (rsync/s3/...)──> replica_from ──replay──> replica (read-only)
```

## How it works

1. **Primary** seals a WAL segment (on roll, or at clean shutdown) and writes a
   byte-identical copy into `[shipping].ship_dir`, appending one line to
   `shipped.log`:

   ```text
   <segment_id> <seal_seq> <sha256_hex> <hmac_hex>
   ```

   The HMAC field is keyed by `[shipping].hmac_key_file` (required) and
   authenticates the manifest entry end to end.

2. **Sidecar** (yours) transports `ship_dir` to the replica host's
   `[replica].from` directory. Order does not matter; the replica enforces it.

3. **Replica** (`--replica-from <dir>`) polls `from`, and for each segment in
   `shipped.log` not yet applied:
   - verifies the file's SHA-256 matches the recorded digest **and** the
     entry's HMAC under the shared key (a partial, corrupt, or forged transfer
     is refused),
   - installs it into its own WAL directory atomically,
   - reopens the engine to replay the new segment (flushing already-applied data
     to SSTables first, so each catch-up replays only the new bytes).

   The replica serves reads and **rejects every write/DDL command with
   `Forbidden`**.

## Configure the primary (ship WAL)

`config/zydecodb.example.toml`:

```toml
[shipping]
ship_dir = "/var/lib/zydecodb/ship"
mode = "hardlink"   # same filesystem; use "copy" across filesystems
# Required: authenticates every shipped.log entry (share with the replica).
hmac_key_file = "/etc/zydecodb/ship.hmac"
```

Point your sidecar at `ship_dir`. Ship the whole directory, including
`shipped.log`. Never delete a segment from `ship_dir` until the replica (and any
archive) has consumed it — the replica needs the full ordered stream.

## Configure the replica (replay WAL)

The replica is just `serve` with a replication source. Give it its **own**
`data_dir` and `wal_dir` (do not share the primary's):

```bash
zydecodb serve --config /etc/zydecodb/replica.toml \
  --replica-from /var/lib/zydecodb/replica_from \
  --replica-poll-ms 1000
```

or in the config file:

```toml
[replica]
from = "/var/lib/zydecodb/replica_from"
poll_ms = 1000
# Required: must match the primary's [shipping] hmac_key_file.
hmac_key_file = "/etc/zydecodb/ship.hmac"
```

## Liveness: the heartbeat

A primary refreshes a `shipped.heartbeat` file in `ship_dir` on a fixed cadence
(`[shipping].heartbeat_ms`, default 1000ms) **even while idle**, so a replica can
tell a *quiet* primary from a *dead* one. The heartbeat records the primary's
wall-clock time and current write sequence, and rides along in `ship_dir` like
the segments. Disable it with `heartbeat_ms = 0`.

## Check status (lag + primary liveness)

`zydecodb replica status` reads the shipped stream and the replica's persisted
position — no connection to the running server required, so it is safe to poll:

```bash
zydecodb replica status --config /etc/zydecodb/replica.toml
# primary_heartbeat: 1s ago
# primary_seq:       1284
# shipped_high_seq:  1280
# applied_seq:       1280
# seq_lag:           4
# caught_up:         true
# healthy:           true (max_stale=10s)
```

It **exits non-zero when the primary's heartbeat is older than `--max-stale-secs`**
(default 10), so an orchestrator can use it directly as a health probe:

```bash
zydecodb replica status --config replica.toml --json --max-stale-secs 5 \
  || echo "primary looks dead -- consider promotion"
```

A running replica also exports `zydecodb_replica_lag_seqs` and
`zydecodb_replica_heartbeat_age_seconds` on its `/metrics` endpoint.

## Failover / promotion (assisted)

Promotion is **assisted, not autonomous**: an external orchestrator (or you)
decides the primary is truly dead — and is responsible for *hard* fencing it
(stop the host / pull its address) — then ZydecoDB automates the node-side
mechanics and applies a cooperative epoch fence.

1. **Confirm the primary is dead and fence it.** Use `replica status` (stale
   heartbeat) plus whatever your platform provides. Make sure the old primary
   cannot keep taking writes. **This step is yours; the database cannot do it.**

2. **Stop ingest.** Stop the sidecar feeding the replica's `from` directory so no
   further segments arrive mid-promotion.

3. **Promote.** With the replica process stopped:

   ```bash
   zydecodb replica promote --config /etc/zydecodb/replica.toml
   # promoted: drained 3 segment(s), epoch 1 -> 2 (applied_seq 1280)
   # next: restart as primary without a replication source -> ...
   ```

   This drains every delivered segment into the WAL and bumps this node's
   promotion **epoch** (in `data_dir/EPOCH`) past anything seen in the stream.

4. **Restart as a primary.** Remove the `[replica].from` setting (and the
   `--replica-from` flag) and start `serve` against the *same* `data_dir` and
   `wal_dir`. The node now accepts writes. Keep `[shipping]` enabled if this new
   primary should feed a replica; on start it stamps its epoch into the stream's
   `FENCE` file.

5. **Redirect clients** to the promoted node's address.

6. **Rebuild a new replica** from the promoted primary (fresh `data_dir` /
   `wal_dir` + `--replica-from`) to restore redundancy.

### The epoch fence (cooperative split-brain guard)

Each node carries a monotonic promotion epoch (`data_dir/EPOCH`, absent = 1).
A primary stamps its epoch into the shipped stream's `FENCE` file on start, and
`promote` bumps the epoch past the fence it observes. If an **old primary wakes
up and re-attaches to the same shipped stream**, it sees a higher `FENCE` epoch
than its own and **refuses to start** rather than create a second writer.

This is *best-effort, cooperative* fencing for the shared-stream case — it is not
hardware fencing. If two nodes can write to physically separate stores, only your
orchestrator's hard fencing prevents divergence. **Never deliberately run two
primaries against the same shipped stream.**

### Notes & limits

- Replication is **asynchronous**: a replica lags the primary by at most one
  un-sealed segment plus transport time. A primary that dies before a segment
  seals can lose the writes in that open segment (the same window as any async
  log-shipping system). Use `durability = "sync"` so acknowledged writes are at
  least locally durable on the primary.
- The replica is **eventually consistent** with the primary and **read-only**.
  Promotion is deliberate; the epoch fence guards the shared-stream case but the
  death decision and hard fencing remain the operator's responsibility.
- **Catch-up path:** after installing new segments, the replica **incrementally
  applies** them into the live engine (`Engine::apply_installed_wal_segment`)
  under the engine lock (flush + replay). A full `Engine::open` reopen is only
  the fallback if incremental apply fails. Catch-up still pauses writers briefly
  while the lock is held; happy-path RTO is bounded by flush+replay of new
  segments, not a cold open of the whole WAL history.
- SSTables are **not** shipped; the replica reconstructs state purely from the
  WAL stream, so the primary must ship every segment and the replica must replay
  them all in order.
- For recovery that does **not** want to replay the full WAL history — or that
  needs a specific point in time — pair a base snapshot with the shipped WAL:
  `zydecodb admin snapshot` captures SSTables + manifest (run it offline or
  against a replica's `data_dir` for zero primary impact), and
  `zydecodb admin restore --base <snap> --wal <ship_dir> --to-seq <N>` lays the
  snapshot down and replays the shipped WAL up to a sequence (or, best-effort,
  `--to-time`). See [`docs/SHIPPING.md`](SHIPPING.md).
