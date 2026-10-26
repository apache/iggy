<!-- markdownlint-disable MD053 -->

- Feature Name: `tiered_storage`
- Start Date: 2026-09-29
- RFC PR: [apache/iggy#0000](https://github.com/apache/iggy/pull/0000)
- Discussion: [apache/iggy#0000](https://github.com/apache/iggy/discussions/0000)
- Iggy Issue: [apache/iggy#0000](https://github.com/apache/iggy/issues/0000)

## Summary

[summary]: #summary

Move sealed segment files from a partition's local directory to an object store (S3 and compatible
stores first, GCS and Azure Blob behind the same abstraction) and serve them back through a local
chunk cache, without changing what clients see. The partition primary uploads a segment once it
seals and commits a `tiered_up_to_offset` watermark to the partition's control log, a replicated
log separate from the data log, where a small state machine applies it on every replica; each
replica may then evict its local copy under a per-topic local retention policy. A tiered
segment stays in the `SegmentedLog` with its metadata, its sparse index and a 128-byte `.remote`
marker on local disk, so offset and timestamp resolution never touch the network. Total retention
is evaluated on the primary over local and remote data together and replicated as the existing
`TruncatePartition` watermark. Object store access goes through an Iggy-owned abstraction with one
compio-native HTTP transport and a small signer per provider; no third-party storage crate and no
tokio runtime enter the server.

## Motivation

[motivation]: #motivation

Local disk alone does not scale to terabyte partitions, and most reads target the tail. Operators
size NVMe for peak retention today and pay for it whether the data is read or not. Tiered storage
keeps hot data on local disk and moves cold data to storage that costs an order of magnitude less,
while reads of that data stay transparent to clients and go through a local cache.

The expected outcomes are:

- Retention expressed in total data rather than local disk size, with a separate, smaller local
  retention envelope.
- Local disk usage bounded by the local envelope, independent of total retention.
- Reads of tiered data served through a cache with one small object store request for a cold poll
  and none for a warm one.
- A bucket layout that is self-describing enough to be the foundation for backups and restores.
- The same tier service, provider layer, key layout and cache reusable by any later storage mode.

This RFC targets the following model: a replicated local log stays the write path, the
acknowledgement never waits on object storage, and sealed segments are copied out afterwards. It
does not propose a diskless architecture. Object-storage-first systems were surveyed for mechanics
worth borrowing (see [Prior art](#prior-art)); the write path they use is rejected here because the
VSR log already sequences and replicates, which is the job their coordinators exist for.

The README currently advertises "data backups and archiving to disk or S3". No archiver exists on
master; this RFC is what makes that sentence true.

## Guide-level explanation

[guide-level-explanation]: #guide-level-explanation

### For operators

Tiered storage is enabled per node in `config.toml` and switched on per topic. A node needs a
backend and, for the object store backends, credentials:

```toml
[tiered_storage]
enabled = true
backend = "s3"                # "s3" | "gcs" | "azure" | "fs"

[tiered_storage.s3]
bucket = "iggy-prod"
region = "eu-central-1"
endpoint = ""                 # empty = AWS; set for MinIO, Ceph, R2
prefix = "iggy"
access_key = ""               # IGGY_TIERED_STORAGE_S3_ACCESS_KEY
secret_key = ""               # IGGY_TIERED_STORAGE_S3_SECRET_KEY

[tiered_storage.cache]
size = "4 GiB"
size_percent = 20             # of the data disk; the smaller wins
chunk_size = "8 MiB"
```

At boot the node probes the bucket once (a one-key LIST) and refuses to start on bad credentials,
endpoint or bucket, so a misconfiguration fails at boot rather than an hour later at the first
upload. The `fs` backend points at a directory on another filesystem and needs no credentials; it
is what tests use and what "archive to disk" means.

A topic opts in with options, at creation or later:

```text
iggy topic create my-stream events --partitions 8 \
  --set tiered_storage=true \
  --set local_retention_age=6h \
  --set local_retention_size=50GiB \
  --set message_expiry=30d
```

From then on:

- `message_expiry` and `max_topic_size` mean total data, local and remote together.
- `local_retention_age` and `local_retention_size` bound what stays on local disk. Data older or
  larger than that is evicted locally once it is safely in the store.
- Turning `tiered_storage` off again is refused while remote segments exist.

The operator sees, per node, Prometheus gauges for local and remote bytes, uploads and fetches with
their latencies, cache hits and misses, and an `upload_stalled` gauge that rises when the store is
unreachable. A store outage never affects producers or consumers of warm data: uploads retry with
backoff and the watermark stops advancing. Segments uploaded before the outage keep being evicted,
so local disk grows past the local envelope only once data written during the outage fills it;
from then on the topic behaves as an untiered one until the store is back.

### For client developers

Nothing changes on the wire. A poll for a range that spans remote and local segments returns one
response, exactly as today. The only visible effect is latency: a poll that touches a segment
nobody has read recently pays one small object store request, typically 30 to 100 ms in-region,
after which neighbouring polls are warm. Polls are served by the partition primary, so that is
where cold reads happen. If the store is slow the poll replies with `TransientNotAccepted` (58)
after a bounded wait, and the SDKs already retry it with backoff until their 30 s call deadline.

Consumer offsets, consumer groups, auto-commit, and time-based polls behave the same. A segment
that a slow consumer has not read yet may be evicted locally, because the data remains readable;
it is never deleted from the store while a committed consumer offset points before it.

### For contributors

A tiered segment is a normal `Segment` whose `location` is `Remote` instead of `Local`. Its
`.log` file is gone, its `.index` file is kept (or fetched on first use), and a `.remote` marker
records every field boot needs to rebuild the segment without the network. Boot recovery treats a
marker-only stem as a legitimate segment; a missing middle `.log` with no marker is still a hole.

The read path resolves a `SegmentSource` per segment on the pump when it builds the poll plan. The
off-pump walk reads local segments with `read_exact_at` as today, and remote segments through a
chunk cache that the shard's own tier tasks fill with ranged GETs. The walk crosses from a remote
segment into a local one by continuing at byte zero of the next segment, whatever its source.

All object store I/O lives in a new `core/tiered_storage` crate behind an `ObjectStore` trait, with
`s3`, `fs` and `sim` implementations in the first delivery and `gcs` and `azure` as later provider
modules behind the same contract tests. Object store requests run as cooperative tasks on the
shard that owns the partition, and never block it.

## Reference-level explanation

[reference-level-explanation]: #reference-level-explanation

### Facts about master this design depends on

All verified on `master @ 6ce2f92a9`.

- Sealed segments hold only committed data and are byte-identical across replicas. Only the primary
  assigns offsets; it stamps `base_offset`, `batch_checksum` and a `base_timestamp` taken from the
  prepare header it assigned, and backups check rather than re-stamp. Payload encryption, when a
  stream has a key, runs once on the primary before consensus, so every replica stores the same
  ciphertext. Segment rolls are keyed on committed bytes: materialization seals at
  `segment.size >= max_size`, the seal completes in `accept_rotation`, and the WAL applies the same
  roll rule at prepare time, before placing the arriving batch. The WAL records `max_size` in its
  durable layout and refuses a different one, and `segment_size` is a create-only topic option, so
  the roll rule never changes under a live partition. The `.index` is not identical: it has one
  24-byte entry per flush chunk, and flush timing differs per replica.
- Every disk-backed partition has a WAL under `prepares-{created_revision}/`, whatever its
  durability. It writes `SendMessages` bodies into the public `.log` at prepare time and keeps hard
  links to segment inodes until its checkpoint passes, so the active `.log` can hold bytes past the
  committed `segment.size`; a sealed segment's committed size is authoritative. Control operations
  are stored inline in the WAL. Boot restores a receipts checkpoint and replays WAL prepares above
  it, control operations included.
- Boot recovery collects segment stems from `*.log` files only and unlinks any `.index` or `.anchor`
  without a `.log` (`segment_recovery.rs`). A gap between consecutive segments is refused as a `Hole`
  unless an `.anchor` covers it, an empty segment in the middle of the chain is refused, and a
  missing front prefix is accepted. Quarantine, converge and the install sweep know only `.log`,
  `.index`, `.staging` and `.anchor`; the install backup hard-links every file in the directory, so
  a new file type is included there automatically.
- All partition file work runs as `PartitionIoJob`s on a per-shard I/O lane (#4311): one active job
  per partition, 16 slots per shard, and a job that outlives `superblock_wedged_fatal_timeout`
  (2 min) stops the node. Retention is local per replica and runs on every shard: a per-shard timer
  (`data_maintenance.messages.interval`, 1 m) hands the pump a `CleanPartition` frame for every
  owned partition with a retention policy, and the pump records a watermark and runs the removal
  as `RemoveSegment` jobs, stopping at the active segment, the WAL checkpoint and the consumer
  barrier (external groups never hold it). A frame the pump refuses waits for the next tick. Only
  sealed segments are removed, so on a topic whose active segment never fills, `message_expiry`
  never removes anything. Every unlink also drops the shard's cached state-transfer offer and
  payload cache, which would otherwise name deleted files.
- Client-driven `DeleteSegments` resolves a count to an offset on the partition primary and commits
  `TruncatePartition`, which raises `Partition.deleted_up_to_offset` in the metadata plane; the
  reconciler enforces it on every replica, staging the removal on the owner shard's pump as soon as
  it observes the committed watermark rather than waiting for the cleaner's timer. Topic and partition deletes are two-phase: the primary
  commits `TransitionPartitionHistory` to the partition log, after which the incarnation's history
  is deleted for good and requests get `HistoryUnavailable` (87); metadata then completes the delete
  with `CompleteLifecycle` and the reconciler removes the directory. Some partition records live
  outside the partition directory, under `partition-initialization/{namespace}/`.
- Segments contain only message batches. The partition log interleaves `SendMessages` with control
  operations (consumer offsets, `RetireSession`, and the consumer-group and history fences 167 to
  169), but the flush writes only message batches, and message offsets are stamped independently of
  op numbers, so no offset translation exists or is needed. Master already keeps replicated
  partition state outside segments: the fences have their own apply hook
  (`apply_committed_lifecycle`), and that state, with dedup receipts, is persisted in the receipts
  checkpoint and carried in state transfer inside `ConsumerOffsetsWire` (version 8, strictly
  versioned). There is no general state-machine facility and no separate control log on the
  partition plane; the metadata plane has one (`StateHandler`, `collect_handlers!`, versioned
  snapshots) and holds per-partition facts such as `deleted_up_to_offset`.
- Every node replicates every partition (`view % replica_count` picks the primary). Since #4506 all
  polls, over TCP and HTTP, are served by the partition primary and backups serve none; since #4619
  primaries are spread across nodes at creation and rebalanced one at a time through a drained view
  change, and the drain waits only for pending history writes. The metadata plane is one group on
  shard 0 of every node.
- Disk polls are planned on the pump and executed as a detached compio task on the same thread
  (`bus.spawn`), returning through a bounded completion lane. The requester waits at most
  `PARTITION_READ_TIMEOUT` (10 s); expiry answers 11001, which the SDK does not retry, while
  `TransientNotAccepted` (58) is retried until the SDK's 30 s deadline. The poll plan has no error
  channel: a failed disk walk returns an empty poll. The walk reads chunks of 64 KiB to 1 MiB and
  continues into the next segment from byte zero.
- Consensus repair never reads sealed segment bytes. Local presence of sealed segments matters in
  state-transfer serving (the offer is the whole local chain), the WAL's hard links, retention,
  `install_backup`, and boot chain validation. WAL recovery can also re-create the public `.log` of
  the WAL's tail segment, but never of a fully checkpointed sealed segment.
- The partition superblock payload may be 64 KiB (`SUPERBLOCK_VERSION` 2), but `VsrState` is decoded
  at exact lengths, so a new field is a `RECOVERY_VERSION` bump. Upgrades already require fresh
  storage, and the replica handshake refuses peers with a different release or storage format.
- Shards run with `thread_pool_limit(0)` except on macOS, so `spawn_blocking` panics a shard;
  `run_blocking` offloads one blocking call at a time per shard to its own OS thread. The server
  builds no tokio runtime. `cyper` (a compio-native HTTP client over rustls) is already used for
  JWKS and HTTP forwarding. Server metrics have no histograms, and there is no counting semaphore or
  token bucket primitive.
- Topic options live in a catalog (#4615). A key is an `OptionKey` variant plus a `CATALOG`
  descriptor; mutability is create-only, updatable or read-only; defaults are compile-time catalog
  constants, not server configuration; a `critical` key makes a node that does not understand it
  refuse the resource. Updatable retention options are re-read by the cleaner on every pass.

### Object layout

```text
{prefix}/{cluster_id}/{stream_id}/{topic_id}/{partition_id}/{created_revision}/{start_offset:020}.log
{prefix}/{cluster_id}/{stream_id}/{topic_id}/{partition_id}/{created_revision}/{start_offset:020}.index
{prefix}/{cluster_id}/meta/{stream_id}/{topic_id}/{partition_id}/{created_revision}/manifest.bin
```

Stream, topic and partition ids are slab-allocated and reused after deletion, so
`Partition.created_revision` (unique across the cluster) identifies the incarnation. Offsets never
restart within an incarnation: the offset frontier is durable and never rewound, and
`DeleteSegments` only trims from the front, so `created_revision` alone separates every offset
space. `cluster_id` lets several clusters share one bucket. The zero-padded stem matches the on-disk name so a listing sorts by
offset.

Objects are immutable. The `.log` object is canonical and identical whichever replica produced it,
and for an encrypted stream it is the same ciphertext the segment holds.
The `.index` object is the uploader's sparse index; any sparse index whose entries point at batch
starts is valid for lookups. User metadata on the `.log` object carries `end_offset`,
`start_timestamp`, `end_timestamp`, `max_timestamp`, `size`, `index_size` and a format version, so a
listing plus HEAD rebuilds segment metadata without a download.

### The local marker

A tiered segment leaves `{start:020}.remote`, a fixed 128-byte record modelled on `.anchor`:

```text
magic "IGGYREMT"   u8[8]
version            u16      flags u16      reserved u32
start_offset       u64      end_offset     u64
size               u64      index_size     u64
start_timestamp    u64      end_timestamp  u64      max_timestamp u64
created_revision   u64      reserved       u64
log_checksum       u64      (the state-transfer artifact hash of the .log, XxHash3-64)
record_checksum    u64      padding to 128
```

The marker is written and directory-fsynced before the `.log` is unlinked, the ordering `.anchor`
uses.

Boot rules:

- A stem with a marker and no `.log` is a tiered segment. Its `.index` is kept; today's sweep unlinks
  any `.index` without a `.log`, so the sweep learns `.remote` stems. A missing `.index` is fetched
  on first read.
- A stem with both a marker and a `.log` is local. The marker records that a remote copy exists so
  eviction can resume without re-uploading.
- `ensure_contiguous_chain` uses marker bounds for tiered segments. A missing middle `.log` with no
  marker is still a `Hole`.
- Tiered segments are always a prefix of the chain. A marker after a local sealed segment is a
  refusal.
- Marker `created_revision` must match the partition's; a stale marker is quarantined with the
  partition.
- Quarantine, converge and the install sweep handle `.remote` alongside `.log`, `.index`, `.staging`
  and `.anchor`. The install backup needs no change, since it links every file.

In memory, `Segment` gains `location: SegmentLocation` with `Local`, `Uploaded` (both copies) and
`Remote`. Tiered segments stay in `SegmentedLog.segments`, so `disk_poll_start` keeps resolving the
start segment by offset or by `max_timestamp` exactly as it does now.

### Roles

#### Primary: upload and floor advancement

Upload is primary-gated. Two triggers feed one queue: `accept_rotation` enqueues the just-sealed
segment when the topic is tiered and the partition is primary, and a per-shard upload loop scans
owned primary partitions for sealed segments above the committed floor that are not in flight. The
loop backs off exponentially from 100 ms to 10 s while it finds nothing. Uploads are oldest-first
per partition, one in flight per partition, so the floor advances contiguously. Node-wide
concurrency scales with the upload backlog between one and `upload_concurrency`. A partition whose
local eviction is blocked because its floor lags is flagged as under local storage pressure and its
uploads go to the head of the queue. A failed upload retries with backoff and blocks the floor.

Primaries move. A rebalance drains only pending history writes, so an upload in flight on the old
primary is abandoned and the new primary's scan uploads the segment again; identical bytes and the
conditional PUT make the repeat harmless. Upload reads the sealed `.log` up to its committed
`segment.size`, through its own file descriptor, which keeps working if retention unlinks the file
mid-upload.

On success the primary commits `AdvanceTieredFloor { up_to_offset }` to the partition's control
log. Only the primary submits it, and a stale primary's command is rejected by the view of whichever
group realizes that log, so no further fencing is needed. Every replica applies it through the
partition state machine below.

#### Every replica: local eviction

Eviction follows the retention pattern: a pass on the pump records a local-eviction watermark and
the partition's I/O lane runs one `EvictSegment` job per segment, the way it runs `RemoveSegment`
today. Passes are staged two ways. First, a floor that rises stages one on the owner shard's pump
as soon as it applies, as `TruncatePartition` enforcement is staged today. With a per-shard control
group the apply already runs on the owner shard; on the metadata plane the reconciler stages the
pass when it observes the raised floor for an owned partition. Second, the cleaner's timer stages
one, which covers segments that age out of the local
envelope with no floor change and any staging frame the pump refused. A pass is a no-op for a
partition inside its local envelope, so staging on every raise costs one pump frame per uploaded
segment, and a partition under local storage pressure starts evicting when its floor applies
rather than up to one cleaner interval later.

The eligible run starts at the front and stops at the first of: the active segment,
`end_offset > tiered_up_to_offset`, `end_offset >= persistence checkpoint`, or the local retention
budget being satisfied. It does not consult the consumer barrier. Per segment the job writes the
marker, fsyncs the directory and unlinks the `.log`; on acceptance the owner flips `location` to
`Remote`, drops the segment's sealed read handle and, as retention does after an unlink, the
shard's cached state-transfer offer, and leaves `PartitionStats.size_bytes` untouched.
One segment per job keeps eviction from holding the partition's single I/O slot ahead of
materialization.

#### Partition state machine

The floor lives in a replicated state machine fed by the partition's **control log**: a consensus
sequence separate from the data log, so that segments and the data journal hold only user messages
and control commands never enter the data path. This RFC does not fix how the control log is
realized. A general partition state-machine facility, specified in its own RFC because queue
semantics, log compaction and the key-value store need it as well, decides between one control
group per shard carrying the commands of every partition that shard owns, and the metadata plane,
which already is a separate control log holding per-partition facts. Master already applies,
checkpoints and transfers its own replicated partition state (the fences and dedup receipts), but
in the data log and without a general facility; the facility generalizes that. The floor advances
at most once per uploaded segment, so it works with either realization; this RFC depends on the
facility only for the command encoding, the apply hook, the durable snapshot and state-transfer
inclusion.

The tiered storage state machine holds one value, `tiered_up_to_offset`, and its apply is
`max(current, up_to_offset)`, checked against the partition's committed end because the control log
is not ordered with the data log. It holds no per-segment data on purpose: deterministic keys plus
the local markers carry the segment list, so the snapshot stays a few bytes. Where it is persisted
follows the realization. On a partition-plane control log it joins the replicated partition state
master already keeps: the receipts checkpoint at rest and `ConsumerOffsetsWire` in state transfer,
both strictly versioned, so both get a version bump. On the metadata plane it is a field of the
metadata `Partition` with a snapshot version bump. The superblock is a poor fit: its writes are
driven by view and offset-reservation intents, not by applies, and `VsrState` decodes at exact
lengths. Either way the floor rides state transfer, so a rebuilt replica never starts at zero. The
floor is monotone for the life of the partition incarnation, and eviction reads it directly from
the partition.

`TruncatePartition` stays in the metadata plane, where it already ships. If the facility settles on
the metadata plane, both watermarks share a log from the start; if it settles on per-shard control
groups, migrating `TruncatePartition` is a follow-up amendment (see unresolved questions).

### Segment lifecycle

| State | Local files | Remote | Transition out |
| --- | --- | --- | --- |
| Active | `.log` (written by the WAL), `.index` | none | rotation seals it |
| Sealed, local | `.log`, `.index` | none | primary upload succeeds |
| Uploaded | `.log`, `.index` | `.log`, `.index` | floor committed and local retention exceeded |
| Remote | `.remote`, `.index` (optional) | `.log`, `.index` | total retention raises `deleted_up_to_offset` |
| Retired | none | deleted | terminal |

A segment never moves back from Remote to Sealed-local in the first delivery.

#### Bounding how long data stays unuploaded

Uploading part of the active segment would create objects that match no local file, and a replica
cannot roll by its own clock because segment boundaries must be a function of the batches alone.
Batch timestamps are such a function: the primary assigns them in the prepare header and every
replica stores them unchanged. So the age bound is a roll rule keyed on them, with no new
operation: a topic option `segment_max_age`, and an arriving batch whose timestamp is at least
`segment_max_age` past the active segment's first timestamp seals the active segment and starts
the next one. Every replica evaluates the rule on the same two values and seals at the same batch,
so segments stay byte-identical, and the sealed segment uploads normally. The rule needs no clock
monotonicity: a primary whose clock lags only delays a roll.

Rolls are decided in two places, at materialization and in the WAL at prepare time, and both must
apply the new rule identically. The WAL already rolls before placing an arriving batch, so the rule
sits beside its size check, and its fixed-size durable segment state, which records `max_size` and
the tail position, gains `segment_max_age` and the tail segment's first timestamp: a WAL format
change. `segment_max_age` is create-only for the reason `segment_size` is: an update arrives through
the metadata plane, which is not ordered with the partition's data log, so replicas would apply it
at different batches and roll differently.

The rule covers steady traffic and traffic with long gaps, since the first batch after a gap seals
the old segment. It is not a tiering option. On any topic that keeps receiving writes it bounds how
long the active segment stays unsealed, which also lets `message_expiry` reach data on a topic that
never fills a segment. A fully idle partition is the case it leaves: nothing arrives to trigger the
roll, so its last segment stays local and unexpired until the next write. Closing that needs a roll
ordered with the batches, which means an operation in the data log, which is why master's fences
live there; it is left open. Small segments produced by the age roll require an adjacent-segment merger, which
will have to be implemented.

### Read path

`DiskReadPlan` carries a `SegmentSource` per snapshot segment, `Local { path }` or
`Remote { key, size, index_state }`, decided on the pump from `Segment.location`.
`resolve_segment_file` becomes the dispatch point.

#### Chunk cache

A directory under the node's data path holds chunks of remote `.log` objects as
`{partition_key}/{start:020}.log_chunks/{chunk_start}`. Chunk boundaries snap to batch boundaries:
chunk `k` starts at the sparse index entry nearest to `k × chunk_size` (default 8 MiB), so a batch
never costs two GETs. A miss hands `FetchChunk` to the shard's tier service, which reserves the
bytes in the cache budget first, issues one ranged GET, writes a `.part` file, fsyncs it and renames
it into place.

A cold poll does not wait for a whole chunk: the tier service first fetches exactly the walk's range,
64 KiB to 1 MiB, into a small cache file the walk reads immediately, and hydrates the surrounding
chunk in the background.

Readahead is adaptive: one chunk, doubling on consecutive sequential hits up to `readahead_max`
(default 4); a reader that stops leaves its prefetched chunks first in line for eviction. Each shard
has at most one outstanding fetch per chunk, so two consumers reading the same cold segment share
one GET. A poll whose start offset is further behind the head than the cache could hold is a cold
reader: its chunks enter the LRU at the tail so a backfill cannot evict the working set near the
head.

Space is reserved before a download. The budget is the smaller of `size` and `size_percent` of the
data disk, with an object-count cap. Eviction is LRU by access time down to 80 % of the budget, with
at least 5 s between trims. The cache is partitioned by shard, because a partition's chunks are
only ever read by the shard that owns it, so each shard runs its own LRU and budget with no
cross-shard coordination. The cache persists across restarts: each shard keeps an access-time table
for its files, saves it every 60 s and at shutdown, and at boot walks its directory before serving,
deletes leftover `.part` files and reconciles the table. Downloads are rate-limited by a per-shard
token bucket derived from the node-level `fetch_bandwidth`.

#### Indexes for remote segments

`resolve_sealed_start` prefers the cached index, then the local `.index`. For a remote segment with
no local index it asks the tier service for the `.index` object, stores it under the canonical name,
and proceeds. The 512 KiB residency cap applies unchanged.

#### Latency, timeouts and errors

The poll plan has no error channel today: a failed disk walk returns an empty poll. Tiering adds one,
so that a store error or an expired `fetch_timeout` (default 5 s) replies `TransientNotAccepted`
(58), which the SDK retries; the fetch keeps running so the retry hits the cache. The reply must
arrive well inside the 10 s `PARTITION_READ_TIMEOUT`, because its expiry answers 11001, which the
SDK does not retry, and an HTTP poll has 10 s in total including any relay to the primary.
`fetch_concurrency × fetch_timeout` worth of completion-lane slots can be held by cold reads, so
`sharding.poll_completion_capacity` must exceed that. Per-batch `batch_checksum` validation, on by
default through `[partition] validate_checksum`, is forced on for remote chunks.

Only the partition primary serves polls, so cold reads and uploads happen on the same node. A
backup's cache is used only once it becomes primary, and after a rebalance the new primary's cache
is cold for every remote segment.

### Retention

| Envelope | Options | Applies to | Decided by | Consumer barrier |
| --- | --- | --- | --- | --- |
| Local | `local_retention_age`, `local_retention_size` | uploaded sealed segments | each replica, locally | no |
| Total | `message_expiry`, `max_topic_size`, `DeleteSegments` | local and remote together | primary, replicated as `TruncatePartition` | yes |

For a tiered topic the cleaner on the primary evaluates `leading_expired_end` and
`leading_oversized_end` over the full chain (remote segments count their marker `size` and
`max_timestamp`), clamps to the consumer barrier, and submits `TruncatePartition { up_to }` when the
watermark advances. The reconciler's truncate enforcement handles remote segments: the
`RemoveSegment` job unlinks the marker and index of a remote segment as it unlinks `.log`, `.index`
and `.anchor` today, and acceptance retires it and decrements stats. External consumer groups never
hold the barrier, as today. On the primary it additionally deletes objects at or below
the watermark, idempotently, bounded per pass (300) and per partition pending (5000), with one LIST
per partition after restart to re-establish its cursor. Deletion is two-phase with a grace period:
an object becomes deletable only `delete_grace` (60 s) after the watermark that retired it was
committed, and no housekeeping job deletes any object younger than `delete_grace`, whatever the
reason. The grace closes the window in which a cold poll that resolved a key just before the
truncation would meet a 404, and it protects a freshly uploaded object whose floor command has not
landed yet from every sweep.

`PartitionStats.size_bytes` keeps meaning total retained bytes. Local bytes become a Prometheus
gauge.

### Tier service and backends

All tier work runs on the shard that owns the partition, as cooperative compio tasks, with no extra
threads and no cross-thread channels. Each shard has one `TierService` (an `Rc`, like the
thread-local `cyper` client the JWKS code already keeps) owning the HTTP client, the signer, the
shard's slice of the cache, and the per-shard caps. Jobs are `UploadSegment`, `FetchChunk`,
`FetchIndex`, `DeleteObjects` and `ListPrefix`; each runs as a detached task spawned the way disk
polls are (`bus.spawn`), never inline in a frame handler, so the pump's `select_biased!` loop,
including the consensus tick, keeps running between the task's awaits.

Two rules keep a 1 GiB upload from delaying the pump. First, work is done in pieces: a segment is
read and sent `upload_piece_size` (1 MiB) at a time, a chunk is written and hashed the same way, and
every piece ends in a real I/O await (a `read_at`, a socket write) or, for CPU-only stretches such
as hashing and decoding, in `yield_to_reactor()` from `server_common`, as state transfer does on
its long walks. Second, concurrency is capped per class with a small single-threaded semaphore written for this
purpose: an `Rc<RefCell<_>>` holding a permit count and a FIFO of waiters, `acquire().await`
returning an RAII permit released on drop, no atomics because nothing crosses threads. The same
primitive with permits counted in bytes is the cache reservation (reserve before download) and the
download token bucket. One semaphore per class gives the priority order the design needs: fetches
for waiting polls hold their own permits and are never starved by uploads; uploads for partitions
under local storage pressure are queued ahead of ordinary uploads; housekeeping runs only when the
shard's request rate has been below `housekeeping.idle_threshold_rps` for
`housekeeping.idle_timeout`, or once per `housekeeping.interval`, spending at most
`housekeeping.request_quota` requests per run.

Tier jobs deliberately do not run on the partition I/O lane. The lane holds one job per partition
and stops the node when a job outlives its two-minute deadline, so a 1 GiB upload there would block
the partition's materialization and could take the node down on a slow store. Only the local file
mutations, eviction and the marker and index removals, go through the lane. Nothing in master
provides the counting semaphore: `LocalGate` is a binary gate and the existing admission counters
have no wait queue.

#### The abstraction

```rust
pub trait ObjectStore {
    async fn put(&self, key: &str, body: Frozen<4096>, meta: &ObjectMeta, if_absent: bool)
        -> Result<PutOutcome, ObjectStoreError>;
    async fn get_range(&self, key: &str, range: Range<u64>) -> Result<Owned<4096>, ObjectStoreError>;
    async fn head(&self, key: &str) -> Result<Option<ObjectMeta>, ObjectStoreError>;
    async fn delete(&self, keys: &[String]) -> Result<(), ObjectStoreError>;
    async fn list(&self, prefix: &str, after: Option<&str>) -> Result<ListPage, ObjectStoreError>;
}
```

Multipart methods exist for objects above 5 GiB, which Iggy cannot produce; a segment is one
streamed PUT with an exact `Content-Length`.

Three layers:

1. The trait. Plain string keys, the aligned buffers the server already uses, one logical operation
   per method, and a typed error enum (`NotFound`, `PreconditionFailed`, `Throttled`, `Transport`,
   `Auth`, `Malformed`). Retries, backoff and timeouts live above the trait in the tier service.
2. The provider: a signer, a URL layout and a response parser, pure and sans-I/O, unit-tested against
   recorded fixtures.

   | Provider | Auth | Conditional create | Covers |
   | --- | --- | --- | --- |
   | `s3` | SigV4, `UNSIGNED-PAYLOAD` for PUT bodies | `If-None-Match: *` | AWS S3, MinIO, Ceph RGW, R2, GCS interop |
   | `gcs` | S3 XML dialect with an OAuth bearer | `x-goog-if-generation-match: 0` | GCS without HMAC keys |
   | `azure` | SharedKey, SAS, or OAuth bearer | `If-None-Match: *` | Azure Blob, hierarchical namespaces detected |
   | `fs` | none | `O_EXCL` | a directory; Docker-free tests |
   | `sim` | none | map insert | the deterministic simulator, with fault injection |

   The S3 signer may be `rusty-s3` (sans-I/O, MIT/Apache-2.0) or hand-written SigV4; GCS and Azure
   signers are hand-written.
3. The transport: one `cyper::Client` per shard over rustls, a connection pool per endpoint,
   connections idle longer than 5 s closed before reuse, a lease watchdog per request, and a boot
   self-configuration probe that picks virtual-host or path style and fails fast.

Credentials are applied per request through a shared, hot-swappable applier. Static config and
environment variables ship first; provider chains (IMDSv2 and STS web identity, GCE metadata, Azure
workload and VM identity) are each an HTTP call on the same transport, refreshed at 90 % of
lifetime, with a rate-limited out-of-band refresh on 401 or 403.

Every operation runs under a deadline tree. A retry is permitted
only while `now + backoff` stays inside the parent's deadline, and the remaining time is the
per-request timeout. Backoff is exponential with jitter from 100 ms. Provider errors map to `retry`,
`fail`, `not_found`, `auth` or `unsupported` before any retry logic sees them. Uploads retry within
a 90 s deadline per segment.

### Cluster interactions

- **State transfer.** The manifest gains a `REMOTE_CHAIN` artifact carrying all of a partition's
  markers (the manifest caps at 65536 entries, so one artifact rather than one per segment). The
  receiver writes the markers and fetches indexes lazily, and the state machine's floor travels with
  the offsets artifact the way consumer offsets do, so an installed replica never starts at floor
  zero. No segment bytes cross between replicas for tiered data. The install backup links markers
  like every other file; quarantine, converge and the install sweep handle `.remote` alongside
  `.log`, `.index`, `.staging` and `.anchor`.
- **Delete.** Deletes are two-phase. Once the history fence commits, the incarnation's requests are
  refused with `HistoryUnavailable`, so no floor command for it can commit, and when metadata
  completes the delete the reconciler removes the directory, markers included. Objects of a deleted
  incarnation are removed by an orphan sweep on the metadata primary: it lists `{prefix}/{cluster_id}/` at incarnation
  granularity under a metadata revision snapshot and deletes any incarnation absent from metadata
  with `created_revision` below the snapshot.
- **Manifest object.** Housekeeping on the primary uploads `manifest.bin` under `meta/` whenever the
  remote set changed, at most every 60 s. It is a derived artifact, never the source of truth for a
  live cluster; it exists so a partition can be restored into an empty cluster by reading one object
  and planting markers.
- **Verify pass.** A housekeeping job HEADs every key below the floor, records misses per partition,
  and compares the checksum recorded in each object's user metadata with the marker's
  `log_checksum`, so a divergent or corrupted upload is reported as an anomaly instead of being
  found by a reader. Missing or mismatched objects above `deleted_up_to_offset` are data loss and are
  never repaired automatically.

### Configuration

```toml
[tiered_storage]
enabled = false
backend = "s3"
upload_concurrency = 2               # per shard
upload_piece_size = "1 MiB"          # read, hash and send unit; one yield per piece
upload_loop_backoff_max = "10 s"
segment_upload_timeout = "90 s"
fetch_concurrency = 4                # per shard
fetch_timeout = "5 s"
fetch_bandwidth = "unlimited"        # per node, divided across shards
connection_idle_timeout = "5 s"
delete_batch_per_pass = 300
delete_pending_per_partition = 5000
delete_grace = "60 s"                # no object is deleted sooner than this after it became deletable

[tiered_storage.housekeeping]
interval = "5 m"
idle_timeout = "10 s"
idle_threshold_rps = 10
request_quota = 5000
orphan_sweep_interval = "1 h"
manifest_upload_interval = "60 s"

[tiered_storage.s3]
bucket = ""
region = "us-east-1"
endpoint = ""
url_style = "auto"
prefix = "iggy"
access_key = ""
secret_key = ""
credentials_source = "static"
conditional_writes = true

[tiered_storage.fs]
path = ""

[tiered_storage.cache]
path = ""
size = "4 GiB"
size_percent = 20
max_objects = 100000
chunk_size = "8 MiB"
readahead_max = 4
low_watermark_percent = 80
trim_min_interval = "5 s"
```

Validation: `enabled = true` requires a backend with its required fields; `fetch_timeout < 10 s`;
`chunk_size` a multiple of 4096 within 1 MiB to 64 MiB; cache size at least
`2 × chunk_size × fetch_concurrency × shard_count`. The section joins the root `ServerConfig` with
`#[serde(default)]` and an entry in the embedded `config.toml`, and new sub-structs use
`deny_unknown_fields`, as the cluster sections do. Credentials use `#[serde(default,
skip_serializing)]` with `#[config_env(secret)]`, the pattern the cluster secrets use, so they never
appear in printed or serialized configuration.

Topic options are catalog keys. Their defaults are catalog constants, not server configuration, so
there are no server-level local-retention defaults.

| Key | Kind | Mutability | Default | Critical | Notes |
| --- | --- | --- | --- | --- | --- |
| `tiered_storage` | Bool | updatable | off | yes | admission refuses on to off while remote segments exist, since the catalog has no "off to on only" mode |
| `local_retention_age` | expiry-style | updatable | 1 day | no | needs a type that tells "server default" from "unlimited", as `message_expiry` does; `IggyDuration` parses both as 0 |
| `local_retention_size` | size-style | updatable | unlimited | no | per topic, divided by partition count like `max_topic_size`, and typed like it for the same reason |
| `segment_max_age` | expiry-style | create-only | never | yes | age roll on any topic, tiered or not; create-only like `segment_size` |

`tiered_storage` is critical so that a node that does not understand it refuses the topic instead of
serving it without tiering. `segment_max_age` is critical because a node that ignored it would roll
at different batches from its peers.

### Invariants

1. A replica evicts a local segment only when `end_offset <= tiered_up_to_offset` as committed in
   the partition's control log, and the floor advances only after the primary observed a successful
   PUT of both objects. No replica ever drops the last copy of committed data.
2. Object keys are unique per partition incarnation, objects are immutable, and
   `.log` bytes are identical from any replica. A duplicate upload is a no-op.
3. Local eviction never crosses the WAL checkpoint, the active segment, or a local sealed segment not
   yet uploaded. Tiered segments are always a prefix of the chain.
4. Boot accepts a chain whose prefix is markers and refuses any other absence of a `.log`.
5. Reads are transparent: a poll walk crosses a remote-to-local boundary by continuing at byte zero
   of the next segment, whichever source it has.
6. Deletion from the store is decided by the primary and replicated as an offset watermark, never by
   per-replica clocks or counts.
7. The tiered floor is monotone for the life of a partition incarnation.
8. The consumer barrier applies to total retention and to nothing else.
9. A segment with a local `.log` is read locally. The store is consulted only for segments whose
   local copy is gone.
10. Correctness never depends on object-store conditional writes. A conditional PUT is a cost
    optimization with a HEAD-then-PUT fallback.

### Failure handling

| Failure | Behaviour |
| --- | --- |
| Store unreachable during upload | Retry with backoff; floor stays; eviction waits; producers and consumers unaffected; stalled gauge rises. Local data overshoots the local envelope until the store returns, and the overshoot is reported as a gauge; the only hard limit is the disk. |
| Store unreachable during a cold poll | `TransientNotAccepted` (58) after `fetch_timeout`, inside the 10 s request timeout; SDK retries; warm data keeps serving. |
| Primary dies or is moved by a rebalance mid-upload | The rebalance does not wait for uploads; the new primary re-uploads, and identical bytes plus the conditional PUT make the repeat harmless. |
| Crash between marker write and `.log` unlink | Boot sees both; treats the segment as local; eviction resumes without re-upload. |
| Crash between unlink and directory fsync | Marker was fsynced first, so the `.log` either reappears or is gone with a valid marker. No hole. |
| Object missing on GET | Below `deleted_up_to_offset`: retire locally and clamp forward. Otherwise data loss: fail the poll with an error rather than today's empty poll, and raise a metric. |
| Replica behind on the control log | Has not applied the latest floor yet, so it evicts less. Reads unaffected. |
| Replica restarts or is rebuilt by state transfer | The floor is restored from its durable home or arrives with state transfer; it never starts at zero, so nothing is re-uploaded or wrongly kept. |
| Topic or partition deleted during an upload | Upload lands under the deleted incarnation's prefix; once the history fence commits, the floor command is refused with `HistoryUnavailable`; the orphan sweep deletes the objects. |
| WAL still hard-links an evicted segment | Every partition has a WAL. Eviction stops at the WAL checkpoint; after it, unlinking the public name is what retention does today, and WAL recovery never re-creates a fully checkpointed sealed segment. |
| Cache directory full | Fetch fails typed; poll replies transient; eviction is driven harder; gauge rises. |
| Node restart | Markers rebuild segment metadata; the cache survives via its access-time table. |
| Clock skew against the store | One re-sign with the offset from the rejection's `Date` header; a second failure is a hard error. |

### Implementation plan

Each phase is one reviewable PR or a short stack. The phases form a graph, not a chain:

| Phase | Needs |
| --- | --- |
| 1. Local model and the `fs` store | nothing |
| 2. The floor | the partition state-machine facility RFC |
| 3. Tier service and S3 | phase 1, for the trait and the semaphore only |
| 4. End to end | phases 1, 2 and 3 |
| 5. Age roll | nothing |
| 6. The bucket as an archive | phase 4; the GCS and Azure providers need only phase 3 |

Phases 1, 3 and 5 can proceed in parallel. Phase 2 is the only one that waits on another RFC, and
phase 4 is where the branches join.

1. **Local model without a network.** The `ObjectStore` trait with its typed errors, the `fs`
   provider and the provider contract test suite, and the single-threaded semaphore with its
   byte-counted variant. On top of them: `SegmentLocation`, the marker codec, boot sweep and chain
   acceptance, quarantine, converge and install-sweep handling, the `EvictSegment` I/O job driven by
   a test-set floor, `SegmentSource` in the poll plan, and the chunk cache with index-snapped
   boundaries, reservation and the persisted access-time table, all against the `fs` backend.
   Docker-free tests.
2. **Partition state machine and the floor.** Starts once the partition state-machine facility
   lands, including its choice of control log. The `AdvanceTieredFloor` command and its apply, the
   floor's durable snapshot and its inclusion in state transfer. Tests: a partitions unit test that
   the floor is monotone and survives a restart; a simulator scenario across view changes and
   restarts.
3. **Tier service and S3 provider.** The per-shard `TierService` with piece-wise tasks, the S3
   provider over `cyper` with single streamed PUTs, unsigned payloads, conditional create, the boot
   probe and the deadline-tree retry model, the `sim` backend, config, Prometheus counters, and the
   pump tick latency measurement, the server's first latency distribution metric. Provider contract
   tests against `fs` and MinIO; a shard test that a 1 GiB upload never delays the tick beyond one
   piece. The piece size and caps it ships are provisional until phase 4 measures them.
4. **End to end.** The upload loop, eviction staged on floor apply, the poll error channel, topic
   options, the local and total retention split with the reconciler's truncate enforcement extended
   to remote segments, idle-gated housekeeping with primary object deletion, deletion caps and the
   orphan sweep, the download token bucket, the `REMOTE_CHAIN` artifact. Integration scenarios
   under `--features vsr`. The same PR carries the measurement that settles phase 3's defaults: on
   one shard, an upload backlog, a cold-read storm, eviction passes and primary object deletion
   running together, against the pump tick latency gauge. An upload backlog first exists here, so
   the defaults cannot be settled earlier.
5. **Age roll.** `segment_max_age` as a general topic option, the timestamp roll rule in
   materialization and in the WAL, and the WAL format change. Independent of tiering, so it can
   land first. Tests: the WAL and materialization rules seal at the same batch, including after WAL
   replay, under the `SimStorage` harness; a three-node scenario in which every replica's sealed
   segments stay identical across a view change; and an integration test that `message_expiry`
   removes data on a topic that never fills a segment.
6. **The bucket as an archive.** Lazy manifests under `meta/`, shallow restore, the verify pass,
   and the GCS and Azure providers, which can land any time after phase 3. Anything that watches
   manifests, restore included, detects change by ETag: one LIST of a `meta/` prefix returns the
   ETag of every partition manifest under it (1000 keys per page), and a conditional GET with
   `If-None-Match` costs a 304 when nothing changed.

Later, outside the plan: the adjacent-segment merger, read-replica topics, stats wire fields, CLI
and Web UI visibility, rehydration so tiering can be turned off, compression at upload with chunk
framing, advisory local retention once a disk-space manager exists.

## Drawbacks

[drawbacks]: #drawbacks

- One command per uploaded segment in the partition's control log, and a dependency on a partition
  state-machine facility and a control log that master has only in part: it applies, checkpoints
  and transfers its own fences, but in the data log and without a general facility. At the default
  1 GiB segment size the operation rate is negligible; with a short `segment_max_age` on many
  slow partitions it could become chatty, one floor command per small segment.
- A new crate with its own HTTP client, signers and provider parsers is code the project has to own
  and keep correct against three providers' quirks, where a vendor SDK would have absorbed them.
- Tier work shares the shard's thread with producers and consumers, and compio has no scheduling
  groups to cap its CPU share. Piece-wise work with explicit yields bounds how long the pump waits
  for any one piece, and the per-shard caps bound how many pieces are in flight, but a shard with
  many cold readers and a large upload backlog will give tiering more of its CPU than a
  shares-based scheduler would.
- The state-transfer manifest, boot recovery, quarantine, converge and the install sweep all learn
  about a new file type. Every one of those paths has crash-consistency rules that must be extended.
- Cold reads add a latency mode clients have not seen before. A poll can now take tens to hundreds
  of milliseconds and can reply with a transient error, which SDKs handle but users must expect.
- Turning tiering off is refused while remote data exists, which is a one-way door until
  rehydration is built.
- A quiet partition at the default segment size holds its active segment locally until the age roll
  lands, and a fully idle one holds it until its next write even then, so the bucket can trail the
  log by up to one segment per partition.
- Uploading at seal stores the local envelope twice, on disk and in the bucket, and uploads data
  that total retention may delete before it ever leaves the envelope. A topic whose total retention
  is close to its local retention pays for tiering and gets little from it.

## Rationale and alternatives

[rationale-and-alternatives]: #rationale-and-alternatives

### Why upload at seal rather than when a segment leaves the local envelope

The alternative uploads a segment only once local retention wants it gone. That saves storing the
local window twice and skips uploading data that total retention deletes first, and it is worse on
every other count:

- **Backup.** Uploading at seal keeps the bucket at most one segment per partition behind the log,
  bounded in time once the age roll lands. Uploading at the envelope's edge leaves the whole local
  window, hours or days of data, with no copy outside the cluster, and the bucket can serve as an
  archive and restore source only if it is close to complete.
- **Eviction stays local.** By the time a segment leaves the envelope it was uploaded long ago, so
  freeing its space is a local file operation gated on an offset that is already replicated. The
  alternative puts a PUT and a floor commit between "over budget" and "space freed", on the path
  that runs exactly when the disk is filling up.
- **A buffer during a store outage.** Segments uploaded before an outage keep being evicted during
  it, so local disk first exceeds the envelope only once data written during the outage fills it.
  With uploads at the envelope's edge, eviction stops the moment the store does.
- **Smoother load.** Uploads follow the write rate, one segment per seal. With uploads at the
  envelope's edge, lowering `local_retention_size` or `local_retention_age` turns much of the
  envelope into an upload burst at once; with uploads at seal it turns into eviction, which is
  local and fast. Changing the options never creates network work.
- **One loop.** The upload loop needs only "sealed and above the floor", not a second evaluation of
  local retention on the primary that must agree with every replica's own.

The cost is recorded under drawbacks. KIP-405 and Redpanda make the same choice: both copy a
segment once it rolls, independently of local retention.

### Why a replicated floor rather than per-replica knowledge

Alternatives: every replica uploads (triples PUT traffic and egress), or each replica HEADs an
object before evicting it (puts an object store round trip on a backup's housekeeping path and makes
a store outage stall eviction cluster-wide). A replicated offset is one command per uploaded
segment, applied on every replica by the same machinery.

### Why the state machine holds one offset, not a manifest

This RFC puts the floor in a partition state machine and keeps per-segment data out of it: deterministic keys
plus local markers carry the same information for a live cluster, so the snapshot stays a few bytes
wherever it is persisted. The cost is that a fresh replica learns its remote segments from a peer
(via state transfer) rather than from the bucket, and restore-from-bucket needs the lazily uploaded
manifest; both are accepted.

### Why a control log separate from the data log

Putting the floor command into the partition's data group would work: segments would stay pure,
because the flush already writes only message batches, and no offset translation would arise,
because message offsets are independent of op numbers. What it would do is interleave control
commands into the data journal and the data group's pipeline, which is harmless for one command per
gigabyte and harmful for the high-rate state machines the facility exists to host, such as
per-message delivery state. A separate control log keeps the data path for user messages only, at
the price of cross-log ordering, which the floor tolerates by checking against the committed end at
apply time. This departs from master's own precedent: the consumer-group and history fences and
session retirement went into the data log, with an apply hook beside the message path.

Between the two realizations, the metadata plane exists today and already holds
`deleted_up_to_offset`. The submit path from the owner shard and the reconciler pass a floor there
needs now exist, since master uses them for lifecycle completion and partition fences, but such a
floor still lags the data by a reconcile interval and shares one cluster-wide group with everything
else. A control group per shard applies on the owner shard with no hop and no
lag and scales with shards. The floor's rate makes the choice immaterial for this RFC, so it is
deferred to the facility RFC rather than decided here.

### Why an Iggy-owned object store abstraction

OpenDAL's S3 and GCS paths are tokio-bound through reqwest, and every vendor SDK drags a second
runtime into a server that runs none. The team ruled out third-party storage crates. Other systems
made the same choice (see [Prior art](#prior-art)): hand-written SigV4 and SharedKey signers, their
own HTTP/1.1 client, a per-shard connection pool and credential refresh on one shard. Owning the
abstraction also puts every provider under one contract test suite.

### Why on-shard tasks rather than worker threads

compio has no scheduling groups, and discussion #3312 preferred staying on the shard with
cooperative yielding to keep shared-nothing and core affinity. The repository already has the two
primitives on-shard work needs: detached tasks that return through a completion lane, which is how
disk polls run, and `yield_to_reactor()`, which state transfer already uses for exactly this kind of
long walk. Worker threads would have needed cross-thread channels, a second client per thread, and
a cache whose accounting lived off-shard, none of which exists today. And a cache partitioned by
shard needs no coordination at all, since a partition's chunks are read only by its owner shard.
The partition I/O lane added in #4311 is not the home either: it runs one job per partition under a
deadline that stops the node, and `run_blocking` serializes its OS-thread calls per shard.
The cost, a soft rather than hard cap on tiering's CPU share, is recorded under drawbacks and
watched through a pump tick latency gauge.

### Why not a diskless architecture

WarpStream, Bufstream, KIP-1150 and Ursa batch many partitions into one object per broker, upload,
and have a coordinator assign offsets after the PUT. Their ack floor is buffer plus PUT plus commit,
250 to 500 ms at p50. Iggy's VSR log already sequences and replicates, which is the job those
coordinators exist for, and the only sub-10 ms diskless variants front object storage with a
replicated-disk WAL, which is what Iggy already has. Tiered storage with prompt uploads and small
local retention gives the same cost profile at lower ack latency.

### Impact of not doing this

Retention stays bounded by local NVMe, terabyte partitions stay expensive, backups stay a
connector-level copy that cannot be read back, and the README keeps advertising a feature that does
not exist.

## Prior art

[prior-art]: #prior-art

- **Redpanda tiered storage** (`src/v/cluster/archival`, `cloud_storage`, `cloud_io`,
  `cloud_storage_clients`, `cloud_roles`). Leader-only uploads gated on the term; a Raft-replicated
  archival metadata state machine with a lazily uploaded manifest; local-deletion fence
  `min(last uploaded, last manifest upload)`; a persistent chunk cache with reserve-before-download,
  20 % of disk sizing and a 16 MiB chunk; single streamed PUTs with unsigned payloads; its own
  HTTP client and signers; scheduling groups for CPU fairness; advisory local retention under a
  disk-space manager; a scrubber that records anomalies without repairing them. This RFC copies the
  upload loop shape, the cache admission and persistence, the retry deadline tree, the signing and
  probing rules, and the per-pass deletion caps, and diverges on what the state machine holds (one
  offset rather than the manifest), on conditional PUTs (possible here because bytes are replica-identical), on stitching one poll across
  remote and local segments, and on explicit yields plus per-class semaphores standing in for
  scheduling groups.
- **Apache Kafka KIP-405** tiered storage and **Apache Pulsar** ledger offload: the same shape as
  this RFC, whole sealed units copied to per-partition objects with an index object beside them.
- **Rivet's SQLite storage engine** (July 2026) and its cold-tier code: a replicated hot tier with
  commits in low milliseconds, S3 never on the write path, 256 KiB chunks offloaded after 7 idle days
  and rehydrated on a miss, the cold tier doubling as the backup. Its cold tier used immutable keys
  carrying a content hash, kept leases and watermarks in the metadata store rather than in S3, and
  deleted in two phases with a grace period. Borrowed: the deletion grace period and the checksum
  comparison in the verify pass. Its October 2026 broker replacement, per-node S3 logs with a
  conditionally written head object and change detection by ETag from one LIST, supplies the ETag
  polling used for manifests here; its listing cost grows with the square of the node count, which is
  why LIST stays off every hot path in this RFC.
- **KIP-1150 Diskless Topics** (accepted 2026-03-02) and Aiven's Inkless. KIP-1165 rejected merging
  shared WAL objects and converts diskless data into ordinary KIP-405 segments through a returned
  per-partition leader, which is the strongest external signal that per-partition segment objects
  are the long-term format and that tiering should come first.
- **WarpStream, Confluent Freight, AutoMQ, Bufstream, StreamNative Ursa.** Object-storage-first
  streaming. Borrowed: treating API cost as a design input, LIST only for orphan reconciliation
  (WarpStream's deadscanner), cold readers isolated from the cache (Inkless), adaptive readahead
  that grows on misses and evicts unwatched blocks (AutoMQ). Rejected: the leaderless coordinator,
  shared multi-partition write objects, asynchronous commits.
- **turbopuffer and SlateDB.** Object-store-native with conditional writes as the fencing primitive.
  SlateDB's measurement that 4 MiB aligned cache parts cause p99 spikes motivated the small first
  fetch for cold polls.
- **Iggy discussions [#3312](https://github.com/apache/iggy/discussions/3312) and
  [#3232](https://github.com/apache/iggy/discussions/3232).** #3312 is the pre-RFC thread for this
  document. It converged on native compio access rather than OpenDAL,
  a local index, and infrastructure before format work; its participants prefer on-shard uploads
  with yielding, and hubcio prefers shipping sealed-segment compression first. #3232's spike against
  real S3 with `rusty-s3` and `cyper` established the transport this RFC assumes and three findings
  the implementation must keep: install the rustls ring provider at boot, trim quoted ETags, and
  coalesce parts under 5 MiB (moot once segments are single PUTs).

## Unresolved questions

[unresolved-questions]: #unresolved-questions

Open design decisions:

- **Piece size and caps.** `upload_piece_size`, `upload_concurrency` and `fetch_concurrency` bound
  the pump's exposure to tier work. The defaults here are guesses, provisional through phase 3 and
  settled by phase 4's measurement against the pump tick latency gauge: an upload backlog, a
  cold-read storm, eviction passes and primary object deletion on one shard.
- **Generic or `dyn` for the shard-side handle.** The codebase removed `DynSuperblockStore` in
  favour of generics. A `TierClient` type parameter on `IggyPartitions` with a `NoTier` default is
  consistent with that; the alternative is an `Rc<dyn RemoteChunkSource>` confined to `poll_plan`.
- **Compression ordering.** hubcio leans toward shipping server-side compression of sealed segments
  before tiering. Per-flush-unit compression at seal time is compatible with ranged GETs; whole-object
  compression is not. If compression lands first, tiering uploads the compressed segment unchanged
  and the cache stores compressed units.
- **Cache warmth after a primary move.** Only the primary serves polls, so a rebalance hands
  readers to a node whose cache is cold for every remote segment. Warming the target during the
  drain, or accepting a burst of GETs after each move, is open.
- **Whether the floor requires the `.index` upload or only the `.log`.** Redpanda tolerates an index
  upload failure because the reader rebuilds; requiring both blocks eviction on a transient failure
  for a segment whose data is already safe.
- **Two watermarks, possibly two logs.** `TruncatePartition` lives in the metadata plane today. If
  the facility realizes the control log as per-shard groups, the consistent end state is both
  watermarks in the control log, with `DeleteSegments` resolved and applied there; migrating it is a
  follow-up amendment, not part of this RFC.
- **The partition state-machine facility.** The control log's realization (a control group per shard
  or the metadata plane), command encoding, the apply hook, the durable snapshot and state-transfer
  inclusion are specified in their own RFC, not yet drafted, which can generalize what master
  already does for its fences. Phase 2 here cannot start before it lands, and it is the only phase
  that waits on it. Mixed-version clusters cannot form, since the
  replica handshake refuses a different release or storage format, so no replica ever meets a
  command it does not know.
- **The idle-partition roll and an updatable age.** The age roll cannot seal a partition that
  receives nothing, and `segment_max_age` is create-only. Both need a roll ordered with the batches:
  a data-log operation the primary appends when the active segment has outlived `segment_max_age`
  with no batch arriving, or one that carries a new `segment_max_age` to the point in the stream
  where it takes effect. Whether either is worth a data-log operation, the place master already
  keeps its fences, is open.
- **Self-healing from local copies.** When the verify pass finds a missing object, a backup may still
  hold the segment locally. Re-uploading from it automatically is cheap to specify here because
  segments are byte-identical; Redpanda only records the anomaly.
- **Encryption of objects at rest.** A stream with an encryption key already stores ciphertext in its
  segments, so its objects and cache chunks are ciphertext too, and objects follow whatever the
  stream's key handling does to segment bytes. Open is the unencrypted case: bucket-side encryption
  headers, or plaintext objects documented as such.
- **The segment checksum in the object key.** Keys are offset-only and rely on byte-identical
  replicas plus the conditional PUT. Putting `log_checksum` into the key, as Rivet's cold tier does
  with a content hash, turns a violation of the byte-identical invariant into two objects for one
  offset, which the verify pass can detect, instead of a silent overwrite or a 412. The cost is that
  an object can no longer be found by offset alone: a reader needs the marker or a LIST of the
  offset prefix. The verify pass's checksum comparison covers most of the benefit without the cost.
- Defaults to measure rather than argue: the chunk size (8 MiB here, 16 MiB in Redpanda),
  `readahead_max`, and the recommended `segment_size` for tiered topics, all against S3 in-region.
- Whether backups may upload when the primary is stalled (this RFC says no; failover re-uploads
  idempotently), whether enabling `tiered_storage` on a topic with existing sealed segments uploads
  the backlog or only later segments, and whether eviction should request a WAL checkpoint eagerly,
  since every partition's WAL keeps evicted inodes alive through hard links until the checkpoint
  passes.
- The error code a poll returns for a remote segment that exists but cannot be read, distinct from
  the empty poll a walk fault produces today and from the transient retry code, since a new code is
  work in every SDK.

Out of scope for this RFC, addressable later independently of it:

- S3-direct storage modes. Not planned; if ever wanted, the shape is Redpanda's cloud topics and it
  reuses every component here.
- Restore and mount: with the lazily uploaded manifest under `meta/`, restoring a partition into an
  empty cluster is a shallow recovery that plants markers and an empty active segment at
  `last_offset + 1`, with a topic manifest beside it carrying the topic's options.
- Read-replica topics that follow another cluster's manifests and serve them read-only, polling by
  ETag rather than downloading each manifest on an interval.
- Adjacent-segment merging, required once the age roll produces small segments.
- Compression at rest and log compaction, both of which interact with uploaded objects and are
  tracked on the roadmap separately.
- Protocol-aware recovery using a tiered object as a second source for a damaged local segment.
- Stats wire fields for local versus remote bytes, CLI and Web UI visibility, and a rehydration job
  so tiering can be turned off.
- Direct I/O: cache chunk files are read with the same 4096-aligned buffers as segment files, so an
  `O_DIRECT` switch covers both, provided sealed segment bytes stay unchanged.

Simulator versus integration coverage:

- The deterministic simulator can cover the protocol: monotonicity of `tiered_up_to_offset` across
  view changes and restarts; the eviction eligibility rules (floor, WAL checkpoint, active
  segment, local retention budget); the `REMOTE_CHAIN` state-transfer artifact; the orphan sweep's
  incarnation logic; and the `sim` object store with injected latency, throttling and partial
  failures. The `SimStorage` harness that already drives the WAL and persistence with real bytes
  can cover the marker codec, the crash points between marker write, unlink and directory fsync,
  boot acceptance of a marker-only prefix, and the age roll's WAL and materialization rules
  agreeing, including after WAL replay.
- The simulator cannot cover byte-level reads of tiered data in its main loop, because partitions
  there have no directory and segments are accounting only. The chunk cache, index-snapped chunk
  boundaries, cold-read latency and the completion-lane sizing need integration tests against the
  `fs` backend (Docker-free) and MinIO through `testcontainers-modules`, and the three-node
  scenarios (eviction on every replica, cold reads after a rebalance moves the primary, a poll
  spanning remote and local segments, the primary killed mid-upload, topic delete followed by the
  sweep) need the
  `--features vsr` cluster harness.
- Not testable locally at all: real-provider quirks such as GCS interoperability conditional
  headers, Azure hierarchical namespaces, and the IAM, STS and metadata-server credential chains.
  These rely on the provider contract tests plus manual verification against each provider.
