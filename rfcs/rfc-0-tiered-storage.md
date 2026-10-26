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
segment stays in the `SegmentedLog` with its metadata, its sparse index and a 128-byte marker in
the partition's `remote.chain` file on local disk, so offset and timestamp resolution never touch
the network. Total retention
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
- Reads of tiered data served through a cache, with one small object store request on a cold
  poll's path (two when the segment's index is not local) and none for a warm one.
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
backend = "s3"                # "s3" | "fs"; "gcs" | "azure" once their providers ship

[tiered_storage.s3]
bucket = "iggy-prod"
region = "eu-central-1"
endpoint = ""                 # empty = AWS; set for MinIO, Ceph, R2
prefix = "iggy"
access_key = ""               # IGGY_TIERED_STORAGE_S3_ACCESS_KEY
secret_key = ""               # IGGY_TIERED_STORAGE_S3_SECRET_KEY

[tiered_storage.cache]
size = "auto"                 # 256 MiB per shard
size_percent = 20             # of the data disk; the smaller wins
chunk_size = "8 MiB"
```

At boot the node probes the bucket once (a one-key LIST) and refuses to start on a deterministic
misconfiguration: rejected credentials or signature, a missing bucket, a bad endpoint. A probe that
fails transiently (a timeout, a 5xx, a DNS failure) boots the node with `upload_stalled` raised, so
a restart during a store outage never costs the cluster a replica. The credentials need
`s3:ListBucket`, `s3:GetObject`, `s3:PutObject` and `s3:DeleteObject` on the prefix. The prefix
must have no bucket lifecycle rule that deletes objects or moves them to an archive tier: Iggy
deletes them itself, and an archived object cannot be read without a restore. The `fs` backend
points at a directory on another filesystem and needs no credentials; it is what tests use and
what "archive to disk" means.

Every node must point at the same backend. Each node registers a fingerprint of its backend (kind,
bucket or path, prefix, endpoint) in the metadata plane at boot. Enabling tiering on a topic records
the fingerprint with the topic and is refused while any node's differs, and a node whose
fingerprint differs from a topic's neither uploads nor serves polls for that topic's partitions. A
node that finds tiered segments on disk with no backend configured refuses to boot.

A topic opts in with options, at creation or later:

```text
iggy topic create my-stream events 8 none 30d \
  --set tiered_storage=true \
  --set local_retention_age=6h \
  --set local_retention_size=50GiB \
  --set segment_max_age=1h
```

From then on:

- `message_expiry` and `max_topic_size` mean total data, local and remote together.
- `local_retention_age` and `local_retention_size` bound what stays on local disk. Data older or
  larger than that is evicted locally once it is safely in the store.
- Enabling `tiered_storage` is one-way until rehydration exists: an update or reset that turns it
  off is refused with `OptionNotUpdatable`.

The operator sees, per node, Prometheus counters and gauges on `/metrics`, which needs
`http.enabled`: local and remote bytes, upload and fetch counts with their summed latency, cache
hits and misses, a per-partition upload lag, and an `upload_stalled` gauge that rises when the
store is unreachable. A store outage never affects producers or consumers of warm data: uploads
retry with backoff, the watermark stops advancing, and cold reads have their own admission cap, so
they cannot take the completion-lane slots warm polls use. Segments uploaded before the outage keep
being evicted, so local disk grows past the local envelope only once data written during the outage
fills it; from then on the topic behaves as an untiered one until the store is back.

### For client developers

Nothing changes on the wire. A poll for a range that spans remote and local segments returns one
response, exactly as today. The only visible effect is latency: a poll that touches a segment
nobody has read recently pays one small object store request (two if the segment's index is not
local), typically 30 to 100 ms in-region, after which neighbouring polls are warm. Polls are served
by the partition primary, so that is where cold reads happen. If the store is slow, the poll
returns what it read within a bounded wait, or `TransientNotAccepted` (58) if it read nothing, and
the SDKs retry 58 until their 30 s call deadline. A store failure no retry can fix (rejected
credentials, an archived or missing object) answers a new non-retryable error, `TieredReadFailed`.

Consumer offsets, consumer groups, auto-commit, and time-based polls behave the same. A segment
that a slow consumer has not read yet may be evicted locally, because the data remains readable;
it is never deleted from the store while a committed consumer offset points before it, except the
offsets of external consumer groups, which never hold retention.

### For contributors

A tiered segment is a normal `Segment` whose `location` is `Remote` instead of `Local`. Its
`.log` file is gone, its `.index` file is kept (or fetched on first use), and a marker in the
partition's `remote.chain` file records every field boot needs to rebuild the segment without the
network. Boot recovery treats a stem with a marker and no `.log` as a legitimate segment; a missing
middle `.log` with no marker is still a hole.

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
  links to segment inodes until its checkpoint passes, and to its tail and checkpoint-cursor
  segments always, so the active `.log` can hold bytes past the committed `segment.size`; a sealed
  segment's committed size is authoritative. Control operations are stored inline in the WAL. Boot
  restores a receipts checkpoint, cut at the data log's commit point, and replays WAL prepares above
  it, control operations included.
- Boot recovery collects segment stems from `*.log` files only and unlinks any `.index` or `.anchor`
  without a `.log` (`segment_recovery.rs`). It keeps only stems below the WAL checkpoint's
  `next_offset` plus the checkpoint-cursor stem, and reads that segment's `.log` for exactly the
  checkpoint's length, refusing a shorter file as `StorageSizeMismatch`. A gap between consecutive
  segments is refused as a `Hole` unless an `.anchor` covers it, an undecodable `.anchor` proves
  nothing, an empty segment in the middle of the chain is refused, and a missing front prefix is
  accepted. Quarantine, converge and the install sweep know only `.log`, `.index`, `.staging` and
  `.anchor`; the install backup hard-links and fsyncs every file in the directory inside one
  I/O-lane job, so a new file type is included there automatically.
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
  it observes the committed watermark rather than waiting for the cleaner's timer. The resolved
  offset is not clamped to the consumer barrier, but enforcement is, so local removal can stop below
  the watermark. `TruncatePartition` carries `expected_history`, the `created_revision` it was
  resolved against, and its apply refuses a mismatch with `HistoryChanged`. Topic and partition
  deletes are two-phase: the primary commits `TransitionPartitionHistory` to the partition log,
  after which the incarnation's history is deleted for good and data-log requests get
  `HistoryUnavailable` (87); metadata then completes the delete with `CompleteLifecycle` and the
  reconciler removes the directory. Some partition records live outside the partition directory,
  under `partition-initialization/{namespace}/`.
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
  shard 0 of every node. A node places a partition on shard `hash(namespace) % shard_count` with its
  own shard count, which the default `cpu_allocation = "numa:auto"` lets differ between nodes, and
  the replica handshake does not compare shard counts.
- Disk polls are planned on the pump and executed as a detached compio task on the same thread
  (`bus.spawn`), returning through a bounded completion lane. A lane slot is held from dispatch
  until the result is dequeued, whatever the requester's timeout, and a full lane rejects every new
  disk poll with 58. The requester waits at most `PARTITION_READ_TIMEOUT` (10 s); expiry answers
  11001, which the SDK does not retry, while `TransientNotAccepted` (58) is retried until the SDK's
  30 s deadline: the Rust SDK backs off and looks the route up again, the Java SDK's poll path
  retries every 50 ms. The poll plan has no error channel: a failed disk walk returns an empty poll.
  The plan snapshots every segment from the start segment to the tail on the pump, one `Rc` clone
  each. The walk reads chunks of 64 KiB to 1 MiB and continues into the next segment from byte
  zero.
- Consensus repair never reads sealed segment bytes. Local presence of sealed segments matters in
  state-transfer serving (the offer is the whole local chain), the WAL's hard links, retention,
  `install_backup`, and boot chain validation. WAL recovery can also re-create the public `.log` of
  the WAL's tail segment, but never of a fully checkpointed sealed segment.
- The partition superblock payload may be 64 KiB (`SUPERBLOCK_VERSION` 2), but `VsrState` is decoded
  at exact lengths, so a new field is a `RECOVERY_VERSION` bump. Upgrades already require fresh
  storage: boot refuses a data directory whose `STORAGE_FORMAT` differs from the binary's, and the
  replica handshake refuses peers with a different release or storage format.
- Shards run with `thread_pool_limit(0)` except on macOS, so `spawn_blocking` panics a shard;
  `run_blocking` offloads one blocking call at a time per shard to its own OS thread. The server
  builds no tokio runtime. `cyper` (a compio-native HTTP client over rustls) is already used for
  JWKS and HTTP forwarding. It is built without `hickory-dns`, so it resolves a hostname with
  `getaddrinfo` through compio's blocking pool, which panics a shard. A JWKS fetch from a hostname
  issuer carries that panic today. Server metrics have no histograms, and there is no counting
  semaphore or token bucket primitive.
- Topic options live in a catalog (#4615). A key is an `OptionKey` variant plus a `CATALOG`
  descriptor; mutability is create-only, updatable or read-only; defaults are compile-time catalog
  constants, not server configuration. A node refuses a resource whose `critical_keys` marker names
  a key it does not understand, but master writes that marker only when an update sets a critical
  key and when a stream is created with an encryption key, not when `CreateTopic` sets one.
  Updatable retention options are re-read by the cleaner on every pass.

### Object layout

```text
{prefix}/{cluster_uid}/{stream_id}/{topic_id}/{partition_id}/{created_revision}/{start_offset:020}.log
{prefix}/{cluster_uid}/{stream_id}/{topic_id}/{partition_id}/{created_revision}/{start_offset:020}.index
{prefix}/{cluster_uid}/meta/{stream_id}/{topic_id}/{partition_id}/{created_revision}/manifest.bin
```

Stream, topic and partition ids are slab-allocated and reused after deletion, so
`Partition.created_revision` (unique across the cluster) identifies the incarnation. Offsets never
restart within an incarnation: the offset frontier is durable and never rewound, and
`DeleteSegments` only trims from the front, so `created_revision` separates every offset space
within one cluster lifetime. It does not separate lifetimes: `created_revision` restarts on fresh
storage, which every upgrade needs. `cluster_uid` is therefore a random 128-bit id, written as 32
hex digits, that the metadata plane mints once when its log is created and keeps in its snapshot.
It is not master's `cluster_id`, a hash of `cluster.name`, whose default is the same for every
cluster. Keys never repeat across clusters sharing a bucket or across lifetimes of one cluster, and
no cluster ever deletes, sweeps or reads under another's `cluster_uid`. The zero-padded stem
matches the on-disk name so a listing sorts by offset.

Objects are immutable. The `.log` object is canonical and identical whichever replica produced it,
and for an encrypted stream it is the same ciphertext the segment holds.
The `.index` object is the uploader's sparse index; any sparse index whose entries point at batch
starts is valid for lookups, and its length is known only from the object itself. User metadata on
the `.log` object carries `end_offset`, `start_timestamp`, `end_timestamp`, `max_timestamp`,
`size`, `log_checksum` and a format version, so a listing plus HEAD rebuilds segment metadata
without a download.

### The local marker

A tiered segment leaves a marker: a fixed 128-byte record, modelled on `.anchor`, in the
partition's `remote.chain` file. One file per partition rather than one per segment halves the
files a remote segment leaves behind, which matters to the install backup: it links and fsyncs
every file in one I/O-lane job under the two-minute deadline.

```text
magic "IGGYREMT"   u8[8]
version            u16      flags u16      reserved u32
start_offset       u64      end_offset     u64
size               u64      reserved       u64
start_timestamp    u64      end_timestamp  u64      max_timestamp u64
created_revision   u64      reserved       u64
log_checksum       u64      (the segment's commit-path hash, XxHash3-64)
record_checksum    u64      padding to 128
```

The file opens with a header record of the same size, which carries `created_revision` and the
replica's trim point, the offset just below its first stem. A replica creates the file when it
applies the enable of `tiered_storage`, staged through the reconciler on the owner shard as
`TruncatePartition` enforcement is, so the trim point records the front its own retention left
before tiering. A partition created on a tiered topic gets the file with its directory, with an
empty trim point that counts as `trim + 1 = 0` as an empty floor does, and a state-transfer install
takes the sender's. A replica that finds its tiered partition without the file, after a crash
between the enable's apply and the creation or after losing it, creates it at its current front
(see the boot rules). Until the file exists the replica neither evicts nor, as primary, deletes
objects, uploads or commits the front truncation (see [Retention](#retention)). From then on every
path that moves the front first rewrites the file with the trim point raised to just below the new
first stem, and only then unlinks anything: a front removal, of a marker or of a local segment, and
a boot re-anchor that unlinks the chain's only segment, an empty one, and plants at the mint
frontier. Creation and every rewrite go through a temporary file, fsync, rename and a directory
fsync, the protocol `.anchor` uses, and a rewrite drops the retired markers. Eviction appends the
marker and fsyncs the file before it unlinks the `.log`. Removing the last marker keeps the header,
so the file lives as long as the incarnation. Appends are sequential per partition, so only the
last marker can be torn.

Boot rules:

- A torn last marker is truncated: its fsync never completed, so its `.log` was never unlinked. Any
  other undecodable record, the header included, refuses the partition. An unreadable `.anchor`
  proves nothing, but an unreadable marker read as absent would silently drop a tiered prefix,
  since boot accepts a missing front prefix.
- A stem with a marker and no `.log` is a tiered segment. Its `.index` and `.anchor` are kept;
  today's sweep unlinks any `.index` or `.anchor` without a `.log`, so the sweep learns marker
  stems. A kept `.index` must have ascending entries inside the marker's `size`; a missing or
  failing one is fetched on first read.
- A stem with both a marker and a `.log` is local. The marker records that a remote copy exists so
  eviction can resume without re-uploading.
- `ensure_contiguous_chain` uses marker bounds for tiered segments. A missing middle `.log` with no
  marker is still a `Hole`. A gap master's boot re-anchor left is admitted with its `.anchor`
  whether either side is local or tiered, exactly as between local segments today.
- Tiered segments are always a prefix of the chain. A marker after a local sealed segment, or at or
  above the WAL checkpoint-cursor segment, is a refusal.
- The first stem must start at or below the header's trim point plus one. A later start means
  markers or front segments were lost, and boot refuses the partition rather than accepting the
  shorter chain. Master's refusal path then rebuilds it from its peers, or on a single replica
  leaves it unserved with its files in place, so a lost front never becomes a primary's front. The
  check is local on purpose: metadata boot applies only the ops a commit watermark proves, a lower
  bound that can trail the last `TruncatePartition` this replica enforced, so comparing the chain
  with `deleted_up_to_offset` at boot would refuse healthy replicas, and every replica at once after
  a full-cluster restart.
- A missing file is never a refusal. Boot cannot tell a lost file from a crash between the enable's
  apply and the file's creation, and refusing would leave a healthy single-replica partition
  unserved. A replica that sees its partition tiered without the file creates it at its current
  front. That trim point exceeds the lost one only when segments were evicted, whose markers went
  with the file, and eviction leaves the first local stem at or below `floor + 1`, a trim point the
  front truncation never acts on. A fault that takes local front segments together with the file
  goes unseen.
- Marker `created_revision` must match the partition's; a stale marker is quarantined with the
  partition.
- Quarantine, converge and the install sweep handle `remote.chain` alongside `.log`, `.index`,
  `.staging` and `.anchor`. The install backup needs no change, since it links every file.

In memory, `Segment` gains `location: SegmentLocation` with `Local`, `Uploaded` (both copies) and
`Remote`. Tiered segments stay in `SegmentedLog.segments`, so `disk_poll_start` keeps resolving the
start segment by offset or by `max_timestamp` exactly as it does now.

### Roles

Every replica of a tiered topic hashes each segment as it commits it. Materialization feeds an
XxHash3-64 from the committed prepares in commit order, finalizes it in `accept_rotation`, and
persists it with the partition's replicated state in the receipts checkpoint until the segment is
evicted, when it moves into the marker. The hash describes the bytes consensus agreed on, not what
a disk returns later, and no path reads a segment back to compute it. The active segment's running
hash is not persisted: boot recomputes it once, along with the hash of any segment that sealed
after the last checkpoint or before tiering was enabled, in a detached tier task that reads in
pieces and yields, never on the I/O lane or in a frame body.

#### Primary: upload and floor advancement

Upload is primary-gated. Two triggers feed one queue: `accept_rotation` enqueues the just-sealed
segment when the topic is tiered and the partition is primary, and a per-shard upload loop scans
owned primary partitions for local sealed segments above the committed floor that are not in
flight. The loop backs off exponentially from 100 ms to 10 s while it finds nothing. Uploads are
strictly oldest-first per partition, one in flight per partition: a segment uploads only when no
older local sealed segment above the floor is waiting, so a seal-time enqueue never overtakes a
backlog, and enabling tiering on a topic with sealed history uploads that history first. One
upload in flight caps a partition's tiering rate at one PUT stream, and a partition whose ingest
outruns it shows a growing upload lag and raises an alarm. Per-shard concurrency adapts between one
and `upload_concurrency`: it grows by one per completed upload while a backlog exists and halves
when an upload stalls, and a node-wide `upload_bandwidth` token bucket keeps uploads from taking
the NIC from replication. A partition whose local eviction is blocked because its floor lags is
flagged as under local storage pressure and its uploads go to the head of the queue. A failed
upload retries with backoff and blocks the floor.

Primaries move. A rebalance drains only pending history writes, so the old primary cancels its
in-flight uploads when it loses primacy, and the new primary uploads those segments again. A
replica that becomes primary first re-commits the floor through its own markers, one advance per
marker above the floor, since a marker proves a floor that was once committed. It then LISTs the
partition's prefix once to rebuild its deletion cursor, commits the front truncation a skewed front
needs (both in [Retention](#retention)), and only then uploads, skipping every segment that is not
`Local`.

Upload reads the sealed `.log` up to its committed `segment.size`, through its own file
descriptor, which keeps working if retention unlinks the file mid-upload. The same pass validates
every batch's `batch_checksum` and hashes the stream. A stream hash that differs from the
commit-path hash fails the upload and raises an alarm, so a primary's media fault never reaches the
store. The object carries a body checksum the store itself verifies (see the provider table), and
the upload counts only when it matches the uploader's. A conditional create that fails because the
key exists (412, or an existing key on `fs`) counts as success only after a HEAD shows the same
size and `log_checksum`. A difference raises a divergence alarm and the floor stays. After the PUT
lands the uploader rechecks `deleted_up_to_offset`, and if the watermark passed the segment in the
meantime it schedules the object's deletion after `delete_grace`.

On success the primary commits `AdvanceTieredFloor { created_revision, start, up_to, hash }` to the
partition's control log: the incarnation, the first offset the advance covers, the segment's last
offset and its commit-path hash. `start` is the segment's first offset, or `floor + 1` when no
offset between the two holds a message: each is at or below `deleted_up_to_offset`, or inside the
gap that a durable `.anchor` beside the segment records. Master's boot re-anchor leaves such a gap
on a single-replica partition with the default `replicated` durability, after a crash that took
acknowledged but unflushed messages: it seals the tail and plants the next segment at the mint
frontier, above offsets that were handed out and hold nothing. Without the anchor case, apply would
refuse that segment's advance forever, the floor would stop below it, and eviction with it. The
primary checks both cases before it submits. Only the primary submits the command, but nothing in
the realizing group fences on partition primacy: neither the metadata group's view nor a control
group's view says who the partition's primary is. The command is safe by its contents instead.
Apply refuses a stale incarnation or a gap, and every replica checks the hash against its own
bytes, so a late command from a former primary can only repeat a true statement. Every replica
applies it through the partition state machine below.

#### Every replica: local eviction

Eviction follows the retention pattern: a pass on the pump records a local-eviction watermark and
the partition's I/O lane runs one `EvictSegment` job per segment, the way it runs `RemoveSegment`
today. Passes are staged two ways. First, a floor that rises stages one on the owner shard's pump
as soon as the owner shard observes it, as `TruncatePartition` enforcement is staged today:
directly if the apply runs on that shard, through the reconciler if it runs on the metadata plane.
Second, the cleaner's timer stages one, which covers segments that age out of the local envelope
with no floor change and any staging frame the pump refused. A pass is a no-op for a partition
inside its local envelope, so staging on every raise costs one pump frame per uploaded segment, and
a partition under local storage pressure starts evicting when its floor applies rather than up to
one cleaner interval later.

The eligible run starts at the front and stops at the first of: the active segment, the WAL tail
segment, the WAL checkpoint-cursor segment (`start_offset >= checkpoint.start_offset`),
`end_offset > tiered_up_to_offset`, a segment whose hash check has not passed, or the local
retention budget being satisfied. Boot reads the checkpoint-cursor segment's `.log` for the
checkpoint's length and refuses a shorter file, and the WAL hard-links the tail and
checkpoint-cursor inodes whatever its checkpoint, so evicting either would wedge boot on every
replica and free nothing. The run does not consult the consumer barrier. Per segment the job
appends the marker, fsyncs it and unlinks the `.log`; on acceptance the owner flips `location` to
`Remote`, drops the segment's sealed read handle and, as retention does after an unlink, the
shard's cached state-transfer offer, and leaves `PartitionStats.size_bytes` untouched.
One segment per job keeps eviction from holding the partition's single I/O slot ahead of
materialization, and the job never reads or hashes the segment.

The hash check compares each advance with the replica's own segment: the segment must end at
`up_to` and its commit-path hash must equal `hash`. It runs when the advance applies, or when the
replica seals that segment if its data log trails its control log. The replica keeps the advance's
hash beside its own until it evicts the segment, and one that lost it in a restart reads it back
from the object's metadata with one HEAD. A mismatch never evicts that segment and raises a
divergence alarm.

#### Partition state machine

The floor lives in a replicated state machine fed by the partition's **control log**: a consensus
sequence separate from the data log, so that segments and the data journal hold only user messages
and control commands never enter the data path. This RFC does not fix how the control log is
realized. A general partition state-machine facility, specified in its own RFC because queue
semantics, log compaction and the key-value store need it as well, decides between one control
group per shard carrying the commands of every partition that shard owns, and the metadata plane,
which already is a separate control log holding per-partition facts. A per-shard group cannot be
keyed by a node's local shard index, since shard counts can differ between nodes, so the facility
defines its membership independently of them. Either way the partition primary forwards its command
to the realizing group's primary, which may sit on another node. Master already applies,
checkpoints and transfers its own replicated partition state (the fences and dedup receipts), but
in the data log and without a general facility; the facility generalizes that. The floor advances
at most once per uploaded segment, so it works with either realization; this RFC depends on the
facility only for the command encoding, the apply hook, the durable snapshot with a replay point of
the control log's own, and state-transfer inclusion.

The tiered storage state machine holds one value, `tiered_up_to_offset`, for the incarnation named
by its `created_revision`. `AdvanceTieredFloor { created_revision, start, up_to, hash }` applies as
follows. A `created_revision` other than the partition's is refused, as `TruncatePartition` refuses
a stale `expected_history` with `HistoryChanged`. A `start` above `floor + 1` is refused, so the
floor never jumps over a segment nobody uploaded. An empty floor counts as `floor + 1 = 0`.
Otherwise the floor becomes `max(floor, up_to)`, and a repeated advance is a no-op. Apply reads
nothing but the command and the state, so every replica reaches the same floor whatever its data
log's progress, and eviction, not apply, clamps the floor to the local sealed chain. The state
holds no per-segment data on purpose: deterministic keys plus the local markers carry the segment
list, so the snapshot stays a few bytes, and the hash is checked by each replica outside the state
machine.

Where the floor is persisted follows the realization. On a partition-plane control log it joins
the replicated partition state master already keeps: the receipts checkpoint at rest and
`ConsumerOffsetsWire` in state transfer, both strictly versioned, so both get a version bump. The
receipts checkpoint is cut at the data log's commit point and boot replays only the data WAL, so
the control log needs a snapshot and replay point of its own, or a floor applied after the last
checkpoint regresses on restart. On the metadata plane it is a field of the metadata `Partition`
with a snapshot version bump. The superblock is a poor fit: its writes are driven by view and
offset-reservation intents, not by applies, and `VsrState` decodes at exact lengths. Either way the
floor rides state transfer, and the receiver installs the larger of its own floor and the
sender's, so a rebuilt replica never starts at zero. Should a floor still trail a replica's
markers, the markers win: they prove an earlier committed floor, the upload loop never treats a
non-`Local` segment as pending, and a new primary re-commits the floor through them before it
uploads. The floor is monotone for the life of the partition incarnation, and eviction reads it
directly from the partition.

`TruncatePartition` stays in the metadata plane, where it already ships. If the facility settles on
the metadata plane, both watermarks share a log from the start; if it settles on per-shard control
groups, migrating `TruncatePartition` is a follow-up amendment (see unresolved questions).

### Segment lifecycle

| State | Local files | Remote | Transition out |
| --- | --- | --- | --- |
| Active | `.log` (written by the WAL), `.index` | none | rotation seals it |
| Sealed, local | `.log`, `.index` | none | primary upload succeeds |
| Uploaded | `.log`, `.index`, and a marker if a crash interrupted eviction | `.log`, `.index` | floor committed, hash checked, local retention exceeded |
| Remote | marker, `.index` (optional) | `.log`, `.index` | total retention raises `deleted_up_to_offset` |
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
monotonicity: a primary whose clock lags only delays a roll. A clock that ran ahead is different:
master's monotone stamp floor carries a far-future stamp forward, later batches stamp it plus one
microsecond, and the age roll stays silent until the wall clock passes it.

Rolls are decided in two places, at materialization and in the WAL at prepare time, and both must
apply the new rule identically. The WAL already rolls before placing an arriving batch, so the rule
sits beside its size check, and its fixed-size durable segment state, which records `max_size` and
the tail position, gains `segment_max_age` and a first timestamp on both of its cursors, the tail
and the checkpoint cursor: a WAL format change. Both are needed because truncation rewinds the
tail to an older entry's segment or falls back to the checkpoint cursor. A rewind into an entry's
segment rereads the header of that segment's first batch, and install and re-anchor set the first
timestamp whenever they build a cursor. Materialization takes the first timestamp from the
segment's first batch header too, never from a field filled in later. `segment_max_age` is
create-only for the reason `segment_size` is: an update arrives through the metadata plane, which
is not ordered with the partition's data log, so replicas would apply it at different batches and
roll differently.

Rolling by time makes an existing master defect likelier. A truncation that rewinds the WAL tail
below a roll removes the rolled-into public `.log` only when the replacement re-rolls at the same
offset. A replacement stamped later can roll earlier, which leaves that file inside the new
segment's range, and boot refuses it as a `Hole`. The age roll needs truncation to unlink every
rolled public name above the rewound tail.

The rule covers steady traffic and traffic with long gaps, since the first batch after a gap seals
the old segment. It is not a tiering option. On any topic that keeps receiving writes it bounds how
long the active segment stays unsealed, which also lets `message_expiry` reach data on a topic that
never fills a segment. A fully idle partition is the case it leaves: nothing arrives to trigger the
roll, so its last segment stays local and unexpired until the next write. Closing that needs a roll
ordered with the batches, which means an operation in the data log, which is why master's fences
live there; it is left open.

`segment_max_age` is at least 10 minutes, Redpanda's default lower bound for the same setting.
Every segment costs two PUTs, a floor command, a marker, an eviction job and in-memory state on
every replica, and the bound caps that at 144 segments a day per partition, so an adjacent-segment
merger stays a later optimization rather than a requirement. A topic with `preallocate_segments`
cannot set `segment_max_age`: preallocation reserves a full `segment_size` of blocks per segment
and nothing trims it at seal, so every small age-rolled segment would pin a full segment's space.
Both options are create-only, so create refuses the combination.

### Read path

`DiskReadPlan` carries a `SegmentSource` per snapshot segment, `Local { path }` or
`Remote { start, size, index_state }`, decided on the pump from `Segment.location`. A remote
segment's object key is derived off the pump from the partition's key prefix.
`resolve_segment_file` becomes the dispatch point. The snapshot stops where the poll's byte budget
runs out instead of at the tail: remote segments make the chain as long as total retention, and
cloning all of it on the pump for every poll grows with it. A walk that meets `ENOENT` on a segment
planned `Local`, because eviction unlinked it mid-poll, re-plans that segment as `Remote` instead
of failing the poll.

#### Chunk cache

A directory under the node's data path holds chunks of remote `.log` objects under their full
object key, as `{object_key}.chunks/{chunk_start}`, so `cluster_uid` and `created_revision` are
part of every path and no incarnation ever reads another's chunks. Chunk boundaries snap to batch
boundaries: chunk `k` starts at the sparse index entry nearest to `k × chunk_size` (default 8 MiB),
so a batch never costs two GETs. A miss hands `FetchChunk` to the shard's tier service, which
reserves the bytes in the cache budget first, issues one ranged GET, writes a `.part` file and
renames it into place. Chunk files are not fsynced: boot drops a chunk whose length does not match
the index-snapped range it covers, and a chunk that fails `batch_checksum` validation is dropped and
fetched again rather than faulting every later poll. Chunk file descriptors are cached the way
sealed segment descriptors are.

A cold poll does not wait for a whole chunk: the tier service first fetches exactly the walk's
range, 64 KiB to 1 MiB, serves the walk from the response buffer, and hydrates the surrounding
chunk in the background.

Readahead is adaptive: one chunk, doubling on consecutive sequential hits up to `readahead_max`
(default 4); a reader that stops leaves its prefetched chunks first in line for eviction. Each shard
has at most one outstanding fetch per chunk, so two consumers reading the same cold segment share
one GET. A poll whose start offset is further behind the head than the cache could hold is a cold
reader: its walk streams from the response buffers and its chunks are not admitted, so a backfill
cannot evict the working set near the head and costs no local disk writes.

Space is reserved before a download, with a non-blocking attempt on a poll's path. The budget is
the smaller of `size` and `size_percent` of the data disk, with an object-count cap, split evenly
across shards: a partition's chunks are only ever read by the shard that owns it, so each shard
runs its own LRU and budget with no cross-shard coordination. The default `size` is 256 MiB per
shard, so the shipped defaults never refuse boot on a host with many shards. A reservation that
does not fit trims the slice by access time down to 80 % of it. A walk pins the chunks it reads
until it finishes, pins hold at most half a slice, and a reservation that finds no unpinned space
fails the fetch as transient. The cache persists across restarts: each shard keeps an access-time
table for its files, saves it every 60 s and at shutdown, and at boot walks its directory before
serving, deletes leftover `.part` files and reconciles the table. When an incarnation retires, the
reconciler purges its chunks with its directory, and boot drops chunks of incarnations metadata no
longer has. Downloads are rate-limited by a per-shard token bucket derived from the node-level
`fetch_bandwidth`.

#### Indexes for remote segments

`resolve_sealed_start` prefers the cached index, then the local `.index`. For a remote segment with
no usable local index it asks the tier service for the `.index` object, checks that its entries
ascend and point inside the segment's `size`, writes it as `.staging`, fsyncs and renames it to the
canonical name, and proceeds. The index's length comes from the object itself, never from segment
metadata: sparse indexes differ per replica, and after a primary move the `.index` object may come
from a different uploader than the `.log`. The 512 KiB residency cap applies unchanged.

#### Latency, timeouts and errors

The poll plan has no error channel today: a failed disk walk returns an empty poll. Tiering adds
one. Every wait a poll makes on the tier sits under one deadline, `poll_budget` (default 5 s), set
when the poll is admitted: admission, permits, the cache reservation, the bandwidth bucket, an index
fetch and each chunk fetch, across every segment the walk crosses. `poll_budget` is at most 8 s, so
the reply and any HTTP relay to the primary land inside the 10 s `PARTITION_READ_TIMEOUT`, whose
expiry answers 11001, which the SDK does not retry. When the budget runs out the poll returns the
batches it already has, and only a poll with nothing yet replies 58. The fetch keeps running so the
retry hits the cache.

Cold polls have their own admission cap, `cold_poll_capacity` per shard, checked before a poll
takes a completion-lane slot. A full cap replies 58 at once, so cold reads never hold more than
that many of the lane's `sharding.poll_completion_capacity` slots, and warm polls keep the rest
even while the store is down.

A failed fetch puts its chunk into a per-shard negative cache with exponential backoff from 1 s to
30 s. A poll that needs a chunk in it replies at once, without a GET and without parking on the
lane, so client retries cost the store nothing. Store errors map to two client codes. Transient
ones (throttling, timeouts, transport errors, a full cache) reply `TransientNotAccepted` (58).
Permanent ones (rejected credentials, an archived object, an object missing above what total
retention removed, a malformed response, a backend fingerprint mismatch) reply a new non-retryable
code, `TieredReadFailed`, which every SDK learns in phase 4. The Java SDK's poll path, which
retries 58 every 50 ms, gains the backoff the Rust SDK already has. Per-batch `batch_checksum`
validation, on by default through `[partition] validate_checksum`, is forced on for remote chunks.

Only the partition primary serves polls, so cold reads and uploads happen on the same node. A
backup's cache is used only once it becomes primary, and after a rebalance the new primary's cache
is cold for every remote segment.

### Retention

| Envelope | Options | Applies to | Decided by | Consumer barrier |
| --- | --- | --- | --- | --- |
| Local | `local_retention_age`, `local_retention_size` | uploaded sealed segments | each replica, locally | no |
| Total | `message_expiry`, `max_topic_size`, `DeleteSegments` | local and remote together | primary, replicated as `TruncatePartition` | yes |

For a tiered topic only the primary evaluates total retention. Per-replica expiry is off: no
replica's cleaner removes segments by its own clock, so every replica's chain loses remote segments
only through the replicated watermark. Before a topic enables tiering, each of its replicas
expires its prefix by its own clock, so they can start with different fronts, and the primary's
need not be the furthest. Each replica records its front in its chain file's trim point when
it applies the enable, and raises the trim point before every later front move (see
[The local marker](#the-local-marker)). A primary whose recorded trim point is above both the
floor and `deleted_up_to_offset` therefore commits `TruncatePartition` up to that trim point
before it uploads, so its first advance's `start` passes the contiguity rule: the primary at
enable, and any later one whose clock had expired further. The bound is the recorded trim
point, never the first segment the primary holds: a primary without the file commits nothing,
and a front lost after the enable is refused at boot before its replica can act on it. On its
peers the truncation removes only data below the front the primary's file recorded at the
enable: data its own retention had expired, or a prefix it lost before tiering, which
master's boot accepts and nothing afterwards can tell from expiry. Nothing assumes the fronts
agree: boot checks each chain against its own trim point, and a new primary's
LIST deletes nothing above its own. The cleaner on the primary evaluates `leading_expired_end` and
`leading_oversized_end` over the full chain (remote segments count their marker `size` and
`max_timestamp`), clamps to the consumer barrier, and submits `TruncatePartition { up_to }` when the
watermark advances. The reconciler's truncate enforcement handles remote segments: the
`RemoveSegment` job trims a remote segment's marker from the chain and unlinks its index, as it
unlinks `.log`, `.index` and `.anchor` today, and every removal path drops a segment's marker
whatever its location, so a marker left by an interrupted eviction cannot resurrect a removed
segment. Acceptance retires the segment and decrements stats. External consumer groups never hold
the barrier, as today.

The primary also deletes objects, but only those of segments its own `RemoveSegment` accepted,
never straight from the watermark: `DeleteSegments` resolves its offset unclamped while enforcement
stops at the consumer barrier, so the watermark can sit above segments every replica still keeps.
Deletion is idempotent and bounded per pass (300) and per partition pending (5000). A replica that
becomes primary LISTs the partition's prefix once and deletes every object whose segment ends at or
below both `deleted_up_to_offset` and its own trim point, which covers a primary move inside the
grace period and an upload that landed after its deletion. It never deletes merely below its own
front: a replica that expired further by its own clock before tiering has a front above segments
its peers still hold as markers. A partition whose pending deletions overflow the cap falls back to
the same LIST.
Deletion is two-phase with a grace period: an object becomes deletable only `delete_grace` (60 s)
after the removal that retired it was accepted, and no housekeeping job deletes any object younger
than `delete_grace`, whatever the reason. The grace closes the window in which a cold poll that
resolved a key just before the truncation would meet a 404, and it protects a freshly uploaded
object whose floor command has not landed yet from every sweep.

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
read and sent `upload_piece_size` (1 MiB) at a time, a chunk is written the same way, and every
piece ends in a real I/O await (a `read_at`, a socket write). CPU-only stretches such as hashing
and decoding call `yield_to_reactor()` from `server_common` every 4 MiB, the granularity state
transfer chose because one yield costs about as much as hashing half a MiB. Tier CPU work never
runs inside a frame body, where a yield does not let the consensus tick run. Second, concurrency is
capped per class with a small single-threaded semaphore written for this purpose: an
`Rc<RefCell<_>>` holding a permit count and a FIFO of waiters, `acquire().await` returning an RAII
permit released on drop, no atomics because nothing crosses threads. The same primitive with
permits counted in bytes is the cache reservation (reserve before download) and the upload and
download token buckets.

One semaphore per class gives the priority order the design needs. Demand fetches for waiting polls
hold their own permits and are never starved by uploads or by speculative fetches: hydration and
readahead have a smaller class of their own, are cancelled when demand fetches queue, and never
hold a permit a demand fetch needs. Uploads for partitions under local storage pressure are queued
ahead of ordinary uploads. Object deletes run at `housekeeping.delete_rate` on their own quota,
never gated on idleness, so deletion keeps pace with object creation on a busy shard. The rest of
housekeeping (verify HEADs, manifest PUTs, the orphan sweep) runs only when the shard's request
rate has been below `housekeeping.idle_threshold_rps` for `housekeeping.idle_timeout`, or once per
`housekeeping.interval`, spending at most `housekeeping.request_quota` requests per run.

Tier jobs deliberately do not run on the partition I/O lane. The lane holds one job per partition
and stops the node when a job outlives its two-minute deadline, so a 1 GiB upload there would block
the partition's materialization and could take the node down on a slow store. Only the local file
mutations, eviction and the marker and index removals, go through the lane. Nothing in master
provides the counting semaphore: `LocalGate` is a binary gate and the existing admission counters
have no wait queue. Directory reads have no io_uring operation and need `run_blocking`, one call at
a time per shard and shared with the WAL's preallocation and truncation, so the cache walks its
directory only at boot and the `fs` provider keeps keys in nested directories and lists one
partition directory per call.

#### The abstraction

```rust
pub trait ObjectStore {
    async fn put(&self, key: &str, body: &mut impl PieceSource, meta: &ObjectMeta, if_absent: bool)
        -> Result<PutOutcome, ObjectStoreError>;
    async fn get_range(&self, key: &str, range: Range<u64>) -> Result<Owned<4096>, ObjectStoreError>;
    async fn head(&self, key: &str) -> Result<Option<ObjectMeta>, ObjectStoreError>;
    async fn delete(&self, keys: &[String]) -> Result<(), ObjectStoreError>;
    async fn list(&self, prefix: &str, after: Option<&str>) -> Result<ListPage, ObjectStoreError>;
}
```

Multipart methods exist for objects above 5 GiB, which Iggy cannot produce; a segment is one
streamed PUT with an exact `Content-Length`. `PieceSource` declares the body's exact length and
yields it in reused `upload_piece_size` buffers, so no provider, `fs` and `sim` included, ever holds
a whole segment in memory.

Three layers:

1. The trait. Plain string keys, the aligned buffers the server already uses, one logical operation
   per method, and a typed error enum (`NotFound`, `PreconditionFailed`, `Throttled`, `Transport`,
   `Auth`, `Archived`, `Malformed`). Retries, backoff and timeouts live above the trait in the tier
   service.
2. The provider: a signer, a URL layout and a response parser, pure and sans-I/O, unit-tested against
   recorded fixtures.

   | Provider | Auth | Conditional create | Body checksum the store verifies | Covers |
   | --- | --- | --- | --- | --- |
   | `s3` | SigV4, `STREAMING-UNSIGNED-PAYLOAD-TRAILER` for PUT bodies | `If-None-Match: *` | `CRC64NVME` trailer | AWS S3, MinIO, Ceph RGW, R2, GCS interop |
   | `gcs` | S3 XML dialect with an OAuth bearer | `x-goog-if-generation-match: 0` | `crc32c` | GCS without HMAC keys |
   | `azure` | SharedKey, SAS, or OAuth bearer | `If-None-Match: *` | `crc64` | Azure Blob, hierarchical namespaces detected |
   | `fs` | none | publish by `link` or `renameat2(RENAME_NOREPLACE)` | none beyond the uploader's stream hash | a directory; Docker-free tests |
   | `sim` | none | map insert | checked on insert | the deterministic simulator, with fault injection |

   The `fs` provider never writes under a final key. It writes the body and its metadata to a
   temporary file, fsyncs it, publishes it with `link` or `renameat2(RENAME_NOREPLACE)` and fsyncs
   the directory, so a crash leaves a temporary file the next sweep removes, never a torn object
   under a final key. The contract tests cover a crash mid-PUT on every provider. The S3 signer may
   be `rusty-s3` (sans-I/O, MIT/Apache-2.0) or hand-written SigV4; GCS and Azure signers are
   hand-written.
3. The transport: one `cyper::Client` per shard over rustls, a connection pool per endpoint,
   connections idle longer than 5 s closed before reuse, a lease watchdog per request, and a boot
   self-configuration probe that picks virtual-host or path style and fails fast on deterministic
   errors only. The client gets an async DNS resolver over compio's own sockets, with a TTL cache
   and periodic re-resolution so a shard's pool spreads over the store's front-end addresses: the
   default resolver runs `getaddrinfo` on compio's blocking pool, which panics a shard.

Credentials are applied per request. One refresher task per node, on shard 0, keeps them current
and publishes them to every shard through an atomic swap, the only cross-thread state tier work
has, so a provider chain is called once per node rather than once per shard. `credentials_source`
is `static` (the config or the `IGGY_TIERED_STORAGE_S3_*` variables) in the first delivery. Then
come `imds` (IMDSv2), `web_identity` (STS, reading the standard `AWS_ROLE_ARN` and
`AWS_WEB_IDENTITY_TOKEN_FILE`), and the GCE metadata and Azure workload and VM identity sources,
each an HTTP call on the same transport, refreshed at 90 % of lifetime, with a rate-limited
out-of-band refresh on 401 or 403. Other `AWS_*` variables are not read.

Every operation runs under a deadline tree. A retry is permitted
only while `now + backoff` stays inside the parent's deadline, and the remaining time is the
per-request timeout. Backoff is exponential with jitter from 100 ms. Provider errors map to `retry`,
`fail`, `not_found`, `auth` or `unsupported` before any retry logic sees them. An upload has no
fixed total deadline, so a large segment on a congested link is never restarted from byte zero for
being slow. An upload that makes no progress for `upload_stall_timeout` is cancelled and retried
with backoff, and one whose shard loses primacy of the partition, or whose incarnation retires, is
cancelled for good.

### Cluster interactions

- **State transfer.** The manifest gains a `REMOTE_CHAIN` artifact carrying the partition's
  `remote.chain` file (the manifest caps at 65536 entries, so one artifact rather than one per
  segment). The receiver installs the file and fetches indexes lazily, and the state machine's floor
  travels with the offsets artifact the way consumer offsets do, installed as the larger of the
  local and the received value, so an installed replica never starts at floor zero or below its
  markers. No segment bytes cross between replicas for tiered data. The install backup links the
  chain file like every other file; quarantine, converge and the install sweep handle
  `remote.chain` alongside `.log`, `.index`, `.staging` and `.anchor`.
- **Delete.** Deletes are two-phase. Once the history fence commits, the incarnation's data-log
  requests are refused with `HistoryUnavailable`, but that fence does not reach the control log. A
  delayed floor command for a deleted incarnation is refused at apply instead, because it names its
  `created_revision`, which matches neither a deleted partition nor one re-created in the reused
  slot. When metadata completes the delete the reconciler removes the directory, markers included,
  and purges the incarnation's cache chunks. The metadata primary queues the deleted incarnation's
  prefix for object deletion directly. An orphan sweep remains as a rare safety net: it lists
  `{prefix}/{cluster_uid}/` at incarnation granularity under a metadata revision snapshot and
  deletes any incarnation absent from metadata with `created_revision` below the snapshot.
- **Manifest object.** Housekeeping on the primary uploads `manifest.bin` under `meta/` whenever the
  remote set changed, at most every 60 s. It is a derived artifact, never the source of truth for a
  live cluster; it exists so a partition can be restored into an empty cluster by reading one object
  and planting markers.
- **Verify pass.** A housekeeping job HEADs keys below the floor within its quota, cycling through
  them, records misses per partition, and compares the `log_checksum` in each object's user
  metadata with the marker's, so a missing or divergent object is reported as an anomaly instead of
  being found by a reader. Corruption in transit is caught earlier, by the body checksum the store
  verified at upload. Missing or mismatched objects above what total retention removed are data
  loss and are never repaired automatically.

### Configuration

```toml
[tiered_storage]
enabled = false
backend = "s3"                       # "s3" | "fs"; "gcs" and "azure" once their providers ship
upload_concurrency = 2               # per shard, the ceiling of the adaptive concurrency
upload_bandwidth = "unlimited"       # per node, divided across shards
upload_piece_size = "1 MiB"          # read, hash and send unit
upload_loop_backoff_max = "10 s"
upload_stall_timeout = "30 s"        # cancel and retry an upload that makes no progress this long
fetch_concurrency = 4                # per shard, demand fetches for waiting polls
speculative_fetch_concurrency = 2    # per shard, hydration and readahead
poll_budget = "5 s"                  # every tier wait of one poll; at most 8 s
cold_poll_capacity = 64              # per shard; below sharding.poll_completion_capacity
fetch_bandwidth = "unlimited"        # per node, divided across shards
connection_idle_timeout = "5 s"
delete_batch_per_pass = 300
delete_pending_per_partition = 5000
delete_grace = "60 s"                # no object is deleted sooner than this after it became deletable

[tiered_storage.housekeeping]
delete_rate = 10                     # delete requests per second per shard, never idle-gated
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
credentials_source = "static"        # "static"; later "imds" | "web_identity"
conditional_writes = true

[tiered_storage.fs]
path = ""

[tiered_storage.cache]
path = ""
size = "auto"                        # 256 MiB per shard
size_percent = 20
max_objects = 100000
chunk_size = "8 MiB"
readahead_max = 4
low_watermark_percent = 80
```

Validation: `enabled = true` requires a backend whose provider ships, with its required fields;
`poll_budget` at most 8 s, leaving any HTTP relay its margin inside the 10 s
`PARTITION_READ_TIMEOUT`; `cold_poll_capacity` below `sharding.poll_completion_capacity`;
`chunk_size` a multiple of 4096 within 1 MiB to 64 MiB; each shard's cache slice at least
`2 × chunk_size × (fetch_concurrency + speculative_fetch_concurrency)`, a deterministic refusal
only on a host whose disk cannot give every shard that much. The section joins the root
`ServerConfig` with `#[serde(default)]` and an entry in the embedded `config.toml`, and new
sub-structs use `deny_unknown_fields`, as the cluster sections do. Credentials use
`#[serde(default, skip_serializing)]` with `#[config_env(secret)]`, the pattern the cluster secrets
use, so they never appear in printed or serialized configuration.

Topic options are catalog keys. Their defaults are catalog constants, not server configuration, so
there are no server-level local-retention defaults.

| Key | Kind | Mutability | Default | Critical | Notes |
| --- | --- | --- | --- | --- | --- |
| `tiered_storage` | Bool | off to on only | off | yes | a new catalog mutability: admission and the metadata apply both refuse an update or reset that turns it off, with `OptionNotUpdatable`; enabling records the cluster's backend fingerprint and is refused while any node's differs |
| `local_retention_age` | expiry-style | updatable | 1 day | no | needs a type that tells "catalog default" from "unlimited", as `message_expiry` does; `IggyDuration` parses both as 0; an explicit value above a finite `message_expiry` is refused, and the default yields to a shorter one |
| `local_retention_size` | size-style | updatable | unlimited | no | per topic, divided by partition count like `max_topic_size`, and typed like it for the same reason; an explicit value above a finite `max_topic_size` is refused |
| `segment_max_age` | expiry-style | create-only | never | yes | age roll on any topic, tiered or not; create-only like `segment_size`; at least 10 minutes; refused together with `preallocate_segments` |

The local keys mirror the total ones: `local_retention_age` bounds locally what `message_expiry`
bounds in total, and `local_retention_size` what `max_topic_size` does. Their cross-key rules are
checked on create and update, at admission and at apply, the way `max_topic_size` is checked
against `segment_size`.

`tiered_storage` is critical so that a node that does not understand it refuses the topic instead
of serving it without tiering, and `segment_max_age` because a node that ignored it would roll at
different batches from its peers. Master records critical keys only when an update sets one, so
phases 4 and 5 extend create admission to record explicitly set critical keys as well. The marker
guards downgrade and restore: within one cluster the handshake already refuses mixed releases, and
`STORAGE_FORMAT` refuses an older binary on newer storage.

### Invariants

1. A replica evicts a local segment only when `end_offset <= tiered_up_to_offset` as committed in
   the partition's control log and its own commit-path hash of the segment equals the hash the
   advance carried. The floor advances only after the primary observed successful PUTs of both
   objects, validated every batch of the bytes it sent, matched their hash with its commit-path
   hash, and saw the store accept the body checksum. No replica ever drops the last copy of
   committed data.
2. Object keys are unique per cluster lifetime and partition incarnation, objects are immutable,
   and `.log` bytes are identical from any replica. An existing key counts as a duplicate upload
   only when its size and `log_checksum` match.
3. Local eviction never touches the active segment, the WAL tail or checkpoint-cursor segment, or a
   local sealed segment not yet uploaded. Tiered segments are always a prefix of the chain.
4. Boot accepts a chain whose prefix is markers and refuses any other absence of a `.log`, an
   undecodable chain record other than a torn last marker, and a front that starts above the trim
   point its own chain file records, a file every replica of a tiered partition has from the enable
   on. A missing file is created again at the current front, never refused. Boot never checks the
   chain against another plane's state.
5. Reads are transparent: a poll walk crosses a remote-to-local boundary by continuing at byte zero
   of the next segment, whichever source it has.
6. Deletion from the store is decided by the primary and replicated as an offset watermark, never by
   per-replica clocks or counts. Per-replica expiry is off for tiered topics, and the primary
   deletes objects only of segments its barrier-clamped removal accepted. A primary's front
   truncation goes no higher than the trim point its chain file records, so a front lost after the
   enable is refused at boot and never replicated.
7. The tiered floor is monotone for the life of a partition incarnation, and an advance applies
   only to the incarnation it names and only contiguously with the floor.
8. The consumer barrier applies to total retention and to nothing else.
9. A segment with a local `.log` is read locally. The store is consulted only for segments whose
   local copy is gone.
10. Correctness never depends on object-store conditional writes. A conditional PUT is a cost
    optimization with a HEAD-then-PUT fallback, and neither a 412 nor an existing key is success
    without a size and checksum match.
11. Every tier wait of a poll runs under one deadline inside `PARTITION_READ_TIMEOUT`, and cold
    polls never hold more completion-lane slots than their own cap, so a store outage never refuses
    a warm poll.

### Failure handling

| Failure | Behaviour |
| --- | --- |
| Store unreachable during upload | Retry with backoff; floor stays; eviction waits; producers and consumers unaffected; stalled gauge rises. Local data overshoots the local envelope until the store returns, and the overshoot is reported as a gauge; the only hard limit is the disk. |
| Store unreachable during a cold poll | The poll returns what it read within `poll_budget`, or `TransientNotAccepted` (58) if nothing; failed chunks enter the negative cache, so retries cost no GETs; the cold admission cap keeps warm polls serving. |
| Store refuses for a permanent reason | `TieredReadFailed`, which no SDK retries; an alarm names the cause (credentials, archived object, missing object, fingerprint mismatch). |
| Primary dies or is moved by a rebalance mid-upload | The rebalance does not wait for uploads; the old primary cancels them; the new primary re-commits the floor through its markers, LISTs the prefix, truncates to its recorded trim point if its clock expired further before tiering, and re-uploads; a 412 counts only after a HEAD shows the same size and `log_checksum`. |
| Upload lands after its segment was deleted | The uploader rechecks `deleted_up_to_offset` after the PUT and deletes the object after `delete_grace`; the LIST on becoming primary catches what a lost primary left. |
| Primary's stream hash differs from its commit-path hash, or a batch checksum fails during upload | The upload fails, the floor stays, an alarm rises; the primary's local copy is suspect and the other replicas still hold good ones. |
| Replica's hash differs from the advance's | That replica never evicts the segment and raises a divergence alarm. |
| Crash during a marker append | The torn last marker is truncated at boot; its `.log` was never unlinked, so the segment is local. |
| Crash between marker fsync and `.log` unlink | Boot sees both; treats the segment as local; eviction resumes without re-upload. |
| Crash between unlink and directory fsync | Marker was fsynced first, so the `.log` either reappears or is gone with a valid marker. No hole. |
| Undecodable chain record other than the last marker, or chain front above the chain file's trim point | Boot refuses the partition rather than accepting a shorter chain, and master's refusal path rebuilds it from its peers, so a lost front never reaches the front truncation. |
| Chain file missing on a tiered partition | Not refused, since a crash between the enable's apply and the file's creation looks the same; the replica creates the file at its current front. The new trim point exceeds the lost one only across evicted segments, at or below the floor, where the front truncation never acts. |
| Single-replica boot re-anchor planted a segment past a gap | The `.anchor` stays beside the segment once either side is evicted; the segment's advance carries `start = floor + 1`, since the gap holds no message, so the floor and eviction keep moving. |
| Object missing on GET | Below the front the primary's barrier-clamped removal reached: retire locally and clamp forward. A primary whose metadata has not yet applied the latest watermark treats the 404 as transient until it has. Otherwise data loss: fail the poll with `TieredReadFailed` rather than today's empty poll, and raise a metric. |
| Replica behind on the control log | Has not applied the latest floor yet, so it evicts less. Reads unaffected. |
| Replica restarts or is rebuilt by state transfer | The floor is restored from the control log's own replay point, or installed from state transfer as the larger of the two; a floor that still trails the markers is re-committed through them by the next primary, so nothing is re-uploaded or wrongly kept. |
| Topic or partition deleted during an upload | Upload lands under the deleted incarnation's prefix; the floor command names the deleted `created_revision`, so its apply is refused whether the slot is empty or re-created; the metadata primary queues the prefix for deletion. |
| WAL still hard-links an evicted segment | Every partition has a WAL. Eviction never touches the WAL tail or checkpoint-cursor segment, which the WAL always links and boot reads; below them, unlinking the public name is what retention does today, and WAL recovery never re-creates a fully checkpointed sealed segment. |
| Cache directory full | Trimming runs on the failed reservation; if pinned chunks fill the slice, the fetch fails typed and the poll replies 58; gauge rises. |
| Node boots during a store outage | Boots with `upload_stalled` raised; only a deterministic misconfiguration refuses boot. |
| Node's backend fingerprint differs from a topic's | The node neither uploads nor serves that topic's partitions; enabling tiering on a topic is refused until fingerprints match. |
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
| 5. Age roll | the master fix that unlinks rolled public names on WAL truncation |
| 6. The bucket as an archive | phase 4; the GCS and Azure providers need only phase 3 |

Phases 1, 3 and 5 can proceed in parallel. Phase 2 is the only one that waits on another RFC, and
phase 4 is where the branches join. Every phase that changes what is on disk (1, 2 and 5) bumps
`STORAGE_FORMAT`, so an older binary refuses the directory instead of unlinking kept indexes and
accepting a missing tiered prefix as a front trim.

1. **Local model without a network.** The `ObjectStore` trait with its typed errors and
   `PieceSource` bodies, the `fs` provider with its temporary-file publish, the provider contract
   test suite including a crash mid-PUT, and the single-threaded semaphore with its byte-counted
   variant. On top of them: `SegmentLocation`, the commit-path segment hash and its place in the
   receipts checkpoint, the `remote.chain` codec with its header, trim point, append, torn-tail and
   trim rules, the file's creation with a new partition and at the current front for a partition
   found without one, boot sweep and chain acceptance with the front checked against the trim point
   and `.anchor` gaps beside marker stems, quarantine, converge and install-sweep handling, the
   `EvictSegment` I/O job driven by a test-set floor and stopping at the WAL tail and
   checkpoint-cursor segments,
   `SegmentSource` in the poll plan, and the chunk cache keyed by full object key, with
   index-snapped boundaries, reservation, read pins and the persisted access-time table, filled by
   a direct call here and wrapped as a `FetchChunk` task in phase 3, all against the `fs` backend.
   The `STORAGE_FORMAT` bump ships here, with the first chain file on disk. Docker-free tests,
   including a boot after eviction reached the segment before the checkpoint cursor, a boot after
   eviction passed a re-anchored gap, a crash between a front removal's chain rewrite and its
   unlink, a boot that refuses front segments lost under a chain file, and one that recreates a
   missing file.
2. **Partition state machine and the floor.** Starts once the partition state-machine facility
   lands, including its choice of control log and its own replay point. The
   `AdvanceTieredFloor { created_revision, start, up_to, hash }` command, its apply, the floor's
   durable snapshot, its max-merged inclusion in state transfer, and the per-replica hash check.
   Tests: a partitions unit test that the floor is monotone, refuses a stale revision and a gap,
   and survives a restart; a simulator scenario across view changes, restarts and a delete and
   re-create of the same slot.
3. **Tier service and S3 provider.** The per-shard `TierService` with piece-wise tasks, the S3
   provider over `cyper` with an async DNS resolver, single streamed PUTs with a `CRC64NVME`
   trailer, conditional create with the HEAD comparison, the boot probe that refuses only
   deterministic errors, the deadline-tree retry model with stall timeouts and adaptive upload
   concurrency, the upload token bucket, the node credential refresher, the `sim` backend, config,
   Prometheus counters, and the pump tick latency measurement, the server's first latency
   distribution metric. Provider contract tests against `fs` and MinIO; a shard test that a 1 GiB
   upload never delays the tick beyond one piece while every class cap is saturated. The piece size
   and caps it ships are provisional until phase 4 measures them.
4. **End to end.** The upload loop with its strict oldest-first order and its `start` across a
   re-anchored gap, cancellation on loss of primacy, the post-PUT recheck and the duties of a new
   primary (floor re-commit, prefix LIST and front truncation, both bounded by its trim point),
   eviction staged on floor apply, the poll error channel with `poll_budget`, the cold admission
   cap, the negative cache and `TieredReadFailed` in every SDK, topic options with the one-way
   `tiered_storage` mutability and the chain file every replica creates when the enable applies, the
   cross-key rules and critical keys recorded on create, backend fingerprints, the local and total
   retention split with per-replica expiry off for tiered topics and the reconciler's truncate
   enforcement extended to remote segments, barrier-clamped primary object deletion with its own
   rate, deletion caps, incarnation deletion queued by metadata and the orphan sweep, the download
   token bucket, the `REMOTE_CHAIN` artifact. Integration scenarios under `--features vsr`,
   including a single-node re-anchor followed by uploads, eviction and a restart, a backup that
   expired further before tiering becoming primary, and one that lost front segments after the
   enable being rebuilt instead of truncating its peers. The same PR carries the measurement that
   settles phase 3's defaults: on one shard, an upload backlog, a cold-read storm, eviction passes
   and primary object deletion running together, against the pump tick latency gauge. An upload
   backlog first exists here, so the defaults cannot be settled earlier.
5. **Age roll.** `segment_max_age` as a general topic option with its 10-minute floor and its
   refusal beside `preallocate_segments`, the timestamp roll rule in materialization and in the
   WAL, first timestamps on the WAL's tail and checkpoint cursors across rewind, install and
   re-anchor, the WAL format change and its `STORAGE_FORMAT` bump. Independent of tiering, so it
   can land first, once master unlinks rolled public names above a rewound tail. Tests: the WAL and
   materialization rules seal at the same batch, including after WAL replay and after a truncation
   across an age roll, under the `SimStorage` harness; a three-node scenario in which every
   replica's sealed segments stay identical across a view change; and an integration test that
   `message_expiry` removes data on a topic that never fills a segment.
6. **The bucket as an archive.** Lazy manifests under `meta/`, shallow restore, the verify pass,
   and the GCS and Azure providers with their store-verified checksums, which can land any time
   after phase 3. Anything that watches manifests, restore included, detects change by ETag: one
   LIST of a `meta/` prefix returns the ETag of every partition manifest under it (1000 keys per
   page), and a conditional GET with `If-None-Match` costs a 304 when nothing changed.

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
  of milliseconds and can reply with a transient error, which SDKs handle but users must expect,
  or with the new `TieredReadFailed`, which every SDK has to learn.
- Enabling tiering on a topic is a one-way door until rehydration is built, even for a topic that
  never uploaded anything.
- Every replica of a tiered topic hashes every committed byte once more on its pump, and recomputes
  the active segment's hash after each restart.
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
  local and fast. Changing the local retention options never creates network work.
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
the price of cross-log ordering. The floor tolerates it because its command carries everything its
apply checks (the incarnation, the first offset it covers and the hash), so apply never reads the
data log, and eviction clamps the floor to each replica's local sealed chain. This departs from
master's own precedent: the consumer-group and history fences and session retirement went into the
data log, with an apply hook beside the message path.

Between the two realizations, the metadata plane exists today and already holds
`deleted_up_to_offset`. The submit path from the owner shard and the reconciler pass a floor there
needs now exist, since master uses them for lifecycle completion and partition fences, but such a
floor still lags the data by a reconcile interval and shares one cluster-wide group with everything
else. A control group per shard avoids the shared group and the reconcile lag and scales with
shards, though the partition primary still forwards its command to the group's primary, which may
be on another node, and the group's membership must not depend on each node's shard count. The
floor's rate makes the choice immaterial for this RFC, so it is deferred to the facility RFC rather
than decided here.

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
  already does for its fences. It must give the control log a snapshot and replay point of its own,
  and define a per-shard group's membership independently of each node's shard count. Phase 2 here
  cannot start before it lands, and it is the only phase that waits on it. Mixed-version clusters
  cannot form, since the replica handshake refuses a different release or storage format, so no
  replica ever meets a command it does not know.
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
  offset, which the verify pass can detect, instead of a 412. The cost is that an object can no
  longer be found by offset alone: a reader needs the marker or a LIST of the offset prefix. The
  HEAD comparison on every 412 and the hash in the floor command cover most of the benefit without
  the cost.
- Defaults to measure rather than argue: the chunk size (8 MiB here, 16 MiB in Redpanda),
  `readahead_max`, `cold_poll_capacity`, the upload stall timeout, and the recommended
  `segment_size` for tiered topics, all against S3 in-region. Whether tiered topics need a
  `segment_size` floor above the catalog's 1 MiB, for the same per-segment costs the age roll's
  floor bounds, is decided by the same measurement.
- Whether backups may upload when the primary is stalled (this RFC says no; failover re-uploads),
  and whether eviction should request a WAL checkpoint eagerly, since eviction stops at the WAL
  checkpoint-cursor segment and a lagging checkpoint holds local data past the envelope.
- Whether the rebalance should avoid placing a tiered partition's primary on a node whose backend
  fingerprint differs, rather than letting that node refuse to serve it.
- Whether a remote segment's `.index` should leave the partition directory at eviction and live in
  the chunk cache, so the files the install backup links stop growing with total retention, at the
  price of an index fetch on a cold poll that today's kept index avoids.

Out of scope for this RFC, addressable later independently of it:

- S3-direct storage modes. Not planned; if ever wanted, the shape is Redpanda's cloud topics and it
  reuses every component here.
- Restore and mount: with the lazily uploaded manifest under `meta/`, restoring a partition into an
  empty cluster is a shallow recovery that plants markers and an empty active segment at
  `last_offset + 1`, with a topic manifest beside it carrying the topic's options. The restored
  partition is a new incarnation in a cluster with another `cluster_uid`, so its markers must name
  the source prefix their objects live under.
- Read-replica topics that follow another cluster's manifests and serve them read-only, polling by
  ETag rather than downloading each manifest on an interval.
- Adjacent-segment merging, an optimization once age-rolled segments accumulate.
- Compression at rest and log compaction, both of which interact with uploaded objects and are
  tracked on the roadmap separately.
- Protocol-aware recovery using a tiered object as a second source for a damaged local segment.
- Stats wire fields for local versus remote bytes, CLI and Web UI visibility, and a rehydration job
  so tiering can be turned off.
- Direct I/O: cache chunk files are read with the same 4096-aligned buffers as segment files, so an
  `O_DIRECT` switch covers both, provided sealed segment bytes stay unchanged.

Simulator versus integration coverage:

- The deterministic simulator can cover the protocol: monotonicity of `tiered_up_to_offset` across
  view changes and restarts; the apply's refusal of a stale revision and of a gap, including a
  delayed advance across a delete and re-create of the same slot; the eviction eligibility rules
  (floor, hash check, WAL tail and checkpoint-cursor segments, active segment, local retention
  budget); the `REMOTE_CHAIN` state-transfer artifact and its max-merged floor; the orphan sweep's
  incarnation logic; and the `sim` object store with injected latency, throttling and partial
  failures. The `SimStorage` harness that already drives the WAL and persistence with real bytes
  can cover the chain file codec, the crash points between marker append, fsync, unlink and chain
  trim, torn-tail truncation, boot acceptance of a marker-only prefix and refusal of a lost front,
  and the age roll's WAL and materialization rules agreeing, including after WAL replay and
  truncation.
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
