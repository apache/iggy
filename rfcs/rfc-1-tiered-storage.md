<!-- markdownlint-disable MD053 -->

- Feature Name: `tiered_storage`
- Start Date: 2026-09-29
- RFC PR: [apache/iggy#4404](https://github.com/apache/iggy/pull/4404)
- Discussion: [apache/iggy#4685](https://github.com/apache/iggy/discussions/4685)
- Iggy Issue: [apache/iggy#0000](https://github.com/apache/iggy/issues/0000)

## Summary

[summary]: #summary

Move sealed segment files from a partition's local directory to an object store (S3 and compatible
stores first, GCS and Azure Blob behind the same abstraction) and serve them back through a local
chunk cache, without changing what clients see. The partition primary uploads a segment once it
seals and commits a `tiered_up_to_offset` floor to the partition's control log, a replicated log
separate from the data log, where a small state machine applies it on every replica. Each replica
may then evict its local copy under a per-topic local retention policy. A tiered segment stays in
the partition's segment list, backed by a small local marker, so offset and timestamp resolution
never touch the network. Total retention is evaluated on the primary over local and remote data
together and replicated as the existing `TruncatePartition` watermark. Object store access goes
through an Iggy-owned abstraction with one compio-native HTTP transport and a small signer per
provider, so no third-party storage crate and no tokio runtime enter the server.

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
worth borrowing (see [Prior art](#prior-art)). The write path they use is rejected here because the
VSR log already sequences and replicates, which is the job their coordinators exist for. The README
advertises "data backups and archiving to disk or S3", and no archiver exists on master. This RFC
is what makes that sentence true.

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
endpoint = ""                 # empty = AWS; set for MinIO, Ceph, R2
prefix = "iggy"               # credentials from the config or IGGY_TIERED_STORAGE_S3_*
```

At boot the node probes the bucket once and refuses to start only on a deterministic
misconfiguration, such as rejected credentials or a missing bucket. A transient failure boots the
node with `upload_stalled` raised, so a restart during a store outage never costs the cluster a
replica. The credentials need list, get, put and delete on the prefix, and the prefix must have no
lifecycle rule that deletes or archives objects: Iggy deletes them itself, and an archived object
cannot be read without a restore. The `fs` backend points at a directory on another filesystem. It
is what tests use and what "archive to disk" means.

Every node must reach the same store, and matching configuration cannot prove it: per-node `fs`
directories at one path, or a MinIO on each node's `localhost`, look identical. The cluster writes a
store identity object under its prefix once, and a node uploads, evicts or reads remote segments
only after reading that object back through its own backend since it booted. Enabling tiering on a
topic is refused while any node cannot. A store that becomes unreachable later is an outage, during
which eviction of floored segments continues, while an identity found missing or different stops
the node's uploads, eviction and remote reads and answers `TieredReadFailed`. A node booted during
an outage evicts nothing until the store returns. A node that finds tiered segments on disk with no
backend configured refuses to boot.

A topic opts in with options, at creation or later:

```text
iggy topic create my-stream events 8 none 30d \
  --set tiered_storage=true \
  --set local_retention_age=6h \
  --set local_retention_size=50GiB \
  --set segment_max_age=1h
```

From then on `message_expiry` and `max_topic_size` mean total data, local and remote together, and
`local_retention_age` and `local_retention_size` bound what stays on local disk once it is safely
in the store. Enabling `tiered_storage` is one-way until rehydration exists: an update or reset
that turns it off is refused with `OptionNotUpdatable`.

Each node exports local and remote bytes, upload and fetch counts, cache hits and misses, a
per-partition upload lag and an `upload_stalled` gauge. A store outage affects neither producers
nor consumers of warm data while the disk has room: uploads retry, the floor stops, and cold reads
have their own admission cap. Segments uploaded before the outage keep being evicted, so local disk
passes the local envelope only once data written during the outage fills it. An outage that
outlasts the disk meets the node's [free-space reserve](#free-space-reserve), and producers get the
retryable `TransientNotAccepted` (58) instead of a full disk.

### For client developers

Nothing changes on the wire. A poll for a range that spans remote and local segments returns one
response, exactly as today. The only visible effect is latency: a poll that touches a segment
nobody has read recently pays one small object store request (two if the segment's index is not
local), typically 30 to 100 ms in-region, after which neighbouring polls are warm. Polls are served
by the partition primary, so that is where cold reads happen. If the store is slow, the poll
returns what it read within a bounded wait, or `TransientNotAccepted` (58) if it read nothing, and
the SDKs retry 58. A store failure no retry can fix (rejected credentials, an archived or missing
object) answers a new non-retryable error, `TieredReadFailed`.

Consumer offsets, consumer groups, auto-commit and time-based polls behave the same. A segment that
a slow consumer has not read yet may be evicted locally, because the data remains readable. It is
never deleted from the store while a committed consumer offset points before it, except the offsets
of external consumer groups, which never hold retention.

### For contributors

A tiered segment is a normal segment whose location is `Remote` instead of `Local`: its `.log` is
gone, its `.index` is kept or fetched on first use, and a marker in the partition's chain file lets
boot rebuild it without the network. The poll plan resolves a local or remote source per segment.
Remote segments are read through a chunk cache that the shard's own tier tasks fill with ranged
GETs, and a walk crosses into the next segment at byte zero, whatever its source.

All object store I/O lives in a new `core/tiered_storage` crate behind an `ObjectStore` trait, with
`s3`, `fs` and `sim` implementations first and `gcs` and `azure` later, behind the same contract
tests. Object store requests run as cooperative tasks on the shard that owns the partition and
never block it.

## Reference-level explanation

[reference-level-explanation]: #reference-level-explanation

### Facts about master this design depends on

Verified on `master @ 6ce2f92a9`.

- Sealed segments hold only committed data and are byte-identical across replicas: the primary
  alone assigns offsets, checksums and timestamps, and encrypts payloads once. The sparse `.index`
  differs per replica. Rolls are keyed on committed bytes and decided by the same rule at
  materialization and in the WAL at prepare time, and `segment_size` is create-only.
- Every disk-backed partition has a WAL that writes bodies into the public `.log` at prepare time and
  always hard-links its tail and checkpoint-cursor segments. Boot reads the checkpoint-cursor
  segment's `.log`, restores replicated partition state from a checkpoint cut at the data log's
  commit point, and replays the WAL above it.
- Boot finds segments by their `.log` files, unlinks an `.index` or `.anchor` without one, refuses a
  gap as a hole unless an `.anchor` covers it, and accepts a missing front prefix. A refused
  partition is rebuilt from its peers.
- Partition file work runs on a per-shard I/O lane, one job per partition at a time, and a job that
  outlives its deadline stops the node. The install backup links every file in one such job.
- Retention is local per replica and removes sealed segments only, stopping at the WAL checkpoint
  and the consumer barrier, which external groups never hold. On a topic whose active segment never
  fills, `message_expiry` removes nothing.
- `DeleteSegments` resolves an offset on the primary, unclamped by the barrier, and commits
  `TruncatePartition`, raising `deleted_up_to_offset` in the metadata plane. Every replica enforces
  it clamped by the barrier, and its apply refuses a stale incarnation. Topic and partition deletes
  are two-phase: a history fence in the partition log, then completion in metadata.
- Segments contain only message batches, and offsets are independent of op numbers. Replicated
  partition state (fences, dedup receipts) lives in the data log. The partition plane has no
  general state-machine facility and no control log, while the metadata plane has one.
- Every node replicates every partition and only the primary serves polls. Primaries are spread and
  rebalanced through a drained view change that waits only for pending history writes. Nodes can
  differ in shard count. Consensus repair never reads sealed segment bytes.
- A disk poll runs as a detached task on the pump's thread and returns through a bounded completion
  lane that rejects new polls with 58 when full. The requester waits at most 10 s, and the SDKs do
  not retry its expiry. A failed disk walk returns an empty poll.
- Upgrades require fresh storage: boot refuses a different `STORAGE_FORMAT`, and the replica
  handshake refuses peers with a different release or storage format.
- Shards have no blocking thread pool, the server runs no tokio runtime, and `cyper`, a
  compio-native HTTP client, is already in use.
- Topic options live in a catalog with create-only, updatable and read-only keys and compile-time
  defaults. A node refuses a resource whose critical-keys marker names a key it does not know, but
  master records the marker only on update.

### Object layout

```text
{prefix}/{cluster_uid}/{stream_id}/{topic_id}/{partition_id}/{created_revision}/{start_offset:020}.log
{prefix}/{cluster_uid}/{stream_id}/{topic_id}/{partition_id}/{created_revision}/{start_offset:020}.index
{prefix}/{cluster_uid}/meta/{stream_id}/{topic_id}/{partition_id}/{created_revision}/manifest.bin
```

Stream, topic and partition ids are reused after deletion, so `created_revision`, unique across the
cluster, identifies the partition incarnation. Offsets never restart within an incarnation, so it
separates every offset space within one cluster lifetime. It restarts on fresh storage, which every
upgrade needs, so `cluster_uid` is a random 128-bit id the metadata plane mints once when its log is
created. Master's `cluster_id` cannot serve, since it is a hash of the cluster name, whose default
every cluster shares. Keys never repeat across clusters sharing a bucket or across lifetimes of one
cluster, and no cluster deletes, sweeps or reads under another's `cluster_uid`. The zero-padded stem
matches the on-disk name, so a listing sorts by offset.

Objects are immutable. The `.log` object is canonical and identical whichever replica produced it,
and for an encrypted stream it is the same ciphertext the segment holds. The `.index` object is the
uploader's sparse index, valid for lookups on any replica, and its length is known only from the
object. User metadata on the `.log` object carries the segment's offsets, timestamps, size and
checksum, so a listing plus HEAD rebuilds segment metadata without a download.

### The local marker

A tiered segment leaves a marker, a small fixed-size record holding its offsets, timestamps, size,
incarnation and commit-path checksum, with a checksum of its own. Markers live in one file per
partition, `remote.chain`, rather than one per segment, which halves the files a remote segment
leaves for the install backup to link under its deadline. The chain file holds the markers plus a
trim point: the front the replica's own retention left, recorded when it applies the enable of
`tiered_storage` and raised before any later front move unlinks anything. A partition created on a
tiered topic gets the file with its directory. Writes to the file are crash-safe, and eviction
appends and fsyncs a marker before it unlinks the `.log`. A lost chain file or a lost front is
either rebuilt and verified against the store, or the partition is rebuilt from its peers. A
replica never drives a front truncation or an object deletion from a front it cannot verify, and an
unverified front on the primary must not stall the floor. A state-transfer install carries the
sender's trim point. Until a replica holds the file it neither evicts nor, as primary, uploads,
deletes objects or truncates the front.

Boot rules:

- A torn last marker is truncated, since its `.log` was never unlinked. Any other undecodable record
  refuses the partition, because a marker read as absent would silently drop a tiered prefix.
- A stem with a marker and no `.log` is tiered, and its `.index` and `.anchor` are kept. A stem with
  both is local, and eviction resumes without re-uploading.
- Tiered segments are always a prefix of the chain, below the WAL checkpoint-cursor segment.
  Contiguity uses marker bounds, a missing middle `.log` with no marker is still a hole, and an
  `.anchor` gap is admitted whichever side is tiered.
- The first segment must start at or below the trim point plus one, or boot refuses the partition.
  The check is local on purpose: metadata at boot can trail the last truncation this replica
  enforced, so comparing with `deleted_up_to_offset` would refuse healthy replicas.
- A missing chain file is not a refusal by itself, since boot cannot tell it from a crash between
  the enable and the file's creation. The replica rebuilds its markers from the store and refuses
  the partition when a front stays unaccounted for. A rebuilt trim point is unverified until no
  replica holds data below it.
- A marker of another incarnation is quarantined with the partition. Quarantine, converge and the
  install sweep handle the chain file beside the other partition files.

In memory a segment gains a location, `Local`, `Uploaded` or `Remote`, and tiered segments stay in
the segment list, so resolving a poll's start by offset or timestamp is unchanged.

### Roles

Every replica can check the uploaded bytes against its own copy before evicting, without re-reading
the segment on the eviction path. A mismatch never evicts and never advances the floor. To that end
each replica of a tiered topic hashes every segment on its commit path, so the hash describes the
bytes consensus agreed on rather than what a disk returns later. A segment not fully hashed that
way, because it sealed or was active before the replica applied the enable, or arrived by state
transfer, is hashed once from disk, a weaker guarantee named as such. Hashes are durable from the
moment they exist: a replica keeps each segment's hash, each applied advance's hash until its check
has run, and the check's result, and state transfer carries them, so no restart, snapshot or
install loses a hash or a pending check.

#### Primary: upload and floor advancement

Upload is primary-gated. A segment is queued when it seals, and a per-shard loop finds local sealed
segments above the floor. Uploads are strictly oldest-first per partition, one in flight at a time,
so a seal never overtakes a backlog and enabling tiering on a topic with sealed history uploads
that history first. Uploads are rate-limited so they do not take the network from replication, a
partition whose eviction waits on its floor goes first, a growing upload lag raises an alarm, and a
failed upload retries with backoff and blocks the floor.

A rebalance does not wait for uploads: the old primary cancels them when it loses primacy. The new
primary first re-commits the floor through its own markers, since a marker proves a floor once
committed. It then lists the partition's prefix once to rebuild its deletion cursor, commits any
front truncation its trim point needs (see [Retention](#retention)), and only then uploads,
skipping every segment that is not `Local`.

Upload reads the sealed `.log` up to its committed size through its own file descriptor, which keeps
working if retention unlinks the file. It validates every batch checksum and compares the stream's
hash with the commit-path hash, so a primary's media fault fails the upload and raises an alarm
instead of reaching the store. The store verifies a body checksum, and the upload counts only when
it matches. A `.log` key that already exists counts as uploaded only after a HEAD shows the same
size and checksum, and a difference raises a divergence alarm and leaves the floor. An existing
`.index` key needs only to be a valid index for that `.log`, since sparse indexes differ per
replica. If the primary's barrier-clamped removal retired the segment during the upload, its objects
are scheduled for deletion and no advance covers it. The watermark alone never deletes an object.

On success the primary commits `AdvanceTieredFloor { created_revision, start, up_to, hash }` to the
partition's control log: the incarnation, the first offset the advance covers, the segment's last
offset and its commit-path hash. `start` is the segment's first offset, or `floor + 1` when no
offset between the two holds a message, because each is at or below `deleted_up_to_offset` or inside
a gap a durable `.anchor` records. Master's boot re-anchor leaves such a gap on a single-replica
partition after a crash that lost unflushed messages, and without this case the floor would stop
there forever. Nothing in the group that realizes the control log fences on partition primacy, so
the command is safe by its contents: apply refuses a stale incarnation or a gap, and every replica
checks the hash against its own bytes, so a late command from a former primary can only repeat a
true statement.

#### Every replica: local eviction

Eviction follows the retention pattern: a pass on the pump picks the segments and the partition's
I/O lane runs one eviction job per segment, so eviction never holds the lane ahead of
materialization and never reads or hashes a segment. A pass is staged when the floor rises, as
truncation enforcement is today, and by the cleaner's timer for segments that age out of the local
envelope with no floor change.

The eligible run starts at the front and stops at the first of: the active segment, the WAL tail
segment, the WAL checkpoint-cursor segment, a segment that ends above the floor, a segment whose
hash check has not passed, or the local retention budget being satisfied. The WAL links its tail
and checkpoint-cursor segments and boot reads the latter, so evicting either would wedge boot on
every replica and free nothing. The run does not consult the consumer barrier. Per segment the job
appends the marker, fsyncs it and unlinks the `.log`. The owner then marks the segment `Remote` and,
as retention does after an unlink, drops what still names the file. The total size is unchanged.

The hash check compares each advance with the replica's own segment, which must end at `up_to` and
hash to `hash`. It runs when the advance applies, or when the replica seals that segment if its data
log trails its control log. A mismatch never evicts that segment and raises a divergence alarm.

#### Free-space reserve

Tiered storage must never turn a store outage or a stalled floor into a full disk: each node keeps a
free-space reserve large enough for reclaim to run, producers get the retryable error before it is
crossed, and a shortfall on one node alone must not stop the cluster. The reserve exists because a
stalled floor frees nothing on any replica, every reclaim step writes before it frees, and master
stops a node whose WAL write fails. A partition whose ingest outruns its uploads is throttled the
same way, so it writes at its upload pace rather than spending the reserve.

#### Partition state machine

The floor lives in a replicated state machine fed by the partition's **control log**, a consensus
sequence separate from the data log, so segments and the data journal hold only user messages. Its
realization belongs to a general partition state-machine facility, specified in its own RFC because
queue semantics, log compaction and the key-value store need it too: one control group per shard,
whose membership cannot depend on a node's shard count, or the metadata plane, which already is a
separate control log holding per-partition facts. Either way the primary forwards its command to
that group's primary, possibly on another node. The floor advances at most once per uploaded
segment, so either serves. This RFC needs from the facility the command encoding, the apply hook, a
durable snapshot with the control log's own replay point, and state-transfer inclusion.

The state machine holds one value, `tiered_up_to_offset`, and the `created_revision` it belongs to.
The partition's current incarnation lives in metadata, which the control log is not ordered with, so
the state carries its own. `AdvanceTieredFloor` applies as follows:

- A `created_revision` below the state's is refused, so a delayed command of a deleted incarnation
  never reaches its successor. One above the state's starts an empty floor for that incarnation.
- A `start` above `floor + 1` is refused, so the floor never jumps over a segment nobody uploaded.
  An empty floor counts as `floor + 1 = 0`.
- Otherwise the floor becomes `max(floor, up_to)`, and a repeated advance is a no-op.

The state learns of a new incarnation only from its first advance, so every reader of the floor
(the upload loop, a new primary's duties, eviction) and every hash check uses the state or an
advance only when its `created_revision` is the partition's local incarnation, and otherwise treats
the floor as empty and the check as not yet due.

Apply reads nothing but the command and the state, so every replica reaches the same floor whatever
its data log's progress, and eviction, not apply, clamps the floor to the local sealed chain. The
state holds no per-segment data: keys and markers carry the segment list, so the snapshot stays a
few bytes, and each replica checks the hash outside the state machine.

The control log needs its own replay point: master's partition-state checkpoint is cut at the data
log's commit point, so a floor applied after it would regress on restart, and the superblock, whose
writes follow view changes rather than applies, is a poor fit. The floor rides state transfer,
installed as the larger `(created_revision, floor)` pair, so a rebuilt replica never starts at
zero, and a floor that still trails a replica's markers is re-committed through them by the next
primary. The floor is monotone for the life of the partition incarnation.

`TruncatePartition` stays in the metadata plane, where it already ships. If the facility settles on
the metadata plane, both watermarks share a log from the start, and otherwise migrating it is a
follow-up amendment (see [Unresolved questions](#unresolved-questions)).

### Segment lifecycle

| State | Local files | Remote | Transition out |
| --- | --- | --- | --- |
| Active | `.log` (written by the WAL), `.index` | none | rotation seals it |
| Sealed, local | `.log`, `.index` | none | primary upload succeeds |
| Uploaded | `.log`, `.index`, a marker if a crash interrupted eviction | `.log`, `.index` | floor committed, hash checked, local retention exceeded |
| Remote | marker, `.index` (optional) | `.log`, `.index` | total retention raises `deleted_up_to_offset` |
| Retired | none | deleted | terminal |

A segment never moves back from Remote to Sealed-local in the first delivery.

#### Bounding how long data stays unuploaded

Uploading part of the active segment would create objects that match no local file, and a replica
cannot roll by its own clock, because segment boundaries must be a function of the batches alone.
Batch timestamps qualify: the primary assigns them and every replica stores them unchanged. So the
age bound is a roll rule with no new operation. With the topic option `segment_max_age`, a batch
whose timestamp is at least `segment_max_age` past the active segment's first timestamp seals it
and starts the next one, at the same batch on every replica, so segments stay byte-identical. A
primary whose clock lags only delays a roll. One whose clock ran ahead leaves a far-future stamp
that master's monotone stamp floor carries forward, and the roll stays silent until the wall clock
passes it.

Both roll sites, materialization and the WAL at prepare time, apply the rule identically, so the
WAL's durable state gains `segment_max_age` and each cursor's first timestamp, a WAL format change,
restored from a batch header whenever a cursor is rewound or rebuilt. The rule also makes an
existing master defect likelier: a WAL truncation below a roll unlinks the rolled-into `.log` only
when the replacement re-rolls at the same offset, and boot refuses a leftover file as a hole, so
truncation must unlink every rolled `.log` above the rewound tail. `segment_max_age` is create-only
for the reason `segment_size` is: an update arrives through the metadata plane, unordered with the
data log, so replicas would roll at different batches.

The rule is not a tiering option. On any topic that keeps receiving writes it bounds how long the
active segment stays unsealed, gaps included, since the first batch after a gap seals the old
segment, and it lets `message_expiry` reach data on a topic that never fills a segment. A fully idle
partition is left open, because a roll ordered with the batches needs an operation in the data log.

`segment_max_age` is at least 10 minutes, Redpanda's lower bound for the same setting. Each segment
costs two PUTs, a floor command, a marker, an eviction job and in-memory state on every replica, and
the bound caps that at 144 segments a day per partition, so an adjacent-segment merger stays an
optimization. A topic with `preallocate_segments` cannot set it: preallocation reserves a full
`segment_size` per segment, so every small age-rolled segment would pin a full segment's space.

### Read path

The poll plan carries a local or remote source per segment, decided on the pump from the segment's
location. The plan stops where the poll's byte budget runs out rather than at the tail, because
remote segments make the chain as long as total retention. A walk that finds a segment planned as
local already evicted re-plans it as remote instead of failing the poll.

#### Chunk cache

A directory under the node's data path holds chunks of remote `.log` objects under their full object
key, so `cluster_uid` and `created_revision` are part of every path and no incarnation ever reads
another's chunks. Chunk boundaries snap to batch boundaries through the sparse index, so a batch
never costs two GETs. A miss reserves cache space before it downloads. Chunks need no fsync: a
chunk whose length or batch checksums do not match is dropped and fetched again.

A cold poll does not wait for a whole chunk. It fetches exactly the walk's range first, serves the
walk from the response and hydrates the surrounding chunk in the background. Readers of the same
cold segment share one fetch per chunk, readahead adapts to sequential readers, and a poll further
behind the head than the cache could hold streams without admitting chunks, so a backfill cannot
evict the working set near the head. Each shard has its own slice of the cache budget, with no
cross-shard coordination, because a partition's chunks are only ever read by the shard that owns
it. The cache persists across restarts, and chunks of a retired incarnation are removed with its
directory.

#### Indexes for remote segments

A remote segment's index is the local `.index` when one is usable, and otherwise the `.index` object,
fetched on first use, checked to ascend inside the segment's size and written to disk crash-safely.
Its length comes from the object itself, never from segment metadata: sparse indexes differ per
replica, and after a primary move the `.index` object may come from a different uploader.

#### Latency, timeouts and errors

Tiering gives the poll plan the error channel it lacks today. Every wait a poll makes on the tier
sits under one deadline, set at admission and short enough that the reply, and any HTTP relay to
the primary, lands inside the 10 s read timeout, whose expiry the SDKs do not retry. A poll past its
deadline returns the batches it has, or 58 when it has none, and the fetch keeps running so the
retry hits the cache. Cold polls have their own per-shard admission cap, checked before a poll takes
a completion-lane slot, so a store outage never refuses a warm poll. A failed fetch is remembered
with backoff, so client retries reply at once and cost the store nothing.

Transient store errors (throttling, timeouts, transport errors, a full cache) reply 58. Permanent
ones (rejected credentials, an archived object, an object missing above what total retention
removed, a malformed response, a store identity mismatch) reply a new non-retryable code,
`TieredReadFailed`, which every SDK learns. The Java SDK's poll path, which retries 58 every 50 ms,
gains the backoff the Rust SDK has. Batch-checksum validation is always on for remote chunks. Only
the primary serves polls, so cold reads and uploads happen on the same node, and after a rebalance
the new primary's cache is cold.

### Retention

| Envelope | Options | Applies to | Decided by | Consumer barrier |
| --- | --- | --- | --- | --- |
| Local | `local_retention_age`, `local_retention_size` | uploaded sealed segments | each replica, locally | no |
| Total | `message_expiry`, `max_topic_size`, `DeleteSegments` | local and remote together | primary, replicated as `TruncatePartition` | yes |

For a tiered topic only the primary evaluates total retention, and per-replica expiry is off, so
every replica's chain loses remote segments only through the replicated watermark. The primary
evaluates expiry and size over the full chain, counting remote segments by their markers, clamps the
result to the consumer barrier and commits `TruncatePartition`. Truncation enforcement removes a
remote segment's marker and index as it removes local files, and every removal path drops a
segment's marker whatever its location, so a marker left by an interrupted eviction cannot
resurrect a removed segment.

Before a topic enables tiering, each replica expires its prefix by its own clock, so fronts can
differ and the primary's need not be the furthest. Each replica records its front as the chain
file's trim point when it applies the enable. A primary whose trim point is above both the floor and
`deleted_up_to_offset` commits `TruncatePartition` up to it before uploading, so its first advance
passes the contiguity rule. The bound is the recorded and verified trim point (see
[The local marker](#the-local-marker)), never the first segment the primary happens to hold. On the
peers it removes only data the primary itself had expired or lost before tiering, which master's
boot accepts and nothing afterwards can tell from expiry.

The primary deletes objects only of segments its own barrier-clamped removal accepted, never
straight from the watermark, because `DeleteSegments` resolves its offset unclamped and the
watermark can sit above segments every replica still keeps. A replica that becomes primary lists
the prefix once and deletes every object whose segment ends at or below both `deleted_up_to_offset`
and its own verified trim point, which covers a primary move mid-deletion and an upload that landed
after its segment was deleted. It never deletes merely below its own front, since a replica that
expired further before tiering has a front above segments its peers still hold as markers.

Deletion is two-phase: an object becomes deletable only a grace period after the removal that
retired it was accepted, and no housekeeping job deletes an object younger than the grace period,
whatever the reason. The grace covers a cold poll that resolved a key just before the truncation,
and a freshly uploaded object whose floor command has not landed yet. Deletion is idempotent and
bounded per pass. The partition's size statistic keeps meaning total retained bytes.

### Tier service and backends

All tier work runs on the shard that owns the partition, as cooperative compio tasks, with no extra
threads. Each shard has one tier service owning its HTTP client, its slice of the cache and its
per-class caps. Every job runs as a detached task, the way disk polls do, never inline in a frame
handler, so the pump, including the consensus tick, keeps running between the task's awaits. Two
rules keep a 1 GiB upload from delaying the pump:

- Work is done in pieces, each ending in a real I/O await, and CPU-only stretches such as hashing
  yield to the reactor periodically, never inside a frame body.
- Concurrency is capped per class with a single-threaded counting semaphore, and demand fetches for
  waiting polls are never starved by uploads or speculative fetches. Object deletes run on their own
  quota, never gated on idleness, so deletion keeps pace with creation, and the rest of
  housekeeping (verify, manifests, the orphan sweep) runs when the shard is idle, within a quota.

Tier jobs do not run on the partition I/O lane. The lane holds one job per partition and stops the
node when a job outlives its deadline, so an upload there would block the partition's
materialization and could take the node down on a slow store. Only local file mutations, eviction
and marker and index removal, go through the lane. The `fs` provider never blocks a shard thread on
store I/O, and a hung mount affects only the provider, which sees it as a store outage.

#### The abstraction

The `ObjectStore` trait has five operations: a streamed PUT of exact length with an optional
conditional create, a ranged GET, HEAD, batch DELETE and a paged LIST. Keys are plain strings,
errors are typed (not found, precondition failed, throttled, transport, auth, archived, malformed),
and retries and timeouts live above the trait. A segment is one streamed PUT, since Iggy never
produces an object above the single-PUT limit, and no provider holds a whole segment in memory.
Each provider is a sans-I/O signer, URL layout and response parser, unit-tested against recorded
fixtures, over one compio-native HTTP client per shard (`cyper` over rustls) whose hostname
resolution never blocks the shard.

The `s3` provider (AWS S3, MinIO, Ceph RGW, R2, GCS interoperability), `fs` (a directory on another
filesystem, used by Docker-free tests) and `sim` (the deterministic simulator, with fault injection)
ship first. `gcs`, for GCS without HMAC keys, and `azure`, for Azure Blob including hierarchical
namespaces, follow in phase 6. Every provider uses the store's conditional create and a
store-verified body checksum where the store offers them, and `fs` publishes atomically, so a crash
never leaves a torn object under a final key. Credentials are refreshed once per node and shared
with every shard: static first, then instance metadata, web identity and the GCS and Azure workload
identities. An upload has no total deadline, so a large segment on a congested link is never
restarted for being slow, but one that makes no progress is cancelled and retried, and one whose
shard loses primacy or whose incarnation retires is cancelled for good.

### Cluster interactions

- **State transfer.** The chain file travels as one artifact rather than one entry per segment,
  since the transfer manifest's entry count is capped. The receiver installs it, trim point
  included, with the floor, and fetches indexes lazily. No segment bytes cross between replicas
  for tiered data.
- **Delete.** The history fence of a two-phase delete does not reach the control log, so a delayed
  floor command for a deleted incarnation is refused at apply once the new incarnation's first
  advance has applied, and before that it only moves a floor no reader of the new incarnation
  uses. When metadata completes the delete, the reconciler removes the directory and the
  incarnation's cache chunks, and the metadata primary queues its prefix for object deletion. An
  orphan sweep lists `{prefix}/{cluster_uid}/` at incarnation granularity under a metadata
  revision snapshot and deletes incarnations absent from metadata, as a safety net.
- **Manifest object.** The primary uploads `manifest.bin` under `meta/` when the remote set changes,
  rate-limited. It is derived, never the source of truth for a live cluster, and exists so a
  partition can be restored into an empty cluster by reading one object and planting markers.
- **Verify pass.** Housekeeping HEADs keys below the floor within a quota and compares each object's
  checksum with the marker's, so a missing or divergent object is reported as an anomaly rather than
  found by a reader. Such an object above what total retention removed is data loss, never
  repaired automatically.

### Configuration

The server section names the backend and its location (bucket, prefix and endpoint for `s3`, a path
for `fs`), the credentials and the chunk cache's size. It joins the server configuration with
defaults, so existing files keep working, and credentials follow the pattern of the cluster secrets,
so they never appear in printed or serialized configuration. Concurrency, bandwidth, timeouts, the
poll deadline, deletion pacing and the grace period are tuning knobs settled in implementation.

Topic options are catalog keys. Their defaults are catalog constants, not server configuration, so
there are no server-level local retention defaults.

| Key | Kind | Mutability | Default | Critical | Notes |
| --- | --- | --- | --- | --- | --- |
| `tiered_storage` | Bool | off to on only | off | yes | a new catalog mutability: turning it off is refused with `OptionNotUpdatable`; enabling is refused while any node cannot read the store identity |
| `local_retention_age` | expiry-style | updatable | 1 day | no | typed to tell "catalog default" from "unlimited", as `message_expiry` is; an explicit value above a finite `message_expiry` is refused, and the default yields to a shorter one |
| `local_retention_size` | size-style | updatable | unlimited | no | per topic, divided by partition count like `max_topic_size`; an explicit value above a finite `max_topic_size` is refused |
| `segment_max_age` | expiry-style | create-only | never | yes | age roll on any topic, tiered or not; at least 10 minutes; refused together with `preallocate_segments` |

The local keys mirror the total ones, and their cross-key rules are checked on create and update,
at admission and at apply, the way `max_topic_size` is checked against `segment_size`.
`tiered_storage` is critical so that a node that does not understand it refuses the topic instead of
serving it without tiering, and `segment_max_age` because a node that ignored it would roll at
different batches from its peers. Master records critical keys only on an update, so create
admission records explicitly set critical keys as well.

### Invariants

1. A replica evicts a local segment only when it ends at or below the committed floor and its own
   commit-path hash equals the advance's. The floor advances only after the primary stored both
   objects with every upload check passed (see [Roles](#roles)). No replica ever drops the last
   copy of committed data.
2. Object keys are unique per cluster lifetime and partition incarnation, objects are immutable,
   and `.log` bytes are identical from any replica. An existing `.log` key counts as a duplicate
   upload only when its size and checksum match.
3. Local eviction never touches the active segment, the WAL tail or checkpoint-cursor segment, or a
   sealed segment not yet uploaded. Tiered segments are always a prefix of the chain.
4. Boot accepts a chain whose prefix is markers and refuses every other missing `.log`, every
   undecodable record but a torn last marker, and a front above the trim point. Outside rebuilding
   a lost chain file, boot never checks the chain against another plane.
5. Reads are transparent: a poll walk crosses a remote-to-local boundary by continuing at byte zero
   of the next segment, whichever source it has.
6. Deletion from the store is decided by the primary and replicated as an offset watermark, never
   by per-replica clocks, and objects are deleted only for segments a barrier-clamped removal
   accepted. A front truncation goes no higher than a verified trim point, so a front lost after
   the enable never reaches the peers or the store.
7. The tiered floor is monotone for the life of a partition incarnation, and an advance applies only
   to the incarnation it names and only contiguously with the floor.
8. The consumer barrier applies to total retention and to nothing else.
9. A segment with a local `.log` is read locally. The store is consulted only for segments whose
   local copy is gone.
10. Correctness never depends on object store conditional writes. A conditional PUT is a cost
    optimization, and neither a precondition failure nor an existing `.log` key is success without
    a size and checksum match.
11. Every tier wait of a poll runs under one deadline inside the read timeout, and cold polls never
    hold more completion-lane slots than their own cap, so a store outage never refuses a warm poll.
12. Tiering never fills a data disk. Producers are refused before a node crosses its free-space
    reserve, so reclaim can always run once the floor moves again.

### Failure handling

| Failure | Behaviour |
| --- | --- |
| Store unreachable during upload | Uploads retry with backoff, the floor stays and eviction waits. Producers and consumers are unaffected while the disk has room, and the stalled gauge rises. Producers are refused before the free-space reserve, and eviction frees the space once the store returns. |
| Store unreachable during a cold poll | The poll returns what it read within its deadline, or 58. Failed chunks are remembered, so retries cost no GETs, and the cold admission cap keeps warm polls serving. |
| Store refuses for a permanent reason | `TieredReadFailed`, which no SDK retries, and an alarm naming the cause. |
| Primary dies or is moved mid-upload | The old primary cancels its uploads. The new one re-commits the floor through its markers, lists the prefix, truncates to a verified trim point if needed and re-uploads. An existing `.log` key counts only after a size and checksum match, and an existing `.index` only needs to be valid. |
| Upload lands after its segment was deleted | The uploader schedules the object's deletion. The listing on becoming primary catches what a lost primary left. |
| Primary's stream hash or a batch checksum fails during upload | The upload fails, the floor stays and an alarm rises. The other replicas still hold good copies. |
| Replica's hash differs from the advance's | That replica never evicts the segment and raises a divergence alarm. |
| Crash during eviction | A torn last marker is truncated at boot, and its `.log` was never unlinked. A marker beside its `.log` is a local segment whose eviction resumes without re-upload. The marker is fsynced before the unlink, so no crash leaves a hole. |
| Undecodable chain record, or chain front above its trim point | Boot refuses the partition and it is rebuilt from its peers, so a lost front never reaches the front truncation. |
| Chain file missing on a tiered partition | The replica rebuilds its markers from the store before serving the partition, or refuses it when a front stays unaccounted for. The rebuilt trim point drives no truncation or deletion until verified. |
| Single-replica re-anchor gap | The `.anchor` stays beside the segment, and the segment's advance carries `start = floor + 1`, so the floor and eviction keep moving. |
| Object missing on GET | Below what the primary's removal reached: retire locally. A primary whose metadata trails the latest watermark treats it as transient. Otherwise data loss: `TieredReadFailed` and a metric. |
| Replica behind on the control log | It evicts less until it applies the latest floor. Reads are unaffected. |
| Replica restarts or is rebuilt by state transfer | Markers rebuild segment metadata and the cache survives. The floor comes back from the control log's own replay point, or is installed as the larger of the two, and one that still trails the markers is re-committed through them by the next primary. |
| Topic or partition deleted during an upload | The floor command names the deleted incarnation, so its apply is refused, and the metadata primary queues the prefix for deletion. |
| WAL still hard-links an evicted segment | Eviction never touches the WAL tail or checkpoint-cursor segment. Below them, unlinking the public name is what retention does today. |
| Cache full | The cache trims on the failed reservation. A fetch that still finds no room fails as transient, and the poll replies 58. |
| Node boots during a store outage | It boots with `upload_stalled` raised and evicts nothing until it has read the store identity. Only a deterministic misconfiguration refuses boot. |
| Node finds the store identity missing or different | The node neither uploads, evicts nor reads remote segments, and answers `TieredReadFailed`. Enabling tiering is refused until every node reads it. |
| Clock skew against the store | One re-sign with the offset the store's rejection reports. A second failure is a hard error. |

### Implementation plan

Each phase is one reviewable PR or a short stack. The phases form a graph, not a chain:

| Phase | Needs |
| --- | --- |
| 1. Local model and the `fs` store | nothing |
| 2. The floor | the partition state-machine facility RFC |
| 3. Tier service and S3 | phase 1, for the trait and the semaphore only |
| 4. End to end | phases 1, 2 and 3 |
| 5. Age roll | the master fix that unlinks rolled `.log` files on WAL truncation |
| 6. The bucket as an archive | phase 4; the GCS and Azure providers need only phase 3 |

Phases 1, 3 and 5 can proceed in parallel. Phase 2 is the only one that waits on another RFC, and
phase 4 is where the branches join. Every phase that changes what is on disk (1, 2 and 5) bumps
`STORAGE_FORMAT`, so an older binary refuses the directory instead of unlinking kept indexes and
accepting a missing tiered prefix as a front trim.

1. **Local model without a network.** The `ObjectStore` trait, the `fs` provider, the provider
   contract tests and the counting semaphore. On top of them the segment location and commit-path
   hash, the chain file with its boot rules and its rebuild from the store, eviction driven by a
   test-set floor, remote sources in the poll plan and the chunk cache, all against `fs`.
2. **Partition state machine and the floor.** Starts once the facility lands. The floor command,
   its apply, its snapshot and state-transfer inclusion, and the per-replica hash check.
3. **Tier service and S3 provider.** The per-shard tier service with piece-wise tasks and class
   caps, the S3 provider, conditional create, the boot probe, retries, credentials, the `sim`
   backend, configuration, metrics, and a pump tick latency metric, the server's first latency
   distribution. Its defaults are provisional until phase 4 measures them.
4. **End to end.** The upload loop and the duties of a new primary, eviction staged on floor apply,
   the poll error channel and `TieredReadFailed` in every SDK, topic options and the store
   identity, the retention split, primary object deletion and the orphan sweep, the chain file in
   state transfer, and the free-space reserve. An upload backlog first exists here, so this phase
   also measures uploads, cold reads, eviction and deletion together on one shard against the pump
   tick latency, which settles the tuning defaults.
5. **Age roll.** `segment_max_age` as a general topic option, the roll rule in materialization and
   the WAL, and the WAL format change. Independent of tiering, so it can land first.
6. **The bucket as an archive.** Manifests under `meta/`, shallow restore, the verify pass, and the
   GCS and Azure providers. Anything that watches manifests detects change by ETag: one LIST of a
   `meta/` prefix, then a conditional GET.

Tests per phase: crash-point and boot tests against `fs` and the simulator's storage harness for
phases 1 and 5, simulator scenarios for phase 2, provider contract tests against `fs` and MinIO for
phase 3, and three-node integration scenarios under `--features vsr` for phase 4.

## Drawbacks

[drawbacks]: #drawbacks

- One command per uploaded segment in the partition's control log, and a dependency on a partition
  state-machine facility and a control log that master has only in part. At the default 1 GiB
  segment size the rate is negligible. With a short `segment_max_age` on many slow partitions it
  could become chatty.
- A new crate with its own HTTP client, signers and provider parsers is code the project has to own
  and keep correct against three providers' quirks, where a vendor SDK would have absorbed them.
- Tier work shares the shard's thread with producers and consumers, and compio has no scheduling
  groups to cap its CPU share. Piece-wise work with explicit yields and per-class caps bound how
  long the pump waits, but a shard with many cold readers and a large upload backlog gives tiering
  more of its CPU than a shares-based scheduler would.
- The state-transfer manifest, boot recovery, quarantine, converge and the install sweep all learn
  about a new file type, and each of those paths has crash-consistency rules to extend.
- Cold reads add a latency mode clients have not seen before. A poll can take tens to hundreds of
  milliseconds and can reply with a transient error, or with the new `TieredReadFailed`, which every
  SDK has to learn.
- Enabling tiering on a topic is a one-way door until rehydration is built, even for a topic that
  never uploaded anything.
- Every replica of a tiered topic hashes every committed byte once more on its pump.
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
  envelope's edge, lowering a local retention option turns much of the envelope into an upload
  burst. With uploads at seal it turns into eviction, which is local and fast, and changing local
  retention never creates network work.
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

Deterministic keys plus local markers carry the per-segment information for a live cluster, so the
state machine holds only the floor and its snapshot stays a few bytes wherever it is persisted. The
cost is that a fresh replica learns its remote segments from a peer through state transfer rather
than from the bucket, and restore from the bucket needs the lazily uploaded manifest. Both are
accepted.

### Why a control log separate from the data log

Putting the floor command into the partition's data group would work: segments would stay pure,
because the flush writes only message batches, and no offset translation would arise, because
message offsets are independent of op numbers. What it would do is interleave control commands
into the data journal and the data group's pipeline, which is harmless for one command per gigabyte
and harmful for the high-rate state machines the facility exists to host, such as per-message
delivery state. A separate control log keeps the data path for user messages only, at the price of
cross-log ordering. The floor tolerates it because its command carries everything its apply checks
(the incarnation, the first offset it covers and the hash), so apply never reads the data log, and
eviction clamps the floor to each replica's local sealed chain. This departs from master's own
precedent: the consumer-group and history fences and session retirement went into the data log,
with an apply hook beside the message path.

Between the two realizations, the metadata plane exists today and already holds
`deleted_up_to_offset`, and master already has the submit path from the owner shard and the
reconciler pass a floor there would need. Such a floor still lags the data by a reconcile interval
and shares one cluster-wide group with everything else. A control group per shard avoids the shared
group and the reconcile lag and scales with shards, though the partition primary still forwards its
command to the group's primary, which may be on another node, and the group's membership must not
depend on each node's shard count. The floor's rate makes the choice immaterial for this RFC, so it
is deferred to the facility RFC rather than decided here.

### Why an Iggy-owned object store abstraction

OpenDAL's S3 and GCS paths are tokio-bound through reqwest, and every vendor SDK drags a second
runtime into a server that runs none. The team ruled out third-party storage crates. Other systems
made the same choice (see [Prior art](#prior-art)): hand-written signers, their own HTTP client, a
per-shard connection pool and credential refresh in one place. Owning the abstraction also puts
every provider under one contract test suite.

### Why on-shard tasks rather than worker threads

compio has no scheduling groups, and discussion #3312 preferred staying on the shard with
cooperative yielding to keep shared-nothing and core affinity. The repository already has the two
primitives on-shard work needs: detached tasks that return through a completion lane, which is how
disk polls run, and an explicit yield to the reactor, which state transfer already uses for exactly
this kind of long walk. Worker threads would have needed cross-thread channels, a second client per
thread and a cache whose accounting lived off-shard, none of which exists today, while a cache
partitioned by shard needs no coordination at all. The partition I/O lane added in #4311 is not the
home either: it runs one job per partition under a deadline that stops the node, and the shard's
blocking-call offload runs one call at a time. The cost, a soft rather than hard cap on tiering's
CPU share, is recorded under drawbacks and watched through a pump tick latency metric.

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

- **Redpanda tiered storage.** Leader-only uploads gated on the term, a Raft-replicated archival
  state machine with a lazily uploaded manifest, a local-deletion fence at the last uploaded offset,
  a persistent chunk cache, its own HTTP client and signers, scheduling groups for CPU fairness, and
  a scrubber that records anomalies without repairing them. This RFC copies the upload loop, the
  cache admission and persistence, the retry model and the probing rules. It diverges on what the
  state machine holds (one offset, not the manifest), on conditional PUTs (possible here because
  bytes are replica-identical), on stitching one poll across remote and local segments, and on
  explicit yields plus per-class caps instead of scheduling groups.
- **Apache Kafka KIP-405** tiered storage and **Apache Pulsar** ledger offload: the same shape as
  this RFC, whole sealed units copied to per-partition objects with an index object beside them.
- **Rivet's SQLite storage engine** (July 2026): a replicated hot tier with S3 never on the write
  path and a cold tier that doubles as the backup, with immutable keys, leases and watermarks kept
  in the metadata store, and two-phase deletion with a grace period. Borrowed: the grace period and
  the checksum comparison in the verify pass. Its later broker replacement supplies the ETag polling
  used for manifests here, and its listing cost, growing with the square of the node count, is why
  LIST stays off every hot path in this RFC.
- **KIP-1150 Diskless Topics** (accepted 2026-03-02) and Aiven's Inkless. KIP-1165 converts diskless
  data into ordinary KIP-405 segments through a per-partition leader, the strongest external signal
  that per-partition segment objects are the long-term format and that tiering should come first.
- **WarpStream, Confluent Freight, AutoMQ, Bufstream, StreamNative Ursa.** Object-storage-first
  streaming. Borrowed: treating API cost as a design input, LIST only for orphan reconciliation,
  cold readers isolated from the cache (Inkless), adaptive readahead (AutoMQ). Rejected: the
  leaderless coordinator, shared multi-partition write objects, asynchronous commits.
- **turbopuffer and SlateDB.** Object-store-native with conditional writes as the fencing primitive.
  SlateDB's measurement that large aligned cache parts cause p99 spikes motivated the small first
  fetch for cold polls.
- **Iggy discussions [#3312](https://github.com/apache/iggy/discussions/3312) and
  [#3232](https://github.com/apache/iggy/discussions/3232).** #3312 is the pre-RFC thread for this
  document. It converged on native compio access rather than OpenDAL, a local index, and
  infrastructure before format work, its participants prefer on-shard uploads with yielding, and
  hubcio prefers shipping sealed-segment compression first. #3232's spike against real S3 with
  `rusty-s3` and `cyper` established the transport this RFC assumes.

## Unresolved questions

[unresolved-questions]: #unresolved-questions

Open design decisions:

- **The partition state-machine facility.** The control log's realization, command encoding, apply
  hook, snapshot and state-transfer inclusion belong to their own RFC, not yet drafted, and phase 2
  waits on it. Mixed-version clusters cannot form, so no replica meets a command it does not know.
- **Two watermarks, possibly two logs.** If the control log becomes per-shard groups, the consistent
  end state moves `TruncatePartition`, with `DeleteSegments` resolution, there as a follow-up.
- **Compression ordering.** Per-flush-unit compression at seal is compatible with ranged GETs and
  whole-object compression is not. If compression lands first, tiering uploads segments unchanged.
- **Cache warmth after a primary move.** Warming the target during the drain, or accepting a burst
  of GETs after each move.
- **Whether the floor requires the `.index` upload or only the `.log`.** Requiring both blocks
  eviction on a transient index failure for data that is already safe.
- **The idle-partition roll and an updatable age.** Both need a roll ordered with the batches, a
  data-log operation. Whether either is worth one is open.
- **Self-healing from local copies.** Re-uploading a missing object from a backup's local copy is
  cheap because segments are byte-identical, where Redpanda only records the anomaly.
- **Encryption at rest for unencrypted streams.** Bucket-side encryption headers, or plaintext
  objects documented as such. Encrypted streams already yield ciphertext objects.
- **The segment checksum in the object key.** It turns a byte-identity violation into two objects
  instead of a precondition failure, at the price of finding an object by offset alone.
- **A `segment_size` floor for tiered topics** above the catalog's 1 MiB, settled by measurement.
- **Eager WAL checkpoints.** A lagging checkpoint holds local data past the envelope, since eviction
  stops at the checkpoint-cursor segment. Whether eviction should request one is open.
- **Placement and store identity.** Whether the rebalance should avoid giving primacy to a node that
  cannot read the store identity, rather than letting that node refuse the partition.
- **Indexes in the cache.** Whether a remote segment's `.index` should move into the chunk cache, so
  the files the install backup links stop growing with total retention, at the price of an index
  fetch on a cold poll.

Out of scope for this RFC, addressable later independently of it: S3-direct storage modes (not
planned, and Redpanda's cloud topics would be the shape), restore and mount from the manifest, whose
markers must name the source cluster's prefix, read-replica topics that follow another cluster's
manifests by ETag, adjacent-segment merging, compression at rest and log compaction,
protocol-aware recovery from a tiered object, stats wire fields, CLI and Web UI visibility,
rehydration, advisory local retention once a disk-space manager exists, and direct I/O.

Simulator versus integration coverage: the deterministic simulator covers the protocol (the floor
across view changes, restarts and slot reuse, the eviction rules, the chain file in state transfer,
the orphan sweep, the `sim` store with injected faults), and its storage harness covers the chain
file's crash points and boot rules and the two roll sites agreeing. Simulator partitions have no
directory, so byte-level reads, the chunk cache and cold-read latency need integration tests against
`fs` and MinIO, and the three-node scenarios need the `--features vsr` cluster harness. Real
provider quirks and cloud credential chains rely on the contract tests and manual verification.
