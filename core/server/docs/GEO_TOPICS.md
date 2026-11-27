# Geo topics

A geo topic is a topic that several datacenters write at the same time, without any replication
traffic between them. Every datacenter runs its own Iggy cluster and its own object store bucket.
Each cluster writes the messages it accepts to its own bucket, and reads the buckets of the other
datacenters. Nothing is copied from one cluster to another.

This page is for operators. It covers the model, the setup of two clusters, the clients, the
delays, what fails and what is lost, and the limits of this version. The reference for each
part of the configuration is in the [server README](../README.md): [tiered
storage](../README.md#tiered-storage), [geo peers](../README.md#geo-peers), [geo topic
objects](../README.md#geo-topic-objects), [shadow streams](../README.md#shadow-streams),
[following peer manifests](../README.md#following-peer-manifests) and [consumer group
positions](../README.md#consumer-group-positions).

> [!IMPORTANT]
> **Status in this version.** A cluster writes its geo topics to its bucket, publishes them, and
> adopts the geo topics of its peers as shadow topics. A shadow topic follows the manifests that
> its origin publishes, and serves the published messages at the origin's offsets. A message that
> the origin committed and did not publish yet is readable only from the cluster that wrote it.
> Consumer groups carry their positions to the other datacenters through the buckets. See [What
> works in this version](#what-works-in-this-version).

## Contents

- [Words used on this page](#words-used-on-this-page)
- [What a geo topic is](#what-a-geo-topic-is)
- [What works in this version](#what-works-in-this-version)
- [How a message moves](#how-a-message-moves)
- [Prerequisites](#prerequisites)
- [Configure each cluster](#configure-each-cluster)
- [Create a geo topic](#create-a-geo-topic)
  - [Durations of a geo topic](#durations-of-a-geo-topic)
- [Produce and consume](#produce-and-consume)
- [Delays](#delays)
- [What fails and what is lost](#what-fails-and-what-is-lost)
- [Failover and failback](#failover-and-failback)
- [Retire an origin](#retire-an-origin)
- [Recover an origin that lost its disks](#recover-an-origin-that-lost-its-disks)
- [Cost](#cost)
- [Metrics](#metrics)
- [Tunables](#tunables)
- [Limits of this version](#limits-of-this-version)

## Words used on this page

- **Datacenter**: a site that runs one Iggy cluster and one bucket.
- **Bucket**: where a cluster keeps its objects. It is the `[tiered_storage]` store of the
  cluster: an S3 bucket and key prefix, or a directory and prefix for the `fs` backend.
- **Origin**: the name of a cluster among the datacenters, `[geo] origin`. The origin of a
  message is the cluster that accepted it.
- **Peer**: another cluster whose bucket this cluster reads, one `[[geo.peers]]` table.
- **Log**: the messages of one partition, with their offsets. Each cluster writes only its own
  log of a geo topic.
- **Shadow stream** and **shadow topic**: how a cluster shows the geo topics of a peer. The
  shadow stream of the peer `dc-a` and its stream `orders` is `__shadow_dc-a_orders`.
- **Committed**: the message is stored and its cluster acknowledged it to the producer.
- **Published**: the message is in a long-term segment that the partition's manifest lists.
  Other datacenters read only published messages.
- **Proven**: a proof row in a stored fresh object binds the committed message to its offset. A
  cluster rebuilt after it lost its disks takes the proven messages back exactly.

## What a geo topic is

A geo topic is a topic type, `topic_type = geo`, fixed at creation. A normal topic never becomes
a geo topic, and a geo topic never becomes a normal topic.

Take two datacenters, `dc-a` and `dc-b`, with the stream `orders` and its geo topic `payments`
in each cluster:

- A producer connected to `dc-a` sends to `orders/payments` in `dc-a`. Cluster `dc-a` appends the
  messages to its own log and stores them in its own bucket.
- A producer connected to `dc-b` sends to `orders/payments` in `dc-b`. Cluster `dc-b` appends them
  to its own log in its own bucket.
- Each cluster reads the bucket of the other, and adopts the other's geo topic as a shadow topic.
  Cluster `dc-b` holds `__shadow_dc-a_orders/payments`, which is built to serve the log of `dc-a`,
  and cluster `dc-a` holds `__shadow_dc-b_orders/payments` for the log of `dc-b`.
- A consumer connects to one cluster. It reads that cluster's own topic, and reads the log of
  each other datacenter from the shadow topic in the same cluster.

The two logs are separate. Each one has its own offsets, assigned only by its origin. Offset 0
in the log of `dc-a` and offset 0 in the log of `dc-b` are two unrelated messages.

### The guarantee

Geo topics are built to give this guarantee: every message acknowledged in one datacenter
becomes readable in every other, exactly once, in the order its origin wrote it.

A message is stored once, in the log of the cluster that accepted it, and is never written back.
That is why a message cannot loop between datacenters or appear twice through replication. The
origin publishes its log in its bucket, and each peer serves that log from the bucket through a
shadow topic. A message becomes readable in another datacenter once its origin published it.

### What a geo topic is not

- It is not one merged log. A consumer that wants every message reads one log per datacenter.
- It has no order between datacenters. Messages written in `dc-a` keep the order `dc-a` gave them,
  and messages written in `dc-b` keep theirs. Nothing orders a message of `dc-a` against a message
  of `dc-b`.
- The same key written in two datacenters has no defined order. If `dc-a` and `dc-b` both write
  key `42`, no reader can tell which write came last.
- It is not a standby. No cluster is promoted, and no cluster ever writes into the log of another.
- It shares only geo topics and the positions of their consumers. Users, permissions, normal
  topics and the consumer groups themselves stay in the cluster that holds them. Create them in
  each cluster.

## What works in this version

| Part                                                                          | In this version |
| ----------------------------------------------------------------------------- | --------------- |
| Create a geo topic in mode `geo`                                              | Yes             |
| Produce to a geo topic: each send is stored in the bucket before it commits   | Yes             |
| Poll a geo topic in the cluster that wrote it                                 | Yes             |
| Long-term segments, the partition manifest, merges, retention and deletes     | Yes             |
| Publish an object for each geo topic in the bucket, and a tombstone on delete | Yes             |
| Read and verify the bucket of each peer                                       | Yes             |
| Adopt the geo topics of each peer as shadow streams                           | Yes             |
| A shadow topic serves the published messages of its origin                    | Yes             |
| A command that retires an origin that never returns                           | Yes             |
| Recover the geo topics of a cluster that lost every disk, from its bucket     | Yes             |
| Consumer positions that move between datacenters                              | Yes             |
| Positions of plain consumers, and of Kafka groups through the Kafka gateway   | Yes             |
| The `geo_tiered` mode, and a switch between modes                             | No              |
| Geo topics in encrypted streams                                               | No              |
| Create, produce to or read geo topics through the Kafka gateway               | No              |

## How a message moves

1. A producer sends to a partition of a geo topic. The shard that leads the partition adds the
   messages to its open upload window, which takes the sends of every geo partition of that
   shard.
2. The shard stores the window as one **fresh object** in its bucket, under
   `<prefix>/meta/fresh/`. A window closes when it holds 4 MiB, or when a send finds a free upload
   slot and `fresh_upload_interval` has passed since the last upload started. The object starts
   with an index that names the partition of each batch. It also carries a **proof row** for each
   batch that committed since the shard's last object: where its messages sit, and the offset and
   timestamp it committed at. So the bucket alone says which messages committed and where, which
   is what a cluster rebuilt after it lost its disks reads. Rows that no window takes within
   250 ms go out in an object of their own.
3. Once the object is stored, the partition commits a placed batch through its consensus: the
   batch header and where its messages sit in the fresh object, 320 bytes in all. The replicas
   store this record on their disks. The messages themselves are only in the bucket.
4. The producer is acknowledged when the placed batch commits. The message is **committed**.
   Polls in this cluster read it at once. The shard that stored the object holds it in memory
   until the long-term segment below takes its messages, even with `read_cache_size` at 0, and
   other polls read the bucket through the shard's read cache, `[tiered_storage] read_cache_size`,
   within the topic's own `read_cache_size` when it sets one. When the cache needs the room for
   newer objects first, it lets an object go before that, and polls of its messages read them from
   the bucket.
5. In the background, the partition's primary rewrites committed messages into **long-term
   segments** in the bucket, at most 64 MiB each, and commits each one to the partition's
   long-term log. It then stores the partition's **manifest**,
   `<prefix>/meta/<stream>/<topic>/<partition>/<created_revision>/manifest.bin`, which lists the
   long-term segments. The message is **published**.
6. Each node of another datacenter that leads a shadow partition of this topic lists the
   manifests of the stream every `[geo] manifest_poll_interval`, reads each one that changed, and
   commits it to the shadow partition. Its shadow partitions then serve the messages that the
   manifest lists, from the long-term segments. The manifest is the only thing another datacenter
   reads of a partition: it never reads the fresh objects.
7. Once every message of a fresh object is in long-term segments, no record names the object
   any more. A sweep that runs every minute deletes it 10 minutes after it first finds it so, once
   the long-term segments also hold every batch its proof rows name.

The rest of the long-term work runs on the same primary: small segments are merged, the topic's
retention trims the oldest segments, and the objects that the log no longer lists are deleted
about an hour later.

## Prerequisites

- **One cluster per datacenter.** Each cluster has its own `[cluster]` configuration. Every node
  of one cluster gets the same `[tiered_storage]` and `[geo]` sections.
- **One bucket per datacenter.** Use the `s3` backend for S3 or a store with its API, such as
  MinIO, Ceph RGW or R2, and the `gcs` backend for Google Cloud Storage. Do not point the `s3`
  backend at the S3 interoperability endpoint of GCS: GCS honors a create-only put only as its
  own precondition, and a node refuses a store that ignores create-only puts. The `fs` backend
  takes a directory, which suits tests on one host. The
  `mock_s3` backend keeps the objects in the memory of the server with the latency of
  `[tiered_storage.mock_s3]`, for benchmarks only: a restart loses every message of its geo topics,
  and no peer can read it. Create the bucket before the first start: a missing bucket stops the
  boot.
- **The bucket must not share fate with the cluster that writes to it.** A geo topic is exactly
  as durable as its bucket: the messages are only there, and the replicas hold only where they
  sit. Use a store that outlives the site, such as a regional cloud object store. The server
  cannot check this rule.
- **Credentials.** Each cluster needs read and write access to its own bucket: get, put, list
  and delete. On S3 that is `s3:GetObject`, `s3:PutObject`, `s3:DeleteObject` and
  `s3:ListBucket`. Each cluster needs only read access to the bucket of each peer: `s3:GetObject`
  and `s3:ListBucket`. On GCS, the role Storage Object User on the own bucket and Storage Object
  Viewer on each peer's bucket. The server never writes to a peer's bucket.
- **On GCS, one write per second per object.** GCS throttles a second write to one object name
  within a second. A partition stores its manifest at most once a second, within that limit. Each
  node rewrites one positions object per log, so set `[geo] consumer_offset_upload_interval` to
  `1 s` or more, or to `disabled`, on a `gcs` backend.
- **No lifecycle rule that expires objects under the prefix.** The cluster deletes what it no
  longer needs. An object that a rule removes early is lost data.
- **The segment cleaner stays on.** `[data_maintenance.messages] cleaner_enabled = true` is the
  default. The cleaner starts the first rewrite of each partition after a node starts, and runs
  retention and the deletes of long-term objects. With it off, a geo topic publishes nothing and
  its fresh objects are never deleted.
- **No server-wide stream encryption.** With `[encryption] enabled = true`, every new stream gets
  a key, and a geo topic cannot be created in a stream with a key.

## Configure each cluster

Each cluster needs three things: its origin name, its own bucket, and the bucket of each peer.

### Origin

`[geo] origin` names the cluster. It holds 1 to 32 bytes of lowercase letters, digits and `-`,
starts with a letter, and does not end with `-`. Shadow stream names carry it, so pick it once:
this version has no procedure to rename an origin.

A cluster needs an origin for its own geo topics, not only for peers. Without an origin, the
cluster accepts a geo topic but publishes no object for it, and the topic's partitions refuse
every send with `TransientNotAccepted` (58).

### Own bucket

`[tiered_storage]` names the bucket of this cluster. The [tiered storage
section](../README.md#tiered-storage) of the server README describes each key, the credential
sources of the `s3` backend, and the store identity object `<prefix>/meta/identity`, with which
replica 0 claims the prefix for the cluster. The cluster accepts a geo topic only while every
node passes that check. Until then a create returns `FeatureUnavailable` (5).

Replica 0 writes the claim only while it is the caught-up metadata primary. If another replica
leads metadata when the cluster first starts, nothing claims the prefix, and geo topics are
refused. The README says how to move the lead back to replica 0.

### Peers

Each `[[geo.peers]]` table names one peer: its `origin`, which must equal that cluster's own
`[geo] origin`, and the keys of that cluster's `[tiered_storage]` section. A cluster reads at most
16 peers. Two peers cannot name the same bucket and prefix.

An `s3` peer reads with the first credentials that the node has, in this order:

1. The static keys `access_key_id` and `secret_access_key` of the peer's table. Set both or
   neither. In the environment they are `IGGY_GEO_PEERS_<n>_ACCESS_KEY_ID` and
   `IGGY_GEO_PEERS_<n>_SECRET_ACCESS_KEY`, where `<n>` is the peer's place in the list, from 0.
   A variable for one key of a peer replaces the whole list of peers from the file, so a peer
   whose keys come from the environment takes every key from there, as the example below
   shows.
2. Web identity, as for the own bucket: `AWS_WEB_IDENTITY_TOKEN_FILE` and `AWS_ROLE_ARN`. EKS sets
   both for a pod with a service account role.
3. The role of the EC2 instance, from its instance metadata service.

With web identity or an instance role, the node reads the peer's bucket with its own role. That
role must be allowed `s3:GetObject` and `s3:ListBucket` on the peer's bucket. When the peer's
bucket is in another AWS account, the policy of that bucket must also grant both actions to the
role. If the peer's bucket encrypts with a KMS key of its own, the role also needs `kms:Decrypt`
on that key. The node fetches these credentials in the background. Until they arrive, the peer
counts as unreachable. That is also what a node shows that chose a source which hands out
nothing, such as an instance with no role attached or a role that refuses the token: the node
starts, keeps fetching, and logs each failure under the peer's name. Only a node on which no
source can be chosen does not start: no web identity variables, and
`AWS_EC2_METADATA_DISABLED` set to `true`. Peers that take the same role, and the own bucket
when it takes it too, share one fetch.

A `gcs` peer takes a `[geo.peers.gcs]` table with the keys of `[tiered_storage.gcs]`, and gets its
token as the `gcs` backend does: from a service account key file or the metadata server.

A peer that does not answer never stops the boot and never delays local traffic.

### Two-cluster example

Two datacenters: `dc-a` with the bucket `iggy-dc-a` in `eu-central-1`, and `dc-b` with the bucket
`iggy-dc-b` in `us-east-1`, both on AWS S3.

On every node of `dc-a`, in `config.toml`:

```toml
[tiered_storage]
backend = "s3"
prefix = "iggy"
bucket = "iggy-dc-a"
region = "eu-central-1"

[geo]
origin = "dc-a"

[[geo.peers]]
origin = "dc-b"
backend = "s3"
prefix = "iggy"
bucket = "iggy-dc-b"
region = "us-east-1"
```

On every node of `dc-b`:

```toml
[tiered_storage]
backend = "s3"
prefix = "iggy"
bucket = "iggy-dc-b"
region = "us-east-1"

[geo]
origin = "dc-b"

[[geo.peers]]
origin = "dc-a"
backend = "s3"
prefix = "iggy"
bucket = "iggy-dc-a"
region = "eu-central-1"
```

The credentials go in the environment of each node. On a node of `dc-a`:

```sh
# Read and write keys for iggy-dc-a. Leave them out to use web identity or the
# instance role instead.
export IGGY_TIERED_STORAGE_ACCESS_KEY_ID="..."
export IGGY_TIERED_STORAGE_SECRET_ACCESS_KEY="..."
```

With web identity or an instance role the peer needs nothing here: its table in the file sets
no keys, and the node's role needs read access to `iggy-dc-b`.

With static keys for the peer, give the whole peer in the environment and leave its
`[[geo.peers]]` table out of the file. A variable for one key of a peer replaces the list of
peers from the file, so the two keys alone would leave a peer with no origin and the node
would not start:

```sh
# The first peer, iggy-dc-b, with read-only keys.
export IGGY_GEO_PEERS_0_ORIGIN="dc-b"
export IGGY_GEO_PEERS_0_BACKEND="s3"
export IGGY_GEO_PEERS_0_BUCKET="iggy-dc-b"
export IGGY_GEO_PEERS_0_REGION="us-east-1"
export IGGY_GEO_PEERS_0_ACCESS_KEY_ID="..."
export IGGY_GEO_PEERS_0_SECRET_ACCESS_KEY="..."
```

The other settings work from the environment too, for example `IGGY_GEO_ORIGIN=dc-a`.

For a store other than AWS, add its URL as `endpoint`, for example
`endpoint = "https://minio.dc-b.example.com"`. For an `fs` peer, set `backend = "fs"` and `path`
to the directory that holds the peer's objects.

A datacenter on Google Cloud uses the `gcs` backend. If `dc-b` keeps its bucket on GCS, its nodes
take:

```toml
[tiered_storage]
backend = "gcs"
prefix = "iggy"

[tiered_storage.gcs]
bucket = "iggy-dc-b"

[geo]
origin = "dc-b"
consumer_offset_upload_interval = "1 s"
```

and the nodes of `dc-a` read it with:

```toml
[[geo.peers]]
origin = "dc-b"
backend = "gcs"
prefix = "iggy"

[geo.peers.gcs]
bucket = "iggy-dc-b"
service_account_key = "/etc/iggy/dc-b-reader.json"
```

On GCE or GKE the nodes need no key file: the metadata server hands out the token of the
instance's service account, or of the pod's with Workload Identity.

### Check the setup

Start both clusters, in any order. Then check on every node:

1. `object_store_identity_verified` is 1. The node can write to its own bucket.
2. `geo_peer_identity_verified{origin="dc-b"}` is 1 on the nodes of `dc-a`, and
   `geo_peer_identity_verified{origin="dc-a"}` is 1 on the nodes of `dc-b`. The node reads the
   peer's bucket.

The gauges are on the HTTP endpoint `/metrics`, which takes a bearer token, such as a personal
access token:

```sh
curl -s -H "Authorization: Bearer $IGGY_TOKEN" http://dc-a-node-1:3000/metrics \
  | grep -E '^(object_store_identity_verified|geo_peer_identity_verified|geo_peer_unreachable)'
```

The node logs the reason when a check does not pass. The README lists the causes: a prefix
claimed by another cluster, metadata that was rebuilt, or a peer entry that points at the wrong
bucket.

## Create a geo topic

Create the stream in each cluster first. Then create the geo topic in each cluster where
producers write. Adoption creates only the read-only shadow topic in the other clusters, so a
cluster that takes writes needs its own geo topic, created there.

Choose the partition count and the retention at creation. A geo topic keeps its name, its
partition count and its options for good, all but its durations: see
[Durations of a geo topic](#durations-of-a-geo-topic).

With the CLI, against a node of `dc-a`:

```sh
export IGGY_USERNAME=iggy IGGY_PASSWORD="..."
iggy --tcp-server-address dc-a-node-1:8090 stream create orders
iggy --tcp-server-address dc-a-node-1:8090 topic create orders payments 3 none 7days \
  --max-topic-size 500GB --set topic_type=geo
```

Over HTTP:

```http
POST /streams/orders/topics
Content-Type: application/json
Authorization: Bearer ...

{
  "name": "payments",
  "partitions_count": 3,
  "options": {
    "topic_type": "geo",
    "message_expiry": "7 days",
    "max_topic_size": "500 GB"
  }
}
```

With the Rust SDK:

```rust
use iggy::prelude::*;

let topic = client
    .create_topic(
        &Identifier::named("orders")?,
        "payments",
        &TopicCreateOptions {
            partitions_count: Some(3),
            topic_type: Some(TopicType::Geo),
            ..TopicCreateOptions::default()
        },
    )
    .await?;
```

The other SDKs set the same option keys, `topic_type` and `geo_mode`. `iggy options topic` lists
every topic option the server takes.

`geo_mode` defaults to `geo` on a geo topic, the only mode clients can pick in this version. The
server stores `partition_resize_policy` as `fixed` on a geo topic.

A geo topic in the `geo` mode has no durability choice: it is always `persisted`. The server
ignores the `durability` option on create, which the CLI and every SDK send as `replicated`
unless told otherwise, and `GetTopic` reports `persisted` as derived. A send to a geo topic
already waits for its upload to the bucket. With `persisted` it also waits until the record that
places its batch is on disk on a quorum of the cluster's nodes. Without that record no
datacenter can read the batch. On GCP, a power loss of every node of a cluster lost 70 of 10,500
acknowledged messages with `replicated` and none with `persisted`. The flush cost about 4 ms on
a 65 ms send. The `geo_tiered` mode, not available in this version, is the one meant for a
faster path.

### What a create refuses

| Request                                                              | Answer                        |
| -------------------------------------------------------------------- | ----------------------------- |
| `topic_type=geo` while a node does not pass the store identity check | `FeatureUnavailable` (5)      |
| `geo_mode=geo_tiered`                                                | `FeatureUnavailable` (5)      |
| A geo topic in a stream that has an encryption key                   | `FeatureUnavailable` (5)      |
| `geo_mode` on a normal topic                                         | `InvalidOptionValue` (4042)   |
| A duration of a geo topic on a normal topic                          | `InvalidOptionValue` (4042)   |
| `tiered_storage=true` on a geo topic                                 | `InvalidOptionValue` (4042)   |
| `partition_resize_policy` other than `fixed` on a geo topic          | `InvalidOptionValue` (4042)   |
| Any topic create in a shadow stream                                  | `ShadowStreamReadOnly` (1021) |

After the create, the topic refuses changes, all but a change of its durations and of its
`read_cache_size` and `read_ahead`:

| Request on a geo topic                                                      | Answer                           |
| --------------------------------------------------------------------------- | -------------------------------- |
| An update of its name or of any option but a duration or a read cache key   | `OptionNotUpdatable` (4103)      |
| A reset of a duration or of a read cache key                                | `OptionNotResettable` (4104)     |
| Adding or deleting partitions                                               | `PartitionResizeDisabled` (2023) |

A `FeatureUnavailable` from a cluster that is not ready is worth retrying while its nodes pass
their store identity checks. `object_store_identity_verified` shows when each node does.

### Durations of a geo topic

A geo topic can set its own value for six server settings. Each one overrides the setting of the
same name for this topic, on every node of the cluster. For a duration that the topic does not
set, each node applies its own setting.

| Topic option                      | Overrides                                | Bounds                         |
| --------------------------------- | ---------------------------------------- | ------------------------------ |
| `fresh_upload_interval`           | `[tiered_storage] fresh_upload_interval` | `0` to `10 s`                  |
| `consumer_offset_upload_interval` | `[geo] consumer_offset_upload_interval`  | `0` or `disabled`, up to `1 s` |
| `positions_publish_interval`      | `[geo] positions_publish_interval`       | longer than `0`                |
| `tombstone_grace`                 | `[geo] tombstone_grace`                  | longer than `0`                |
| `positions_resume_timeout`        | `[geo] positions_resume_timeout`         | `0` or longer                  |
| `long_term_rewrite_max_interval`  | `[geo] long_term_rewrite_max_interval`   | at least `1 s`                 |

A value is a number of microseconds or a duration like `50 ms`. The meaning of each one is in
[Tunables](#tunables).

- Set them in the create of the topic, or change them later with an update of the topic. No node
  needs a restart. A send takes a new upload interval at once, and a stored offset takes a new
  offset upload interval at once. A rewrite takes a new ceiling at its next pass. A new publish
  interval applies from the next round of every log, within the node's
  `positions_publish_interval`. A new resume timeout applies at the next start of a node.
- An update cannot reset a duration to the node's setting. To follow the node again, set the
  node's value.
- A normal topic refuses them. A shadow topic has none: its cluster applies its own settings,
  because the durations of a topic do not go to other datacenters.
- The topic's object records its `tombstone_grace`, so the grace still applies after the delete,
  when the topic is gone from metadata.

One fresh object carries the sends of every partition that a shard leads, of all its topics. So
the `fresh_upload_interval` of a topic is the longest time that its sends wait for an upload to
start, not a pace of their own. A shard uploads at the shortest interval among the sends that it
holds. When it holds sends of a topic with a short interval, the sends of a topic with a long
interval go with those uploads. They are answered sooner than their interval asks, and save no
requests on that shard.

With the CLI:

```sh
iggy topic create orders payments 3 none 7days --set topic_type=geo \
  --set fresh_upload_interval=50ms --set tombstone_grace=6h
iggy topic update orders payments --set consumer_offset_upload_interval=200ms
```

`iggy topic get` shows the durations that the topic sets. Every SDK has typed fields for them in
its topic create options, its topic update and its topic read model.

A geo topic can also set `read_cache_size` and `read_ahead`, as any topic can. They override the
`[tiered_storage]` settings of the same name for the reads of the topic that go to the bucket:
long-term segments, and fresh objects that the shard does not hold. `read_cache_size` is shared
evenly between the topic's partitions, and each share is a limit within the read cache of the
shard, not a reservation. `0` keeps nothing that those reads fetch, and turns readahead off. The
fresh objects that a shard uploaded stay in memory for the polls at the tail whatever the topic
sets. An update can change both and cannot reset them. See [tiered
storage](../README.md#tiered-storage) in the server README.

The topic's object carries both keys, so each shadow topic of the topic in another datacenter
reads with the same values: it takes them at its adoption, and follows each update. A key that
the geo topic does not set is unset on the shadow topic too, and the nodes that hold the shadow
apply their own settings. The values are kept as the origin set them, also when they do not fit
the nodes of the other cluster: a share of `read_cache_size` below 8 blocks of a node's
`read_cache_block_size` keeps nothing on that node, which logs it once for each such partition,
and a partition reads ahead at most an eighth of its share. A client cannot set them on a shadow
topic. An update typically reaches the shadow topics within about 35 seconds, and later while
earlier shadow operations are queued, see [Delays](#delays). After a recovery of the origin, a
shadow topic follows the successor's updates. While the origin's bucket does not answer, and after
its origin retired, a shadow topic keeps the last values it took.

### What happens after a create

1. The metadata primary stores the topic's object,
   `<prefix>/meta/topics/<stream_id>/<topic_id>/<created_revision>`, within about a second.
2. The primary of each partition reads that object back before it takes messages. Until then the
   partition refuses sends with `TransientNotAccepted` (58), which the SDKs retry, and logs one
   warning.
3. The metadata primary of each peer lists the topic objects of this cluster every 5 seconds. It
   adopts the new topic into the shadow stream `__shadow_<origin>_<stream>`, with the same topic
   id, topic name and partition ids, and the `read_cache_size` and `read_ahead` that the topic
   sets.

## Produce and consume

### Producers

A producer writes to the geo topic of the cluster it is connected to. The server has no notion
of a producer's home datacenter: a message belongs to the log of the cluster that accepted it.

The producer is acknowledged after its messages are stored in the bucket and the placed batch
commits. When the bucket does not answer, a send is answered with `TransientNotAccepted` (58),
which the SDKs retry, after at most 3 attempts at the upload within 10 seconds. A shard also
answers 58 while it holds `fresh_pending_bytes` (32 MiB by default) of sends that wait for the
bucket or for their partition, or `fresh_partition_pending_bytes` (8 MiB by default) of the sends
of one partition. [Tunables](#tunables) says when to raise them. When the messages a partition
committed and no long-term segment holds yet pass `long_term_backlog_limit`, its sends are slowed
instead of refused: each waits 10 ms to 250 ms before the shard takes it.

A send to a shadow topic returns `ShadowStreamReadOnly` (1021). No SDK retries it, since a
shadow topic never takes a send.

### Consumers

A consumer connects to the cluster of its own datacenter, and reads:

- the geo topic, for the messages written in this datacenter, for example `orders/payments`;
- the shadow topic of each peer, for the messages written there, for example
  `__shadow_dc-b_orders/payments` in `dc-a`.

A client recognizes a shadow stream by the read-only key `shadow_origin`, which names the origin.
A stream read reports it, alone or in a listing, and no other stream carries it. Every SDK has a
typed getter for it, and for the `topic_type` and `geo_mode` of a topic. A shadow topic reads as a
geo topic with the `geo_mode` of its origin, and in the `geo` mode as `persisted`, although this
cluster stores it as a normal topic and runs none of a geo topic's write path for it. The CLI
shows the origin in `iggy stream list`, and the type, the mode and the origin in `iggy topic get`.

A shadow topic is read like any other topic, by stream name and topic name or id. It has the
topic id and the partition ids of its origin's topic, and it is built to serve the origin's
offsets.

A shadow topic serves the messages that its origin published in long-term segments, and reads
their bytes from the origin's bucket. A message that the origin committed and did not publish yet
is not in the shadow topic: see [Delays](#delays). Every node of the cluster serves the same
messages of a shadow partition, also after a restart. A consumer whose stored position is ahead
of what the shadow partition serves waits until it gets there; its position is never moved back.

### Consumer groups and positions

- The topic and each shadow topic are separate topics, so a consumer group keeps one position per
  log in each cluster.
- A group is known across datacenters by its name on the topic. The group `billing` of
  `orders/payments` in `dc-a` and the group `billing` of `__shadow_dc-a_orders/payments` in
  `dc-b` read the same log, and share one position on each of its partitions. So do the group
  `billing` of `__shadow_dc-b_orders/payments` in `dc-a` and the group `billing` of
  `orders/payments` in `dc-b`.
- A group that stores its offset itself, with `StoreConsumerOffset`, gets the answer only once
  the node's bucket holds the position it stored. So an acknowledged store survives a datacenter
  that fails right after it. The SDKs' manual commits send this request, and so do their
  client-side automatic commits. The stores of one log that arrive while a put of its positions
  runs, or within `[geo] consumer_offset_upload_interval` (50 ms by default) of the start of the
  last one, share the next put.
- A poll that commits on read (`auto_commit`, `CommitPolicy::OnRead` in the Rust SDK) is
  committed by the server and answered at once. Its offset reaches the bucket with the next
  store of the node's positions, at most `[geo] positions_publish_interval` (1 s by default)
  later. A failover can therefore read again what such a group consumed in that time.
- Each cluster takes the positions that the other clusters store in their buckets when they are
  higher, within `[geo] positions_poll_interval` (1 s by default) of their store. A group that
  moves to another datacenter resumes from where it stopped: see [Failover and
  failback](#failover-and-failback).
- The node that leads a partition stores its positions, as it uploads its messages. Groups
  commit at the leader, and the other replicas hold the same positions. A node that stops
  leading a partition keeps its last positions of it in its object, and the new leader stores
  the partition at its first round. The other clusters take the highest position across the
  nodes, so the older ones do not win.
- A position is a reset counter and an offset. Of two positions the one with the higher reset
  wins, and within one reset the higher offset. A group that stores an offset below the one it
  had, a seek backwards, or deletes its offset, counts one reset up, so the other datacenters
  take the move instead of undoing it.
- A position from another datacenter never moves a group that has members in this cluster. The
  consumers that read here decide its position here, and the other datacenters take it once the
  group stops there.
- A position ahead of what this cluster serves is kept. The group's polls wait there until the
  messages arrive, and the position is never moved back.
- A group created in this cluster after a position for its name arrived starts from that
  position. Its first polls are answered with `TransientNotAccepted` (58), which the SDKs retry,
  until the position is bound to it.
- Deleting a group in one cluster stores a tombstone for its name, which keeps its old position
  from coming back from the other clusters. The other clusters keep their groups of that name.
- Membership, generations and partition assignments do not move. Consumers join and rebalance in
  the cluster they connect to.
- Grant read permissions on each shadow stream as on any other stream.

### Plain consumers and Kafka groups

A plain consumer, which reads without a group, carries its positions between datacenters too. So
does a Kafka group that reads through the Kafka gateway. The rules of a group apply, with these
differences:

- A plain consumer is known by its id: the number its client sets, or the hash of its name. The
  same consumer has the same id in every cluster.
- The Kafka gateway keeps the offsets of the Kafka group `billing` as the external offsets of the
  Iggy group `kafka.cg.billing` on the topic. They are known by that name. They are apart from the
  offsets that the Iggy group stores as itself.
- A store of either waits for the bucket, as a group's `StoreConsumerOffset` does. So the gateway
  answers a Kafka `OffsetCommit` once the node's bucket holds the offset. An acknowledged commit
  survives a datacenter that fails right after it.
- The gateway creates the Iggy group at the Kafka group's first commit to a topic. In the other
  datacenter, the gateway's first `OffsetFetch` comes before that commit, so it reads the
  offsets by the group's name.
- After a start, the polls of plain consumers and the gateway's offset reads on geo and shadow
  topics wait for the peers' positions, as group polls do, with `TransientNotAccepted` (58). The
  gateway answers such an `OffsetFetch` with `COORDINATOR_LOAD_IN_PROGRESS` (14), which Kafka
  clients retry.
- With the coordinator log of several gateways, Iggy answers `OffsetFetch` on the partitions of
  geo and shadow topics, because only Iggy holds what another datacenter moved. The log adds its
  commit metadata where it holds the same offset.
- Neither has members in Iggy. So a position from another datacenter moves it at any time, also
  while it reads here. Read a log with one plain consumer or one Kafka group in one datacenter at
  a time. Two datacenters that read one log under one name at once undo each other's moves.

### Duplicates

A send that was not acknowledged can still have committed. A producer that retries it in another
datacenter can write it twice, once in each log. Consumers that must not process it twice
deduplicate on an id of their own, such as the message id that the producer sets.

## Delays

| From                    | To                                   | How long                                                                                                                                                                                                                                                             |
| ----------------------- | ------------------------------------ | -------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| Send                    | Committed and acknowledged           | One upload of the fresh object and one commit. A busy shard adds up to `fresh_upload_interval`, 10 ms by default, or the wait for a free upload slot. A partition past `long_term_backlog_limit` adds 10 ms to 250 ms.                                               |
| Committed               | Readable in the origin cluster       | At once.                                                                                                                                                                                                                                                             |
| Committed               | Published                            | The partition's next rewrite, every 1 s to `[geo] long_term_rewrite_max_interval`, 10 s by default, plus the upload and the commit of the segment. The manifest is stored after the commit, at most once a second: the commits within a second share the next store. |
| Published               | Readable in another datacenter       | Up to `[geo] manifest_poll_interval`, 5 s by default, until the peer lists the manifest, then one read of the manifest and one commit in the peer.                                                                                                                   |
| Committed               | Readable in another datacenter       | The two rows above added up, seconds: the next rewrite and manifest, then the peer's next listing. Nothing else brings a message there.                                                                                                                              |
| Geo topic created       | Shadow topic in each peer            | About a second to store the topic object, then up to 5 s until a peer lists it.                                                                                                                                                                                      |
| Read cache updated      | Shadow topic reads with it           | Typically about a second to store the topic object again, then up to 30 s until a peer reads it again and up to 5 s until its next pass, then one commit in the peer. Earlier shadow operations queued in the peer can delay it by more passes.                      |
| Group offset store      | Acknowledged, position in the bucket | One commit of the store, one commit of the fold and one put of the node's object of the log. A put that runs, or one that started less than `[geo] consumer_offset_upload_interval` (50 ms by default) before, delays it to the next put.                            |
| Poll with `auto_commit` | Position stored in the bucket        | Up to `[geo] positions_publish_interval`, 1 s by default, then a commit of the fold and the upload of the object.                                                                                                                                                    |
| Position stored         | Taken by another datacenter          | Up to `[geo] positions_poll_interval`, 1 s by default, then one read of the object and one commit in that cluster.                                                                                                                                                   |
| Geo topic deleted       | Shadow topic detached in each peer   | Up to a minute until the origin writes the tombstone, then up to 30 s until a peer reads it again.                                                                                                                                                                   |
| Shadow topic detached   | Shadow topic removed                 | `[geo] tombstone_grace` of the cluster that holds the shadow, 24 hours by default, counted from the tombstone's write.                                                                                                                                               |

A geo topic that sets its own duration replaces the node's setting in these rows, see
[Durations of a geo topic](#durations-of-a-geo-topic). The detach of a shadow topic always uses the
setting of the cluster that holds the shadow.

Each partition adjusts its rewrite interval between 1 s and `[geo] long_term_rewrite_max_interval`,
10 s by default, toward the interval at which a long-term segment comes out 80% of 64 MiB full. A
segment cut at 64 MiB starts the next rewrite as soon as it commits, so a partition that commits
more than 64 MiB a second rewrites back to back. Its manifest is still stored at most once a second,
with what the long-term log holds then. A change that came within the second is stored once the
second passed, also when nothing follows it. A long-term log that did not change is not stored
again, but for one repeat 2 minutes after a new primary's first store. After a node starts, its
partitions rewrite nothing until the first pass of the segment cleaner, one
`[data_maintenance.messages] interval` after the start, 1 minute by default.

`partition_long_term_rewrite_lag_messages` shows how far publication trails commits on the
origin. It is the most committed messages that one partition of the shard holds and no long-term
segment holds yet.

The manifest is the only source of a shadow partition: it never serves an offset past the end of
the last long-term segment that the manifest it committed lists. The slowest step is the rewrite
interval of a partition with little load. Such a partition waits the longest interval, so a shadow
of it is about `[geo] long_term_rewrite_max_interval` plus `[geo] manifest_poll_interval` behind.
A lower `long_term_rewrite_max_interval` brings messages to the shadow topics sooner. It also leaves
more and smaller long-term segments on quiet partitions: more merge work and more manifest stores,
still at most one store a second per partition. At `1 s` a partition rewrites every second.

In a test with two single-node clusters on one host and an `fs` bucket, with one send every
50 ms for a minute, a consumer of the shadow topic read each message this long after the origin
acknowledged it. At that load each partition rewrites every 10 s, the default longest interval.

| `manifest_poll_interval` | Median | 95th percentile | Longest |
| ------------------------ | ------ | --------------- | ------- |
| `5 s`, the default       | 8.9 s  | 13.4 s          | 14.0 s  |
| `1 s`                    | 6.0 s  | 10.4 s          | 11.0 s  |

## What fails and what is lost

| Event                                                   | Effect                                                                                                                                                                                                                                                                                                                           |
| ------------------------------------------------------- | -------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| One node of a cluster fails                             | The partitions elect another primary as for any topic. Nothing is lost.                                                                                                                                                                                                                                                          |
| A cluster's bucket does not answer                      | Its geo topics refuse sends with 58. Polls that need bytes from the bucket get 58, which the SDKs retry. A group's own offset store on a geo or shadow topic commits, and is answered with `TransientNotCommitted` (57) after 10 s, which the SDKs send again. Rewrites wait. Peers mark the shadow topics of that origin stale. |
| A cluster fails and returns, its bucket stays           | On restart the cluster replays its consensus log, and the rewrite publishes the committed messages it had not published. A geo topic in the `geo` mode is always `persisted`, so it keeps every acknowledged message: see [above](#create-a-geo-topic).                                                                          |
| A cluster is destroyed with its disks, its bucket stays | A cluster rebuilt on empty disks takes back its geo topics when an operator asks for it: see [Recover an origin that lost its disks](#recover-an-origin-that-lost-its-disks). The published messages come back, and so do the committed ones that self-describing fresh objects prove. The other committed messages are lost.    |
| A cluster is destroyed and never returns                | Its shadow topics in the other clusters stop growing. Retire its origin in each of them: see [Retire an origin](#retire-an-origin). Its shadow topics then expire by their own retention.                                                                                                                                        |
| A bucket is lost                                        | Every message of the geo topics of that cluster is lost, committed or published. This is the reason for the deployment rule.                                                                                                                                                                                                     |
| A peer's bucket does not answer                         | This cluster keeps the last manifest that each shadow partition committed, removes no shadow topic except those whose grace ended, and sets `geo_shadow_topics_stale` for that origin. Polls of a shadow topic that need bytes from that bucket get 58, which the SDKs retry. Local traffic is not affected.                     |

What a destroyed cluster loses for certain is what it committed and had not yet proven in a
stored fresh object. `fresh_proof_lag_seconds` shows how far behind the proofs are. A recovery
that adopts the unproven tail brings most of the rest back.

A cluster rebuilt on empty disks gets a new cluster id. Its store identity check then
refuses the old bucket prefix, and its geo topics are refused with `FeatureUnavailable`.
Give it another prefix, empty the old one once nothing needs it, or recover the geo
topics of the lost cluster from it, as the next sections describe.

## Failover and failback

A datacenter that takes over takes over the traffic, not the log. Nothing is promoted and
nothing is restored. The steps below assume `dc-a` fails and `dc-b` stays up, and that both
clusters already have the geo topic `orders/payments`.

### Failover

1. Point the producers of `dc-a` at `dc-b`. They write to `orders/payments` in `dc-b`, which
   appends to the log of `dc-b`. No setting changes in `dc-b`.
2. Let the producers retry in `dc-b` every send that `dc-a` did not acknowledge. Such a send may
   exist in both logs. See [Duplicates](#duplicates).
3. Start the consumers of `dc-a` in `dc-b`, at least `positions_poll_interval` (1 s by default)
   after `dc-a` failed. A group that read `orders/payments` in `dc-a` reads
   `__shadow_dc-a_orders/payments` in `dc-b`, and a group that read `__shadow_dc-b_orders/payments`
   in `dc-a` reads `orders/payments` in `dc-b`. Create the groups in `dc-b` with the same names,
   before or at the failover. Each group resumes from the position that `dc-a` last stored in its
   bucket and `dc-b` took, and skips nothing:
   - a group that stores its offsets itself resumes on the message after its last acknowledged
     store, and reads nothing again that it acknowledged;
   - a group that commits on read reads again at most what it consumed in the last
     `positions_publish_interval` plus `positions_poll_interval` before `dc-a` failed.

   `dc-b` takes the positions of `dc-a` only for a group with no members in `dc-b`. A group whose
   consumers start sooner keeps the position `dc-b` took before them, and reads again what came
   after it. Plain consumers and Kafka groups move the same way, by their ids and names. Start
   them in `dc-b` only after they stopped in `dc-a`, see
   [Plain consumers and Kafka groups](#plain-consumers-and-kafka-groups).
4. Consumers in `dc-b` read the messages that `dc-a` published or proved before it failed from
   the shadow topic `__shadow_dc-a_orders/payments`. The messages that `dc-a` acknowledged and did
   not publish stay in the log of `dc-a` until it returns. A group whose position is past what the
   shadow topic serves waits there.

### Planned failover

To move a group with no message read twice:

1. Stop the group's consumers in `dc-a`. A group that stores its offsets itself has every
   acknowledged store in the bucket of `dc-a` already. A group that commits on read has its last
   commit there within `positions_publish_interval`, 1 s by default.
2. Wait until `dc-b` shows the positions, `positions_poll_interval` later:
   `GET /streams/__shadow_dc-a_orders/topics/payments/consumer-offsets?consumer_kind=consumer_group&consumer_id=billing&partition_id=0`
   on a node of `dc-b`, or the SDK's `get_consumer_offset`.
3. Start the consumers in `dc-b`. Each group resumes on the next message.

### Failback

1. Start `dc-a`. It replays its consensus log, and its rewrite publishes the committed messages
   it had not published yet. There is nothing to reconcile: `dc-b` never wrote into the log of
   `dc-a`.
2. Move the producers back to `dc-a`, or leave them in `dc-b`. Either way, each message goes to
   the log of the cluster that accepts it.
3. Move the consumers back to `dc-a`. After it starts, `dc-a` answers the group polls of geo and
   shadow topics with `TransientNotAccepted` (58) until it took the positions of every peer, or
   until `positions_resume_timeout`, 10 s by default, passed. So each group continues from where
   it got to in `dc-b`, and reads the messages that `dc-a` acknowledged before it failed and
   never published once it reaches them. They read the messages written in `dc-b` during the
   outage from the shadow topic `__shadow_dc-b_orders/payments` in `dc-a`.

## Retire an origin

If the datacenter of an origin never returns, retire the origin in each surviving cluster:

```bash
iggy geo retire dc-a
```

Over HTTP, send `POST /geo/origins/dc-a/retire`. The command needs root or the global
`manage_servers` permission. The cluster commits the retirement through its metadata, so every
node keeps it across restarts. A retirement of an origin that is retired already changes nothing.

After the command:

- The nodes send the origin's bucket no request but the reads of polls, and after each start
  one read of its identity object, which the polls wait for. A node serves the polls only when
  that object names a cluster that the retirement recorded, or a successor that recovery records
  prove. They list none of its topic objects, manifests or consumer group positions.
  `geo_peer_retired` is 1 for that origin.
- After a node starts, its group polls wait for the positions of the other peers only.
- The shadow topics of the origin stop growing. A poll reads the messages that the last
  committed manifest lists, from the bucket.
- Each shadow partition drops its oldest segments by the retention of its topic in the origin:
  `message_expiry`, and `max_topic_size` shared between the partitions. The last segment stays.
  The segment cleaner runs this, so `[data_maintenance.messages] cleaner_enabled = false` stops
  it.
- A shadow topic is removed once its `message_expiry` passed since the retirement, and a shadow
  stream once it holds no topic. A topic whose messages never expire stays.
- A user with the permission to delete streams or topics can delete the shadow streams and
  shadow topics of the origin. For any other origin, these deletes stay refused.

A retirement cannot be undone, and it names the origin. A node never reads a peer entry with that
origin name again, whatever its bucket holds. A datacenter that comes back needs another
`[geo] origin`, and the other clusters need a peer entry for that name.

Keep the `[[geo.peers]]` entry of the retired origin while consumers read its shadow topics,
because a node reads the bucket through that entry. Remove the entry after that. Iggy never
writes to or deletes from the bucket of a peer, so cleaning the retired origin's bucket is the job
of the operator. Clean it only after its shadow topics are gone.

## Recover an origin that lost its disks

When every node of a cluster lost its disk and its bucket is intact, a cluster rebuilt on
empty disks can take back the geo topics of the lost one from the bucket. It runs only when
the operator names the lost cluster: without that, a rebuilt cluster refuses the bucket, as
[What fails and what is lost](#what-fails-and-what-is-lost) says.

### What comes back

For each partition of each geo topic that the lost cluster held:

- **The published messages**, from the long-term segments that the partition's manifest
  lists, up to its floor F.
- **The proven messages.** Each fresh object of the self-describing format carries proof
  rows, which bind the batches that committed to their offsets and timestamps. The batches
  that proofs name from F on without a gap come back at exactly those offsets and
  timestamps, up to P.
- **The adopted messages**, unless `recover_unproven = false`. A fresh object also names
  the offsets its writer expected each of its batches to take. The batches that no proof
  names and that follow P without a gap come back at those offsets, up to the cutover Q.
  Of two batches that expect one offset, the one written in the higher consensus view wins,
  then the one of the later writer, then the later object of that writer. A batch that
  loses one offset is dropped whole, and the first offset that no batch expects ends the
  adoption. An adopted batch takes the timestamp of the batch before it plus one, or the
  time its object was written if that is later.

The proven messages come back exactly once. The adopted ones are at least once: one can be
a send that the lost cluster refused and the producer sent again, and a committed batch can
be missing when another batch took its offsets. Turn adoption off for at most once. Two
proofs that bind one offset to different batches end P below that offset, and nothing is
adopted for that partition.

Fresh objects of the format before (`IGGYFRS1`) say neither whose batches they hold nor
where those committed, so their messages come back only when they were published. The
recovery reports each such object.

A restored topic takes its partitions, compression, retention, its own `tombstone_grace`, the
only one of its durations that the object records, and its `read_cache_size` and `read_ahead`
from its topic object. It is in the `geo` mode, so it is `persisted`.

Nothing else comes back from the bucket: consumer offsets, consumer groups, users and
permissions, normal topics, and the idempotent producer state of the published messages are
not in it. The producers of proven and adopted batches are restored, so their retries are
still recognized. The rebuilt cluster takes the positions of the geo topics' consumer groups
from its peers, as a returning cluster does: create the groups again with their names, and each
one starts from the position its peers hold.

### Procedure

1. Stop the lost cluster for good, and revoke its credentials to the bucket. Nothing stops a
   node of it that still runs from writing.
2. Read the lost cluster's `cluster_uid`, the second line of `<prefix>/meta/identity`.
3. Start the rebuilt cluster on empty disks, with the same `[tiered_storage]` section and the
   same `[geo] origin` as the lost one on every node, and on replica 0 also:

   ```toml
   [geo]
   origin = "dc-a"
   recover_cluster_uid = "0123456789abcdef0011223344556677"
   # Optional: take back only what the bucket proves.
   #recover_unproven = false
   ```

   Or `IGGY_GEO_RECOVER_CLUSTER_UID` and `IGGY_GEO_RECOVER_UNPROVEN`. The other nodes need
   neither. Create no stream before the recovery ends: it restores the streams and topics
   under the ids they had, and refuses a cluster that holds other streams.
4. Replica 0 recovers once it is the caught-up metadata primary, as it would claim an empty
   bucket. If another replica leads metadata, restart that replica until replica 0 leads.
   Replica 0 checks the bucket every 5 seconds until the recovery ends, and so does every
   node once the restored topics reach its metadata.
5. The recovery is done when `object_store_identity_verified` is 1 on every node, and replica
   0 logs `the retired cluster's geo topics are recovered and this cluster owns the store`.
   Each restored topic carries a mark in metadata that names the lost cluster, so every node
   that builds one of its partitions, now or later, installs what the recovery took of it
   first, whatever its own configuration says. The partitions then serve their messages and
   take new sends after the cutover.
6. Read the report, `<prefix>/meta/recovery/<cluster_uid>/report.json`.
7. Remove `recover_cluster_uid` from replica 0. It does nothing once the bucket names the
   rebuilt cluster.

A recovery that fails logs the reason and runs again at the next check, 5 seconds later.
Each attempt finds what an earlier one stored and goes on. It refuses a topic of another
`[geo] origin`, a cluster that already holds streams, and a bucket that another cluster
recovered already. It also refuses a bucket with a topic object that this build cannot read,
damaged or in a format it does not know, so that it never recovers only some of the topics.
The log names the object and its format marker, the first 8 bytes, such as `IGGYTOP3`.

### What it writes

Under `<prefix>/meta/recovery/<cluster_uid>/`, before it claims the bucket:

| Object                                            | Content                                                                                                                                                                |
| ------------------------------------------------- | ---------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `<stream>-<topic>-<partition>-<created_revision>` | The plan of one partition: its manifest and the batches taken above it. Every replica installs it into its empty partition, so the replicas start from the same state. |
| `record`                                          | The retirement of the lost cluster: the rebuilt cluster's `cluster_uid` and, per partition, where F, P and Q end. Written once, never changed.                         |
| `report.json`                                     | What each partition took and dropped, for operators.                                                                                                                   |

Then it replaces `<prefix>/meta/identity` with the rebuilt cluster's own. A node of the lost
cluster that comes back finds another cluster there at its next check, within a minute, and
stops writing. A peer that follows the lost cluster's fresh objects takes its batches only
below each partition's cutover in the record: from there on the partition's log is the
rebuilt cluster's. A peer whose shadow streams came from the lost cluster follows the record
from it to the rebuilt cluster, and reads the bucket as that cluster's. It reads no record of a
bucket whose identity object names a cluster that its shadow streams came from. When the lost
cluster is the only one that the peer's metadata records of the origin, those shadow streams
also follow the rebuilt cluster's topic objects: a tombstone detaches its topic, and a new topic
of a shadowed stream gets a shadow topic. A stream that has no shadow stream gets none, and the
peer logs it.

The report lists, per partition:

| Field                               | Meaning                                                         |
| ----------------------------------- | --------------------------------------------------------------- |
| `floor`                             | F, the last published offset. `null` without one.               |
| `proven_through`                    | P, the last offset that proofs name without a gap from F.       |
| `cutover_through`                   | Q, the last offset taken. The next send takes the one after it. |
| `proven_batches`, `adopted_batches` | Batches taken from proofs and from claims.                      |
| `conflict_at`                       | The first offset two different proofs bind, when there is one.  |
| `dropped`                           | Each batch left out, with its object, its offsets and why.      |

The reasons a batch is dropped: `lost_overlap` (a batch of a higher view, writer or object
expected one of its offsets), `past_gap` (an offset before it is not taken),
`overlaps_proven` (it expected offsets that a proven batch holds), `proven_past_gap` (a
proof names it past a gap), `invalid_proof` (its proof does not match the batch),
`proofs_conflict` (two proofs disagree, so nothing is adopted), `adoption_off`, and
`invalid_producer`. `skipped_objects` lists the fresh objects not read, such as
`not_self_describing` ones.

## Cost

A geo topic is designed so that cross-datacenter traffic is paid only for what consumers in one
datacenter read from the shadow topics of another. Message bytes cross between datacenters only
when a poll of a shadow topic reads them from the peer's bucket. The cross-datacenter requests,
per peer:

- each node reads the peer's store identity object once a minute, and every 5 seconds while the
  check does not pass, and a recovery record under the peer's root only when the object names a
  cluster that the shadow streams did not come from;
- each manifest or positions pass that reads an object of the peer reads the identity object
  before and after it: two more gets at most every `[geo] manifest_poll_interval` per shard for
  each 16 changed manifests that it reads, and every `[geo] positions_poll_interval` per node
  while the peer's positions change;
- the metadata primary lists the peer's topic objects every 5 seconds, reads each new object,
  and reads each known object again every 30 seconds;
- every `[geo] positions_poll_interval`, shard 0 of each node that leads a partition of a geo or
  shadow topic lists the peer's consumer group positions, one page per 1,000 objects, and reads
  each object that changed: one per log and node of the peer that leads a partition of the log
  whose groups moved;
- every `[geo] manifest_poll_interval`, shard 0 of each node lists the manifests of each peer
  stream for which the node leads a shadow partition, and the shard that owns the partition reads
  each manifest that changed;
- gets for the segments and indexes that polls of the shadow topics read.

Only the last item grows with what consumers read. The others stand, whether or not anyone reads
the shadow topics, and none of them grows with the messages the peer writes: a node reads none
of the peer's fresh objects, and a busy peer partition costs at most one manifest read per
listing.

In a test where the peer stored about a hundred fresh objects a second for 30 s and no consumer
read the shadow topic, the node sent the peer's bucket six listings of the manifests and three
manifest reads, one for each manifest the peer stored in that time, and nothing for its fresh
objects or segments.

Requests to a cluster's own bucket:

- one put per fresh object: up to one object per `fresh_upload_interval` on each shard, plus one
  for each object that fills to 4 MiB first;
- up to four puts a second on a shard whose sends are more than 250 ms apart, for the proof rows
  that no object with messages carried in that time;
- a get for each run of blocks a poll misses in the shard's read cache, and gets for the rewrites
  and merges, which read the messages back;
- a put for each long-term segment and its index, and a put of the partition's manifest at most
  once a second, only when its long-term log changed. The manifest is one object, about 88 bytes
  per segment it lists, that each put rewrites whole;
- up to one put a `positions_publish_interval` on each node for each log in which a partition
  that the node leads had its consumer groups move;
- while groups store their offsets themselves, up to one put a `consumer_offset_upload_interval`
  on each node for each log in which they store, 20 a second at the default 50 ms. Each put
  rewrites the node's whole object of the log: 18 bytes plus the group's name for each group on
  each partition of the log that the node leads;
- listings of the replica's fresh objects every minute, and the deletes of fresh and long-term
  objects.

Storage: a message stays in its fresh object until at least 10 minutes after every message of
that object is published, and in a long-term segment until retention removes it. Each replica
keeps 320 bytes per batch on its disk.

## Metrics

All metrics are on the HTTP endpoint `/metrics` of each node, with a `shard` label. Counters carry
the `_total` suffix, and histograms the `_bucket`, `_sum` and `_count` suffixes.

### Buckets and peers

| Metric                                  | Meaning                                                                                                                                                                        |
| --------------------------------------- | ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------ |
| `object_store_identity_verified`        | 1 while the node passes the store identity check of its own bucket.                                                                                                            |
| `object_store_unreachable`              | 1 while its own bucket does not answer the check.                                                                                                                              |
| `geo_peer_identity_verified`            | Per `origin`: 1 while the node reads the peer's bucket, whose identity object names a cluster that the shadow streams came from or a proven successor.                         |
| `geo_peer_unreachable`                  | Per `origin`: 1 while the peer's bucket does not answer the check.                                                                                                             |
| `geo_peer_retired`                      | Per `origin`: 1 once this cluster retired the origin. The node then sends its bucket only the reads of polls, and one identity read after a start.                             |
| `geo_shadow_topics_stale`               | Per `origin`: stale shadow topics, counted on the metadata primary.                                                                                                            |
| `geo_peer_manifest_requests_total`      | Per `origin` and `request`, `list` or `get`: requests for the peer's manifests.                                                                                                |
| `geo_peer_manifest_failures_total`      | The same requests that failed, and the manifest reads of a pass that found another claim on the bucket.                                                                        |
| `geo_peer_manifests_refused_total`      | Per `origin`: manifests that this cluster refused.                                                                                                                             |
| `geo_peer_manifest_commands_total`      | Per `origin`: commands that committed a manifest to a shadow partition.                                                                                                        |
| `geo_peer_manifest_listed_seconds`      | Per `origin`: Unix time of the last listing that succeeded, on shard 0.                                                                                                        |
| `geo_shadow_partitions_behind`          | Per `origin`: shadow partitions whose committed manifest is behind the listed one.                                                                                             |
| `geo_shadow_manifest_lag_seconds`       | Per `origin`: the longest time one of them has been behind.                                                                                                                    |
| `geo_peer_position_requests_total`      | Per `origin` and `request`, `list` or `get`: requests for the peer's consumer group positions.                                                                                 |
| `geo_peer_position_failures_total`      | The same requests that failed.                                                                                                                                                 |
| `geo_peer_positions_adopted_total`      | Per `origin`: positions of the peer that the partitions here proposed to take.                                                                                                 |
| `geo_positions_stored_total`            | Objects of consumer group positions that the node stored in its bucket.                                                                                                        |
| `geo_positions_store_failures_total`    | The ones the bucket refused.                                                                                                                                                   |
| `geo_positions_commits_confirmed_total` | Offset stores of consumer groups on geo and shadow topics answered once the bucket held their positions.                                                                       |
| `geo_positions_commits_uncertain_total` | The ones answered with `TransientNotCommitted` (57) instead: their positions were not stored within 10 s, or their partition stopped being led here. The SDKs send them again. |
| `geo_positions_resume_waiting`          | 1 while group polls on the shard wait for the peers' positions after the node started.                                                                                         |

### Write path

| Metric                               | Kind      | Meaning                                                                                                 |
| ------------------------------------ | --------- | ------------------------------------------------------------------------------------------------------- |
| `fresh_uploads`                      | counter   | Fresh objects the shard stored.                                                                         |
| `fresh_upload_bytes`                 | counter   | Bytes of those objects.                                                                                 |
| `fresh_upload_retries`               | counter   | Attempts after an attempt that failed.                                                                  |
| `fresh_upload_failures`              | counter   | Objects the bucket refused. Their sends were answered as not accepted.                                  |
| `fresh_sends_denied`                 | counter   | Sends the shard answered as not accepted, for any reason.                                               |
| `fresh_sends_denied_partition_share` | counter   | Of those, sends past `fresh_partition_pending_bytes`, what the waiting sends of one partition may hold. |
| `fresh_sends_denied_shard_cap`       | counter   | Of those, sends past `fresh_pending_bytes`, what the waiting sends of the shard may hold.               |
| `fresh_uploads_in_flight`            | gauge     | Uploads that hold one of the shard's `fresh_uploads` slots.                                             |
| `fresh_staged_sends`                 | gauge     | Sends whose messages the shard holds and whose record has not reached its partition.                    |
| `fresh_window_wait_seconds`          | histogram | How long an upload window stayed open after its first send.                                             |
| `fresh_upload_seconds`               | histogram | How long the upload of a fresh object took.                                                             |
| `fresh_commit_seconds`               | histogram | How long a send waited for its answer after its fresh object was stored.                                |
| `fresh_object_size_bytes`            | histogram | Size of each fresh object the shard stored, in bytes.                                                   |
| `fresh_proof_lag_seconds`            | gauge     | How long the oldest proof row of a committed batch has waited for a stored object.                      |
| `fresh_confirmations`                | counter   | Fresh objects of proof rows alone, stored when no window closed within 250 ms.                          |

### Fresh objects

The first shard of each node sweeps the fresh objects that this replica wrote.

| Metric                            | Kind    | Meaning                                                                                      |
| --------------------------------- | ------- | -------------------------------------------------------------------------------------------- |
| `fresh_objects`                   | gauge   | Fresh objects of this replica that the last sweep listed.                                    |
| `fresh_object_bytes`              | gauge   | Their bytes.                                                                                 |
| `fresh_object_oldest_age_seconds` | gauge   | Age of the oldest one. It grows when publication stops.                                      |
| `fresh_objects_unnamed`           | gauge   | Objects that no record names, which wait out their 10 minutes.                               |
| `fresh_objects_due`               | gauge   | Objects past their 10 minutes that the last sweep left in the bucket.                        |
| `fresh_objects_proving`           | gauge   | Objects past their 10 minutes kept for proof rows of batches not in long-term segments yet.  |
| `fresh_listings_truncated`        | gauge   | Listings of the last sweep that stopped at their share of keys. `fresh_objects` is then low. |
| `fresh_objects_deleted`           | counter | Fresh objects this replica deleted.                                                          |
| `fresh_sweeps_held`               | gauge   | 1 while the last sweep deleted nothing because a shard could not tell what it reads.         |

A sweep deletes nothing while a placing partition of this node is rebuilt or waits for a state
transfer, since the rebuilt copy still reads those objects. `fresh_sweeps_held` is then 1, and
`fresh_objects` and `fresh_object_oldest_age_seconds` grow until the partition is back. A hold
that lasts longer than the 10 minutes of grace is also logged as a warning.

### Long-term segments

These cover the partitions that the shard leads.

| Metric                                     | Kind    | Meaning                                                                             |
| ------------------------------------------ | ------- | ----------------------------------------------------------------------------------- |
| `partition_long_term_rewrite_lag_messages` | gauge   | Most committed messages of one partition that no long-term segment holds yet.       |
| `partition_long_term_backlog_bytes`        | gauge   | Most bytes of messages one partition committed that no long-term segment holds yet. |
| `topic_partition_long_term_lag_messages`   | gauge   | Per partition this node leads: committed messages no long-term segment holds yet.   |
| `topic_partition_long_term_lag_bytes`      | gauge   | Their bytes, absent until the partition counted them.                               |
| `partition_long_term_delayed_sends`        | counter | Sends held back because their partition's backlog passed `long_term_backlog_limit`. |
| `partition_long_term_segments`             | gauge   | Most segments in the long-term log of one partition, of the 65,536 a log takes.     |
| `partition_long_term_merges`               | counter | Merges of small segments that committed.                                            |
| `partition_long_term_merged_segments`      | counter | Segments that those merges replaced.                                                |
| `partition_long_term_trims`                | counter | Trims that retention sent, one per pass until the trim commits.                     |
| `partition_long_term_dead_objects`         | gauge   | Objects the log no longer lists, which wait to be deleted.                          |
| `partition_long_term_deleted_objects`      | counter | Long-term objects deleted, the log and the index of a segment counted as one.       |
| `partition_long_term_unannounced_objects`  | gauge   | Objects in the bucket that the log does not list and may still list later.          |

The two `topic_partition_long_term_lag_*` series carry the labels `stream`,
`topic` and `partition`, so one slow partition shows by name.

Alert on these first:

- `object_store_identity_verified` at 0: the node cannot write geo topics.
- `fresh_upload_failures` or `fresh_sends_denied` that keep growing: producers are refused.
- `partition_long_term_rewrite_lag_messages` or `fresh_object_oldest_age_seconds` that keep
  growing: publication stopped, and the window a destroyed cluster would lose grows with them.
- `partition_long_term_delayed_sends` that keeps growing: the rewrites fall behind and the
  producers of those partitions are slowed.
- `partition_long_term_segments` near 65,536: the log stops taking segments when it is full.
- `geo_peer_identity_verified` at 0 or `geo_shadow_topics_stale` above 0: a peer is not read.
- `geo_peer_manifests_refused_total` that grows: a peer published a manifest that this cluster
  refused. The shadow partitions keep the last manifest they committed.
- `geo_shadow_manifest_lag_seconds` that keeps growing past a few `manifest_poll_interval`: shadow
  topics fall behind their origin.

## Tunables

| Key                                              | Default  | Meaning                                                                                                                                                                                                                                                                                                                                                                                                                                   |
| ------------------------------------------------ | -------- | ----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `[tiered_storage] fresh_upload_interval`         | `10 ms`  | The shortest time between the starts of two uploads of fresh objects on one shard, unless an object fills to 4 MiB first. `0` uploads as soon as a slot is free. Up to `10 s`. A longer interval makes fewer and larger objects, and makes every producer wait longer. A geo topic can set its own.                                                                                                                                       |
| `[tiered_storage] long_term_backlog_limit`       | `1 GiB`  | The bytes of messages one partition may have committed that no long-term segment holds yet. Past it, the primary holds each send to that partition for 10 ms, up to 250 ms at twice the limit, and never refuses one for it. Its next rewrite then waits only the shortest interval, 1 s after its last. `0` turns this off. Otherwise at least `64 MiB`.                                                                                 |
| `[tiered_storage] fresh_pending_bytes`           | `32 MiB` | The most bytes of messages one shard holds for sends to geo topics that have not reached their partitions: in the open window, in fresh objects that upload or wait for a slot, and in stored objects whose partition has no room yet. A send past it is answered with 58 and sent again by the client. A send larger than it still gets in while nothing waits on the shard. At least `4 MiB`, one fresh object.                         |
| `[tiered_storage] fresh_partition_pending_bytes` | `8 MiB`  | The most of `fresh_pending_bytes` that the waiting sends of one partition hold. A send past it is answered with 58 the same way. A send larger than it still gets in while nothing of its partition waits. At least `4 MiB`, and at most `fresh_pending_bytes`.                                                                                                                                                                           |
| `[tiered_storage.jobs] fresh_uploads`            | `16`     | Uploads of fresh objects that run at once on one shard. They have slots of their own and never wait behind segment uploads. A shard keeps to `fresh_upload_interval` only while the slots cover the time of a put: 16 slots cover puts of up to 160 ms at the default 10 ms. What the uploads hold counts in `fresh_pending_bytes`, so more slots take no more memory.                                                                    |
| `[geo] positions_publish_interval`               | `1 s`    | How often a node stores the consumer group positions of the partitions it leads in its bucket, when they changed. A group that commits on read and fails over reads again at most this long, plus the poll interval of the other datacenter. A geo topic can set its own.                                                                                                                                                                 |
| `[geo] consumer_offset_upload_interval`          | `50 ms`  | The shortest time between two puts of a log's positions for the offsets that groups store themselves, whose answers wait for the put. A store that finds no put running and the interval passed is stored at once. A longer interval makes fewer puts under steady stores, and answers them later. `disabled` answers them at the local commit and leaves them to `positions_publish_interval`. Up to `1 s`. A geo topic can set its own. |
| `[geo] positions_poll_interval`                  | `1 s`    | How often a node lists the positions that each peer stores, and takes those that win. A shorter interval lists the peer's bucket more often. The node lists at most once a pass of its partition reconciler, `[sharding] reconcile_periodic_interval`.                                                                                                                                                                                    |
| `[geo] positions_resume_timeout`                 | `10 s`   | How long, after a node starts, the group polls of geo and shadow topics wait for the positions of every peer that did not retire. A peer that does not answer delays them this long. A geo topic can set its own.                                                                                                                                                                                                                         |
| `[geo] manifest_poll_interval`                   | `5 s`    | How often shard 0 of each node lists the manifests of each peer stream for which the node leads a shadow partition. A shorter interval brings published messages to shadow topics sooner, and lists the peer's bucket more often.                                                                                                                                                                                                         |
| `[geo] long_term_rewrite_max_interval`           | `10 s`   | The longest wait between two rewrites of a geo partition into long-term segments, which a partition with little load waits. Its shadow topics are about this plus `manifest_poll_interval` behind. A lower value publishes sooner, and leaves more and smaller segments on quiet partitions: more merges and manifest stores, still at most one a second. At least `1 s`, which rewrites every second. A geo topic can set its own.       |
| `[geo] recover_cluster_uid`                      | empty    | On replica 0: the `cluster_uid` of a cluster that lost every disk, whose geo topics this cluster takes back from its bucket. See [Recover an origin that lost its disks](#recover-an-origin-that-lost-its-disks).                                                                                                                                                                                                                         |
| `[geo] recover_unproven`                         | `true`   | Whether that recovery also takes the batches no proof names, at the offsets their writer expected. `false` takes back only what the bucket proves.                                                                                                                                                                                                                                                                                        |
| `[geo] tombstone_grace`                          | `24 h`   | How long a deleted geo topic keeps its tombstone and its objects in this cluster's bucket, and how long this cluster keeps a detached shadow topic. Make it longer than any peer outage. A geo topic can set its own.                                                                                                                                                                                                                     |

The keys that a geo topic can set for itself are in
[Durations of a geo topic](#durations-of-a-geo-topic), with `read_cache_size` and `read_ahead`.

The two bounds of waiting sends protect the memory of a node when the bucket or a partition stops
answering. A node holds at most `fresh_pending_bytes` times its shards for them. The share of one
partition keeps a partition that stops answering from taking the room of the others on its shard.
A send past a bound is refused for a moment, and the SDKs send it again. Raise both when producers
send batches of several MiB to the same partition at once. With batches of 10 KB to 10 MB from 32
producers on 64 partitions, 17 % of sends were refused once at the defaults.
`fresh_sends_denied_partition_share` and `fresh_sends_denied_shard_cap` count these refusals.

The other `[tiered_storage]` keys apply to geo topics as they do to tiered topics: the timeouts
and retries of store calls, and the job caps of `[tiered_storage.jobs]`. Each shard runs one tier
service per peer with the same caps, so a slow peer takes no slot from the cluster's own bucket.

These values are fixed in this version: 4 MiB per fresh object before it closes early, 3 attempts
and 10 seconds per fresh object, 64 MiB per long-term segment, 10 minutes before an unnamed fresh
object is deleted, and 1 hour before an unlisted long-term object is deleted.

## Limits of this version

- **Shadow topics serve only published messages.** A message reaches the shadow topics of the
  peers after its origin's next rewrite and manifest and the peer's next listing, seconds after
  its commit, not at its commit.
- **Only offset stores wait for the bucket.** A poll that commits on read and a delete of an
  offset are answered at their commit. A node whose store identity check of its own
  bucket does not pass publishes no positions, and answers the stores at their commit. A store on
  a detached shadow topic is answered at its commit too: its origin deleted the topic, so no node
  publishes or takes its positions.
- **A position reaches another datacenter one poll after its store.** A group started there
  sooner reads from the position that datacenter took before.
- **A store of exactly the offset a group had when another datacenter's position arrived is not
  seen as a change.** The group keeps reading from the other datacenter's position.
- **Deleted topics leave a node's positions object behind** when the node was down at the
  delete. A node deletes its object of a log its cluster no longer holds while it runs.
- **No encrypted streams.** A geo topic in a stream with an encryption key is refused with
  `FeatureUnavailable` (5), because its sends reach the bucket before the stream's encryption
  runs.
- **The Kafka gateway carries only offsets.** It carries the offsets of Kafka groups between
  datacenters. It does not support geo topics or shadow streams for anything else.
- **No member check for plain consumers and Kafka groups.** A position from another datacenter
  moves them also while they read here. Read a log with one of them in one datacenter at a time.
- **Frozen partition count and options.** A geo topic keeps its name, its options, including
  `message_expiry` and `max_topic_size`, and its partition count. Only its durations can change.
  In the `geo` mode it is always `persisted`.
- **No reset of a duration.** An update cannot return a duration of a geo topic to the node's
  setting. Set the node's value instead.
- **Only the `geo` mode.** `geo_tiered` is refused, and a geo topic cannot change its mode.
- **Each cluster's geo topic is created there.** Adoption creates only shadow topics. Nothing
  checks that the geo topics of the same name in two clusters have the same partition count or
  options.
- **A recreated topic waits for its old shadow.** When a geo topic is deleted and created again
  with the same name, a peer adopts the new one only after it removes the old shadow topic, which
  takes `tombstone_grace`.
- **A cluster with geo topics needs an origin.** Without `[geo] origin`, a geo topic takes no
  messages.
- **Up to 16 peers.** A cluster reads at most 16 peers.
- **Geo topics do not lock the store identity claim.** Once a topic is tiered, replica 0 never
  claims the prefix again. A geo topic does not have that effect: replica 0 keeps claiming again
  after any check that did not pass. With the `fs` backend, keep the mount of the bucket directory
  reliable, or a dropped mount is claimed as an empty bucket.
- **Retention works on whole long-term segments.** Retention removes the oldest long-term segments
  that expired or exceed the size budget, never the last one, and never past the lowest offset
  that a consumer or consumer group of the origin cluster committed on the partition. The size
  budget of one partition is at least 64 MiB.
- **Deleting segments keeps the bucket.** A client's delete of segments on a geo topic does not
  remove long-term segments. Only retention and a delete of the topic do.
- **65,536 long-term segments per partition.** Merges keep the count down. A full log takes no new
  segment until merges or retention free room.
- **No consumer that reads a topic with its shadow topics.** A consumer reads the topic and each
  shadow topic separately. Stream reads mark a shadow stream with `shadow_origin`.
