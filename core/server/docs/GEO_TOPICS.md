# Geo topics

A geo topic is a topic that several datacenters write at the same time, without any replication
traffic between them. Every datacenter runs its own Iggy cluster and its own object store bucket.
Each cluster writes the messages it accepts to its own bucket, and reads the buckets of the other
datacenters. Nothing is copied from one cluster to another.

This page is for operators. It covers the model, the setup of two clusters, the clients, the
delays, what fails and what is lost, and the limits of this version. The reference for each
part of the configuration is in the [server README](../README.md): [tiered
storage](../README.md#tiered-storage), [geo peers](../README.md#geo-peers), [geo topic
objects](../README.md#geo-topic-objects), [shadow streams](../README.md#shadow-streams) and
[following peer manifests](../README.md#following-peer-manifests).

> [!IMPORTANT]
> **Status in this version.** A cluster writes its geo topics to its bucket, publishes them, and
> adopts the geo topics of its peers as shadow topics. A shadow topic follows the manifests that
> its origin publishes, and serves the published messages at the origin's offsets. A message that
> the origin committed and did not publish yet is readable only from the cluster that wrote it.
> See [What works in this version](#what-works-in-this-version).

## Contents

- [Words used on this page](#words-used-on-this-page)
- [What a geo topic is](#what-a-geo-topic-is)
- [What works in this version](#what-works-in-this-version)
- [How a message moves](#how-a-message-moves)
- [Prerequisites](#prerequisites)
- [Configure each cluster](#configure-each-cluster)
- [Create a geo topic](#create-a-geo-topic)
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
- **Published**: the message is in a long-term segment that the partition's manifest lists. Only
  published messages are meant to be read by other datacenters.

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
- It shares only geo topics. Users, permissions, normal topics and consumer groups stay in the
  cluster that holds them. Create them in each cluster.

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
| A command that retires an origin that never returns                           | No              |
| Recover the geo topics of a cluster that lost every disk, from its bucket     | Yes             |
| Consumer positions that move between datacenters                              | No              |
| The `geo_tiered` mode, and a switch between modes                             | No              |
| Geo topics in encrypted streams                                               | No              |
| Geo topics through the Kafka gateway                                          | No              |

## How a message moves

1. A producer sends to a partition of a geo topic. The shard that leads the partition adds the
   messages to its open upload window, which takes the sends of every geo partition of that
   shard.
2. The shard stores the window as one **fresh object** in its bucket, under
   `<prefix>/meta/fresh/`. A window closes when it holds 4 MiB, or when a send finds a free upload
   slot and `fresh_upload_interval` has passed since the last upload started. The object starts
   with an index that names the partition of each batch. It also carries a **proof row** for each
   batch that committed since the shard's last object: where its messages sit, and the offset and
   timestamp it committed at. So the bucket alone says which messages committed and where. Rows
   that no window takes within 250 ms go out in an object of their own.
3. Once the object is stored, the partition commits a placed batch through its consensus: the
   batch header and where its messages sit in the fresh object, 320 bytes in all. The replicas
   store this record on their disks. The messages themselves are only in the bucket.
4. The producer is acknowledged when the placed batch commits. The message is **committed**.
   Polls in this cluster read it at once, from the bucket, through a cache of 64 MiB per shard.
5. In the background, the partition's primary rewrites committed messages into **long-term
   segments** in the bucket, at most 64 MiB each, and commits each one to the partition's
   long-term log. It then stores the partition's **manifest**,
   `<prefix>/meta/<stream>/<topic>/<partition>/<created_revision>/manifest.bin`, which lists the
   long-term segments. The message is **published**.
6. Once every message of a fresh object is in long-term segments, no record names the object
   any more. A sweep that runs every minute deletes it 10 minutes after it first finds it so, once
   the long-term segments also hold every batch its proof rows name.

The rest of the long-term work runs on the same primary: small segments are merged, the topic's
retention trims the oldest segments, and the objects that the log no longer lists are deleted
about an hour later.

## Prerequisites

- **One cluster per datacenter.** Each cluster has its own `[cluster]` configuration. Every node
  of one cluster gets the same `[tiered_storage]` and `[geo]` sections.
- **One bucket per datacenter.** Use the `s3` backend for S3 or a store with its API, such as
  MinIO, Ceph RGW or R2. The `fs` backend takes a directory, which suits tests on one host.
  Create the bucket before the first start: a missing bucket stops the boot.
- **The bucket must not share fate with the cluster that writes to it.** A geo topic is exactly
  as durable as its bucket: the messages are only there, and the replicas hold only where they
  sit. Use a store that outlives the site, such as a regional cloud object store. The server
  cannot check this rule.
- **Credentials.** Each cluster needs read and write access to its own bucket: get, put, list
  and delete. On S3 that is `s3:GetObject`, `s3:PutObject`, `s3:DeleteObject` and
  `s3:ListBucket`. Each cluster needs only read access to the bucket of each peer: `s3:GetObject`
  and `s3:ListBucket`. The server never writes to a peer's bucket.
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
16 peers. Two peers cannot name the same bucket and prefix. An `s3` peer takes only static keys:
web identity and instance roles do not apply to peers. Set the keys in the environment, as
`IGGY_GEO_PEERS_<n>_ACCESS_KEY_ID` and `IGGY_GEO_PEERS_<n>_SECRET_ACCESS_KEY`, where `<n>` is
the peer's place in the list, from 0.

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
# Read-only keys for iggy-dc-b, the first peer.
export IGGY_GEO_PEERS_0_ACCESS_KEY_ID="..."
export IGGY_GEO_PEERS_0_SECRET_ACCESS_KEY="..."
```

The same settings work from the environment alone, for example `IGGY_GEO_ORIGIN=dc-a`,
`IGGY_GEO_PEERS_0_ORIGIN=dc-b`, `IGGY_GEO_PEERS_0_BACKEND=s3`, `IGGY_GEO_PEERS_0_BUCKET=iggy-dc-b`
and `IGGY_GEO_PEERS_0_REGION=us-east-1`.

For a store other than AWS, add its URL as `endpoint`, for example
`endpoint = "https://minio.dc-b.example.com"`. For an `fs` peer, set `backend = "fs"` and `path`
to the directory that holds the peer's objects.

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

Choose the partition count and the retention at creation. A geo topic keeps its name, its options
and its partition count for good.

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

### What a create refuses

| Request                                                              | Answer                        |
| -------------------------------------------------------------------- | ----------------------------- |
| `topic_type=geo` while a node does not pass the store identity check | `FeatureUnavailable` (5)      |
| `geo_mode=geo_tiered`                                                | `FeatureUnavailable` (5)      |
| A geo topic in a stream that has an encryption key                   | `FeatureUnavailable` (5)      |
| `geo_mode` on a normal topic                                         | `InvalidOptionValue` (4042)   |
| `tiered_storage=true` on a geo topic                                 | `InvalidOptionValue` (4042)   |
| `partition_resize_policy` other than `fixed` on a geo topic          | `InvalidOptionValue` (4042)   |
| Any topic create in a shadow stream                                  | `ShadowStreamReadOnly` (1021) |

After the create, the topic refuses changes:

| Request on a geo topic                 | Answer                           |
| -------------------------------------- | -------------------------------- |
| An update of its name or of any option | `OptionNotUpdatable` (4103)      |
| Adding or deleting partitions          | `PartitionResizeDisabled` (2023) |

A `FeatureUnavailable` from a cluster that is not ready is worth retrying while its nodes pass
their store identity checks. `object_store_identity_verified` shows when each node does.

### What happens after a create

1. The metadata primary stores the topic's object,
   `<prefix>/meta/topics/<stream_id>/<topic_id>/<created_revision>`, within about a second.
2. The primary of each partition reads that object back before it takes messages. Until then the
   partition refuses sends with `TransientNotAccepted` (58), which the SDKs retry, and logs one
   warning.
3. The metadata primary of each peer lists the topic objects of this cluster every 5 seconds. It
   adopts the new topic into the shadow stream `__shadow_<origin>_<stream>`, with the same topic
   id, topic name and partition ids.

## Produce and consume

### Producers

A producer writes to the geo topic of the cluster it is connected to. The server has no notion
of a producer's home datacenter: a message belongs to the log of the cluster that accepted it.

The producer is acknowledged after its messages are stored in the bucket and the placed batch
commits. When the bucket does not answer, a send is answered with `TransientNotAccepted` (58),
which the SDKs retry, after at most 3 attempts at the upload within 10 seconds. A shard also
answers 58 while it holds 32 MiB of sends that wait for the bucket or for their partition, or
8 MiB of the sends of one partition. When the messages a partition committed and no long-term
segment holds yet pass `long_term_backlog_limit`, its sends are slowed instead of refused: each
waits 10 ms to 250 ms before the shard takes it.

A send to a shadow topic returns `ShadowStreamReadOnly` (1021).

### Consumers

A consumer connects to the cluster of its own datacenter, and reads:

- the geo topic, for the messages written in this datacenter, for example `orders/payments`;
- the shadow topic of each peer, for the messages written there, for example
  `__shadow_dc-b_orders/payments` in `dc-a`.

A shadow topic is read like any other topic, by stream name and topic name or id. It has the
topic id and the partition ids of its origin's topic, and it is built to serve the origin's
offsets.

A shadow topic serves the messages that its origin published in long-term segments, and reads
their bytes from the origin's bucket. A message that the origin committed and did not publish yet
is not in the shadow topic: see [Delays](#delays). Every node of the cluster serves the same
messages of a shadow partition, also after a restart.

### Consumer groups and positions

- The topic and each shadow topic are separate topics, so a consumer group keeps one position per
  log, and all of them are stored in its own cluster.
- Positions never move between datacenters. A group that moves to another datacenter starts from
  the positions it has there, if any.
- The same group name can run in several datacenters. The groups are independent, and each one
  reads every message it is given.
- Grant read permissions on each shadow stream as on any other stream.

### Duplicates

A send that was not acknowledged can still have committed. A producer that retries it in another
datacenter can write it twice, once in each log. Consumers that must not process it twice
deduplicate on an id of their own, such as the message id that the producer sets.

## Delays

| From                  | To                                 | How long                                                                                                                                                                                                               |
| --------------------- | ---------------------------------- | ---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| Send                  | Committed and acknowledged         | One upload of the fresh object and one commit. A busy shard adds up to `fresh_upload_interval`, 10 ms by default, or the wait for a free upload slot. A partition past `long_term_backlog_limit` adds 10 ms to 250 ms. |
| Committed             | Readable in the origin cluster     | At once.                                                                                                                                                                                                               |
| Committed             | Published                          | The partition's next rewrite, every 250 ms to 10 s, plus the upload and the commit of the segment. The manifest is stored after the commit.                                                                            |
| Published             | Readable in another datacenter     | Up to `[geo] manifest_poll_interval`, 5 s by default, until the peer lists the manifest, then one read of the manifest and one commit in the peer.                                                                     |
| Geo topic created     | Shadow topic in each peer          | About a second to store the topic object, then up to 5 s until a peer lists it.                                                                                                                                        |
| Geo topic deleted     | Shadow topic detached in each peer | Up to a minute until the origin writes the tombstone, then up to 30 s until a peer reads it again.                                                                                                                     |
| Shadow topic detached | Shadow topic removed               | `[geo] tombstone_grace` of the cluster that holds the shadow, 24 hours by default, counted from the tombstone's write.                                                                                                 |

Each partition adjusts its rewrite interval between 250 ms and 10 s, toward the interval at which
a long-term segment comes out 80% of 64 MiB full. A segment cut at 64 MiB starts the next rewrite
as soon as it commits. After a node starts, its partitions rewrite nothing until the first pass
of the segment cleaner, one `[data_maintenance.messages] interval` after the start, 1 minute by
default.

`partition_long_term_rewrite_lag_messages` shows how far publication trails commits on the
origin. It is the most committed messages that one partition of the shard holds and no long-term
segment holds yet.

## What fails and what is lost

| Event                                                   | Effect                                                                                                                                                                                                                                                                                                                        |
| ------------------------------------------------------- | ----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| One node of a cluster fails                             | The partitions elect another primary as for any topic. Nothing is lost.                                                                                                                                                                                                                                                       |
| A cluster's bucket does not answer                      | Its geo topics refuse sends with 58. Polls that need bytes from the bucket get 58, which the SDKs retry. Rewrites wait. Peers mark the shadow topics of that origin stale.                                                                                                                                                    |
| A cluster fails and returns, its bucket stays           | On restart the cluster replays its consensus log, and the rewrite publishes the committed messages it had not published. As for any topic, the topic's `durability` decides whether a crash of every node at once can lose acknowledged messages: see [replication factor](../README.md#replication-factor).                  |
| A cluster is destroyed with its disks, its bucket stays | A cluster rebuilt on empty disks takes back its geo topics when an operator asks for it: see [Recover an origin that lost its disks](#recover-an-origin-that-lost-its-disks). The published messages come back, and so do the committed ones that self-describing fresh objects prove. The other committed messages are lost. |
| A bucket is lost                                        | Every message of the geo topics of that cluster is lost, committed or published. This is the reason for the deployment rule.                                                                                                                                                                                                  |
| A peer's bucket does not answer                         | This cluster keeps the last manifest that each shadow partition committed, removes no shadow topic except those whose grace ended, and sets `geo_shadow_topics_stale` for that origin. Polls of a shadow topic that need bytes from that bucket get 58, which the SDKs retry. Local traffic is not affected.                  |

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
3. Start the consumers of `dc-a` in `dc-b`, on `orders/payments`. Their positions in `dc-a` stay
   in `dc-a`. In `dc-b` each group continues from its own position there, or starts where the
   consumer is told to start when the group has none.
4. Consumers in `dc-b` read the messages that `dc-a` published before it failed from the shadow
   topic `__shadow_dc-a_orders/payments`. The messages that `dc-a` acknowledged and did not
   publish stay in the log of `dc-a` until it returns.

### Failback

1. Start `dc-a`. It replays its consensus log, and its rewrite publishes the committed messages
   it had not published yet. There is nothing to reconcile: `dc-b` never wrote into the log of
   `dc-a`.
2. Move the producers back to `dc-a`, or leave them in `dc-b`. Either way, each message goes to
   the log of the cluster that accepts it.
3. Move the consumers back to `dc-a`. They continue from their positions in `dc-a`, and so read
   the messages that `dc-a` acknowledged before it failed. They read the messages written in
   `dc-b` during the outage from the shadow topic `__shadow_dc-b_orders/payments` in `dc-a`.

## Retire an origin

This version has no command to retire an origin that never returns.

What an operator can do: remove the origin's `[[geo.peers]]` entry from every node of each
surviving cluster and restart those nodes. The nodes then stop reading that bucket. The shadow
streams of that origin stay as they are, read-only, and nothing in this version removes them. No
client can delete a shadow stream. Iggy never writes to or deletes from the bucket of a peer, so
cleaning the retired origin's bucket is the job of the operator.

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

Nothing else comes back: consumer offsets, consumer groups, users and permissions, normal
topics, and the idempotent producer state of the published messages are not in the bucket.
The producers of proven and adopted batches are restored, so their retries are still
recognized.

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
recovered already.

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
rebuilt cluster's.

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
  check does not pass;
- the metadata primary lists the peer's topic objects every 5 seconds, reads each new object,
  and reads each known object again every 30 seconds;
- every `[geo] manifest_poll_interval`, shard 0 of each node lists the manifests of each peer
  stream for which the node leads a shadow partition, and the shard that owns the partition reads
  each manifest that changed;
- gets for the segments and indexes that polls of the shadow topics read.

Requests to a cluster's own bucket:

- one put per fresh object: up to one object per `fresh_upload_interval` on each shard, plus one
  for each object that fills to 4 MiB first;
- up to four puts a second on a shard whose sends are more than 250 ms apart, for the proof rows
  that no object with messages carried in that time;
- a get for each poll that misses the shard's 64 MiB cache, and gets for the rewrites and merges,
  which read the messages back;
- a put for each long-term segment and its index, and one for each manifest;
- listings of the replica's fresh objects every minute, and the deletes of fresh and long-term
  objects.

Storage: a message stays in its fresh object until at least 10 minutes after every message of
that object is published, and in a long-term segment until retention removes it. Each replica
keeps 320 bytes per batch on its disk.

## Metrics

All metrics are on the HTTP endpoint `/metrics` of each node, with a `shard` label. Counters carry
the `_total` suffix, and histograms the `_bucket`, `_sum` and `_count` suffixes.

### Buckets and peers

| Metric                             | Meaning                                                                            |
| ---------------------------------- | ---------------------------------------------------------------------------------- |
| `object_store_identity_verified`   | 1 while the node passes the store identity check of its own bucket.                |
| `object_store_unreachable`         | 1 while its own bucket does not answer the check.                                  |
| `geo_peer_identity_verified`       | Per `origin`: 1 while the node reads the peer's bucket.                            |
| `geo_peer_unreachable`             | Per `origin`: 1 while the peer's bucket does not answer the check.                 |
| `geo_shadow_topics_stale`          | Per `origin`: stale shadow topics, counted on the metadata primary.                |
| `geo_peer_manifest_requests_total` | Per `origin` and `request`, `list` or `get`: requests for the peer's manifests.    |
| `geo_peer_manifest_failures_total` | The same requests that failed.                                                     |
| `geo_peer_manifests_refused_total` | Per `origin`: manifests that this cluster refused.                                 |
| `geo_peer_manifest_commands_total` | Per `origin`: commands that committed a manifest to a shadow partition.            |
| `geo_peer_manifest_listed_seconds` | Per `origin`: Unix time of the last listing that succeeded, on shard 0.            |
| `geo_shadow_partitions_behind`     | Per `origin`: shadow partitions whose committed manifest is behind the listed one. |
| `geo_shadow_manifest_lag_seconds`  | Per `origin`: the longest time one of them has been behind.                        |

### Write path

| Metric                               | Kind      | Meaning                                                                              |
| ------------------------------------ | --------- | ------------------------------------------------------------------------------------ |
| `fresh_uploads`                      | counter   | Fresh objects the shard stored.                                                      |
| `fresh_upload_bytes`                 | counter   | Bytes of those objects.                                                              |
| `fresh_upload_retries`               | counter   | Attempts after an attempt that failed.                                               |
| `fresh_upload_failures`              | counter   | Objects the bucket refused. Their sends were answered as not accepted.               |
| `fresh_sends_denied`                 | counter   | Sends the shard answered as not accepted, for any reason.                            |
| `fresh_sends_denied_partition_share` | counter   | Of those, sends past the 8 MiB that the waiting sends of one partition may hold.     |
| `fresh_sends_denied_shard_cap`       | counter   | Of those, sends past the 32 MiB that the waiting sends of the shard may hold.        |
| `fresh_uploads_in_flight`            | gauge     | Uploads that hold one of the shard's `fresh_uploads` slots.                          |
| `fresh_staged_sends`                 | gauge     | Sends whose messages the shard holds and whose record has not reached its partition. |
| `fresh_window_wait_seconds`          | histogram | How long an upload window stayed open after its first send.                          |
| `fresh_upload_seconds`               | histogram | How long the upload of a fresh object took.                                          |
| `fresh_commit_seconds`               | histogram | How long a send waited for its answer after its fresh object was stored.             |
| `fresh_object_size_bytes`            | histogram | Size of each fresh object the shard stored, in bytes.                                |
| `fresh_proof_lag_seconds`            | gauge     | How long the oldest proof row of a committed batch has waited for a stored object.   |
| `fresh_confirmations`                | counter   | Fresh objects of proof rows alone, stored when no window closed within 250 ms.       |

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

| Key                                        | Default | Meaning                                                                                                                                                                                                                                                                                                                                                      |
| ------------------------------------------ | ------- | ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------ |
| `[tiered_storage] fresh_upload_interval`   | `10 ms` | The shortest time between the starts of two uploads of fresh objects on one shard, unless an object fills to 4 MiB first. `0` uploads as soon as a slot is free. Up to `10 s`. A longer interval makes fewer and larger objects, and makes every producer wait longer.                                                                                       |
| `[tiered_storage] long_term_backlog_limit` | `1 GiB` | The bytes of messages one partition may have committed that no long-term segment holds yet. Past it, the primary holds each send to that partition for 10 ms, up to 250 ms at twice the limit, and never refuses one for it. Its next rewrite then waits only the shortest interval, 250 ms after its last. `0` turns this off. Otherwise at least `64 MiB`. |
| `[tiered_storage.jobs] fresh_uploads`      | `4`     | Uploads of fresh objects that run at once on one shard. They have slots of their own and never wait behind segment uploads.                                                                                                                                                                                                                                  |
| `[geo] manifest_poll_interval`             | `5 s`   | How often shard 0 of each node lists the manifests of each peer stream for which the node leads a shadow partition. A shorter interval brings published messages to shadow topics sooner, and lists the peer's bucket more often.                                                                                                                            |
| `[geo] recover_cluster_uid`                | empty   | On replica 0: the `cluster_uid` of a cluster that lost every disk, whose geo topics this cluster takes back from its bucket. See [Recover an origin that lost its disks](#recover-an-origin-that-lost-its-disks).                                                                                                                                            |
| `[geo] recover_unproven`                   | `true`  | Whether that recovery also takes the batches no proof names, at the offsets their writer expected. `false` takes back only what the bucket proves.                                                                                                                                                                                                           |
| `[geo] tombstone_grace`                    | `24 h`  | How long a deleted geo topic keeps its tombstone and its objects in this cluster's bucket, and how long this cluster keeps a detached shadow topic. Make it longer than any peer outage.                                                                                                                                                                     |

The other `[tiered_storage]` keys apply to geo topics as they do to tiered topics: the timeouts
and retries of store calls, and the job caps of `[tiered_storage.jobs]`. Each shard runs one tier
service per peer with the same caps, so a slow peer takes no slot from the cluster's own bucket.

These values are fixed in this version: 4 MiB per fresh object before it closes early, 32 MiB
of waiting sends per shard and 8 MiB per partition, 3 attempts and 10 seconds per fresh object,
64 MiB per long-term segment, 10 minutes before an unnamed fresh object is deleted, and 1 hour
before an unlisted long-term object is deleted.

## Limits of this version

- **Shadow topics serve only published messages.** A message reaches the shadow topics of the
  peers after its origin's next rewrite and manifest, not at its commit.
- **Consumer positions stay in their datacenter.** No position moves with a consumer group.
- **No encrypted streams.** A geo topic in a stream with an encryption key is refused with
  `FeatureUnavailable` (5), because its sends reach the bucket before the stream's encryption
  runs.
- **No Kafka gateway.** The Kafka gateway does not support geo topics or shadow streams.
- **Frozen partition count and options.** A geo topic keeps its name, its options, including
  `message_expiry` and `max_topic_size`, and its partition count.
- **Only the `geo` mode.** `geo_tiered` is refused, and a geo topic cannot change its mode.
- **No command to retire an origin.** See [Retire an origin](#retire-an-origin).
- **Each cluster's geo topic is created there.** Adoption creates only shadow topics. Nothing
  checks that the geo topics of the same name in two clusters have the same partition count or
  options.
- **A recreated topic waits for its old shadow.** When a geo topic is deleted and created again
  with the same name, a peer adopts the new one only after it removes the old shadow topic, which
  takes `tombstone_grace`.
- **A cluster with geo topics needs an origin.** Without `[geo] origin`, a geo topic takes no
  messages.
- **Peers take static keys only.** An `s3` peer needs `access_key_id` and `secret_access_key`.
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
- **No client tooling for the model yet.** No SDK reads a topic and its shadow topics together, and
  stream listings do not mark shadow streams. A shadow stream is recognised by its `__shadow_`
  name prefix.
