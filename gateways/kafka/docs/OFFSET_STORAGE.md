# Consumer offset storage

Status: proposed. Answers [#3540](https://github.com/apache/iggy/issues/3540) and blocks
[#3542](https://github.com/apache/iggy/issues/3542), OffsetCommit and OffsetFetch.

## Decision

Store Kafka group offsets as Iggy consumer offsets, one key per partition, under a consumer
group whose name is derived from the Kafka group id.

The issue lists three options. None of them is this one.

| Option | Why not |
| -------- | --------- |
| A, an Iggy-backed `__consumer_offsets` topic | Rebuilds what Iggy already has. A compacted offset topic needs compaction, which Iggy does not have, so the gateway replays the whole topic at every startup |
| B, a SQLite file on the gateway host | A second durability story, a second backup story, and offsets that do not survive moving the gateway |
| C, in memory only | Fails the acceptance criterion in #3542, which is that offsets survive a restart |

Iggy already stores a durable offset per consumer and per partition, replicated with the
partition itself. Using it costs one call per partition on commit and one on fetch.

## The key

One Iggy consumer offset per Kafka `(group, topic, partition)`.

- consumer kind: `ConsumerKind::ConsumerGroup`
- consumer id: `Identifier::named("kafka.cg.<group>")`
- stream and topic: whatever `TopicMapping` resolves the Kafka topic to
- partition: the Kafka partition index, unchanged, because both sides number from 0

The gateway calls `create_consumer_group(stream, topic, "kafka.cg.<group>")` before the first
commit for a group on a topic. If the group does not resolve in metadata, the server rejects the
offset write. The group has to exist first. The gateway never joins the group. Offsets are
readable by any client, member or not.

### Why the group kind and not a named consumer

`ConsumerKind::Consumer` with a name looks simpler, because it needs no registration call. It is
not. The server hashes a named consumer id to a `u32` with `XxHash32`
(`core/server/src/dispatch/partition.rs:916`), and that hash is the offset key. Two different
group names can collide and silently share one offset.

A consumer group name resolves through metadata to a monotonic id instead. No hash, no
collision, and `get_consumer_groups(stream, topic)` lists what exists.

### Why the prefix

`kafka.cg.` keeps a Kafka group called `orders` off the key that a native Iggy consumer group
called `orders` uses. Without it the two share an offset and each one moves the other.

The prefix does not make the offsets safe to poll with. That is the next section.

## What is stored

The Kafka committed offset, verbatim, with no conversion.

The two systems mean different things by the number. Kafka commits the next offset to read.
Iggy stores the last offset processed, and `PollingKind::Next` resumes at the stored value plus
one (`core/partitions/src/iggy_partition.rs:3835`). A Kafka offset stored in an Iggy key is
therefore one greater than Iggy's own convention for that key.

This is inert because the gateway never polls that way. Fetch always polls with an explicit
offset, `PollingKind::Offset`, taken from the Kafka request. Nothing in the gateway reads the
stored value to decide where to resume. It is returned to the client on OffsetFetch and
otherwise untouched.

The rule this creates: no code path polls a `kafka.cg.*` key with `PollingKind::Next`. Doing so
skips one record per partition. The prefix is what keeps a native Iggy consumer from doing it by
accident.

Converting on write instead, and storing the Kafka offset minus one, breaks at offset 0. It also
makes an empty commit look the same as a commit of the first record. Storing verbatim is the
smaller problem.

### A commit of -1

Kafka does not validate the sign of a committed offset. `OffsetMetadataManager` checks the
metadata length and nothing else, so a real broker stores `-1` and hands it back on the next
OffsetFetch, where a consumer reads it as no committed offset.

Iggy cannot store that value, because `store_consumer_offset` takes a `u64`. A commit of `-1`
therefore calls `delete_consumer_offset` on the key. What a client can observe is the same: the
next OffsetFetch finds nothing and the gateway answers `-1`.

Deleting a key that is not there returns `ConsumerOffsetNotFound` (3021). On this path that
counts as success, because the client asked for the offset to be absent and it is absent. Any
other negative offset is rejected with `OFFSET_OUT_OF_RANGE` (1).

## OffsetFetch with no topics named

OffsetFetch v2 and later let a client pass a null topic list, which asks for every offset the
group holds. `kafka-consumer-groups.sh --describe` does this.

Iggy has no lookup by consumer. Offsets are read one partition at a time
(`core/common/src/traits/consumer_offset_client.rs:41`). The gateway answers by enumerating the
topics in the mapped stream and querying each partition of each one.

That is one round trip per partition on an admin call. The cost is bounded by the topic and
partition count of one stream. This is an admin path and not a data path, so the cost is
acceptable. It is written here so nobody discovers it in a test.

## What is dropped

Kafka lets a client attach a metadata string to a commit. Iggy stores a number and nothing else.
The string is dropped on commit, and OffsetFetch returns an empty string.

`committed_leader_epoch` is dropped the same way. The gateway reports `-1`.

## Limits

A partition admits a bounded number of distinct offset keys per consumer kind. The default is
4096, set by `partition.consumer_offsets_max`
(`core/configs/src/server_config/partition.rs:151`). The configurable ceiling is 262144. Passing
the limit returns `TooManyConsumerOffsets` (3024). Kafka has no error code for this condition, so
it maps to `UNKNOWN_SERVER_ERROR`, which is what `bridge/error.rs` already does where no honest
code exists.

Consumer groups and plain consumers count against separate limits, so Kafka groups do not
compete with native Iggy consumers for the same 4096.

A client cannot act on that error, so the operator has to. `partition.consumer_offsets_max` is
named in the gateway README for that reason, and a handler that hits the limit logs the Iggy
error at `error!` level, which is what `bridge/error.rs` already asks handlers to do wherever the
Kafka code it sends is less specific than the Iggy error it received.

An Iggy name is capped at 255 bytes (`core/common/src/lib.rs:168`), which leaves 246 for a Kafka
group id after the prefix. A longer group id is rejected with `INVALID_GROUP_ID` (24).

## More than one gateway instance

Two gateway instances that share an Iggy cluster read and write the same offset keys. The key
comes from the Kafka group id and nothing else. Two instances that serve one group therefore
agree on committed offsets without talking to each other.

They do not agree on group membership. That belongs to the coordinator
([#3541](https://github.com/apache/iggy/issues/3541)) and is not settled here.

## Open question

Iggy consumer offsets keyed by group, as above, or one of A, B and C from the issue?

If no answer lands by 2026-09-22, the design above is taken and the work proceeds. This document
is then updated to record that it was decided by default.

## References

- Record mapping: [`BRIDGE_MAPPING.md`](BRIDGE_MAPPING.md)
- Scope and phases: [`SCOPE.md`](SCOPE.md)
- Offset API: `core/common/src/traits/consumer_offset_client.rs`
- Group API: `core/common/src/traits/consumer_group_client.rs`
- Offset key resolution: `core/server/src/dispatch/partition.rs`
