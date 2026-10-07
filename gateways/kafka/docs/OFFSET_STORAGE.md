# Consumer offset storage

Status: accepted. Replaces the first design ([#4205](https://github.com/apache/iggy/pull/4205),
[#4289](https://github.com/apache/iggy/pull/4289)). Answers
[#3540](https://github.com/apache/iggy/issues/3540). Used by OffsetCommit and OffsetFetch
([#3542](https://github.com/apache/iggy/issues/3542)).

## Decision

Kafka group offsets go in a third Iggy consumer kind, `ConsumerKind::ExternalGroup` (wire kind 3),
next to `Consumer` and `ConsumerGroup`.

| Property | Value |
| --- | --- |
| Key | The partition, plus Iggy consumer group `kafka.cg.<group>` on the mapped topic, resolved by name |
| Value | The Kafka offset as sent. Iggy does not read it |
| Membership | Not needed. The gateway never joins the group |
| Range check | None. 0 on an empty partition and the high watermark are both stored |
| Polling | Refused for this kind |
| Retention | Not held back by these offsets, as in Kafka |
| Group deleted | Its offsets go with it |

## Why not the first design

It stored offsets as `ConsumerGroup` offsets. Two server rules block that:

- Only a member that owns the partition can store or delete a group offset (`fence_group_offset`,
  `core/server/src/namespace.rs:239`). The fence stays. It holds for an empty group too
  (`partition_primary_routing.rs:888`).
- A stored offset must be at most the last message, and an empty partition takes none
  (`store_offset_range_error`, `core/partitions/src/iggy_partition.rs:3625`). A caught-up Kafka
  consumer commits last + 1. A Java consumer on defaults commits 0 on an empty partition.

A plain `Consumer` key passes both checks, but its name is hashed to 32 bits and deleting a group
leaves it behind.

## Gateway rules

- A commit stores the offset. If group `kafka.cg.<group>` is not on the topic, the gateway creates it
  and stores again. A group that already exists counts as created. An operator can delete the group,
  and the next commit creates it again.
- The prefix keeps a Kafka group apart from a native Iggy group with the same name.
- Group id: 1 to 246 bytes, else `INVALID_GROUP_ID` (24). Iggy names stop at 255.
- Negative offset: delete the key. A missing key or group counts as success. Kafka consumers read any
  negative offset as none.
- Metadata string and leader epoch: dropped. OffsetFetch returns `""` and -1.
- No key, unknown group or unknown topic: OffsetFetch returns -1 and no error.
- Any other failed read fails the whole group in OffsetFetch. A lost read never goes back as -1,
  because the consumer then resets its position. A read that its owning shard does not answer in
  time, or that finds the partition not loaded there, comes back from the server as
  `TransientNotAccepted`, not as "no offset".
- Null topic list (OffsetFetch v2+): read every Kafka topic that has group `kafka.cg.<group>`. Only
  the partitions with an offset come back. A topic deleted during the read is skipped.
- A failure that a retry can fix goes back as `COORDINATOR_LOAD_IN_PROGRESS` (14). The Java client
  fails an offset call on 6, and fails OffsetFetch on 7. `RequestTooOld` goes back as 14 too: the
  retry is a new request, and storing an offset twice does no harm.
- A replayed write that Iggy already applied (`RequestAlreadyApplied`) counts as done.
- A failure sent as `UNKNOWN_SERVER_ERROR` (-1) is logged at `error!`: once per OffsetCommit
  request, and once per group in OffsetFetch.
- Committed offset below the oldest kept message: Fetch reads from the first kept message.

## Calls

- Offset calls run on 4 offset slots, each with its own Iggy client. They never wait behind
  Produce. The topic listing for a null topic list runs on a slot too.
- The partitions of a request, the groups of a v8 OffsetFetch and the topics of a null topic list
  all run at once.
- A partition always takes the same slot. A commit keeps its slot until the SDK lets go of it, so
  the commits of one partition reach Iggy in order, even after a caller gives up. A read waits for
  them, and frees its slot at the deadline.
- A request gives up at half of the minimum session timeout: 3 s at the default 6 s. A heartbeat
  on the same connection waits behind it, so a request also gives up at half of what is left of
  the session of the member that heartbeats there. A partition that the deadline leaves out
  answers 14 and makes no call.
- Cost: one Iggy call per partition. A null topic list costs one topic listing, one call per Kafka
  topic, and one call per partition of each topic that holds the group.

## Limits

| Limit | Value |
| --- | --- |
| Keys per partition, per kind | 4096, at most 262144: `partition.consumer_offsets_max` |
| Past the limit | `TooManyConsumerOffsets` (3024). Sent as `UNKNOWN_SERVER_ERROR`, logged at `error!` |

The gateway never deletes a `kafka.cg.<group>` group, and does not serve DeleteGroups (42). A group
nobody uses keeps its keys until an operator deletes its Iggy group on each topic:

```bash
iggy consumer-group list <stream> <topic>
iggy consumer-group delete <stream> <topic> kafka.cg.<group>
```

The gateway does not expire groups. No gateway knows whether another gateway or a standalone
consumer still uses a group, and Iggy keeps no commit time.

## More than one gateway

Offsets are shared, since the key depends only on the Kafka group id. Membership is not
([`CONSUMER_GROUPS.md`](CONSUMER_GROUPS.md)).

- A gateway with no members in a group accepts a commit with no generation, as admin tools and
  `assign()` consumers send, even when another gateway has live members in it. That commit
  overwrites their offsets. Point admin tools at the gateway that the group's consumers use.
- A commit that a gateway gave up on can still land after a newer commit made through another
  gateway. It then puts back the older offset, or deletes the key. Iggy cannot refuse the older
  write yet.

## Server side

[#4392](https://github.com/apache/iggy/pull/4392) adds the kind:

- Wire: `KIND_EXTERNAL_GROUP = 3`, next to kinds 1 and 2 in `core/binary_protocol/src/primitives/consumer.rs`.
- SDK: `ConsumerKind::ExternalGroup`. The enum is not `#[non_exhaustive]`, so this breaks the API.
- `core/partitions`: a third offset table, with recovery, state transfer and its own key count.
- Server: no fence and no range check for the kind, refuse polls, leave it out of
  `min_committed_offset`, delete its keys with the group (`partition_reconciler.rs`).

## Upgrade

Every Iggy server must run a build with kind 3 before the first external group store. Nothing
enforces this yet ([#4416](https://github.com/apache/iggy/issues/4416)).

- A server with an older build refuses kind 3 with `InvalidCommand`. The gateway sends that as
  `UNKNOWN_SERVER_ERROR` and logs it at `error!`.
- A primary with the new build accepts the store. If most replicas of that partition run an older
  build, they cannot decode it, and the partition stops committing (`core/server/README.md`).

The Iggy HTTP API cannot carry kind 3. The gateway uses TCP, so this does not limit it.

## Reads in a cluster

An offset read goes to the Iggy node that the gateway is connected to. That is not always the
partition primary (the node that orders the partition writes). So an OffsetFetch right after an
OffsetCommit can return the older value until that node applies the commit.

- After a later commit, the consumer reads some records again.
- After the first commit of a group, the node has no offset and answers -1. The consumer then
  applies `auto.offset.reset`. With `latest`, it skips records.

Reads from the primary are [#4409](https://github.com/apache/iggy/issues/4409). Until then, use
`auto.offset.reset=earliest` for a group that a cluster serves.

## References

- Record mapping: [`BRIDGE_MAPPING.md`](BRIDGE_MAPPING.md)
- Scope: [`SCOPE.md`](SCOPE.md)
- Offset API: `core/common/src/traits/consumer_offset_client.rs`
