# Consumer group coordination

What [#3541](https://github.com/apache/iggy/issues/3541) added: `FindCoordinator` (10),
`JoinGroup` (11), `Heartbeat` (12) and `SyncGroup` (14), backed by an in-memory coordinator in
`src/group/`. [#3543](https://github.com/apache/iggy/issues/3543) added `LeaveGroup` (13). This is Kafka's *classic* group protocol. Offsets are a separate concern and live
in Iggy ([`OFFSET_STORAGE.md`](OFFSET_STORAGE.md)).

| API key | Name | Versions | Notes |
| --- | --- | --- | --- |
| 10 | FindCoordinator | 0-4 | Always answers "this gateway", the same node the Metadata broker list advertises |
| 11 | JoinGroup | 0-9 | Parks until the group's join barrier completes |
| 12 | Heartbeat | 0-4 | Refreshes a session; `REBALANCE_IN_PROGRESS` is how a follower learns to rejoin |
| 13 | LeaveGroup | 0-5 | Removes members; survivors' parked joins and syncs are released at once |
| 14 | SyncGroup | 0-5 | Relays the leader's assignment blobs; a follower parks until the leader syncs |

`kafka-protocol` can encode FindCoordinator v5 and v6 as well, and they are byte-identical to v4.
They are not advertised because `SCOPE.md`'s governance model only admits a version once it has
been manually tested.

## Assignment is the client's job

The gateway elects a leader, hands it every member's subscription metadata, and fans the blobs the
leader returns back out - one per member, exactly the bytes filed under that member's id, in the
same generation. It never decodes `ConsumerProtocolSubscription` or `ConsumerProtocolAssignment`.

The practical consequence: **whatever assignor the client ships is the assignor** - range,
round-robin, sticky, cooperative-sticky, or a custom one. There is no supported subset to
document, and nothing to configure. Partitions come out disjoint because the leader's assignor
made them disjoint.

When two members list different protocols, the coordinator picks the one every member supports
with the most first-preference votes; a tie is broken by the leader's own list order. Kafka breaks
that tie by set iteration order, which is not deterministic - this one is, deliberately, so tests
can pin it.

## One gateway per bootstrap endpoint

Group membership is process memory. Two gateway instances fronting one Iggy cluster do **not**
share it, and a client that can reach both ends up in two independent groups under one name:

1. Each gateway advertises itself as the coordinator. A client that joined on gateway A and later
   connects to B is told `UNKNOWN_MEMBER_ID` and rejoins on B with a fresh id.
2. Each group's leader assigns **all** partitions to its own members, so every partition has two
   consumers and every record is delivered twice.
3. Both groups commit against the same Iggy key, so committed offsets flip between the two
   groups' positions.

The rule that follows: one gateway per Kafka bootstrap endpoint, with
`IGGY_KAFKA_ADVERTISED_HOST`/`_PORT` routing back to that same instance, and no load balancer in
front of more than one gateway for consumers. Fixing this needs a shared coordinator (Iggy-backed
group state, or gateway-to-gateway forwarding) and is out of scope here.

Group state also does not survive a gateway restart. Clients recover on their own - a member id
the coordinator does not know is answered with `UNKNOWN_MEMBER_ID`, which is what makes the Java
and librdkafka clients discard it and join from scratch - and committed offsets are unaffected
because they live in Iggy.

## Timeouts without a background task

There is no timer thread. Every request that touches a group first expires whatever is overdue in
it (sessions, unclaimed member ids, an elapsed join or sync window), and a parked `JoinGroup` or
`SyncGroup` handler sleeps until that group's next deadline. The coroutine waiting on a barrier is
therefore also the timer that fires it.

Expiry is judged against the recorded deadline, not against when the sweep happens to run, so a
heartbeat that arrives after its own member's deadline finds the member already gone. A broker
whose timer thread has not fired yet would still accept it. This is stricter, and deterministic.

What this does not do is evict a member of a group **nobody is talking to**. If the surviving
members are heartbeating, a dead member is evicted at most one heartbeat interval after its
deadline, and that same heartbeat returns `REBALANCE_IN_PROGRESS`, so the rebalance starts in the
same round trip. If *every* member is dead, nobody is evicted until the next request for that
group, at which point the joiner clears the stale ids and completes immediately. No consumer can
observe the difference, because there is no consumer. The only cost is stale memory, bounded by
the caps below.

## Generations

The generation id increments only when a join barrier completes. Leaving never bumps it: the
survivors keep the current generation and learn about the rebalance through
`REBALANCE_IN_PROGRESS` (27) on their next heartbeat, which the Java client handles as a graceful
rejoin rather than as lost partitions.

When a group empties, by leave or by expiry, it is dropped, so the next group under that name
starts again at generation 1. Kafka instead keeps the `Empty` group and its generation. This is
safe because member ids carry a UUID and every generation check is preceded by a membership
check, so a stale client is told `UNKNOWN_MEMBER_ID` before its generation is ever compared.

## Leaving a group

A dynamic consumer that closes cleanly sends `LeaveGroup`, and the survivors rebalance on their
next heartbeat instead of after `session.timeout.ms`. A survivor already parked in `JoinGroup` is
answered as soon as the leave completes the barrier. One parked in `SyncGroup` is answered
`REBALANCE_IN_PROGRESS` and rejoins. A request left parked by the leaver itself on another
connection is answered `UNKNOWN_MEMBER_ID`.

| Client | Dynamic member on close | Static member on close |
| --- | --- | --- |
| Java consumer, classic protocol | Leaves (best effort) | Never leaves; Java 4.1+ `CloseOptions.LEAVE_GROUP` forces it |
| Kafka Streams, classic protocol | Does not leave by default | Does not leave |
| librdkafka / kcat | Leaves on `rd_kafka_consumer_close`, v0 or v1 only | Leaves on unsubscribe, not on termination |
| Admin `removeMembersFromConsumerGroup` | | Leaves by `group_instance_id` with an empty `member_id`, v3+ |

How each identity in a request is resolved:

| `member_id` | `group_instance_id` | Result |
| --- | --- | --- |
| id of a member | null | removed, 0 |
| id handed out with `MEMBER_ID_REQUIRED` and not yet claimed | any | id dropped, 0 |
| id of a member holding `X` | `X` | removed, 0 |
| id of any member | `X`, held by other members | `FENCED_INSTANCE_ID` (82) |
| any | `X`, held by nobody | `UNKNOWN_MEMBER_ID` (25) |
| `""` | `X`, held by members | every holder removed, 0 |
| `""` | null or unheld | `UNKNOWN_MEMBER_ID` (25) |
| unknown | null | `UNKNOWN_MEMBER_ID` (25) |

A group that does not exist answers `UNKNOWN_MEMBER_ID` for every identity and is not created.
From v3 the response carries one entry per request identity, in request order, echoing exactly
the `member_id` and `group_instance_id` that were sent. Below v3 it carries one top-level code:
the request's own error, else the first member's.

## Static membership is accepted, not honoured

`group.instance.id` (JoinGroup v5+) is stored and echoed back to the leader, and it seeds the
generated member id so a static member is recognisable in logs. Nothing else about KIP-345 is
implemented, and it would need its own issue: a returning static member is **not** matched to its
previous identity. It gets a new member id and its join triggers a rebalance, while its previous
incarnation stays a member, holding its partitions, until its session expires and triggers a
second one. Kafka replaces the old identity with no rebalance at all.

The consequences:

- One instance id can be held by several members at once. A `LeaveGroup` by instance id removes
  every holder, since leaving by instance id means that instance is gone, and still answers with
  one entry. With at most one holder this is Kafka's behaviour.
- `FENCED_INSTANCE_ID` (82) is returned only by `LeaveGroup`, never by `JoinGroup`, `SyncGroup` or
  `Heartbeat`.
- A static consumer does not send `LeaveGroup` on close, with either the Java client or
  librdkafka, so its partitions stay assigned until its session expires, exactly as before
  `LeaveGroup` existed. For prompt release use a short `session.timeout.ms`, or Java 4.1+
  `CloseOptions.LEAVE_GROUP`.

## Capacity caps

`GroupCoordinatorConfig` (no environment variables yet; `GatewayConfig.group`):

| Setting | Default | Meaning |
| --- | --- | --- |
| `min_session_timeout` / `max_session_timeout` | 6s / 30min | Kafka's `group.min/max.session.timeout.ms`; outside the range is `INVALID_SESSION_TIMEOUT` (26) |
| `initial_rebalance_delay` | 3s | Kafka's `group.initial.rebalance.delay.ms`; a new group waits this out so consumers starting together land in one generation |
| `max_groups` | 1000 | Beyond it, a new group is `COORDINATOR_NOT_AVAILABLE` (15, retriable) |
| `max_members_per_group` | 1000 | Kafka's `group.max.size`; beyond it, `GROUP_MAX_SIZE_REACHED` (81) |
| `max_total_members` | 10000 | Across every group, checked before a member id is minted; `COORDINATOR_NOT_AVAILABLE` (15) |
| `max_member_blob_bytes` | 64 KiB | One JoinGroup's total protocol metadata, and one SyncGroup assignment blob; beyond it, `INVALID_REQUEST` (42) |
| `max_group_roster_bytes` | 4 MiB | Member ids, instance ids and largest protocol metadata summed across one group, which bounds the leader's JoinGroup response; beyond it, `GROUP_MAX_SIZE_REACHED` (81) |

The frame-level bounds guard (`src/protocol/bounds_guard.rs`) bounds one request. These bound what
is *retained*: a member's subscription and assignment outlive the connection that sent them, up to
`max_session_timeout`. Worst case at the defaults is roughly 1.3 GiB of opaque bytes.

A group id is capped at 246 bytes, not Kafka's 249: the Iggy offset key is `kafka.cg.<group>` and
an Iggy name caps at 255. A longer id is `INVALID_GROUP_ID` (24) here rather than a failure later
at commit time.

## What a real consumer still cannot do

A consumer completes JoinGroup and SyncGroup and then holds no partitions. Metadata is a stub that
answers `UNKNOWN_TOPIC_OR_PARTITION` (3) for every topic, so the leader's assignor sees no
partitions and hands every member an empty assignment. The consumer stays a member and keeps
heartbeating, but has nothing to fetch.

Once Metadata reports partitions, the next wall is `OffsetFetch` (9), which a consumer sends after
`SyncGroup` for a non-empty assignment and which is not in scope
([#3542](https://github.com/apache/iggy/issues/3542)). An unlisted key closes the connection, so
that consumer would loop: coordinator connection closes, client marks the coordinator unknown,
re-runs FindCoordinator, retries OffsetFetch, closes again. Fetch is a stub in any case, so nothing
can be consumed until [#3535](https://github.com/apache/iggy/issues/3535)/#3542 land.

A dynamic consumer releases its partitions on close through `LeaveGroup`. A static one does not
send it (see [Static membership](#static-membership-is-accepted-not-honoured)), so a static
consumer's shutdown still costs the survivors up to `session.timeout.ms` before they rebalance.
That is exactly what Kafka does for a *crashed* consumer, so it is a degraded shutdown rather than
a wedge.

`ConsumerGroupHeartbeat` (68), the KIP-848 protocol, is not implemented and a client cannot fall
back from it. A Kafka 4.0 client may need `group.protocol=classic` to reach these keys at all.
