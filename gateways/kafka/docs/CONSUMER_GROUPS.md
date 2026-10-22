# Consumer group coordination

What [#3541](https://github.com/apache/iggy/issues/3541) added: `FindCoordinator` (10),
`JoinGroup` (11), `Heartbeat` (12) and `SyncGroup` (14), backed by an in-memory coordinator in
`src/group/`. [#3543](https://github.com/apache/iggy/issues/3543) added `LeaveGroup` (13).
[#3548](https://github.com/apache/iggy/issues/3548) added `DescribeGroups` (15) and `ListGroups`
(16). [#4511](https://github.com/apache/iggy/issues/4511) added `DeleteGroups` (42) and
`OffsetDelete` (47). This is Kafka's *classic* group protocol. Offsets are a separate concern and
live in Iggy ([`OFFSET_STORAGE.md`](OFFSET_STORAGE.md)).

| API key | Name | Versions | Notes |
| --- | --- | --- | --- |
| 10 | FindCoordinator | 0-4 | Always answers "this gateway", the same node the Metadata broker list advertises |
| 11 | JoinGroup | 0-9 | Parks until the group's join barrier completes |
| 12 | Heartbeat | 0-4 | Refreshes a session; `REBALANCE_IN_PROGRESS` is how a follower learns to rejoin |
| 13 | LeaveGroup | 0-5 | Removes members; survivors' parked joins and syncs are released at once |
| 14 | SyncGroup | 0-5 | Relays the leader's assignment blobs; a follower parks until the leader syncs |
| 15 | DescribeGroups | 0-6 | Members, assignment and state. A group that is not here but holds offsets in Iggy is `Empty`. Otherwise it is `Dead`, and v6 also returns `GROUP_ID_NOT_FOUND` (69) |
| 16 | ListGroups | 0-5 | Groups currently on this coordinator, plus the groups that hold offsets in Iggy as `Empty`. v4+ can filter by state, v5 by type `classic` |
| 42 | DeleteGroups | 0-2 | Deletes a group without members, with its offsets. While it has members: `NON_EMPTY_GROUP` (68). When it does not exist: `GROUP_ID_NOT_FOUND` (69) |
| 47 | OffsetDelete | 0 | Deletes offsets of a group. `GROUP_SUBSCRIBED_TO_TOPIC` (86) for a topic that a member subscribes to |

`kafka-protocol` can encode FindCoordinator v5 and v6 as well, and they are byte-identical to v4.
They are not advertised because `SCOPE.md`'s governance model only admits a version once it has
been manually tested.

## Admin views

`DescribeGroups` and `ListGroups` read the same in-memory map the join path writes. State strings
are `Empty`, `PreparingRebalance`, `CompletingRebalance` and `Stable`. `Empty` means the group
has no joined members. Either a v4+ client got a member id and did not rejoin with it yet, or
the group only holds offsets
([below](#groups-that-only-hold-offsets)). `Dead` is only the describe answer for an id
that is not in the map and holds no offsets.

`ListGroups` does not tick. It leaves sessions, pending ids and phases untouched, and it omits a
group whose member sessions and pending-id deadlines are all already past. `DescribeGroups` does
tick each named group, so that same id is then `Dead`, or `Empty` for a group that holds offsets.

While a group is not `Stable`, `DescribeGroups` leaves `protocol_data`, member metadata and
member assignment empty. Kafka does the same: during `PreparingRebalance` the stored assignment
is the previous generation, and a rejoined member has already replaced its subscription. A
`Stable` group whose own description does not fit `max_frame_size` is returned in that same
shape. Further groups that do not fit the remaining response are returned with a per-group
`UNKNOWN_SERVER_ERROR` (-1). The connection stays open.

A group id repeated in one `DescribeGroups` request is answered once, not once per repeat: a
second snapshot of the same members would otherwise multiply the encoded response by the repeat
count.

`kafka-consumer-groups.sh --list`, `--describe --members` and `--describe --state` use these
two APIs. With a bridge, the default `--describe` also reads each committed offset with
`OffsetFetch` (9) and each end offset with `ListOffsets` (2), and prints the lag.
`--reset-offsets` writes the new offsets with `OffsetCommit` (8). Both work for a group whose
consumers stopped, because that group is `Empty` while it holds offsets. Without a bridge,
`OffsetFetch` answers `COORDINATOR_LOAD_IN_PROGRESS` (14). `--delete` and `--delete-offsets` use
`DeleteGroups` and `OffsetDelete` ([below](#deleting-groups-and-offsets)). See
[OFFSET_STORAGE.md](OFFSET_STORAGE.md) for offset behavior and limits.

Member `client_id` and `client_host` are empty. The coordinator does not keep the request
header's client id or the connection's peer address. `include_authorized_operations` is answered
with the omitted sentinel. There is no group ACL bitfield. `ListGroups` v5 sets `group_type` to
`classic`, which is the only protocol this coordinator runs.

## Groups that only hold offsets

A group with neither members nor pending member ids is removed from the map. Its offsets stay in
Iggy, and Kafka keeps such a group and reports it as `Empty`. So both APIs also report each group
that holds offsets in Iggy and has no members here. Such a group is `Empty`, with protocol type
`consumer` and no members. Iggy stores no protocol type. Kafka keeps `consumer` for a group whose
consumers left, and the Java admin client takes `consumer` or an empty type as a consumer group.

The gateway finds these groups with a scan. A scan lists the Kafka topics, then calls
`get_consumer_groups` once per topic and keeps the Iggy groups named `kafka.cg.<group>`
([`OFFSET_STORAGE.md`](OFFSET_STORAGE.md)). It runs on an Iggy client of its own, which sends the
calls for 16 topics at once. So it never waits behind Produce, and no offset call waits behind it.
At most one scan runs at a time. A commit through this gateway adds its group at once. A deletion
through it removes the group at once. Neither waits for the next scan. A group that another gateway
created appears with the next scan.

The groups of a scan answer at once for 10 seconds after the scan ends. So the `ListGroups` and the
`DescribeGroups` of one `kafka-consumer-groups.sh` run share one scan, which costs one Iggy call per
topic. After that, a request starts a new scan. Until 1 minute after the last scan ended, the
request still gets the groups of that scan at once. The new scan serves later requests. So kafka-ui,
which lists groups every 30 seconds, never waits for a scan. A request that finds no groups, or only
older ones, waits for the new scan until its offset deadline, 3 seconds by default.

A scan keeps running after its request stops waiting, for up to 30 seconds, and serves the next
request. A scan that reaches that limit keeps the groups it found, and the groups of the last scan
stay too. If a scan fails, the groups of the last scan that found them stay the answer. While the
first scan runs, `ListGroups` answers `COORDINATOR_LOAD_IN_PROGRESS` (14), and `DescribeGroups`
answers 14 for each named group that is not in the map. The Java admin client retries that code.
If every scan so far failed, for example because Iggy is down, both APIs report only the groups in
the map, as without a bridge. A `ListGroups` state filter without `Empty`, or a type filter without
`classic`, needs no scan. `DescribeGroups` scans only for a named group that is not in the map.

A negative commit, or `OffsetDelete`, deletes an offset but not the Iggy group. So a group whose
offsets were all deleted that way still shows as `Empty`, until `DeleteGroups` deletes it. Without
a bridge, no group holds offsets, and both APIs report only the groups in the map.

## Deleting groups and offsets

`DeleteGroups` and `OffsetDelete` follow Kafka's rules for a classic group
(`ClassicGroup.validateDeleteGroup` and `validateOffsetDelete`).

| Group | `DeleteGroups` | `OffsetDelete` |
| --- | --- | --- |
| Has members of protocol type `consumer` | `NON_EMPTY_GROUP` (68) | Per partition: `GROUP_SUBSCRIBED_TO_TOPIC` (86) for a topic that a member subscribes to, else deleted |
| Has members of another protocol type | `NON_EMPTY_GROUP` (68) | `NON_EMPTY_GROUP` (68) |
| Has no members here, holds offsets in Iggy | Deleted | Deleted |
| Only has ids that wait to join | Deleted | Deleted |
| Not here, holds no offsets | `GROUP_ID_NOT_FOUND` (69) | `GROUP_ID_NOT_FOUND` (69) |
| Empty id, or longer than 246 bytes | `INVALID_GROUP_ID` (24) | `INVALID_GROUP_ID` (24) |

`DeleteGroups` deletes the Iggy group `kafka.cg.<group>` on every Kafka topic, and Iggy deletes its
offsets with it. It first lists the Kafka topics and the Iggy groups of each topic. It lists them on
the client of the scan, so the listing does not wait behind `OffsetFetch`. That costs one call per
topic, and at least two per deletion. In a cluster, that client can read a replica that missed a
recent commit. So `DeleteGroups` also deletes on each topic that a recent commit through this
gateway stored offsets of the group to. A commit is recent for a minute after it ends. When the
commit created the Iggy group, the gateway keeps the numeric id that the create returned, and
deletes by that id. No read can miss such a group. It answers a repeated group id once. A group that
only has ids that wait to join loses those ids only after its offsets are gone. So a deletion that
fails changes nothing, as in Kafka.

The gateway deletes an Iggy group by its numeric id, so a delete that lands late cannot hit a newer
group of the same name. Iggy never gives an id to a second group of the same topic. A topic that is
deleted and created again numbers its groups from the start, though. So before the gateway deletes
by an id that it kept, it reads the group with that id. When that group has another name, the kept
group went with the older topic, and the gateway looks the group up by name instead. Iggy answers
the delete of a group once every partition of the topic dropped the group, and it deletes the
offsets after that. In a cluster, another replica can still list the group for a short time. So the
gateway then looks up the id on the topic every 5 to 100 milliseconds. It looks on the client that
deleted the group, which reads what it wrote. When the id is gone, or names another group, the
gateway answers. Then `OffsetFetch` returns -1 for the group, and the admin APIs no longer find it.

`OffsetDelete` first finds out whether a group with no members here holds offsets. When the last
scan found the group, that scan answers. Otherwise the gateway asks each requested topic, which also
finds a group that another gateway committed for since that scan. Only a group that no requested
topic holds waits for a scan, which tells whether the group holds offsets on another topic.

While a deletion runs, the group takes no join and no commit. `JoinGroup` and `OffsetCommit` answer
`COORDINATOR_LOAD_IN_PROGRESS` (14), and clients retry. The deletion waits for each commit that
passed its check before the deletion began. So a commit lands whole before the deletion or whole
after it. Also, no member subscribes to a topic between the check of `OffsetDelete` and its
deletions. If one of those commits still runs at the deadline of the deletion, the deletion
answers 14.

`DeleteGroups` and `OffsetDelete` give up at 25 seconds on a connection where no member heartbeats,
as on an admin client's. A deletion can list every topic, which takes longer than the offset
deadline. On a connection where a member heartbeats, they give up at the offset deadline.

A commit or a deletion counts until none of its Iggy calls can land. A call that its request
stopped waiting for goes on, and can still land. A call that Iggy has not answered after 30 seconds
ends in doubt, and Iggy can still run it. So the gateway sends it again until Iggy proves what it
did. When the SDK lost such a delete, the gateway deletes the group again, by numeric id, until Iggy
answers. Each call holds its lane for at most 45 seconds. A later write on the same client first
waits up to 45 seconds for the write in doubt to settle. A delete that still waits then is not sent,
and its deletion answers 14. While Iggy deletes a group, an offset stored there is deleted with the
group. That happens even after the gateway acknowledged the commit.

A deletion that does not end by its deadline answers 14. The group then takes no join and no commit
until no Iggy call of the deletion can land. Some topics can then hold no offsets of the group while
others still do. A consumer that joins before the admin retries reads the former from
`auto.offset.reset` and the latter from their old offsets. A retry deletes the rest. When the
deletion ends before the retry, the retry finds no offsets of the group. Within a minute it still
answers 0, once, as Kafka answers the deletion that the client asked for. A later request answers
`GROUP_ID_NOT_FOUND` (69).

A gateway does not see the members on another gateway. Through a gateway where a group has no
members, `DeleteGroups` deletes the group. `OffsetDelete` there also deletes offsets of the topics
that those members subscribe to. Point admin tools at the gateway that the group's consumers use
([`OFFSET_STORAGE.md`](OFFSET_STORAGE.md#more-than-one-gateway)).

`OffsetDelete` reads the topics that the members subscribe to from their metadata for the selected
protocol, as Kafka does. It reads only the topic list of the consumer protocol's v0 prefix, which
every later version keeps. If a member's metadata does not decode or has a negative version, or the
group has not chosen a protocol yet, every topic counts as subscribed. A topic or partition that does not exist gets
`UNKNOWN_TOPIC_OR_PARTITION` (3). Any other partition gets 0. A partition that held no offset gets
0 too.

A call to Iggy that a retry can fix answers `COORDINATOR_LOAD_IN_PROGRESS` (14), and the Java admin
client retries it. `DeleteGroups` answers it for the group, and `OffsetDelete` for the whole
request. A second delete of an offset or a group does no harm. When Iggy refuses the bridge user,
`DeleteGroups` answers `GROUP_AUTHORIZATION_FAILED` (30). When the bridge user has no permission to
list the Iggy groups of a Kafka topic, `DeleteGroups` deletes nothing. It answers 30 for each group
to delete. The same applies to an override topic that the bridge user has no permission to read.
`Metadata` leaves such a topic out. A group can hold offsets there, so the gateway cannot delete all
of them. The log names the topics.

A SASL listener names principals, and so does an `SSL` listener that verifies client certificates.
When a listener names principals, both APIs require the principal to hold the global Iggy permission
`manage_topics` or `manage_streams`, as `DeleteTopics` does. Kafka asks for `DELETE` on the group,
and Iggy has no group permission. Without it, `DeleteGroups` answers `GROUP_AUTHORIZATION_FAILED`
(30) for every group, `OffsetDelete` answers it for the request, and the gateway deletes nothing. A
connection that proved no identity is `ANONYMOUS` and holds nothing, also on a `PLAINTEXT` listener
beside such a listener. When no listener names principals, any client that reaches the gateway can
delete groups and offsets.

## Assignment is the client's job

The gateway elects a leader, hands it every member's subscription metadata, and fans the blobs the
leader returns back out - one per member, exactly the bytes filed under that member's id, in the
same generation. It never decodes `ConsumerProtocolAssignment`, and it reads only the topic list of
a `ConsumerProtocolSubscription`, for `OffsetDelete`.

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

When a group empties, by leave or by expiry, it is dropped, so the next group under that name starts
again at generation 1. The pending-member-id window above is the one case that stays and reports
`Empty`. Kafka also keeps an `Empty` group that only holds committed offsets, and its generation
with it. This gateway reports that group as `Empty` from its offsets in Iggy ([Groups that only hold
offsets](#groups-that-only-hold-offsets)), but keeps no generation for it. This is safe because
member ids carry a UUID and every generation check is preceded by a membership check. So a stale
client is told `UNKNOWN_MEMBER_ID` before its generation is ever compared.

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
| id of any member | `X`, held by another member | `FENCED_INSTANCE_ID` (82) |
| any | `X`, held by nobody | `UNKNOWN_MEMBER_ID` (25) |
| `""` | `X`, held by a member | that member removed, 0 |
| `""` | null or unheld | `UNKNOWN_MEMBER_ID` (25) |
| unknown | null | `UNKNOWN_MEMBER_ID` (25) |

A group that does not exist answers `UNKNOWN_MEMBER_ID` for every identity and is not created.
From v3 the response carries one entry per request identity, in request order, echoing exactly
the `member_id` and `group_instance_id` that were sent. Below v3 it carries one top-level code:
the request's own error, else the first member's.

## Static membership

`group.instance.id` (JoinGroup v5+) makes a member static, as KIP-345 defines it. One instance id
is held by at most one member, and the generated member id starts with the instance id.

A static member that joins with an empty member id while its instance id is held takes that
member over under a new member id, and the old member id is fenced. In `Stable` it keeps the
assignment and the generation:

| Group state when it returns | What happens |
| --- | --- |
| `Stable` | No rebalance. The member gets the current generation and syncs to its previous assignment. From JoinGroup v9 a returning leader is sent the roster with `skip_assignment` set. Below v9 the response names the previous leader, so the client acts as a follower and does not reassign. A rebalance opens only if the member's protocols change the group's selected protocol |
| `CompletingRebalance` | A rebalance opens, since the leader may already be assigning to the old member id |
| `PreparingRebalance` | The member counts as rejoined for the open barrier |

A static member joining for the first time is admitted at once, without the `MEMBER_ID_REQUIRED`
(79) round trip a dynamic member makes from v4, as in Kafka. The replacement does not count
against `max_members_per_group` or `max_total_members`, since the group does not grow. Kafka checks
`group.max.size` on a replacement too, though its default is unlimited.

`JoinGroup`, `SyncGroup`, `Heartbeat`, `OffsetCommit` and `LeaveGroup` that name an instance id
are checked against its holder. Another member's id gets `FENCED_INSTANCE_ID` (82), and an
instance id nobody holds gets `UNKNOWN_MEMBER_ID` (25). A request parked in `JoinGroup` or
`SyncGroup` by the replaced member is checked against the holder again when it resumes, so it is
answered 82. A request from the replaced member id that names no instance id gets 25, because
that member id is gone. Nothing is kept per replaced id: one is fenced for as long as another
member holds its instance id, and gets 25 once that holder leaves or expires.

A static member that rejoins without its instance id stays static. A dynamic member cannot become
static by naming an instance id: it gets 25, or 82 if another member holds it. A member id handed
out with `MEMBER_ID_REQUIRED` that comes back naming an instance id gets 25 and is dropped.

A static member that does not rejoin before a join window closes stays in the group and in the
leader's roster, as in Kafka, so it can come back to its assignment without another rebalance. A
dynamic member that does not rejoin is removed. When the sync deadline passes, every member of
that generation that has not synced is removed and the group rebalances, an absent static member
included, as in Kafka. The deadline still applies after the leader syncs and the group is
`Stable`. A member that syncs late, or a static member that returns to its assignment, no longer
owes a sync.

The session still bounds a static member. If it does not come back within `session.timeout.ms`,
it is evicted and the group rebalances, as for a dynamic member.

A static consumer does not send `LeaveGroup` on close, with either the Java client or librdkafka,
so its partitions stay assigned until it returns or its session expires. For prompt release use a
short `session.timeout.ms`, or Java 4.1+ `CloseOptions.LEAVE_GROUP`.

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

A JoinGroup may list at most `MAX_PROTOCOLS_PER_MEMBER` (16) protocols; more is `INVALID_REQUEST`
(42). The byte cap above does not bound the count, protocol selection runs under the coordinator
lock, and stock clients list one or two assignors.

## What a real consumer can and cannot do

Without a bridge, a consumer completes JoinGroup and SyncGroup and then holds no partitions.
Metadata is a stub that answers `UNKNOWN_TOPIC_OR_PARTITION` (3) for every topic, so the leader's
assignor sees no partitions and hands every member an empty assignment. The consumer stays a
member and keeps heartbeating, but has nothing to fetch.

With a bridge, Metadata reports the partitions and a group member consumes. After `SyncGroup`, it
reads its start position with `OffsetFetch` (9) and later commits with `OffsetCommit` (8). Both
keep the offsets in Iggy ([`OFFSET_STORAGE.md`](OFFSET_STORAGE.md)). Without a bridge, both answer
`COORDINATOR_LOAD_IN_PROGRESS` (14), and the client retries them.

A dynamic consumer releases its partitions on close through `LeaveGroup`. A static one does not
send it (see [Static membership](#static-membership)), so a static consumer that stops for good
costs the survivors up to `session.timeout.ms` before they rebalance, as in Kafka. One that
restarts within that time takes its partitions back with no rebalance.

`ConsumerGroupHeartbeat` (68), the KIP-848 protocol, is not implemented and a client cannot fall
back from it. It is opt-in: a Kafka 4.0 client still defaults to `group.protocol=classic`, which reaches
these keys.
