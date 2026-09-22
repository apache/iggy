# InitProducerId and idempotent producers

Status: proposed. Answers the open half of
[#3545](https://github.com/apache/iggy/issues/3545) and gates the Phase 1 end-to-end test
([#3539](https://github.com/apache/iggy/issues/3539)), which drives
`kafka-console-producer.sh`.

## The problem

A stock Java producer sets `enable.idempotence=true` without being asked. That default arrived
in Kafka 3.0 and took effect from 3.0.1, 3.1.1 and 3.2.0, where a bug that suppressed it was
fixed. `kafka-console-producer.sh` leaves it on.

An idempotent producer sends InitProducerId (key 22) before its first record. The gateway does
not list key 22, so ApiVersions does not advertise it, and the producer raises
`UnsupportedVersionException`. That exception is fatal.
`TransactionManager.maybeTransitionToErrorState` tests it above the `isTransactional()` branch.
The producer therefore enters a fatal error state instead of dropping back to weaker semantics.
It fails at startup, before it sends a record.

The gateway's stated purpose is that a Kafka user swaps the broker and changes no application
code. A broker that the default producer cannot start against does not meet it.

## What Iggy already deduplicates

Iggy deduplicates on the partition plane, and did so before this gateway existed. The key is
`(client_id, user_id, request)`. `client_id` is a `u128` the client generates, and `request` is a
monotonic counter on the client's session. Each partition group holds a per-client watermark with
a 128-bit `committed_window` under it. A request below the watermark with its bit clear is a
reordered arrival still to execute. A request below the window reads as committed.
`partition.dedup_clients_max` caps the distinct clients one partition tracks, at 4096 by default
and 65536 at the ceiling.

The shape is closer to Kafka's than it looks. A producer id with a per-partition sequence maps
onto a session id with a request id. The window is also deeper than the five batches a Kafka
producer keeps in flight.

What breaks the mapping is the hop each side protects. Iggy's covers gateway to Iggy. Kafka's
covers producer to gateway. A retrying producer sends a fresh Produce request, and the gateway
turns it into a fresh Iggy send carrying a fresh request id. Nothing recognises the replay.

## Closing that gap

Both halves of Iggy's key have to come from the Kafka request rather than from the SDK.

The client id is not a per-request field. `dispatch_partition_request` takes it from the
transport's bound session, and the header transmute at
`core/server/src/dispatch/partition.rs:247` overwrites whatever the SDK sent. Deriving it from a
Kafka producer id therefore needs one bound session per producer.

The request id is different. The transmute copies the header and replaces only `group`, `client`,
`session` and `user_id`, so a caller-supplied request id survives to the `ClientTable`.

One session per producer is a connection pool keyed by producer id. That is the pool the
README's "Concurrency ceiling" section already owes before
[#3535](https://github.com/apache/iggy/issues/3535) and
[#3536](https://github.com/apache/iggy/issues/3536).

Two SDK seams are missing for it:

- a caller-chosen client id at build time. `ConsensusSession::with_client_id` is public, but both
  construction sites build with `ConsensusSession::new()`
  (`core/sdk/src/tcp/tcp_client.rs:378` and `:583`)
- a caller-supplied request id. `send_raw_with_response` already takes
  `preencoded: Option<RequestHeader>` for transient replay, and it is private

None of this lands in this phase. Produce and Fetch do not need it, delivery is at-least-once
before and after, and the additions belong to whoever owns that SDK surface.

### Restart

A session registers with a fresh random client id per gateway process. A restarted gateway
therefore cannot collide with a watermark that outlived it, and a retry that spans a restart is
not deduplicated. That is still at-least-once, which the README states plainly.

The alternative is a stable client id derived from the producer id. It deduplicates across a
restart, and it is unsafe without also persisting the last request id per producer. Request ids
restarting at 1 under a live watermark read as duplicates, so fresh writes are discarded. Silent
loss is worse than duplicate delivery, so the fresh random id wins.

### Confirmed and not

The hop mismatch, one session per producer, and the restart choice are agreed in the maintainer
thread on [#3545](https://github.com/apache/iggy/issues/3545) and in Discord.

Two further readings are ours and are not confirmed yet. One session per producer is enough,
rather than one per producer and partition, because the `ClientTable` is per partition group. The
same request number on two partitions is two entries, so `request_id = base_sequence + 1` stays
monotonic inside each. And the gateway has to preserve per-partition ordering per producer,
because sequences advance by record count. A lower sequence arriving after a higher one lands
below the watermark, outside the window, and reads as committed.

## Options

| Option | Cost | What a stock producer does |
| -------- | ------ | ---------------------------- |
| Stub with `UNSUPPORTED_VERSION` | none | fails at startup unless the user sets `enable.idempotence=false` |
| Allocate only | about a day | works untouched, at-least-once delivery |
| Allocate and pool | weeks, blocked on the SDK | works untouched, retries absorbed on both hops |

## Decision

Allocate only, and defer the pool.

Rejecting the stock producer to avoid the pool trades away the one requirement the maintainers
named. It buys a guarantee nobody is asking for yet. Allocating costs about a day, leaves
delivery where it already is, and blocks nothing the pool later needs.

## Behavior

Add key 22 to `SUPPORTED_RANGES` in `src/protocol/api.rs` and advertise it through ApiVersions.
Without both, the producer never sends the request. `kafka-protocol` 0.18 carries the schemas,
request v0 to v5 and response v0 to v6, flexible from v2.

InitProducerId with no `transactional_id`:

- allocate the next producer id, return it with epoch 0 and error code 0
- build the id from an instance number in the high 16 bits and a counter in the low 47 bits,
  leaving bit 63 clear. `producer_id` is an `i64` and `-1` means no producer id, so the value has
  to stay non-negative. That leaves room for 65536 instances holding 140 trillion ids each

The instance number comes from configuration (`IGGY_KAFKA_INSTANCE_ID`, default 0), not from a
draw at startup. A random 16-bit number collides with even odds at around 300 instances. That is
a birthday collision, not a remote one.

The id is a pool key, not a dedup identity. Under the design above, the dedup identity is the
session's own random client id, minted at register. The producer id only decides which connection
serves a producer. Kafka still requires it to be unique across the cluster, which is what the
instance number buys. It does not have to survive a restart.

InitProducerId with a `transactional_id`:

- answer `UNSUPPORTED_VERSION` (35), unchanged. Transactions stay out of scope, and so do
  AddPartitionsToTxn (24), AddOffsetsToTxn (25), EndTxn (26) and TxnOffsetCommit (28)

35 rather than `INVALID_REQUEST` (42), because of the same fatal set quoted above.
`maybeTransitionToErrorState` holds ClusterAuthorization, TransactionalIdAuthorization,
ProducerFenced, UnsupportedVersion and InvalidPidMapping. `INVALID_REQUEST` is not in it, so a
transactional producer moves to an abortable error instead. The application is then told to abort
and retry something that can never succeed. `COORDINATOR_NOT_AVAILABLE` (15) is worse again. It
is retriable, so the producer never stops trying.

Produce:

- accept `producer_id`, `producer_epoch` and `base_sequence` on the request and ignore them
- never answer `OUT_OF_ORDER_SEQUENCE_NUMBER` (45) or `DUPLICATE_SEQUENCE_NUMBER` (46)

Those two codes stay unsent even once the pool lands. The watermark accepts any request above it
without noticing a gap, so a gap cannot be told apart from ordinary traffic. Sending either code
claims a detection the gateway does not have.

## What this does not give you

A producer that holds an id believes its retries are deduplicated. They are not. A retry after a
network timeout writes the record twice, and both copies reach the stream with their own offsets.

Iggy's own deduplication does not help, because it guards the other hop. Delivery through the
gateway is at-least-once until the pool lands, and at-least-once across a gateway restart after
that.

State that limitation in the README, next to the transaction section, in those words. Do not
leave a user to infer it from the presence of key 22.

## Open question

Allocate only, as above, or stub and document `enable.idempotence=false`?

If no answer lands by 2026-09-22, allocate only is taken and the work proceeds. This document
is then updated to record that it was decided by default.

## References

- Record mapping: [`BRIDGE_MAPPING.md`](BRIDGE_MAPPING.md), batch-level fields
- Scope and phases: [`SCOPE.md`](SCOPE.md)
- Version firewall: `src/protocol/api.rs`, `SUPPORTED_RANGES`
- Dedup key and window: `core/consensus/src/client_table.rs`
- Session identity: `core/sdk/src/session.rs`, `core/sdk/src/tcp/tcp_client.rs`
- Header rewrite: `core/server/src/dispatch/partition.rs`
- Fatal path: `TransactionManager.maybeTransitionToErrorState`, apache/kafka trunk
