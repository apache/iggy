# InitProducerId and idempotent producers

Status: implemented, [#3545](https://github.com/apache/iggy/issues/3545). Gates the Phase 1
end-to-end test ([#3539](https://github.com/apache/iggy/issues/3539)), which drives
`kafka-console-producer.sh`.

## The problem

A stock Java producer sets `enable.idempotence=true` without being asked. That default arrived
in Kafka 3.0 and took effect from 3.0.1, 3.1.1 and 3.2.0, where a bug that suppressed it was
fixed. `kafka-console-producer.sh` leaves it on.

An idempotent producer sends InitProducerId (key 22) before its first record. A gateway that
does not list key 22 does not advertise it either, and the producer raises
`UnsupportedVersionException` rather than dropping back to weaker semantics. It fails at
startup, before it sends a record.

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

Key 22 is in `SUPPORTED_RANGES` (`src/protocol/api.rs`) and therefore advertised through
ApiVersions. Without both, the producer never sends the request. `kafka-protocol` 0.18 carries
the schemas, request v0 to v5 and response v0 to v6, flexible from v2; the gateway serves v0 to
v5. The handler is `src/protocol/handlers/init_producer_id.rs`.

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
instance number buys. It also has to stay unique across a restart once the pool lands or
Produce persists, because `producer_epoch` is always 0, so a replayed id is a replayed
`(producer_id, producer_epoch)` pair a live producer may still hold. The counter therefore starts
at the wall clock in milliseconds rather than at 0. A restarted gateway starts above every id its
previous run handed out unless that run averaged more than one allocation per millisecond of its
uptime, or the clock stepped back across the restart. A clock that reads before the Unix epoch
seeds 0 and replays from the bottom. 2^47 milliseconds leaves the counter space thousands of years
from running out.

Nothing is persisted for this, so the clock seed narrows the replay window without closing it. That
is harmless while no code path reads the producer id. It stops being harmless once one does:
[#3535](https://github.com/apache/iggy/issues/3535) must first either persist a high-water mark
and seed the counter above it, or bump `producer_epoch` on every gateway start. Either one makes a
replayed `(producer_id, producer_epoch)` pair impossible rather than unlikely.

`IGGY_KAFKA_INSTANCE_ID` defaults to 0 so a single gateway needs no configuration. Two gateways
left on the default draw from the same 47-bit space and can hand out identical ids, on the first
allocation if they start in the same millisecond. Nothing in the cluster detects that, so a gateway started without the
variable logs a warning, and the running value is in the startup log line.

An empty `transactional_id` reads as absent. A wire null decodes to `None`, but
`kafka-protocol`'s own `Default` is `Some("")`, and a producer that is idempotent-only names no
transaction either way.

InitProducerId with a `transactional_id`:

- answer `UNSUPPORTED_VERSION` (35). Transactions stay out of scope, and so do
  AddPartitionsToTxn (24), AddOffsetsToTxn (25), EndTxn (26) and TxnOffsetCommit (28)

Not for the reason the Produce path uses. `maybeTransitionToErrorState` governs a failed Produce
batch and never sees an InitProducerId response; those reach
`InitProducerIdHandler.handleResponse`, whose trailing `else` is `fatalError(new
KafkaException("Unexpected error in InitProducerIdResponse; ..."))`. Anything it does not
recognise is fatal there, so `INVALID_REQUEST` (42) would be equally fatal and the "42 is
abortable, therefore 35" argument does not apply to this API. 35 is chosen for consistency with
the Produce guard below, and because it is the one code that also states the truth: the gateway
does not implement this version of the transactional protocol. What must be avoided is a
*retriable* code: that same handler re-enqueues `COORDINATOR_LOAD_IN_PROGRESS` (14) and
`CONCURRENT_TRANSACTIONS` (51), so the producer would never stop trying.

No single response code is terminal on both target clients. 35 is fatal for the Java producer
and an infinite retry for librdkafka, whose `rd_kafka_idemp_check_error` treats only
`__UNSUPPORTED_FEATURE`, `INVALID_TRANSACTION_TIMEOUT` (50), 53 and 31 as fatal. 35 stays the
choice, and librdkafka is stopped at FindCoordinator instead: key 10 is unadvertised today, so
`rd_kafka_init_transactions()` fails before InitProducerId is reached. Phase 3 advertises key 10
for consumer groups and has to refuse a `TXN`-type coordinator lookup explicitly.

Produce:

- accept `producer_id`, `producer_epoch` and `base_sequence` on the request and ignore them
- never answer `OUT_OF_ORDER_SEQUENCE_NUMBER` (45) or `DUPLICATE_SEQUENCE_NUMBER` (46)
- reject a non-empty `transactional_id` (v3+) with `UNSUPPORTED_VERSION` (35), at the partition
  level, keeping the connection open. Under `acks=0` there is no response to carry 35, so the
  gateway closes the connection instead, as a Kafka broker does on any `acks=0` produce error.
  The check runs before the `acks=0` branch, so no write path can see a transactional batch

Those first two codes stay unsent even once the pool lands. The watermark accepts any request
above it without noticing a gap, so a gap cannot be told apart from ordinary traffic. Sending
either code claims a detection the gateway does not have.

The third is where `maybeTransitionToErrorState` is exact. `Sender.completeBatch` ->
`canRetry` false -> `failBatch` -> `handleFailedBatch` -> `maybeTransitionToErrorState`, whose
explicit fatal set holds ClusterAuthorization, TransactionalIdAuthorization, ProducerFenced,
UnsupportedVersion and InvalidPidMapping. `INVALID_TXN_STATE` (48) is explicitly rewritten to
abortable there, and `INVALID_REQUEST` (42) and `UNSUPPORTED_FOR_MESSAGE_FORMAT` (43) fall
through to abortable, so any of those would tell the application to abort and retry something
that can never succeed.

Without this guard a transactional batch would land as ordinary records once
[#3535](https://github.com/apache/iggy/issues/3535) wires the bridge: no last stable offset, no
abort markers, `read_committed` unimplementable, and an aborted transaction's records delivered
to every consumer.

The guard reads the request-level `transactional_id` only. A record batch also carries a
transactional bit in its attributes, and the stub keeps records opaque, so a hand-built frame
that sets the bit without the request field gets the retriable stub error (6). Java and
librdkafka set both, so no stock client reaches it, and the stub does not parse batches to close
it. [#3535](https://github.com/apache/iggy/issues/3535) decodes each batch before persisting it,
and must refuse one with the transactional bit set there, the same way.

## Invariants this design rests on

Both are absences. Losing either is caught: `golden_wire_fixtures_tests.rs` pins the ApiVersions
v1 and v3 bodies byte-exactly, so adding a finalized feature or any advertised key fails both
goldens.

**Never advertise `transaction.version >= 2` in the ApiVersions `finalized_features`.**
`TransactionManager.maybeUpdateTransactionV2Enabled` reads it, and under TV2 `maybeAddPartition`
adds partitions client-side, so AddPartitionsToTxn and AddOffsetsToTxn are never sent at all.
That collapses the absence gate this design depends on. `api_versions::encode_response` builds
an `ApiVersionsResponse` with no finalized features, which is correct and load-bearing.

**The transactional handshake order is fixed**: FindCoordinator(TXN) -> InitProducerId ->
AddPartitionsToTxn. While FindCoordinator (key 10) stays unadvertised, `initTransactions()`
already dies before InitProducerId is reached, and the InitProducerId branch is belt and braces.
Advertising FindCoordinator for consumer groups
([#3541](https://github.com/apache/iggy/issues/3541)) removes that shield, and is what makes the
branch load-bearing.

## What this does not give you

A producer that holds an id believes its retries are deduplicated. They are not. A retry after a
network timeout writes the record twice, and both copies reach the stream with their own offsets.

Iggy's own deduplication does not help, because it guards the other hop. Delivery through the
gateway is at-least-once until the pool lands, and at-least-once across a gateway restart after
that.

The README states that limitation under "Delivery guarantees", so a user does not have to infer
it from the presence of key 22.

## Resolution

The open question was allocate only, as above, against stub and document
`enable.idempotence=false`. Allocate only was taken, which this document named as the default
outcome, and is what shipped.

## References

- Record mapping: [`BRIDGE_MAPPING.md`](BRIDGE_MAPPING.md), batch-level fields
- Scope and phases: [`SCOPE.md`](SCOPE.md)
- Version firewall: `src/protocol/api.rs`, `SUPPORTED_RANGES`
- Dedup key and window: `core/consensus/src/client_table.rs`
- Session identity: `core/sdk/src/session.rs`, `core/sdk/src/tcp/tcp_client.rs`
- Header rewrite: `core/server/src/dispatch/partition.rs`
- Produce fatal path: `TransactionManager.maybeTransitionToErrorState`, apache/kafka trunk
- InitProducerId fatal path: `TransactionManager.InitProducerIdHandler.handleResponse`, same file
- librdkafka's fatal set: `rd_kafka_idemp_check_error`, `src/rdkafka_idempotence.c`
