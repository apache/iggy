# InitProducerId and idempotent producers

Status: implemented, [#3545](https://github.com/apache/iggy/issues/3545) for InitProducerId and
[#4513](https://github.com/apache/iggy/issues/4513) for writing each batch once. Gates the Phase 1
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

## How a resend is written once

An Iggy partition keeps the state of each idempotent producer that writes to it, as a Kafka log
does. The core SDK README has the rules, under "Idempotent producers". In short:

- Each batch carries the producer id, the epoch and the sequence of its first record.
- A partition keeps the last 5 batches of each producer, and at most 4096 producers.
- A partition drops a producer that wrote nothing for a day.
- Before the partition primary assigns offsets, it compares each batch with that state and with
  its uncommitted writes.

With `IGGY_KAFKA_IDEMPOTENT_PRODUCERS=true`, Produce passes the three fields of a Kafka batch to
Iggy unchanged. So every resend finds the first write: through any gateway, on any Iggy session,
and across a gateway restart.

The switch is off by default. Released gateways, and released consumers built on the Rust SDK,
refuse to read a batch that carries a producer. Upgrade every gateway and every consumer of the
topics, then turn the switch on for every gateway. While it is off, Produce drops the three
fields, and an idempotent producer gets at-least-once delivery.

Iggy's client table does not do this job. It guards the hop from the gateway to Iggy. It keys on a
request number that the Iggy client picks. A Kafka retry is a new Produce request. The gateway
sends it as a new Iggy request, with a new number. The producer state keys on the numbers that
the Kafka producer picks, so every resend carries the same ones.

## Behavior

Key 22 is in `SUPPORTED_RANGES` (`src/protocol/api.rs`) and therefore advertised through
ApiVersions. Without both, the producer never sends the request. `kafka-protocol` 0.18 carries
the schemas, request v0 to v5 and response v0 to v6, flexible from v2; the gateway serves v0 to
v5. The handler is `src/protocol/handlers/init_producer_id.rs`.

InitProducerId with no `transactional_id`, with the bridge:

- ask Iggy for the next producer id, and return it with epoch 0 and error code 0.
- Iggy builds the id from the session number of the bridge's id client and an index. No other
  client of the cluster gets that id, on any gateway and across restarts.
- a request that names its current id and epoch (KIP-360, v3 and later) also gets a new id. Kafka
  does the same for a producer without a transactional id.
- if Iggy cannot give an id, answer `COORDINATOR_LOAD_IN_PROGRESS` (14), which the Java producer
  retries. Not 15 or 16: those send it to look for a transaction coordinator, which this gateway
  has none of.
- one Iggy session hands out 2^20 ids. After that, the id client signs in again and gets a new
  range.

For a gap or a lost producer, an idempotent Java producer moves to the next epoch by itself. A new
epoch starts its sequences at 0. The producer asks for a new id only at epoch 32767.

Without the bridge, a local allocator gives the id. It puts an instance number in the high 16 bits
and a counter in the low 47 bits. Bit 63 stays clear, because `producer_id` is an `i64`, and `-1`
means no producer. The instance number comes from configuration (`IGGY_KAFKA_INSTANCE_ID`, else
`IGGY_KAFKA_NODE_ID`, else 0). The counter starts at the wall clock in milliseconds. So a restarted
gateway starts above the ids of its previous run. Without the bridge nothing is written, so these
ids only let a stock producer start.

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

- pass `producer_id`, `producer_epoch` and `base_sequence` of an idempotent batch to Iggy, when
  `IGGY_KAFKA_IDEMPOTENT_PRODUCERS` is `true`. A batch with a producer id but a negative epoch or
  sequence gets `INVALID_RECORD` (87).
- a resend of a batch that the partition holds answers the offset of its first write.
- answer `OUT_OF_ORDER_SEQUENCE_NUMBER` (45) for a gap, as Kafka does. The Java producer retries
  a 45. For a real gap, it moves to the next epoch.
- answer 45 also for a resend older than the 5 batches that the partition keeps.
- answer `INVALID_PRODUCER_EPOCH` (47) for a batch of an older epoch.
- answer `UNKNOWN_PRODUCER_ID` (59) for a batch above sequence 0 from a producer that the partition
  does not hold. That partition response carries `log_start_offset` 0. With a log start of `-1`,
  the Java producer retries a 59 unchanged up to its delivery timeout.
- a duplicate that is still uncommitted gets Iggy's `TransientNotCommitted`, because the first
  copy can still commit. The SDK retries it up to the deadline. Past it, the client gets a
  retriable 7.
- never answer `DUPLICATE_SEQUENCE_NUMBER` (46). A Kafka broker sends 45 for an old duplicate too.
- an id that this gateway gave out lately also picks the producer's Iggy session.
- reject a non-empty `transactional_id` (v3+) with `UNSUPPORTED_VERSION` (35), at the partition
  level, keeping the connection open. Under `acks=0` there is no response to carry 35, so the
  gateway closes the connection instead, as a Kafka broker does on any `acks=0` produce error.
  The check runs before the `acks=0` branch, so no write path can see a transactional batch.

The transactional refusal is where `maybeTransitionToErrorState` is exact. `Sender.completeBatch` ->
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

- A producer without idempotence still gets at-least-once delivery. A retry that follows a
  network timeout writes the record twice.
- A partition forgets a producer that wrote nothing for a day. Past 4096 producers, it also
  forgets the one idle the longest. A resend of an old batch of a forgotten producer can write it
  twice, as in Kafka.
- A resend older than the 5 batches that the partition keeps gets 45, as in Kafka.
- Released builds refuse a polled batch with a producer section: the gateway, the Rust SDK and
  every client that embeds it (the Python, C++ and PHP SDKs, the CLI and the connectors runtime),
  and the Swift SDK. Before you set `IGGY_KAFKA_IDEMPOTENT_PRODUCERS=true`, upgrade every gateway
  and every consumer of the topics.
- Transactions are not supported.

## Resolution

InitProducerId first only allocated ids, and delivery stayed at-least-once. The producer state in
Iggy partitions ([#4513](https://github.com/apache/iggy/issues/4513)) now writes each batch of an
idempotent producer once.

## References

- Record mapping: [`BRIDGE_MAPPING.md`](BRIDGE_MAPPING.md), batch-level fields
- Scope and phases: [`SCOPE.md`](SCOPE.md)
- Version firewall: `src/protocol/api.rs`, `SUPPORTED_RANGES`
- Producer state of a partition: `core/partitions/src/producer_state.rs`
- Producer ids from the session number: `core/sdk/src/session_owner/producer_ids.rs`
- The gateway's id client: `src/bridge/iggy_bridge/producer_ids.rs`
- Produce fatal path: `TransactionManager.maybeTransitionToErrorState`, apache/kafka trunk
- InitProducerId fatal path: `TransactionManager.InitProducerIdHandler.handleResponse`, same file
- librdkafka's fatal set: `rd_kafka_idemp_check_error`, `src/rdkafka_idempotence.c`
