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

## Options

| Option | Cost | What a stock producer does |
| -------- | ------ | ---------------------------- |
| Stub with `UNSUPPORTED_VERSION` | none | fails at startup unless the user sets `enable.idempotence=false` |
| Allocate only | about a day | works untouched, at-least-once delivery |
| Real deduplication | large | works untouched, exactly once per partition, single gateway instance only |

Real deduplication means tracking a sequence number per producer and per partition, and
rejecting a duplicate or a gap. It is correct only while one gateway instance sees every write
from a producer, so it cannot be decided before the multi-instance question is.

## Decision

Allocate only.

Rejecting the stock producer to avoid implementing deduplication trades the one requirement the
maintainers named against a guarantee Iggy does not offer today anyway. Allocating costs about
a day and keeps delivery exactly where it already is.

## Behavior

Add key 22 to `SUPPORTED_RANGES` in `src/protocol/api.rs` and advertise it through ApiVersions.
Without both, the producer never sends the request. `kafka-protocol` 0.18 carries the schemas,
request v0 to v5 and response v0 to v6, flexible from v2.

InitProducerId with no `transactional_id`:

- allocate the next producer id, return it with epoch 0 and error code 0
- draw ids from a counter seeded per gateway instance, so two instances never hand out the same
  id. Nothing reads the id today. Seeding it now is what stops a later deduplication layer from
  being born broken

InitProducerId with a `transactional_id`:

- answer `UNSUPPORTED_VERSION` (35), unchanged. Transactions stay out of scope, and so do
  AddPartitionsToTxn (24), AddOffsetsToTxn (25), EndTxn (26) and TxnOffsetCommit (28)

Produce:

- accept `producer_id`, `producer_epoch` and `base_sequence` on the request and ignore them
- never answer `OUT_OF_ORDER_SEQUENCE_NUMBER` (45) or `DUPLICATE_SEQUENCE_NUMBER` (46). The
  gateway tracks no sequences, so it cannot tell the two apart from ordinary traffic

## What this does not give you

A producer that has an id believes its retries are deduplicated. They are not. A retry after a
network timeout writes the record twice, and both copies reach the stream with their own
offsets.

Delivery through the gateway is at-least-once, with or without this change. The difference is
that the producer now starts.

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
- Fatal path: `TransactionManager.maybeTransitionToErrorState`, apache/kafka trunk
