# Client matrix results

Recorded from a full run of `client_matrix_tests.rs` (rootless Podman, Linux) on
2026-11-18 against `apache/kafka:4.3.1`. Only the nightly workflow publishes the
tables, to the job summary and the `client-matrix` artifact. The smoke clients
run on every pull request in the Rust test job without publishing. Re-run
locally with the command in [README.md](README.md) to refresh this file.

A cell is `pass` when every fact of that step on the gateway equals Apache
Kafka's; `fail, known (#issue)` when a difference is declared in
`allowed_differences.toml`; `n/a` when the client does not have that step.

## Smoke (every pull request)

| Client | Version | run | produce | consume | commit | resume | Seconds (gateway / kafka) |
| --- | --- | --- | --- | --- | --- | --- | --- |
| kcat | kcat 1.7.1, librdkafka 1.7.0 | pass | pass | pass | n/a | pass | 1 / 2 |
| confluent-kafka-python | confluent-kafka 2.16.0 (librdkafka 2.16.0) | pass | pass | pass | pass | pass | 2 / 2 |
| franz-go | franz-go 1.22.1 | pass | pass | pass | pass | pass | 0 / 1 |
| java-3.9 | kafka-clients 3.9.0 | pass | pass | pass | pass | pass | 2 / 3 |
| java-4.3 | kafka-clients 4.3.1 | pass | pass | pass | pass | pass | 3 / 3 |

kcat has no commit column: it commits on close but cannot print committed
offsets; its resume step shows the commit took effect.

## Nightly

| Client | Version | Steps | Result |
| --- | --- | --- | --- |
| Vector | 0.59.0 | run, produce, commit, resume, sink, source | pass in every step (12 s / 12 s) |
| Fluent Bit | 5.0.11 | run, produce, commit, resume, sink, source | pass in every step (16 s / 16 s) |
| RisingWave | 3.0.5, single node | sink, source, stream | pass in every step (11 s / 12 s) |
| Flink | 2.2.1, `flink-sql-connector-kafka` 5.0.0-2.2 | run, produce, sink (bounded copy, `AT_LEAST_ONCE`) | pass in every step (11 s / 11 s) |
| Kafka Connect + Debezium | Debezium 3.6.2.Final image, PostgreSQL 17.10 | start, snapshot, stream, restart | pass in every step (45 s / 38 s) |

## What the matrix does not cover yet

- Flink's offset commit on checkpoint (the bounded job finishes before a
  checkpoint) and its `EXACTLY_ONCE` sink (#4522).
- Clients with non-default settings: `enable.idempotence=true` on librdkafka,
  `group.protocol=consumer` (#4523), compression codecs, SASL and TLS (#4512).
- Kafka Streams, Schema Registry and Apicurio (#4508).
