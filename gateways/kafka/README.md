# Kafka gateway (`iggy-gateway-kafka`)

Foundation layer for [apache/iggy#3421](https://github.com/apache/iggy/issues/3421): a TCP listener on the Kafka wire port that decodes requests, validates scoped API keys and versions, and returns stub responses.

> **Stub warning:** Produce does **not** persist records. Valid Produce requests return retriable `NOT_LEADER_OR_FOLLOWER` (6) so clients keep data locally. CreateTopics does **not** create topics; valid requests return `NOT_CONTROLLER` (41). Metadata still reports requested topics as unknown. Persistence lands with the Iggy bridge (see [docs/SCOPE.md](docs/SCOPE.md)).

## Run

```bash
cargo run -p iggy-gateway-kafka
```

Default bind: `127.0.0.1:9093`. Environment variables:

| Variable | Default | Description |
| --- | --- | --- |
| `KAFKA_BIND_ADDR` | `127.0.0.1:9093` | TCP address to listen on |
| `KAFKA_ADVERTISED_HOST` | bind IP | Hostname/IP clients use to reach this broker (required when binding to `0.0.0.0`/`::`) |
| `KAFKA_ADVERTISED_PORT` | bind port | Port advertised in Metadata responses |

## Test

```bash
cargo test -p iggy-gateway-kafka
```

103 regression tests across 12 suites — see [docs/TEST_SUITE.md](docs/TEST_SUITE.md) for the full catalog.

`decode_validation_tests` require wire fixtures under `tools/kafka-tool/kafka_messages/` (gitignored locally; CI generates them via `scripts/ci-wire-fixtures.sh`):

```bash
./gateways/kafka/scripts/ci-wire-fixtures.sh generate
cargo test -p iggy-gateway-kafka
./gateways/kafka/scripts/ci-wire-fixtures.sh cleanup   # optional
```

Or generate only the keys the tests need:

```bash
for key in 0 1 2 19; do
  cargo run -p kafka-message-gen -- generate \
    --output gateways/kafka/tools/kafka-tool/kafka_messages \
    --api-key "$key"
done
```

## Manual testing

Before check-in, run the procedure in [docs/MANUAL_TESTING.md](docs/MANUAL_TESTING.md) (smoke, version firewall, kcat, adversarial cases).

## Scoped APIs

See [docs/SCOPE.md](docs/SCOPE.md) for [#3421](https://github.com/apache/iggy/issues/3421) deliverables, supported API key/version table, and post-foundation TODO backlog.

## Wire fixture tool

See [tools/kafka-tool/README.md](tools/kafka-tool/README.md).
