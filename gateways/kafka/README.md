# Kafka gateway (`iggy_gateway_kafka`)

Foundation layer for [apache/iggy#3421](https://github.com/apache/iggy/issues/3421): a TCP listener on the Kafka wire port that decodes requests, validates scoped API keys and versions, and returns stub responses.

## Run

```bash
cargo run -p iggy_gateway_kafka --bin iggy-kafka-gateway
```

Default bind: `127.0.0.1:9093`.

## Test

```bash
cargo test -p iggy_gateway_kafka
```

`decode_validation_tests` require wire fixtures under `tools/kafka-tool/kafka_messages/`:

```bash
cargo run -p kafka-message-gen -- generate \
  --output gateways/kafka/tools/kafka-tool/kafka_messages \
  --api-key 0 --api-key 1 --api-key 2 --api-key 19
```

(Run from workspace root; adjust paths if needed.)

## Scoped APIs

See [docs/SCOPE.md](docs/SCOPE.md).

## Wire fixture tool

See [tools/kafka-tool/README.md](tools/kafka-tool/README.md).
