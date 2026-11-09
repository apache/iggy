# Iggy PHP Examples

This directory contains sample applications that show how to use the Apache
Iggy PHP SDK extension.

## Running Examples

Start the server from the repository root:

```bash
cargo run --bin iggy-server -- --fresh --with-default-root-credentials
```

From `examples/php`, build the PHP extension and point PHP at it:

```bash
(cd ../../foreign/php && cargo build)
export PHP_IGGY_EXTENSION="$(pwd)/../../foreign/php/target/debug/libiggy_php.so"
```

On macOS, use `../../foreign/php/target/debug/libiggy_php.dylib` instead.

### Getting Started

```bash
php -d extension="${PHP_IGGY_EXTENSION:-../../foreign/php/target/debug/libiggy_php.so}" getting-started/producer.php
php -d extension="${PHP_IGGY_EXTENSION:-../../foreign/php/target/debug/libiggy_php.so}" getting-started/consumer.php
```

### Basic Usage

```bash
php -d extension="${PHP_IGGY_EXTENSION:-../../foreign/php/target/debug/libiggy_php.so}" basic/producer.php
php -d extension="${PHP_IGGY_EXTENSION:-../../foreign/php/target/debug/libiggy_php.so}" basic/consumer.php
```

### Sink Data Producer

The sink data producer generates JSON records and publishes them to an Apache Iggy stream and topic.
It creates the stream and topic if they do not exist, then publishes 100 batches of 100 to 499
records each. The stream and topic are retained after execution.

Each record contains: `user_id`, `user_type`, `email`, `source`, `state`, `created_at`,
and `message`.

Default stream: `qw` | Default topic: `records`

Environment variables:

- `IGGY_STREAM` - stream name (default: `qw`)
- `IGGY_TOPIC` - topic name (default: `records`)
- `IGGY_HOST` - server host (default: `127.0.0.1`)
- `IGGY_PORT` - server port (default: `8090`)
- `IGGY_USERNAME` - authentication username (default: `iggy`)
- `IGGY_PASSWORD` - authentication password (default: `iggy`)

The example uses `IGGY_CONNECTION_STRING` when it is set; otherwise it connects over TCP using
`IGGY_HOST` and `IGGY_PORT`, then logs in with `IGGY_USERNAME` and `IGGY_PASSWORD`.
Defaults are `127.0.0.1:8090` and `iggy`/`iggy`.

Build the extension from `foreign/php`:

```bash
(cd ../../foreign/php && cargo build)
```

Execute the producer from `examples/php`:

```bash
# Default stream and topic
php -d extension="${PHP_IGGY_EXTENSION:-../../foreign/php/target/debug/libiggy_php.so}" sink-data-producer/producer.php

# With custom stream and topic
IGGY_STREAM=test-stream IGGY_TOPIC=test-topic php -d extension="${PHP_IGGY_EXTENSION:-../../foreign/php/target/debug/libiggy_php.so}" sink-data-producer/producer.php
```
