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

### Message Envelopes

The message envelope pattern wraps each payload in a JSON object with a
`message_type` and a `payload` string, so one topic can carry several kinds of
messages. The producer sends `OrderCreated`, `OrderConfirmed` and
`OrderRejected` messages, and the consumer dispatches on `message_type`. A
message with an unknown type is logged and skipped.

```bash
php -d extension="${PHP_IGGY_EXTENSION:-../../foreign/php/target/debug/libiggy_php.so}" message-envelope/producer.php
php -d extension="${PHP_IGGY_EXTENSION:-../../foreign/php/target/debug/libiggy_php.so}" message-envelope/consumer.php
```

The examples use `IGGY_CONNECTION_STRING` when it is set; use a binary transport
with credentials for automatic login. Otherwise they connect over TCP using
`IGGY_HOST` and `IGGY_PORT`, then log in with `IGGY_USERNAME` and `IGGY_PASSWORD`
without URL encoding. Defaults are `127.0.0.1:8090` and `iggy`/`iggy`.
