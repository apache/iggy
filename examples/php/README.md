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

The examples use `IGGY_CONNECTION_STRING` when it is set. Otherwise they connect
to `iggy+tcp://iggy:iggy@127.0.0.1:8090`.
