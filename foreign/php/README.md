# iggy-php

PHP extension bindings for [Apache Iggy](https://iggy.apache.org/), built in Rust with
[`ext-php-rs`](https://github.com/davidcole1340/ext-php-rs).

This repository is experimental. The extension exposes `IggyClient`, a blocking
synchronous PHP API over the Rust Iggy client.

## Requirements

- Rust and Cargo
- PHP with `php-config`
- `cargo-php`
- Composer, for installing PHPUnit
- Docker, for running the integration test server

On macOS with Homebrew PHP:

```sh
export PATH="/opt/homebrew/opt/php/bin:$PATH"
export PHP=/opt/homebrew/opt/php/bin/php
export PHP_CONFIG=/opt/homebrew/opt/php/bin/php-config
```

## Build

```sh
cargo build --release
```

## Install

```sh
cargo php install --release --yes
```

If the extension is already enabled, reinstall it with:

```sh
cargo php remove --yes
cargo php install --release --yes
```

Verify PHP can load it:

```sh
php -r 'var_dump(extension_loaded("iggy-php"));'
```

## Run Iggy

```sh
docker run --rm --name iggy-php-test \
  -p 8090:8090 \
  -p 3000:3000 \
  apache/iggy:latest
```

The tests assume:

- host: `127.0.0.1`
- port: `8090`
- username: `iggy`
- password: `iggy`

Override them with `IGGY_HOST`, `IGGY_PORT`, `IGGY_USERNAME`, and `IGGY_PASSWORD`.

## Usage

```php
<?php

$client = new IggyClient('127.0.0.1:8090');
$client->connect();
$client->loginUser('iggy', 'iggy');

$stream = 'php-stream';
$topic = 'php-topic';
$partitionId = 0;

$client->createStream($stream);
$client->createTopic($stream, $topic, 1, null, null, null, null);

$client->sendMessages($stream, $topic, $partitionId, [
    new SendMessage('hello from PHP'),
]);

$messages = $client->pollMessages(
    $stream,
    $topic,
    $partitionId,
    PollingStrategy::first(),
    10,
    true,
);

foreach ($messages as $message) {
    echo $message->payload(), PHP_EOL;
}
```

## Tests

Run the Dockerized integration suite:

```sh
docker compose -f docker-compose.test.yml up --build --abort-on-container-exit --exit-code-from php-tests
```

Run the PHP test suite:

```sh
composer install
composer test
```

Run Rust verification:

```sh
cargo test
```

TLS tests are opt-in because they require a TLS-enabled Iggy server and certificate
setup. Set `IGGY_TLS_CONNECTION_STRING` to enable TLS connection tests. Set
`IGGY_TLS_PLAINTEXT_ADDRESS` to run the negative plaintext-to-TLS test.

## API Notes

- Methods are exposed to PHP as camelCase, for example `createStream()` and
  `pollMessages()`.
- Partition IDs use the Iggy partition index. For a topic with one partition, use `0`.
- Large unsigned values that can overflow PHP integers, such as message checksums,
  are returned as decimal strings.
- `IggyClient` is synchronous and blocks the current PHP thread.
