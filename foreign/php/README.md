# iggy-php

PHP extension bindings for [Apache Iggy](https://iggy.apache.org/), built in Rust with
[`ext-php-rs`](https://github.com/davidcole1340/ext-php-rs).

This repository is experimental. The Rust Iggy SDK is async and Tokio-based, but
this extension exposes `Iggy\Client` as a blocking synchronous PHP API. Each call
drives the lazy global Tokio runtime and blocks the calling PHP thread until the
future resolves; it does not provide fiber-aware or non-blocking I/O.

## Requirements

- Rust and Cargo
- PHP 8.3 or newer with `php-config`
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

Generate IDE stubs after changing the exported PHP API:

```sh
cargo php stubs --manifest Cargo.toml -o iggy-php.stubs.php
```

The CI lint job regenerates this file and fails if the checked-in stubs drift
from the Rust signatures.

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

You can also run a local server from the repository root:

```sh
cargo run --bin iggy-server --fresh --with-default-root-credentials
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

$client = new \Iggy\Client('127.0.0.1:8090');
$client->connect();
$client->loginUser('iggy', 'iggy');

$stream = 'php-stream';
$topic = 'php-topic';
$partitionId = 0;

$client->createStream($stream);
$client->createTopic($stream, $topic, 1, null, null, null, null);

$client->sendMessages($stream, $topic, $partitionId, [
    new \Iggy\SendMessage('hello from PHP'),
]);

$messages = $client->pollMessages(
    $stream,
    $topic,
    $partitionId,
    \Iggy\PollingStrategy::first(),
    10,
    true,
);

foreach ($messages as $message) {
    echo $message->payload(), PHP_EOL;
}
```

Consumer group callbacks require a finite message limit:

```php
<?php

$consumer = $client->consumerGroup(
    'php-consumer',
    $stream,
    $topic,
    $partitionId,
    \Iggy\PollingStrategy::next(),
    10,
    \Iggy\AutoCommit::disabled(),
    true,
    true,
    1_000_000,
    null,
    null,
    null,
    false,
);

$consumer->consumeMessages(
    function (\Iggy\ReceiveMessage $message) use ($consumer): void {
        process($message->payload());
        $consumer->storeOffset($message->offset(), $message->partitionId());
    },
    100,
);

foreach ($consumer->iterMessages() as $message) {
    process($message->payload());
    $consumer->storeOffset($message->offset(), $message->partitionId());

    if (shouldStop()) {
        break;
    }
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

TLS connection strings use the Rust SDK connection-string format, for example:

```text
iggy+tcp://iggy:iggy@127.0.0.1:8090?tls=true&tls_domain=localhost&tls_ca_file=/path/to/ca.pem
```

## API Notes

- Methods are exposed to PHP as camelCase, for example `createStream()` and
  `pollMessages()`.
- Classes live in the `Iggy` namespace, for example `Iggy\Client` and
  `Iggy\SendMessage`.
- Partition IDs use the Iggy partition index. For a topic with one partition, use `0`.
- Passing `null` as the partition to `storeOffset()` or `deleteOffset()` uses the
  current consumer partition, and is rejected until at least one message has been
  polled. Pass an explicit partition id before the first poll.
- `consumeMessages()` requires an explicit finite limit. It does not run forever
  by default.
- `iterMessages()` returns a PHP `Iterator` and can be used with `foreach`.
  Break out of the loop when the caller's processing limit or shutdown signal is reached.
- `AutoCommit::when()` may queue an offset commit before the PHP callback runs.
  If callback success must control commits, use `AutoCommit::disabled()` and call
  `storeOffset()` after the callback work succeeds.
- `Iggy\PollingStrategy::timestamp()` and `Iggy\PollingStrategy::timestampMicros()`
  expect microseconds since the Unix epoch. Use
  `Iggy\PollingStrategy::timestampSeconds()` for PHP `time()` values.
- PHP strings are passed as named identifiers, including strings that contain
  only digits. PHP integers are passed as numeric identifiers.
- `Iggy\SendMessage::payload` and `Iggy\ReceiveMessage::payload()` copy the payload
  bytes into a PHP string on each read. Cache large payloads in PHP if they will
  be read repeatedly.
- Large unsigned values that can overflow PHP integers, such as message checksums,
  are returned as decimal strings.
- `Iggy\Client::sendBinaryRequest(int $code, string $payload): string` sends a
  command code and payload and returns the raw response body.
- `Iggy\Client` is synchronous and blocks the current PHP thread.
- The extension owns a lazy global Tokio runtime. Do not call `pcntl_fork()` after
  the first Iggy SDK call; the child process inherits file descriptors but not
  Tokio worker threads. Runtime initialization failure is unrecoverable and aborts
  extension use.
