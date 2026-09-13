# iggy-php

PHP extension bindings for [Apache Iggy](https://iggy.apache.org/), built in Rust with
[`ext-php-rs`](https://github.com/davidcole1340/ext-php-rs).

This repository is experimental. The Rust Iggy SDK is async and Tokio-based, but
this extension exposes `Iggy\Client` as a blocking synchronous PHP API. Each call
drives the lazy global Tokio runtime and blocks the calling PHP thread until the
future resolves; it does not provide fiber-aware or non-blocking I/O.

## Requirements

- Rust and Cargo
- PHP 8.3 or newer with `php-config` (non-thread-safe builds only, no ZTS)
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
cargo build
cargo php stubs target/debug/libiggy_php.so -o /tmp/iggy-php.stubs.php
```

Stub generation requires a debug build. On macOS, use `libiggy_php.dylib`.
Preserve the existing Apache license header when updating `iggy-php.stubs.php`.
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

Use Iggy 0.9.0. When testing unreleased SDK changes, build the server from the
same source checkout.

```sh
docker run --rm --name iggy-php-test \
  --cap-add=SYS_NICE --security-opt seccomp=unconfined --ulimit memlock=-1:-1 \
  -p 8090:8090 \
  -p 3000:3000 \
  -e IGGY_TCP_ADDRESS=0.0.0.0:8090 -e IGGY_HTTP_ADDRESS=0.0.0.0:3000 \
  -e IGGY_NODE_ADVERTISED_ADDRESS=localhost \
  -e IGGY_ROOT_USERNAME=iggy -e IGGY_ROOT_PASSWORD=iggy \
  apache/iggy:0.9.0
```

You can also run a local server from the repository root:

```sh
cargo run --bin iggy-server -- --fresh --with-default-root-credentials
```

The root variables bootstrap a new data directory; they do not replace stored
credentials. Environment credentials override `--with-default-root-credentials`.
Use `--fresh` only with disposable development data: it deletes local replica state.

The tests assume:

- host: `127.0.0.1`
- port: `8090`
- username: `iggy`
- password: `iggy`

Override them with `IGGY_TCP_ADDRESS`, `IGGY_ROOT_USERNAME`, and `IGGY_ROOT_PASSWORD`.

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
    new \Iggy\SendMessage('hello from PHP 1'),
    new \Iggy\SendMessage('hello from PHP 2'),
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

Consumer group callbacks require a finite message limit. The partition id
argument is ignored for a consumer group, since the member reads the partitions
the server assigns to it. After the example above, each loop below consumes one
of its two messages:

```php
<?php

$consumer = $client->consumerGroup(
    'php-consumer',
    $stream,
    $topic,
    null,
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
        echo $message->payload(), PHP_EOL;
        $consumer->storeOffset($message->offset(), $message->partitionId());
    },
    1,
);

foreach ($consumer->iterMessages() as $message) {
    echo $message->payload(), PHP_EOL;
    $consumer->storeOffset($message->offset(), $message->partitionId());

    break;
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

Run Rust verification with a matching PHP embedding library (`libphp`) available
to the linker and dynamic loader:

```sh
cargo test --features ext-php-rs/embed
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
- Message IDs and checksums are returned as decimal strings. Offset and timestamp
  getters return PHP integers and are limited to `PHP_INT_MAX`.
- `Iggy\Client::sendBinaryRequest(int $code, string $payload): string` sends a
  command code and payload and returns the raw response body.
- `Iggy\Client` is synchronous and blocks the current PHP thread.
- The extension owns a lazy global Tokio runtime. Do not call `pcntl_fork()` after
  the first Iggy SDK call; the child process inherits file descriptors but not
  Tokio worker threads. Runtime initialization failure is unrecoverable and aborts
  extension use.
