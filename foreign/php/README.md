# iggy-php

PHP extension bindings for [Apache Iggy](https://iggy.apache.org/), built in Rust with
[`ext-php-rs`](https://github.com/davidcole1340/ext-php-rs).

This repository is experimental. The extension exposes two PHP clients over the Rust
Iggy client:

- `IggyClient`: blocking, synchronous PHP API.
- `IggyAsyncClient`: Fiber/Revolt-aware API powered by `php-tokio`.

## Requirements

- Rust and Cargo
- PHP with `php-config`
- `cargo-php`
- Composer, for async test dependencies
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

### Synchronous Client

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

### Async Client

`IggyAsyncClient` integrates Tokio with PHP Fibers through
[`php-tokio`](https://github.com/danog/php-tokio) and Revolt. The methods look like
normal PHP calls, but they suspend the current Fiber while the Rust future is pending.

Install the PHP event-loop dependencies:

```sh
composer install
```

Register the file descriptor returned by `IggyAsyncClient::init()` with Revolt and call
`IggyAsyncClient::wakeup()` when it becomes readable:

```php
<?php

require __DIR__ . '/vendor/autoload.php';

use Revolt\EventLoop;
use function Amp\async;

$fd = IggyAsyncClient::init();
$readable = fopen('php://fd/' . $fd, 'r');
stream_set_blocking($readable, false);

$watcher = EventLoop::onReadable(
    $readable,
    static fn () => IggyAsyncClient::wakeup(),
);

try {
    $future = async(function () {
        $client = new IggyAsyncClient('127.0.0.1:8090');
        $client->connect();
        $client->loginUser('iggy', 'iggy');
        $client->ping();

        return 'pong';
    });

    echo $future->await(), PHP_EOL;
} finally {
    EventLoop::cancel($watcher);
    fclose($readable);
    IggyAsyncClient::shutdown();
}
```

The async client covers the core connection, stream, topic, send, poll, and
consumer-group operations. `IggyAsyncConsumer::iterMessages()->next()` and
`IggyAsyncConsumer::consumeMessages()` suspend the current Fiber while waiting for
messages.

## Tests

Run the Dockerized integration suite:

```sh
docker compose -f docker-compose.test.yml up --build --abort-on-container-exit --exit-code-from php-tests
```

Run the PHP test suite:

```sh
php tests/run.php
```

Async tests require Composer dependencies:

```sh
composer install
php tests/run.php IggyAsyncSdkTest
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
- `IggyAsyncClient` is Fiber/Revolt-aware. Its methods must be called after
  `IggyAsyncClient::init()` and from a Fiber-aware runtime such as Amp/Revolt.
