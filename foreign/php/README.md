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

Stub generation requires a debug build and cargo-php 0.2.1, the version CI installs. Older
cargo-php releases cannot load the extension. On macOS, use `libiggy_php.dylib`.
The checked-in `iggy-php.stubs.php` is the generated file with the Apache license header
in place of its first line and the `= null` default removed from every `public readonly
?string` property, which PHP refuses on readonly properties. The CI lint job
(`.github/actions/php/pre-merge/action.yml`) applies the same post-processing and fails
if the checked-in stubs drift from the Rust signatures, so copy its commands when
regenerating locally.

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
$consumerId = 1;

$client->createStream($stream);
$client->createTopic($stream, $topic, 1, null, null, null, null);

$client->sendMessages($stream, $topic, $partitionId, [
    new \Iggy\SendMessage('hello from PHP 1'),
    new \Iggy\SendMessage('hello from PHP 2'),
]);

$polled = $client->pollMessages(
    $stream,
    $topic,
    $partitionId,
    $consumerId,
    \Iggy\PollingStrategy::first(),
    new \Iggy\PollOptions(count: 10),
);

foreach ($polled->messages as $message) {
    echo $message->payload(), PHP_EOL;
}

$next = $client->pollMessages($stream, $topic, $partitionId, $consumerId, $polled->continuation());
```

Consumer group callbacks require a finite message limit. The member reads the
partitions that the server assigns to it. After the example above, each loop
below consumes one of its two messages:

```php
<?php

$consumer = $client->consumerGroup(
    'php-consumer',
    $stream,
    $topic,
    \Iggy\PollingStrategy::next(),
);

$consumer->consumeMessages(
    function (\Iggy\ReceiveMessage $message) use ($consumer): void {
        echo $message->payload(), PHP_EOL;
        $consumer->commit($message->checkpoint());
    },
    1,
);

foreach ($consumer->iterMessages() as $message) {
    echo $message->payload(), PHP_EOL;
    $consumer->commit($message->checkpoint());

    break;
}

$consumer->shutdown();
```

A reader returns whole batches of one partition, and it reads ahead in the
background:

```php
<?php

$reader = $client->reader($stream, $topic, 'php-reader', [$partitionId], \Iggy\PollingStrategy::first());

try {
    $batch = $reader->next();
    foreach ($batch->messages as $message) {
        echo $message->payload(), PHP_EOL;
    }
    $reader->commit($batch->checkpoint());
} finally {
    $reader->close();
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

## Resource Options

Streams and topics carry options. A read returns two arrays of option name to value: `options` holds what the creating or updating client set, `derived_options` what the server filled in. Integers above `PHP_INT_MAX`, such as the never-expire and unlimited value 18446744073709551615, are decimal strings, and the 64-bit topic arguments take an int or a decimal string.

```php
$client->createStream('orders', base64_encode(random_bytes(32)));
$stream = $client->getStream('orders');
echo $stream->encryption_key_source, ' ', $stream->encryption_key_fingerprint, PHP_EOL;

$client->createTopic('orders', 'events', 1, 'gzip', 3_600_000_000, null, null,
    partition_resize_policy: 'fixed');
$client->updateTopic('orders', 'events', message_expiry_micros: '18446744073709551615',
    reset: [Iggy\TopicOptionKey::MaxTopicSize]);
print_r($client->getTopic('orders', 'events')->options);

foreach ($client->describeOptions(Iggy\OptionsScope::Topic) as $spec) {
    echo $spec->key, $spec->updatable ? ' updatable' : '', $spec->resettable ? ' resettable' : '', PHP_EOL;
}
```

- The stream encryption key is the stream's own data key: 32 random bytes, passed as canonical padded standard base64. Raw bytes or any other text are refused as `Invalid encryption key` (code 70) before anything is sent. The key travels in the create request, so protecting it in transit with TLS is the caller's job. The server stores it wrapped and never returns it.

  The key, the `loginUser()` password and the `fromConnectionString()` string are marked `#[\SensitiveParameter]`, so exception traces hold a `SensitiveParameterValue` in their place. The `raw` arrays of `createStream()`, `updateStream()`, `createTopic()` and `updateTopic()` are marked as well, because an unknown key may hold a secret.

  Reads report only `encryption_key_source` (`stream`, `server`, or `none` for a plaintext stream), `encryption_key_fingerprint` and `encryption_kek_fingerprint`, comma-separated during a server key rotation. `Iggy\Client::encryptionKeyFingerprint($key)` computes the fingerprint of a key you hold. Without a key, a server with encryption enabled generates one.
- `updateStream()` renames a stream and `updateTopic()` renames a topic or changes `message_expiry_micros` and `max_topic_size`. Null arguments keep stored values. `reset` takes `Iggy\StreamOptionKey` or `Iggy\TopicOptionKey` cases and returns keys to their server defaults. Only the topic's message expiry and size cap are resettable, every other case is refused with the server's error.
- `raw` on create and update sends string-valued keys this build does not know, and `raw_reset` resets keys by name. The server decides whether each one is accepted. A name listed twice across `reset` and `raw_reset` is refused as `Invalid command`.
- `describeOptions(Iggy\OptionsScope)` returns the server catalog as `Iggy\OptionSpec` objects with `key`, `kind`, `updatable`, `resettable`, `secret`, `read_only`, `critical`, `default_value` and `description`.
- SDK exceptions carry the server error code in `getCode()`, for example 4103 (option cannot be updated), 4104 (option cannot be reset), 4105 (read-only option), 4108 (root user cannot be changed by this caller) and 4041 (unsupported option key).

## Metadata Changes

`getMetadataChanges()` reads the node's change log: one record per public result of a committed metadata operation, such as a stream created, a topic renamed, a user's permissions changed or a session closed. A mirror keeps a cursor, asks for the records after it, and stores the returned `next` cursor. The call needs root or the global `manage_servers` permission, and a query the server would refuse throws `Invalid command` (code 3) before anything is sent.

```php
use Iggy\MetadataChangeGroup;
use Iggy\MetadataChangeStart;
use Iggy\MetadataChangeStatus;

function follow(Iggy\Client $client, array $groups): void
{
    $start = MetadataChangeStart::earliest();
    while (true) {
        $page = $client->getMetadataChanges($start, $groups);
        switch ($page->status) {
            case MetadataChangeStatus::Ok:
                foreach ($page->records as $record) {
                    echo $record->op, ' ', $record->kind?->value ?? 'unknown', PHP_EOL;
                }
                if ($page->next !== null) {
                    $start = MetadataChangeStart::after($page->next);
                }
                if (!$page->has_more) {
                    sleep(1);
                }
                break;
            case MetadataChangeStatus::Behind:
                usleep(100_000);
                break;
            default:
                $start = rebuild($client, $groups);
        }
    }
}

// Pages the current state, then returns the position to follow from.
function rebuild(Iggy\Client $client, array $groups): MetadataChangeStart
{
    $start = MetadataChangeStart::current();
    while (true) {
        $page = $client->getMetadataChanges($start, $groups);
        if ($page->status === MetadataChangeStatus::Behind) {
            usleep(100_000);
            continue;
        }
        if ($page->status !== MetadataChangeStatus::Ok) {
            $start = MetadataChangeStart::current();
            continue;
        }
        foreach ($page->items as $item) {
            echo $item->group?->value ?? 'unknown', PHP_EOL;
        }
        if ($page->current_page_token !== null) {
            $start = MetadataChangeStart::current($page->current_page_token);
        } elseif ($page->next !== null) {
            return MetadataChangeStart::after($page->next);
        } else {
            $start = MetadataChangeStart::current();
        }
    }
}

follow($client, [MetadataChangeGroup::Streams, MetadataChangeGroup::Topics]);
```

- `Expired` means the log no longer holds the history after the cursor, and `Reset` means the cursor belongs to another history, for example after the data directory was rebuilt. Either way the mirror rebuilds from current state pages and then follows the changes after the anchor they return.
- Current pages need the parent groups of every selected group, for example `Topics` with `Streams`. Keep a rebuilt state private until a change page answers `Ok` with `has_more` false and a `head_op` at or above the last current page's.
- Pages, cursors, page tokens, records, items and bodies expose their values as readonly snake_case properties, like `StreamDetails`, for example `$record->change->stream_created_op`, so `==` compares their contents. `$record->kind` names the body class in `$record->change`, for example `Iggy\MetadataStreamChange` for `StreamCreated` and `StreamUpdated`.
- A record or item this build does not know has a null `kind` or `group` and a `MetadataUnknownChange` or `MetadataUnknownItem` body with its codes and raw body, so a mirror can stop there rather than skip a change.
- Stream, topic and user bodies return `options` and `derived_options` arrays like `StreamDetails`: a stream's key source and fingerprints, never its key.
- Unsigned 64-bit values such as ops and timestamps are ints, or decimal strings above `PHP_INT_MAX`. Log ids, checksums and client ids are 128-bit decimal strings. A cursor cast to string gives its `mc1_` text and a page token its `cs1_` text, and `MetadataChangeCursor::parse()` and `MetadataCurrentPageToken::parse()` read them back, so a mirror can store its position as a string.
- `MetadataChanges::decode()` reads the payload `sendBinaryRequest(17, ...)` returns, and `MetadataChanges::fromJson()` the body of `GET /metadata/changes`.

## API Notes

- `new Iggy\Client($address, new Iggy\PendingOperationsConfig(256, 64 * 1024 * 1024))`
  configures client-wide active/retained mutation limits. Both limits must be positive;
  omission uses 256 operations and 64 MiB of frozen headers and bodies. Admission waits
  within the operation deadline before assigning an ID. Queued writer references keep
  bytes charged after abandonment or reset until their final owner releases the frame.
  TCP, QUIC and WebSocket connection strings accept `max_pending_operations` and
  `max_pending_operation_bytes`, for example
  `iggy+quic://iggy:iggy@localhost:8080?max_pending_operations=256&max_pending_operation_bytes=67108864`.
- `Iggy\Exception\PendingOperationException` preserves the read-only `operation_id`
  decimal string and `kind` (`uncertain`, `lane_blocked`, `fetch_progress_uncertain`,
  or `not_retained`).
  Its read-only `partition_id` names the blocked partition of a fetch, or is
  `null` for other pending operations.
  `getCode()` returns the SDK error code. Keep IDs as strings: PHP integers cannot
  represent the full unsigned 64-bit range. `lane_blocked` identifies an earlier
  operation, and the blocked call was not sent. `not_retained` does not prove
  that the earlier operation failed to commit.
- `Iggy\Client::resolveOperation(string $operation_id, ?int $timeout_micros = null)`
  replays that retained operation, with a positive timeout (30 seconds by default).
  A proved committed business rejection raises its original exception and frees the
  lane. An uncertain result keeps the operation retained for another resolution attempt.
  `abandonOperation(string $operation_id)` releases it without proving whether it
  committed. Both methods reject malformed or overflowing IDs. HTTP does not support
  retained-operation recovery. A session reset, disconnect or shutdown discards
  replayable retained mutations. The client keeps the proved outcomes of its last
  `max_pending_operations` replays, also across a session reset. A later
  `resolveOperation()` of a proved operation returns the same outcome without a request.
- Unknown committing fetch progress raises code 14010 with kind `fetch_progress_uncertain`. Its local recovery ID and affected progress blocks survive reconnect and session replacement.

  `resolveOperation()` returns that error without I/O. `abandonOperation()` explicitly releases all its local blocks while accepting the unknown outcome. Abandonment does not stop the old request.

  GetOffset and stateless reads remain available for inspection. Raw partial replies keep wire statuses. A later affected progress call raises code 14009 with the partition and recovery ID. Fetch retention uses a separate bounded pool with the configured operation/byte limits and stores target metadata only.
- Receipt access follows the server's [session and revocation policy](../../core/server/README.md#sessions-and-retry-receipts).
  An unresolved operation cannot recover its receipt after its original session
  ends, even after a new login or account reactivation. A fresh send can duplicate it.
- Each node counts the messages in its stream, topic and system stats. After a
  successful send, a node that is not the partition primary shows the new count
  only after the next prepare or commit broadcast. When no other write follows,
  this delay is at most `cluster.commit_broadcast_interval` (500 ms by default).
  See [metadata freshness and counters](../../core/server/README.md#metadata-freshness-and-counters).
- Methods are exposed to PHP as camelCase, for example `createStream()` and
  `pollMessages()`.
- Classes live in the `Iggy` namespace, for example `Iggy\Client` and
  `Iggy\SendMessage`.
- Partition IDs use the Iggy partition index. For a topic with one partition, use `0`.
- Passing `null` as the partition to `storeOffset()` or `deleteOffset()` uses the
  current consumer partition, and is rejected until at least one message has been
  polled. Pass an explicit partition id before the first poll.
- `consumeMessages()` requires an explicit finite limit. It does not run forever
  by default. An exception of the callback ends the run, and `consumeMessages()`
  throws it. In the callback, `commit()`, `storeOffset()`, `deleteOffset()` and the
  getters work. `seek()`, `shutdown()`, `iterMessages()` and `consumeMessages()` throw
  there.
- `iterMessages()` returns a PHP `Iterator` and can be used with `foreach`.
  Break out of the loop when the caller's processing limit or shutdown signal is reached.
  A new `foreach` over the same iterator reads on after the last message it yielded.
- `pollMessages()` returns an `Iggy\PolledMessages` batch with `partition_id`,
  `current_offset`, `count` and `messages`. `Iggy\PollOptions` sets `count`,
  `max_wait_micros`, `min_count`, `max_bytes` and `commit`, and refuses
  invalid limits before anything is sent.
- `commit` takes an `Iggy\CommitPolicy`: `CommitPolicy::manual()`, the default, stores
  nothing, `CommitPolicy::onRead()` makes the server store the offset of each reply, and
  `CommitPolicy::auto(new AutoCommit(...))` makes a reader or a consumer store the latest
  counted position every `interval_micros` or on a `CommitEvery` event, and on close or
  shutdown. A message counts as its `CommitAfter` says: after processing (the default),
  at delivery, or when `mark($checkpoint)` marks it. `AutoCommit::default()` stores every
  10,000 messages or 5 seconds after processing. `pollMessages()` refuses `auto()`.
- With `CommitPolicy::onRead()`, the server stores the offset of each reply. A lost reply, also to a `disconnect()` while the poll waits, then throws `FetchProgressUncertain` (14010), and the partition stays blocked until `abandonOperation()`. `disconnect()` closes the connection until the next `connect()`, after which `loginUser()` signs in again.
- `consumerGroup()` builds a member of a consumer group, and creates a missing group
  by default. `reader()` builds a batch reader of chosen partitions as a standalone
  consumer, and `groupReader()` a batch reader of an existing consumer group.
- `PartitionReader::next()` waits until a batch arrives. Give `timeout_micros` to
  limit the wait. At the timeout, `next()` returns `null`, and the script gets control
  back. A batch that arrives later stays in the reader for the next call.
- `startPollMessages()` runs the poll of `pollMessages()` in the background and
  returns a `PendingPoll` at once. `wait($timeout_micros)` returns its batch, or
  `null` while the poll still runs, and `cancel()` drops the request, which cancels
  a parked deferred poll on the server. A parked poll holds its connection, so start
  it on a client that sends nothing else meanwhile.
- The default options of a reader or a consumer wait 5 seconds for up to 1000
  messages. A wait of 0 polls an idle topic in a busy loop. `prefetch_bytes`
  (16 MiB) and `prefetch_messages` bound the requests in flight and the queued
  batches.
- With `CommitPolicy::manual()`, the consumer stores no offset. Commit checkpoints
  after processing, or each restart reads the same messages again.
- Under `CommitPolicy::auto()`, `consumeMessages()` counts a message after its callback
  returns with `CommitAfter::Processing`, and before it with `CommitAfter::Delivery`. An
  exception of the callback ends the run, and under `CommitAfter::Processing` its message
  stays uncounted, so the next run reads it again.
- Every received message has a `checkpoint()`: the position after the message, with
  the partition incarnation, which changes when the partition is deleted and recreated,
  and the consumer group owner generation. Commit it with `Iggy\Consumer::commit()` or
  `Iggy\PartitionReader::commit()`, on the consumer or the reader that delivered it. A
  commit after a recreation or an owner change is refused instead of landing on the newer
  partition. `storeOffset()` commits under the context of the latest message consumed
  from that partition. `PolledMessages::checkpoint()` covers the whole batch.
- `PolledMessages::continuation()` continues a poll after the batch, under the
  partition context of the batch. A read after a recreation fails with
  `HistoryUnavailable` (87). A continuation cannot move to another process: store a
  checkpoint there, and start the other process with `PollingStrategy::next()`.
- `Client::deleteConsumerPosition()` deletes the offset stored for the partition of a
  checkpoint, under the partition history that delivered it.
- A failed read of a reader or of `iterMessages()` throws `Iggy\Exception\ReadException`.
  `consumeMessages()` throws it only for an error that ends its run, such as
  `HistoryUnavailable` (87), and the consumer retries the other failed reads.
  `getCode()` is the error code, `getPrevious()` the typed exception that a direct call
  would throw, `partition_id` the partition or `null`, and `skip` the continuation after
  a message that does not decode. The reader retries by itself. `HistoryUnavailable`
  (87) stops the partition until `seek()`.
- Call `close()` on a reader and `shutdown()` on a consumer before the script ends.
  Under `CommitPolicy::auto()`, they store the latest counted position first. Under
  `CommitPolicy::onRead()`, they move the offset back to the last delivered
  message, so the next run reads the prefetched messages again. Destroying the object
  without them skips those messages.
- A poll with a fresh strategy, or an offset write without a checkpoint, uses the
  context of its route. It can fail once with `HistoryUnavailable` (87), or
  `ConsumerGroupPartitionNotOwned` (5009) for a consumer group, after another client
  deleted and recreated the partition. The failed route is dropped, and the next call
  routes again. Both errors surface as `Iggy\Exception\IggyException`.
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
- `Iggy\Client::sendBinaryRequestToReplica(int $code, string $payload, int $replica_id): string` sends a raw command to one node of a cluster, named by the replica id that cluster metadata returns, for example to read that node's `GetStats`. Replicated commands, polls, consumer-offset reads and session-control codes throw an invalid-command exception before anything is sent. An unknown id throws an invalid-identifier exception, and a `NotPrimary` reply is thrown, never sent to another node.
- `Iggy\Client` is synchronous and blocks the current PHP thread.
- The extension owns a lazy global Tokio runtime. Do not call `pcntl_fork()` after
  the first Iggy SDK call; the child process inherits file descriptors but not
  Tokio worker threads. Runtime initialization failure is unrecoverable and aborts
  extension use.
