<div align="center">
  <picture>
    <source media="(prefers-color-scheme: dark)" srcset="https://raw.githubusercontent.com/apache/iggy/refs/heads/master/assets/logo/SVG/iggy-apache-color-darkbg.svg">
    <source media="(prefers-color-scheme: light)" srcset="https://raw.githubusercontent.com/apache/iggy/refs/heads/master/assets/logo/SVG/iggy-apache-color-lightbg.svg">
    <img alt="Apache Iggy" src="https://raw.githubusercontent.com/apache/iggy/refs/heads/master/assets/logo/SVG/iggy-apache-color-lightbg.svg" width="320">
  </picture>
</div>

# apache-iggy

[![discord-badge](https://img.shields.io/discord/1144142576266530928)](https://discord.gg/C5Sux5NcRa)

Apache Iggy is the persistent message streaming platform written in Rust, supporting QUIC, TCP and HTTP transport protocols, capable of processing millions of messages per second.

## Installation

### Basic Installation

```bash
# Using uv in an existing project
uv add apache-iggy

# Using pip
python3 -m venv .venv
source .venv/bin/activate
pip install apache-iggy
```

### Prerequisites

- Python 3.11+

Published wheels include the Rust extension; installing a wheel does not require
Rust. Building from source and running the development checks below also requires:

- Rust toolchain: `curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh`
- `uv`: `curl -LsSf https://astral.sh/uv/install.sh | sh`
- All checks tooling from [CONTRIBUTING.md](https://github.com/apache/iggy/blob/master/CONTRIBUTING.md).
- Docker

Use an SDK release compatible with your server. For unreleased changes, build
the SDK and server from the same source checkout.

### Local Development

**IMPORTANT: All commands are supposed to be ran from `foreign/python` unless it's specified to run in repository's root folder.**

1. Build a project for development

   With `uv`:

   ```bash
   # Create a venv
   uv venv

   # Sync the environment without updating it
   uv sync --frozen --all-extras --no-install-project

   # Build the project -- this builds the rust extension into the venv (debug profile) - re-run after any rust change
   uv run --no-sync maturin develop
   ```

   With `pip`:

   ```bash
   # Create a venv
   python3 -m venv .venv

   # Activate the venv
   source .venv/bin/activate

   # Install the dependencies
   pip install -e ".[all]"

   # Build the project -- this builds the rust extension into the venv (debug profile) - re-run after any rust change
   maturin develop
   ```

2. Run the server to be able to run the tests (this blocks the terminal - run steps 3-5 in a separate one). `--fresh` deletes `local_data/` on every run - drop it if you have existing data you want to keep.

   ```bash
   # run from the repository's root directory
   cargo run --bin iggy-server -- --with-default-root-credentials --fresh
   ```

3. Run the tests

   `uv`:

   ```bash
   uv run --no-sync pytest tests/ -v
   ```

   `pip`:

   ```bash
   pytest tests/ -v # make sure iggy-server is running and the venv is activated
   ```

4. To update the stubs, after changing the pyo3 API surface, use

   ```bash
   # run from foreign/python
   cargo run --bin stub_gen
   ```

5. Before committing, test the pre-commit and pre-push hooks. `prek` only inspects staged content, so stage your work first:

   ```bash
   git add -A
   prek run # runs pre-commit hooks
   prek run --hook-stage pre-push
   # if a hook modifies files, re-run `git add -A` and `prek run`.
   ```

   These are some of the essential commands prek is running, so it's recommended to run them manually before
running prek / committing / pushing. This list is not exhaustive and other hook failures are possible.

   ```bash
   uv run --no-sync ruff format .
   ```

   ```bash
   uv run --no-sync ruff check --fix .
   ```

   ```bash
   cargo fmt --manifest-path Cargo.toml
   ```

   ```bash
   cargo clippy --manifest-path Cargo.toml --all-targets --all-features -- -D warnings
   ```

   ```bash
   # run from the repository's root directory
   ./scripts/ci/markdownlint.sh --fix foreign/python/README.md # read the diff after applying this, sometimes it gives unwanted results, e.g. messing up enumerations
   ```

## Client Configuration

`IggyClient` takes a server address, a `TcpConfig`, a `QuicConfig`, an
`HttpConfig`, or a `WebSocketConfig`:

```python
import asyncio
from datetime import timedelta

from apache_iggy import AutoLogin, IggyClient, TcpConfig, TcpReconnectionConfig


async def main():
    client = IggyClient(
        TcpConfig(
            server_address="127.0.0.1:8090",
            auto_login=AutoLogin.username_password("iggy", "iggy"),
            reconnection=TcpReconnectionConfig(
                enabled=True,
                max_retries=10,
                interval=timedelta(seconds=2),
                reestablish_after=timedelta(seconds=30),
            ),
            heartbeat_interval=timedelta(seconds=5),
            # tls_enabled=True,
            # tls_domain="localhost",
            # tls_ca_file="../../core/certs/iggy_ca_cert.pem",
            # tls_validate_certificate=True,
            # nodelay=True,
        )
    )
    await client.connect()


asyncio.run(main())
```

`IggyClient(...)` also accepts a `QuicConfig` for the QUIC transport, an
`HttpConfig` for the HTTP transport, and a `WebSocketConfig` for the WebSocket
transport. `examples/python/getting-started/producer.py` shows each swap in
context.

Established binary sessions survive transport loss. Password/PAT changes and
PAT expiry do not end these sessions; logout, session lease expiry and account
deactivation do. Reconnection binds the retained session before attempting a
fresh login.

`HttpConfig` differs from TCP in two ways. There is no reconnection policy and no
`AutoLogin`: `connect()` does not dial over HTTP, but it does start the
heartbeat that `heartbeat_interval` configures, so call it and then
`login_user(...)`. And HTTP is single-consumer only: `consumer_group(...)`, and
`poll_messages(...)` or `reader(...)` with a `Consumer.Group(...)`, fail with
`Feature is unavailable` (code 5). Use `Consumer.Single(...)`. Delivery is
also at-least-once: the default `retries=3` replays the full request body, so a
send whose response was lost is applied twice, and only `retries=0` opts out.

```python
import asyncio

from apache_iggy import HttpConfig, IggyClient


async def main():
    client = IggyClient(HttpConfig(api_url="http://127.0.0.1:3000"))
    await client.connect()
    await client.login_user("iggy", "iggy")


asyncio.run(main())
```

## Stream, Topic and User Options

`create_stream`, `create_topic` and `create_user` take typed options: `StreamOptions`, `TopicOptions` and `UserOptions`. Updates are patches: `update_stream`, `update_topic` and `update_user` change only the fields you pass, and `reset` returns keys to their server defaults.

A topic's `message_expiry` and `max_topic_size` are the only resettable keys. Settings such as the compression algorithm are fixed at creation and have no update field. `describe_options("stream" | "topic" | "user")` reports which keys are updatable, resettable, secret or read-only.

The update methods also take `options` and `raw_reset`, which send keys and reset names verbatim, so a key newer than this build can be set or reset. The server decides those. A refused option raises `OptionError`, a `RuntimeError` whose `code` and `kind` name the refusal, for example `4103` and `"option_not_updatable"`, the same on every transport.

Root user rename and status refusals also raise `OptionError` with code `4108` and kind `"cannot_modify_root_user"`.

```python
import asyncio
import secrets
from datetime import timedelta

from apache_iggy import (
    IggyClient,
    IggyExpiry,
    OptionKey,
    StreamOptions,
    TopicOptions,
    encryption_key_fingerprint,
)


async def main():
    client = IggyClient.from_connection_string("iggy+tcp://iggy:iggy@127.0.0.1:8090")
    await client.connect()

    key = secrets.token_bytes(32)
    stream = await client.create_stream("orders", StreamOptions(encryption_key=key))
    assert stream.encryption_key_source == "stream"
    assert stream.encryption_key_fingerprint == encryption_key_fingerprint(key)

    await client.create_topic(
        "orders",
        "created",
        TopicOptions(
            partitions_count=3,
            compression_algorithm="gzip",
            message_expiry=IggyExpiry.ExpireDuration(timedelta(days=7)),
        ),
    )
    await client.update_topic(
        "orders",
        "created",
        message_expiry=IggyExpiry.ExpireDuration(timedelta(days=1)),
        reset=[OptionKey.MaxTopicSize],
    )
    await client.update_stream("orders", name="orders-v2")


asyncio.run(main())
```

A stream encryption key must be 32 random bytes, given as `bytes` or as their canonical base64 text. The key travels inside the create request over whatever transport the client uses, so protecting it in transit with TLS is your responsibility.

The server never returns the key, only `encryption_key_source`, `encryption_key_fingerprint` and `encryption_kek_fingerprints`. Without a key, a server with encryption enabled generates one for the stream and reports the source `"server"`. An invalid key raises `ValueError` before anything is sent, and no repr or error message contains it.

## Metadata Changes

`get_metadata_changes` reads the node's change log: one record per public result of a committed metadata operation, such as a stream created, a topic renamed, a user's permissions changed or a session closed. A mirror keeps a cursor, asks for the records after it, and stores the returned `next` cursor. The call needs root or the global `manage_servers` permission. A query the server would refuse raises `IggyError` with the invalid command code before anything is sent.

```python
import asyncio

from apache_iggy import (
    IggyClient,
    MetadataChangeGroup,
    MetadataChangeStart,
    MetadataChangeStatus,
)


async def follow(client, groups):
    start = MetadataChangeStart.Earliest()
    while True:
        page = await client.get_metadata_changes(start, groups=groups)
        if page.status == MetadataChangeStatus.Ok:
            for record in page.records:
                print(record.op, record.actor_user_id, record.kind, record.change)
            if page.next is not None:
                start = MetadataChangeStart.After(page.next)
            if not page.has_more:
                await asyncio.sleep(1)
        elif page.status == MetadataChangeStatus.Behind:
            await asyncio.sleep(0.1)
        else:
            start = await rebuild(client, groups)


async def rebuild(client, groups):
    """Pages the current state, then returns the position to follow from."""
    start = MetadataChangeStart.Current()
    while True:
        page = await client.get_metadata_changes(start, groups=groups)
        if page.status == MetadataChangeStatus.Behind:
            await asyncio.sleep(0.1)
            continue
        if page.status != MetadataChangeStatus.Ok:
            start = MetadataChangeStart.Current()
            continue
        for item in page.items:
            print(item.group, item.body)
        if page.current_page_token is not None:
            start = MetadataChangeStart.Current(page.current_page_token)
        elif page.next is not None:
            return MetadataChangeStart.After(page.next)
        else:
            start = MetadataChangeStart.Current()


async def main():
    client = IggyClient.from_connection_string("iggy+tcp://iggy:iggy@127.0.0.1:8090")
    await client.connect()
    await follow(client, [MetadataChangeGroup.Streams, MetadataChangeGroup.Topics])


asyncio.run(main())
```

`Expired` means the log no longer holds the history after the cursor, and `Reset` means the cursor belongs to another history, for example after the data directory was rebuilt. Either way the mirror rebuilds from current state pages and then follows the changes after the anchor they return.

Current pages need the parent groups of every selected group, for example `Topics` with `Streams`. Keep a rebuilt state private until a change page answers `Ok` with `has_more` false and a `head_op` at or above the last current page's.

`record.kind` names the body in `record.change`, for example a `MetadataStreamChange` for `StreamCreated` and `StreamUpdated`. A record or item this build does not know has `kind` or `group` set to `None` and a `MetadataUnknownChange` or `MetadataUnknownItem` body with its codes and raw body, so a mirror can stop there rather than skip a change. Stream, topic and user bodies expose `options` and `derived_options` as `StreamDetails` does: a stream's key source and fingerprints, never its key.

`str()` of a cursor gives its `mc1_` text and of a page token its `cs1_` text, and `MetadataChangeCursor.parse` and `MetadataCurrentPageToken.parse` read them back, so a mirror can store its position as a string. Log ids, checksums and client ids are Python ints.

## Raw Commands

`send_binary_request(code, payload)` sends a command code with a payload and returns the raw response bytes. `send_binary_request_to_replica(code, payload, replica_id)` sends it to one node of a cluster, named by the replica id that cluster metadata returns.

Each node answers non-replicated commands such as `GetStats` from its own state, so this reads the state of that node. Replicated commands, polls, consumer-offset reads and session-control codes raise an invalid-command error before anything is sent. An id that cluster metadata does not name raises an invalid-identifier error. An unreachable node is retried until the request timeout, and a `NotPrimary` reply is raised rather than sent to another node. HTTP does not support raw commands.

## High-Level Producer

The Python high-level producer API is a port of the Rust high-level producer
API. For detailed producer semantics and configuration guidance, see the
[Rust high-level SDK documentation](https://iggy.apache.org/docs/sdk/rust/high-level-sdk/).

Use `IggyClient.producer()` when an application repeatedly publishes to one
stream and topic. Producer creation is asynchronous because it initializes the
destination before returning. By default, it creates a missing stream and topic,
uses balanced partitioning and sends directly in batches of up to 1,000 messages.
The binary driver retries each chunk within one 30-second deadline, preserving
its request identity. The producer adds no retry under a new identity.

The default mode is direct. Pass `BackgroundProducerConfig` to queue sends on
background workers instead.

```python
import asyncio
from datetime import timedelta

from apache_iggy import DirectProducerConfig, IggyClient, Partitioning, SendMessage


async def main():
    client = IggyClient.from_connection_string("iggy+tcp://iggy:iggy@127.0.0.1:8090")
    await client.connect()

    producer = await client.producer(
        "orders",
        "created",
        partitioning=Partitioning.balanced(),
        mode=DirectProducerConfig(
            batch_length=500,
            linger_time=timedelta(milliseconds=5),
        ),
        create_stream_if_not_exists=True,
        create_topic_if_not_exists=True,
        topic_partitions_count=3,
        topic_message_expiry=None,
        topic_max_size=None,
    )

    async with producer:
        await producer.send_one(SendMessage("order-1"))
        response = await producer.send([SendMessage("order-2"), SendMessage("order-3")])
        print(f"Received {len(response.confirmations)} partition confirmations")


asyncio.run(main())
```

The producer is bound to the stream and topic passed to `producer()`. Use
`send_with_partitioning(messages, partitioning)` to override its partitioning
strategy for one call, or `send_to(stream, topic, messages, partitioning)` to
send to another existing destination. `send_to()` does not create or initialize
that destination.

Binary transports retain the original request identity after an uncertain
exchange. Persisted sends also provide crash-safe retry receipts; weaker topic
policies can lose data and receipts on a crash. HTTP NoAck confirms dispatch
only. An application resend creates a new request and can duplicate messages.

Mutation retries belong to the shared client driver. The producer no longer accepts
`send_retries` or `send_retry_interval`; passing either raises `TypeError`. HTTP
keeps its configured HTTP retry policy and does not support operation recovery.
HTTP 503 `relay_refused` follows that policy; the Rust-backed HTTP retry schedule
does not use the server's `Retry-After` header.

On a multi-node cluster, an HTTP call often reaches a node that is not the
partition primary. For message sends, polls and consumer offset calls, that
node relays the call to the primary. The relay needs identical `http.jwt`
secrets or `cluster.auth` on every node. Without that key material, these calls
answer 503 with code 58. See
[HTTP routing and backpressure](../../core/server/README.md#http-routing-and-backpressure).

Receipt access follows the server's [session and revocation policy](../../core/server/README.md#sessions-and-retry-receipts).
An unresolved operation cannot recover its receipt after its original session
ends, even after a new login or account reactivation. A fresh send can duplicate
that operation.

Each node counts the messages in its stream, topic and system stats. After a
successful send, a node that is not the partition primary shows the new count
only after the next prepare or commit broadcast. When no other write follows,
this delay is at most `cluster.commit_broadcast_interval` (500 ms by default).
See [metadata freshness and counters](../../core/server/README.md#metadata-freshness-and-counters).

Every error that the Rust SDK reports reaches Python as `IggyError`, a `RuntimeError` subclass. Its `code` is the numeric Iggy error code, the same code that the other SDKs report, and its message is the SDK's error text, so branch on `code` rather than on the message. Three codes tell what happened to a request that did not complete:

- 58 (`TransientNotAccepted`): the server did not admit the request, so a retry is safe.
- 57 (`TransientNotCommitted`): the outcome is unknown, and the server possibly applied the write. Read the state before you write it again.
- 40 (`Unauthenticated`): the request needs a session. Sign in, then retry.

`OptionError`, `PendingOperationError`, `ProducerSendError` and `ReadError` are `IggyError` subclasses. An error that the binding raises itself, such as a `ValueError`, a `TypeError`, or a plain `RuntimeError` for an invalid argument or a shut-down producer, is not an `IggyError` and carries no code.

`PendingOperationError` exposes the exact integer `operation_id`, the numeric
`code`, and `kind` (`"uncertain"`, `"lane_blocked"`, `"fetch_progress_uncertain"`,
or `"not_retained"`). Its
`partition_id` names the blocked partition of a fetch, or is `None` for other
pending operations. On the
same live client, `await client.resolve_operation(operation_id,
timeout=timedelta(seconds=30))` replays the original operation. A committed
business rejection raises its original error and releases the lane; uncertainty
keeps the operation retained. The default timeout is 30 seconds; it must be a
positive `timedelta`. `await client.abandon_operation(operation_id)` releases the
lane without establishing whether the operation committed. A session reset
forgets replayable retained mutations, so their recovery must use the original session.
The client keeps the proved outcomes of its last `max_operations` replays, also
across a session reset. A later `resolve_operation` of a proved operation
returns the same outcome without a request.

A committing fetch with an unknown outcome raises code 14010 and kind `"fetch_progress_uncertain"`. Its local ID and affected progress blocks survive reconnect and session replacement.

`resolve_operation` returns the same error without sending anything. Explicit `abandon_operation` releases all its local blocks and accepts the unknown outcome. It does not stop the old server request.

GetOffset and stateless reads remain available for inspection. Raw partial replies preserve their wire section statuses. A later progress call on an affected target raises code 14009 with its partition and recovery ID. Consumers report the block and stop affected targets. Abandon explicitly before starting a replacement consumer.

`TcpConfig`, `QuicConfig`, and `WebSocketConfig` accept `pending_operations=
PendingOperationsConfig(max_operations=256, max_bytes=64 * 1024 * 1024)`. These
client-wide defaults bound active and retained mutations across every partition.
Committing fetch reservations and retained progress blocks use a separate pool
with those same limits, charging target metadata rather than replay frames.
Admission waits before allocating a request ID. Queued writer frames keep their
bytes charged until their last reference is released, even after abandonment.
TCP, QUIC and WebSocket connection strings accept `max_pending_operations` and
`max_pending_operation_bytes` as positive integers, for example
`iggy+tcp://iggy:iggy@localhost:8090?max_pending_operations=256&max_pending_operation_bytes=67108864`.

A failed direct send raises `ProducerSendError`, an `IggyError` with code 4056 (`ProducerSendFailed`). Its `cause` property contains the underlying error text, and its `__cause__` is the `IggyError` that ended the send, a structured `PendingOperationError` when applicable.

The optional `operation_id` of a `ProducerSendError` identifies the retained send to resolve or abandon. Its
`committed` property contains confirmations from completed chunks, and `failed`
contains the remaining messages without usable confirmations. For an `uncertain`
cause, the first `uncertain` messages of `failed` form one chunk that may already
have committed. Resolve its `operation_id` without submitting that chunk as a new
send. A cause with code 57 retains no operation, yet its first `uncertain` messages may also have committed. For any other cause, `uncertain` is zero. For `lane_blocked`,
none of `failed` was sent: the ID names an earlier operation to resolve before
retrying. Abandonment or a session reset discards retained results, and
`not_retained` does not prove non-commit. Resubmitting an uncertain chunk can
duplicate messages. Before any safe resubmission, restore payloads changed by
encryption.

### Background Mode

A successful background send means the dispatcher accepted the messages. The
worker writes them later, so all four send methods return a
`SendMessagesResponse` with an empty `confirmations` list. It does not mean the
server has committed the messages.

```python
from datetime import timedelta

from apache_iggy import (
    BackgroundProducerConfig,
    BackpressureMode,
    ProducerSharding,
    SendMessage,
)

producer = await client.producer(
    "orders",
    "created",
    mode=BackgroundProducerConfig(
        num_shards=4,
        linger_time=timedelta(milliseconds=10),
        batch_size=1024 * 1024,
        batch_length=100,
        max_buffer_size=32 * 1024 * 1024,
        failure_mode=BackpressureMode.block_with_timeout(timedelta(seconds=1)),
        max_in_flight=4,
        sharding=ProducerSharding.ORDERED,
    ),
)

async with producer:
    accepted = await producer.send_one(SendMessage("order-1"))
    assert accepted.confirmations == []
```

Each shard flushes when any configured condition is met: `batch_length` queued
send calls, `batch_size` reported bytes, or `linger_time` since the first send
entered an empty buffer. `batch_length` counts calls, not individual messages.
Zero disables either batching threshold, while a zero linger flushes as soon as
the worker receives a send.

`ProducerSharding.ORDERED` hashes the stream/topic destination so its sends use
one sequential worker and retain dispatch order. `ProducerSharding.BALANCED`
assigns consecutive sends round-robin across the shards for throughput; ordering
for one destination is not guaranteed.

`max_buffer_size` bounds bytes queued or in flight across the whole producer.
When it is full, `BackpressureMode.block()` waits indefinitely,
`block_with_timeout(duration)` waits up to that duration, and
`fail_immediately()` raises `RuntimeError` without accepting the batch. A single
batch larger than the whole budget always fails. A zero byte budget is
unlimited. `max_in_flight` separately bounds concurrent write requests across
all shards; zero uses the runtime maximum.

Binary driver retries and transport reconnection happen inside background workers.
The Python API does not currently expose a background error callback, so a write
that fails at its operation deadline is logged by the Rust SDK and its messages
are dropped from the worker queue. An uncertain chunk remains retained by the
client for recovery; it may already have committed.

See the complete runnable
[`background_producer.py`](../../examples/python/high-level/background_producer.py)
example for all background configuration fields and deterministic shutdown.

Cleanup is asynchronous and must be explicit. Prefer `async with`, as above, so
shutdown runs on both successful and exceptional exits. Otherwise, call
`await producer.shutdown()` in a `finally` block. Shutdown waits for active
sends, is safe to call more than once, and rejects later sends with
`RuntimeError`. In background mode it also drains every queue and flushes all
accepted messages before returning. Object destruction does not perform
asynchronous cleanup; dropping a background producer without `shutdown()` can
lose buffered messages.

## Reading Messages

`poll_messages()` reads one batch of one partition. `PollOptions` sets the
limits of the read and who stores its offset. The default `PollOptions()`
returns at most 1,000 messages and 1 MiB, answers at once and stores no offset.
A first message larger than `max_bytes` comes alone in its own batch. To read on, pass `continuation()` of the batch as the next
`polling_strategy`.

```python
import asyncio

from apache_iggy import Consumer, IggyClient, PollingStrategy, PollOptions


async def main():
    client = IggyClient.from_connection_string("iggy+tcp://iggy:iggy@127.0.0.1:8090")
    await client.connect()

    strategy = PollingStrategy.First()
    for _ in range(3):
        polled = await client.poll_messages(
            "orders",
            "created",
            partition_id=0,
            consumer=Consumer.Single("billing"),
            polling_strategy=strategy,
            options=PollOptions(count=100),
        )
        for message in polled.messages:
            print(message.offset(), message.payload())
        strategy = polled.continuation()


asyncio.run(main())
```

A continuation keeps the partition history of its batch. If the partition was
deleted and created again, the next poll raises `HistoryUnavailable` (87)
instead of reading the new partition. With
`PollOptions(commit=CommitPolicy.OnRead())`, the server stores the offset of the
last message as part of the reply, so `PollingStrategy.Next()` reads on after a
restart. A lost reply then blocks the partition with a `PendingOperationError`
until `abandon_operation()`, as described above. `poll_messages()` refuses
`CommitPolicy.Auto`, which only a reader or a consumer can count.

`reader()` returns a `PartitionReader`, which reads the partitions of a topic
with prefetch and keeps the position of each one. A `Consumer.Single` reads the
`partitions` that you pass. A `Consumer.Group` joins its group and reads the
partitions that the server assigns to it. `next()` returns the next batch of
one partition and never an empty one, and `commit()` stores the offset of a
checkpoint. The default options wait up to five seconds for messages. `next()`
has no time limit. To bound it, use `asyncio.wait_for()`: a cancelled `next()`
leaves its batch in the reader. Call `close()` when you are done, or use the
reader in `async with`, which closes it. `async for` reads batches until
`close()`. `delete_consumer_position()` deletes the offset stored for the
partition of a checkpoint.

```python
from apache_iggy import Consumer, IggyClient, PollingStrategy


async def read_batches(client: IggyClient):
    reader = await client.reader(
        "orders",
        "created",
        consumer=Consumer.Single("billing"),
        partitions=[0, 1, 2],
        start=PollingStrategy.First(),
    )
    try:
        for _ in range(10):
            batch = await reader.next()
            for message in batch.messages:
                print(batch.partition_id, message.offset(), message.payload())
            await reader.commit(batch.messages[-1].checkpoint())
    finally:
        await reader.close()
```

`consumer_group()` returns an `IggyConsumer`, which joins a consumer group and
yields its messages one at a time. The `commit` of its `poll_options` says when
offsets are stored. `CommitPolicy.Manual()`, the default, stores none: commit the
checkpoint of a message after you process it. `CommitPolicy.OnRead()` makes the
server store the offset of each read. `CommitPolicy.Auto(AutoCommit(...))` makes
the consumer store the latest counted position at an `interval` or on a
`CommitEvery` event, and on shutdown. A message counts after its processing with
`CommitAfter.Processing`, when it is handed over with `CommitAfter.Delivery`, or
when you call `mark(checkpoint)` with `CommitAfter.Marked`.
`AutoCommit.default()` stores every 10,000 messages or 5 seconds after
processing. With `CommitAfter.Processing`, a `consume_messages()` callback that
raises ends the run with its exception, and its message is read again by the
next run. A reader takes the same `commit`. Call `shutdown()` before you drop the consumer, or use it in
`async with`, which shuts it down. `shutdown()` also ends an `async for` that
waits for a message. In a `consume_messages()` callback, `shutdown()`, iterating
the consumer and a nested `consume_messages()` raise `RuntimeError`, because the
run waits for the callback: set the shutdown event instead.

```python
from apache_iggy import IggyClient


async def consume(client: IggyClient):
    consumer = await client.consumer_group("billing", "orders", "created")
    try:
        async for message in consumer.iter_messages():
            print(message.partition_id(), message.offset(), message.payload())
            await consumer.commit(message.checkpoint())
    finally:
        await consumer.shutdown()
```

A failed read of a reader or a consumer raises `ReadError`. Its `partition_id`
names the partition, or is `None` for the whole reader, and `error` is the
underlying `IggyError`. The reader or the consumer stays usable and retries by
itself. A partition that was deleted and created again stops with
`HistoryUnavailable` (87) until `seek(partition_id, start)` picks a new start.
`consume_messages()` then raises the error and stops. Call it again after the
seek. If a message does not decode, pass `skip` to `seek()` to read past it.

## Examples

Refer to the [examples/python/](https://github.com/apache/iggy/tree/master/examples/python) directory for usage examples.

## Contributing

See [CONTRIBUTING.md](https://github.com/apache/iggy/blob/master/CONTRIBUTING.md) for contribution guidelines.

## License

Licensed under the Apache License 2.0. See [LICENSE](https://github.com/apache/iggy/blob/master/foreign/python/LICENSE) for details.
