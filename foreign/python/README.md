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

Every installation below compiles the Rust extension, so you'll need:

- Python 3.10+
- Rust toolchain: `curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh`
- `uv`: `curl -LsSf https://astral.sh/uv/install.sh | sh`
- All checks tooling from [CONTRIBUTING.md](https://github.com/apache/iggy/blob/master/CONTRIBUTING.md).
- Docker

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

`HttpConfig` differs from TCP in two ways. There is no reconnection policy and no
`AutoLogin`: `connect()` does not dial over HTTP, but it does start the
heartbeat that `heartbeat_interval` configures, so call it and then
`login_user(...)`. And HTTP is single-consumer only: the `consumer_group(...)`
path always fails with `Feature is unavailable`, at the join by default and at
the returned consumer's first poll if you disable `auto_join_consumer_group`,
so disabling it is not a workaround. A direct
`poll_messages(consumer=Consumer.Group(...))` fails the same way unless you
pass an explicit `partition_id`, and with one it degrades silently instead: the
consumer kind is not carried on the HTTP wire, so the group is served as an
ordinary consumer named after it, with no membership or partition assignment
behind it. Use `Consumer.Single(...)` with `poll_messages(...)`. Delivery is
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

## High-Level Producer

Use `IggyClient.producer()` when an application repeatedly publishes to one
stream and topic. Producer creation is asynchronous because it initializes the
destination before returning. By default, it creates a missing stream and topic,
uses balanced partitioning, sends directly in batches of up to 1,000 messages,
and retries failed sends up to three times with a one-second retry interval.

```python
import asyncio
from datetime import timedelta

from apache_iggy import DirectProducerConfig, IggyClient, Partitioning, SendMessage


async def main():
    client = IggyClient.from_connection_string(
        "iggy+tcp://iggy:iggy@127.0.0.1:8090"
    )
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
        send_retries=3,
        send_retry_interval=timedelta(seconds=1),
    )

    async with producer:
        await producer.send_one(SendMessage("order-1"))
        response = await producer.send(
            [SendMessage("order-2"), SendMessage("order-3")]
        )
        print(f"Received {len(response.confirmations)} partition confirmations")


asyncio.run(main())
```

The producer is bound to the stream and topic passed to `producer()`. Use
`send_with_partitioning(messages, partitioning)` to override its partitioning
strategy for one call, or `send_to(stream, topic, messages, partitioning)` to
send to another existing destination. `send_to()` does not create or initialize
that destination.

Cleanup is asynchronous and must be explicit. Prefer `async with`, as above, so
shutdown runs on both successful and exceptional exits. Otherwise, call
`await producer.shutdown()` in a `finally` block. Shutdown waits for active
sends, is safe to call more than once, and rejects later sends with
`RuntimeError`. Object destruction does not perform asynchronous cleanup.

Only direct mode is implemented currently. Passing `BackgroundProducerConfig`
to `producer()` raises `NotImplementedError`.

## Examples

Refer to the [examples/python/](https://github.com/apache/iggy/tree/master/examples/python) directory for usage examples.

## Contributing

See [CONTRIBUTING.md](https://github.com/apache/iggy/blob/master/CONTRIBUTING.md) for contribution guidelines.

## License

Licensed under the Apache License 2.0. See [LICENSE](https://github.com/apache/iggy/blob/master/foreign/python/LICENSE) for details.
