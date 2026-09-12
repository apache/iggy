# Iggy Examples

This directory contains Python SDK examples for connection configuration, sending and polling messages, user headers, and TLS. To learn more about building applications with Iggy, please refer to the [getting started](https://iggy.apache.org/docs/introduction/getting-started) guide.

## Running Examples

These examples target server 0.9.0. For unreleased changes, build the SDK and
server from the same source checkout. Start the server in a separate terminal,
from the repository root:

```bash
# Server 0.9.0
docker run --rm \
  --cap-add=SYS_NICE --security-opt seccomp=unconfined --ulimit memlock=-1:-1 \
  -p 8090:8090 \
  -e IGGY_TCP_ADDRESS=0.0.0.0:8090 \
  -e IGGY_NODE_ADVERTISED_ADDRESS=localhost \
  -e IGGY_ROOT_USERNAME=iggy -e IGGY_ROOT_PASSWORD=iggy \
  apache/iggy:0.9.0

# Or build from source
cargo run --bin iggy-server -- --with-default-root-credentials --fresh
```

The container variables expose the TCP listener and bootstrap `iggy`/`iggy` for
new data. Stored credentials are not replaced, and environment credentials take
precedence over the source command's default-credentials flag. Use `--fresh`
only with disposable local replica data.

For server configuration options and help:

```bash
cargo run --bin iggy-server -- --help
```

You can also customize the server using environment variables:

```bash
# Enable HTTP transport and set its address
IGGY_HTTP_ENABLED=true IGGY_HTTP_ADDRESS=127.0.0.1:3000 cargo run --bin iggy-server
```

With Python 3.10 or newer and Rust/Cargo available, install dependencies from
`examples/python`. `uv` selects the local SDK path in `pyproject.toml`; pip needs
that path explicitly:

```bash
# Using uv
uv sync

# Using pip with the dependencies declared in pyproject.toml
python -m venv .venv
source .venv/bin/activate
pip install ../../foreign/python .
```

## Basic Examples

### Getting Started

Perfect introduction for newcomers to Iggy:

```bash
# Using uv
uv run getting-started/producer.py
uv run getting-started/consumer.py

# Without using uv
python getting-started/producer.py
python getting-started/consumer.py
```

### Basic Usage

Core functionality with detailed configuration options:

```bash
# Using uv
uv run basic/producer.py
uv run basic/consumer.py

# Without using uv
python basic/producer.py
python basic/consumer.py
```

Demonstrates client connection, authentication, batch message sending, and polling
over TCP, QUIC, or WebSocket. HTTP requires an explicit login call; its
connection-string credentials are not applied automatically.

### Message Headers

Shows how to attach and read Python SDK user headers with `str`, `bytes`, `bool`, `int`, and `float` values. Two variants share their logic through `message-headers/common.py`:

- `plain-headers/` uses the convenient `dict[str, str | bytes | bool | int | float]` form; the SDK infers a wire type for each value.
- `typed-headers/` uses explicit `HeaderKey`/`HeaderValue` for full control over the wire type.

Both producers store typed headers on the wire. The plain consumer converts them to Python scalars, while the typed consumer preserves and inspects the explicit header kinds.

```bash
# Using uv
uv run message-headers/plain-headers/producer.py
uv run message-headers/plain-headers/consumer.py
uv run message-headers/typed-headers/producer.py
uv run message-headers/typed-headers/consumer.py

# Without using uv
python message-headers/plain-headers/producer.py
python message-headers/plain-headers/consumer.py
python message-headers/typed-headers/producer.py
python message-headers/typed-headers/consumer.py
```

## TLS Examples

To test with a TLS-enabled server, start the server with TLS configured (see main README), then run:

```bash
uv run getting-started/producer.py --tcp-server-address localhost:8090 --tls --tls-ca-file ../../core/certs/iggy_ca_cert.pem
uv run getting-started/consumer.py --tcp-server-address localhost:8090 --tls --tls-ca-file ../../core/certs/iggy_ca_cert.pem
```
