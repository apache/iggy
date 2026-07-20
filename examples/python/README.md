# Iggy Examples

This directory contains comprehensive sample applications that showcase various usage patterns of the Iggy client SDK for Python, from basic operations to advanced multi-tenant scenarios. To learn more about building applications with Iggy, please refer to the [getting started](https://iggy.apache.org/docs/introduction/getting-started) guide.

## Running Examples

To run any example, first start the server with

```bash
# Using latest release
docker run --rm -p 8080:8080 -p 3000:3000 -p 8090:8090 apache/iggy:latest

# Or build from source (recommended for development)
cd ../../ && cargo run --bin iggy-server
```

For server configuration options and help:

```bash
cargo run --bin iggy-server -- --help
```

You can also customize the server using environment variables:

```bash
## Example: Enable HTTP transport and set custom address
IGGY_HTTP_ENABLED=true IGGY_TCP_ADDRESS=0.0.0.0:8090 cargo run --bin iggy-server
```

and then install Python dependencies:

```bash
# Using uv
uv sync

# Using pip with the dependencies declared in pyproject.toml
python -m venv .venv
source .venv/bin/activate
pip install .
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

Demonstrates fundamental client connection, authentication, batch message sending, and polling with support for TCP/QUIC/HTTP protocols.

### Message Headers

Shows how to attach and read Python SDK user headers with `str`, `bytes`, `bool`, `int`, and `float` values. Two variants share their logic through `message-headers/common.py`:

- `plain-headers/` uses the convenient `dict[str, str | bytes | bool | int | float]` form; the SDK infers a wire type for each value.
- `typed-headers/` uses explicit `HeaderKey`/`HeaderValue` for full control over the wire type.

Both are consumed identically, since plain headers are stored as typed headers on the wire.

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
