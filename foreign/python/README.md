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

> Apache Iggy (Incubating) is an effort undergoing incubation at the Apache Software Foundation (ASF), sponsored by the Apache Incubator PMC.
>
> Incubation is required of all newly accepted projects until a further review indicates that the infrastructure, communications, and decision making process have stabilized in a manner consistent with other successful ASF projects.
>
> While incubation status is not necessarily a reflection of the completeness or stability of the code, it does indicate that the project has yet to be fully endorsed by the ASF.

## Installation

### Basic Installation

```bash
# Using uv
uv add apache-iggy

# Using pip
python3 -m venv .venv
source .venv/bin/activate
pip install apache-iggy
```

### Supported Python Versions

- Python 3.10+

### Local Development

```bash
# Start server for testing using docker
docker compose -f docker-compose.test.yml up --build

# Or use cargo
cargo run --bin iggy-server -- --with-default-root-credentials --fresh

# Using uv:
uv sync --all-extras
uv run maturin develop
uv run pytest tests/ -v # Run tests (requires iggy-server running)

# Using pip:
python3 -m venv .venv
source .venv/bin/activate
pip install -e ".[all]"
maturin develop
pytest tests/ -v # Run tests (requires iggy-server running)
```

## Connecting

`IggyClient` supports all four transport protocols. Use the matching constructor, or build a
client from a connection string (`iggy+<protocol>://user:pass@host:port`, e.g. `iggy+quic://...`):

```python
from apache_iggy import IggyClient

# TCP (also the `IggyClient(server_address)` shorthand). Default: 127.0.0.1:8090
client = IggyClient.tcp(server_address="127.0.0.1:8090")

# QUIC. Default: 127.0.0.1:8080
client = IggyClient.quic(server_address="127.0.0.1:8080")

# HTTP. Default: http://127.0.0.1:3000
client = IggyClient.http(api_url="http://127.0.0.1:3000")

# WebSocket. Default: 127.0.0.1:8092
client = IggyClient.websocket(server_address="127.0.0.1:8092")

# Or from a connection string, which also supports TLS and other options:
client = IggyClient.from_connection_string("iggy+tcp://iggy:iggy@127.0.0.1:8090")
```

`tcp()` and `websocket()` also accept `tls_enabled`, `tls_domain`, `tls_ca_file`, and
`tls_validate_certificate` keyword arguments.

## Examples

Refer to the [examples/python/](https://github.com/apache/iggy/tree/master/examples/python) directory for usage examples.

## Contributing

See [CONTRIBUTING.md](https://github.com/apache/iggy/blob/master/foreign/python/CONTRIBUTING.md) for development setup and guidelines.

## License

Licensed under the Apache License 2.0. See [LICENSE](https://github.com/apache/iggy/blob/master/foreign/python/LICENSE) for details.
