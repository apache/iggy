# apache-iggy

[![discord-badge](https://img.shields.io/discord/1144142576266530928)](https://discord.gg/C5Sux5NcRa)

Apache Iggy is the persistent message streaming platform written in Rust, supporting QUIC, TCP and HTTP transport protocols, capable of processing millions of messages per second.

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

## Examples

Refer to the [examples/python/](https://github.com/apache/iggy/tree/master/examples/python) directory for usage examples.

## Contributing

See [CONTRIBUTING.md](https://github.com/apache/iggy/blob/master/foreign/python/CONTRIBUTING.md) for development setup and guidelines.

## License

Licensed under the Apache License 2.0. See [LICENSE](https://github.com/apache/iggy/blob/master/foreign/python/LICENSE) for details.
