# apache-iggy

[![discord-badge](https://img.shields.io/discord/1144142576266530928)](https://discord.gg/C5Sux5NcRa)

Apache Iggy is the persistent message streaming platform written in Rust, supporting QUIC, TCP and HTTP transport protocols, capable of processing millions of messages per second.

## Installation

### Basic Installation

```bash
pip install apache-iggy
```

### Development Installation

For testing:

```bash
pip install -e ".[testing]"
```

For development with all tools:

```bash
pip install -e ".[dev,testing]"
```

### Supported Python Versions

- Python 3.10+

## Testing

### Quick Test

```bash
# Run tests with Docker (recommended)
docker compose -f docker-compose.test.yml up --build
```

### Local Development

```bash
# Install dependencies and build
pip install -e ".[testing]"
maturin develop

# Run tests (requires iggy-server running)
pytest tests/ -v
```

## Examples

Refer to the [examples/python/](https://github.com/apache/iggy/tree/master/examples/python) directory for usage examples.

## Contributing

See [CONTRIBUTING.md](https://github.com/apache/iggy/blob/master/foreign/python/CONTRIBUTING.md) for development setup and guidelines.

## License

Licensed under the Apache License 2.0. See [LICENSE](https://github.com/apache/iggy/blob/master/foreign/python/LICENSE) for details.
