# iggy_py

[![discord-badge](https://img.shields.io/discord/1144142576266530928)](https://discord.gg/C5Sux5NcRa)

Apache Iggy is the persistent message streaming platform written in Rust, supporting QUIC, TCP and HTTP transport protocols, capable of processing millions of messages per second.

## Installation

### Basic Installation

```bash
pip install iggy-py
```

### Development Installation

For testing:

```bash
pip install -e ".[testing]"
```

For development with all tools:

```bash
pip install -e ".[dev,testing,examples]"
```

### Supported Python Versions

- Python 3.7+

## Usage and Examples

All examples rely on a running iggy server. To start the server, execute:

```bash
# Using latest version
docker run --rm -p 8080:8080 -p 3000:3000 -p 8090:8090 apache/iggy:latest

# Or build from source (recommended for development)
cd ../../ && cargo run --bin iggy-server
```

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

Refer to the [python_examples/](https://github.com/apache/iggy/tree/master/foreign/python/python_examples) directory for usage examples.

## Contributing

See [CONTRIBUTING.md](https://github.com/apache/iggy/blob/master/foreign/python/CONTRIBUTING.md) for development setup and guidelines.

## License

Licensed under the Apache License 2.0. See [LICENSE](https://github.com/apache/iggy/blob/master/foreign/python/LICENSE) for details.
