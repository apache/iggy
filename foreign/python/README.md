# iggy_py

[![discord-badge](https://img.shields.io/discord/1144142576266530928)](https://iggy.rs/discord)

Apache Iggy is the persistent message streaming platform written in Rust, supporting QUIC, TCP and HTTP transport protocols, capable of processing millions of messages per second.


## Installation

To install `iggy`, use pip:

```bash
pip install iggy-py
```

### Supported Python Versions

- Python 3.7+

## Usage and Examples:

All examples rely on a running iggy server. To start the server, execute:

```
docker run --rm -p 8080:8080 -p 3000:3000 -p 8090:8090 iggyrs/iggy:0.4.21
```

## Generating Stub Files
To generate a stub file, execute the following command:

```
cargo run --bin stub_gen
```

Refer to the python_examples directory for examples on how to use the iggy library.

## Running the Examples:

Go to [python_examples/README.md](python_examples/README.md) for instructions on running the examples.


## API Reference

For detailed documentation, visit [Apache Iggy's official Docs](https://docs.iggy.rs/).

## Contributing

Contributions are welcome! Please:

1. Fork the repository on GitHub.
2. Create an issue for any bugs or features you'd like to address.
3. Submit pull requests following our code style guidelines.

For more details, see the [Developer README](CONTRIBUTING.md).

## License

Apache iggy is distributed under the Apache 2.0 License. See [LICENSE](LICENSE) for terms and conditions.

## Contact Information

For questions, suggestions, or issues, contact the developers at [your email address] or raise an issue on GitHub.