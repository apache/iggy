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
uv venv # if not already created
uv add apache-iggy

# Using pip
python3 -m venv .venv
source .venv/bin/activate
pip install apache-iggy
```

### Supported Python Versions

- Python 3.10+

### Local Development

1. Build a project for development

   With `uv`:

   > Create a venv:
   >
   > ```bash
   > uv venv
   > ```
   >
   > Sync the environment without updating it:
   >
   > ```bash
   > uv sync --frozen --all-extras --no-install-project
   > ```
   >
   > Build the project - this runs cargo build and performs an editable install:
   >
   > ```bash
   > uv run maturin develop
   > ```

   With `pip`:

   > Create a venv:
   >
   > ```bash
   > python3 -m venv .venv
   > ```
   >
   > Activate the venv:
   >
   > ```bash
   > source .venv/bin/activate
   > ```
   >
   > Install the dependencies with `pip`:
   >
   > ```bash
   > pip install -e ".[all]"
   > ```

2. Run the server to be able to run the tests

   ```bash
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

4. To update the stubs, use

   ```bash
   cargo run --bin stub_gen
   ```

5. Before committing, test the pre-commit and pre-push hooks. `prek` only inspects staged content, so stage your work first:

   ```bash
   git add -A
   prek run # runs pre-commit hooks
   prek run --hook-stage pre-push
   ```

   These are some of the essential commands prek is running, so it's recommended to run them manually before  running prek / committing / pushing. This list is not exhaustive and other hook failures are possible.

   ```bash
   ruff format .
   ```

   ```bash
   ruff check --fix .
   ```

   ```bash
   cargo fmt --all
   ```

   ```bash
   cargo clippy --all-targets --all-features -- -D warnings
   ```

   ```bash
   ./scripts/ci/markdownlint.sh --fix # read the diff after applying this, sometimes it gives unwanted results, e.g. messing up enumerations
   ```

## Examples

Refer to the [examples/python/](https://github.com/apache/iggy/tree/master/examples/python) directory for usage examples.

## Contributing

See [CONTRIBUTING.md](https://github.com/apache/iggy/blob/master/CONTRIBUTING.md) for development setup and guidelines.

## License

Licensed under the Apache License 2.0. See [LICENSE](https://github.com/apache/iggy/blob/master/foreign/python/LICENSE) for details.
