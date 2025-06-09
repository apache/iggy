# Apache Iggy Connectors - Runtime

Runtime is responsible for managing the lifecycle of the connectors and providing the necessary infrastructure for the connectors to run.

The runtime uses a shared [Tokio runtime](https://tokio.rs) to manage the asynchronous tasks and events across all connectors. Additionally, it has built-in support for logging via [tracing](https://docs.rs/tracing/latest/tracing/) crate.

The connector are implemented as Rust libraries, and these are loaded dynamically during the runtime initialization process.

Internally, [dlopen2](https://github.com/OpenByteDev/dlopen2) provides a safe and efficient way of loading the plugins via C FFI.

By default, runtime will look for the configuration file, to decide which connectors to load and how to configure them.

The minimal viable configuration requires at least the Iggy credentials, to create 2 separate instances of producer & consumer connections.

```toml
[iggy]
address = "localhost:8090"
username = "iggy"
password = "iggy"
# token = "secret" # Personal Access Token (PAT) can be used instead of username and password
```

All the other config sections start either with `sources` or `sinks` depending on the connector type.

Keep in mind that either of `toml`, `yaml`, or `json` formats are supported for the configuration file. The path to the configuration can be overriden by `IGGY_CONNECTORS_RUNTIME_CONFIG_PATH` environment variable. Each configuration section can be also additionally updated by using the following convention `IGGY_CONNECTORS_SECTION_NAME.KEY_NAME` e.g. `IGGY_CONNECTORS_IGGY_USERNAME` and so on.
