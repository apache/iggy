# Apache Iggy Connectors - Runtime

Runtime is responsible for managing the lifecycle of the connectors and providing the necessary infrastructure for the connectors to run.

The runtime uses a shared [Tokio runtime](https://tokio.rs) to manage the asynchronous tasks and events across all connectors. Additionally, it has built-in support for logging via [tracing](https://docs.rs/tracing/latest/tracing/) crate.

The connector are implemented as Rust libraries, and these are loaded dynamically during the runtime initialization process.

Internally, [dlopen2](https://github.com/OpenByteDev/dlopen2) provides a safe and efficient way of loading the plugins via C FFI.

By default, runtime will look for the configuration file, to decide which connectors to load and how to configure them.

To start the connector runtime, simply run `cargo run --bin iggy-connectors`.

The [docker image](https://hub.docker.com/r/apache/iggy-connect) is available, and can be fetched via `docker pull apache/iggy-connect`.

The minimal viable configuration requires at least the Iggy credentials, to create 2 separate instances of producer & consumer connections and the state directory path where source connectors can store their optional state.

```toml
[iggy]
address = "localhost:8090"
username = "iggy"
password = "iggy"
# token = "secret" # Personal Access Token (PAT) can be used instead of username and password

[state]
path = "local_state"
```

All the other config sections start either with `sources` or `sinks` depending on the connector type.

Keep in mind that either of `toml`, `yaml`, or `json` formats are supported for the configuration file. The path to the configuration can be overriden by `IGGY_CONNECTORS_CONFIG_PATH` environment variable. Each configuration section can be also additionally updated by using the following convention `IGGY_CONNECTORS_SECTION_NAME.KEY_NAME` e.g. `IGGY_CONNECTORS_IGGY_USERNAME` and so on.

## HTTP API

Connector runtime has an optional HTTP API that can be enabled by setting the `enabled` flag to `true` in the `[http_api]` section.

```toml
[http_api] # Optional HTTP API configuration
enabled = true
address = "127.0.0.1:8081"
# api_key = "secret" # Optional API key for authentication to be passed as `api-key` header

[http_api.cors] # Optional CORS configuration for HTTP API
enabled = false
allowed_methods = ["GET", "POST", "PUT", "DELETE"]
allowed_origins = ["*"]
allowed_headers = ["content-type"]
exposed_headers = [""]
allow_credentials = false
allow_private_network = false

[http_api.tls] # Optional TLS configuration for HTTP API
enabled = false
cert_file = "core/certs/iggy_cert.pem"
key_file = "core/certs/iggy_key.pem"
```

Currently, it does expose the following endpoints:

- `GET /`: welcome message.
- `GET /health`: health status of the runtime.
- `GET /sinks`: list of sinks.
- `GET /sinks/{key}`: sink details.
- `GET /sinks/{key}/config`: sink config, including the optional `format` query parameter to specify the config format.
- `GET /sinks/{key}/transforms`: sink transforms to be applied to the fields.
- `GET /sources`: list of sources.
- `GET /sources/{key}`: source details.
- `GET /sources/{key}/config`: source config, including the optional `format` query parameter to specify the config format.
- `GET /sources/{key}/transforms`: source transforms to be applied to the fields.
