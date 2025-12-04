# Apache Iggy Connectors - Runtime

Runtime is responsible for managing the lifecycle of the connectors and providing the necessary infrastructure for the connectors to run.

The runtime uses a shared [Tokio runtime](https://tokio.rs) to manage the asynchronous tasks and events across all connectors. Additionally, it has built-in support for logging via [tracing](https://docs.rs/tracing/latest/tracing/) crate.

The connector are implemented as Rust libraries, and these are loaded dynamically during the runtime initialization process.

Internally, [dlopen2](https://github.com/OpenByteDev/dlopen2) provides a safe and efficient way of loading the plugins via C FFI.

By default, runtime will look for the configuration file, to decide which connectors to load and how to configure them.

To start the connector runtime, simply run `cargo run --bin iggy-connectors`.

The [docker image](https://hub.docker.com/r/apache/iggy-connect) is available, and can be fetched via `docker pull apache/iggy-connect`.

The minimal viable configuration requires at least the Iggy credentials to create 2 separate instances of producer & consumer connections, the state directory path where source connectors can store their optional state, and the connectors configuration provider settings.

```toml
[iggy]
address = "localhost:8090"
username = "iggy"
password = "iggy"
token = "" # Personal Access Token (PAT) can be used instead of username and password

[iggy.tls] # Optional TLS configuration for Iggy TCP connection
enabled = false
ca_file = "core/certs/iggy_ca_cert.pem"
domain = "" # Optional domain for TLS connection

[state]
path = "local_state"

[connectors]
config_type = "local"
config_dir = "path/to/connectors"
```

The path to the configuration can be overridden by `IGGY_CONNECTORS_CONFIG_PATH` environment variable. Each configuration section can be also additionally updated by using the following convention `IGGY_CONNECTORS_SECTION_NAME.KEY_NAME` e.g. `IGGY_CONNECTORS_IGGY_USERNAME` and so on.

## Configuration Providers

The runtime supports two types of configuration providers for managing connector configurations:

### Local File Provider

The default configuration provider reads connector configurations from local files. Each connector (source or sink) is configured in its own separate file within the directory specified by `connectors.config_dir`. If `config_dir` is empty or the directory doesn't exist, no connectors will be loaded.

```toml
[connectors]
config_type = "local"
config_dir = "path/to/connectors"
```

### HTTP Configuration Provider

The HTTP configuration provider allows the runtime to fetch connector configurations from a remote HTTP/REST API. This enables centralized configuration management and dynamic configuration updates.

```toml
[connectors]
config_type = "http"
base_url = "http://localhost:8080/api"
timeout = "10s"

[connectors.request_headers]
api-key = "your-api-key"

[connectors.retry]
enabled = true
max_attempts = 3
initial_backoff = "1 s"
max_backoff = "30 s"
backoff_multiplier = 2

[connectors.url_templates]
# Optional: Customize URL templates for specific operations
# If not specified, default RESTful URL patterns are used
create_sink = "/sinks/{key}/configs"
create_source = "/sources/{key}/configs"
get_active_configs = "/configs/active"

[connectors.response]
# Optional: Extract data from nested response structures
data_path = "data"        # Path to data in response (e.g., {"data": {...}})
error_path = "error"      # Path to error in response (e.g., {"error": "..."})
```

#### Configuration Options

- **base_url** (required): Base URL of the configuration API endpoint
- **timeout** (optional): HTTP request timeout (default: 10s)
- **request_headers** (optional): Custom headers to include in all HTTP requests (e.g., authentication headers)
- **url_templates** (optional): Custom URL templates for API endpoints. Supports variable substitution with `{key}` and `{version}` placeholders.
- **response.data_path** (optional): JSON path to extract response data from nested structures (e.g., "data.config")
- **response.error_path** (optional): JSON path to check for errors in responses

#### Default URL Templates

If not customized, the HTTP provider uses the following RESTful URL patterns:

- Create sink config: `POST {base_url}/sinks/{key}/configs`
- Create source config: `POST {base_url}/sources/{key}/configs`
- Get active configs: `GET {base_url}/configs/active`
- Get active versions: `GET {base_url}/configs/active/versions`
- Set active sink version: `PUT {base_url}/sinks/{key}/configs/active`
- Set active source version: `PUT {base_url}/sources/{key}/configs/active`
- Get sink configs: `GET {base_url}/sinks/{key}/configs`
- Get sink config by version: `GET {base_url}/sinks/{key}/configs/{version}`
- Get active sink config: `GET {base_url}/sinks/{key}/configs/active`
- Get source configs: `GET {base_url}/sources/{key}/configs`
- Get source config by version: `GET {base_url}/sources/{key}/configs/{version}`
- Get active source config: `GET {base_url}/sources/{key}/configs/active`
- Delete sink config: `DELETE {base_url}/sinks/{key}/configs`
- Delete source config: `DELETE {base_url}/sources/{key}/configs`

The HTTP provider expects the remote API to implement these endpoints and return connector configuration data in the same format as used by the local provider.

## HTTP API

Connector runtime has an optional HTTP API that can be enabled by setting the `enabled` flag to `true` in the `[http]` section.

```toml
[http] # Optional HTTP API configuration
enabled = true
address = "127.0.0.1:8081"
api_key = "" # Optional API key for authentication to be passed as `api-key` header

[http.cors] # Optional CORS configuration for HTTP API
enabled = false
allowed_methods = ["GET", "POST", "PUT", "DELETE"]
allowed_origins = ["*"]
allowed_headers = ["content-type"]
exposed_headers = [""]
allow_credentials = false
allow_private_network = false

[http.tls] # Optional TLS configuration for HTTP API
enabled = false
cert_file = "core/certs/iggy_cert.pem"
key_file = "core/certs/iggy_key.pem"
```

Currently, it does expose the following endpoints:

- `GET /`: welcome message.
- `GET /health`: health status of the runtime.
- `GET /sinks`: list of sinks.
- `GET /sinks/{key}`: sink details.
- `GET /sinks/{key}/configs`: list of configuration versions for the sink.
- `POST /sinks/{key}/configs`: add a new configuration version for the sink.
- `GET /sinks/{key}/configs/{version}`: configuration details for a specific version.
- `GET /sinks/{key}/configs/active`: active configuration details.
- `PUT /sinks/{key}/configs/active`: activate a specific configuration version for the sink.
- `GET /sinks/{key}/configs/plugin`: sink plugin config, including the optional `format` query parameter to specify the config format.
- `GET /sinks/{key}/transforms`: sink transforms to be applied to the fields.
- `GET /sources`: list of sources.
- `GET /sources/{key}`: source details.
- `GET /sources/{key}/configs`: list of configuration versions for the source.
- `POST /sources/{key}/configs`: add a new configuration version for the source.
- `GET /sources/{key}/configs/{version}`: configuration details for a specific version.
- `GET /sources/{key}/configs/active`: active configuration details.
- `PUT /sources/{key}/configs/active`: activate a specific configuration version for the source.
- `GET /sources/{key}/configs/plugin`: source plugin config, including the optional `format` query parameter to specify the config format.
- `GET /sources/{key}/transforms`: source transforms to be applied to the fields.
