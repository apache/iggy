# Apache Iggy MCP Server

The [Model Context Protocol](https://modelcontextprotocol.io) (MCP) is an open protocol that standardizes how applications provide context to LLMs. The Apache Iggy MCP Server is an implementation of the MCP protocol for the message streaming infrastructure.

Start an Iggy broker from the same checkout first. For a development broker started with `--with-default-root-credentials`, run from the repository root:

```sh
IGGY_MCP_IGGY_USERNAME=iggy IGGY_MCP_IGGY_PASSWORD=iggy cargo run --bin iggy-mcp
```

Use the credentials or PAT of your existing broker when connecting to another installation.

The [docker image](https://hub.docker.com/r/apache/iggy-mcp) is available, and can be fetched via `docker pull apache/iggy-mcp:edge`.

The minimal viable configuration requires at least the Iggy credentials, to create the connection with the running Iggy server using TCP with which the MCP server will communicate. You can choose between HTTP (the default) and STDIO transports (e.g. for the local usage with tools such as [Claude Desktop](https://claude.ai/download) choose `stdio`).

```toml
transport = "stdio" # http or stdio are supported

[iggy]
address = "localhost:8090" # TCP address of the Iggy server
username = "iggy"
password = "iggy"
# token = "secret" # Personal Access Token (PAT) can be used instead of username and password
# consumer = "iggy-mcp" # Optional consumer name

[iggy.tls] # Optional TLS configuration for Iggy TCP connection
enabled = false
ca_file = "core/certs/iggy_cert.pem"
domain = "" # Optional domain for TLS connection

[http] # Optional HTTP API configuration
address = "127.0.0.1:8082"
path = "/mcp"

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

[permissions]
create = true
read = true
update = true
delete = true
```

The configuration file must use TOML. The default path is `core/ai/mcp/config.toml`, relative to the working directory; override it with `IGGY_MCP_CONFIG_PATH`. Embedded defaults are loaded first, then the file if present, then environment overrides such as `IGGY_MCP_IGGY_USERNAME` and `IGGY_MCP_HTTP_ADDRESS`. Nested settings also use underscores, for example `IGGY_MCP_IGGY_TLS_ENABLED`.

Set `IGGY_MCP_ENV_PATH` to load a particular dotenv file. Otherwise `.env` is searched for in the current directory and its parents. Existing environment variables take precedence over dotenv values.

A non-empty `iggy.token` takes precedence over username and password. It accepts a literal PAT or a `file:` reference such as `file:/run/secrets/iggy_pat`; file contents are trimmed and a leading `~/` expands to the home directory.

Set `command` to the absolute path of the built executable. This Claude Desktop example uses the development broker credentials:

```json
{
  "mcpServers": {
    "iggy": {
      "command": "/path/to/iggy-mcp",
      "args": [],
      "env": {
        "IGGY_MCP_TRANSPORT": "stdio",
        "IGGY_MCP_IGGY_ADDRESS": "localhost:8090",
        "IGGY_MCP_IGGY_USERNAME": "iggy",
        "IGGY_MCP_IGGY_PASSWORD": "iggy"
      }
    }
  }
}
```

**Remember to use the appropriate Iggy account credentials for your environment** (e.g. create the user with read-only permissions to avoid modifying the data). On top of this, you can also configure the `permissions` for the MCP server to control which operations are allowed (this will be checked first, before forwarding the actual request to the Iggy server).

![MCP](../../../assets/iggy_mcp_server.png)

## Systemd integration

Build with the `systemd` feature to enable systemd readiness and watchdog notifications:

```sh
cargo build --bin iggy-mcp --release --features iggy-mcp/systemd
```

Readiness is sent after the HTTP listener starts or the stdio session initializes. The watchdog sends keep-alive notifications at half the interval supplied by systemd. SIGINT, SIGTERM, and stdio client disconnect trigger shutdown and a stopping notification.

## Telemetry

The MCP server supports OpenTelemetry for logs and traces. To enable telemetry, add the following configuration:

```toml
[telemetry]
enabled = true
service_name = "iggy-mcp"

[telemetry.logs]
transport = "grpc" # Options: "grpc", "http"
endpoint = "http://localhost:4317"

[telemetry.traces]
transport = "grpc" # Options: "grpc", "http"
endpoint = "http://localhost:4317"
```

For HTTP export, set `transport = "http"` and use complete signal URLs: `http://localhost:4318/v1/logs` for logs and `http://localhost:4318/v1/traces` for traces. The MCP server does not append those paths.
