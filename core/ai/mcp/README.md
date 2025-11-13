# Apache Iggy MCP Server

The [Model Context Protocol](https://modelcontextprotocol.io) (MCP) is an open protocol that standardizes how applications provide context to LLMs. The Apache Iggy MCP Server is an implementation of the MCP protocol for the message streaming infrastructure.

To start the MCP server, simply run `cargo run --bin iggy-mcp`.

The [docker image](https://hub.docker.com/r/apache/iggy-mcp) is available, and can be fetched via `docker pull apache/iggy-mcp`.

The minimal viable configuration requires at least the Iggy credentials, to create the connection with the running Iggy server using TCP with which the MCP server will communicate. You can choose between HTTP and STDIO transports (e.g. for the local usage with tools such as [Claude Desktop](https://claude.ai/download) choose `stdio`).

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

Keep in mind that either of `toml`, `yaml`, or `json` formats are supported for the configuration file. The path to the configuration can be overriden by `IGGY_MCP_CONFIG_PATH` environment variable. Each configuration section can be also additionally updated by using the following convention `IGGY_MCP_SECTION_NAME.KEY_NAME` e.g. `IGGY_MCP_IGGY_USERNAME` and so on.

Here's the example configuration to be used with Claude Desktop:

```json
{
  "mcpServers": {
    "iggy": {
      "command": "/path/to/iggy-mcp",
      "args": [],
      "env": {
        "IGGY_MCP_TRANSPORT": "stdio"
      }
    }
  }
}
```

**Remember to use the appropriate Iggy account credentials for your environment** (e.g. create the user with read-only permissions to avoid modifying the data). On top of this, you can also configure the `permissions` for the MCP server to control which operations are allowed (this will be checked first, before forwarding the actual request to the Iggy server).

![MCP](../../../assets/iggy_mcp_server.png)
