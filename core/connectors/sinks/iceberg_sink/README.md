# Iceberg Sink Connector

The Iceberg Sink Connector allows you to consume messages from Iggy topics and store them in Iceberg tables.

## Features

- **Support for S3-compatible storage**
- **Support for REST catalogs**
- **Single destination table**
- **Multiple-table fan-out static routing**
- **Multiple-table fan-out dynamic routing**

## Configuration example

```toml
[plugin_config]
tables = ["nyc.users"]
catalog_type = "rest"
warehouse = "warehouse"
uri = "http://localhost:8181"
dynamic_routing = true
dynamic_route_field = "db_table"
store_url = "http://localhost:9000"
store_access_key_id = "admin"
store_secret_access_key = "password"
store_region = "us-east-1"
store_class = "s3"
```

## Configuration Options

- **tables**: The names of the Iceberg tables you want to statically route Iggy messages to. The name should include the table’s namespace, separated by a dot (`.`).
- **catalog_type**: The type of catalog you are routing data to. **Currently, only REST catalogs are fully supported.**
- **warehouse**: The name of the bucket or warehouse where Iggy will upload data files.
- **URI**: The URI of the Iceberg catalog.
- **dynamic_routing**: Enables dynamic routing. See more details later in this document.
- **dynamic_route_field**: The name of the message field that specifies the Iceberg table to route data to. See more details below.
- **store_url**: The URL of the object storage for data uploads.
- **store_access_key_id**: The access key ID of the object storage.
- **store_secret_access_key**: The secret key used to upload data to the object storage.
- **store_region**: The region of the object storage, if applicable.
- **store_class**: The storage class to use. **Currently, only S3-compatible storage is supported.**

## Dynamic Routing

If you don't know the names of the Iceberg tables you want to route data to in advance, you can use the dynamic routing feature.
Insert a field in your Iggy messages with the name of the Iceberg table the message should be routed to. The Iggy connector will parse this field at runtime and route the message to the correct table.

The Iggy Iceberg Connector will skip messages in the following cases:

- The table declared in the message field does not exist.
- The message does not contain the field specified in the `dynamic_route_field` configuration option.

### Dynamic routing configuration example

```toml
[plugin_config]
tables = [""]
catalog_type = "rest"
warehouse = "warehouse"
uri = "http://localhost:8181"
dynamic_routing = true
dynamic_route_field = "db_table"
store_url = "http://localhost:9000"
store_access_key_id = "admin"
store_secret_access_key = "password"
store_region = "us-east-1"
store_class = "s3"

[sinks.iceberg.transforms.add_fields]
enabled = true

[[sinks.iceberg.transforms.add_fields.fields]]
key = "db_table"
value.static = "nyc.users"
```

**Note:** The value in the message field **must** contain both the namespace and the table name, separated by a dot (`.`).
Example:

- Namespace: `nyc`
- Table name: `users`
