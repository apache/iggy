# Iceberg Sink Connector

The Iceberg Sink Connector allows you to consume messages from Iggy topics and store them in Iceberg tables.

## Features

- **Support for S3-compatible storage**
- **Support for REST catalogs**
- **Single destination table**
- **Multiple-table fan-out static routing**
- **Multiple-table fan-out dynamic routing**

## Configuration example

The catalog, storage bucket, namespace and target tables must already exist. Save this
`plugin_config` section in a complete sink connector file with `schema = "json"` on the stream.
The sink does not create tables.

```toml
[plugin_config]
tables = ["nyc.users"]
catalog_type = "rest"
warehouse = "warehouse"
uri = "http://localhost:8181"
dynamic_routing = false
dynamic_route_field = "db_table"
store_url = "http://localhost:9000"
store_access_key_id = "admin"
store_secret_access_key = "password"
store_region = "us-east-1"
store_class = "s3"
store_path_style_access = true
```

## Configuration Options

- **tables**: The names of the Iceberg tables you want to statically route Iggy messages to. The name should include the table’s namespace, separated by a dot (`.`). Ignored when `dynamic_routing = true`.
- **catalog_type**: The type of catalog you are routing data to. **Currently, only REST catalogs are fully supported.**
- **warehouse**: The warehouse value sent to the REST catalog. Its meaning depends on the catalog service; data file destinations come from each table’s metadata.
- **uri**: The URI of the Iceberg catalog.
- **dynamic_routing**: Enables dynamic routing. See more details later in this document.
- **dynamic_route_field**: The top-level message field that specifies the Iceberg table to route data to. Ignored in static mode. See more details below.
- **store_url**: The URL of the object storage for data uploads.
- **store_access_key_id**: The optional access key ID of the object storage.
- **store_secret_access_key**: The optional secret key used to upload data to the object storage. Supply both credential fields or omit both to use the default AWS credential provider chain.
- **store_region**: The region of the object storage. Required, including for S3-compatible stores that ignore it.
- **store_class**: The storage class to use. **Currently, only S3-compatible storage is supported.**
- **store_path_style_access**: Use path-style S3 URLs (`http://host/bucket/key`). Defaults to `true`, which MinIO-style endpoints require; set to `false` for stores that only accept virtual-hosted-style URLs.

All options above are required except the credential pair and `store_path_style_access`.
`tables` and `dynamic_route_field` must be present even when the selected mode does not use them.

## Static Routing

With `dynamic_routing = false`, every batch is copied to every successfully loaded table in
`tables`. Invalid names and tables that cannot be loaded are skipped at startup. Startup fails
if none can be loaded. The writer uses the schema and default partition spec captured at
startup, so restart the connector after changing them.

## Dynamic Routing

If you don't know the names of the Iceberg tables you want to route data to in advance, you can use the dynamic routing feature.
Insert a top-level field in your JSON messages with the name of the Iceberg table the message should be routed to. The Iggy connector will parse this field at runtime and route the message to the correct table.

The Iggy Iceberg Connector will skip messages in the following cases:

- The table declared in the message field cannot be loaded, including missing tables and catalog lookup failures.
- The table name has no namespace or contains an empty name component.
- The message does not contain the field specified in the `dynamic_route_field` configuration option, or is not a JSON object.

A lookup failure skips the affected message without failing the batch. Each batch loads its
destination tables again. The route field remains in the row, but the writer ignores it if the
target schema has no matching column.

### Dynamic routing configuration example

```toml
[plugin_config]
tables = []
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
store_path_style_access = true

[transforms.add_fields]
enabled = true

[[transforms.add_fields.fields]]
key = "db_table"
value.static = "nyc.users"
```

**Note:** The value in the message field **must** contain both the namespace and the table name, separated by a dot (`.`).
Example:

- Namespace: `nyc`
- Table name: `users`

## Partitioned Tables

Data files follow the table's default partition spec. Each batch is split by the spec's transforms
(`identity`, `bucket`, `truncate`, `year`, `month`, `day`, `hour`) and every partition value present
in a successful batch gets at least one Parquet file under its partition path. Writers can roll
over into multiple files, including for unpartitioned tables.

Each destination table has its own append transaction. Fan-out is not atomic across tables:
a write or commit error stops work on the remaining tables, while earlier commits remain.
The Iceberg library refreshes metadata before committing and retries eligible commit errors
according to the table's retry properties. The plugin does not replay failed batches. The
runtime records a plugin error and continues polling with consumer auto-commit, so messages
can be lost after failed or skipped writes. Replaying committed rows can create duplicates.

## Source Compatibility

Use `schema = "json"` on the stream. Each JSON object represents a table row; nested objects
and arrays are supported when they match the table schema. Unknown fields are ignored. Missing
nullable fields become null; missing required fields or incompatible values fail the table write.

Sources that wrap row data in an envelope need a transform when the table schema describes the
inner row. Otherwise, the Arrow JSON reader maps envelope keys to table columns, producing nulls
or schema errors.

If your source emits envelope-wrapped JSON, use the `unwrap_envelope` transform to extract the
inner data field before it reaches the sink:

```toml
[transforms.unwrap_envelope]
enabled = true
field = "data"
```

Set `field` to the envelope key that contains the actual row data. See the SDK README and your
source connector's documentation for details on the envelope shape.
