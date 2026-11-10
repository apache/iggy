# Iceberg Sink Connector

The Iceberg Sink Connector allows you to consume messages from Iggy topics and store them in Iceberg tables.

## Features

- **Support for S3-compatible storage**
- **Support for REST catalogs**
- **Single destination table**
- **Multiple-table fan-out static routing**
- **Multiple-table fan-out dynamic routing**
- **At-least-once delivery**, with redelivered messages appended once (see [Delivery Guarantee](#delivery-guarantee))

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
invalid_rows = "fail"
max_retries = 5
retry_delay = "1s"
max_retry_delay = "30s"
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
- **invalid_rows**: What to do with a row that does not match the table schema. `fail`, the default, commits the rows before the first such row and fails the call at it. `skip` writes the rows that match and logs each skipped row as a warning with its offset. At least once, it also decides what happens to a message without a JSON payload and to a message that dynamic routing cannot route. At most once, a message without a JSON payload is always skipped with a warning.
- **max_retries**: Total attempts per catalog commit, including the first. Defaults to `5`. See [Commit Retries](#commit-retries).
- **retry_delay**: Delay before the first commit retry, doubled for each further retry. Defaults to `1s`.
- **max_retry_delay**: Upper bound for one delay between commit attempts. Defaults to `30s`.

All options above are required except the credential pair, `store_path_style_access`, `invalid_rows`, the retry settings, and the unused routing field.
Static routing requires a non-empty `tables` list; dynamic routing requires a non-empty `dynamic_route_field`.

## Static Routing

With `dynamic_routing = false`, every batch is copied to every table in `tables`. Startup fails
if none of them loads. At most once, a table that does not load at startup is skipped until the
connector restarts. At least once, each batch loads it again and fails while it does not load:
transiently while the catalog cannot be reached, so that the runtime delivers it again, and for
good when the table does not exist. A name without a namespace is skipped at startup, and at
least once the sink refuses to start with one. The writer uses the schema and default partition
spec captured when the table loaded, so restart the connector after changing them.

## Dynamic Routing

If you don't know the names of the Iceberg tables you want to route data to in advance, you can use the dynamic routing feature.
Insert a top-level field in your JSON messages with the name of the Iceberg table the message should be routed to. The Iggy connector will parse this field at runtime and route the message to the correct table.

A message cannot be routed in the following cases:

- The message does not contain the field specified in the `dynamic_route_field` configuration option, or is not a JSON object.
- The table name has no namespace or contains an empty name component.
- The table named in the message field, or its namespace, does not exist in the catalog.

At most once, such a message is skipped, and so is a message whose table does not load for any
other reason, with a warning naming its offset. At least once, a message that cannot be routed
stops the call at that message, unless `invalid_rows = "skip"` skips it the same way. Any other
failure to load a table, such as an unreachable catalog or denied access, fails the call without
skipping anything (see [Delivery Guarantee](#delivery-guarantee)). Each batch loads its
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
The Iceberg library refreshes metadata before committing and retries commit conflicts
according to the table's retry properties, and the sink retries other transient commit errors.
At most once, once those retries run out, the runtime records a plugin error and continues
polling with consumer auto-commit, so messages can be lost after failed or skipped writes.
Replaying committed rows can create duplicates. At least once, the runtime delivers the batch
again, and each table appends only the messages it does not hold yet.

## Commit Retries

The sink writes the data files of a batch once, then retries the catalog commit when the catalog
cannot be reached, fails with a 5xx, or throttles the sink with a 429. It makes up to
`max_retries` attempts in total and waits about `retry_delay` before the first retry, doubling the
wait up to `max_retry_delay`. A commit that the catalog refuses for good, such as one rejected with
another 4xx or one to a table that no longer exists, fails without a retry.

Each commit tags its snapshot with an `iggy.commit-id` summary property. Before a retry, the sink
reloads the table and stops if a snapshot with that id is already there, so a commit whose response
was lost is not appended twice. A commit request that the catalog applies only after the sink has
checked for it, for example behind a gateway that timed out while the catalog kept working, can
still be appended twice.

When the attempts run out, the batch fails. At most once, it is lost, because the runtime stored
its offset when it read the batch: raise `max_retries` or `max_retry_delay` to ride out a longer
catalog outage. At least once, the runtime delivers the batch again after its own backoff.

## Delivery Guarantee

The sink delivers at most once unless the connector's `[delivery]` table asks for at least once:

```toml
[delivery]
guarantee = "at_least_once"
```

The [runtime README](../../runtime/README.md#sink-delivery-guarantees) describes the table, the
retries and how a stopped delivery resumes. At least once:

- **What a successful call proves.** The sink writes and commits each batch inside `consume`, so a
  call returns only after the catalog committed the batch to every destination table. The runtime
  stores the offset only after that, so a crash, a failed write, or an outage of the catalog or the
  object store delivers the batch again instead of losing it.
- **Failures.** A write or commit that fails after the sink's own retries, and a catalog that cannot
  be reached, fail the call transiently, and the runtime delivers the batch again after its
  backoff. A commit that the catalog refuses for good, such as one denied by its permissions, and a
  table that does not exist stop the delivery of the topic.
- **Rejected messages.** A row that does not match the table schema, a payload that holds no JSON
  document, and a message that dynamic routing cannot route stop the delivery of the topic at
  that message. The messages before it are committed first, and the error names its offset, so
  storing that offset, as the runtime README describes, gives up that message alone.
  `invalid_rows = "skip"` skips such a message instead, with a warning naming its offset. Skipping
  is an explicit choice that drops the message, and the runtime stores offsets past it.
- **Redelivered messages are appended once.** Each append records in its snapshot summary the
  source of its rows (`iggy.stream`, `iggy.topic`, `iggy.partition-id`), the offset of its first
  message (`iggy.first-offset`), and its last message (`iggy.last-offset`, `iggy.last-message-id`,
  and `iggy.last-timestamp`, the time the server stored it). An append on the table's current
  snapshot history counts as proven when a batch that arrives again carries its last message.
  The appends after the end of that batch, up to the newest one, count too, so a batch cut at
  other offsets than before still finds them. The sink skips the start of the batch that the
  counted appends cover without a gap, and appends the rest. A crash between the commit and the
  offset store, a backlog of several unflushed batches, or a batch that committed to one table and
  failed on another, therefore does not append rows twice. Each table keeps its own record, so a
  batch copied to several tables is appended once to each.
- **Limits.** The record is the range of offsets each append covers, not a list of its rows. A
  message inside a counted append is skipped even when the table lacks its row, for example a row
  deleted later, or one that `invalid_rows = "skip"` left out. To append such messages again,
  replay them with `guarantee = "at_most_once"`, which does not skip anything. A message that no
  append covers, for example one an at-most-once run lost, is appended again, and so is every
  message after it in the batch, even when an append covers some of them. A batch that carries no
  appended last message, for example after `batch_length` changed, cannot be proven and is
  appended in full. After a rollback of the table, or after it is dropped and created again under
  the same name, restart the sink, since it keeps the record of its own commits in memory and,
  with static routing, the table it loaded. Keep one sink writing each table and partition, and
  keep the stream and topic identifiers in its configuration unchanged, since the record names
  them. At most once, appends record nothing.

## Commit Rate and Table Maintenance

Each batch that the runtime hands to the sink becomes one append commit per destination table,
which adds a snapshot and a manifest to the table. Commits slow down as the table metadata grows,
so fewer, larger batches keep both the commit latency and the metadata small. Set the batch size on
the stream:

```toml
[[streams]]
stream = "events"
topics = ["users"]
schema = "json"
batch_length = 1000 # most messages in one batch
min_count = 1000    # wait until this many messages are ready,
max_wait = "5s"     # or this long, whichever comes first
```

At 2,000 messages per second, `batch_length = 100` creates about 20 snapshots per second, and the
values above about 2. A poll also returns once its reply reaches 1 MiB, and `max_wait` bounds how
long a message waits for its batch.

At least once, each snapshot summary also carries the seven `iggy.*` properties described in
[Delivery Guarantee](#delivery-guarantee), which add a few hundred bytes per snapshot to the table
metadata until the snapshot expires.

The sink does not expire snapshots or rewrite manifests. Run that maintenance from an engine that
supports it, for example Spark's `expire_snapshots` and `rewrite_manifests` procedures, on a
schedule that keeps the time travel you need. Some managed catalogs run it automatically.

## Source Compatibility

Use `schema = "json"` on the stream. Each JSON object represents a table row; nested objects
and arrays are supported when they match the table schema. Unknown fields are ignored. Missing
nullable fields become null. A missing required field or an incompatible value stops the write
at that row: the rows before it are committed and the call fails, unless `invalid_rows = "skip"`
leaves out only that row.

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
