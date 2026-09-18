# Delta Lake Sink Connector

The Delta Lake Sink Connector allows you to consume messages from Iggy topics and store them in Delta Lake tables.

## Features

- **Support for local filesystem, AWS S3, Azure Blob Storage, and Google Cloud Storage**
- **Intelligent type coercion** to match Delta table schemas (e.g. ISO 8601 strings to timestamps)
- **Transactional writes** with atomic flush-and-commit operations

The table must already exist. The connector appends each successful nonempty batch in one Delta transaction and keeps its schema snapshot until restart. The plugin has no failed-batch retry loop; the Delta library can retry eligible commit conflicts and storage requests. Write or commit errors clear the writer buffers and return an error. The runtime uses consumer auto-commit and does not replay failed sink batches, so end-to-end at-least-once delivery is not guaranteed.

## How to configure a Delta Sink connector

First, make sure that the Delta table already exists in the location you're providing. You can use this script for an example workload:

```python
import pyarrow as pa
from deltalake import DeltaTable

table_uri = "s3://test_location/tables/test"

schema = pa.schema([
    pa.field("user_id", pa.string(), nullable=True),
    pa.field("user_type", pa.uint8(), nullable=True),
    pa.field("email", pa.string(), nullable=True),
    pa.field("source", pa.string(), nullable=True),
    pa.field("state", pa.string(), nullable=True),
    pa.field("message", pa.string(), nullable=True),
    pa.field("created_at", pa.timestamp("us"), nullable=True),
])

DeltaTable.create(
    table_uri,
    schema,
    name="test",
    storage_options={"AWS_REGION": "us-east-1"},
)

print(f"Created table at {table_uri}")
```

The configuration is usually wrtitten individually for every connector and consists of two parts: the runtime settings which are registering the sink and telling which streams should plug into it, and the plugin's settings themselves. Here's an example of a working configuration:

  ```toml
  type = "sink"
  key = "delta"
  enabled = true
  version = 0
  name = "Delta Lake sink"
  path = "target/release/libiggy_connector_delta_sink" # make sure you have a compiled plugin binary in this path
  verbose = true

  # these settings are common between all sinks
  [[streams]]
  stream = "your_stream"
  topics = ["topic_inside_of_your_stream"]
  schema = "json"
  # important: read about batch and poll interval settings below
  batch_length = 10000
  poll_interval = "3s"
  consumer_group = "delta_sink_connector"

  # these settings are specific to each plugin, and in case of Delta sink, to the type of storage used
  [plugin_config]
  # the table must exist in the given location
  table_uri = "s3://iggy-sandbox/tables/test"
  storage_backend_type = "s3"
  aws_s3_region = "eu-central-1"
  aws_s3_allow_http = false
  ```

### `[[streams]]` section

- Find a topic that you need and the corresponding stream that this topic is in and add it into the configuration.
- `batch_length` and `poll_interval` have to be carefully set for Delta tables. The write happens when there is either `batch_length` number of records in the buffer or we hit `poll_interval` timeout. For Delta, writing means creating a log entry and a separate Parquet file, so if these values are too low (roughly `batch_length` < 1000 and `poll_interval` < 1s), the connector is going to write lots of small files.
  This is highly undesirable for the reader as it will have to read from many small files instead of a few larger ones. For query performance optimization, the system consuming these files will have to apply `OPTIMIZE` query in order to consolidate the files. We recommend setting these values pretty high based on your workload so that ideally the files are already optimized for reading.
  The guideline recommended by the developers of Delta is to keep individual file sizes between 128 MB and 1 GB. In the context of this document, it means that ideally `batch_length` + `poll_interval` should cut off the files in the way that their size is in the suggested range. The given range is only a rough guideline and you need to find a good setting based on the patterns of reading and writing in your systems.

### Plugin configuration

#### Attributes common to all types of storage

- **table_uri** (required): Path or URI to the Delta table. Supported schemes: `file://`, `s3://`, `az://`, `gs://`.
- **storage_backend_type** (optional): The cloud storage backend to use. One of `"s3"`, `"azure"`, or `"gcs"`. Omit for local filesystem tables.

#### Local filesystem

  ```toml
  [plugin_config]
  table_uri = "file:///tmp/iggy_delta_table"
  ```

#### AWS S3

Currently the implementation offers two possible ways of accessing the bucket.

1. Temporary security credentials issued by AWS STS, which is a best practice recommended by AWS. The role assumed by the writer should allow these actions on the bucket, here is how a working policy looks in HCL:

    ```hcl
    data "aws_iam_policy_document" "s3_write" {
      statement {
        sid    = "BucketLevelActions"
        effect = "Allow"
        actions = [
          "s3:ListBucket"
        ]
        resources = [aws_s3_bucket.unity_catalog["iggy-sandbox"].arn]
      }

      statement {
        sid    = "ObjectLevelActions"
        effect = "Allow"
        actions = [
          "s3:GetObject",
          "s3:PutObject",
          "s3:PutObjectAcl",
          "s3:AbortMultipartUpload"
        ]
        resources = ["${aws_s3_bucket.unity_catalog["iggy-sandbox"].arn}/*"]
      }
    }
    ```

    The corresponding plugin configuration will look like this:

    ```toml
    [plugin_config]
    table_uri = "s3://iggy-sandbox/tables/test"
    storage_backend_type = "s3"
    aws_s3_region = "us-east-1"
    ```

2. Long-term access key pairs (access + secret key), which AWS recommends to avoid for security reasons. The example configuration:

    ```toml
    [plugin_config]
    table_uri = "s3://my-bucket/delta-tables/users"
    storage_backend_type = "s3"
    aws_s3_access_key = "your-access-key"
    aws_s3_secret_key = "your-secret-key"
    aws_s3_region = "us-east-1"
    ```

Parameter descriptions:

- **aws_s3_access_key**: Optional. AWS access key ID. Can only be passed together with the secret key.
- **aws_s3_secret_key**: Optional. AWS secret access key. Can only be passed together with the access key.
- **aws_s3_region**: Required. AWS region (e.g. `us-east-1`).
- **aws_s3_endpoint_url**: Optional. S3 endpoint URL. Use for S3-compatible services. Make sure that the URL implies the same regions that is set in **aws_s3_region**, otherwise you'll have an error.
- **aws_s3_allow_http**: Optional. Set to `true` to allow HTTP connections (for local development).

#### Azure Blob Storage

```toml
[plugin_config]
table_uri = "az://my-container/delta-tables/users"
storage_backend_type = "azure"
azure_storage_account_name = "mystorageaccount"
azure_storage_account_key = "account-key"
azure_storage_sas_token = "sas-token"
azure_container_name = "my-container"
```

Required when `storage_backend_type = "azure"`.

- **azure_storage_account_name**: Azure storage account name.
- **azure_storage_account_key**: Azure storage account key.
- **azure_storage_sas_token**: Shared Access Signature token.
- **azure_container_name**: Azure container name.

#### Google Cloud Storage

```toml
[plugin_config]
table_uri = "gs://my-bucket/delta-tables/users"
storage_backend_type = "gcs"
gcs_service_account_key = '{"type": "service_account", "project_id": "...", ...}'
```

Required when `storage_backend_type = "gcs"`.

- **gcs_service_account_key**: GCS service account JSON key (as a string). The bucket is inferred from the `gs://` URI in `table_uri`.

## Type Coercion

The connector automatically coerces JSON values to match the Delta table schema:

- **Timestamp fields**: ISO 8601 / RFC 3339 formatted strings (e.g. `"2021-11-11T22:11:58Z", "2021-11-11 22:11:58"`) are converted to microsecond timestamps. Numeric timestamps pass through unchanged.
- **String fields**: Non-string values (numbers, booleans, objects, arrays) are converted to their string representation.
- **Nested fields**: Coercions are applied recursively to nested structs and arrays.
