# Delta Lake Sink Connector

The Delta Lake Sink Connector allows you to consume messages from Iggy topics and store them in Delta Lake tables.

## Features

- **Support for local filesystem, AWS S3, Azure Blob Storage, and Google Cloud Storage**
- **Automatic table creation from a configured schema**
- **Intelligent type coercion** to match Delta table schemas (e.g. ISO 8601 strings to timestamps)
- **Transactional writes** with atomic flush-and-commit operations

## Configuration example

### Local filesystem

```toml
[plugin_config]
table_uri = "file:///tmp/iggy_delta_table"
schema = [
    "id long",
    "name string",
    "count integer",
    "amount double",
    "active boolean",
    "created_at timestamp",
]
```

### AWS S3

```toml
[plugin_config]
table_uri = "s3://my-bucket/delta-tables/users"
storage_backend_type = "s3"
aws_s3_access_key = "your-access-key"
aws_s3_secret_key = "your-secret-key"
aws_s3_region = "us-east-1"
aws_s3_endpoint_url = "https://s3.amazonaws.com"
aws_s3_allow_http = false
```

### Azure Blob Storage

```toml
[plugin_config]
table_uri = "az://my-container/delta-tables/users"
storage_backend_type = "azure"
azure_storage_account_name = "mystorageaccount"
azure_storage_account_key = "account-key"
azure_storage_sas_token = "sas-token"
azure_container_name = "my-container"
```

### Google Cloud Storage

```toml
[plugin_config]
table_uri = "gs://my-bucket/delta-tables/users"
storage_backend_type = "gcs"
gcs_service_account_key = '{"type": "service_account", "project_id": "...", ...}'
gcs_bucket = "my-bucket"
```

## Configuration Options

### Core

- **table_uri** (required): Path or URI to the Delta table. Supported schemes: `file://`, `s3://`, `az://`, `gs://`.
- **schema** (optional): Schema definition for automatic table creation when the table does not already exist. Each entry is a string in the format `"<field_name> <field_type>"`. If the table already exists, the schema is inferred from the table and this field is ignored.
- **storage_backend_type** (optional): The cloud storage backend to use. One of `"s3"`, `"azure"`, or `"gcs"`. Omit for local filesystem tables.

### AWS S3

Required when `storage_backend_type = "s3"`.

- **aws_s3_access_key**: AWS access key ID.
- **aws_s3_secret_key**: AWS secret access key.
- **aws_s3_region**: AWS region (e.g. `us-east-1`).
- **aws_s3_endpoint_url**: S3 endpoint URL. Useful for S3-compatible services like MinIO.
- **aws_s3_allow_http**: Set to `true` to allow HTTP connections (for local development).

### Azure Blob Storage

Required when `storage_backend_type = "azure"`.

- **azure_storage_account_name**: Azure storage account name.
- **azure_storage_account_key**: Azure storage account key.
- **azure_storage_sas_token**: Shared Access Signature token.
- **azure_container_name**: Azure container name.

### Google Cloud Storage

Required when `storage_backend_type = "gcs"`.

- **gcs_service_account_key**: GCS service account JSON key (as a string).
- **gcs_bucket**: GCS bucket name.

## Supported Schema Types

The following types can be used in the `schema` configuration:

`string`, `byte`, `short`, `integer`, `long`, `float`, `double`, `boolean`, `binary`, `date`, `timestamp`, `timestampNtz`

## Type Coercion

The connector automatically coerces JSON values to match the Delta table schema:

- **Timestamp fields**: ISO 8601 / RFC 3339 formatted strings (e.g. `"2021-11-11T22:11:58Z"`) are converted to microsecond timestamps. Numeric timestamps pass through unchanged.
- **String fields**: Non-string values (numbers, booleans, objects, arrays) are converted to their string representation.
- **Nested fields**: Coercions are applied recursively to nested structs and arrays.
