# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.

"""Prints the version, row count and newest rows of the Delta table."""

import os
import sys

import pyarrow as pa
from deltalake import DeltaTable
from deltalake.exceptions import DeltaError

ENDPOINT_URL = os.environ.get("S3_ENDPOINT_URL", "http://floci:4566")
BUCKET = os.environ.get("S3_BUCKET", "delta-warehouse")
TABLE_PATH = os.environ.get("DELTA_TABLE_PATH", "users")
PREVIEW_ROWS = 5
SORT_KEYS = [("created_at", "descending")]

STORAGE_OPTIONS = {
    "AWS_ACCESS_KEY_ID": os.environ.get("AWS_ACCESS_KEY_ID", "test"),
    "AWS_SECRET_ACCESS_KEY": os.environ.get("AWS_SECRET_ACCESS_KEY", "test"),
    "AWS_REGION": os.environ.get("AWS_REGION", "us-east-1"),
    "AWS_ENDPOINT_URL": ENDPOINT_URL,
    "AWS_ALLOW_HTTP": "true",
    "AWS_S3_ALLOW_HTTP": "true",
}


def main() -> None:
    table_uri = f"s3://{BUCKET}/{TABLE_PATH}"
    try:
        table = DeltaTable(table_uri, storage_options=STORAGE_OPTIONS)
    except (DeltaError, OSError) as error:
        sys.exit(f"Cannot open Delta table {table_uri}: {error}")

    print(f"Table: {table_uri}")
    print(f"Version: {table.version()}")

    # Scan batch by batch and keep only the newest rows of each batch.
    total_rows = 0
    newest = []
    for batch in table.to_pyarrow_dataset().to_batches():
        total_rows += batch.num_rows
        top = pa.Table.from_batches([batch]).sort_by(SORT_KEYS).slice(0, PREVIEW_ROWS)
        newest.append(top)

    print(f"Rows: {total_rows}")
    if total_rows == 0:
        print("The table is empty. Run the producer and check again in a few seconds.")
        return

    print(f"Newest {PREVIEW_ROWS} rows by created_at:")
    for row in (
        pa.concat_tables(newest).sort_by(SORT_KEYS).slice(0, PREVIEW_ROWS).to_pylist()
    ):
        print(row)


if __name__ == "__main__":
    main()
