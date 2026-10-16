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

"""Creates the S3 bucket and the empty Delta table the Delta sink appends to."""

import os
import sys
import time

import boto3
import pyarrow as pa
from botocore.client import Config
from botocore.exceptions import BotoCoreError, ClientError
from deltalake import DeltaTable
from deltalake.exceptions import DeltaError

ENDPOINT_URL = os.environ.get("S3_ENDPOINT_URL", "http://floci:4566")
ACCESS_KEY = os.environ.get("AWS_ACCESS_KEY_ID", "test")
SECRET_KEY = os.environ.get("AWS_SECRET_ACCESS_KEY", "test")
REGION = os.environ.get("AWS_REGION", "us-east-1")
BUCKET = os.environ.get("S3_BUCKET", "delta-warehouse")
TABLE_PATH = os.environ.get("DELTA_TABLE_PATH", "users")
MAX_ATTEMPTS = 30

STORAGE_OPTIONS = {
    "AWS_ACCESS_KEY_ID": ACCESS_KEY,
    "AWS_SECRET_ACCESS_KEY": SECRET_KEY,
    "AWS_REGION": REGION,
    "AWS_ENDPOINT_URL": ENDPOINT_URL,
    "AWS_ALLOW_HTTP": "true",
    "AWS_S3_ALLOW_HTTP": "true",
}

SCHEMA = pa.schema(
    [
        pa.field("user_id", pa.string(), nullable=True),
        pa.field("user_type", pa.uint8(), nullable=True),
        pa.field("email", pa.string(), nullable=True),
        pa.field("source", pa.string(), nullable=True),
        pa.field("state", pa.string(), nullable=True),
        pa.field("message", pa.string(), nullable=True),
        pa.field("created_at", pa.timestamp("us"), nullable=True),
    ]
)


def main() -> None:
    s3 = boto3.client(
        "s3",
        endpoint_url=ENDPOINT_URL,
        aws_access_key_id=ACCESS_KEY,
        aws_secret_access_key=SECRET_KEY,
        region_name=REGION,
        # Path-style addressing, because the emulator host has no per-bucket DNS names.
        config=Config(s3={"addressing_style": "path"}),
    )
    wait_for_s3(s3)
    ensure_bucket(s3)
    ensure_table()


def wait_for_s3(s3) -> None:
    for attempt in range(1, MAX_ATTEMPTS + 1):
        try:
            s3.list_buckets()
            print(f"S3 endpoint {ENDPOINT_URL} is ready")
            return
        except (BotoCoreError, ClientError) as error:
            print(
                f"Waiting for S3 endpoint {ENDPOINT_URL} ({attempt}/{MAX_ATTEMPTS}): {error}"
            )
            time.sleep(1)
    sys.exit(
        f"S3 endpoint {ENDPOINT_URL} did not become ready after {MAX_ATTEMPTS} attempts"
    )


def ensure_bucket(s3) -> None:
    try:
        s3.head_bucket(Bucket=BUCKET)
        print(f"Bucket {BUCKET} already exists")
        return
    except ClientError as error:
        if error.response["Error"]["Code"] not in ("404", "NoSuchBucket", "NotFound"):
            sys.exit(f"Cannot check bucket {BUCKET}: {error}")
    try:
        s3.create_bucket(Bucket=BUCKET)
    except (BotoCoreError, ClientError) as error:
        sys.exit(f"Cannot create bucket {BUCKET}: {error}")
    print(f"Created bucket {BUCKET}")


def ensure_table() -> None:
    table_uri = f"s3://{BUCKET}/{TABLE_PATH}"
    try:
        if DeltaTable.is_deltatable(table_uri, storage_options=STORAGE_OPTIONS):
            print(f"Delta table {table_uri} already exists")
            return
        DeltaTable.create(
            table_uri,
            SCHEMA,
            name=TABLE_PATH,
            storage_options=STORAGE_OPTIONS,
        )
    except (DeltaError, OSError) as error:
        sys.exit(f"Cannot create Delta table {table_uri}: {error}")
    print(f"Created Delta table {table_uri}")


if __name__ == "__main__":
    main()
