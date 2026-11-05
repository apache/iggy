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

import argparse
import asyncio
import json
import os
import random
import string
from datetime import UTC, datetime, timedelta
from typing import NamedTuple

from apache_iggy import (
    AutoLogin,
    DirectProducerConfig,
    IggyClient,
    Partitioning,
    TcpConfig,
)
from apache_iggy import SendMessage as Message
from loguru import logger

STREAM_NAME = os.getenv("IGGY_STREAM", "qw")
TOPIC_NAME = os.getenv("IGGY_TOPIC", "records")

SOURCES = ["browser", "mobile", "desktop", "email", "network", "other"]
STATES = ["active", "inactive", "blocked", "deleted", "unknown"]
DOMAINS = [
    "gmail.com",
    "yahoo.com",
    "hotmail.com",
    "outlook.com",
    "aol.com",
]

MAX_BATCHES = 100
MIN_RECORDS_PER_BATCH = 100
MAX_RECORDS_PER_BATCH = 500


class ArgNamespace(NamedTuple):
    connection_string: str | None
    address: str
    username: str
    password: str


def parse_args() -> ArgNamespace:
    address = os.getenv("IGGY_ADDRESS", "127.0.0.1:8090")
    username = os.getenv("IGGY_USERNAME", "iggy")
    password = os.getenv("IGGY_PASSWORD", "iggy")
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "connection_string",
        help=(
            "Connection string for Iggy, for example "
            "'iggy+tcp://iggy:iggy@127.0.0.1:8090'"
        ),
        default=None,
        nargs="?",
    )
    args = parser.parse_args()
    return ArgNamespace(
        connection_string=args.connection_string,
        address=address,
        username=username,
        password=password,
    )


def random_alnum(length: int) -> str:
    alphabet = string.ascii_letters + string.digits
    return "".join(random.choices(alphabet, k=length))  # noqa: S311


def random_record() -> dict:
    return {
        "user_id": f"user_{1 + random.randrange(99)}",  # noqa: S311
        "user_type": 1 + random.randrange(4),  # noqa: S311
        "email": (
            f"{random_alnum(3 + random.randrange(17))}"  # noqa: S311
            f"@{random.choice(DOMAINS)}"  # noqa: S311
        ),
        "source": random.choice(SOURCES),  # noqa: S311
        "state": random.choice(STATES),  # noqa: S311
        "message": random_alnum(10 + random.randrange(90)),  # noqa: S311
        "created_at": (
            datetime.now(UTC) - timedelta(days=random.randrange(1000))  # noqa: S311
        ).isoformat(),
    }


def validate_connection_string(connection_string: str | None) -> None:
    if connection_string is not None and connection_string.startswith(
        ("iggy+http://", "iggy+https://", "http://", "https://")
    ):
        raise ValueError(
            "HTTP connection strings are not supported by this example. "
            "Use a TCP connection string (e.g. 'iggy+tcp://iggy:iggy@127.0.0.1:8090') "
            "or set IGGY_ADDRESS, IGGY_USERNAME, and IGGY_PASSWORD "
            "environment variables instead."
        )


async def main() -> None:
    args = parse_args()

    validate_connection_string(args.connection_string)

    if args.connection_string:
        client = IggyClient.from_connection_string(args.connection_string)
    else:
        config = TcpConfig(
            server_address=args.address,
            auto_login=AutoLogin.username_password(args.username, args.password),
        )
        client = IggyClient(config)

    logger.info("Starting data producer...")
    logger.info(f"Stream: {STREAM_NAME}, Topic: {TOPIC_NAME}")

    await client.connect()

    producer = await client.producer(
        STREAM_NAME,
        TOPIC_NAME,
        partitioning=Partitioning.balanced(),
        mode=DirectProducerConfig(
            batch_length=1000,
            linger_time=timedelta(milliseconds=5),
        ),
        create_stream_if_not_exists=True,
        create_topic_if_not_exists=True,
    )

    total_msg = 0

    async with producer:
        for _ in range(MAX_BATCHES):
            records_count = MIN_RECORDS_PER_BATCH + random.randrange(  # noqa: S311
                MAX_RECORDS_PER_BATCH - MIN_RECORDS_PER_BATCH
            )
            messages = [
                Message(json.dumps(random_record())) for _ in range(records_count)
            ]
            await producer.send(messages)
            logger.info(f"Sent {records_count} messages")
            total_msg += records_count

    logger.info("Reached maximum batches count")
    logger.info(f"Produced {total_msg} messages.")

    await client.disconnect()


if __name__ == "__main__":
    asyncio.run(main())
