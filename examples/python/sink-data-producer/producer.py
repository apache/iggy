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
import secrets
import string
from dataclasses import asdict, dataclass
from datetime import UTC, datetime, timedelta
from typing import NamedTuple

from apache_iggy import (
    DirectProducerConfig,
    IggyClient,
    Partitioning,
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


class ArgNamespace(NamedTuple):
    connection_string: str


@dataclass
class Record:
    user_id: str
    user_type: int
    email: str
    source: str
    state: str
    created_at: str
    message: str


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
        default=f"iggy+tcp://{username}:{password}@{address}",
        nargs="?",
    )
    return ArgNamespace(**vars(parser.parse_args()))


def random_alnum(length: int) -> str:
    alphabet = string.ascii_letters + string.digits
    return "".join(secrets.choice(alphabet) for _ in range(length))


def random_record() -> Record:
    source = SOURCES[secrets.randbelow(len(SOURCES))]
    state = STATES[secrets.randbelow(len(STATES))]
    email = (
        f"{random_alnum(3 + secrets.randbelow(17))}"
        f"@{DOMAINS[secrets.randbelow(len(DOMAINS))]}"
    )
    created_at = (
        datetime.now(UTC) - timedelta(days=secrets.randbelow(1000))
    ).isoformat()

    return Record(
        user_id=f"user_{1 + secrets.randbelow(99)}",
        user_type=1 + secrets.randbelow(4),
        email=email,
        source=source,
        state=state,
        message=random_alnum(10 + secrets.randbelow(90)),
        created_at=created_at,
    )


async def main() -> None:
    args = parse_args()
    client = IggyClient.from_connection_string(args.connection_string)
    logger.info("Starting data producer...")
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

    batches_count = 0
    total_msg = 0
    async with producer:
        while batches_count < 100:
            records_count = 100 + secrets.randbelow(400)

            messages = [
                Message(json.dumps(asdict(random_record())))
                for _ in range(records_count)
            ]
            await producer.send(messages)

            logger.info(f"Sent {records_count} messages")

            total_msg += records_count
            batches_count += 1

    logger.info("Reached maximum batches count")
    logger.info(f"Produced {total_msg} messages.")


if __name__ == "__main__":
    asyncio.run(main())
