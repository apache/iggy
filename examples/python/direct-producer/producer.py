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
from datetime import timedelta
from typing import NamedTuple

from apache_iggy import DirectProducerConfig, IggyClient, Partitioning, SendMessage
from loguru import logger

STREAM_NAME = "direct-producer-stream"
TOPIC_NAME = "direct-producer-topic"


class ArgNamespace(NamedTuple):
    connection_string: str


def parse_args() -> ArgNamespace:
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "connection_string",
        help=(
            "Connection string for Iggy, for example "
            "'iggy+tcp://iggy:iggy@127.0.0.1:8090'"
        ),
        default="iggy+tcp://iggy:iggy@127.0.0.1:8090",
        nargs="?",
    )
    return ArgNamespace(**vars(parser.parse_args()))


async def main() -> None:
    args = parse_args()
    client = IggyClient.from_connection_string(args.connection_string)

    logger.info("Connecting to Iggy")
    await client.connect()

    producer = await client.producer(
        STREAM_NAME,
        TOPIC_NAME,
        partitioning=Partitioning.balanced(),
        mode=DirectProducerConfig(
            batch_length=100,
            linger_time=timedelta(milliseconds=5),
        ),
        create_stream_if_not_exists=True,
        create_topic_if_not_exists=True,
        topic_partitions_count=3,
        topic_message_expiry=None,
        topic_max_size=None,
        send_retries=3,
        send_retry_interval=timedelta(seconds=1),
    )

    async with producer:
        confirmation = await producer.send_one(SendMessage("single message"))
        logger.info(
            "Sent one message with {} confirmation(s)",
            len(confirmation.confirmations),
        )

        messages = [SendMessage(f"batch message {index}") for index in range(10)]
        confirmation = await producer.send(messages)
        logger.info(
            "Sent a batch with {} confirmation(s)",
            len(confirmation.confirmations),
        )


if __name__ == "__main__":
    asyncio.run(main())
