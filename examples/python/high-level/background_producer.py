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

from apache_iggy import (
    BackgroundProducerConfig,
    BackpressureMode,
    IggyClient,
    Partitioning,
    ProducerSharding,
    SendMessage,
)
from loguru import logger

STREAM_NAME = "high-level-stream"
TOPIC_NAME = "high-level-topic"
MESSAGES_TO_SEND = 12


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
        mode=BackgroundProducerConfig(
            num_shards=4,
            linger_time=timedelta(milliseconds=10),
            batch_size=1024 * 1024,
            batch_length=100,
            max_buffer_size=32 * 1024 * 1024,
            failure_mode=BackpressureMode.block_with_timeout(timedelta(seconds=1)),
            max_in_flight=4,
            # Ordered keeps one destination on one sequential worker. Balanced
            # uses all workers but does not guarantee order for one destination.
            sharding=ProducerSharding.ORDERED,
        ),
        create_stream_if_not_exists=True,
        create_topic_if_not_exists=True,
        topic_partitions_count=3,
        send_retries=3,
        send_retry_interval=timedelta(seconds=1),
    )

    async with producer:
        first = await producer.send_one(SendMessage("background message 0"))
        logger.info(
            "Dispatcher accepted the first message with {} confirmations",
            len(first.confirmations),
        )

        messages = [
            SendMessage(f"background message {index}")
            for index in range(1, MESSAGES_TO_SEND)
        ]
        accepted = await producer.send(messages)
        logger.info(
            "Dispatcher accepted {} more messages with {} confirmations",
            len(messages),
            len(accepted.confirmations),
        )

    logger.info("Shutdown flushed every accepted message")


if __name__ == "__main__":
    asyncio.run(main())
