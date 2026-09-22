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
    AutoCommit,
    AutoCommitAfter,
    IggyClient,
    PollingStrategy,
    ReceiveMessage,
)
from loguru import logger

STREAM_NAME = "high-level-stream"
TOPIC_NAME = "high-level-topic"
CONSUMER_GROUP_NAME = "high-level-consumer"
MESSAGES_TO_CONSUME = 12


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

    consumer = await client.consumer_group(
        CONSUMER_GROUP_NAME,
        STREAM_NAME,
        TOPIC_NAME,
        polling_strategy=PollingStrategy.First(),
        batch_length=100,
        auto_commit=AutoCommit.After(AutoCommitAfter.ConsumingEachMessage()),
        poll_interval=timedelta(milliseconds=100),
    )

    shutdown_event = asyncio.Event()
    consumed_messages = 0

    async def handle_message(message: ReceiveMessage) -> None:
        nonlocal consumed_messages
        consumed_messages += 1
        logger.info(
            "Received message from partition {} at offset {}: {}",
            message.partition_id(),
            message.offset(),
            message.payload().decode("utf-8"),
        )
        if consumed_messages == MESSAGES_TO_CONSUME:
            shutdown_event.set()

    await consumer.consume_messages(handle_message, shutdown_event)
    logger.info("Consumed {} messages, exiting", consumed_messages)


if __name__ == "__main__":
    asyncio.run(main())
