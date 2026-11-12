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
from dataclasses import dataclass
from enum import StrEnum

from apache_iggy import (
    CommitPolicy,
    Consumer,
    IggyClient,
    PollingStrategy,
    PollOptions,
    ReceiveMessage,
)
from loguru import logger

STREAM_NAME = "message-envelope-stream"
TOPIC_NAME = "orders"
PARTITION_ID = 0
BATCHES_LIMIT = 5
MESSAGES_PER_BATCH = 10


class OrderType(StrEnum):
    CREATED = "OrderCreated"
    CONFIRMED = "OrderConfirmed"
    REJECTED = "OrderRejected"


@dataclass(frozen=True, slots=True)
class ArgNamespace:
    connection_string: str


def parse_args() -> ArgNamespace:
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "connection_string",
        help=(
            "Connection string for Iggy client, e.g. "
            "'iggy+tcp://iggy:iggy@127.0.0.1:8090'"
        ),
        default="iggy+tcp://iggy:iggy@127.0.0.1:8090",
        nargs="?",
        type=str,
    )
    return ArgNamespace(**vars(parser.parse_args()))


async def connect(connection_string: str) -> IggyClient:
    client = IggyClient.from_connection_string(connection_string)
    logger.info("Connecting to Iggy")
    await client.connect()
    logger.info("Connected")
    return client


def log_order(message_type: OrderType | str | None, payload: object) -> None:
    match message_type:
        case OrderType.CREATED:
            logger.info(f"Order Created: {payload}")
        case OrderType.CONFIRMED:
            logger.info(f"Order Confirmed: {payload}")
        case OrderType.REJECTED:
            logger.info(f"Order Rejected: {payload}")
        case _:
            logger.warning(f"Received unknown message type: {message_type}")


def handle_message(message: ReceiveMessage) -> None:
    try:
        raw_payload = message.payload().decode("utf-8")
    except UnicodeDecodeError as error:
        logger.error(f"Skipping message with invalid UTF-8 payload: {error}")
        return

    logger.info(
        f"Handling message at offset {message.offset()} "
        f"with origin timestamp {message.origin_timestamp()}."
    )

    try:
        envelope = json.loads(raw_payload)
    except json.JSONDecodeError as error:
        logger.error(f"Failed to parse envelope JSON: {error}")
        return

    if not isinstance(envelope, dict):
        logger.warning(f"Expected JSON object envelope, got: {type(envelope).__name__}")
        return

    message_type = envelope.get("message_type")
    payload = envelope.get("payload")

    if payload is not None:
        try:
            parsed_payload = json.loads(payload)
        except (json.JSONDecodeError, TypeError):
            parsed_payload = payload
    else:
        parsed_payload = None

    log_order(message_type, parsed_payload)


async def consume_messages(
    client: IggyClient, consumer_name: str = "message-envelope-consumer"
) -> None:
    interval = 0.5
    logger.info(
        f"Messages will be consumed from stream: {STREAM_NAME}, "
        f"topic: {TOPIC_NAME}, partition: {PARTITION_ID} "
        f"as consumer: {consumer_name} "
        f"with interval {interval * 1000} ms."
    )
    consumed_batches = 0

    while consumed_batches < BATCHES_LIMIT:
        try:
            logger.debug("Polling for messages...")
            polled = await client.poll_messages(
                stream=STREAM_NAME,
                topic=TOPIC_NAME,
                partition_id=PARTITION_ID,
                consumer=Consumer.Single(consumer_name),
                polling_strategy=PollingStrategy.Next(),
                options=PollOptions(
                    count=MESSAGES_PER_BATCH,
                    commit=CommitPolicy.OnRead(),
                ),
            )
            if not polled.messages:
                logger.info("No messages found in current poll")
                await asyncio.sleep(interval)
                continue

            for message in polled.messages:
                handle_message(message)

            consumed_batches += 1
            logger.info(f"Consumed {len(polled.messages)} message(s).")
            await asyncio.sleep(interval)
        except Exception as error:
            logger.exception(f"Exception occurred while consuming messages: {error}")
            raise

    logger.info(f"Consumed {consumed_batches} batches of messages, exiting.")


async def main() -> None:
    args = parse_args()
    client = await connect(args.connection_string)
    await consume_messages(client)


if __name__ == "__main__":
    asyncio.run(main())
