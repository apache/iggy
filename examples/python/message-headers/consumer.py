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
from collections.abc import Mapping
from typing import Any, NamedTuple

from apache_iggy import (
    HeaderKey,
    HeaderValue,
    IggyClient,
    PollingStrategy,
    ReceiveMessage,
    UserHeaders,
)
from loguru import logger

STREAM_NAME = "message-headers-stream"
TOPIC_NAME = "orders"
PARTITION_ID = 0
BATCHES_LIMIT = 5
MESSAGES_PER_BATCH = 10

ORDER_CREATED_TYPE = "OrderCreated"
ORDER_CONFIRMED_TYPE = "OrderConfirmed"
ORDER_REJECTED_TYPE = "OrderRejected"


class ArgNamespace(NamedTuple):
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


async def main():
    args: ArgNamespace = parse_args()
    client = IggyClient.from_connection_string(args.connection_string)
    logger.info("Connecting to Iggy")
    await client.connect()
    logger.info("Connected")
    await consume_messages(client)


async def consume_messages(client: IggyClient):
    interval = 0.5
    logger.info(
        f"Messages will be consumed from stream: {STREAM_NAME}, "
        f"topic: {TOPIC_NAME}, partition: {PARTITION_ID} "
        f"with interval {interval * 1000} ms."
    )
    consumed_batches = 0

    while consumed_batches < BATCHES_LIMIT:
        try:
            logger.debug("Polling for messages...")
            polled_messages = await client.poll_messages(
                stream=STREAM_NAME,
                topic=TOPIC_NAME,
                partition_id=PARTITION_ID,
                polling_strategy=PollingStrategy.Next(),
                count=MESSAGES_PER_BATCH,
                auto_commit=True,
            )
            if not polled_messages:
                logger.info("No messages found in current poll")
                await asyncio.sleep(interval)
                continue

            for message in polled_messages:
                handle_message(message)

            consumed_batches += 1
            logger.info(f"Consumed {len(polled_messages)} message(s).")
            await asyncio.sleep(interval)
        except Exception as error:
            logger.exception(f"Exception occurred while consuming messages: {error}")
            break

    logger.info(f"Consumed {consumed_batches} batches of messages, exiting.")


def handle_message(message: ReceiveMessage):
    payload = json.loads(message.payload().decode("utf-8"))
    # `user_headers()` returns the explicitly typed `UserHeaders` mapping
    # (a dict subclass) or None when the message carries no headers.
    headers = message.user_headers()
    message_type = get_message_type(headers)

    logger.info(
        f"Handling message at offset {message.offset()} "
        f"with origin timestamp {message.origin_timestamp()}."
    )
    if headers is not None:
        logger.info(f"Headers: {format_headers(headers)}")
        # Opt into the convenient plain form.
        logger.info(f"Plain headers: {format_headers(headers.to_plain())}")

    if message_type == ORDER_CREATED_TYPE:
        handle_order_created(payload)
    elif message_type == ORDER_CONFIRMED_TYPE:
        handle_order_confirmed(payload)
    elif message_type == ORDER_REJECTED_TYPE:
        handle_order_rejected(payload)
    else:
        logger.warning(f"Received unknown message type: {message_type}")


def handle_order_created(order_created: Mapping[str, Any]):
    logger.info(f"Order Created: {order_created}")


def handle_order_confirmed(order_confirmed: Mapping[str, Any]):
    logger.info(f"Order Confirmed: {order_confirmed}")


def handle_order_rejected(order_rejected: Mapping[str, Any]):
    logger.info(f"Order Rejected: {order_rejected}")


def get_message_type(headers: UserHeaders | None) -> str | None:
    if headers is None:
        return None
    message_type = headers.get(HeaderKey.String("message-type"))
    if isinstance(message_type, HeaderValue.String):
        return message_type.value
    return None


def format_headers(headers: Mapping[Any, Any]) -> dict[str, str]:
    formatted: dict[str, str] = {}
    for key, value in headers.items():
        formatted_key = repr(key) if isinstance(key, HeaderKey) else key
        if isinstance(value, bytes):
            formatted[formatted_key] = f"bytes({value.hex()})"
        else:
            formatted[formatted_key] = f"{value!r} ({type(value).__name__})"
    return formatted


if __name__ == "__main__":
    asyncio.run(main())
