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
import secrets
import time
from collections.abc import Callable, Iterator, Mapping
from dataclasses import dataclass
from enum import StrEnum
from typing import Any

from apache_iggy import (
    HeaderKey,
    HeaderValue,
    IggyClient,
    PollingStrategy,
    ReceiveMessage,
    StreamDetails,
    TopicDetails,
)
from apache_iggy import SendMessage as Message
from loguru import logger

STREAM_NAME = "message-headers-stream"
TOPIC_NAME = "orders"
PARTITION_ID = 0
BATCHES_LIMIT = 5
MESSAGES_PER_BATCH = 10

PlainHeaderValue = str | bytes | bool | int | float
PlainHeaders = dict[str, PlainHeaderValue]
TypedHeaders = dict[HeaderKey, HeaderValue]
HeadersBuilder = Callable[["Order"], PlainHeaders | TypedHeaders]
MessageHandler = Callable[[ReceiveMessage], None]


class OrderType(StrEnum):
    CREATED = "OrderCreated"
    CONFIRMED = "OrderConfirmed"
    REJECTED = "OrderRejected"


@dataclass(frozen=True, slots=True)
class ArgNamespace:
    connection_string: str


@dataclass(frozen=True, slots=True)
class Order:
    order_type: OrderType
    payload: str


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


def generate_orders() -> Iterator[Order]:
    order_id = 0
    while True:
        order_id += 1
        match order_id % 3:
            case 0:
                payload = {
                    "orderId": f"order-{order_id}",
                    "customerId": f"customer-{secrets.randbelow(100)}",
                    "amount": secrets.randbelow(10000) + 1,
                }
                yield Order(OrderType.CREATED, json.dumps(payload))
            case 1:
                payload = {
                    "orderId": f"order-{order_id // 3}",
                    "timestamp": int(time.time() * 1000),
                }
                yield Order(OrderType.CONFIRMED, json.dumps(payload))
            case _:
                payload = {
                    "orderId": f"order-{order_id // 3}",
                    "reason": "Insufficient balance",
                }
                yield Order(OrderType.REJECTED, json.dumps(payload))


async def connect(connection_string: str) -> IggyClient:
    client = IggyClient.from_connection_string(connection_string)
    logger.info("Connecting to Iggy")
    await client.connect()
    logger.info("Connected")
    return client


async def init_system(client: IggyClient) -> None:
    try:
        logger.info(f"Creating stream with name {STREAM_NAME}...")
        stream: StreamDetails | None = await client.get_stream(STREAM_NAME)
        if stream is None:
            await client.create_stream(name=STREAM_NAME)
            logger.info("Stream was created successfully.")
        else:
            logger.warning(f"Stream {stream.name} already exists with ID {stream.id}")
    except Exception as error:
        logger.error(f"Error creating stream: {error}")
        logger.exception(error)

    try:
        logger.info(f"Creating topic {TOPIC_NAME} in stream {STREAM_NAME}")
        topic: TopicDetails | None = await client.get_topic(STREAM_NAME, TOPIC_NAME)
        if topic is None:
            await client.create_topic(
                stream=STREAM_NAME,
                partitions_count=1,
                name=TOPIC_NAME,
                replication_factor=1,
            )
            logger.info("Topic was created successfully.")
        else:
            logger.warning(f"Topic {topic.name} already exists with ID {topic.id}")
    except Exception as error:
        logger.error(f"Error creating topic {error}")
        logger.exception(error)


async def produce_messages(client: IggyClient, build_headers: HeadersBuilder) -> None:
    interval = 0.5
    logger.info(
        f"Messages will be sent to stream: {STREAM_NAME}, "
        f"topic: {TOPIC_NAME}, partition: {PARTITION_ID} "
        f"with interval {interval * 1000} ms."
    )
    orders = generate_orders()
    sent_batches = 0

    while sent_batches < BATCHES_LIMIT:
        messages: list[Message] = []
        for order in (next(orders) for _ in range(MESSAGES_PER_BATCH)):
            headers = build_headers(order)
            messages.append(Message(order.payload, user_headers=headers))
            logger.info(
                f"Prepared {order.order_type} with headers: {format_headers(headers)}"
            )

        try:
            await client.send_messages(
                stream=STREAM_NAME,
                topic=TOPIC_NAME,
                partitioning=PARTITION_ID,
                messages=messages,
            )
            sent_batches += 1
            logger.info(f"Sent {len(messages)} message(s).")
        except Exception as error:
            logger.error(f"Exception type: {type(error).__name__}, message: {error}")
            logger.exception(error)

        await asyncio.sleep(interval)

    logger.info(f"Sent {sent_batches} batches of messages, exiting.")


async def consume_messages(client: IggyClient, handle_message: MessageHandler) -> None:
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


def log_order(message_type: OrderType | None, payload: object) -> None:
    match message_type:
        case OrderType.CREATED:
            logger.info(f"Order Created: {payload}")
        case OrderType.CONFIRMED:
            logger.info(f"Order Confirmed: {payload}")
        case OrderType.REJECTED:
            logger.info(f"Order Rejected: {payload}")
        case _:
            logger.warning(f"Received unknown message type: {message_type}")


def format_headers(headers: Mapping[Any, Any]) -> dict[str, str]:
    formatted: dict[str, str] = {}
    for key, value in headers.items():
        formatted_key = repr(key) if isinstance(key, HeaderKey) else str(key)
        if isinstance(value, bytes):
            formatted[formatted_key] = f"bytes({value.hex()})"
        elif isinstance(value, HeaderValue):
            formatted[formatted_key] = repr(value)
        else:
            formatted[formatted_key] = f"{value!r} ({type(value).__name__})"
    return formatted
