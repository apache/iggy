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
from collections.abc import Mapping
from typing import NamedTuple

from apache_iggy import HeaderKey, HeaderValue, IggyClient, StreamDetails, TopicDetails
from apache_iggy import SendMessage as Message
from loguru import logger

STREAM_NAME = "message-headers-stream"
TOPIC_NAME = "orders"
PARTITION_ID = 0
BATCHES_LIMIT = 5
MESSAGES_PER_BATCH = 10

ORDER_CREATED_TYPE = "OrderCreated"
ORDER_CONFIRMED_TYPE = "OrderConfirmed"
ORDER_REJECTED_TYPE = "OrderRejected"

PlainHeaderValue = str | bytes | bool | int | float
PlainHeaders = dict[str, PlainHeaderValue]
TypedHeaders = dict[HeaderKey, HeaderValue]


class ArgNamespace(NamedTuple):
    connection_string: str


class SerializedMessage(NamedTuple):
    message_type: str
    payload: str
    headers: PlainHeaders | TypedHeaders


class MessagesGenerator:
    def __init__(self):
        self.order_id = 0

    def generate(self) -> SerializedMessage:
        self.order_id += 1
        message_type = self.order_id % 3

        if message_type == 0:
            payload = {
                "orderId": f"order-{self.order_id}",
                "customerId": f"customer-{secrets.randbelow(100)}",
                "amount": secrets.randbelow(10000) + 1,
            }
            return self._serialize(ORDER_CREATED_TYPE, payload)

        if message_type == 1:
            payload = {
                "orderId": f"order-{self.order_id // 3}",
                "timestamp": int(time.time() * 1000),
            }
            return self._serialize(ORDER_CONFIRMED_TYPE, payload)

        payload = {
            "orderId": f"order-{self.order_id // 3}",
            "reason": "Insufficient balance",
        }
        return self._serialize(ORDER_REJECTED_TYPE, payload)

    def _serialize(
        self, message_type: str, payload: Mapping[str, object]
    ) -> SerializedMessage:
        encoded_payload = json.dumps(payload)
        if self.order_id % 5 == 0:
            # Add typed headers similar to the Rust core
            typed_headers: TypedHeaders = {
                HeaderKey.String("message-type"): HeaderValue.String(message_type),
                HeaderKey.String("content-type"): HeaderValue.String(
                    "application/json"
                ),
                HeaderKey.String("schema-version"): HeaderValue.UnsignedInt16(1),
                HeaderKey.String("created-at-ms"): HeaderValue.UnsignedInt64(
                    int(time.time() * 1000)
                ),
                HeaderKey.String("retryable"): HeaderValue.Bool(
                    message_type == ORDER_REJECTED_TYPE
                ),
                HeaderKey.String("priority-score"): HeaderValue.Float32(
                    (self.order_id % 100) / 100
                ),
                HeaderKey.String("trace-bin"): HeaderValue.Raw(
                    f"trace-{self.order_id}".encode()
                ),
                HeaderKey.UnsignedInt32(self.order_id): HeaderValue.String("order-id"),
            }
            return SerializedMessage(message_type, encoded_payload, typed_headers)

        # Add easy-to-use `dict[str, str | bytes | bool | int | float]` headers
        # which will be translated into typed headers by the Python SDK
        plain_headers: PlainHeaders = {
            "message-type": message_type,
            "content-type": "application/json",
            "schema-version": 1,
            "created-at-ms": int(time.time() * 1000),
            "retryable": message_type == ORDER_REJECTED_TYPE,
            "priority-score": (self.order_id % 100) / 100,
            "trace-bin": f"trace-{self.order_id}".encode(),
        }
        return SerializedMessage(message_type, encoded_payload, plain_headers)


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
    await init_system(client)
    await produce_messages(client)


async def init_system(client: IggyClient):
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


async def produce_messages(client: IggyClient):
    interval = 0.5
    logger.info(
        f"Messages will be sent to stream: {STREAM_NAME}, "
        f"topic: {TOPIC_NAME}, partition: {PARTITION_ID} "
        f"with interval {interval * 1000} ms."
    )
    generator = MessagesGenerator()
    sent_batches = 0

    while sent_batches < BATCHES_LIMIT:
        messages: list[Message] = []
        for _ in range(MESSAGES_PER_BATCH):
            serialized = generator.generate()
            messages.append(
                Message(serialized.payload, user_headers=serialized.headers)
            )
            logger.info(
                f"Prepared {serialized.message_type} with headers: "
                f"{format_headers(serialized.headers)}"
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


def format_headers(
    headers: Mapping[str, PlainHeaderValue] | Mapping[HeaderKey, HeaderValue],
) -> dict[str, str]:
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
