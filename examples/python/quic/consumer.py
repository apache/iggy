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
import typing
import urllib.parse
from datetime import timedelta

from apache_iggy import (
    AutoLogin,
    Consumer,
    IggyClient,
    PollingStrategy,
    QuicConfig,
    QuicReconnectionConfig,
    ReceiveMessage,
)
from loguru import logger

STREAM_NAME = "sample-stream"
TOPIC_NAME = "sample-topic"
STREAM_ID = 0
TOPIC_ID = 0
PARTITION_ID = 0
CONSUMER_NAME = "sample-consumer"
BATCHES_LIMIT = 5


class ArgNamespace(typing.NamedTuple):
    server_address: str
    server_name: str
    username: str
    password: str


class ValidateUrl(argparse.Action):
    def __call__(
        self,
        parser: argparse.ArgumentParser,
        namespace: argparse.Namespace,
        values: str,
        _option_string: str | None = None,
    ):
        parsed_url: urllib.parse.ParseResult = urllib.parse.urlparse("//" + values)
        if parsed_url.netloc == "" or parsed_url.path != "":
            parser.error(f"Invalid server address: {values}")
        setattr(namespace, self.dest, values)


def parse_args() -> ArgNamespace:
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--server-address",
        help="Iggy QUIC server address (host:port)",
        action=ValidateUrl,
        default="127.0.0.1:8080",
    )
    parser.add_argument(
        "--server-name",
        default="localhost",
        help="Server name used for the QUIC/TLS handshake",
    )
    parser.add_argument(
        "--username",
        default="iggy",
        help="Username for authentication",
    )
    parser.add_argument(
        "--password",
        default="iggy",
        help="Password for authentication",
    )
    args = parser.parse_args()
    return ArgNamespace(**vars(args))


def build_config(args: ArgNamespace) -> QuicConfig:
    """Build a QUIC client configuration with auto-login and reconnection."""

    return QuicConfig(
        server_address=args.server_address,
        server_name=args.server_name,
        auto_login=AutoLogin.username_password(args.username, args.password),
        reconnection=QuicReconnectionConfig(
            enabled=True,
            interval=timedelta(seconds=1),
        ),
    )


async def main():
    args: ArgNamespace = parse_args()
    try:
        config = build_config(args)
    except ValueError as error:
        logger.error(f"Invalid client configuration: {error}")
        return
    logger.info(
        f"Connecting to {args.server_address} (server name: {args.server_name})"
    )

    client = IggyClient.quic(config)
    try:
        logger.info("Connecting to IggyClient...")
        # No login_user() call: auto_login replays the credentials on every connect.
        await client.connect()
        logger.info("Connected.")
        await consume_messages(client)
    except Exception as error:
        logger.exception(f"Exception occurred in main function: {error}")


async def consume_messages(client: IggyClient):
    interval = 0.5  # 500 milliseconds in seconds for asyncio.sleep
    logger.info(
        f"Messages will be consumed from stream: {STREAM_NAME}, "
        f"topic: {TOPIC_NAME}, partition: {PARTITION_ID} "
        f"with interval {interval * 1000} ms."
    )
    offset = 0
    messages_per_batch = 10
    n_consumed_batches = 0
    while n_consumed_batches < BATCHES_LIMIT:
        try:
            logger.debug("Polling for messages...")
            polled_messages = await client.poll_messages(
                stream=STREAM_NAME,
                topic=TOPIC_NAME,
                consumer=Consumer.Single(CONSUMER_NAME),
                partition_id=PARTITION_ID,
                polling_strategy=PollingStrategy.Next(),
                count=messages_per_batch,
                auto_commit=True,
            )
            if not polled_messages:
                logger.info("No messages found in current poll")
                await asyncio.sleep(interval)
                continue

            offset += len(polled_messages)
            for message in polled_messages:
                handle_message(message)
            n_consumed_batches += 1
            await asyncio.sleep(interval)
        except Exception as error:
            logger.exception(f"Exception occurred while consuming messages: {error}")
            break

    logger.info(f"Consumed {n_consumed_batches} batches of messages, exiting.")


def handle_message(message: ReceiveMessage):
    payload = message.payload().decode("utf-8")
    logger.info(
        f"Handling message at offset: {message.offset()} with payload: {payload}..."
    )


if __name__ == "__main__":
    asyncio.run(main())
