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

from apache_iggy import (
    Consumer,
    HttpConfig,
    IggyClient,
    PollingStrategy,
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
    api_url: str
    retries: int


class ValidateUrl(argparse.Action):
    def __call__(
        self,
        parser: argparse.ArgumentParser,
        namespace: argparse.Namespace,
        values: str,
        _option_string: str | None = None,
    ):
        parsed_url: urllib.parse.ParseResult = urllib.parse.urlparse(values)
        if parsed_url.scheme not in ("http", "https") or parsed_url.netloc == "":
            parser.error(f"Invalid API URL: {values}")
        setattr(namespace, self.dest, values)


def parse_args() -> ArgNamespace:
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--api-url",
        help="Iggy HTTP API URL",
        action=ValidateUrl,
        default="http://127.0.0.1:3000",
    )
    parser.add_argument(
        "--retries",
        type=int,
        default=3,
        help="Number of retries to perform on transient errors",
    )
    args = parser.parse_args()
    return ArgNamespace(**vars(args))


def build_config(args: ArgNamespace) -> HttpConfig:
    """Build an HTTP client configuration."""

    return HttpConfig(
        api_url=args.api_url,
        retries=args.retries,
    )


async def main():
    args: ArgNamespace = parse_args()
    try:
        config = build_config(args)
    except ValueError as error:
        logger.error(f"Invalid client configuration: {error}")
        return
    logger.info(f"Connecting to {args.api_url}")

    client = IggyClient.http(config)
    try:
        logger.info("Connecting to IggyClient...")
        await client.connect()
        logger.info("Connected.")
        # Log in explicitly rather than relying on auto-login, which
        # HttpConfig does not expose.
        await client.login_user("iggy", "iggy")
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
