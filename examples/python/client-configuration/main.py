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

"""
Configures the TCP client explicitly instead of passing a bare address.

The point of `auto_login` is that the credentials are replayed every time the
client connects, including after a reconnect. That is what lets the SDK
recover a session the server dropped: without it, a restart of the server
surfaces as `Unauthenticated` on the next call and the application has to
reconnect and log in by hand.

Run this, then restart the server while it is polling: the client reconnects,
replays the login and keeps going.
"""

import argparse
import asyncio
import typing
from datetime import timedelta

from apache_iggy import (
    AutoLogin,
    IggyClient,
    PollingStrategy,
    StreamDetails,
    TcpConfig,
    TcpReconnectionConfig,
    TopicDetails,
)
from apache_iggy import SendMessage as Message
from loguru import logger

STREAM_NAME = "configured-stream"
TOPIC_NAME = "configured-topic"
PARTITION_ID = 0
BATCHES_LIMIT = 5


class ArgNamespace(typing.NamedTuple):
    tcp_server_address: str
    username: str
    password: str


def parse_args() -> ArgNamespace:
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--tcp-server-address",
        default="127.0.0.1:8090",
        help="Iggy TCP server address (host:port)",
    )
    parser.add_argument("--username", default="iggy", help="Username to log in with")
    parser.add_argument("--password", default="iggy", help="Password to log in with")
    return ArgNamespace(**vars(parser.parse_args()))


def build_config(args: ArgNamespace) -> TcpConfig:
    return TcpConfig(
        server_address=args.tcp_server_address,
        auto_login=AutoLogin.username_password(args.username, args.password),
        reconnection=TcpReconnectionConfig(
            enabled=True,
            max_retries=None,  # retry forever
            interval=timedelta(seconds=1),
            reestablish_after=timedelta(seconds=5),
        ),
        heartbeat_interval=timedelta(seconds=5),
        nodelay=True,
    )


async def main():
    args = parse_args()
    config = build_config(args)
    logger.info(f"Connecting with {config}")

    client = IggyClient(config)
    # No login_user() call: auto_login replays the credentials on every connect.
    await client.connect()
    logger.info("Connected and authenticated.")

    await init_system(client)
    await produce_and_consume(client)


async def init_system(client: IggyClient):
    stream: StreamDetails | None = await client.get_stream(STREAM_NAME)
    if stream is None:
        await client.create_stream(name=STREAM_NAME)
        logger.info(f"Created stream {STREAM_NAME}.")

    topic: TopicDetails | None = await client.get_topic(STREAM_NAME, TOPIC_NAME)
    if topic is None:
        await client.create_topic(
            stream=STREAM_NAME,
            name=TOPIC_NAME,
            partitions_count=1,
            replication_factor=1,
        )
        logger.info(f"Created topic {TOPIC_NAME}.")


async def produce_and_consume(client: IggyClient):
    for batch in range(BATCHES_LIMIT):
        messages = [Message(f"message-{batch}-{i}") for i in range(10)]
        await client.send_messages(
            stream=STREAM_NAME,
            topic=TOPIC_NAME,
            partitioning=PARTITION_ID,
            messages=messages,
        )
        logger.info(f"Sent batch {batch}.")

        polled = await client.poll_messages(
            stream=STREAM_NAME,
            topic=TOPIC_NAME,
            partition_id=PARTITION_ID,
            polling_strategy=PollingStrategy.Next(),
            count=len(messages),
            auto_commit=True,
        )
        logger.info(f"Polled {len(polled)} messages.")
        await asyncio.sleep(0.5)


if __name__ == "__main__":
    asyncio.run(main())
