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
import urllib
import urllib.parse
from collections import namedtuple

from apache_iggy import IggyClient, StreamDetails, TopicDetails
from apache_iggy import SendMessage as Message
from loguru import logger

STREAM_NAME = "sample-stream"
TOPIC_NAME = "sample-topic"
STREAM_ID = 0
TOPIC_ID = 0
PARTITION_ID = 0
BATCHES_LIMIT = 5

ArgNamespace = namedtuple("ArgNamespace", ["tcp_server_address", "tls", "tls_ca_file", "username", "password"])


class ValidateUrl(argparse.Action):
    def __call__(
        self,
        parser: argparse.ArgumentParser,
        namespace: argparse.Namespace,
        values: typing.List[typing.Any],
        option_string: typing.Optional[str] = None,
    ):
        parsed_url: urllib.parse.ParseResult = urllib.parse.urlparse("//" + values)
        if parsed_url.netloc == "" or parsed_url.path != "":
            parser.error(f"Invalid server address: {values}")
        setattr(namespace, self.dest, values)


def parse_args():
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--tcp-server-address",
        help="Iggy TCP server address (host:port)",
        action=ValidateUrl,
        default="127.0.0.1:8090",
    )
    parser.add_argument(
        "--tls",
        action="store_true",
        default=False,
        help="Enable TLS for TCP connection",
    )
    parser.add_argument(
        "--tls-ca-file",
        default="",
        help="Path to TLS CA certificate file",
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
    
    # Validate TLS requirements
    if args.tls and not args.tls_ca_file:
        parser.error("--tls requires --tls-ca-file")
    
    return ArgNamespace(**vars(args))

def build_connection_string(args) -> str:
    """Build a connection string with TLS support."""

    conn_str = f"iggy://{args.username}:{args.password}@{args.tcp_server_address}"
    
    if args.tls:
        # Extract domain from server address (host:port -> host)
        host = args.tcp_server_address.split(":")[0]
        query_params = [f"tls=true", f"tls_domain={host}"]
        
        # Add CA file if provided
        if args.tls_ca_file:
            query_params.append(f"tls_ca_file={args.tls_ca_file}")
        conn_str += "?" + "&".join(query_params)
    
    return conn_str

async def main():
    args: ArgNamespace = parse_args()
    # Build connection string with TLS support
    connection_string = build_connection_string(args)
    logger.info(f"Connection string: {connection_string}")
    logger.info(f"Connecting to {args.tcp_server_address} (TLS: {args.tls})")

    
    client = IggyClient.from_connection_string(connection_string)
    logger.info("Connecting to IggyClient")
    await client.connect()
    logger.info("Connected. Logging in user...")
    await client.login_user(args.username, args.password)
    logger.info("Logged in.")
    await init_system(client)
    await produce_messages(client)


async def init_system(client: IggyClient):
    try:
        logger.info(f"Creating stream with name {STREAM_NAME}...")
        stream: StreamDetails = await client.get_stream(STREAM_NAME)
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
        topic: TopicDetails = await client.get_topic(STREAM_NAME, TOPIC_NAME)
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
    interval = 0.5  # 500 milliseconds in seconds for asyncio.sleep
    logger.info(
        f"Messages will be sent to stream: {STREAM_NAME}, topic: {TOPIC_NAME}, partition: {PARTITION_ID} with interval {interval * 1000} ms."
    )
    current_id = 0
    messages_per_batch = 10
    n_sent_batches = 0
    while n_sent_batches < BATCHES_LIMIT:
        messages = []
        for _ in range(messages_per_batch):
            current_id += 1
            payload = f"message-{current_id}"
            message = Message(payload)
            messages.append(message)
        logger.info(
            f"Attempting to send batch of {messages_per_batch} messages. Batch ID: {current_id // messages_per_batch}"
        )
        try:
            await client.send_messages(
                stream=STREAM_NAME,
                topic=TOPIC_NAME,
                partitioning=PARTITION_ID,
                messages=messages,
            )
            n_sent_batches += 1
            logger.info(
                f"Successfully sent batch of {messages_per_batch} messages. Batch ID: {current_id // messages_per_batch}"
            )
        except Exception as error:
            logger.error(f"Exception type: {type(error).__name__}, message: {error}")
            logger.exception(error)

        await asyncio.sleep(interval)
    logger.info(f"Sent {n_sent_batches} batches of messages, exiting.")


if __name__ == "__main__":
    asyncio.run(main())
