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

from apache_iggy import IggyClient, Partitioning, SendMessage
from loguru import logger

STREAM_NAME = "partitioning-stream"
TOPIC_NAME = "partitioning-topic"
PARTITIONS_COUNT = 3


async def init_system(client: IggyClient) -> None:
    if await client.get_stream(STREAM_NAME) is None:
        await client.create_stream(STREAM_NAME)
    if await client.get_topic(STREAM_NAME, TOPIC_NAME) is None:
        await client.create_topic(
            stream=STREAM_NAME,
            name=TOPIC_NAME,
            partitions_count=PARTITIONS_COUNT,
        )


async def send(client: IggyClient, label: str, partitioning: Partitioning) -> None:
    response = await client.send_messages(
        stream=STREAM_NAME,
        topic=TOPIC_NAME,
        partitioning=partitioning,
        messages=[SendMessage(label)],
    )
    for confirmation in response.confirmations:
        logger.info(
            "{} was written to partition {} at offset {}",
            label,
            confirmation.partition_id,
            confirmation.base_offset,
        )


async def main(connection_string: str) -> None:
    client = IggyClient.from_connection_string(connection_string)
    await client.connect()
    await init_system(client)

    await send(client, "fixed", Partitioning.partition_id(0))
    await send(client, "balanced", Partitioning.balanced())
    await send(client, "keyed", Partitioning.messages_key(b"customer-42"))


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "connection_string",
        nargs="?",
        default="iggy+tcp://iggy:iggy@127.0.0.1:8090",
    )
    args = parser.parse_args()
    asyncio.run(main(args.connection_string))
