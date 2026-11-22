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

import asyncio
from dataclasses import dataclass

from apache_iggy import IggyClient, Permissions, StreamPermissions
from apache_iggy import SendMessage as Message
from loguru import logger
from tenants import (
    PARTITION_ID,
    TOPICS,
    build_parser,
    connect,
    connect_as,
    ensure_stream_access,
    ensure_user,
    read_positive_int,
    stream_name,
)

BATCHES_LIMIT = 5
MESSAGES_PER_BATCH = 10


@dataclass(frozen=True, slots=True)
class ArgNamespace:
    connection_string: str
    tenants_count: int


def parse_args() -> ArgNamespace:
    parser = build_parser("TENANTS_COUNT sets the number of tenants (default 3).")
    args = parser.parse_args()
    tenants_count = read_positive_int(parser, "TENANTS_COUNT", "3")
    return ArgNamespace(args.connection_string, tenants_count)


async def main() -> None:
    args = parse_args()
    tenant_ids = range(1, args.tenants_count + 1)
    logger.info(f"Multi-tenant producer has started, tenants: {args.tenants_count}")

    logger.info("Connecting as root to create a stream and a user for each tenant")
    root = await connect(args.connection_string)
    for tenant_id in tenant_ids:
        await create_stream_and_user(root, tenant_id)
    await root.shutdown()

    for tenant_id in tenant_ids:
        client = await connect_as(args.connection_string, producer_username(tenant_id))
        await ensure_stream_access(client, tenant_id, args.tenants_count)
        await produce_messages(client, tenant_id)
        await client.shutdown()

    logger.info("Multi-tenant producer has finished")


async def create_stream_and_user(root: IggyClient, tenant_id: int) -> None:
    name = stream_name(tenant_id)
    stream = await root.get_stream(name)
    if stream is None:
        stream = await root.create_stream(name=name)
        logger.info(f"Created stream {name} with ID {stream.id}")
    else:
        logger.warning(f"Stream {name} already exists with ID {stream.id}")

    # Managing topics includes reading them and sending messages. The user gets
    # no global permissions and no other stream, so the rest of the server is
    # closed to it.
    permissions = Permissions(
        streams={stream.id: StreamPermissions(read_stream=True, manage_topics=True)}
    )
    await ensure_user(root, producer_username(tenant_id), permissions)


async def produce_messages(client: IggyClient, tenant_id: int) -> None:
    stream = stream_name(tenant_id)
    for topic in TOPICS:
        if await client.get_topic(stream, topic) is None:
            await client.create_topic(stream=stream, name=topic)
            logger.info(f"Tenant {tenant_id} created topic {topic} in {stream}")

        for batch in range(BATCHES_LIMIT):
            first_id = batch * MESSAGES_PER_BATCH + 1
            messages = [
                Message(f"tenant-{tenant_id}-{topic}-{message_id}")
                for message_id in range(first_id, first_id + MESSAGES_PER_BATCH)
            ]
            await client.send_messages(
                stream=stream,
                topic=topic,
                partitioning=PARTITION_ID,
                messages=messages,
            )
        logger.info(
            f"Tenant {tenant_id} sent {BATCHES_LIMIT * MESSAGES_PER_BATCH} "
            f"messages to {stream}/{topic}"
        )


def producer_username(tenant_id: int) -> str:
    return f"tenant_{tenant_id}_producer"


if __name__ == "__main__":
    asyncio.run(main())
