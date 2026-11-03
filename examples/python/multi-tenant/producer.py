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
from dataclasses import dataclass

from apache_iggy import (
    IggyClient,
    Permissions,
    StreamDetails,
    StreamPermissions,
)
from apache_iggy import SendMessage as Message
from common import (
    DEFAULT_CONNECTION_STRING,
    PARTITION_ID,
    TENANT_PASSWORD,
    TOPIC_NAME,
    Tenant,
    connect,
    ensure_stream_access,
    make_tenants,
    tenant_connection_string,
)
from loguru import logger

MESSAGES_PER_TENANT = 5


@dataclass(frozen=True, slots=True)
class ArgNamespace:
    connection_string: str


def parse_args() -> ArgNamespace:
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "connection_string",
        help=(
            "Connection string of the root user, which creates the tenants, e.g. "
            "'iggy+tcp://iggy:iggy@127.0.0.1:8090'"
        ),
        default=DEFAULT_CONNECTION_STRING,
        nargs="?",
        type=str,
    )
    return ArgNamespace(**vars(parser.parse_args()))


async def create_stream_topic_and_user(root_client: IggyClient, tenant: Tenant) -> None:
    stream: StreamDetails | None = await root_client.get_stream(tenant.stream)
    if stream is None:
        stream = await root_client.create_stream(name=tenant.stream)
        logger.info(f"Created stream {tenant.stream} with ID {stream.id}")
    else:
        logger.warning(f"Stream {stream.name} already exists with ID {stream.id}")

    if await root_client.get_topic(tenant.stream, TOPIC_NAME) is None:
        await root_client.create_topic(stream=tenant.stream, name=TOPIC_NAME)
        logger.info(f"Created topic {TOPIC_NAME} in stream {tenant.stream}")

    # No global permissions: the tenant can only reach its own stream.
    permissions = Permissions(
        streams={stream.id: StreamPermissions(read_stream=True, send_messages=True)}
    )
    if await root_client.get_user(tenant.user) is None:
        user = await root_client.create_user(
            tenant.user, TENANT_PASSWORD, permissions=permissions
        )
        logger.info(
            f"Created user {tenant.user} with ID {user.id}, "
            f"with permissions for stream {tenant.stream}"
        )
    else:
        await root_client.update_permissions(tenant.user, permissions)
        logger.warning(
            f"User {tenant.user} already exists, "
            f"permissions set for stream {tenant.stream}"
        )


async def send_tenant_messages(client: IggyClient, tenant: Tenant) -> None:
    messages = [
        Message(f"event-{tenant.id}-{message_id}")
        for message_id in range(1, MESSAGES_PER_TENANT + 1)
    ]
    await client.send_messages(
        stream=tenant.stream,
        topic=TOPIC_NAME,
        partitioning=PARTITION_ID,
        messages=messages,
    )
    logger.info(
        f"Sent {len(messages)} message(s) as {tenant.user} "
        f"to stream: {tenant.stream}, topic: {TOPIC_NAME}"
    )


async def main() -> None:
    args = parse_args()
    tenants = make_tenants()
    logger.info(f"Multi-tenant producer has started, tenants: {len(tenants)}")

    root_client = await connect(args.connection_string, "root")
    try:
        for tenant in tenants:
            await create_stream_topic_and_user(root_client, tenant)
    finally:
        await root_client.shutdown()
        logger.info("Root client disconnected")

    for tenant in tenants:
        client = await connect(
            tenant_connection_string(args.connection_string, tenant), tenant.user
        )
        try:
            other_tenants = [other for other in tenants if other.id != tenant.id]
            await ensure_stream_access(client, tenant, other_tenants)
            await send_tenant_messages(client, tenant)
        finally:
            await client.shutdown()

    logger.info("All tenants sent their messages, exiting.")


if __name__ == "__main__":
    asyncio.run(main())
