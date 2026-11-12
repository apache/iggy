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

from apache_iggy import (
    CommitPolicy,
    Consumer,
    IggyClient,
    Permissions,
    PollingStrategy,
    PollOptions,
    StreamPermissions,
)
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

CONSUMER_NAME = "multi-tenant"
MESSAGES_PER_POLL = 100


@dataclass(frozen=True, slots=True)
class ArgNamespace:
    connection_string: str
    tenants_count: int
    tenant_id: int


def parse_args() -> ArgNamespace:
    parser = build_parser(
        "TENANTS_COUNT sets the number of tenants (default 3). "
        "TENANT_ID sets the tenant that the consumer reads (default 1)."
    )
    args = parser.parse_args()
    tenants_count = read_positive_int(parser, "TENANTS_COUNT", "3")
    tenant_id = read_positive_int(parser, "TENANT_ID", "1")
    if tenant_id > tenants_count:
        parser.error(f"TENANT_ID {tenant_id} is above TENANTS_COUNT {tenants_count}")
    return ArgNamespace(args.connection_string, tenants_count, tenant_id)


async def main() -> None:
    args = parse_args()
    logger.info(
        f"Multi-tenant consumer has started, tenant: {args.tenant_id}, "
        f"tenants: {args.tenants_count}"
    )

    logger.info("Connecting as root to create a read-only user for the tenant")
    root = await connect(args.connection_string)
    await create_consumer_user(root, args.tenant_id)
    await root.shutdown()

    client = await connect_as(args.connection_string, consumer_username(args.tenant_id))
    await ensure_stream_access(client, args.tenant_id, args.tenants_count)
    await consume_messages(client, args.tenant_id)
    await client.shutdown()

    logger.info("Multi-tenant consumer has finished")


async def create_consumer_user(root: IggyClient, tenant_id: int) -> None:
    name = stream_name(tenant_id)
    stream = await root.get_stream(name)
    if stream is None:
        raise RuntimeError(f"Stream {name} does not exist, run the producer first")

    # Reading the stream includes polling its topics, storing consumer offsets,
    # and creating or deleting consumer groups in the stream. It does not allow
    # sending messages or changing topics.
    permissions = Permissions(streams={stream.id: StreamPermissions(read_stream=True)})
    await ensure_user(root, consumer_username(tenant_id), permissions)


async def consume_messages(client: IggyClient, tenant_id: int) -> None:
    stream = stream_name(tenant_id)
    for topic in TOPICS:
        consumed = 0
        # The server stores the offset after each poll, so the next poll and the
        # next run continue after the last message read.
        while True:
            polled = await client.poll_messages(
                stream,
                topic,
                partition_id=PARTITION_ID,
                consumer=Consumer.Single(CONSUMER_NAME),
                polling_strategy=PollingStrategy.Next(),
                options=PollOptions(
                    count=MESSAGES_PER_POLL,
                    commit=CommitPolicy.OnRead(),
                ),
            )
            if not polled.messages:
                break
            for message in polled.messages:
                # The offset of this batch is already stored, so a payload that
                # is not UTF-8 is logged with replacement characters instead of
                # stopping the run and skipping the rest of the batch.
                payload = message.payload().decode("utf-8", errors="replace")
                logger.info(
                    f"Tenant {tenant_id} received {payload} from {stream}/{topic} "
                    f"at offset {message.offset()}"
                )
            consumed += len(polled.messages)
        logger.info(f"Tenant {tenant_id} consumed {consumed} messages from {topic}")


def consumer_username(tenant_id: int) -> str:
    return f"tenant_{tenant_id}_consumer"


if __name__ == "__main__":
    asyncio.run(main())
