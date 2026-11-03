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

from apache_iggy import Consumer, IggyClient, PollingStrategy, PollOptions
from common import (
    DEFAULT_CONNECTION_STRING,
    MESSAGES_PER_BATCH,
    PARTITION_ID,
    TOPIC_NAME,
    Tenant,
    connect,
    ensure_stream_access,
    make_tenants,
    tenant_connection_string,
)
from loguru import logger


@dataclass(frozen=True, slots=True)
class ArgNamespace:
    connection_string: str
    tenant_id: int


def parse_args() -> ArgNamespace:
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "connection_string",
        help=(
            "Connection string used to locate the server, e.g. "
            "'iggy+tcp://iggy:iggy@127.0.0.1:8090'. Its credentials are "
            "replaced by those of the tenant user."
        ),
        default=DEFAULT_CONNECTION_STRING,
        nargs="?",
        type=str,
    )
    parser.add_argument(
        "--tenant-id",
        help="Tenant to consume as, from 1 to TENANTS_COUNT (default: 1)",
        default=1,
        type=int,
    )
    return ArgNamespace(**vars(parser.parse_args()))


async def consume_messages(client: IggyClient, tenant: Tenant) -> None:
    consumer_name = f"{tenant.user}-consumer"
    logger.info(
        f"Messages will be consumed from stream: {tenant.stream}, "
        f"topic: {TOPIC_NAME}, partition: {PARTITION_ID} "
        f"as consumer: {consumer_name}"
    )
    consumer = Consumer.Single(consumer_name)
    strategy = PollingStrategy.First()
    consumed = 0

    while True:
        polled = await client.poll_messages(
            stream=tenant.stream,
            topic=TOPIC_NAME,
            partition_id=PARTITION_ID,
            consumer=consumer,
            polling_strategy=strategy,
            options=PollOptions(count=MESSAGES_PER_BATCH),
        )
        if not polled.messages:
            break

        for message in polled.messages:
            payload = message.payload().decode("utf-8")
            logger.info(
                f"Tenant {tenant.id} received: {payload} "
                f"from stream: {tenant.stream}, at offset: {message.offset()}"
            )
        consumed += len(polled.messages)
        strategy = polled.continuation()

    if consumed == 0:
        logger.warning("No messages found, run producer.py first")
    logger.info(f"Consumed {consumed} message(s) from {tenant.stream}, exiting.")


async def main() -> None:
    args = parse_args()
    tenants = make_tenants()
    if not 1 <= args.tenant_id <= len(tenants):
        raise SystemExit(
            f"--tenant-id must be between 1 and {len(tenants)} (TENANTS_COUNT)"
        )

    tenant = tenants[args.tenant_id - 1]
    logger.info(f"Multi-tenant consumer has started, tenant: {tenant.id}")

    client = await connect(
        tenant_connection_string(args.connection_string, tenant), tenant.user
    )
    try:
        other_tenants = [other for other in tenants if other.id != tenant.id]
        await ensure_stream_access(client, tenant, other_tenants)
        await consume_messages(client, tenant)
    finally:
        await client.shutdown()
        logger.info("Disconnected from server.")


if __name__ == "__main__":
    asyncio.run(main())
