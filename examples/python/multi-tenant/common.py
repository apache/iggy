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

import os
from dataclasses import dataclass
from urllib.parse import urlsplit, urlunsplit

from apache_iggy import Consumer, IggyClient, PollingStrategy
from loguru import logger

DEFAULT_CONNECTION_STRING = "iggy+tcp://iggy:iggy@127.0.0.1:8090"
DEFAULT_TENANTS_COUNT = 3
TOPIC_NAME = "events"
PARTITION_ID = 0
MESSAGES_PER_BATCH = 10
# Shared by all tenant users. Hard-coded for the example only.
TENANT_PASSWORD = "secret"  # noqa: S105


@dataclass(frozen=True, slots=True)
class Tenant:
    id: int
    stream: str
    user: str


def make_tenants() -> list[Tenant]:
    """Builds the tenants, their count is read from `TENANTS_COUNT` (default 3)."""
    raw_count = os.environ.get("TENANTS_COUNT", str(DEFAULT_TENANTS_COUNT))
    try:
        count = int(raw_count)
    except ValueError:
        count = 0
    if count < 1:
        raise SystemExit(f"TENANTS_COUNT must be a positive integer, got {raw_count!r}")

    return [
        Tenant(
            id=tenant_id,
            stream=f"tenant_{tenant_id}_stream",
            user=f"tenant_{tenant_id}_user",
        )
        for tenant_id in range(1, count + 1)
    ]


def tenant_connection_string(connection_string: str, tenant: Tenant) -> str:
    """Keeps the scheme, host and port, and swaps in the tenant's credentials."""
    parts = urlsplit(connection_string)
    host = parts.netloc.rpartition("@")[2]
    return urlunsplit(parts._replace(netloc=f"{tenant.user}:{TENANT_PASSWORD}@{host}"))


async def connect(connection_string: str, label: str) -> IggyClient:
    client = IggyClient.from_connection_string(connection_string)
    logger.info(f"Connecting to Iggy as {label}")
    await client.connect()
    logger.info("Connected")
    return client


async def ensure_stream_access(
    client: IggyClient, tenant: Tenant, other_tenants: list[Tenant]
) -> None:
    """Checks that the tenant sees its own stream and nobody else's."""
    own_stream = await client.get_stream(tenant.stream)
    if own_stream is None:
        raise RuntimeError(f"{tenant.user} has no access to stream {tenant.stream}")
    logger.info(f"Ensured access to stream: {tenant.stream}")

    for other in other_tenants:
        await ensure_no_access(client, tenant, other.stream)


async def ensure_no_access(client: IggyClient, tenant: Tenant, stream: str) -> None:
    try:
        details = await client.get_stream(stream)
    except RuntimeError as error:
        logger.info(
            f"Ensured no access to stream: {stream} "
            f"(get_stream refused: {type(error).__name__}: {error})"
        )
    else:
        if details is not None:
            raise RuntimeError(f"{tenant.user} must not see stream {stream}")
        logger.info(f"Ensured no access to stream: {stream} (not visible)")

    try:
        await client.poll_messages(
            stream=stream,
            topic=TOPIC_NAME,
            partition_id=PARTITION_ID,
            consumer=Consumer.Single(tenant.user),
            polling_strategy=PollingStrategy.First(),
        )
    except RuntimeError as error:
        logger.info(
            f"Ensured no read access to stream: {stream} "
            f"(poll refused: {type(error).__name__}: {error})"
        )
    else:
        raise RuntimeError(f"{tenant.user} must not read from stream {stream}")
