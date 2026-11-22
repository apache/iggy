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
import os

from apache_iggy import IggyClient, IggyError, Permissions
from loguru import logger

TOPICS = ("events", "logs", "notifications")
# One fixed password keeps the example short and lets it run again against the
# same server. Real tenants would each get their own secret.
TENANT_PASSWORD = "secret"  # noqa: S105
PARTITION_ID = 0
UNAUTHORIZED = 41


def build_parser(environment_help: str) -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        epilog=f"Environment variables: {environment_help}"
    )
    parser.add_argument(
        "connection_string",
        help=(
            "Connection string of the root user, e.g. "
            "'iggy+tcp://iggy:iggy@127.0.0.1:8090'"
        ),
        default="iggy+tcp://iggy:iggy@127.0.0.1:8090",
        nargs="?",
        type=str,
    )
    return parser


def read_positive_int(parser: argparse.ArgumentParser, name: str, default: str) -> int:
    value = os.environ.get(name, default)
    if not value.isdecimal() or int(value) < 1:
        parser.error(f"{name} must be a positive integer, got {value!r}")
    return int(value)


def stream_name(tenant_id: int) -> str:
    return f"tenant_{tenant_id}_stream"


async def connect(connection_string: str) -> IggyClient:
    client = IggyClient.from_connection_string(connection_string)
    await client.connect()
    return client


async def connect_as(root_connection_string: str, username: str) -> IggyClient:
    logger.info(f"Connecting as {username}")
    return await connect(tenant_connection_string(root_connection_string, username))


async def ensure_user(
    root: IggyClient, username: str, permissions: Permissions
) -> None:
    if await root.get_user(username) is None:
        await root.create_user(username, TENANT_PASSWORD, permissions=permissions)
        logger.info(f"Created user {username}")
        return

    # The stream may have been deleted and created again under a new ID since
    # the last run, so the stored permissions are replaced, not trusted.
    await root.update_permissions(username, permissions)
    logger.warning(f"User {username} already exists, its permissions were updated")


async def ensure_stream_access(
    client: IggyClient, tenant_id: int, tenants_count: int
) -> None:
    own_stream = stream_name(tenant_id)
    if await client.get_stream(own_stream) is None:
        raise RuntimeError(f"Tenant {tenant_id} cannot access stream {own_stream}")
    logger.info(f"Tenant {tenant_id} can access stream {own_stream}")

    for other_id in range(1, tenants_count + 1):
        if other_id == tenant_id:
            continue
        other_stream = stream_name(other_id)
        try:
            stream = await client.get_stream(other_stream)
        except IggyError as error:
            if error.code != UNAUTHORIZED:
                raise
            logger.info(
                f"Tenant {tenant_id} has no access to stream {other_stream} "
                "(unauthorized)"
            )
            continue
        if stream is not None:
            raise RuntimeError(
                f"Tenant {tenant_id} must not have access to stream {other_stream}"
            )
        logger.info(
            f"Tenant {tenant_id} has no access to stream {other_stream} (not visible)"
        )


def tenant_connection_string(root_connection_string: str, username: str) -> str:
    # Each tenant signs in as its own user, so only the credentials change. The
    # string is split the way the SDK parser splits it, at "://" and at "@", so
    # a root password with "/", "?" or "#" keeps the address intact. The SDK
    # does not decode percent escapes, so the credentials go in as they are.
    scheme, _, rest = root_connection_string.partition("://")
    address = rest.rpartition("@")[2]
    return f"{scheme}://{username}:{TENANT_PASSWORD}@{address}"
