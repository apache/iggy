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
BDD test configuration and fixtures for Python SDK tests.
"""

import asyncio
import os
import socket
from dataclasses import dataclass

import pytest
from apache_iggy import (
    IggyClient,
    IggyExpiry,
    MetadataChangeCursor,
    MetadataChangeGroup,
    MetadataChangeRecord,
    MetadataCurrentItem,
    OptionSpec,
    ReceiveMessage,
    StreamOptions,
    TopicOptions,
)
from pytest_bdd import given, parsers, when

STREAM_KEY_LENGTH = 32


@dataclass
class GlobalContext:
    """Global test context similar to Rust implementation."""

    client: IggyClient | None = None
    server_addr: str | None = None
    last_stream_id: int | None = None
    last_stream_name: str | None = None
    last_topic_id: int | None = None
    last_topic_name: str | None = None
    last_topic_partitions: int | None = None
    last_polled_messages: list[ReceiveMessage] | None = None
    last_sent_message: str | None = None  # Store message payload as string
    last_raw_response: bytes | None = None
    last_raw_error: RuntimeError | None = None
    last_stream_key: bytes | None = None
    last_update_error: RuntimeError | None = None
    last_option_specs: list[OptionSpec] | None = None
    last_user_id: int | None = None
    last_call_error: RuntimeError | None = None
    last_change_groups: list[MetadataChangeGroup] | None = None
    last_change_cursor: MetadataChangeCursor | None = None
    last_changes: list[MetadataChangeRecord] | None = None
    last_current_items: list[MetadataCurrentItem] | None = None


def pytest_configure(config: pytest.Config) -> None:
    """Register the shared feature tags as markers.

    The Docker image runs pytest from /app, where bdd/python/pyproject.toml is
    not the rootdir config, so markers declared there would never load.
    """
    for marker in (
        "basic-messaging: shared basic messaging scenarios",
        "raw-command: shared raw command scenarios",
        "session-contract: shared session contract scenarios",
        "stream-options: shared stream option scenarios",
        "resource-options-patch: shared resource patch scenarios",
        "metadata-changes: shared metadata change log scenarios",
        "serial: scenarios that must not share the server with other runs",
        "message-headers: scenarios that assert message headers",
        "describe-options: scenarios that assert the option catalog",
    ):
        config.addinivalue_line("markers", marker)


def required_env(name: str) -> str:
    """Read a variable the suite cannot run without.

    A default here would turn a dropped compose variable into a run against
    whatever happens to listen on the fallback address, so a missing value
    aborts the suite instead.
    """
    value = os.environ.get(name)
    if not value:
        raise RuntimeError(
            f"{name} must be set; run the suite via scripts/run-bdd-tests.sh"
        )
    return value


@pytest.fixture(scope="session")
def root_credentials() -> tuple[str, str]:
    """Root username and password the server was started with."""
    return required_env("IGGY_ROOT_USERNAME"), required_env("IGGY_ROOT_PASSWORD")


@pytest.fixture(scope="session")
def no_quorum_address() -> str:
    """Address of the cluster node that cannot reach quorum."""
    return required_env("IGGY_TCP_ADDRESS_NO_QUORUM")


@pytest.fixture(scope="session")
def event_loop():
    """Create an instance of the default event loop for the test session."""
    loop = asyncio.get_event_loop_policy().new_event_loop()
    yield loop
    loop.close()


@pytest.fixture(scope="function")
def context():
    """Create a fresh context for each test scenario."""
    ctx = GlobalContext()

    ctx.server_addr = required_env("IGGY_TCP_ADDRESS")

    yield ctx


# Steps shared by the stream options and metadata changes features. A test
# module that defines the same step text keeps its own definition.


@given("I have a running Iggy server")
def running_server(context):
    async def connect():
        host, port = context.server_addr.split(":")
        try:
            address = f"{socket.gethostbyname(host)}:{port}"
        except socket.gaierror:
            address = context.server_addr

        context.client = IggyClient(address)
        await context.client.connect()
        await context.client.ping()

    asyncio.run(connect())


@given("I am authenticated as the root user")
def authenticated_root_user(context, root_credentials):
    async def login():
        await context.client.login_user(*root_credentials)

    asyncio.run(login())


@when(
    parsers.re(
        r'I create a stream named "(?P<name>[^"]+)" with an encryption key '
        r"of 32 bytes valued (?P<byte>\d+)"
    ),
    converters={"byte": int},
)
def create_stream_with_key(context, name, byte):
    create_keyed_stream(context, name, byte)


@given(
    parsers.re(
        r'a stream named "(?P<name>[^"]+)" with an encryption key '
        r"of 32 bytes valued (?P<byte>\d+) exists"
    ),
    converters={"byte": int},
)
def keyed_stream_exists(context, name, byte):
    create_keyed_stream(context, name, byte)


@when(
    parsers.re(r'I create a stream named "(?P<name>[^"]+)" without an encryption key')
)
def create_stream_without_key(context, name):
    async def create():
        stream = await context.client.create_stream(name)
        context.last_stream_id = stream.id
        context.last_stream_name = stream.name
        context.last_stream_key = None

    asyncio.run(create())


@when(
    parsers.re(
        r'I create a topic named "(?P<name>[^"]+)" with (?P<partitions>\d+) '
        r"partitions? in the stream"
    ),
    converters={"partitions": int},
)
def create_topic_in_stream(context, name, partitions):
    async def create():
        await context.client.create_topic(
            context.last_stream_id,
            name,
            TopicOptions(
                partitions_count=partitions,
                message_expiry=IggyExpiry.NeverExpire(),
            ),
        )
        topic = await context.client.get_topic(context.last_stream_id, name)
        assert topic is not None
        context.last_topic_id = topic.id
        context.last_topic_name = topic.name
        context.last_topic_partitions = partitions

    asyncio.run(create())


@when(parsers.re(r'I rename the stream to "(?P<name>[^"]+)"'))
def rename_stream(context, name):
    async def rename():
        await context.client.update_stream(context.last_stream_id, name=name)

    asyncio.run(rename())


def create_keyed_stream(context, name: str, byte: int) -> None:
    key = bytes([byte]) * STREAM_KEY_LENGTH

    async def create():
        stream = await context.client.create_stream(
            name, StreamOptions(encryption_key=key)
        )
        context.last_stream_id = stream.id
        context.last_stream_name = stream.name
        context.last_stream_key = key

    asyncio.run(create())
