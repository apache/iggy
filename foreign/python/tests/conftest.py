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
Test configuration and fixtures for the Python SDK test suite.

This module provides pytest fixtures for setting up test environments
and connecting to Iggy servers in various configurations.
"""

import asyncio
import os
import secrets
import string
from collections.abc import AsyncGenerator
from pathlib import Path

import pytest

from apache_iggy import IggyClient

from .utils import get_server_config, wait_for_ping, wait_for_server

_ROOT_USER_ID = 0


async def _delete_test_resources(client: IggyClient) -> None:
    streams = await client.get_streams()

    for stream in streams:
        topics = await client.get_topics(stream.id)
        for topic in topics:
            consumer_groups = await client.get_consumer_groups(stream.id, topic.id)
            for consumer_group in consumer_groups:
                await client.delete_consumer_group(
                    stream.id, topic.id, consumer_group.id
                )

    for stream in streams:
        await client.delete_stream(stream.id)

    users = await client.get_users()
    for user in users:
        if user.id != _ROOT_USER_ID:
            await client.delete_user(user.id)


@pytest.fixture(scope="session", autouse=True)
async def iggy_client() -> AsyncGenerator[IggyClient, None]:
    """
    Create and configure an Iggy client for testing.

    This fixture:
    1. Gets server configuration from environment
    2. Waits for server to be available
    3. Creates and connects the client
    4. Authenticates with default credentials
    5. Verifies connectivity with ping

    Yields:
        IggyClient: Authenticated client ready for testing
    """
    host, port = get_server_config()

    # Wait for server to be ready
    wait_for_server(host, port)

    # Create and connect client
    client = IggyClient(f"{host}:{port}")
    await client.connect()

    # Wait for server to be fully ready
    await wait_for_ping(client)

    # Authenticate
    await client.login_user("iggy", "iggy")

    yield client

    await _delete_test_resources(client)


@pytest.fixture
def unique_name():
    """Return a factory for generating unique test names."""

    def make_name(
        prefix: str = "",
        *,
        min_bytes: int = 8,
        max_bytes: int = 255,
    ) -> str:
        if min_bytes < 0:
            raise ValueError("min_bytes must be non-negative")
        if max_bytes < 0:
            raise ValueError("max_bytes must be non-negative")
        if max_bytes < min_bytes:
            raise ValueError("max_bytes must be greater than or equal to min_bytes")

        prefix_bytes = len(prefix.encode())
        if prefix_bytes > max_bytes:
            raise ValueError("prefix exceeds max_bytes")

        min_suffix_bytes = max(0, min_bytes - prefix_bytes)
        max_suffix_bytes = max_bytes - prefix_bytes
        if min_suffix_bytes > max_suffix_bytes:
            raise ValueError("prefix leaves insufficient room within byte bounds")

        alphabet = string.ascii_lowercase + string.digits
        suffix_bytes = (
            secrets.randbelow(max_suffix_bytes - min_suffix_bytes + 1)
            + min_suffix_bytes
        )
        suffix = "".join(secrets.choice(alphabet) for _ in range(suffix_bytes))
        return f"{prefix}{suffix}"

    return make_name


@pytest.fixture(scope="session", autouse=True)
def configure_asyncio():
    """Configure asyncio settings for tests."""
    # Set event loop policy if needed
    if os.name == "nt":  # Windows
        asyncio.set_event_loop_policy(asyncio.WindowsProactorEventLoopPolicy())


# Pytest configuration
def pytest_configure(config):
    """Configure pytest with custom markers."""
    config.addinivalue_line(
        "markers", "integration: marks tests as integration tests (may be slow)"
    )
    config.addinivalue_line("markers", "unit: marks tests as unit tests (fast)")


def pytest_collection_modifyitems(items):
    """Modify test collection to add markers automatically."""
    integration_modules = {
        path.name for path in Path(__file__).parent.glob("test_*.py")
    }
    for item in items:
        # Tests explicitly marked as unit need no server; auto-marking them
        # integration too would make `-m "not integration"` unable to select
        # them.
        if item.get_closest_marker("unit"):
            continue
        if any(module in item.nodeid for module in integration_modules):
            item.add_marker(pytest.mark.integration)
