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

from apache_iggy import IggyClient, PendingOperationError

from .utils import (
    get_http_server_config,
    get_quic_server_config,
    get_server_config,
    get_websocket_server_config,
    wait_for_ping,
    wait_for_server,
)

# A request and its retained replay can each spend 30 seconds.
STREAM_CLEANUP_TIMEOUT_SECONDS = 60


@pytest.fixture(scope="session")
async def iggy_client() -> IggyClient:
    """
    Create and configure an Iggy client for testing.

    This fixture:
    1. Gets server configuration from environment
    2. Waits for server to be available
    3. Creates and connects the client
    4. Authenticates with default credentials
    5. Verifies connectivity with ping

    Returns:
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

    return client


@pytest.fixture(params=["tcp", "quic", "http", "websocket"])
async def transport_client(request: pytest.FixtureRequest) -> IggyClient:
    """
    Return a client logged in as root over each transport the SDK exposes.

    Every transport is reached through its connection string, so a test that
    takes this fixture runs once per transport against the same server.
    """
    scheme, (host, port), query = {
        "tcp": ("tcp", get_server_config(), ""),
        "quic": ("quic", get_quic_server_config(), "?reconnection_max_retries=0"),
        "http": ("http", get_http_server_config(), ""),
        "websocket": ("ws", get_websocket_server_config(), ""),
    }[request.param]
    # QUIC listens on UDP, which a TCP probe cannot reach.
    if scheme != "quic":
        wait_for_server(host, port)
    client = IggyClient.from_connection_string(
        f"iggy+{scheme}://iggy:iggy@{host}:{port}{query}"
    )
    await client.connect()
    await wait_for_ping(client)
    await client.login_user("iggy", "iggy")
    return client


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


@pytest.fixture(scope="function", autouse=True)
async def cleanup_streams(
    request: pytest.FixtureRequest,
) -> AsyncGenerator[None, None]:
    """Delete streams created by the current test."""
    if "iggy_client" not in request.fixturenames:
        yield
        return

    client: IggyClient = request.getfixturevalue("iggy_client")
    existing_streams = {
        (stream.id, stream.created_at) for stream in await client.get_streams()
    }

    yield

    for stream in await client.get_streams():
        if (stream.id, stream.created_at) not in existing_streams:
            try:
                async with asyncio.timeout(STREAM_CLEANUP_TIMEOUT_SECONDS):
                    while True:
                        try:
                            await client.delete_stream(stream.id)
                            break
                        except PendingOperationError as error:
                            if error.kind not in ("uncertain", "lane_blocked"):
                                raise
                            await client.resolve_operation(error.operation_id)
                            if error.kind == "uncertain":
                                break
                            if await client.get_stream(stream.id) is None:
                                break

                    if await client.get_stream(stream.id) is not None:
                        raise RuntimeError(
                            f"Stream {stream.id} still exists after deletion"
                        )
            except (RuntimeError, TimeoutError) as error:
                reason = (
                    f"Cleanup failed for stream {stream.id} ({stream.name!r}): "
                    f"{error!r}"
                )
                error.add_note(reason)
                request.session.shouldstop = reason
                raise


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
