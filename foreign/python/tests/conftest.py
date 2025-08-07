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
import socket
import time

import pytest

from apache_iggy import IggyClient


def get_server_config() -> tuple[str, int]:
    """
    Get server configuration from environment variables or defaults.

    Returns:
        tuple: (host, port) for the Iggy server
    """
    host = os.environ.get("IGGY_SERVER_HOST", "127.0.0.1")
    port = int(os.environ.get("IGGY_SERVER_TCP_PORT", "8090"))

    # Convert hostname to IP address for the Rust client
    if host not in ("127.0.0.1", "localhost"):
        try:
            # Resolve hostname to IP address
            host_ip = socket.gethostbyname(host)
            host = host_ip
        except socket.gaierror:
            # If resolution fails, keep the original host
            pass
    elif host == "localhost":
        host = "127.0.0.1"

    return host, port


def wait_for_server(host: str, port: int, timeout: int = 60, interval: int = 2) -> None:
    """
    Wait for the server to become available.

    Args:
        host: Server hostname or IP
        port: Server port
        timeout: Maximum time to wait in seconds
        interval: Time between connection attempts in seconds

    Raises:
        TimeoutError: If server doesn't become available within timeout
    """
    start_time = time.time()

    while True:
        try:
            with socket.create_connection((host, port), timeout=interval):
                return
        except (socket.timeout, ConnectionRefusedError, OSError):
            elapsed_time = time.time() - start_time
            if elapsed_time >= timeout:
                raise TimeoutError(
                    f"Server not available at {host}:{port} after {timeout}s"
                )
            time.sleep(interval)


async def wait_for_ping(client: IggyClient, timeout: int = 30, interval: int = 2) -> None:
    """
    Wait for the server to respond to ping requests.

    Args:
        client: Iggy client instance
        timeout: Maximum time to wait in seconds
        interval: Time between ping attempts in seconds

    Raises:
        TimeoutError: If server doesn't respond to ping within timeout
    """
    start_time = time.time()

    while True:
        try:
            await client.ping()
            return
        except Exception:
            elapsed_time = time.time() - start_time
            if elapsed_time >= timeout:
                raise TimeoutError(
                    f"Server not responding to ping after {timeout}s"
                )
            await asyncio.sleep(interval)


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


@pytest.fixture(scope="session", autouse=True)
def configure_asyncio():
    """Configure asyncio settings for tests."""
    # Set event loop policy if needed
    if os.name == 'nt':  # Windows
        asyncio.set_event_loop_policy(asyncio.WindowsProactorEventLoopPolicy())


# Pytest configuration
def pytest_configure(config):
    """Configure pytest with custom markers."""
    config.addinivalue_line(
        "markers",
        "integration: marks tests as integration tests (may be slow)"
    )
    config.addinivalue_line(
        "markers",
        "unit: marks tests as unit tests (fast)"
    )


def pytest_collection_modifyitems(config, items):
    """Modify test collection to add markers automatically."""
    for item in items:
        # Mark all tests in test_iggy_sdk.py as integration tests
        if "test_iggy_sdk" in item.nodeid:
            item.add_marker(pytest.mark.integration)
