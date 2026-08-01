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
Utility functions for tests.
"""

import asyncio
import os
import socket
import time

from apache_iggy import IggyClient


def get_transport_config(port_env_var: str, default_port: int) -> tuple[str, int]:
    """
    Get transport-specific server configuration from environment variables or defaults.

    Args:
        port_env_var: Name of the environment variable holding the port.
        default_port: Port to use if the environment variable is not set.

    Returns:
        tuple: (host, port) for the Iggy server
    """
    host = os.environ.get("IGGY_SERVER_HOST", "127.0.0.1")
    port = int(os.environ.get(port_env_var, str(default_port)))

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


def get_server_config() -> tuple[str, int]:
    """
    Get TCP server configuration from environment variables or defaults.

    Returns:
        tuple: (host, port) for the Iggy server
    """
    return get_transport_config("IGGY_SERVER_TCP_PORT", 8090)


def get_quic_server_config() -> tuple[str, int]:
    """
    Get QUIC server configuration from environment variables or defaults.

    Returns:
        tuple: (host, port) for the Iggy server
    """
    return get_transport_config("IGGY_SERVER_QUIC_PORT", 8080)


def get_http_server_config() -> tuple[str, int]:
    """
    Get HTTP server configuration from environment variables or defaults.

    Returns:
        tuple: (host, port) for the Iggy server
    """
    return get_transport_config("IGGY_SERVER_HTTP_PORT", 3000)


def get_websocket_server_config() -> tuple[str, int]:
    """
    Get WebSocket server configuration from environment variables or defaults.

    Returns:
        tuple: (host, port) for the Iggy server
    """
    return get_transport_config("IGGY_SERVER_WS_PORT", 8092)


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
        except (TimeoutError, ConnectionRefusedError, OSError) as err:
            elapsed_time = time.time() - start_time
            if elapsed_time >= timeout:
                raise TimeoutError(
                    f"Server not available at {host}:{port} after {timeout}s"
                ) from err
            time.sleep(interval)


async def wait_for_ping(
    client: IggyClient, timeout: int = 30, interval: int = 2
) -> None:
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
        except Exception as err:
            elapsed_time = time.time() - start_time
            if elapsed_time >= timeout:
                raise TimeoutError(
                    f"Server not responding to ping after {timeout}s"
                ) from err
            await asyncio.sleep(interval)
