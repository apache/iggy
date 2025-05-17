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

import socket
import pytest
import time
from typing import Generator
from testcontainers.core.container import DockerContainer

from iggy_py import IggyClient


@pytest.fixture(scope="session")
def iggy_container() -> Generator[DockerContainer, None, None]:
    """
    Creates and starts an Iggy server container using Docker.
    """
    container = DockerContainer("iggyrs/iggy:0.4.21").with_exposed_ports(
        8080, 3000, 8090
    )
    container.start()

    yield container

    container.stop()


@pytest.fixture(scope="session")
async def iggy_client(iggy_container: DockerContainer) -> IggyClient:
    """
    Initializes and returns an Iggy client connected to the running Iggy server container.

    This fixture ensures that the client is authenticated and ready for use in tests.

    :param iggy_container: The running Iggy container fixture.
    :return: An instance of IggyClient connected to the server.
    """
    host = iggy_container.get_container_host_ip()
    port = iggy_container.get_exposed_port(8090)
    wait_for_container(port, host, timeout=30, interval=5)

    client = IggyClient(f"{host}:{port}")

    await client.connect()

    await wait_for_ping(client, timeout=30, interval=5)

    await client.login_user("iggy", "iggy")
    return client


def wait_for_container(port: int, host: str, timeout: int, interval: int) -> None:
    """
    Waits for a container to become alive by polling a specified port.

    :param port: The port number to poll.
    :param host: The hostname or IP address of the container (default is 'localhost').
    :param timeout: The maximum time in seconds to wait for the container to become available (default is 30).
    :param interval: The time in seconds between each polling attempt (default is 2).
    """
    start_time = time.time()

    while True:
        try:
            with socket.create_connection((host, port), timeout=interval):
                return
        except (socket.timeout, ConnectionRefusedError):
            elapsed_time = time.time() - start_time
            if elapsed_time >= timeout:
                raise TimeoutError(
                    f"Timed out after {timeout} seconds waiting for container to become available at {host}:{port}"
                )

            time.sleep(interval)


async def wait_for_ping(
    client: IggyClient, timeout: int = 30, interval: int = 5
) -> None:
    """
    Waits for the Iggy server to respond to ping requests before proceeding.

    :param client: The Iggy client instance.
    :param timeout: The maximum time in seconds to wait for the server to respond.
    :param interval: The time in seconds between each ping attempt.
    :raises TimeoutError: If the server does not respond within the timeout period.
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
                    f"Timed out after {timeout} seconds waiting for Iggy server to respond to ping."
                )

            time.sleep(interval)
