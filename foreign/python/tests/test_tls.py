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
Integration tests for TLS connectivity using testcontainers.

These tests spin up a TLS-enabled Iggy server in a Docker container
so they are fully self-contained and work in CI without a pre-running
TLS server.

Requirements:
    - Docker running locally
    - testcontainers[docker] installed (in [testing-docker] extras)
    - CA certificate available at core/certs/iggy_ca_cert.pem
"""

import os
import uuid

import pytest
from apache_iggy import IggyClient, PollingStrategy
from apache_iggy import SendMessage as Message
from testcontainers.core.container import DockerContainer
from testcontainers.core.waiting_utils import wait_for_logs

from .utils import wait_for_ping, wait_for_server

# Paths resolved relative to this file → repo root
REPO_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "..", ".."))
CERTS_DIR = os.path.join(REPO_ROOT, "core", "certs")
CA_FILE = os.path.join(CERTS_DIR, "iggy_ca_cert.pem")

IGGY_IMAGE = os.environ.get("IGGY_SERVER_DOCKER_IMAGE", "apache/iggy:edge")
CONTAINER_TCP_PORT = 8090


@pytest.fixture(scope="module")
def tls_container():
    """Start a TLS-enabled Iggy server in Docker."""
    container = (
        DockerContainer(IGGY_IMAGE)
        .with_exposed_ports(CONTAINER_TCP_PORT)
        .with_env("IGGY_ROOT_USERNAME", "iggy")
        .with_env("IGGY_ROOT_PASSWORD", "iggy")
        .with_env("IGGY_TCP_TLS_ENABLED", "true")
        .with_env("IGGY_TCP_TLS_CERT_FILE", "/app/certs/iggy_cert.pem")
        .with_env("IGGY_TCP_TLS_KEY_FILE", "/app/certs/iggy_key.pem")
        .with_env("IGGY_TCP_ADDRESS", f"0.0.0.0:{CONTAINER_TCP_PORT}")
        .with_volume_mapping(CERTS_DIR, "/app/certs", "ro")
        .with_kwargs(privileged=True)
    )
    container.start()
    # Wait for the server to be ready inside the container
    wait_for_logs(container, "Iggy server is running", timeout=60)
    yield container
    container.stop()


@pytest.fixture(scope="module")
async def tls_client(tls_container) -> IggyClient:
    """Create an authenticated client connected to the TLS container."""
    host = "localhost"
    port = tls_container.get_exposed_port(CONTAINER_TCP_PORT)

    wait_for_server(host, int(port))

    conn_str = (
        f"iggy+tcp://iggy:iggy@{host}:{port}"
        f"?tls=true&tls_domain={host}&tls_ca_file={CA_FILE}"
    )
    client = IggyClient.from_connection_string(conn_str)
    await client.connect()
    await wait_for_ping(client)
    await client.login_user("iggy", "iggy")
    return client


@pytest.mark.integration
class TestTlsConnectivity:
    """Test TLS connection establishment and basic operations."""

    @pytest.mark.asyncio
    async def test_ping_over_tls(self, tls_client: IggyClient):
        """Test that the server responds to ping over a TLS connection."""
        await tls_client.ping()

    @pytest.mark.asyncio
    async def test_client_not_none(self, tls_client: IggyClient):
        """Test that the TLS client fixture is properly initialized."""
        assert tls_client is not None

    @pytest.mark.asyncio
    async def test_create_stream_over_tls(self, tls_client: IggyClient):
        """Test creating and getting a stream over TLS."""
        stream_name = f"tls-test-stream-{uuid.uuid4().hex[:8]}"
        await tls_client.create_stream(stream_name)
        stream = await tls_client.get_stream(stream_name)
        assert stream is not None

    @pytest.mark.asyncio
    async def test_produce_and_consume_over_tls(self, tls_client: IggyClient):
        """Test producing and consuming messages over TLS."""
        stream_name = f"tls-msg-stream-{uuid.uuid4().hex[:8]}"
        topic_name = "tls-test-topic"
        partition_id = 0

        # Create stream and topic
        await tls_client.create_stream(stream_name)
        await tls_client.create_topic(
            stream_name, topic_name, partitions_count=1
        )

        # Produce messages
        test_messages = [f"tls-message-{i}" for i in range(3)]
        messages = [Message(msg) for msg in test_messages]
        await tls_client.send_messages(
            stream=stream_name,
            topic=topic_name,
            partitioning=partition_id,
            messages=messages,
        )

        # Consume messages
        polled = await tls_client.poll_messages(
            stream=stream_name,
            topic=topic_name,
            partition_id=partition_id,
            polling_strategy=PollingStrategy.First(),
            count=10,
            auto_commit=True,
        )
        assert len(polled) >= len(test_messages)

        # Verify message payloads
        for i, expected_msg in enumerate(test_messages):
            if i < len(polled):
                assert polled[i].payload().decode("utf-8") == expected_msg


@pytest.mark.integration
class TestTlsConnectionString:
    """Test TLS connection string variations."""

    @pytest.mark.asyncio
    async def test_connection_string_with_tls_params(self, tls_container):
        """Test creating a client from a connection string with TLS parameters."""
        host = "localhost"
        port = tls_container.get_exposed_port(CONTAINER_TCP_PORT)

        wait_for_server(host, int(port))

        conn_str = (
            f"iggy+tcp://iggy:iggy@{host}:{port}"
            f"?tls=true&tls_domain={host}&tls_ca_file={CA_FILE}"
        )
        client = IggyClient.from_connection_string(conn_str)
        await client.connect()
        await wait_for_ping(client)
        await client.ping()

    @pytest.mark.asyncio
    async def test_connect_without_tls_should_fail(self, tls_container):
        """Test that connecting without TLS to a TLS-enabled server fails."""
        host = "localhost"
        port = tls_container.get_exposed_port(CONTAINER_TCP_PORT)

        wait_for_server(host, int(port))

        # Connect without TLS to a TLS-enabled server
        # IggyClient constructor requires IP address, not hostname
        client = IggyClient(f"127.0.0.1:{port}")
        await client.connect()

        with pytest.raises(RuntimeError):
            await client.login_user("iggy", "iggy")
