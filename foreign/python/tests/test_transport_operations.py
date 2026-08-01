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
Integration tests for stream creation and message produce/consume over the QUIC, HTTP,
and WebSocket transports, parametrized like the other per-feature test files.

TCP already has equivalent coverage via the session-scoped `iggy_client` fixture used
throughout test_stream.py and test_message_operations.py; basic connect+ping coverage
for all four transports (including the explicit constructors added alongside this file)
lives in test_connectivity.py.
"""

import pytest

from apache_iggy import IggyClient, PollingStrategy
from apache_iggy import SendMessage as Message

from .utils import (
    get_http_server_config,
    get_quic_server_config,
    get_websocket_server_config,
    wait_for_ping,
    wait_for_server,
)


async def _quic_client() -> IggyClient:
    host, port = get_quic_server_config()
    client = IggyClient.quic(server_address=f"{host}:{port}")
    await client.connect()
    await wait_for_ping(client)
    await client.login_user("iggy", "iggy")
    return client


async def _http_client() -> IggyClient:
    host, port = get_http_server_config()
    wait_for_server(host, port)
    client = IggyClient.http(api_url=f"http://{host}:{port}")
    await client.connect()
    await wait_for_ping(client)
    await client.login_user("iggy", "iggy")
    return client


async def _websocket_client() -> IggyClient:
    host, port = get_websocket_server_config()
    wait_for_server(host, port)
    client = IggyClient.websocket(server_address=f"{host}:{port}")
    await client.connect()
    await wait_for_ping(client)
    await client.login_user("iggy", "iggy")
    return client


TRANSPORT_CLIENT_FACTORIES = {
    "quic": _quic_client,
    "http": _http_client,
    "websocket": _websocket_client,
}


@pytest.fixture
async def transport_client(request) -> IggyClient:
    """Build an authenticated client for the transport named by the parametrize id."""
    return await TRANSPORT_CLIENT_FACTORIES[request.param]()


@pytest.mark.integration
class TestTransportOperations:
    """Test stream creation and message produce/consume over QUIC, HTTP, WebSocket."""

    @pytest.mark.parametrize(
        "transport_client", ["quic", "http", "websocket"], indirect=True
    )
    @pytest.mark.asyncio
    async def test_create_stream(self, transport_client: IggyClient, unique_name):
        """Test creating and getting a stream over the given transport."""
        stream_name = unique_name()
        await transport_client.create_stream(stream_name)
        stream = await transport_client.get_stream(stream_name)
        assert stream is not None

    @pytest.mark.parametrize(
        "transport_client", ["quic", "http", "websocket"], indirect=True
    )
    @pytest.mark.asyncio
    async def test_produce_and_consume(self, transport_client: IggyClient, unique_name):
        """Test producing and consuming messages over the given transport."""
        stream_name = unique_name()
        topic_name = unique_name()
        partition_id = 0

        await transport_client.create_stream(stream_name)
        await transport_client.create_topic(stream_name, topic_name, partitions_count=1)

        test_messages = [f"message-{i}" for i in range(3)]
        messages = [Message(msg) for msg in test_messages]
        await transport_client.send_messages(
            stream=stream_name,
            topic=topic_name,
            partitioning=partition_id,
            messages=messages,
        )

        polled = await transport_client.poll_messages(
            stream=stream_name,
            topic=topic_name,
            partition_id=partition_id,
            polling_strategy=PollingStrategy.First(),
            count=10,
            auto_commit=True,
        )
        assert len(polled) >= len(test_messages)

        for i, expected_msg in enumerate(test_messages):
            if i < len(polled):
                assert polled[i].payload().decode("utf-8") == expected_msg
