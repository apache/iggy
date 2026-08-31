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
Tests for the HTTP client configuration surface.

`HttpConfig` mirrors the Rust SDK's `HttpClientConfig` the same way
`TcpConfig`/`QuicConfig` do, so most of these assert that a value set from
Python survives to the getters and that unset fields fall back to the Rust
defaults. HTTP is stateless per-request, so unlike TCP/QUIC there is no
`AutoLogin` or reconnection policy to configure.
"""

import ast
from datetime import timedelta

import pytest

from apache_iggy import Consumer, HttpConfig, IggyClient, PollingStrategy
from apache_iggy import SendMessage as Message

from .utils import get_http_server_config, wait_for_ping


@pytest.mark.unit
class TestHttpConfig:
    """Test the transport configuration."""

    def test_defaults_match_the_rust_sdk(self):
        """Test that an unconfigured transport matches the Rust SDK defaults."""
        config = HttpConfig()

        assert config.api_url == "http://127.0.0.1:3000"
        assert config.retries == 3
        assert config.has_jwt is False
        assert config.heartbeat_interval == timedelta(seconds=5)

    def test_every_field_round_trips(self):
        """Test that each configured field is readable back unchanged."""
        config = HttpConfig(
            api_url="http://127.0.0.1:3001",
            retries=5,
            jwt="a-token",
            heartbeat_interval=timedelta(seconds=15),
        )

        assert config.api_url == "http://127.0.0.1:3001"
        assert config.retries == 5
        assert config.has_jwt is True
        assert config.heartbeat_interval == timedelta(seconds=15)

    def test_arguments_are_keyword_only(self):
        """Test that the API URL cannot be passed positionally."""
        with pytest.raises(TypeError):
            # pyrefly: ignore  # bad-argument-count
            HttpConfig("http://127.0.0.1:3000")

    def test_repr_hides_the_jwt(self):
        """Test that the JWT does not leak through repr but still parses as Python."""
        config = HttpConfig(jwt="a-secret-token")

        printed = repr(config)

        assert "a-secret-token" not in printed
        ast.parse(printed)

    def test_repr_shows_every_field_as_python(self):
        """Test that repr covers the configured fields and parses as Python."""
        config = HttpConfig(api_url="http://127.0.0.1:3001", retries=5)

        printed = repr(config)

        assert 'api_url="http://127.0.0.1:3001"' in printed
        assert "retries=5" in printed
        ast.parse(printed)

    @pytest.mark.parametrize(
        "invalid_url",
        ["", "not-a-url", "http://127.0.0.1:0"],
    )
    def test_invalid_api_url_is_rejected(self, invalid_url: str):
        """Test that a malformed API URL fails at construction, not at connect."""
        with pytest.raises(ValueError):
            HttpConfig(api_url=invalid_url)

    @pytest.mark.parametrize("out_of_range", [-1, 2**32])
    def test_out_of_range_retries_is_rejected(self, out_of_range: int):
        """Test that a retry count outside the wire range names the argument.

        The conversion pyo3 does on its own raises OverflowError, which is not a
        ValueError and so escapes the handler a caller wraps construction in.
        """
        with pytest.raises(ValueError, match="retries"):
            HttpConfig(retries=out_of_range)

    def test_negative_heartbeat_interval_is_rejected(self):
        """Test that a negative heartbeat interval fails at construction."""
        with pytest.raises(ValueError, match="negative"):
            HttpConfig(heartbeat_interval=timedelta(seconds=-3))

    def test_zero_heartbeat_interval_is_rejected(self):
        """Test that a zero heartbeat interval fails at construction.

        Nothing downstream reads zero as "disabled"; it heartbeats in a
        continuous loop for as long as the client lives.
        """
        with pytest.raises(ValueError, match="zero"):
            HttpConfig(heartbeat_interval=timedelta(0))


@pytest.mark.unit
class TestHttpClientConstruction:
    """Test what `IggyClient.http(...)` accepts."""

    def test_accepts_a_config(self):
        """Test that a client can be built from a config object."""
        assert IggyClient.http(HttpConfig(api_url="http://127.0.0.1:3000")) is not None

    def test_accepts_nothing(self):
        """Test that the default configuration is used when no argument is given."""
        assert IggyClient.http() is not None


@pytest.mark.integration
class TestHttpConfigAgainstServer:
    """Test that a client built from `HttpConfig` actually connects."""

    @pytest.mark.asyncio
    async def test_client_connects_and_pings(self):
        """Test that a client built with a custom config reaches the server."""
        host, port = get_http_server_config()

        client = IggyClient.http(HttpConfig(api_url=f"http://{host}:{port}"))
        await client.connect()
        await wait_for_ping(client)

    @pytest.mark.asyncio
    async def test_client_sends_and_polls_a_message(self, unique_name):
        """Test a full round trip: login, create stream/topic, send, poll.

        HTTP is a stateless per-request transport, so this is the part
        `test_client_connects_and_pings` above does not cover: that a client
        built from `HttpConfig` can actually carry a real workload.
        """
        host, port = get_http_server_config()
        stream_name = unique_name()
        topic_name = unique_name()
        payload = f"payload-{unique_name()}"

        client = IggyClient.http(HttpConfig(api_url=f"http://{host}:{port}"))
        await client.connect()
        await wait_for_ping(client)
        await client.login_user("iggy", "iggy")

        await client.create_stream(stream_name)
        await client.create_topic(
            stream=stream_name, name=topic_name, partitions_count=1
        )
        await client.send_messages(
            stream=stream_name,
            topic=topic_name,
            partitioning=0,
            messages=[Message(payload)],
        )

        polled_messages = await client.poll_messages(
            stream=stream_name,
            topic=topic_name,
            consumer=Consumer.Single("http-round-trip"),
            partition_id=0,
            polling_strategy=PollingStrategy.First(),
            count=1,
            auto_commit=True,
        )

        assert [message.payload().decode() for message in polled_messages] == [payload]
