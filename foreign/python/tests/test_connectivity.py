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

import pytest

from apache_iggy import IggyClient

from .utils import get_server_config, wait_for_ping, wait_for_server


class TestConnectivity:
    """Test basic connectivity and authentication."""

    @pytest.mark.asyncio
    async def test_client_not_none(self, iggy_client: IggyClient):
        """Test that client fixture is properly initialized."""
        assert iggy_client is not None

    @pytest.mark.parametrize(
        "connection_string",
        [
            "iggy://iggy:iggy@127.0.0.1:8090",
            "iggy+tcp://iggy:iggy@127.0.0.1:8090",
            "iggy+tcp://iggy:iggy@127.0.0.1:8090?reconnection_retries=3&reconnection_interval=1s&reestablish_after=5s&heartbeat_interval=5s&nodelay=true&tls_domain=localhost&tls_ca_file=unused.pem&tls=false",
            "iggy+http://iggy:iggy@127.0.0.1:3000",
            "iggy+http://iggy:iggy@127.0.0.1:3000?heartbeat_interval=5s&retries=3",
            "iggy+ws://iggy:iggy@127.0.0.1:8092",
            "iggy+ws://iggy:iggy@127.0.0.1:8092?heartbeat_interval=5s&reconnection_retries=3&reconnection_interval=1s&reestablish_after=5s&read_buffer_size=4096&write_buffer_size=4096&max_write_buffer_size=8192&max_message_size=16384&max_frame_size=16384&accept_unmasked_frames=false&tls_domain=localhost&tls_ca_file=unused.pem&tls_validate_certificate=false&tls=false",
        ],
    )
    @pytest.mark.asyncio
    async def test_valid_connection_string(self, connection_string: str):
        """Test that valid connection string formats can connect to the server."""

        client = IggyClient.from_connection_string(connection_string)
        await client.connect()
        await wait_for_ping(client, timeout=5, interval=1)

    @pytest.mark.parametrize(
        ("invalid_value", "expected_error"),
        [
            ("", "Invalid connection string"),
            ("bad address", "Invalid connection string"),
            ("http://{host}:{port}", "Invalid connection string"),
            ("tcp://iggy:iggy@{host}:{port}", "Invalid connection string"),
            ("{host}:", "Invalid connection string"),
            (":{port}", "Invalid connection string"),
            ("{host}:not-a-port", "Invalid connection string"),
            ("{host}:70000", "Invalid connection string"),
            ("iggy+tcp://", "Invalid connection string"),
            ("iggy+tcp://iggy:iggy@", "Invalid connection string"),
            (
                "iggy+tcp://iggy:iggy@{host}:not-a-port",
                "Invalid connection string",
            ),
            ("iggy+tcp://iggy:iggy@{host}:-1", "Invalid connection string"),
            ("iggy+tcp://iggy:iggy@{host}:70000", "Invalid connection string"),
            (
                "iggy+tcp://iggy:bad:format@{host}:{port}",
                "Invalid connection string",
            ),
            (
                "iggy+tcp://iggy:iggy@{host}:{port}?invalid_option=value",
                "Invalid connection string",
            ),
            ("iggy+quic://iggy:iggy@127.0.0.1:8080", "Cannot create endpoint"),
        ],
    )
    def test_invalid_connection_string(self, invalid_value: str, expected_error: str):
        """Test malformed server addresses and connection strings are rejected."""
        host, port = get_server_config()
        value = invalid_value.format(host=host, port=port)

        with pytest.raises(RuntimeError):
            IggyClient(server_address=value)

        with pytest.raises(RuntimeError, match=expected_error):
            IggyClient.from_connection_string(value)

    @pytest.mark.asyncio
    async def test_repeated_connect_does_not_error(self):
        """Test calling connect twice on the same client succeeds."""
        host, port = get_server_config()
        wait_for_server(host, port)

        client = IggyClient(server_address=f"{host}:{port}")
        await client.connect()
        await client.connect()

    @pytest.mark.asyncio
    async def test_login_before_connect_does_not_error(self):
        """Test login can establish authentication without an explicit connect call."""
        host, port = get_server_config()
        wait_for_server(host, port)
        client = IggyClient(server_address=f"{host}:{port}")

        await client.login_user("iggy", "iggy")
        await wait_for_ping(client)

    @pytest.mark.parametrize(
        ("username", "password", "expected_exception"),
        [
            ("invalid-user", "iggy", RuntimeError),
            ("iggy", "invalid-password", RuntimeError),
            ("", "iggy", RuntimeError),
            ("iggy", "", RuntimeError),
            (None, "iggy", TypeError),
            ("iggy", None, TypeError),
        ],
    )
    @pytest.mark.asyncio
    async def test_login_with_invalid_credentials_fails(
        self, username, password, expected_exception
    ):
        """Test login rejects invalid credentials and invalid argument values."""
        host, port = get_server_config()
        wait_for_server(host, port)

        client = IggyClient(server_address=f"{host}:{port}")
        await client.connect()
        await wait_for_ping(client)

        with pytest.raises(expected_exception):
            result = client.login_user(username, password)
            await result

    @pytest.mark.asyncio
    async def test_relogin_with_invalid_credentials_fails(self):
        """Test re-login with invalid credentials fails after a successful login."""
        host, port = get_server_config()
        wait_for_server(host, port)

        client = IggyClient(server_address=f"{host}:{port}")
        await client.connect()
        await wait_for_ping(client)
        await client.login_user("iggy", "iggy")

        with pytest.raises(RuntimeError):
            await client.login_user("iggy", "invalid-password")

    @pytest.mark.asyncio
    async def test_relogin_with_same_valid_credentials_does_not_error(self):
        """Test repeated login with the same valid credentials is allowed."""
        host, port = get_server_config()
        wait_for_server(host, port)

        client = IggyClient(server_address=f"{host}:{port}")
        await client.connect()
        await wait_for_ping(client)
        await client.login_user("iggy", "iggy")
        await client.login_user("iggy", "iggy")

    @pytest.mark.asyncio
    async def test_ping(self, iggy_client: IggyClient):
        """Test server ping functionality."""
        await iggy_client.ping()
