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
Tests for the WebSocket client configuration surface.

`WebSocketConfig` and `WebSocketReconnectionConfig` mirror the Rust SDK
types the same way `TcpConfig`/`TcpReconnectionConfig` do, so most of these
assert that a value set from Python survives to the getters and that unset
fields fall back to the Rust defaults. `WebSocketFramingConfig` is new: it
wraps the tungstenite frame- and buffer-level options the Rust SDK nests
under `ws_config`, exposed here as a `framing` argument rather than flat
kwargs, mirroring the Rust struct's own nesting. `AutoLogin` is
transport-agnostic and already covered by `test_client_config.py`.
"""

import ast
from collections.abc import Callable
from datetime import timedelta

import pytest

from apache_iggy import (
    AutoLogin,
    IggyClient,
    WebSocketConfig,
    WebSocketFramingConfig,
    WebSocketReconnectionConfig,
)

from .utils import get_websocket_server_config, wait_for_ping, wait_for_server


@pytest.mark.unit
class TestWebSocketReconnectionConfig:
    """Test the reconnection policy."""

    def test_defaults_match_the_rust_sdk(self):
        """Test that an unconfigured policy reconnects forever, one second apart."""
        reconnection = WebSocketReconnectionConfig()

        assert reconnection.enabled is True
        assert reconnection.max_retries is None
        assert reconnection.interval == timedelta(seconds=1)
        assert reconnection.reestablish_after == timedelta(seconds=5)

    def test_every_field_round_trips(self):
        """Test that each configured field is readable back unchanged."""
        reconnection = WebSocketReconnectionConfig(
            enabled=False,
            max_retries=10,
            interval=timedelta(milliseconds=250),
            reestablish_after=timedelta(seconds=30),
        )

        assert reconnection.enabled is False
        assert reconnection.max_retries == 10
        assert reconnection.interval == timedelta(milliseconds=250)
        assert reconnection.reestablish_after == timedelta(seconds=30)

    def test_arguments_are_keyword_only(self):
        """Test that the adjacent flags cannot be passed positionally."""
        with pytest.raises(TypeError):
            # pyrefly: ignore  # bad-argument-count
            WebSocketReconnectionConfig(True)

    @pytest.mark.parametrize(
        "construct",
        [
            lambda duration: WebSocketReconnectionConfig(interval=duration),
            lambda duration: WebSocketReconnectionConfig(reestablish_after=duration),
        ],
        ids=["interval", "reestablish_after"],
    )
    @pytest.mark.parametrize(
        "negative",
        [timedelta(microseconds=-1), timedelta(seconds=-1), timedelta(days=-1)],
    )
    def test_negative_duration_is_rejected(
        self,
        construct: Callable[[timedelta], WebSocketReconnectionConfig],
        negative: timedelta,
    ):
        """Test that a negative duration fails at construction, not at connect."""
        with pytest.raises(ValueError, match="negative"):
            construct(negative)

    @pytest.mark.parametrize("out_of_range", [-1, 2**32])
    def test_out_of_range_max_retries_is_rejected(self, out_of_range: int):
        """Test that a retry count outside the wire range names the argument.

        The conversion pyo3 does on its own raises OverflowError, which is not a
        ValueError and so escapes the handler a caller wraps construction in.
        """
        with pytest.raises(ValueError, match="max_retries"):
            WebSocketReconnectionConfig(max_retries=out_of_range)

    def test_zero_reestablish_after_is_allowed(self):
        """Test that a zero cooldown is legal and readable back."""
        reconnection = WebSocketReconnectionConfig(reestablish_after=timedelta(0))

        assert reconnection.reestablish_after == timedelta(0)

    @pytest.mark.parametrize(
        "kwargs",
        [
            {},
            {"max_retries": 5},
            {"enabled": False},
        ],
        ids=["unlimited_retries", "bounded_retries", "reconnection_disabled"],
    )
    def test_zero_interval_is_rejected(self, kwargs: dict):
        """Test that a zero interval fails whatever the retry policy is.

        The interval is a delay between passes, so zero reconnects in a
        continuous loop.
        """
        with pytest.raises(ValueError, match="zero"):
            WebSocketReconnectionConfig(interval=timedelta(0), **kwargs)

    def test_very_long_interval_round_trips(self):
        """Test that an interval beyond 68 years survives the i32 boundary."""
        reconnection = WebSocketReconnectionConfig(interval=timedelta(days=30_000))

        assert reconnection.interval == timedelta(days=30_000)

    def test_maximum_interval_round_trips(self):
        """Test that the largest timedelta survives the day conversion."""
        reconnection = WebSocketReconnectionConfig(interval=timedelta(days=999_999_999))

        assert reconnection.interval == timedelta(days=999_999_999)


@pytest.mark.unit
class TestWebSocketFramingConfig:
    """Test the frame- and buffer-level options."""

    def test_defaults_match_tungstenite(self):
        """Test that unconfigured sizes fall back to the tungstenite defaults.

        Every size defaults to `Some(...)`; passing `None` explicitly leaves the
        default untouched rather than clearing the limit, since the constructor
        only assigns a field when its argument is `Some(...)`.
        """
        framing = WebSocketFramingConfig()

        assert framing.read_buffer_size is not None
        assert framing.write_buffer_size is not None
        assert framing.max_write_buffer_size is not None
        assert framing.max_message_size is not None
        assert framing.max_frame_size is not None
        assert framing.accept_unmasked_frames is False

    def test_every_field_round_trips(self):
        """Test that each configured field is readable back unchanged."""
        framing = WebSocketFramingConfig(
            read_buffer_size=4096,
            write_buffer_size=4096,
            max_write_buffer_size=8192,
            max_message_size=16384,
            max_frame_size=16384,
            accept_unmasked_frames=True,
        )

        assert framing.read_buffer_size == 4096
        assert framing.write_buffer_size == 4096
        assert framing.max_write_buffer_size == 8192
        assert framing.max_message_size == 16384
        assert framing.max_frame_size == 16384
        assert framing.accept_unmasked_frames is True

    def test_arguments_are_keyword_only(self):
        """Test that the first field cannot be passed positionally."""
        with pytest.raises(TypeError):
            # pyrefly: ignore  # bad-argument-count
            WebSocketFramingConfig(4096)

    def test_repr_shows_every_field_as_python(self):
        """Test that repr covers every field and parses as Python."""
        framing = WebSocketFramingConfig(
            read_buffer_size=4096,
            accept_unmasked_frames=True,
        )

        printed = repr(framing)

        assert "read_buffer_size=4096" in printed
        assert "accept_unmasked_frames=True" in printed
        ast.parse(printed)

    @pytest.mark.parametrize(
        "field",
        [
            "read_buffer_size",
            "write_buffer_size",
            "max_write_buffer_size",
            "max_message_size",
            "max_frame_size",
        ],
    )
    def test_negative_size_is_rejected(self, field: str):
        """Test that a negative size names the argument that caused it."""
        with pytest.raises(ValueError, match=field):
            # pyrefly: ignore  # bad-argument-type
            WebSocketFramingConfig(**{field: -1})

    def test_max_write_buffer_size_not_greater_than_write_buffer_size_is_rejected(self):
        """Test that a non-increasing write buffer pair fails at construction.

        tungstenite enforces `max_write_buffer_size > write_buffer_size` with an
        `assert!` when the connection is established, which would otherwise
        surface as an unrecoverable Rust panic instead of a catchable error.
        """
        with pytest.raises(ValueError, match="max_write_buffer_size"):
            WebSocketFramingConfig(write_buffer_size=1000, max_write_buffer_size=1000)

    def test_max_write_buffer_size_below_the_default_write_buffer_size_is_rejected(
        self,
    ):
        """Test that the invariant is checked against the default too.

        Setting only `max_write_buffer_size` below the untouched default
        `write_buffer_size` (128 KiB) must fail the same way as setting both.
        """
        with pytest.raises(ValueError, match="max_write_buffer_size"):
            WebSocketFramingConfig(max_write_buffer_size=1000)


@pytest.mark.unit
class TestWebSocketConfig:
    """Test the transport configuration."""

    def test_defaults_match_the_rust_sdk(self):
        """Test that an unconfigured transport matches the Rust SDK defaults."""
        config = WebSocketConfig()

        assert config.server_address == "127.0.0.1:8092"
        assert config.auto_login.enabled is False
        assert config.reconnection.enabled is True
        assert config.heartbeat_interval == timedelta(seconds=5)
        assert config.tls_enabled is False
        assert config.tls_domain == "localhost"
        assert config.tls_ca_file is None
        # Unlike TCP and QUIC, WebSocket does not validate the server
        # certificate by default.
        assert config.tls_validate_certificate is False

    def test_every_field_round_trips(self):
        """Test that each configured field is readable back unchanged."""
        config = WebSocketConfig(
            server_address="127.0.0.1:8093",
            auto_login=AutoLogin.username_password("iggy", "iggy"),
            reconnection=WebSocketReconnectionConfig(max_retries=3),
            heartbeat_interval=timedelta(seconds=15),
            framing=WebSocketFramingConfig(read_buffer_size=4096),
            tls_enabled=True,
            tls_domain="example.com",
            tls_ca_file="ca.pem",
            tls_validate_certificate=True,
        )

        assert config.server_address == "127.0.0.1:8093"
        assert config.auto_login.username == "iggy"
        assert config.reconnection.max_retries == 3
        assert config.heartbeat_interval == timedelta(seconds=15)
        assert config.framing.read_buffer_size == 4096
        assert config.tls_enabled is True
        assert config.tls_domain == "example.com"
        assert config.tls_ca_file == "ca.pem"
        assert config.tls_validate_certificate is True

    def test_arguments_are_keyword_only(self):
        """Test that the address cannot be passed positionally."""
        with pytest.raises(TypeError):
            # pyrefly: ignore  # bad-argument-count
            WebSocketConfig("127.0.0.1:8092")

    def test_repr_hides_the_password(self):
        """Test that the password does not leak through repr."""
        config = WebSocketConfig(
            auto_login=AutoLogin.username_password("iggy", "secret")
        )

        assert "secret" not in repr(config)

    def test_repr_shows_every_field_as_python(self):
        """Test that repr covers the TLS fields and parses as Python."""
        config = WebSocketConfig(
            heartbeat_interval=timedelta(seconds=15),
            tls_enabled=True,
            tls_domain="example.com",
            tls_ca_file="ca.pem",
            tls_validate_certificate=True,
        )

        printed = repr(config)

        assert 'tls_domain="example.com"' in printed
        assert 'tls_ca_file="ca.pem"' in printed
        assert "tls_validate_certificate=True" in printed
        assert "heartbeat_interval=datetime.timedelta(seconds=15)" in printed
        ast.parse(printed)

    @pytest.mark.parametrize(
        "invalid_address",
        ["", "127.0.0.1", "127.0.0.1:not-a-port", "127.0.0.1:70000", "::1:8092"],
    )
    def test_invalid_server_address_is_rejected(self, invalid_address: str):
        """Test that a malformed address fails at construction, not at connect."""
        with pytest.raises(ValueError):
            WebSocketConfig(server_address=invalid_address)

    def test_negative_heartbeat_interval_is_rejected(self):
        """Test that a negative heartbeat interval fails at construction."""
        with pytest.raises(ValueError, match="negative"):
            WebSocketConfig(heartbeat_interval=timedelta(seconds=-3))

    def test_zero_heartbeat_interval_is_rejected(self):
        """Test that a zero heartbeat interval fails at construction.

        Nothing downstream reads zero as "disabled"; it heartbeats in a
        continuous loop for as long as the client lives.
        """
        with pytest.raises(ValueError, match="zero"):
            WebSocketConfig(heartbeat_interval=timedelta(0))


@pytest.mark.unit
class TestClientConstruction:
    """Test what `IggyClient.websocket(...)` accepts."""

    def test_accepts_a_config(self):
        """Test that a client can be built from a config object."""
        assert (
            IggyClient.websocket(WebSocketConfig(server_address="127.0.0.1:8092"))
            is not None
        )

    def test_accepts_nothing(self):
        """Test that the default configuration is used when no argument is given."""
        assert IggyClient.websocket() is not None


@pytest.mark.integration
class TestAutoLoginAgainstServer:
    """Test that configured credentials are actually replayed on connect."""

    @pytest.mark.asyncio
    async def test_auto_login_authenticates_without_login_user(self, unique_name):
        """Test that a privileged call succeeds without a manual login_user()."""
        host, port = get_websocket_server_config()
        wait_for_server(host, port)

        client = IggyClient.websocket(
            WebSocketConfig(
                server_address=f"{host}:{port}",
                auto_login=AutoLogin.username_password("iggy", "iggy"),
                # The default reconnection policy retries forever: a missing
                # listener would hang this test until the CI timeout instead
                # of failing.
                reconnection=WebSocketReconnectionConfig(enabled=False),
            )
        )
        await client.connect()
        await wait_for_ping(client)

        stream_name = unique_name()
        await client.create_stream(stream_name)
        assert await client.get_stream(stream_name) is not None

    @pytest.mark.asyncio
    async def test_without_auto_login_a_privileged_call_is_unauthenticated(
        self, unique_name
    ):
        """Test that the same call fails when no credentials are configured."""
        host, port = get_websocket_server_config()
        wait_for_server(host, port)

        client = IggyClient.websocket(
            WebSocketConfig(
                server_address=f"{host}:{port}",
                # The default reconnection policy retries forever: a missing
                # listener would hang this test until the CI timeout instead
                # of failing.
                reconnection=WebSocketReconnectionConfig(enabled=False),
            )
        )
        await client.connect()
        await wait_for_ping(client)

        with pytest.raises(RuntimeError):
            await client.create_stream(unique_name())

    @pytest.mark.asyncio
    async def test_wrong_auto_login_credentials_fail(self):
        """Test that bad configured credentials surface as a connect failure."""
        host, port = get_websocket_server_config()
        wait_for_server(host, port)

        client = IggyClient.websocket(
            WebSocketConfig(
                server_address=f"{host}:{port}",
                auto_login=AutoLogin.username_password("iggy", "invalid-password"),
                reconnection=WebSocketReconnectionConfig(enabled=False),
            )
        )

        with pytest.raises(RuntimeError):
            await client.connect()
