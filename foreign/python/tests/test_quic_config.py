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
Tests for the QUIC client configuration surface.

`QuicConfig` and `QuicReconnectionConfig` mirror the Rust SDK types the same
way `TcpConfig`/`TcpReconnectionConfig` do, so most of these assert that a
value set from Python survives to the getters and that unset fields fall
back to the Rust defaults. `AutoLogin` is transport-agnostic and already
covered by `test_client_config.py`.
"""

import ast
from collections.abc import Callable
from datetime import timedelta

import pytest

from apache_iggy import AutoLogin, IggyClient, QuicConfig, QuicReconnectionConfig

from .utils import get_quic_server_config, wait_for_ping


@pytest.mark.unit
class TestQuicReconnectionConfig:
    """Test the reconnection policy."""

    def test_defaults_match_the_rust_sdk(self):
        """Test that an unconfigured policy reconnects forever, one second apart."""
        reconnection = QuicReconnectionConfig()

        assert reconnection.enabled is True
        assert reconnection.max_retries is None
        assert reconnection.interval == timedelta(seconds=1)
        assert reconnection.reestablish_after == timedelta(seconds=5)

    def test_every_field_round_trips(self):
        """Test that each configured field is readable back unchanged."""
        reconnection = QuicReconnectionConfig(
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
            QuicReconnectionConfig(True)

    @pytest.mark.parametrize(
        "construct",
        [
            lambda duration: QuicReconnectionConfig(interval=duration),
            lambda duration: QuicReconnectionConfig(reestablish_after=duration),
        ],
        ids=["interval", "reestablish_after"],
    )
    @pytest.mark.parametrize(
        "negative",
        [timedelta(microseconds=-1), timedelta(seconds=-1), timedelta(days=-1)],
    )
    def test_negative_duration_is_rejected(
        self,
        construct: Callable[[timedelta], QuicReconnectionConfig],
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
            QuicReconnectionConfig(max_retries=out_of_range)

    def test_zero_reestablish_after_is_allowed(self):
        """Test that a zero cooldown is legal and readable back."""
        reconnection = QuicReconnectionConfig(reestablish_after=timedelta(0))

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
            QuicReconnectionConfig(interval=timedelta(0), **kwargs)

    def test_very_long_interval_round_trips(self):
        """Test that an interval beyond 68 years survives the i32 boundary."""
        reconnection = QuicReconnectionConfig(interval=timedelta(days=30_000))

        assert reconnection.interval == timedelta(days=30_000)

    def test_maximum_interval_round_trips(self):
        """Test that the largest timedelta survives the day conversion."""
        reconnection = QuicReconnectionConfig(interval=timedelta(days=999_999_999))

        assert reconnection.interval == timedelta(days=999_999_999)


@pytest.mark.unit
class TestQuicConfig:
    """Test the transport configuration."""

    def test_defaults_match_the_rust_sdk(self):
        """Test that an unconfigured transport matches the Rust SDK defaults."""
        config = QuicConfig()

        assert config.server_address == "127.0.0.1:8080"
        assert config.client_address == "127.0.0.1:0"
        assert config.server_name == "localhost"
        assert config.auto_login.enabled is False
        assert config.reconnection.enabled is True
        assert config.heartbeat_interval == timedelta(seconds=5)
        assert config.response_buffer_size == 10_000_000
        assert config.max_concurrent_bidi_streams == 10_000
        assert config.datagram_send_buffer_size == 100_000
        assert config.initial_mtu == 1200
        assert config.send_window == 100_000
        assert config.receive_window == 100_000
        assert config.keep_alive_interval == timedelta(milliseconds=5000)
        assert config.max_idle_timeout == timedelta(milliseconds=10_000)
        assert config.validate_certificate is False

    def test_every_field_round_trips(self):
        """Test that each configured field is readable back unchanged."""
        config = QuicConfig(
            server_address="127.0.0.1:8081",
            client_address="127.0.0.1:9000",
            server_name="example.com",
            auto_login=AutoLogin.username_password("iggy", "iggy"),
            reconnection=QuicReconnectionConfig(max_retries=3),
            heartbeat_interval=timedelta(seconds=15),
            response_buffer_size=5_000_000,
            max_concurrent_bidi_streams=500,
            datagram_send_buffer_size=50_000,
            initial_mtu=1400,
            send_window=200_000,
            receive_window=200_000,
            keep_alive_interval=timedelta(seconds=2),
            max_idle_timeout=timedelta(seconds=20),
            validate_certificate=True,
        )

        assert config.server_address == "127.0.0.1:8081"
        assert config.client_address == "127.0.0.1:9000"
        assert config.server_name == "example.com"
        assert config.auto_login.username == "iggy"
        assert config.reconnection.max_retries == 3
        assert config.heartbeat_interval == timedelta(seconds=15)
        assert config.response_buffer_size == 5_000_000
        assert config.max_concurrent_bidi_streams == 500
        assert config.datagram_send_buffer_size == 50_000
        assert config.initial_mtu == 1400
        assert config.send_window == 200_000
        assert config.receive_window == 200_000
        assert config.keep_alive_interval == timedelta(seconds=2)
        assert config.max_idle_timeout == timedelta(seconds=20)
        assert config.validate_certificate is True

    def test_arguments_are_keyword_only(self):
        """Test that the address cannot be passed positionally."""
        with pytest.raises(TypeError):
            # pyrefly: ignore  # bad-argument-count
            QuicConfig("127.0.0.1:8080")

    def test_repr_hides_the_password(self):
        """Test that the password does not leak through repr."""
        config = QuicConfig(auto_login=AutoLogin.username_password("iggy", "secret"))

        assert "secret" not in repr(config)

    def test_repr_shows_every_field_as_python(self):
        """Test that repr covers the QUIC-specific fields and parses as Python."""
        config = QuicConfig(
            heartbeat_interval=timedelta(seconds=15),
            keep_alive_interval=timedelta(seconds=2),
            max_idle_timeout=timedelta(seconds=20),
            validate_certificate=True,
        )

        printed = repr(config)

        assert "validate_certificate=True" in printed
        assert "heartbeat_interval=datetime.timedelta(seconds=15)" in printed
        assert "keep_alive_interval=datetime.timedelta(seconds=2)" in printed
        assert "max_idle_timeout=datetime.timedelta(seconds=20)" in printed
        ast.parse(printed)

    @pytest.mark.parametrize(
        "invalid_address",
        ["", "127.0.0.1", "127.0.0.1:not-a-port", "127.0.0.1:70000", "::1:8080"],
    )
    def test_invalid_server_address_is_rejected(self, invalid_address: str):
        """Test that a malformed address fails at construction, not at connect."""
        with pytest.raises(ValueError):
            QuicConfig(server_address=invalid_address)

    def test_negative_heartbeat_interval_is_rejected(self):
        """Test that a negative heartbeat interval fails at construction."""
        with pytest.raises(ValueError, match="negative"):
            QuicConfig(heartbeat_interval=timedelta(seconds=-3))

    def test_zero_heartbeat_interval_is_rejected(self):
        """Test that a zero heartbeat interval fails at construction.

        Nothing downstream reads zero as "disabled"; it heartbeats in a
        continuous loop for as long as the client lives.
        """
        with pytest.raises(ValueError, match="zero"):
            QuicConfig(heartbeat_interval=timedelta(0))

    @pytest.mark.parametrize(
        ("field", "out_of_range"),
        [
            ("response_buffer_size", -1),
            ("max_concurrent_bidi_streams", -1),
            ("datagram_send_buffer_size", -1),
            ("send_window", -1),
            ("receive_window", -1),
            ("initial_mtu", -1),
            ("initial_mtu", 2**16),
            ("max_concurrent_bidi_streams", 2**62),
            ("receive_window", 2**62),
        ],
    )
    def test_out_of_range_numeric_field_is_rejected(
        self, field: str, out_of_range: int
    ):
        """Test that a numeric field outside its wire type's range names itself.

        `max_concurrent_bidi_streams` and `receive_window` fit `u64`, but
        quinn narrows them further into a `VarInt` (max `2**62 - 1`), so
        `2**62` fits the wire type and must still be rejected.
        """
        with pytest.raises(ValueError, match=field):
            # pyrefly: ignore  # bad-argument-type
            QuicConfig(**{field: out_of_range})

    @pytest.mark.parametrize("field", ["keep_alive_interval", "max_idle_timeout"])
    def test_duration_rounding_down_to_zero_millis_is_rejected(self, field: str):
        """Test that a non-zero sub-millisecond duration names itself.

        Both fields are raw millisecond counts to the Rust SDK where zero is a
        magic value (disables the keep-alive, or falls back to quinn's own
        default), so a duration that rounds down to zero would silently mean
        something other than what was asked for.
        """
        with pytest.raises(ValueError, match=field):
            # pyrefly: ignore  # bad-argument-type
            QuicConfig(**{field: timedelta(microseconds=500)})

    @pytest.mark.parametrize("field", ["keep_alive_interval", "max_idle_timeout"])
    def test_exact_zero_duration_is_allowed(self, field: str):
        """Test that an exact zero duration is still legal for these fields."""
        # pyrefly: ignore  # bad-argument-type
        config = QuicConfig(**{field: timedelta(0)})

        assert getattr(config, field) == timedelta(0)

    def test_initial_mtu_below_quinns_minimum_is_rejected(self):
        """Test that an initial_mtu below 1200 fails at construction.

        quinn silently raises anything smaller to that floor instead of
        rejecting it, so accepting it here would let the getter read back a
        value that is not the one actually in effect on the connection.
        """
        with pytest.raises(ValueError, match="initial_mtu"):
            QuicConfig(initial_mtu=1199)

    def test_initial_mtu_at_quinns_minimum_is_allowed(self):
        """Test that exactly 1200, quinn's own floor, is accepted."""
        config = QuicConfig(initial_mtu=1200)

        assert config.initial_mtu == 1200


@pytest.mark.unit
class TestQuicClientConstruction:
    """Test that `IggyClient(...)` accepts a `QuicConfig`."""

    def test_accepts_a_config(self):
        """Test that a client can be built from a config object."""
        assert IggyClient(QuicConfig(server_address="127.0.0.1:8080")) is not None

    def test_accepts_the_default_config(self):
        """Test that an explicit default `QuicConfig` is accepted."""
        assert IggyClient(QuicConfig()) is not None


@pytest.mark.integration
class TestAutoLoginAgainstServer:
    """Test that configured credentials are actually replayed on connect."""

    @pytest.mark.asyncio
    async def test_auto_login_authenticates_without_login_user(self, unique_name):
        """Test that a privileged call succeeds without a manual login_user()."""
        host, port = get_quic_server_config()

        client = IggyClient(
            QuicConfig(
                server_address=f"{host}:{port}",
                auto_login=AutoLogin.username_password("iggy", "iggy"),
                # The default reconnection policy retries forever: a missing
                # listener would hang this test until the CI timeout instead
                # of failing.
                reconnection=QuicReconnectionConfig(enabled=False),
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
        host, port = get_quic_server_config()

        client = IggyClient(
            QuicConfig(
                server_address=f"{host}:{port}",
                # The default reconnection policy retries forever: a missing
                # listener would hang this test until the CI timeout instead
                # of failing.
                reconnection=QuicReconnectionConfig(enabled=False),
            )
        )
        await client.connect()
        await wait_for_ping(client)

        with pytest.raises(RuntimeError):
            await client.create_stream(unique_name())

    @pytest.mark.asyncio
    async def test_wrong_auto_login_credentials_fail(self):
        """Test that bad configured credentials surface as a connect failure."""
        host, port = get_quic_server_config()

        client = IggyClient(
            QuicConfig(
                server_address=f"{host}:{port}",
                auto_login=AutoLogin.username_password("iggy", "invalid-password"),
                reconnection=QuicReconnectionConfig(enabled=False),
            )
        )

        with pytest.raises(RuntimeError):
            await client.connect()
