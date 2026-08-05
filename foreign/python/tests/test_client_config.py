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
Tests for the TCP client configuration surface.

`TcpConfig`, `TcpReconnectionConfig` and `AutoLogin` mirror the Rust SDK
types, so most of these assert that a value set from Python survives to the
getters and that unset fields fall back to the Rust defaults. The last class
proves the point of the configuration: with `auto_login` set, credentials are
replayed on connect and no manual `login_user()` is needed.
"""

from collections.abc import Callable
from datetime import timedelta

import pytest

from apache_iggy import (
    AutoCommit,
    AutoCommitAfter,
    AutoCommitWhen,
    AutoLogin,
    IggyClient,
    IggyExpiry,
    TcpConfig,
    TcpReconnectionConfig,
)

from .utils import get_server_config, wait_for_ping, wait_for_server


@pytest.mark.unit
class TestAutoLogin:
    """Test the credentials carried into the client."""

    def test_disabled_has_no_username(self):
        """Test that the disabled variant carries no credentials."""
        auto_login = AutoLogin.disabled()

        assert auto_login.enabled is False
        assert auto_login.username is None

    def test_username_password_exposes_username_only(self):
        """Test that the username is readable back but the password is not."""
        auto_login = AutoLogin.username_password("iggy", "secret")

        assert auto_login.enabled is True
        assert auto_login.username == "iggy"
        assert "secret" not in repr(auto_login)

    def test_personal_access_token_hides_the_token(self):
        """Test that a token login exposes neither a username nor the token."""
        auto_login = AutoLogin.personal_access_token("secret-token")

        assert auto_login.enabled is True
        assert auto_login.username is None
        assert "secret-token" not in repr(auto_login)


@pytest.mark.unit
class TestTcpReconnectionConfig:
    """Test the reconnection policy."""

    def test_defaults_match_the_rust_sdk(self):
        """Test that an unconfigured policy reconnects forever, one second apart."""
        reconnection = TcpReconnectionConfig()

        assert reconnection.enabled is True
        assert reconnection.max_retries is None
        assert reconnection.interval == timedelta(seconds=1)
        assert reconnection.reestablish_after == timedelta(seconds=5)

    def test_every_field_round_trips(self):
        """Test that each configured field is readable back unchanged."""
        reconnection = TcpReconnectionConfig(
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
            TcpReconnectionConfig(True)

    @pytest.mark.parametrize(
        "construct",
        [
            lambda duration: TcpReconnectionConfig(interval=duration),
            lambda duration: TcpReconnectionConfig(reestablish_after=duration),
        ],
        ids=["interval", "reestablish_after"],
    )
    @pytest.mark.parametrize(
        "negative",
        [timedelta(microseconds=-1), timedelta(seconds=-1), timedelta(days=-1)],
    )
    def test_negative_duration_is_rejected(
        self,
        construct: Callable[[timedelta], TcpReconnectionConfig],
        negative: timedelta,
    ):
        """Test that a negative duration fails at construction, not at connect."""
        with pytest.raises(ValueError, match="negative"):
            construct(negative)

    def test_zero_reestablish_after_is_allowed(self):
        """Test that a zero cooldown is legal and readable back."""
        reconnection = TcpReconnectionConfig(reestablish_after=timedelta(0))

        assert reconnection.reestablish_after == timedelta(0)

    def test_zero_interval_is_allowed_with_bounded_retries(self):
        """Test that a zero interval is legal as a bounded fast-retry policy."""
        reconnection = TcpReconnectionConfig(interval=timedelta(0), max_retries=5)

        assert reconnection.interval == timedelta(0)

    def test_zero_interval_is_allowed_when_reconnection_is_disabled(self):
        """Test that a zero interval is legal when nothing ever reads it."""
        reconnection = TcpReconnectionConfig(enabled=False, interval=timedelta(0))

        assert reconnection.interval == timedelta(0)

    def test_zero_interval_with_unlimited_retries_is_rejected(self):
        """Test that the combination that reconnects in a continuous loop fails."""
        with pytest.raises(ValueError, match="zero"):
            TcpReconnectionConfig(interval=timedelta(0))

    def test_very_long_interval_round_trips(self):
        """Test that an interval beyond 68 years survives the i32 boundary."""
        reconnection = TcpReconnectionConfig(interval=timedelta(days=30_000))

        assert reconnection.interval == timedelta(days=30_000)

    def test_maximum_interval_round_trips(self):
        """Test that the largest timedelta survives the u64-microsecond boundary."""
        reconnection = TcpReconnectionConfig(interval=timedelta(days=999_999_999))

        assert reconnection.interval == timedelta(days=999_999_999)


@pytest.mark.unit
class TestTcpConfig:
    """Test the transport configuration."""

    def test_defaults_match_the_rust_sdk(self):
        """Test that an unconfigured transport matches the Rust SDK defaults."""
        config = TcpConfig()

        assert config.server_address == "127.0.0.1:8090"
        assert config.auto_login.enabled is False
        assert config.reconnection.enabled is True
        assert config.heartbeat_interval == timedelta(seconds=5)
        assert config.tls_enabled is False
        assert config.tls_domain == ""
        assert config.tls_ca_file is None
        assert config.tls_validate_certificate is True
        assert config.nodelay is False

    def test_every_field_round_trips(self):
        """Test that each configured field is readable back unchanged."""
        config = TcpConfig(
            server_address="localhost:8090",
            auto_login=AutoLogin.username_password("iggy", "iggy"),
            reconnection=TcpReconnectionConfig(max_retries=3),
            heartbeat_interval=timedelta(seconds=15),
            tls_enabled=True,
            tls_domain="localhost",
            tls_ca_file="ca.pem",
            tls_validate_certificate=False,
            nodelay=True,
        )

        assert config.server_address == "localhost:8090"
        assert config.auto_login.username == "iggy"
        assert config.reconnection.max_retries == 3
        assert config.heartbeat_interval == timedelta(seconds=15)
        assert config.tls_enabled is True
        assert config.tls_domain == "localhost"
        assert config.tls_ca_file == "ca.pem"
        assert config.tls_validate_certificate is False
        assert config.nodelay is True

    def test_arguments_are_keyword_only(self):
        """Test that the address cannot be passed positionally."""
        with pytest.raises(TypeError):
            # pyrefly: ignore  # bad-argument-count
            TcpConfig("127.0.0.1:8090")

    def test_repr_hides_the_password(self):
        """Test that the password does not leak through repr."""
        config = TcpConfig(auto_login=AutoLogin.username_password("iggy", "secret"))

        assert "secret" not in repr(config)

    @pytest.mark.parametrize(
        "invalid_address",
        ["", "127.0.0.1", "127.0.0.1:not-a-port", "127.0.0.1:70000", "::1:8090"],
    )
    def test_invalid_server_address_is_rejected(self, invalid_address: str):
        """Test that a malformed address fails at construction, not at connect."""
        with pytest.raises(ValueError):
            TcpConfig(server_address=invalid_address)

    def test_negative_heartbeat_interval_is_rejected(self):
        """Test that a negative heartbeat interval fails at construction."""
        with pytest.raises(ValueError, match="negative"):
            TcpConfig(heartbeat_interval=timedelta(seconds=-3))

    def test_zero_heartbeat_interval_is_rejected(self):
        """Test that a zero heartbeat interval fails at construction.

        Nothing downstream reads zero as "disabled"; it heartbeats in a
        continuous loop for as long as the client lives.
        """
        with pytest.raises(ValueError, match="zero"):
            TcpConfig(heartbeat_interval=timedelta(0))


@pytest.mark.unit
class TestClientConstruction:
    """Test what the client constructor accepts."""

    def test_accepts_a_config(self):
        """Test that a client can be built from a config object."""
        assert IggyClient(TcpConfig(server_address="127.0.0.1:8090")) is not None

    def test_accepts_an_address(self):
        """Test that the address form still works."""
        assert IggyClient("127.0.0.1:8090") is not None

    def test_accepts_nothing(self):
        """Test that the default address is used when no argument is given."""
        assert IggyClient() is not None

    def test_rejects_an_invalid_address(self):
        """Test that a malformed address is rejected."""
        with pytest.raises(RuntimeError):
            IggyClient("nonsense")

    def test_negative_message_expiry_is_rejected(self):
        """Test that the negative-duration rule reaches create_topic.

        The check runs at the call, before any I/O.
        """
        client = IggyClient()

        with pytest.raises(ValueError, match="negative"):
            client.create_topic(
                stream="stream",
                name="topic",
                partitions_count=1,
                message_expiry=IggyExpiry.ExpireDuration(timedelta(seconds=-1)),
            )

    @pytest.mark.parametrize(
        "interval_kwargs",
        [
            {"polling_retry_interval": timedelta(0)},
            {"init_retries": 3, "init_retry_interval": timedelta(0)},
            {"auto_commit": AutoCommit.Interval(timedelta(0))},
            {
                "auto_commit": AutoCommit.IntervalOrWhen(
                    timedelta(0), AutoCommitWhen.PollingMessages()
                )
            },
            {
                "auto_commit": AutoCommit.IntervalOrAfter(
                    timedelta(0), AutoCommitAfter.ConsumingEachMessage()
                )
            },
        ],
        ids=[
            "polling_retry_interval",
            "init_retry_interval",
            "auto_commit_interval",
            "auto_commit_interval_or_when",
            "auto_commit_interval_or_after",
        ],
    )
    def test_zero_consumer_interval_is_rejected(self, interval_kwargs: dict):
        """Test that a zero consumer interval fails at the call.

        Zero spins the retry loop, floods the server with offset stores, or
        panics inside the runtime timer, and none of those name the argument
        that caused it.
        """
        client = IggyClient()

        with pytest.raises(ValueError, match="zero"):
            client.consumer_group(
                name="group",
                stream="stream",
                topic="topic",
                **interval_kwargs,
            )

    def test_zero_poll_interval_is_allowed(self):
        """Test that a zero poll interval passes validation.

        Zero there means "do not wait before polling" and is short-circuited
        before the sleep, unlike the retry intervals. Reaching the awaitable is
        what proves it: building one without a running loop is the next failure,
        and a rejected value would have raised ValueError first.
        """
        client = IggyClient()

        with pytest.raises(RuntimeError):
            client.consumer_group(
                name="group",
                stream="stream",
                topic="topic",
                poll_interval=timedelta(0),
            )


@pytest.mark.integration
class TestAutoLoginAgainstServer:
    """Test that configured credentials are actually replayed on connect."""

    @pytest.mark.asyncio
    async def test_auto_login_authenticates_without_login_user(self, unique_name):
        """Test that a privileged call succeeds without a manual login_user()."""
        host, port = get_server_config()
        wait_for_server(host, port)

        client = IggyClient(
            TcpConfig(
                server_address=f"{host}:{port}",
                auto_login=AutoLogin.username_password("iggy", "iggy"),
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
        host, port = get_server_config()
        wait_for_server(host, port)

        client = IggyClient(TcpConfig(server_address=f"{host}:{port}"))
        await client.connect()
        await wait_for_ping(client)

        with pytest.raises(RuntimeError):
            await client.create_stream(unique_name())

    @pytest.mark.asyncio
    async def test_config_and_connection_string_are_equivalent(self, unique_name):
        """Test that TcpConfig and a connection string reach the same behavior."""
        host, port = get_server_config()
        wait_for_server(host, port)

        from_config = IggyClient(
            TcpConfig(
                server_address=f"{host}:{port}",
                auto_login=AutoLogin.username_password("iggy", "iggy"),
                reconnection=TcpReconnectionConfig(
                    max_retries=3, interval=timedelta(seconds=1)
                ),
            )
        )
        from_string = IggyClient.from_connection_string(
            f"iggy+tcp://iggy:iggy@{host}:{port}"
            "?reconnection_retries=3&reconnection_interval=1s"
        )

        stream_name = unique_name()
        for client in (from_config, from_string):
            await client.connect()
            await wait_for_ping(client)
            assert await client.get_stream(stream_name) is None

    @pytest.mark.asyncio
    async def test_wrong_auto_login_credentials_fail(self):
        """Test that bad configured credentials surface as a connect failure."""
        host, port = get_server_config()
        wait_for_server(host, port)

        client = IggyClient(
            TcpConfig(
                server_address=f"{host}:{port}",
                auto_login=AutoLogin.username_password("iggy", "invalid-password"),
                reconnection=TcpReconnectionConfig(enabled=False),
            )
        )

        with pytest.raises(RuntimeError):
            await client.connect()
