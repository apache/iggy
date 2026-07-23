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

from typing import Any

import pytest

from apache_iggy import (
    GlobalPermissions,
    IggyClient,
    Permissions,
    StreamPermissions,
    TopicPermissions,
)

from .utils import (
    MAX_PASSWORD_BYTES,
    MAX_USERNAME_BYTES,
    MIN_PASSWORD_BYTES,
    get_server_config,
    login_fresh_client,
    unique_credentials,
    wait_for_ping,
)

GLOBAL_PERMISSION_FLAGS = [
    "manage_servers",
    "read_servers",
    "manage_users",
    "read_users",
    "manage_streams",
    "read_streams",
    "manage_topics",
    "read_topics",
    "poll_messages",
    "send_messages",
]

STREAM_PERMISSION_FLAGS = [
    "manage_stream",
    "read_stream",
    "manage_topics",
    "read_topics",
    "poll_messages",
    "send_messages",
]

TOPIC_PERMISSION_FLAGS = [
    "manage_topic",
    "read_topic",
    "poll_messages",
    "send_messages",
]


class TestPermissionsModel:
    """Test constructing the Permissions classes without a server."""

    def test_default_permissions_deny_everything(self):
        """Test Permissions() has every global flag off and no streams."""
        permissions = Permissions()

        assert permissions.streams is None
        global_permissions = permissions.global_
        assert global_permissions.manage_servers is False
        assert global_permissions.read_servers is False
        assert global_permissions.manage_users is False
        assert global_permissions.read_users is False
        assert global_permissions.manage_streams is False
        assert global_permissions.read_streams is False
        assert global_permissions.manage_topics is False
        assert global_permissions.read_topics is False
        assert global_permissions.poll_messages is False
        assert global_permissions.send_messages is False

    @pytest.mark.parametrize("flag", GLOBAL_PERMISSION_FLAGS)
    def test_each_global_permission_flag_round_trips_alone(self, flag):
        """Test setting one flag flips only that flag."""
        global_permissions = GlobalPermissions(**{flag: True})

        for name in GLOBAL_PERMISSION_FLAGS:
            assert getattr(global_permissions, name) is (name == flag)

    @pytest.mark.parametrize("flag", STREAM_PERMISSION_FLAGS)
    def test_each_stream_permission_flag_round_trips_alone(self, flag):
        """Test setting one flag flips only that flag."""
        flags: dict[str, Any] = {flag: True}
        stream_permissions = StreamPermissions(**flags)

        for name in STREAM_PERMISSION_FLAGS:
            assert getattr(stream_permissions, name) is (name == flag)

    @pytest.mark.parametrize("flag", TOPIC_PERMISSION_FLAGS)
    def test_each_topic_permission_flag_round_trips_alone(self, flag):
        """Test setting one flag flips only that flag."""
        topic_permissions = TopicPermissions(**{flag: True})

        for name in TOPIC_PERMISSION_FLAGS:
            assert getattr(topic_permissions, name) is (name == flag)

    def test_nested_stream_and_topic_permissions_are_preserved(self):
        """Test stream and topic permission dicts survive construction."""
        permissions = Permissions(
            global_=GlobalPermissions(read_servers=True),
            streams={
                1: StreamPermissions(
                    read_stream=True,
                    topics={7: TopicPermissions(poll_messages=True)},
                ),
                42: StreamPermissions(manage_stream=True),
            },
        )

        streams = permissions.streams
        assert streams is not None
        assert set(streams) == {1, 42}
        assert streams[1].read_stream is True
        assert streams[1].manage_stream is False
        topics = streams[1].topics
        assert topics is not None
        assert set(topics) == {7}
        assert topics[7].poll_messages is True
        assert topics[7].manage_topic is False
        assert streams[42].manage_stream is True
        assert streams[42].topics is None

    def test_stream_id_above_u32_is_rejected(self):
        """Test dict keys outside the u32 wire range fail at construction."""
        with pytest.raises(OverflowError):
            Permissions(streams={2**32: StreamPermissions(read_stream=True)})
        with pytest.raises(OverflowError):
            StreamPermissions(topics={2**32: TopicPermissions(read_topic=True)})

    def test_permissions_equality(self):
        """Test structurally identical Permissions compare equal."""
        first = Permissions(
            global_=GlobalPermissions(read_streams=True),
            streams={3: StreamPermissions(poll_messages=True)},
        )
        second = Permissions(
            global_=GlobalPermissions(read_streams=True),
            streams={3: StreamPermissions(poll_messages=True)},
        )
        different = Permissions(global_=GlobalPermissions(manage_streams=True))

        assert first == second
        assert first != different


class TestCreateUserWithPermissions:
    """Test create_user with the permissions argument."""

    @pytest.mark.asyncio
    async def test_create_user_with_global_permissions_round_trips(
        self, iggy_client: IggyClient, unique_name
    ):
        """Test global permissions set at creation come back from get_user."""
        username, password = unique_credentials(unique_name)
        permissions = Permissions(
            global_=GlobalPermissions(read_users=True, read_streams=True)
        )

        created = await iggy_client.create_user(
            username, password, permissions=permissions
        )
        assert created.permissions == permissions

        fetched = await iggy_client.get_user(created.id)
        assert fetched is not None
        assert fetched.permissions == permissions

        await iggy_client.delete_user(created.id)

    @pytest.mark.asyncio
    async def test_create_user_with_nested_permissions_round_trips(
        self, iggy_client: IggyClient, unique_name
    ):
        """Test per-stream and per-topic permissions survive the round trip."""
        username, password = unique_credentials(unique_name)
        stream_name = unique_name(prefix="perm-stream-")
        await iggy_client.create_stream(stream_name)
        stream = await iggy_client.get_stream(stream_name)
        assert stream is not None
        topic_name = unique_name(prefix="perm-topic-")
        await iggy_client.create_topic(stream_name, topic_name, partitions_count=1)
        topic = await iggy_client.get_topic(stream_name, topic_name)
        assert topic is not None

        permissions = Permissions(
            streams={
                stream.id: StreamPermissions(
                    read_stream=True,
                    poll_messages=True,
                    topics={topic.id: TopicPermissions(send_messages=True)},
                )
            }
        )
        created = await iggy_client.create_user(
            username, password, permissions=permissions
        )
        assert created.permissions == permissions

        fetched = await iggy_client.get_user(created.id)
        assert fetched is not None
        fetched_permissions = fetched.permissions
        assert fetched_permissions is not None
        assert fetched_permissions == permissions
        streams = fetched_permissions.streams
        assert streams is not None
        assert streams[stream.id].read_stream is True
        topics = streams[stream.id].topics
        assert topics is not None
        assert topics[topic.id].send_messages is True

        await iggy_client.delete_user(created.id)

    @pytest.mark.asyncio
    async def test_user_with_manage_streams_can_create_stream(
        self, iggy_client: IggyClient, unique_name
    ):
        """Test a granted global permission is enforced for the new user."""
        username, password = unique_credentials(unique_name)
        permissions = Permissions(global_=GlobalPermissions(manage_streams=True))
        created = await iggy_client.create_user(
            username, password, permissions=permissions
        )

        client = await login_fresh_client(username, password)
        stream_name = unique_name(prefix="granted-")
        await client.create_stream(stream_name)

        stream = await iggy_client.get_stream(stream_name)
        assert stream is not None

        await iggy_client.delete_user(created.id)

    @pytest.mark.asyncio
    async def test_user_without_permissions_cannot_create_stream(
        self, iggy_client: IggyClient, unique_name
    ):
        """Test a user created without permissions is denied stream creation."""
        username, password = unique_credentials(unique_name)
        created = await iggy_client.create_user(username, password)

        client = await login_fresh_client(username, password)
        with pytest.raises(RuntimeError):
            await client.create_stream(unique_name(prefix="denied-"))

        await iggy_client.delete_user(created.id)

    @pytest.mark.asyncio
    async def test_per_stream_permission_is_scoped_to_that_stream(
        self, iggy_client: IggyClient, unique_name
    ):
        """Test read access to one stream does not leak to another stream."""
        readable_name = unique_name(prefix="readable-")
        hidden_name = unique_name(prefix="hidden-")
        await iggy_client.create_stream(readable_name)
        await iggy_client.create_stream(hidden_name)
        readable = await iggy_client.get_stream(readable_name)
        assert readable is not None

        username, password = unique_credentials(unique_name)
        permissions = Permissions(
            streams={readable.id: StreamPermissions(read_stream=True)}
        )
        created = await iggy_client.create_user(
            username, password, permissions=permissions
        )

        client = await login_fresh_client(username, password)
        visible = await client.get_stream(readable_name)
        assert visible is not None
        assert visible.id == readable.id
        with pytest.raises(RuntimeError):
            await client.get_stream(hidden_name)

        await iggy_client.delete_user(created.id)


class TestUpdatePermissions:
    """Test permission updates via update_permissions."""

    @pytest.mark.asyncio
    async def test_update_permissions_grants_and_round_trips(
        self, iggy_client: IggyClient, unique_name
    ):
        """Test permissions granted after creation come back from get_user."""
        username, password = unique_credentials(unique_name)
        created = await iggy_client.create_user(username, password)
        assert created.permissions is None

        permissions = Permissions(global_=GlobalPermissions(read_streams=True))
        await iggy_client.update_permissions(created.id, permissions)

        fetched = await iggy_client.get_user(created.id)
        assert fetched is not None
        assert fetched.permissions == permissions

        await iggy_client.delete_user(created.id)

    @pytest.mark.asyncio
    async def test_update_permissions_replaces_previous_permissions(
        self, iggy_client: IggyClient, unique_name
    ):
        """Test update_permissions overwrites instead of merging."""
        username, password = unique_credentials(unique_name)
        initial = Permissions(
            global_=GlobalPermissions(read_streams=True, read_users=True)
        )
        created = await iggy_client.create_user(username, password, permissions=initial)

        replacement = Permissions(global_=GlobalPermissions(read_servers=True))
        await iggy_client.update_permissions(created.id, replacement)

        fetched = await iggy_client.get_user(created.id)
        assert fetched is not None
        fetched_permissions = fetched.permissions
        assert fetched_permissions is not None
        assert fetched_permissions == replacement
        assert fetched_permissions.global_.read_streams is False
        assert fetched_permissions.global_.read_users is False

        await iggy_client.delete_user(created.id)

    @pytest.mark.asyncio
    async def test_update_permissions_none_clears_permissions(
        self, iggy_client: IggyClient, unique_name
    ):
        """Test update_permissions with None removes existing permissions."""
        username, password = unique_credentials(unique_name)
        permissions = Permissions(global_=GlobalPermissions(read_streams=True))
        created = await iggy_client.create_user(
            username, password, permissions=permissions
        )

        await iggy_client.update_permissions(created.id)

        fetched = await iggy_client.get_user(created.id)
        assert fetched is not None
        assert fetched.permissions is None

        await iggy_client.delete_user(created.id)

    @pytest.mark.asyncio
    async def test_update_permissions_takes_effect_for_new_session(
        self, iggy_client: IggyClient, unique_name
    ):
        """Test a permission granted after creation is enforced on next login."""
        username, password = unique_credentials(unique_name)
        created = await iggy_client.create_user(username, password)

        denied_client = await login_fresh_client(username, password)
        with pytest.raises(RuntimeError):
            await denied_client.create_stream(unique_name(prefix="before-grant-"))

        await iggy_client.update_permissions(
            created.id, Permissions(global_=GlobalPermissions(manage_streams=True))
        )

        granted_client = await login_fresh_client(username, password)
        stream_name = unique_name(prefix="after-grant-")
        await granted_client.create_stream(stream_name)
        assert await iggy_client.get_stream(stream_name) is not None

        await iggy_client.delete_user(created.id)

    @pytest.mark.asyncio
    async def test_update_permissions_nonexistent_user_fails(
        self, iggy_client: IggyClient, unique_name
    ):
        """Test update_permissions raises for a non-existent user."""
        with pytest.raises(RuntimeError):
            await iggy_client.update_permissions(
                unique_name(max_bytes=MAX_USERNAME_BYTES),
                Permissions(global_=GlobalPermissions(read_streams=True)),
            )


class TestChangePassword:
    """Test password changes via change_password."""

    @pytest.mark.asyncio
    async def test_change_password_swaps_credentials(
        self, iggy_client: IggyClient, unique_name
    ):
        """Test the new password works for login and the old one is rejected."""
        username, password = unique_credentials(unique_name)
        new_password = unique_name(max_bytes=MAX_PASSWORD_BYTES)
        created = await iggy_client.create_user(username, password)

        await iggy_client.change_password(created.id, password, new_password)

        host, port = get_server_config()
        client = IggyClient(f"{host}:{port}")
        await client.connect()
        await wait_for_ping(client)
        with pytest.raises(RuntimeError):
            await client.login_user(username, password)
        await client.login_user(username, new_password)

        await iggy_client.delete_user(created.id)

    @pytest.mark.asyncio
    async def test_change_password_rejects_wrong_current_password(
        self, iggy_client: IggyClient, unique_name
    ):
        """Test change_password fails when the current password is wrong."""
        username = unique_name(max_bytes=MAX_USERNAME_BYTES)
        # Keep one byte of headroom so the wrong password stays within the
        # server limit and the failure is about the mismatch, not the length.
        password = unique_name(max_bytes=MAX_PASSWORD_BYTES - 1)
        created = await iggy_client.create_user(username, password)

        with pytest.raises(RuntimeError):
            await iggy_client.change_password(
                created.id, f"{password}x", unique_name(max_bytes=MAX_PASSWORD_BYTES)
            )

        client = await login_fresh_client(username, password)
        await client.ping()

        await iggy_client.delete_user(created.id)

    @pytest.mark.asyncio
    async def test_user_can_change_own_password(
        self, iggy_client: IggyClient, unique_name
    ):
        """Test a user without permissions can change its own password."""
        username, password = unique_credentials(unique_name)
        new_password = unique_name(max_bytes=MAX_PASSWORD_BYTES)
        created = await iggy_client.create_user(username, password)

        client = await login_fresh_client(username, password)
        await client.change_password(created.id, password, new_password)

        relogin = await login_fresh_client(username, new_password)
        await relogin.ping()

        await iggy_client.delete_user(created.id)

    @pytest.mark.parametrize(
        "new_password",
        ["a" * (MIN_PASSWORD_BYTES - 1), "a" * (MAX_PASSWORD_BYTES + 1)],
        ids=["too-short", "too-long"],
    )
    @pytest.mark.asyncio
    async def test_change_password_rejects_out_of_bounds_new_password(
        self, iggy_client: IggyClient, unique_name, new_password
    ):
        """Test change_password rejects new passwords outside the 3-100 byte range."""
        username, password = unique_credentials(unique_name)
        created = await iggy_client.create_user(username, password)

        with pytest.raises(RuntimeError):
            await iggy_client.change_password(created.id, password, new_password)

        client = await login_fresh_client(username, password)
        await client.ping()

        await iggy_client.delete_user(created.id)

    @pytest.mark.asyncio
    async def test_change_password_nonexistent_user_fails(
        self, iggy_client: IggyClient, unique_name
    ):
        """Test change_password raises for a non-existent user."""
        with pytest.raises(RuntimeError):
            await iggy_client.change_password(
                unique_name(max_bytes=MAX_USERNAME_BYTES),
                unique_name(max_bytes=MAX_PASSWORD_BYTES),
                unique_name(max_bytes=MAX_PASSWORD_BYTES),
            )


class TestLogoutUser:
    """Test session termination via logout_user."""

    @pytest.mark.asyncio
    async def test_logout_user_ends_the_session(
        self, iggy_client: IggyClient, unique_name
    ):
        """Test authenticated calls fail after logout."""
        username, password = unique_credentials(unique_name)
        created = await iggy_client.create_user(
            username,
            password,
            permissions=Permissions(global_=GlobalPermissions(read_users=True)),
        )

        client = await login_fresh_client(username, password)
        assert await client.get_user(username) is not None

        await client.logout_user()

        with pytest.raises(RuntimeError):
            await client.get_user(username)

        await iggy_client.delete_user(created.id)

    @pytest.mark.asyncio
    async def test_login_after_logout_restores_the_session(
        self, iggy_client: IggyClient, unique_name
    ):
        """Test the same client can log in again after logging out."""
        username, password = unique_credentials(unique_name)
        created = await iggy_client.create_user(
            username,
            password,
            permissions=Permissions(global_=GlobalPermissions(read_users=True)),
        )

        client = await login_fresh_client(username, password)
        await client.logout_user()

        await client.login_user(username, password)
        assert await client.get_user(username) is not None

        await iggy_client.delete_user(created.id)
