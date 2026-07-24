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

from apache_iggy import (
    GlobalPermissions,
    IggyClient,
    Permissions,
    UserInfoDetails,
    UserStatus,
)

from .utils import (
    MAX_PASSWORD_BYTES,
    MAX_USERNAME_BYTES,
    MIN_PASSWORD_BYTES,
    MIN_USERNAME_BYTES,
    get_server_config,
    unique_credentials,
    wait_for_ping,
    wait_for_server,
)


class TestCreateUser:
    """Test user creation via create_user."""

    @pytest.mark.asyncio
    async def test_create_and_get_user(self, iggy_client: IggyClient, unique_name):
        """Test user creation returns details and the user is retrievable."""
        username, password = unique_credentials(unique_name)

        created = await iggy_client.create_user(username, password, UserStatus.Active)
        assert isinstance(created, UserInfoDetails)
        assert isinstance(created.id, int)
        assert created.username == username
        assert created.status == UserStatus.Active
        assert created.permissions is None

        user_by_name = await iggy_client.get_user(username)
        assert user_by_name is not None
        assert user_by_name.id == created.id
        assert user_by_name.username == username
        assert user_by_name.status == UserStatus.Active
        assert user_by_name.permissions is None

        user_by_id = await iggy_client.get_user(created.id)
        assert user_by_id is not None
        assert user_by_id.id == created.id
        assert user_by_id.username == username

        await iggy_client.delete_user(created.id)

    @pytest.mark.asyncio
    async def test_create_user_defaults_to_active_status(
        self, iggy_client: IggyClient, unique_name
    ):
        """Test create_user without an explicit status creates an active user."""
        username, password = unique_credentials(unique_name)

        created = await iggy_client.create_user(username, password)
        assert created.status == UserStatus.Active

        await iggy_client.delete_user(created.id)

    @pytest.mark.asyncio
    async def test_create_inactive_user(self, iggy_client: IggyClient, unique_name):
        """Test create_user with UserStatus.Inactive creates an inactive user."""
        username, password = unique_credentials(unique_name)

        created = await iggy_client.create_user(username, password, UserStatus.Inactive)
        assert created.status == UserStatus.Inactive

        user = await iggy_client.get_user(username)
        assert user is not None
        assert user.status == UserStatus.Inactive

        await iggy_client.delete_user(created.id)

    @pytest.mark.asyncio
    async def test_duplicate_username_fails(self, iggy_client: IggyClient, unique_name):
        """Test create_user rejects an already taken username."""
        username, password = unique_credentials(unique_name)

        created = await iggy_client.create_user(username, password)

        with pytest.raises(RuntimeError):
            await iggy_client.create_user(username, password)

        await iggy_client.delete_user(created.id)

    @pytest.mark.asyncio
    async def test_created_user_can_login(self, iggy_client: IggyClient, unique_name):
        """Test a freshly created user can authenticate with its credentials."""
        username, password = unique_credentials(unique_name)
        created = await iggy_client.create_user(username, password)

        host, port = get_server_config()
        client = IggyClient(f"{host}:{port}")
        await client.connect()
        await wait_for_ping(client)
        await client.login_user(username, password)

        await iggy_client.delete_user(created.id)

    @pytest.mark.asyncio
    async def test_inactive_user_cannot_login(
        self, iggy_client: IggyClient, unique_name
    ):
        """Test an inactive user is denied login even with correct credentials."""
        username, password = unique_credentials(unique_name)
        created = await iggy_client.create_user(username, password, UserStatus.Inactive)

        host, port = get_server_config()
        client = IggyClient(f"{host}:{port}")
        await client.connect()
        with pytest.raises(RuntimeError):
            await client.login_user(username, password)

        await iggy_client.delete_user(created.id)

    @pytest.mark.asyncio
    async def test_deleted_user_cannot_login(
        self, iggy_client: IggyClient, unique_name
    ):
        """Test a deleted user cannot start a fresh authenticated session."""
        username, password = unique_credentials(unique_name)
        created = await iggy_client.create_user(username, password)
        await iggy_client.delete_user(created.id)

        host, port = get_server_config()
        client = IggyClient(f"{host}:{port}")
        await client.connect()
        with pytest.raises(RuntimeError):
            await client.login_user(username, password)

    @pytest.mark.parametrize(
        "username",
        ["a" * (MIN_USERNAME_BYTES - 1), "a" * (MAX_USERNAME_BYTES + 1)],
        ids=["too-short", "too-long"],
    )
    @pytest.mark.asyncio
    async def test_create_user_rejects_out_of_bounds_username(
        self, iggy_client: IggyClient, unique_name, username
    ):
        """Test create_user rejects usernames outside the 3-50 byte range."""
        with pytest.raises(RuntimeError):
            await iggy_client.create_user(
                username, unique_name(max_bytes=MAX_PASSWORD_BYTES)
            )

    @pytest.mark.parametrize(
        "password",
        ["a" * (MIN_PASSWORD_BYTES - 1), "a" * (MAX_PASSWORD_BYTES + 1)],
        ids=["too-short", "too-long"],
    )
    @pytest.mark.asyncio
    async def test_create_user_rejects_out_of_bounds_password(
        self, iggy_client: IggyClient, unique_name, password
    ):
        """Test create_user rejects passwords outside the 3-100 byte range."""
        with pytest.raises(RuntimeError):
            await iggy_client.create_user(
                unique_name(max_bytes=MAX_USERNAME_BYTES), password
            )

    @pytest.mark.parametrize(
        "prefix",
        ["ユーザー", "사용자", "ผู้ใช้", "🦀🚀"],
        ids=["japanese", "korean", "thai", "emoji"],
    )
    @pytest.mark.asyncio
    async def test_create_user_accepts_multibyte_credentials(
        self, iggy_client: IggyClient, unique_name, prefix
    ):
        """Test non-ascii credentials within the byte limits are accepted."""
        username = f"{prefix}{unique_name(min_bytes=4, max_bytes=8)}"
        password = f"{prefix}{unique_name(min_bytes=4, max_bytes=8)}"
        assert len(username.encode()) <= MAX_USERNAME_BYTES

        created = await iggy_client.create_user(username, password)
        assert created.username == username

        fetched = await iggy_client.get_user(username)
        assert fetched is not None
        assert fetched.id == created.id

        host, port = get_server_config()
        client = IggyClient(f"{host}:{port}")
        await client.connect()
        await client.login_user(username, password)

        await iggy_client.delete_user(created.id)


class TestGetUser:
    """Test user retrieval via get_user."""

    @pytest.mark.asyncio
    async def test_get_root_user(self, iggy_client: IggyClient):
        """Test the default root user is retrievable by username."""
        user = await iggy_client.get_user("iggy")
        assert user is not None
        assert user.username == "iggy"
        assert user.status == UserStatus.Active

    @pytest.mark.asyncio
    async def test_get_nonexistent_user_returns_none(
        self, iggy_client: IggyClient, unique_name
    ):
        """Test getting a non-existent user by name or numeric id returns None."""
        user_by_name = await iggy_client.get_user(
            unique_name(max_bytes=MAX_USERNAME_BYTES)
        )
        assert user_by_name is None

        # Deleting a freshly created user guarantees its id is vacant.
        username, password = unique_credentials(unique_name)
        created = await iggy_client.create_user(username, password)
        await iggy_client.delete_user(created.id)

        user_by_id = await iggy_client.get_user(created.id)
        assert user_by_id is None

    @pytest.mark.asyncio
    async def test_get_user_rejects_empty_identifier_locally(
        self, iggy_client: IggyClient
    ):
        """Test the empty string is rejected client-side before any server call."""
        with pytest.raises(ValueError):
            iggy_client.get_user("")

    @pytest.mark.parametrize("user_id", [-1, 2**32], ids=["negative", "above-u32"])
    @pytest.mark.asyncio
    async def test_get_user_rejects_out_of_range_numeric_id(
        self, iggy_client: IggyClient, user_id
    ):
        """Test numeric ids outside the u32 range are rejected client-side."""
        with pytest.raises(TypeError):
            iggy_client.get_user(user_id)

    @pytest.mark.asyncio
    async def test_get_user_returns_same_result_when_called_repeatedly(
        self, iggy_client: IggyClient, unique_name
    ):
        """Test repeated get_user calls return the same user view."""
        username, password = unique_credentials(unique_name)
        created = await iggy_client.create_user(username, password)

        first = await iggy_client.get_user(username)
        second = await iggy_client.get_user(username)
        assert first is not None
        assert second is not None
        assert first.id == second.id
        assert first.username == second.username
        assert first.status == second.status

        await iggy_client.delete_user(created.id)


class TestGetUsers:
    """Test listing users via get_users."""

    @pytest.mark.asyncio
    async def test_get_users_lists_root_and_created_users(
        self, iggy_client: IggyClient, unique_name
    ):
        """Test get_users returns the root user and newly created users."""
        first_username, first_password = unique_credentials(unique_name)
        second_username, second_password = unique_credentials(unique_name)

        first = await iggy_client.create_user(first_username, first_password)
        second = await iggy_client.create_user(second_username, second_password)

        users = await iggy_client.get_users()
        usernames = [user.username for user in users]
        assert "iggy" in usernames
        assert first_username in usernames
        assert second_username in usernames
        assert all(isinstance(user.id, int) for user in users)
        assert all(isinstance(user.status, UserStatus) for user in users)

        user_ids = [user.id for user in users]
        assert user_ids == sorted(set(user_ids)), (
            "get_users must return users in strictly ascending id order"
        )

        await iggy_client.delete_user(first.id)
        await iggy_client.delete_user(second.id)


class TestUpdateUser:
    """Test user updates via update_user."""

    @pytest.mark.asyncio
    async def test_update_username(self, iggy_client: IggyClient, unique_name):
        """Test update_user renames a user."""
        username, password = unique_credentials(unique_name)
        new_username = unique_name(max_bytes=MAX_USERNAME_BYTES)

        created = await iggy_client.create_user(username, password)

        await iggy_client.update_user(username, username=new_username)

        assert await iggy_client.get_user(username) is None
        renamed = await iggy_client.get_user(new_username)
        assert renamed is not None
        assert renamed.id == created.id
        assert renamed.username == new_username

        await iggy_client.delete_user(created.id)

    @pytest.mark.asyncio
    async def test_update_status(self, iggy_client: IggyClient, unique_name):
        """Test update_user changes the user status."""
        username, password = unique_credentials(unique_name)
        created = await iggy_client.create_user(username, password)
        assert created.status == UserStatus.Active

        await iggy_client.update_user(created.id, status=UserStatus.Inactive)

        user = await iggy_client.get_user(created.id)
        assert user is not None
        assert user.status == UserStatus.Inactive
        assert user.username == username

        await iggy_client.delete_user(created.id)

    @pytest.mark.asyncio
    async def test_update_username_and_status_together(
        self, iggy_client: IggyClient, unique_name
    ):
        """Test update_user applies a new username and status in one call."""
        username, password = unique_credentials(unique_name)
        new_username = unique_name(max_bytes=MAX_USERNAME_BYTES)
        created = await iggy_client.create_user(username, password)

        await iggy_client.update_user(
            created.id, username=new_username, status=UserStatus.Inactive
        )

        user = await iggy_client.get_user(created.id)
        assert user is not None
        assert user.username == new_username
        assert user.status == UserStatus.Inactive

        await iggy_client.delete_user(created.id)

    @pytest.mark.asyncio
    async def test_update_user_with_no_fields_is_a_noop(
        self, iggy_client: IggyClient, unique_name
    ):
        """Test the server permits an update with neither username nor status."""
        username, password = unique_credentials(unique_name)
        created = await iggy_client.create_user(username, password)

        await iggy_client.update_user(created.id)

        user = await iggy_client.get_user(created.id)
        assert user is not None
        assert user.username == username
        assert user.status == UserStatus.Active

        await iggy_client.delete_user(created.id)

    @pytest.mark.asyncio
    async def test_update_user_applied_repeatedly_is_idempotent(
        self, iggy_client: IggyClient, unique_name
    ):
        """Test applying the same update twice succeeds and keeps the state."""
        username, password = unique_credentials(unique_name)
        created = await iggy_client.create_user(username, password)

        await iggy_client.update_user(created.id, status=UserStatus.Inactive)
        await iggy_client.update_user(created.id, status=UserStatus.Inactive)

        user = await iggy_client.get_user(created.id)
        assert user is not None
        assert user.status == UserStatus.Inactive
        assert user.username == username

        await iggy_client.delete_user(created.id)

    @pytest.mark.asyncio
    async def test_update_username_to_existing_username_fails(
        self, iggy_client: IggyClient, unique_name
    ):
        """Test update_user rejects renaming to a username another user holds."""
        first_username, first_password = unique_credentials(unique_name)
        second_username, second_password = unique_credentials(unique_name)
        first = await iggy_client.create_user(first_username, first_password)
        second = await iggy_client.create_user(second_username, second_password)

        with pytest.raises(RuntimeError):
            await iggy_client.update_user(first.id, username=second_username)

        unchanged = await iggy_client.get_user(first.id)
        assert unchanged is not None
        assert unchanged.username == first_username
        assert unchanged.status == UserStatus.Active

        await iggy_client.delete_user(first.id)
        await iggy_client.delete_user(second.id)

    @pytest.mark.asyncio
    async def test_update_nonexistent_user_fails(
        self, iggy_client: IggyClient, unique_name
    ):
        """Test update_user raises for a non-existent user."""
        with pytest.raises(RuntimeError):
            await iggy_client.update_user(
                unique_name(max_bytes=MAX_USERNAME_BYTES),
                status=UserStatus.Inactive,
            )

    @pytest.mark.parametrize(
        "new_username",
        ["a" * (MIN_USERNAME_BYTES - 1), "a" * (MAX_USERNAME_BYTES + 1), "あ" * 17],
        ids=["too-short", "too-long", "multibyte-51-bytes"],
    )
    @pytest.mark.asyncio
    async def test_update_user_rejects_out_of_bounds_username(
        self, iggy_client: IggyClient, unique_name, new_username
    ):
        """Test update_user rejects usernames outside the 3-50 byte range."""
        username, password = unique_credentials(unique_name)
        created = await iggy_client.create_user(username, password)

        with pytest.raises(RuntimeError):
            await iggy_client.update_user(created.id, username=new_username)

        unchanged = await iggy_client.get_user(created.id)
        assert unchanged is not None
        assert unchanged.username == username
        assert unchanged.status == UserStatus.Active

        await iggy_client.delete_user(created.id)

    @pytest.mark.asyncio
    async def test_update_user_accepts_multibyte_username(
        self, iggy_client: IggyClient, unique_name
    ):
        """Test update_user accepts a non-ascii username within the byte limit."""
        username, password = unique_credentials(unique_name)
        created = await iggy_client.create_user(username, password)

        new_username = f"사용자{unique_name(min_bytes=4, max_bytes=8)}"
        await iggy_client.update_user(created.id, username=new_username)

        renamed = await iggy_client.get_user(created.id)
        assert renamed is not None
        assert renamed.username == new_username

        await iggy_client.delete_user(created.id)


class TestDeleteUser:
    """Test user deletion via delete_user."""

    @pytest.mark.asyncio
    async def test_delete_user_by_name(self, iggy_client: IggyClient, unique_name):
        """Test delete_user removes a user addressed by username."""
        username, password = unique_credentials(unique_name)
        await iggy_client.create_user(username, password)

        await iggy_client.delete_user(username)

        assert await iggy_client.get_user(username) is None

    @pytest.mark.asyncio
    async def test_delete_user_by_numeric_id(
        self, iggy_client: IggyClient, unique_name
    ):
        """Test delete_user removes a user addressed by numeric id."""
        username, password = unique_credentials(unique_name)
        created = await iggy_client.create_user(username, password)

        await iggy_client.delete_user(created.id)

        assert await iggy_client.get_user(created.id) is None

    @pytest.mark.asyncio
    async def test_delete_nonexistent_user_fails(
        self, iggy_client: IggyClient, unique_name
    ):
        """Test delete_user raises for a non-existent user."""
        with pytest.raises(RuntimeError):
            await iggy_client.delete_user(unique_name(max_bytes=MAX_USERNAME_BYTES))

    @pytest.mark.parametrize("root_identifier", ["iggy", 0], ids=["by-name", "by-id"])
    @pytest.mark.asyncio
    async def test_delete_root_user_fails(
        self, iggy_client: IggyClient, root_identifier
    ):
        """Test the root user cannot be deleted by username or numeric id."""
        with pytest.raises(RuntimeError):
            await iggy_client.delete_user(root_identifier)

        root = await iggy_client.get_user("iggy")
        assert root is not None
        assert root.status == UserStatus.Active

    @pytest.mark.asyncio
    async def test_delete_user_twice_fails(self, iggy_client: IggyClient, unique_name):
        """Test deleting the same user twice fails on the second call."""
        username, password = unique_credentials(unique_name)
        created = await iggy_client.create_user(username, password)

        await iggy_client.delete_user(created.id)

        with pytest.raises(RuntimeError):
            await iggy_client.delete_user(created.id)

    @pytest.mark.asyncio
    async def test_delete_inactive_user(self, iggy_client: IggyClient, unique_name):
        """Test an inactive user can be deleted."""
        username, password = unique_credentials(unique_name)
        created = await iggy_client.create_user(username, password, UserStatus.Inactive)

        await iggy_client.delete_user(created.id)

        assert await iggy_client.get_user(created.id) is None

    @pytest.mark.asyncio
    async def test_deleted_user_disappears_from_listings(
        self, iggy_client: IggyClient, unique_name
    ):
        """Test a deleted user is absent from both get_user and get_users."""
        username, password = unique_credentials(unique_name)
        created = await iggy_client.create_user(username, password)

        await iggy_client.delete_user(created.id)

        assert await iggy_client.get_user(username) is None
        assert await iggy_client.get_user(created.id) is None
        users = await iggy_client.get_users()
        assert created.id not in [user.id for user in users]
        assert username not in [user.username for user in users]

    @pytest.mark.asyncio
    async def test_deleted_user_live_session_loses_identity(
        self, iggy_client: IggyClient, unique_name
    ):
        """Test a session owned by a deleted user can no longer act as that user."""
        username, password = unique_credentials(unique_name)
        created = await iggy_client.create_user(
            username,
            password,
            permissions=Permissions(global_=GlobalPermissions(read_users=True)),
        )

        host, port = get_server_config()
        client = IggyClient(f"{host}:{port}")
        await client.connect()
        await wait_for_ping(client)
        await client.login_user(username, password)
        users_before = await client.get_users()
        assert any(user.id == created.id for user in users_before)

        await iggy_client.delete_user(created.id)

        with pytest.raises(RuntimeError):
            await client.get_users()

    @pytest.mark.asyncio
    async def test_deleted_username_is_reusable_with_fresh_credentials(
        self, iggy_client: IggyClient, unique_name
    ):
        """Test a deleted username can be recreated without old password state."""
        username = unique_name(max_bytes=MAX_USERNAME_BYTES)
        password = unique_name(max_bytes=MAX_USERNAME_BYTES)
        new_password = f"{password}x"

        first = await iggy_client.create_user(username, password)
        await iggy_client.delete_user(first.id)

        recreated = await iggy_client.create_user(username, new_password)

        host, port = get_server_config()
        client = IggyClient(f"{host}:{port}")
        await client.connect()
        with pytest.raises(RuntimeError):
            await client.login_user(username, password)
        await client.login_user(username, new_password)

        await iggy_client.delete_user(recreated.id)


@pytest.mark.parametrize(
    "method_name",
    [
        "get_user",
        "get_users",
        "create_user",
        "update_user",
        "delete_user",
        "update_permissions",
        "change_password",
        "logout_user",
    ],
)
@pytest.mark.asyncio
async def test_user_management_requires_connection_and_auth(method_name, unique_name):
    """Test user management methods fail before connecting and before login."""
    host, port = get_server_config()
    wait_for_server(host, port)

    client = IggyClient(f"{host}:{port}")
    username = unique_name(max_bytes=MAX_USERNAME_BYTES)
    args_by_method = {
        "get_user": (username,),
        "get_users": (),
        "create_user": (username, "secret"),
        "update_user": (username,),
        "delete_user": (username,),
        "update_permissions": (username,),
        "change_password": (username, "secret", "secret2"),
        "logout_user": (),
    }
    method = getattr(client, method_name)
    args = args_by_method[method_name]

    with pytest.raises(RuntimeError):
        await method(*args)

    await client.connect()
    with pytest.raises(RuntimeError):
        await method(*args)
