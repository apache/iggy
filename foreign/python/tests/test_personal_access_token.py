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

import time

import pytest

from apache_iggy import IggyClient, IggyError, PersonalAccessTokenInfo

from .test_user import NEVER_EXPIRE, _create_personal_access_token
from .utils import get_http_server_config, login_fresh_client, unique_credentials

ONE_HOUR_US = 3_600_000_000
# The server may run in a container whose clock drifts slightly from the host.
CLOCK_SLACK_US = 5_000_000
RESOURCE_NOT_FOUND = 20


@pytest.fixture
async def new_user(iggy_client: IggyClient, unique_name):
    """Create a user for one test and delete it afterwards, even on failure."""
    username, password = unique_credentials(unique_name)
    created = await iggy_client.create_user(username, password)
    try:
        yield username, password
    finally:
        await iggy_client.delete_user(created.id)


class TestPersonalAccessTokens:
    """Test listing and deleting personal access tokens."""

    @pytest.mark.asyncio
    async def test_new_user_has_no_tokens(self, new_user):
        """Test get_personal_access_tokens returns an empty list for a new user."""
        username, password = new_user

        client = await login_fresh_client(username, password)
        assert await client.get_personal_access_tokens() == []

    @pytest.mark.asyncio
    async def test_created_token_is_listed(self, new_user, unique_name):
        """Test a created token appears in get_personal_access_tokens."""
        username, password = new_user
        name = unique_name(min_bytes=16, max_bytes=30)
        http_host, http_port = get_http_server_config()
        _create_personal_access_token(
            f"http://{http_host}:{http_port}", username, password, name, NEVER_EXPIRE
        )

        client = await login_fresh_client(username, password)
        tokens = await client.get_personal_access_tokens()

        assert len(tokens) == 1
        assert isinstance(tokens[0], PersonalAccessTokenInfo)
        assert tokens[0].name == name
        assert tokens[0].expiry is None

    @pytest.mark.asyncio
    async def test_finite_expiry_is_listed_in_microseconds(self, new_user, unique_name):
        """Test a finite expiry is listed as a Unix timestamp in microseconds."""
        username, password = new_user
        name = unique_name(min_bytes=16, max_bytes=30)
        http_host, http_port = get_http_server_config()
        before = time.time_ns() // 1_000
        _create_personal_access_token(
            f"http://{http_host}:{http_port}", username, password, name, ONE_HOUR_US
        )
        after = time.time_ns() // 1_000

        client = await login_fresh_client(username, password)
        tokens = await client.get_personal_access_tokens()

        assert len(tokens) == 1
        assert tokens[0].expiry is not None
        assert (
            before + ONE_HOUR_US - CLOCK_SLACK_US
            <= tokens[0].expiry
            <= after + ONE_HOUR_US + CLOCK_SLACK_US
        )

    @pytest.mark.asyncio
    async def test_delete_removes_token(self, new_user, unique_name):
        """Test delete_personal_access_token removes the token from the list."""
        username, password = new_user
        name = unique_name(min_bytes=16, max_bytes=30)
        http_host, http_port = get_http_server_config()
        _create_personal_access_token(
            f"http://{http_host}:{http_port}", username, password, name, NEVER_EXPIRE
        )

        client = await login_fresh_client(username, password)
        await client.delete_personal_access_token(name)

        assert await client.get_personal_access_tokens() == []

    @pytest.mark.asyncio
    async def test_delete_unknown_token_fails(self, new_user, unique_name):
        """Test delete_personal_access_token raises for an unknown name."""
        username, password = new_user

        client = await login_fresh_client(username, password)
        with pytest.raises(IggyError) as refusal:
            await client.delete_personal_access_token(
                unique_name(min_bytes=16, max_bytes=30)
            )
        assert refusal.value.code == RESOURCE_NOT_FOUND
