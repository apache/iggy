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

"""Tests for connection introspection via get_me, get_client and get_clients."""

import pytest

from apache_iggy import (
    ClientInfo,
    ClientInfoDetails,
    GlobalPermissions,
    IggyClient,
    Permissions,
)

from .utils import login_fresh_client, unique_credentials


class TestGetMe:
    """Test the currently connected client via get_me."""

    @pytest.mark.asyncio
    async def test_get_me_returns_connected_client(self, iggy_client: IggyClient):
        """Test get_me describes this connection over the fixture's transport."""
        me = await iggy_client.get_me()

        assert isinstance(me, ClientInfoDetails)
        assert me.client_id > 0
        assert me.address
        assert me.transport == "TCP"

    @pytest.mark.asyncio
    async def test_get_me_user_id_matches_logged_in_user(self, iggy_client: IggyClient):
        """Test the reported user id is the one the fixture authenticated as."""
        me = await iggy_client.get_me()
        root = await iggy_client.get_user("iggy")

        assert root is not None
        assert me.user_id == root.id

    @pytest.mark.asyncio
    async def test_get_me_reports_joined_consumer_group(
        self, iggy_client: IggyClient, unique_name
    ):
        """Test a joined group appears with the stream, topic and group ids."""
        # Topic and consumer group ids restart per parent, so a fresh stream
        # hands its first topic and first group the same id and a swapped
        # field mapping would still satisfy the assertions below. Creating
        # three candidates of each and picking one that collides with
        # nothing keeps the three ids distinct.
        stream_name = unique_name()
        await iggy_client.create_stream(stream_name)
        stream = await iggy_client.get_stream(stream_name)
        assert stream is not None

        topics = []
        for _ in range(3):
            name = unique_name()
            await iggy_client.create_topic(
                stream=stream_name,
                name=name,
                partitions_count=1,
            )
            details = await iggy_client.get_topic(stream_name, name)
            assert details is not None
            topics.append((name, details))
        topic_name, topic = next((n, t) for n, t in topics if t.id != stream.id)

        groups = []
        for _ in range(3):
            name = unique_name()
            await iggy_client.create_consumer_group(stream_name, topic_name, name)
            details = await iggy_client.get_consumer_group(
                stream_name, topic_name, name
            )
            assert details is not None
            groups.append((name, details))
        group_name, group = next(
            (n, g) for n, g in groups if g.id not in (stream.id, topic.id)
        )

        # Guard the assertions below: equal ids would survive a swapped mapping.
        assert len({stream.id, topic.id, group.id}) == 3

        # A fresh connection keeps the session-scoped fixture out of the group.
        member = await login_fresh_client("iggy", "iggy")
        await member.join_consumer_group(stream_name, topic_name, group_name)

        me = await member.get_me()
        assert me.consumer_groups_count == 1
        assert len(me.consumer_groups) == 1

        joined = me.consumer_groups[0]
        assert joined.stream_id == stream.id
        assert joined.topic_id == topic.id
        assert joined.group_id == group.id

        await member.leave_consumer_group(stream_name, topic_name, group_name)
        await iggy_client.delete_consumer_group(stream_name, topic_name, group_name)


class TestGetClients:
    """Test the connected client listing via get_clients."""

    @pytest.mark.asyncio
    async def test_get_clients_contains_this_client(self, iggy_client: IggyClient):
        """Test the id reported by get_me appears in the listing."""
        me = await iggy_client.get_me()
        clients = await iggy_client.get_clients()

        assert all(isinstance(client, ClientInfo) for client in clients)

        # get_clients() is a best-effort scatter-gather across shards: one
        # that misses the server's LIST_CLIENTS_GATHER_TIMEOUT (3s) budget is
        # dropped from the result rather than failing the whole read. This
        # test server is unloaded and single-node, so every shard replies
        # well within budget and the caller's own entry is always present;
        # that would not hold under load or across a busier cluster.
        mine = next((c for c in clients if c.client_id == me.client_id), None)
        assert mine is not None
        assert mine.address == me.address
        assert mine.transport == me.transport
        assert mine.user_id == me.user_id


class TestGetClient:
    """Test single client lookup via get_client."""

    @pytest.mark.asyncio
    async def test_get_client_by_id_matches_get_me(self, iggy_client: IggyClient):
        """Test get_client on this connection's id returns the same details."""
        me = await iggy_client.get_me()

        client = await iggy_client.get_client(me.client_id)
        assert client is not None
        assert client.client_id == me.client_id
        assert client.user_id == me.user_id
        assert client.transport == me.transport

    @pytest.mark.asyncio
    async def test_get_client_unknown_id_returns_none(self, iggy_client: IggyClient):
        """Test an unknown client id resolves to None rather than raising."""
        clients = await iggy_client.get_clients()
        unused_id = max((c.client_id for c in clients), default=0) + 1_000_000

        assert await iggy_client.get_client(unused_id) is None

    @pytest.mark.parametrize("out_of_range", [-1, 2**32], ids=["negative", "above-u32"])
    @pytest.mark.asyncio
    async def test_get_client_out_of_range_id_raises_overflow_error(
        self, iggy_client: IggyClient, out_of_range: int
    ):
        """Test a client_id outside the u32 wire range raises OverflowError.

        pyo3 converts the argument to u32 before the awaitable exists, so this
        fails synchronously with OverflowError rather than surfacing as a
        RuntimeError from the request itself.
        """
        with pytest.raises(OverflowError):
            iggy_client.get_client(out_of_range)


class TestServerInfoPermission:
    """Test the read_servers gate on get_client and get_clients."""

    @pytest.mark.asyncio
    async def test_user_with_read_servers_can_list_clients(
        self, iggy_client: IggyClient, unique_name
    ):
        """Test read_servers grants both get_client and get_clients."""
        username, password = unique_credentials(unique_name)
        permissions = Permissions(
            global_permissions=GlobalPermissions(read_servers=True)
        )
        created = await iggy_client.create_user(
            username, password, permissions=permissions
        )

        client = await login_fresh_client(username, password)
        me = await client.get_me()

        clients = await client.get_clients()
        # Best-effort scatter-gather across shards, see the completeness note
        # on test_get_clients_contains_this_client; holds here for the same
        # reason.
        assert any(other.client_id == me.client_id for other in clients)
        assert (await client.get_client(me.client_id)) is not None

        await iggy_client.delete_user(created.id)

    @pytest.mark.asyncio
    async def test_user_without_read_servers_cannot_list_clients(
        self, iggy_client: IggyClient, unique_name
    ):
        """Test get_client and get_clients are denied without read_servers."""
        username, password = unique_credentials(unique_name)
        created = await iggy_client.create_user(username, password)

        client = await login_fresh_client(username, password)
        # get_me needs authentication only, so it stays available.
        me = await client.get_me()
        assert me.user_id == created.id

        with pytest.raises(RuntimeError):
            await client.get_clients()
        with pytest.raises(RuntimeError):
            await client.get_client(me.client_id)

        await iggy_client.delete_user(created.id)
