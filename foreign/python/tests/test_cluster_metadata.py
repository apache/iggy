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

from apache_iggy import ClusterNodeRole, ClusterNodeStatus, IggyClient

from .utils import (
    get_server_config,
    login_fresh_client,
    unique_credentials,
    wait_for_server,
)


class TestClusterMetadata:
    """Test cluster metadata retrieval."""

    @pytest.mark.asyncio
    async def test_get_cluster_metadata(self, iggy_client: IggyClient):
        """A server running without clustering reports itself as the only node,
        the healthy leader."""
        metadata = await iggy_client.get_cluster_metadata()

        assert metadata.name
        nodes = metadata.nodes
        assert len(nodes) == 1

        node = nodes[0]
        assert node.replica_id == 0
        assert node.name
        assert node.ip
        assert node.role == ClusterNodeRole.LEADER
        assert node.status == ClusterNodeStatus.HEALTHY

        endpoints = node.endpoints
        # The connection under test is TCP, so the node must advertise it. The
        # port is not compared with the one the test connected to, because the
        # server may be reached through a published port that differs from it.
        assert endpoints.tcp > 0

        assert f"name='{metadata.name}'" in repr(metadata)
        assert "nodes_count=1" in repr(metadata)
        assert "replica_id=0" in repr(node)
        assert f"name='{node.name}'" in repr(node)
        assert "role=ClusterNodeRole.LEADER" in repr(node)
        assert "status=ClusterNodeStatus.HEALTHY" in repr(node)
        assert f"tcp={endpoints.tcp}" in repr(endpoints)

    @pytest.mark.asyncio
    async def test_get_cluster_metadata_requires_connection_and_auth(self):
        """get_cluster_metadata fails before connecting, before login, and after
        logout."""
        host, port = get_server_config()
        wait_for_server(host, port)

        client = IggyClient(f"{host}:{port}")
        with pytest.raises(RuntimeError, match="Not connected"):
            await client.get_cluster_metadata()

        await client.connect()
        with pytest.raises(RuntimeError, match="Unauthenticated"):
            await client.get_cluster_metadata()

        await client.login_user("iggy", "iggy")
        await client.get_cluster_metadata()

        await client.logout_user()
        with pytest.raises(RuntimeError, match="Unauthenticated"):
            await client.get_cluster_metadata()

    @pytest.mark.asyncio
    async def test_get_cluster_metadata_needs_no_permission(
        self, iggy_client: IggyClient, unique_name
    ):
        """Unlike get_stats, any logged-in user may read the cluster metadata,
        even one holding no permissions at all."""
        username, password = unique_credentials(unique_name)
        created = await iggy_client.create_user(username, password)

        try:
            plain_client = await login_fresh_client(username, password)
            metadata = await plain_client.get_cluster_metadata()
            assert len(metadata.nodes) >= 1
        finally:
            await iggy_client.delete_user(created.id)
