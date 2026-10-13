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

import contextlib
import enum
import json
import threading
from collections.abc import Iterator
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

import pytest

from apache_iggy import ClusterNodeRole, ClusterNodeStatus, HttpConfig, IggyClient

from .utils import (
    get_server_config,
    login_fresh_client,
    unique_credentials,
    wait_for_server,
)

# A node's role and status as the HTTP API spells them, then the Python values
# they must become. A server without clustering only reports a healthy leader,
# so the TCP tests below never see the other values.
REPORTED_NODES = [
    ("leader", "healthy", ClusterNodeRole.LEADER, ClusterNodeStatus.HEALTHY),
    ("follower", "starting", ClusterNodeRole.FOLLOWER, ClusterNodeStatus.STARTING),
    ("follower", "stopping", ClusterNodeRole.FOLLOWER, ClusterNodeStatus.STOPPING),
    (
        "follower",
        "unreachable",
        ClusterNodeRole.FOLLOWER,
        ClusterNodeStatus.UNREACHABLE,
    ),
    (
        "follower",
        "maintenance",
        ClusterNodeRole.FOLLOWER,
        ClusterNodeStatus.MAINTENANCE,
    ),
    ("follower", "unknown", ClusterNodeRole.FOLLOWER, ClusterNodeStatus.UNKNOWN),
]


@contextlib.contextmanager
def serve_cluster_metadata() -> Iterator[int]:
    """Serve a multi-node `GET /cluster/metadata` on a free local port.

    `/ping` is answered too, because connecting an HTTP client starts a heartbeat.
    """
    metadata = {
        "name": "stub-cluster",
        "nodes": [
            {
                "name": f"node-{index}",
                "ip": f"10.0.0.{index}",
                "endpoints": {
                    "tcp": 8090 + index,
                    "quic": 8080 + index,
                    "http": 3000 + index,
                    "websocket": 8092 + index,
                },
                "role": role,
                "status": status,
            }
            for index, (role, status, _, _) in enumerate(REPORTED_NODES, start=1)
        ],
    }
    responses = {
        "/cluster/metadata": ("application/json", json.dumps(metadata).encode()),
        "/ping": ("text/plain", b"pong"),
    }

    class Handler(BaseHTTPRequestHandler):
        def do_GET(self):
            if self.path not in responses:
                self.send_error(404)
                return
            content_type, body = responses[self.path]
            self.send_response(200)
            self.send_header("Content-Type", content_type)
            self.send_header("Content-Length", str(len(body)))
            self.end_headers()
            self.wfile.write(body)

        def log_message(self, format, *args):  # noqa: ARG002
            """Keep the test output free of per-request log lines."""

    server = ThreadingHTTPServer(("127.0.0.1", 0), Handler)
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    try:
        yield server.server_address[1]
    finally:
        server.shutdown()
        server.server_close()
        thread.join()


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
        assert f"name='{node.name}'" in repr(node)
        assert "role=ClusterNodeRole.LEADER" in repr(node)
        assert "status=ClusterNodeStatus.HEALTHY" in repr(node)
        assert f"tcp={endpoints.tcp}" in repr(endpoints)

    @pytest.mark.unit
    def test_role_and_status_are_python_enums(self):
        """Roles and statuses are str-backed Enums: they iterate, expose `.name`
        and `.value`, look up by the server's spelling, and can key a dict."""
        assert issubclass(ClusterNodeRole, enum.Enum)
        assert issubclass(ClusterNodeStatus, enum.Enum)

        assert [role.name for role in ClusterNodeRole] == ["LEADER", "FOLLOWER"]
        assert [role.value for role in ClusterNodeRole] == ["leader", "follower"]
        assert [status.value for status in ClusterNodeStatus] == [
            "healthy",
            "starting",
            "stopping",
            "unreachable",
            "maintenance",
            "unknown",
        ]
        assert ClusterNodeRole("follower") is ClusterNodeRole.FOLLOWER
        assert ClusterNodeStatus("maintenance") is ClusterNodeStatus.MAINTENANCE
        assert ClusterNodeRole.LEADER == "leader"

        status_by_role = {
            ClusterNodeRole.LEADER: ClusterNodeStatus.HEALTHY,
            ClusterNodeRole.FOLLOWER: ClusterNodeStatus.STARTING,
        }
        assert status_by_role[ClusterNodeRole.FOLLOWER] is ClusterNodeStatus.STARTING

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

    @pytest.mark.unit
    @pytest.mark.asyncio
    async def test_get_cluster_metadata_maps_every_role_and_status(self):
        """A multi-node answer with every role and status reaches Python intact.

        A server without clustering cannot report these, so the answer comes from
        a local stub that serves the JSON shape of the HTTP API.
        """
        with serve_cluster_metadata() as port:
            client = IggyClient(
                HttpConfig(
                    api_url=f"http://127.0.0.1:{port}",
                    jwt="stub-token",
                    retries=0,
                )
            )
            await client.connect()
            try:
                metadata = await client.get_cluster_metadata()
            finally:
                await client.disconnect()

        nodes = metadata.nodes
        assert metadata.name == "stub-cluster"
        assert [node.name for node in nodes] == [
            f"node-{index}" for index in range(1, len(REPORTED_NODES) + 1)
        ]
        assert [node.role for node in nodes] == [row[2] for row in REPORTED_NODES]
        assert [node.status for node in nodes] == [row[3] for row in REPORTED_NODES]
        assert [node.role.value for node in nodes] == [row[0] for row in REPORTED_NODES]
        assert [node.status.value for node in nodes] == [
            row[1] for row in REPORTED_NODES
        ]
        for index, node in enumerate(nodes, start=1):
            assert node.ip == f"10.0.0.{index}"
            assert node.endpoints.tcp == 8090 + index
            assert node.endpoints.quic == 8080 + index
            assert node.endpoints.http == 3000 + index
            assert node.endpoints.websocket == 8092 + index
        assert f"nodes_count={len(REPORTED_NODES)}" in repr(metadata)
