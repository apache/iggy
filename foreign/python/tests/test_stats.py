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

import datetime

import pytest

from apache_iggy import CacheMetricsKey, GlobalPermissions, IggyClient, Permissions
from apache_iggy import SendMessage as Message

from .utils import (
    get_server_config,
    login_fresh_client,
    unique_credentials,
    wait_for_server,
)

# Fields that describe the server process itself and must not change between
# two calls within one server run.
PROCESS_IDENTITY_FIELDS = (
    "process_id",
    "start_time",
    "hostname",
    "os_name",
    "os_version",
    "kernel_version",
    "iggy_server_version",
    "iggy_server_semver",
)


class TestStats:
    """Test server statistics retrieval."""

    @pytest.mark.asyncio
    async def test_get_stats(self, iggy_client: IggyClient, unique_name):
        """Sending messages moves the server counts reported by get_stats."""
        stats_before = await iggy_client.get_stats()

        stream_name = unique_name()
        topic_name = unique_name()
        await iggy_client.create_stream(stream_name)
        await iggy_client.create_topic(
            stream=stream_name, name=topic_name, partitions_count=1
        )
        await iggy_client.send_messages(
            stream=stream_name,
            topic=topic_name,
            partitioning=0,
            messages=[Message(f"stats message {i}") for i in range(3)],
        )

        stats = await iggy_client.get_stats()

        # `>=` rather than exact equality: the counters are server-global, so
        # concurrently running tests (e.g. under pytest-xdist) may bump them too.
        assert stats.streams_count >= stats_before.streams_count + 1
        assert stats.topics_count >= stats_before.topics_count + 1
        assert stats.partitions_count >= stats_before.partitions_count + 1
        assert stats.messages_count >= stats_before.messages_count + 3
        assert stats.messages_size_bytes > stats_before.messages_size_bytes
        assert stats.clients_count >= 1

        assert stats.iggy_server_version
        assert stats.hostname
        assert stats.os_name
        assert stats.os_version
        assert stats.kernel_version
        assert stats.process_id > 0
        # sysinfo cannot enumerate a process's threads on macOS, so a server
        # running there reports 0.
        if stats.os_name != "Darwin":
            assert stats.threads_count > 0
        assert stats.start_time > 0
        assert stats.total_memory > 0
        assert stats.available_memory <= stats.total_memory
        assert stats.total_disk_space > 0
        assert stats.free_disk_space <= stats.total_disk_space

        assert isinstance(stats.run_time, datetime.timedelta)
        assert stats.run_time >= stats_before.run_time
        for field in PROCESS_IDENTITY_FIELDS:
            assert getattr(stats, field) == getattr(stats_before, field)

        assert f"streams_count={stats.streams_count}" in repr(stats)
        assert stats.hostname in repr(stats)

    @pytest.mark.asyncio
    async def test_get_stats_reflects_topology(
        self, iggy_client: IggyClient, unique_name
    ):
        """Streams, topics, partitions, consumer groups and a second client show
        up in the counters, and deleting the streams brings them back down."""
        stats_before = await iggy_client.get_stats()

        streams = [unique_name() for _ in range(2)]
        topics_per_stream = 2
        partitions_per_topic = 3
        topics = []
        for stream_name in streams:
            await iggy_client.create_stream(stream_name)
            for _ in range(topics_per_stream):
                topic_name = unique_name()
                await iggy_client.create_topic(
                    stream=stream_name,
                    name=topic_name,
                    partitions_count=partitions_per_topic,
                )
                await iggy_client.create_consumer_group(
                    stream_name, topic_name, unique_name()
                )
                topics.append((stream_name, topic_name))
        second_client = await login_fresh_client("iggy", "iggy")

        topics_created = len(topics)
        partitions_created = topics_created * partitions_per_topic

        stats = await iggy_client.get_stats()

        assert stats.streams_count >= stats_before.streams_count + len(streams)
        assert stats.topics_count >= stats_before.topics_count + topics_created
        assert (
            stats.partitions_count >= stats_before.partitions_count + partitions_created
        )
        # Every new partition opens with one segment.
        assert stats.segments_count >= stats_before.segments_count + partitions_created
        assert (
            stats.consumer_groups_count
            >= stats_before.consumer_groups_count + topics_created
        )
        assert stats.clients_count >= stats_before.clients_count + 1

        # The SDK has no delete_stream, so the streams stay behind and only the
        # topic-level counters are expected to drop.
        for stream_name, topic_name in topics:
            await iggy_client.delete_topic(stream_name, topic_name)
        del second_client

        stats_after = await iggy_client.get_stats()

        assert stats_after.topics_count <= stats.topics_count - topics_created
        assert (
            stats_after.partitions_count <= stats.partitions_count - partitions_created
        )
        assert stats_after.segments_count <= stats.segments_count - partitions_created
        assert (
            stats_after.consumer_groups_count
            <= stats.consumer_groups_count - topics_created
        )
        # The server reaps a dropped connection asynchronously, so the count
        # may not have decreased yet.
        assert stats_after.clients_count <= stats.clients_count

    @pytest.mark.asyncio
    async def test_get_stats_cache_metrics_dict(self, iggy_client: IggyClient):
        """cache_metrics is a dict the server currently leaves empty, and
        repeated accesses return the same dict rather than rebuilding it."""
        stats = await iggy_client.get_stats()

        assert stats.cache_metrics == {}
        # The getter must not re-collect the map on every access.
        assert stats.cache_metrics is stats.cache_metrics

    def test_cache_metrics_key_is_constructible_and_hashable(self):
        """A key built in Python can address a cache_metrics dict entry."""
        key = CacheMetricsKey(stream_id=1, topic_id=2, partition_id=3)

        assert key.stream_id == 1
        assert key.topic_id == 2
        assert key.partition_id == 3
        assert repr(key) == "CacheMetricsKey(stream_id=1, topic_id=2, partition_id=3)"

        equal_key = CacheMetricsKey(stream_id=1, topic_id=2, partition_id=3)
        other_key = CacheMetricsKey(stream_id=1, topic_id=2, partition_id=4)
        assert key == equal_key
        assert key != other_key
        assert hash(key) == hash(equal_key)

        # An equal key constructed independently hits the same dict slot.
        metrics_by_key = {key: "metrics"}
        assert metrics_by_key[equal_key] == "metrics"
        assert other_key not in metrics_by_key

    @pytest.mark.asyncio
    async def test_get_stats_requires_connection_and_auth(self):
        """get_stats fails before connecting, before login, and after logout."""
        host, port = get_server_config()
        wait_for_server(host, port)

        client = IggyClient(f"{host}:{port}")
        with pytest.raises(RuntimeError):
            await client.get_stats()

        await client.connect()
        with pytest.raises(RuntimeError):
            await client.get_stats()

        await client.login_user("iggy", "iggy")
        await client.get_stats()

        await client.logout_user()
        with pytest.raises(RuntimeError):
            await client.get_stats()

    @pytest.mark.asyncio
    @pytest.mark.parametrize("flag", ["read_servers", "manage_servers"])
    async def test_get_stats_requires_server_permission(
        self, iggy_client: IggyClient, unique_name, flag
    ):
        """A user without read_servers or manage_servers is denied; either grants."""
        username, password = unique_credentials(unique_name)
        created = await iggy_client.create_user(username, password)

        denied_client = await login_fresh_client(username, password)
        with pytest.raises(RuntimeError):
            await denied_client.get_stats()

        await iggy_client.update_permissions(
            created.id,
            Permissions(global_permissions=GlobalPermissions(**{flag: True})),
        )

        granted_client = await login_fresh_client(username, password)
        stats = await granted_client.get_stats()
        assert stats.process_id > 0

        await iggy_client.delete_user(created.id)
