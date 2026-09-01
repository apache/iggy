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

from apache_iggy import CacheMetrics, CacheMetricsKey, IggyClient
from apache_iggy import SendMessage as Message


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

        assert stats.streams_count == stats_before.streams_count + 1
        assert stats.topics_count == stats_before.topics_count + 1
        assert stats.partitions_count == stats_before.partitions_count + 1
        assert stats.messages_count >= stats_before.messages_count + 3
        assert stats.clients_count >= 1

        assert stats.iggy_server_version
        assert stats.hostname
        assert stats.process_id > 0
        assert stats.start_time > 0
        assert stats.total_memory > 0
        assert stats.total_disk_space > 0

    @pytest.mark.asyncio
    async def test_get_stats_cache_metrics_dict(self, iggy_client: IggyClient):
        """cache_metrics is a dict keyed by hashable CacheMetricsKey."""
        stats = await iggy_client.get_stats()

        assert isinstance(stats.cache_metrics, dict)
        for key, metrics in stats.cache_metrics.items():
            assert isinstance(key, CacheMetricsKey)
            assert isinstance(metrics, CacheMetrics)
            # The key round-trips through a dict lookup.
            assert stats.cache_metrics[key] is not None
            assert key.stream_id >= 0
            assert metrics.hits >= 0
            assert metrics.misses >= 0
