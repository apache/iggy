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

from apache_iggy import IggyClient, SendMessage


async def _create_topic(iggy_client: IggyClient, unique_name):
    stream_name = unique_name()
    topic_name = unique_name()

    await iggy_client.create_stream(stream_name)
    await iggy_client.create_topic(
        stream=stream_name, name=topic_name, partitions_count=2
    )
    return stream_name, topic_name


class TestPartitionManagement:
    @pytest.mark.asyncio
    async def test_create_and_delete_partitions(
        self, iggy_client: IggyClient, unique_name
    ):
        stream_name, topic_name = await _create_topic(iggy_client, unique_name)

        await iggy_client.create_partitions(stream_name, topic_name, 2)
        created = await iggy_client.get_topic(stream_name, topic_name)
        assert created is not None
        assert created.partitions_count == 4
        assert [partition.id for partition in created.partitions] == [0, 1, 2, 3]

        await iggy_client.send_messages(
            stream_name, topic_name, 3, [SendMessage("partition payload")]
        )
        await iggy_client.delete_partitions(stream_name, topic_name, 2)
        deleted = await iggy_client.get_topic(stream_name, topic_name)
        assert deleted is not None
        assert deleted.partitions_count == 2
        assert [partition.id for partition in deleted.partitions] == [0, 1]

    @pytest.mark.asyncio
    async def test_partition_management_accepts_numeric_ids(
        self, iggy_client: IggyClient, unique_name
    ):
        stream_name, topic_name = await _create_topic(iggy_client, unique_name)
        stream = await iggy_client.get_stream(stream_name)
        assert stream is not None
        topic = await iggy_client.get_topic(stream.id, topic_name)
        assert topic is not None

        await iggy_client.create_partitions(stream.id, topic.id, 1)
        created = await iggy_client.get_topic(stream.id, topic.id)
        assert created is not None
        assert created.partitions_count == 3
        assert [partition.id for partition in created.partitions] == [0, 1, 2]

        await iggy_client.delete_partitions(stream.id, topic.id, 1)
        deleted = await iggy_client.get_topic(stream.id, topic.id)
        assert deleted is not None
        assert deleted.partitions_count == 2
        assert [partition.id for partition in deleted.partitions] == [0, 1]

    @pytest.mark.asyncio
    async def test_partition_management_rejects_zero_count(
        self, iggy_client: IggyClient, unique_name
    ):
        stream_name, topic_name = await _create_topic(iggy_client, unique_name)

        with pytest.raises(RuntimeError, match="Too many partitions"):
            await iggy_client.create_partitions(stream_name, topic_name, 0)
        with pytest.raises(RuntimeError, match="Too many partitions"):
            await iggy_client.delete_partitions(stream_name, topic_name, 0)

    @pytest.mark.asyncio
    async def test_delete_partitions_rejects_count_larger_than_topic(
        self, iggy_client: IggyClient, unique_name
    ):
        stream_name, topic_name = await _create_topic(iggy_client, unique_name)

        with pytest.raises(RuntimeError, match="Invalid partitions count"):
            await iggy_client.delete_partitions(stream_name, topic_name, 3)

    @pytest.mark.asyncio
    @pytest.mark.parametrize("method", ["create_partitions", "delete_partitions"])
    @pytest.mark.parametrize("missing", ["stream", "topic"])
    async def test_partition_management_rejects_missing_stream_or_topic(
        self, iggy_client: IggyClient, unique_name, method: str, missing: str
    ):
        stream_name, topic_name = await _create_topic(iggy_client, unique_name)
        missing_name = unique_name()
        stream_id = missing_name if missing == "stream" else stream_name
        topic_id = missing_name if missing == "topic" else topic_name

        with pytest.raises(RuntimeError, match=r"was not found\."):
            await getattr(iggy_client, method)(stream_id, topic_id, 1)

    @pytest.mark.asyncio
    @pytest.mark.parametrize("method", ["create_partitions", "delete_partitions"])
    async def test_partition_management_rejects_invalid_identifier(
        self, iggy_client: IggyClient, unique_name, method: str
    ):
        _, topic_name = await _create_topic(iggy_client, unique_name)

        with pytest.raises(ValueError):
            await getattr(iggy_client, method)("", topic_name, 1)

    @pytest.mark.asyncio
    @pytest.mark.parametrize("method", ["create_partitions", "delete_partitions"])
    @pytest.mark.parametrize("partitions_count", [-1, 2**32])
    async def test_partition_management_rejects_out_of_range_python_integer(
        self,
        iggy_client: IggyClient,
        unique_name,
        method: str,
        partitions_count: int,
    ):
        stream_name, topic_name = await _create_topic(iggy_client, unique_name)

        with pytest.raises(OverflowError):
            await getattr(iggy_client, method)(
                stream_name, topic_name, partitions_count
            )

    @pytest.mark.asyncio
    async def test_delete_partitions_accepts_deleting_all_partitions(
        self, iggy_client: IggyClient, unique_name
    ):
        stream_name, topic_name = await _create_topic(iggy_client, unique_name)

        await iggy_client.delete_partitions(stream_name, topic_name, 2)
        topic = await iggy_client.get_topic(stream_name, topic_name)
        assert topic is not None
        assert topic.partitions_count == 0
        assert topic.partitions == []
