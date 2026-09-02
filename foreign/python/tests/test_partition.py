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

from apache_iggy import IggyClient


@pytest.mark.asyncio
async def test_create_and_delete_partitions(iggy_client: IggyClient, unique_name):
    stream_name = unique_name()
    topic_name = unique_name()

    await iggy_client.create_stream(stream_name)
    await iggy_client.create_topic(
        stream=stream_name, name=topic_name, partitions_count=2
    )

    await iggy_client.create_partitions(stream_name, topic_name, 2)
    topic = await iggy_client.get_topic(stream_name, topic_name)
    assert topic is not None
    assert topic.partitions_count == 4
    assert [partition.id for partition in topic.partitions] == [0, 1, 2, 3]

    await iggy_client.delete_partitions(stream_name, topic_name, 2)
    topic = await iggy_client.get_topic(stream_name, topic_name)
    assert topic is not None
    assert topic.partitions_count == 2
    assert [partition.id for partition in topic.partitions] == [0, 1]


@pytest.mark.asyncio
async def test_partition_management_accepts_numeric_ids(
    iggy_client: IggyClient, unique_name
):
    stream_name = unique_name()
    topic_name = unique_name()

    await iggy_client.create_stream(stream_name)
    stream = await iggy_client.get_stream(stream_name)
    assert stream is not None
    await iggy_client.create_topic(
        stream=stream.id, name=topic_name, partitions_count=2
    )
    topic = await iggy_client.get_topic(stream.id, topic_name)
    assert topic is not None

    await iggy_client.create_partitions(stream.id, topic.id, 1)
    await iggy_client.delete_partitions(stream.id, topic.id, 1)

    topic = await iggy_client.get_topic(stream.id, topic.id)
    assert topic is not None
    assert [partition.id for partition in topic.partitions] == [0, 1]


@pytest.mark.asyncio
async def test_partition_management_rejects_zero_count(
    iggy_client: IggyClient, unique_name
):
    stream_name = unique_name()
    topic_name = unique_name()

    await iggy_client.create_stream(stream_name)
    await iggy_client.create_topic(
        stream=stream_name, name=topic_name, partitions_count=2
    )

    with pytest.raises(RuntimeError):
        await iggy_client.create_partitions(stream_name, topic_name, 0)
    with pytest.raises(RuntimeError):
        await iggy_client.delete_partitions(stream_name, topic_name, 0)


@pytest.mark.asyncio
async def test_delete_partitions_rejects_count_larger_than_topic(
    iggy_client: IggyClient, unique_name
):
    stream_name = unique_name()
    topic_name = unique_name()

    await iggy_client.create_stream(stream_name)
    await iggy_client.create_topic(
        stream=stream_name, name=topic_name, partitions_count=2
    )

    with pytest.raises(RuntimeError):
        await iggy_client.delete_partitions(stream_name, topic_name, 3)


@pytest.mark.asyncio
async def test_partition_management_rejects_missing_stream_or_topic(
    iggy_client: IggyClient, unique_name
):
    stream_name = unique_name()
    topic_name = unique_name()
    missing_name = unique_name()

    await iggy_client.create_stream(stream_name)
    await iggy_client.create_topic(
        stream=stream_name, name=topic_name, partitions_count=2
    )

    with pytest.raises(RuntimeError):
        await iggy_client.create_partitions(missing_name, topic_name, 1)
    with pytest.raises(RuntimeError):
        await iggy_client.create_partitions(stream_name, missing_name, 1)
    with pytest.raises(RuntimeError):
        await iggy_client.delete_partitions(missing_name, topic_name, 1)
    with pytest.raises(RuntimeError):
        await iggy_client.delete_partitions(stream_name, missing_name, 1)
