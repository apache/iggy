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

import asyncio

import pytest

from apache_iggy import (
    IggyClient,
    Permissions,
    SendMessage,
    StreamPermissions,
    TopicPermissions,
)

from .utils import login_fresh_client, unique_credentials


async def _create_topic(iggy_client: IggyClient, unique_name):
    stream_name = unique_name()
    topic_name = unique_name()

    await iggy_client.create_stream(stream_name)
    await iggy_client.create_topic(
        stream=stream_name, name=topic_name, partitions_count=2
    )
    return stream_name, topic_name


async def _wait_for_messages(iggy_client, stream_id, topic_id, expected):
    for _ in range(100):
        topic = await iggy_client.get_topic(stream_id, topic_id)
        if topic is not None and topic.messages_count == expected:
            return topic
        await asyncio.sleep(0.01)
    raise AssertionError(f"topic messages_count did not reach {expected}")


class TestPartitionManagement:
    @pytest.mark.asyncio
    @pytest.mark.parametrize("numeric_ids", [False, True])
    async def test_create_and_delete_partitions(
        self, iggy_client: IggyClient, unique_name, numeric_ids: bool
    ):
        stream_name, topic_name = await _create_topic(iggy_client, unique_name)
        stream = await iggy_client.get_stream(stream_name)
        assert stream is not None
        topic = await iggy_client.get_topic(stream.id, topic_name)
        assert topic is not None
        stream_id = stream.id if numeric_ids else stream_name
        topic_id = topic.id if numeric_ids else topic_name

        await iggy_client.create_partitions(stream_id, topic_id, 2)
        created = await iggy_client.get_topic(stream_id, topic_id)
        assert created is not None
        assert created.partitions_count == 4
        assert [partition.id for partition in created.partitions] == [0, 1, 2, 3]

        await iggy_client.delete_partitions(stream_id, topic_id, 2)
        deleted = await iggy_client.get_topic(stream_id, topic_id)
        assert deleted is not None
        assert deleted.partitions_count == 2
        assert [partition.id for partition in deleted.partitions] == [0, 1]

    @pytest.mark.asyncio
    async def test_delete_partitions_rolls_back_stats(
        self, iggy_client: IggyClient, unique_name
    ):
        stream_name, topic_name = await _create_topic(iggy_client, unique_name)
        await iggy_client.create_partitions(stream_name, topic_name, 2)
        await iggy_client.send_messages(
            stream_name, topic_name, 0, [SendMessage("retained")]
        )
        await iggy_client.send_messages(
            stream_name, topic_name, 3, [SendMessage("deleted")]
        )
        await _wait_for_messages(iggy_client, stream_name, topic_name, 2)

        await iggy_client.delete_partitions(stream_name, topic_name, 2)
        deleted = await _wait_for_messages(iggy_client, stream_name, topic_name, 1)
        assert [partition.id for partition in deleted.partitions] == [0, 1]
        assert deleted.partitions[0].messages_count == 1

    @pytest.mark.asyncio
    @pytest.mark.parametrize("method", ["create_partitions", "delete_partitions"])
    @pytest.mark.parametrize("partitions_count", [0, 1001])
    async def test_partition_management_rejects_invalid_count(
        self,
        iggy_client: IggyClient,
        unique_name,
        method: str,
        partitions_count: int,
    ):
        stream_name, topic_name = await _create_topic(iggy_client, unique_name)

        # Zero shares the legacy TooManyPartitions code with an over-limit count.
        with pytest.raises(RuntimeError, match="Too many partitions"):
            await getattr(iggy_client, method)(
                stream_name, topic_name, partitions_count
            )

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

    @pytest.mark.asyncio
    async def test_partition_management_requires_scoped_manage_topic(
        self, iggy_client: IggyClient, unique_name
    ):
        stream_name, topic_name = await _create_topic(iggy_client, unique_name)
        other_topic_name = unique_name()
        await iggy_client.create_topic(
            stream_name, other_topic_name, partitions_count=2
        )
        stream = await iggy_client.get_stream(stream_name)
        assert stream is not None
        topic = await iggy_client.get_topic(stream.id, topic_name)
        other_topic = await iggy_client.get_topic(stream.id, other_topic_name)
        assert topic is not None
        assert other_topic is not None

        denied_username, denied_password = unique_credentials(unique_name)
        denied_user = await iggy_client.create_user(denied_username, denied_password)
        denied = await login_fresh_client(denied_username, denied_password)
        for method in ("create_partitions", "delete_partitions"):
            with pytest.raises(RuntimeError, match="Unauthorized"):
                await getattr(denied, method)(stream.id, topic.id, 1)
            unchanged = await iggy_client.get_topic(stream.id, topic.id)
            assert unchanged is not None
            assert unchanged.partitions_count == 2

        allowed_username, allowed_password = unique_credentials(unique_name)
        allowed_user = await iggy_client.create_user(
            allowed_username,
            allowed_password,
            permissions=Permissions(
                streams={
                    stream.id: StreamPermissions(
                        topics={topic.id: TopicPermissions(manage_topic=True)}
                    )
                }
            ),
        )
        allowed = await login_fresh_client(allowed_username, allowed_password)
        await allowed.create_partitions(stream.id, topic.id, 1)
        await allowed.delete_partitions(stream.id, topic.id, 1)
        for method in ("create_partitions", "delete_partitions"):
            with pytest.raises(RuntimeError, match="Unauthorized"):
                await getattr(allowed, method)(stream.id, other_topic.id, 1)
        scoped = await iggy_client.get_topic(stream.id, topic.id)
        untouched = await iggy_client.get_topic(stream.id, other_topic.id)
        assert scoped is not None and scoped.partitions_count == 2
        assert untouched is not None and untouched.partitions_count == 2

        await iggy_client.delete_user(denied_user.id)
        await iggy_client.delete_user(allowed_user.id)
