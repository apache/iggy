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
    Consumer,
    Durability,
    IggyClient,
    Permissions,
    PollingStrategy,
    SendMessage,
    StreamPermissions,
    TopicPermissions,
)

from .utils import login_fresh_client, unique_credentials

PARTITION_ID = 0

# Smallest segment size a topic may declare. With one message per batch, the
# on-disk size of a 220_000-byte payload seals a segment after five messages
# and not after four, mirroring the layout in the server's segment deletion
# scenario.
SEGMENT_SIZE = 1024 * 1024
PAYLOAD_SIZE = 220_000
MESSAGES_PER_SEALED_SEGMENT = 5


async def _create_topic(iggy_client: IggyClient, unique_name):
    stream_name = unique_name()
    topic_name = unique_name()

    await iggy_client.create_stream(stream_name)
    # Flush every message so each send lands in a segment before the next one,
    # which makes segment boundaries deterministic.
    await iggy_client.create_topic(
        stream=stream_name,
        name=topic_name,
        partitions_count=1,
        segment_size=SEGMENT_SIZE,
        durability=Durability.PERSISTED,
        messages_required_to_save=1,
    )
    return stream_name, topic_name


async def _send_messages(iggy_client: IggyClient, stream_id, topic_id, count: int):
    payload = b"x" * PAYLOAD_SIZE
    for _ in range(count):
        await iggy_client.send_messages(
            stream=stream_id,
            topic=topic_id,
            partitioning=PARTITION_ID,
            messages=[SendMessage(payload)],
        )


async def _poll_offsets(iggy_client: IggyClient, stream_id, topic_id) -> list[int]:
    messages = await iggy_client.poll_messages(
        stream=stream_id,
        topic=topic_id,
        consumer=Consumer.Single(99),
        partition_id=PARTITION_ID,
        polling_strategy=PollingStrategy.First(),
        count=100,
        auto_commit=False,
    )
    return [message.offset() for message in messages]


async def _wait_for_offsets(iggy_client: IggyClient, stream_id, topic_id, expected):
    # Deletion is acknowledged once committed; segment files are removed
    # afterwards, so poll until the surviving offsets settle.
    offsets = await _poll_offsets(iggy_client, stream_id, topic_id)
    for _ in range(100):
        if offsets == expected:
            return
        await asyncio.sleep(0.1)
        offsets = await _poll_offsets(iggy_client, stream_id, topic_id)
    assert offsets == expected


class TestSegmentManagement:
    @pytest.mark.asyncio
    @pytest.mark.parametrize("numeric_ids", [False, True])
    async def test_delete_segments_removes_oldest_sealed_segment(
        self, iggy_client: IggyClient, unique_name, numeric_ids: bool
    ):
        stream_name, topic_name = await _create_topic(iggy_client, unique_name)
        stream = await iggy_client.get_stream(stream_name)
        assert stream is not None
        topic = await iggy_client.get_topic(stream.id, topic_name)
        assert topic is not None
        stream_id = stream.id if numeric_ids else stream_name
        topic_id = topic.id if numeric_ids else topic_name
        await _send_messages(
            iggy_client, stream_id, topic_id, 2 * MESSAGES_PER_SEALED_SEGMENT
        )
        await _wait_for_offsets(iggy_client, stream_id, topic_id, list(range(10)))

        result = await iggy_client.delete_segments(stream_id, topic_id, PARTITION_ID, 1)

        assert result is None
        await _wait_for_offsets(iggy_client, stream_id, topic_id, list(range(5, 10)))

    @pytest.mark.asyncio
    async def test_delete_segments_keeps_active_segment(
        self, iggy_client: IggyClient, unique_name
    ):
        stream_name, topic_name = await _create_topic(iggy_client, unique_name)
        await _send_messages(
            iggy_client, stream_name, topic_name, 2 * MESSAGES_PER_SEALED_SEGMENT + 2
        )
        await _wait_for_offsets(iggy_client, stream_name, topic_name, list(range(12)))

        await iggy_client.delete_segments(
            stream_name, topic_name, PARTITION_ID, 2**32 - 1
        )

        await _wait_for_offsets(iggy_client, stream_name, topic_name, [10, 11])

    @pytest.mark.asyncio
    async def test_delete_segments_stops_at_committed_consumer_offset(
        self, iggy_client: IggyClient, unique_name
    ):
        stream_name, topic_name = await _create_topic(iggy_client, unique_name)
        await _send_messages(
            iggy_client, stream_name, topic_name, 2 * MESSAGES_PER_SEALED_SEGMENT + 2
        )
        # Commit offset 5: the first sealed segment (offsets 0-4) is behind the
        # consumer and deletable, the second (offsets 5-9) is not.
        polled = await iggy_client.poll_messages(
            stream=stream_name,
            topic=topic_name,
            consumer=Consumer.Single(1),
            partition_id=PARTITION_ID,
            polling_strategy=PollingStrategy.First(),
            count=MESSAGES_PER_SEALED_SEGMENT + 1,
            auto_commit=True,
        )
        assert [message.offset() for message in polled] == list(range(6))

        await iggy_client.delete_segments(stream_name, topic_name, PARTITION_ID, 2)

        await _wait_for_offsets(
            iggy_client, stream_name, topic_name, list(range(5, 12))
        )

    @pytest.mark.asyncio
    async def test_delete_segments_zero_count_is_noop(
        self, iggy_client: IggyClient, unique_name
    ):
        stream_name, topic_name = await _create_topic(iggy_client, unique_name)
        await _send_messages(
            iggy_client, stream_name, topic_name, MESSAGES_PER_SEALED_SEGMENT + 1
        )
        await _wait_for_offsets(iggy_client, stream_name, topic_name, list(range(6)))

        await iggy_client.delete_segments(stream_name, topic_name, PARTITION_ID, 0)

        assert await _poll_offsets(iggy_client, stream_name, topic_name) == list(
            range(6)
        )

    @pytest.mark.asyncio
    @pytest.mark.parametrize("missing", ["stream", "topic", "partition"])
    async def test_delete_segments_rejects_missing_target(
        self, iggy_client: IggyClient, unique_name, missing: str
    ):
        stream_name, topic_name = await _create_topic(iggy_client, unique_name)
        missing_name = unique_name()
        stream_id = missing_name if missing == "stream" else stream_name
        topic_id = missing_name if missing == "topic" else topic_name
        partition_id = 999 if missing == "partition" else PARTITION_ID

        with pytest.raises(RuntimeError, match=r"was not found\."):
            await iggy_client.delete_segments(stream_id, topic_id, partition_id, 1)

    @pytest.mark.asyncio
    async def test_delete_segments_rejects_invalid_identifier(
        self, iggy_client: IggyClient, unique_name
    ):
        _, topic_name = await _create_topic(iggy_client, unique_name)

        with pytest.raises(ValueError):
            await iggy_client.delete_segments("", topic_name, PARTITION_ID, 1)

    @pytest.mark.asyncio
    @pytest.mark.parametrize("argument", ["partition_id", "segments_count"])
    @pytest.mark.parametrize("value", [-1, 2**32])
    async def test_delete_segments_rejects_out_of_range_python_integer(
        self, iggy_client: IggyClient, unique_name, argument: str, value: int
    ):
        stream_name, topic_name = await _create_topic(iggy_client, unique_name)
        arguments = {"partition_id": PARTITION_ID, "segments_count": 1, argument: value}

        with pytest.raises(OverflowError):
            await iggy_client.delete_segments(stream_name, topic_name, **arguments)

    @pytest.mark.asyncio
    async def test_delete_segments_requires_scoped_manage_topic(
        self, iggy_client: IggyClient, unique_name
    ):
        stream_name, topic_name = await _create_topic(iggy_client, unique_name)
        other_topic_name = unique_name()
        await iggy_client.create_topic(
            stream_name, other_topic_name, partitions_count=1
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
        with pytest.raises(RuntimeError, match="Unauthorized"):
            await denied.delete_segments(stream.id, topic.id, PARTITION_ID, 1)

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
        await allowed.delete_segments(stream.id, topic.id, PARTITION_ID, 1)
        with pytest.raises(RuntimeError, match="Unauthorized"):
            await allowed.delete_segments(stream.id, other_topic.id, PARTITION_ID, 1)

        await iggy_client.delete_user(denied_user.id)
        await iggy_client.delete_user(allowed_user.id)
