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

# TODO(slbotbm): make a helper function for create_stream + create_topic
# + consumer_group setup once test fixture is in place.

import asyncio
from collections.abc import AsyncIterator
from datetime import timedelta

import pytest

from apache_iggy import (
    AutoCommit,
    CommitAfter,
    CommitEvery,
    CommitPolicy,
    Consumer,
    IggyClient,
    IggyError,
    PollingStrategy,
    PollOptions,
    ReceiveMessage,
    TopicOptions,
)
from apache_iggy import SendMessage as Message

from .utils import (
    get_server_config,
    read_offsets_with_cancels,
    wait_for_consumer_group_assignment,
    wait_for_ping,
    wait_for_server,
)

# A consumer without a wait polls an idle topic in a busy loop.
POLL_WAIT = timedelta(seconds=1)
POLL_OPTIONS = PollOptions(count=10, max_wait=POLL_WAIT)
# Stores the delivered position every 5 seconds and on shutdown.
INTERVAL_COMMIT_OPTIONS = PollOptions(
    count=10,
    max_wait=POLL_WAIT,
    commit=CommitPolicy.Auto(
        AutoCommit(interval=timedelta(seconds=5), after=CommitAfter.Delivery)
    ),
)
# One message per batch and one batch of prefetch, so most iterations wait for a reply.
ONE_MESSAGE = 1
CANCELLED_ITERATIONS_MESSAGES = 100
RUN_TIMEOUT_SECONDS = 10
INVALID_COMMAND = 3
NO_EVENT_RUN_MESSAGES = 5


async def _message_offset(iterator: AsyncIterator[ReceiveMessage]) -> list[int]:
    return [(await anext(iterator)).offset()]


async def _create_consumer_group_with_numeric_ids(
    iggy_client: IggyClient,
    unique_name,
    group_name: str,
) -> tuple[int, int, int]:
    stream_name = unique_name()
    topic_name = unique_name()

    await iggy_client.create_stream(stream_name)
    stream = await iggy_client.get_stream(stream_name)
    assert stream is not None
    assert isinstance(stream.id, int)

    await iggy_client.create_topic(stream=stream.id, name=topic_name)
    topic = await iggy_client.get_topic(stream.id, topic_name)
    assert topic is not None
    assert isinstance(topic.id, int)

    await iggy_client.create_consumer_group(stream.id, topic.id, group_name)
    group = await iggy_client.get_consumer_group(stream.id, topic.id, group_name)
    assert group is not None
    assert isinstance(group.id, int)

    return stream.id, topic.id, group.id


class TestCreateConsumerGroup:
    """Test consumer group creation via create_consumer_group."""

    @pytest.mark.asyncio
    async def test_create_and_get_consumer_group(
        self, iggy_client: IggyClient, unique_name
    ):
        """Test consumer group creation and retrieval."""
        stream_name = unique_name()
        topic_name = unique_name()
        group_name = unique_name()

        await iggy_client.create_stream(stream_name)
        await iggy_client.create_topic(
            stream=stream_name,
            name=topic_name,
            options=TopicOptions(partitions_count=2),
        )
        await iggy_client.create_consumer_group(
            stream_name,
            topic_name,
            group_name,
        )

        group_by_name = await iggy_client.get_consumer_group(
            stream_name,
            topic_name,
            group_name,
        )
        assert group_by_name is not None
        assert group_by_name.name == group_name
        assert group_by_name.partitions_count == 2
        assert group_by_name.members_count == 0
        assert group_by_name.members == []

    @pytest.mark.asyncio
    async def test_create_consumer_group_accepts_numeric_stream_and_topic_ids(
        self, iggy_client: IggyClient, unique_name
    ):
        """Test create_consumer_group accepts numeric stream and topic ids."""
        stream_name = unique_name()
        topic_name = unique_name()
        group_name = unique_name()

        await iggy_client.create_stream(stream_name)
        stream = await iggy_client.get_stream(stream_name)
        assert stream is not None
        stream_id = stream.id
        assert isinstance(stream_id, int)

        await iggy_client.create_topic(stream=stream_id, name=topic_name)
        topic = await iggy_client.get_topic(stream_id, topic_name)
        assert topic is not None
        topic_id = topic.id
        assert isinstance(topic_id, int)

        await iggy_client.create_consumer_group(
            stream_id,
            topic_id,
            group_name,
        )

        group = await iggy_client.get_consumer_group(
            stream_id,
            topic_id,
            group_name,
        )
        assert group is not None
        assert group.name == group_name
        assert group.partitions_count == 1
        assert group.members_count == 0
        assert group.members == []

    @pytest.mark.asyncio
    async def test_duplicate_consumer_group_creation_fails(
        self, iggy_client: IggyClient, unique_name
    ):
        """Test create_consumer_group rejects duplicate group names."""
        stream_name = unique_name()
        topic_name = unique_name()
        group_name = unique_name()

        await iggy_client.create_stream(stream_name)
        await iggy_client.create_topic(stream=stream_name, name=topic_name)
        await iggy_client.create_consumer_group(
            stream_name,
            topic_name,
            group_name,
        )

        with pytest.raises(RuntimeError):
            await iggy_client.create_consumer_group(
                stream_name,
                topic_name,
                group_name,
            )

    @pytest.mark.asyncio
    async def test_create_consumer_group_requires_connection_and_auth(
        self, unique_name
    ):
        """Test create_consumer_group fails before connecting and before login."""
        host, port = get_server_config()
        wait_for_server(host, port)

        client = IggyClient(f"{host}:{port}")
        with pytest.raises(RuntimeError):
            await client.create_consumer_group(
                unique_name(),
                unique_name(),
                unique_name(),
            )

        await client.connect()
        with pytest.raises(RuntimeError):
            await client.create_consumer_group(
                unique_name(),
                unique_name(),
                unique_name(),
            )


class TestGetConsumerGroup:
    """Test consumer group retrieval via get_consumer_group."""

    @pytest.mark.asyncio
    async def test_get_consumer_group_by_name_and_id(
        self, iggy_client: IggyClient, unique_name
    ):
        """Test consumer group lookup works by both name and numeric id."""
        stream_name = unique_name()
        topic_name = unique_name()
        group_name = unique_name()

        await iggy_client.create_stream(stream_name)
        await iggy_client.create_topic(stream=stream_name, name=topic_name)
        await iggy_client.create_consumer_group(
            stream_name,
            topic_name,
            group_name,
        )

        group_by_name = await iggy_client.get_consumer_group(
            stream_name,
            topic_name,
            group_name,
        )
        assert group_by_name is not None
        assert group_by_name.name == group_name
        assert group_by_name.partitions_count == 1
        assert group_by_name.members_count == 0
        assert group_by_name.members == []

        group_by_id = await iggy_client.get_consumer_group(
            stream_name,
            topic_name,
            group_by_name.id,
        )
        assert group_by_id is not None
        assert group_by_id.id == group_by_name.id
        assert group_by_id.name == group_name
        assert group_by_id.partitions_count == 1
        assert group_by_id.members_count == 0
        assert group_by_id.members == []

    @pytest.mark.asyncio
    async def test_get_nonexistent_consumer_group(
        self, iggy_client: IggyClient, unique_name
    ):
        """Test getting a non-existent consumer group by name or numeric id."""
        stream_name = unique_name()
        topic_name = unique_name()

        await iggy_client.create_stream(stream_name)
        await iggy_client.create_topic(stream=stream_name, name=topic_name)

        group_by_name = await iggy_client.get_consumer_group(
            stream_name,
            topic_name,
            unique_name(),
        )
        assert group_by_name is None

        group_by_id = await iggy_client.get_consumer_group(
            stream_name,
            topic_name,
            999999,
        )
        assert group_by_id is None

    @pytest.mark.asyncio
    async def test_get_consumer_group_returns_members(
        self, iggy_client: IggyClient, unique_name
    ):
        """Test get_consumer_group exposes non-empty member details."""
        consumer_name = unique_name()
        stream_name = unique_name()
        topic_name = unique_name()
        partition_id = 0

        await iggy_client.create_stream(stream_name)
        await iggy_client.create_topic(stream=stream_name, name=topic_name)

        await iggy_client.consumer_group(
            consumer_name,
            stream_name,
            topic_name,
            polling_strategy=PollingStrategy.Next(),
            poll_options=POLL_OPTIONS,
        )
        await wait_for_consumer_group_assignment(
            iggy_client, stream_name, topic_name, consumer_name, members_count=1
        )

        group = await iggy_client.get_consumer_group(
            stream_name,
            topic_name,
            consumer_name,
        )
        assert group is not None
        assert group.name == consumer_name
        assert group.partitions_count == 1
        assert group.members_count == 1

        members = group.members
        assert len(members) == 1
        assert members[0].partitions_count == 1
        assert members[0].partitions == [partition_id]

    @pytest.mark.asyncio
    async def test_get_consumer_group_requires_connection_and_auth(self, unique_name):
        """Test get_consumer_group fails before connecting and before login."""
        host, port = get_server_config()
        wait_for_server(host, port)

        client = IggyClient(f"{host}:{port}")
        with pytest.raises(RuntimeError):
            await client.get_consumer_group(
                unique_name(),
                unique_name(),
                unique_name(),
            )

        await client.connect()
        with pytest.raises(RuntimeError):
            await client.get_consumer_group(
                unique_name(),
                unique_name(),
                unique_name(),
            )


class TestGetConsumerGroups:
    """Test listing consumer groups via get_consumer_groups."""

    @pytest.mark.asyncio
    async def test_get_consumer_groups_in_empty_topic_returns_empty_list(
        self, iggy_client: IggyClient, unique_name
    ):
        """Test get_consumer_groups returns an empty list for a topic with no groups."""
        stream_name = unique_name()
        topic_name = unique_name()

        await iggy_client.create_stream(stream_name)
        await iggy_client.create_topic(stream=stream_name, name=topic_name)

        groups = await iggy_client.get_consumer_groups(stream_name, topic_name)
        assert groups == []

    @pytest.mark.asyncio
    async def test_get_consumer_groups_lists_topic_groups(
        self, iggy_client: IggyClient, unique_name
    ):
        """Test listing consumer groups returns groups for the requested topic."""
        stream_name = unique_name()
        topic_name = unique_name()
        first_group_name = unique_name()
        second_group_name = unique_name()

        await iggy_client.create_stream(stream_name)
        await iggy_client.create_topic(stream=stream_name, name=topic_name)
        await iggy_client.create_consumer_group(
            stream_name,
            topic_name,
            first_group_name,
        )
        await iggy_client.create_consumer_group(
            stream_name,
            topic_name,
            second_group_name,
        )

        groups = await iggy_client.get_consumer_groups(stream_name, topic_name)

        assert len(groups) == 2
        assert [group.name for group in groups] == [first_group_name, second_group_name]
        assert all(group.partitions_count == 1 for group in groups)
        assert all(group.members_count == 0 for group in groups)

    @pytest.mark.asyncio
    async def test_get_consumer_groups_accepts_numeric_stream_and_topic_ids(
        self, iggy_client: IggyClient, unique_name
    ):
        """Test get_consumer_groups accepts numeric stream and topic ids."""
        stream_name = unique_name()
        topic_name = unique_name()
        group_name = unique_name()

        await iggy_client.create_stream(stream_name)
        stream = await iggy_client.get_stream(stream_name)
        assert stream is not None
        stream_id = stream.id
        assert isinstance(stream_id, int)

        await iggy_client.create_topic(stream=stream_id, name=topic_name)
        topic = await iggy_client.get_topic(stream_id, topic_name)
        assert topic is not None
        topic_id = topic.id
        assert isinstance(topic_id, int)
        await iggy_client.create_consumer_group(stream_id, topic_id, group_name)

        groups = await iggy_client.get_consumer_groups(stream_id, topic_id)

        assert len(groups) == 1
        assert groups[0].name == group_name
        assert groups[0].partitions_count == 1
        assert groups[0].members_count == 0

    @pytest.mark.asyncio
    async def test_get_consumer_groups_returns_same_result_when_called_repeatedly(
        self, iggy_client: IggyClient, unique_name
    ):
        """Test repeated get_consumer_groups calls return a stable topic view."""
        stream_name = unique_name()
        topic_name = unique_name()
        group_names = [unique_name() for _ in range(2)]

        await iggy_client.create_stream(stream_name)
        await iggy_client.create_topic(stream=stream_name, name=topic_name)
        for group_name in group_names:
            await iggy_client.create_consumer_group(
                stream_name,
                topic_name,
                group_name,
            )

        first = await iggy_client.get_consumer_groups(stream_name, topic_name)
        second = await iggy_client.get_consumer_groups(stream_name, topic_name)

        assert [group.name for group in first] == group_names
        assert [group.id for group in first] == [group.id for group in second]
        assert [group.name for group in first] == [group.name for group in second]

    @pytest.mark.asyncio
    async def test_get_consumer_groups_requires_connection_and_auth(self, unique_name):
        """Test get_consumer_groups fails before connecting and before login."""
        host, port = get_server_config()
        wait_for_server(host, port)

        client = IggyClient(f"{host}:{port}")
        with pytest.raises(RuntimeError):
            await client.get_consumer_groups(unique_name(), unique_name())

        await client.connect()
        with pytest.raises(RuntimeError):
            await client.get_consumer_groups(unique_name(), unique_name())


class TestDeleteConsumerGroup:
    """Test deleting consumer groups via delete_consumer_group."""

    @pytest.mark.asyncio
    async def test_delete_consumer_group_removes_only_target_group(
        self, iggy_client: IggyClient, unique_name
    ):
        """Test delete_consumer_group removes the target group only."""
        stream_name = unique_name()
        topic_name = unique_name()
        group_to_delete = unique_name()
        group_to_keep = unique_name()

        await iggy_client.create_stream(stream_name)
        await iggy_client.create_topic(stream=stream_name, name=topic_name)
        await iggy_client.create_consumer_group(
            stream_name,
            topic_name,
            group_to_delete,
        )
        await iggy_client.create_consumer_group(
            stream_name,
            topic_name,
            group_to_keep,
        )

        await iggy_client.delete_consumer_group(
            stream_name,
            topic_name,
            group_to_delete,
        )

        assert (
            await iggy_client.get_consumer_group(
                stream_name,
                topic_name,
                group_to_delete,
            )
            is None
        )

        remaining = await iggy_client.get_consumer_groups(stream_name, topic_name)
        assert len(remaining) == 1
        assert remaining[0].name == group_to_keep
        assert remaining[0].partitions_count == 1
        assert remaining[0].members_count == 0

    @pytest.mark.asyncio
    async def test_delete_consumer_group_accepts_numeric_ids(
        self, iggy_client: IggyClient, unique_name
    ):
        """Test delete_consumer_group accepts numeric stream, topic, and group ids."""
        group_name = unique_name()

        stream_id, topic_id, group_id = await _create_consumer_group_with_numeric_ids(
            iggy_client,
            unique_name,
            group_name,
        )

        await iggy_client.delete_consumer_group(stream_id, topic_id, group_id)

        assert (
            await iggy_client.get_consumer_group(stream_id, topic_id, group_id) is None
        )

    @pytest.mark.asyncio
    async def test_delete_nonexistent_consumer_group_fails(
        self, iggy_client: IggyClient, unique_name
    ):
        """Test delete_consumer_group raises for a non-existent group."""
        stream_name = unique_name()
        topic_name = unique_name()

        await iggy_client.create_stream(stream_name)
        await iggy_client.create_topic(stream=stream_name, name=topic_name)

        with pytest.raises(RuntimeError, match="Consumer group with ID:"):
            await iggy_client.delete_consumer_group(
                stream_name,
                topic_name,
                unique_name(),
            )

    @pytest.mark.asyncio
    async def test_delete_consumer_group_removes_live_member(
        self, iggy_client: IggyClient, unique_name
    ):
        """Test deleting a consumer group also removes its live member."""
        group_name = unique_name()

        stream_id, topic_id, group_id = await _create_consumer_group_with_numeric_ids(
            iggy_client,
            unique_name,
            group_name,
        )
        await iggy_client.join_consumer_group(stream_id, topic_id, group_id)

        group = await iggy_client.get_consumer_group(
            stream_id,
            topic_id,
            group_id,
        )
        assert group is not None
        assert group.members_count == 1
        assert len(group.members) == 1

        await iggy_client.delete_consumer_group(stream_id, topic_id, group_id)

        assert (
            await iggy_client.get_consumer_group(
                stream_id,
                topic_id,
                group_id,
            )
            is None
        )
        assert await iggy_client.get_consumer_groups(stream_id, topic_id) == []


class TestJoinConsumerGroup:
    """Test joining consumer groups via join_consumer_group."""

    @pytest.mark.asyncio
    async def test_join_consumer_group_adds_member_once_when_repeated(
        self, iggy_client: IggyClient, unique_name
    ):
        """Test join_consumer_group adds one stable member for repeated joins."""
        stream_name = unique_name()
        topic_name = unique_name()
        group_name = unique_name()

        await iggy_client.create_stream(stream_name)
        await iggy_client.create_topic(stream=stream_name, name=topic_name)
        await iggy_client.create_consumer_group(stream_name, topic_name, group_name)

        await iggy_client.join_consumer_group(stream_name, topic_name, group_name)
        await iggy_client.join_consumer_group(stream_name, topic_name, group_name)
        await wait_for_consumer_group_assignment(
            iggy_client, stream_name, topic_name, group_name, members_count=1
        )

        group = await iggy_client.get_consumer_group(
            stream_name,
            topic_name,
            group_name,
        )
        assert group is not None
        assert group.name == group_name
        assert group.partitions_count == 1
        assert group.members_count == 1
        assert len(group.members) == 1
        assert group.members[0].partitions_count == 1
        assert group.members[0].partitions == [0]

    @pytest.mark.asyncio
    async def test_join_consumer_group_accepts_numeric_ids(
        self, iggy_client: IggyClient, unique_name
    ):
        """Test join_consumer_group accepts numeric stream, topic, and group ids."""
        group_name = unique_name()

        stream_id, topic_id, group_id = await _create_consumer_group_with_numeric_ids(
            iggy_client,
            unique_name,
            group_name,
        )

        await iggy_client.join_consumer_group(stream_id, topic_id, group_id)
        await wait_for_consumer_group_assignment(
            iggy_client, stream_id, topic_id, group_id, members_count=1
        )

        joined_group = await iggy_client.get_consumer_group(
            stream_id,
            topic_id,
            group_id,
        )
        assert joined_group is not None
        assert joined_group.id == group_id
        assert joined_group.name == group_name
        assert joined_group.partitions_count == 1
        assert joined_group.members_count == 1
        assert len(joined_group.members) == 1
        assert joined_group.members[0].partitions_count == 1
        assert joined_group.members[0].partitions == [0]

    @pytest.mark.asyncio
    async def test_join_nonexistent_consumer_group_fails(
        self, iggy_client: IggyClient, unique_name
    ):
        """Test join_consumer_group raises for a non-existent group."""
        stream_name = unique_name()
        topic_name = unique_name()

        await iggy_client.create_stream(stream_name)
        await iggy_client.create_topic(stream=stream_name, name=topic_name)

        with pytest.raises(RuntimeError, match="Consumer group with ID:"):
            await iggy_client.join_consumer_group(
                stream_name,
                topic_name,
                unique_name(),
            )


class TestLeaveConsumerGroup:
    """Test leaving consumer groups via leave_consumer_group."""

    @pytest.mark.asyncio
    async def test_leave_consumer_group_removes_member(
        self, iggy_client: IggyClient, unique_name
    ):
        """Test leave_consumer_group removes the current client from the group."""
        stream_name = unique_name()
        topic_name = unique_name()
        group_name = unique_name()

        await iggy_client.create_stream(stream_name)
        await iggy_client.create_topic(stream=stream_name, name=topic_name)
        await iggy_client.create_consumer_group(stream_name, topic_name, group_name)
        await iggy_client.join_consumer_group(stream_name, topic_name, group_name)

        await iggy_client.leave_consumer_group(stream_name, topic_name, group_name)

        group = await iggy_client.get_consumer_group(
            stream_name,
            topic_name,
            group_name,
        )
        assert group is not None
        assert group.name == group_name
        assert group.partitions_count == 1
        assert group.members_count == 0
        assert group.members == []

    @pytest.mark.asyncio
    async def test_leave_consumer_group_accepts_numeric_ids(
        self, iggy_client: IggyClient, unique_name
    ):
        """Test leave_consumer_group accepts numeric stream, topic, and group ids."""
        group_name = unique_name()

        stream_id, topic_id, group_id = await _create_consumer_group_with_numeric_ids(
            iggy_client,
            unique_name,
            group_name,
        )
        await iggy_client.join_consumer_group(stream_id, topic_id, group_id)

        await iggy_client.leave_consumer_group(stream_id, topic_id, group_id)

        left_group = await iggy_client.get_consumer_group(
            stream_id,
            topic_id,
            group_id,
        )
        assert left_group is not None
        assert left_group.id == group_id
        assert left_group.name == group_name
        assert left_group.partitions_count == 1
        assert left_group.members_count == 0
        assert left_group.members == []

    @pytest.mark.asyncio
    async def test_leave_consumer_group_twice_fails_second_time(
        self, iggy_client: IggyClient, unique_name
    ):
        """Test leaving a group twice raises on the second call."""
        stream_name = unique_name()
        topic_name = unique_name()
        group_name = unique_name()

        await iggy_client.create_stream(stream_name)
        await iggy_client.create_topic(stream=stream_name, name=topic_name)
        await iggy_client.create_consumer_group(stream_name, topic_name, group_name)
        await iggy_client.join_consumer_group(stream_name, topic_name, group_name)

        await iggy_client.leave_consumer_group(stream_name, topic_name, group_name)
        with pytest.raises(
            RuntimeError,
            match="Consumer group member with client ID:",
        ):
            await iggy_client.leave_consumer_group(stream_name, topic_name, group_name)

    @pytest.mark.asyncio
    async def test_leave_nonexistent_consumer_group_fails(
        self, iggy_client: IggyClient, unique_name
    ):
        """Test leave_consumer_group raises for a non-existent group."""
        stream_name = unique_name()
        topic_name = unique_name()

        await iggy_client.create_stream(stream_name)
        await iggy_client.create_topic(stream=stream_name, name=topic_name)

        with pytest.raises(RuntimeError, match="Consumer group with ID:"):
            await iggy_client.leave_consumer_group(
                stream_name,
                topic_name,
                unique_name(),
            )


@pytest.mark.parametrize(
    "method_name",
    [
        "delete_consumer_group",
        "join_consumer_group",
        "leave_consumer_group",
    ],
)
@pytest.mark.asyncio
async def test_consumer_group_lifecycle_requires_connection_and_auth(
    method_name, unique_name
):
    """Test lifecycle methods fail before connecting and before login."""
    host, port = get_server_config()
    wait_for_server(host, port)

    client = IggyClient(f"{host}:{port}")
    method = getattr(client, method_name)
    identifiers = (unique_name(), unique_name(), unique_name())

    with pytest.raises(RuntimeError, match="Disconnected"):
        await method(*identifiers)

    await client.connect()
    with pytest.raises(RuntimeError, match="Unauthenticated"):
        await method(*identifiers)


class TestConsumerGroup:
    """Test consumer group consumers."""

    @pytest.mark.asyncio
    async def test_consumer_group_metadata(self, iggy_client: IggyClient, unique_name):
        """Test that metadata information can be read about the consumer group."""
        consumer_name = unique_name()
        stream_name = unique_name()
        topic_name = unique_name()
        partition_id = 0

        await iggy_client.create_stream(stream_name)
        await iggy_client.create_topic(stream=stream_name, name=topic_name)

        consumer = await iggy_client.consumer_group(
            consumer_name,
            stream_name,
            topic_name,
            polling_strategy=PollingStrategy.Next(),
            poll_options=INTERVAL_COMMIT_OPTIONS,
        )

        assert consumer.name() == consumer_name
        assert consumer.stream() == stream_name
        assert consumer.topic() == topic_name
        assert consumer.partition_id() == partition_id
        assert consumer.get_last_consumed_offset(partition_id) is None
        assert consumer.get_last_stored_offset(partition_id) is None

    @pytest.mark.asyncio
    async def test_consumer_group_metadata_while_consuming(
        self, iggy_client: IggyClient, unique_name
    ):
        """Test that metadata can be read while a consumption run is in progress."""
        consumer_name = unique_name()
        stream_name = unique_name()
        topic_name = unique_name()
        partition_id = 0
        message = f"Metadata test - {unique_name()}"
        received_messages = []
        consuming = asyncio.Event()
        shutdown_event = asyncio.Event()

        await iggy_client.create_stream(stream_name)
        await iggy_client.create_topic(stream=stream_name, name=topic_name)

        consumer = await iggy_client.consumer_group(
            consumer_name,
            stream_name,
            topic_name,
            polling_strategy=PollingStrategy.First(),
            poll_options=POLL_OPTIONS,
        )

        async def take(received: ReceiveMessage) -> None:
            received_messages.append(received)
            consuming.set()

        await iggy_client.send_messages(
            stream_name,
            topic_name,
            partition_id,
            [Message(message)],
        )

        consume = consumer.consume_messages(take, shutdown_event)
        try:
            await asyncio.wait_for(consuming.wait(), timeout=10)

            current_partition_id = consumer.partition_id()
            assert consumer.name() == consumer_name
            assert consumer.stream() == stream_name
            assert consumer.topic() == topic_name
            assert (
                consumer.get_last_consumed_offset(current_partition_id)
                == received_messages[-1].offset()
            )
            assert consumer.get_last_stored_offset(current_partition_id) is None
        finally:
            shutdown_event.set()
            await consume

    @pytest.mark.asyncio
    async def test_consumer_group_manual_commit_while_consuming(
        self, iggy_client: IggyClient, unique_name
    ):
        """Test that offsets can be committed from inside a consumption callback."""
        consumer_name = unique_name()
        stream_name = unique_name()
        topic_name = unique_name()
        partition_id = 0
        messages = [f"Manual commit {i} - {unique_name()}" for i in range(2)]
        received_messages = []
        stored_offsets = []
        committed = asyncio.Event()
        shutdown_event = asyncio.Event()

        await iggy_client.create_stream(stream_name)
        await iggy_client.create_topic(stream=stream_name, name=topic_name)

        consumer = await iggy_client.consumer_group(
            consumer_name,
            stream_name,
            topic_name,
            polling_strategy=PollingStrategy.First(),
            poll_options=POLL_OPTIONS,
        )

        async def take(received: ReceiveMessage) -> None:
            received_messages.append(received)
            await consumer.store_offset(received.offset(), None)
            stored_offsets.append(consumer.get_last_stored_offset(partition_id))
            if len(received_messages) == len(messages):
                await consumer.delete_offset(partition_id)
                committed.set()

        await iggy_client.send_messages(
            stream_name,
            topic_name,
            partition_id,
            [Message(message) for message in messages],
        )

        consume = consumer.consume_messages(take, shutdown_event)
        try:
            await asyncio.wait_for(committed.wait(), timeout=10)
            assert stored_offsets == [
                received.offset() for received in received_messages
            ]
        finally:
            shutdown_event.set()
            await consume

    @pytest.mark.asyncio
    async def test_get_last_consumed_offset_updates_as_messages_are_consumed(
        self, iggy_client: IggyClient, unique_name
    ):
        """Test last consumed offset before, during, and after callback consumption."""
        consumer_name = unique_name()
        stream_name = unique_name()
        topic_name = unique_name()
        partition_id = 0
        first_batch_messages = [f"Offset test {i} - {unique_name()}" for i in range(2)]
        final_message = f"Offset test 2 - {unique_name()}"
        received_messages = []
        first_shutdown = asyncio.Event()
        second_shutdown = asyncio.Event()

        await iggy_client.create_stream(stream_name)
        await iggy_client.create_topic(stream=stream_name, name=topic_name)

        consumer = await iggy_client.consumer_group(
            consumer_name,
            stream_name,
            topic_name,
            polling_strategy=PollingStrategy.First(),
            poll_options=POLL_OPTIONS,
        )

        assert consumer.get_last_consumed_offset(partition_id) is None

        await iggy_client.send_messages(
            stream_name,
            topic_name,
            partition_id,
            [Message(message) for message in first_batch_messages],
        )

        async def take_first_batch(message: ReceiveMessage) -> None:
            received_messages.append(message)
            if len(received_messages) == len(first_batch_messages):
                first_shutdown.set()

        await consumer.consume_messages(take_first_batch, first_shutdown)

        assert (
            consumer.get_last_consumed_offset(partition_id)
            == received_messages[-1].offset()
        )
        assert len(received_messages) == len(first_batch_messages)

        await iggy_client.send_messages(
            stream_name,
            topic_name,
            partition_id,
            [Message(final_message)],
        )

        async def take_remaining(message: ReceiveMessage) -> None:
            received_messages.append(message)
            if len(received_messages) == len(first_batch_messages) + 1:
                second_shutdown.set()

        await consumer.consume_messages(take_remaining, second_shutdown)

        assert (
            consumer.get_last_consumed_offset(partition_id)
            == received_messages[-1].offset()
        )

    @pytest.mark.asyncio
    async def test_get_last_stored_offset_updates_when_offsets_are_stored(
        self, iggy_client: IggyClient, unique_name
    ):
        """Test last stored offset before, during, and after manual stores."""
        consumer_name = unique_name()
        stream_name = unique_name()
        topic_name = unique_name()
        partition_id = 0
        first_batch_messages = [
            f"Stored offset test {i} - {unique_name()}" for i in range(2)
        ]
        final_message = f"Stored offset test 2 - {unique_name()}"
        received_messages = []
        first_shutdown = asyncio.Event()
        second_shutdown = asyncio.Event()

        await iggy_client.create_stream(stream_name)
        await iggy_client.create_topic(stream=stream_name, name=topic_name)

        consumer = await iggy_client.consumer_group(
            consumer_name,
            stream_name,
            topic_name,
            polling_strategy=PollingStrategy.First(),
            poll_options=POLL_OPTIONS,
        )

        assert consumer.get_last_stored_offset(partition_id) is None

        await iggy_client.send_messages(
            stream_name,
            topic_name,
            partition_id,
            [Message(message) for message in first_batch_messages],
        )

        async def take_first_batch(message: ReceiveMessage) -> None:
            received_messages.append(message)
            if len(received_messages) == len(first_batch_messages):
                first_shutdown.set()

        await consumer.consume_messages(take_first_batch, first_shutdown)

        await consumer.store_offset(received_messages[-1].offset(), partition_id)
        assert (
            consumer.get_last_stored_offset(partition_id)
            == received_messages[-1].offset()
        )
        assert len(received_messages) == len(first_batch_messages)

        await iggy_client.send_messages(
            stream_name,
            topic_name,
            partition_id,
            [Message(final_message)],
        )

        async def take_remaining(message: ReceiveMessage) -> None:
            received_messages.append(message)
            if len(received_messages) == len(first_batch_messages) + 1:
                second_shutdown.set()

        await consumer.consume_messages(take_remaining, second_shutdown)

        await consumer.store_offset(received_messages[-1].offset(), partition_id)
        assert (
            consumer.get_last_stored_offset(partition_id)
            == received_messages[-1].offset()
        )

    @pytest.mark.asyncio
    async def test_delete_offset_behavior(self, iggy_client: IggyClient, unique_name):
        """Test deleting a missing offset fails and a stored offset succeeds."""
        consumer_name = unique_name()
        stream_name = unique_name()
        topic_name = unique_name()
        partition_id = 0
        shutdown_event = asyncio.Event()
        received_messages = []

        await iggy_client.create_stream(stream_name)
        await iggy_client.create_topic(stream=stream_name, name=topic_name)

        consumer = await iggy_client.consumer_group(
            consumer_name,
            stream_name,
            topic_name,
            polling_strategy=PollingStrategy.First(),
            poll_options=POLL_OPTIONS,
        )
        await wait_for_consumer_group_assignment(
            iggy_client, stream_name, topic_name, consumer_name, members_count=1
        )

        with pytest.raises(
            RuntimeError,
            match=r"Consumer offset for consumer with ID: 0 was not found\.",
        ):
            await consumer.delete_offset(partition_id)
        assert consumer.get_last_stored_offset(partition_id) is None

        await iggy_client.send_messages(
            stream_name,
            topic_name,
            partition_id,
            [Message(f"Delete offset test - {unique_name()}")],
        )

        async def take(message: ReceiveMessage) -> None:
            received_messages.append(message)
            shutdown_event.set()

        await consumer.consume_messages(take, shutdown_event)

        await consumer.store_offset(received_messages[-1].offset(), partition_id)
        assert (
            consumer.get_last_stored_offset(partition_id)
            == received_messages[-1].offset()
        )

        await consumer.delete_offset(partition_id)

    @pytest.mark.asyncio
    async def test_consume_messages(self, iggy_client: IggyClient, unique_name):
        """Test that the consumer group can consume messages."""
        consumer_name = unique_name()
        stream_name = unique_name()
        topic_name = unique_name()
        partition_id = 0
        test_messages = [f"Consumer group test {i} - {unique_name()}" for i in range(5)]
        received_messages = []
        shutdown_event = asyncio.Event()

        await iggy_client.create_stream(stream_name)
        await iggy_client.create_topic(stream=stream_name, name=topic_name)

        consumer = await iggy_client.consumer_group(
            consumer_name,
            stream_name,
            topic_name,
            polling_strategy=PollingStrategy.Next(),
            poll_options=INTERVAL_COMMIT_OPTIONS,
        )

        async def take(message: ReceiveMessage) -> None:
            received_messages.append(message.payload().decode())
            if len(received_messages) == len(test_messages):
                shutdown_event.set()

        async def send() -> None:
            await iggy_client.send_messages(
                stream_name,
                topic_name,
                partition_id,
                [Message(message) for message in test_messages],
            )

        await asyncio.gather(consumer.consume_messages(take, shutdown_event), send())

        assert received_messages == test_messages

    @pytest.mark.asyncio
    async def test_consume_messages_can_read_user_headers(
        self, iggy_client: IggyClient, unique_name
    ):
        """Test consume_messages callback receives user headers."""
        consumer_name = unique_name()
        stream_name = unique_name()
        topic_name = unique_name()
        partition_id = 0
        expected_headers: dict[str, str | bytes | bool | int | float] = {
            "source": "callback",
            "attempt": 1,
        }
        received_headers = []
        shutdown_event = asyncio.Event()

        await iggy_client.create_stream(stream_name)
        await iggy_client.create_topic(stream=stream_name, name=topic_name)

        consumer = await iggy_client.consumer_group(
            consumer_name,
            stream_name,
            topic_name,
            polling_strategy=PollingStrategy.Next(),
            poll_options=POLL_OPTIONS,
        )

        async def take(message: ReceiveMessage) -> None:
            headers = message.user_headers()
            assert headers is not None
            received_headers.append(headers.to_scalar_dict())
            shutdown_event.set()

        async def send() -> None:
            await iggy_client.send_messages(
                stream_name,
                topic_name,
                partition_id,
                [Message("callback headers", user_headers=expected_headers)],
            )

        await asyncio.gather(consumer.consume_messages(take, shutdown_event), send())

        assert received_headers == [expected_headers]

    @pytest.mark.asyncio
    async def test_iter_messages(self, iggy_client: IggyClient, unique_name):
        """Test that the consumer group can iterate over messages."""
        consumer_name = unique_name()
        stream_name = unique_name()
        topic_name = unique_name()
        partition_id = 0
        test_messages = [f"Consumer group test {i} - {unique_name()}" for i in range(5)]
        received_messages = []

        await iggy_client.create_stream(stream_name)
        await iggy_client.create_topic(stream=stream_name, name=topic_name)

        consumer = await iggy_client.consumer_group(
            consumer_name,
            stream_name,
            topic_name,
            polling_strategy=PollingStrategy.Next(),
            poll_options=INTERVAL_COMMIT_OPTIONS,
        )

        await iggy_client.send_messages(
            stream_name,
            topic_name,
            partition_id,
            [Message(message) for message in test_messages],
        )

        iterator = consumer.iter_messages()
        for _ in range(len(test_messages)):
            message = await asyncio.wait_for(iterator.__anext__(), timeout=5)
            received_messages.append(message.payload().decode())

        assert received_messages == test_messages

    @pytest.mark.asyncio
    async def test_iter_messages_can_read_user_headers(
        self, iggy_client: IggyClient, unique_name
    ):
        """Test iter_messages yields messages with user headers."""
        consumer_name = unique_name()
        stream_name = unique_name()
        topic_name = unique_name()
        partition_id = 0
        expected_headers: dict[str, str | bytes | bool | int | float] = {
            "source": "iterator",
            "attempt": 2,
        }

        await iggy_client.create_stream(stream_name)
        await iggy_client.create_topic(stream=stream_name, name=topic_name)

        consumer = await iggy_client.consumer_group(
            consumer_name,
            stream_name,
            topic_name,
            polling_strategy=PollingStrategy.Next(),
            poll_options=POLL_OPTIONS,
        )

        await iggy_client.send_messages(
            stream_name,
            topic_name,
            partition_id,
            [Message("iterator headers", user_headers=expected_headers)],
        )

        iterator = consumer.iter_messages()
        message = await asyncio.wait_for(iterator.__anext__(), timeout=5)

        headers = message.user_headers()
        assert headers is not None
        assert headers.to_scalar_dict() == expected_headers

    @pytest.mark.asyncio
    async def test_iter_messages_with_first_reads_existing_messages(
        self, iggy_client: IggyClient, unique_name
    ):
        """Test iter_messages reads messages that already exist with First."""
        consumer_name = unique_name()
        stream_name = unique_name()
        topic_name = unique_name()
        partition_id = 0
        test_messages = [
            f"Existing message test {i} - {unique_name()}" for i in range(3)
        ]
        received_messages = []

        await iggy_client.create_stream(stream_name)
        await iggy_client.create_topic(stream=stream_name, name=topic_name)

        await iggy_client.send_messages(
            stream_name,
            topic_name,
            partition_id,
            [Message(message) for message in test_messages],
        )

        consumer = await iggy_client.consumer_group(
            consumer_name,
            stream_name,
            topic_name,
            polling_strategy=PollingStrategy.First(),
            poll_options=POLL_OPTIONS,
        )

        iterator = consumer.iter_messages()
        for _ in range(len(test_messages)):
            message = await asyncio.wait_for(iterator.__anext__(), timeout=5)
            received_messages.append(message.payload().decode())

        assert received_messages == test_messages

    @pytest.mark.asyncio
    async def test_iter_messages_with_last_reads_last_existing_message(
        self, iggy_client: IggyClient, unique_name
    ):
        """Test iter_messages with Last starts from the last existing message."""
        consumer_name = unique_name()
        stream_name = unique_name()
        topic_name = unique_name()
        partition_id = 0
        test_messages = [f"Last message test {i} - {unique_name()}" for i in range(3)]
        received_messages = []

        await iggy_client.create_stream(stream_name)
        await iggy_client.create_topic(stream=stream_name, name=topic_name)

        await iggy_client.send_messages(
            stream_name,
            topic_name,
            partition_id,
            [Message(message) for message in test_messages],
        )

        consumer = await iggy_client.consumer_group(
            consumer_name,
            stream_name,
            topic_name,
            polling_strategy=PollingStrategy.Last(),
            poll_options=PollOptions(count=1, max_wait=POLL_WAIT),
        )

        iterator = consumer.iter_messages()
        message = await asyncio.wait_for(iterator.__anext__(), timeout=5)
        received_messages.append(message)

        assert [message.payload().decode() for message in received_messages] == [
            test_messages[-1]
        ]
        assert (
            consumer.get_last_consumed_offset(partition_id)
            == received_messages[-1].offset()
        )

    @pytest.mark.asyncio
    async def test_iter_messages_with_last_and_count_two_reads_last_two_messages(
        self, iggy_client: IggyClient, unique_name
    ):
        """Test iter_messages with Last and count=2 reads the last two."""
        consumer_name = unique_name()
        stream_name = unique_name()
        topic_name = unique_name()
        partition_id = 0
        test_messages = [f"Last message test {i} - {unique_name()}" for i in range(4)]
        received_messages = []

        await iggy_client.create_stream(stream_name)
        await iggy_client.create_topic(stream=stream_name, name=topic_name)

        await iggy_client.send_messages(
            stream_name,
            topic_name,
            partition_id,
            [Message(message) for message in test_messages],
        )

        consumer = await iggy_client.consumer_group(
            consumer_name,
            stream_name,
            topic_name,
            polling_strategy=PollingStrategy.Last(),
            poll_options=PollOptions(count=2, max_wait=POLL_WAIT),
        )

        iterator = consumer.iter_messages()
        for _ in range(2):
            message = await asyncio.wait_for(iterator.__anext__(), timeout=5)
            received_messages.append(message.payload().decode())

        assert received_messages == test_messages[-2:]

    @pytest.mark.asyncio
    async def test_consume_messages_with_last_reads_last_existing_message(
        self, iggy_client: IggyClient, unique_name
    ):
        """Test consume_messages with Last starts from the last existing message."""
        consumer_name = unique_name()
        stream_name = unique_name()
        topic_name = unique_name()
        partition_id = 0
        test_messages = [f"Last message test {i} - {unique_name()}" for i in range(3)]
        received_messages = []
        shutdown_event = asyncio.Event()

        await iggy_client.create_stream(stream_name)
        await iggy_client.create_topic(stream=stream_name, name=topic_name)

        await iggy_client.send_messages(
            stream_name,
            topic_name,
            partition_id,
            [Message(message) for message in test_messages],
        )

        consumer = await iggy_client.consumer_group(
            consumer_name,
            stream_name,
            topic_name,
            polling_strategy=PollingStrategy.Last(),
            poll_options=PollOptions(count=1, max_wait=POLL_WAIT),
        )

        async def take(message: ReceiveMessage) -> None:
            received_messages.append(message)
            shutdown_event.set()

        await consumer.consume_messages(take, shutdown_event)

        assert [message.payload().decode() for message in received_messages] == [
            test_messages[-1]
        ]
        assert (
            consumer.get_last_consumed_offset(partition_id)
            == received_messages[-1].offset()
        )

    @pytest.mark.asyncio
    async def test_iter_messages_with_offset_starts_at_exact_message(
        self, iggy_client: IggyClient, unique_name
    ):
        """Test iter_messages with Offset starts from the requested message offset."""
        stream_name = unique_name()
        topic_name = unique_name()
        partition_id = 0
        test_messages = [f"Offset start test {i} - {unique_name()}" for i in range(4)]
        initial_messages = []
        offset_messages = []

        await iggy_client.create_stream(stream_name)
        await iggy_client.create_topic(stream=stream_name, name=topic_name)

        await iggy_client.send_messages(
            stream_name,
            topic_name,
            partition_id,
            [Message(message) for message in test_messages],
        )

        first_consumer = await iggy_client.consumer_group(
            unique_name(),
            stream_name,
            topic_name,
            polling_strategy=PollingStrategy.First(),
            poll_options=POLL_OPTIONS,
        )

        iterator = first_consumer.iter_messages()
        for _ in range(len(test_messages)):
            message = await asyncio.wait_for(iterator.__anext__(), timeout=5)
            initial_messages.append(message)

        start_offset = initial_messages[2].offset()

        offset_consumer = await iggy_client.consumer_group(
            unique_name(),
            stream_name,
            topic_name,
            polling_strategy=PollingStrategy.Offset(value=start_offset),
            poll_options=POLL_OPTIONS,
        )

        iterator = offset_consumer.iter_messages()
        for _ in range(len(test_messages) - 2):
            message = await asyncio.wait_for(iterator.__anext__(), timeout=5)
            offset_messages.append(message)

        assert offset_messages[0].offset() == start_offset
        assert [
            message.payload().decode() for message in offset_messages
        ] == test_messages[2:]
        assert (
            offset_consumer.get_last_consumed_offset(partition_id)
            == offset_messages[-1].offset()
        )

    @pytest.mark.asyncio
    async def test_consume_messages_with_offset_starts_at_exact_message(
        self, iggy_client: IggyClient, unique_name
    ):
        """Test consume_messages with Offset starts from the requested offset."""
        stream_name = unique_name()
        topic_name = unique_name()
        partition_id = 0
        test_messages = [f"Offset start test {i} - {unique_name()}" for i in range(4)]
        initial_messages = []
        offset_messages = []
        shutdown_event = asyncio.Event()

        await iggy_client.create_stream(stream_name)
        await iggy_client.create_topic(stream=stream_name, name=topic_name)

        await iggy_client.send_messages(
            stream_name,
            topic_name,
            partition_id,
            [Message(message) for message in test_messages],
        )

        first_consumer = await iggy_client.consumer_group(
            unique_name(),
            stream_name,
            topic_name,
            polling_strategy=PollingStrategy.First(),
            poll_options=POLL_OPTIONS,
        )

        iterator = first_consumer.iter_messages()
        for _ in range(len(test_messages)):
            message = await asyncio.wait_for(iterator.__anext__(), timeout=5)
            initial_messages.append(message)

        start_offset = initial_messages[2].offset()

        offset_consumer = await iggy_client.consumer_group(
            unique_name(),
            stream_name,
            topic_name,
            polling_strategy=PollingStrategy.Offset(value=start_offset),
            poll_options=POLL_OPTIONS,
        )

        async def take(message: ReceiveMessage) -> None:
            offset_messages.append(message)
            if len(offset_messages) == len(test_messages) - 2:
                shutdown_event.set()

        await offset_consumer.consume_messages(take, shutdown_event)

        assert offset_messages[0].offset() == start_offset
        assert [
            message.payload().decode() for message in offset_messages
        ] == test_messages[2:]
        assert (
            offset_consumer.get_last_consumed_offset(partition_id)
            == offset_messages[-1].offset()
        )

    @pytest.mark.asyncio
    async def test_iter_messages_with_timestamp_starts_at_or_after_timestamp(
        self, iggy_client: IggyClient, unique_name
    ):
        """Test iter_messages with Timestamp starts on or after that timestamp."""
        stream_name = unique_name()
        topic_name = unique_name()
        partition_id = 0
        test_messages = [
            f"Timestamp start test {i} - {unique_name()}" for i in range(3)
        ]
        initial_messages = []
        timestamp_messages = []

        await iggy_client.create_stream(stream_name)
        await iggy_client.create_topic(stream=stream_name, name=topic_name)

        for message in test_messages:
            await iggy_client.send_messages(
                stream_name,
                topic_name,
                partition_id,
                [Message(message)],
            )
            await asyncio.sleep(0.01)

        first_consumer = await iggy_client.consumer_group(
            unique_name(),
            stream_name,
            topic_name,
            polling_strategy=PollingStrategy.First(),
            poll_options=POLL_OPTIONS,
        )

        iterator = first_consumer.iter_messages()
        for _ in range(len(test_messages)):
            message = await asyncio.wait_for(iterator.__anext__(), timeout=5)
            initial_messages.append(message)

        start_timestamp = initial_messages[1].timestamp()

        timestamp_consumer = await iggy_client.consumer_group(
            unique_name(),
            stream_name,
            topic_name,
            polling_strategy=PollingStrategy.Timestamp(value=start_timestamp),
            poll_options=POLL_OPTIONS,
        )

        iterator = timestamp_consumer.iter_messages()
        for _ in range(len(test_messages) - 1):
            message = await asyncio.wait_for(iterator.__anext__(), timeout=5)
            timestamp_messages.append(message)

        assert timestamp_messages[0].timestamp() >= start_timestamp
        assert [
            message.payload().decode() for message in timestamp_messages
        ] == test_messages[1:]
        assert (
            timestamp_consumer.get_last_consumed_offset(partition_id)
            == timestamp_messages[-1].offset()
        )

    @pytest.mark.asyncio
    async def test_consume_messages_with_timestamp_starts_at_or_after_timestamp(
        self, iggy_client: IggyClient, unique_name
    ):
        """Test consume_messages with Timestamp starts on or after that timestamp."""
        stream_name = unique_name()
        topic_name = unique_name()
        partition_id = 0
        test_messages = [
            f"Timestamp start test {i} - {unique_name()}" for i in range(3)
        ]
        initial_messages = []
        timestamp_messages = []
        shutdown_event = asyncio.Event()

        await iggy_client.create_stream(stream_name)
        await iggy_client.create_topic(stream=stream_name, name=topic_name)

        for message in test_messages:
            await iggy_client.send_messages(
                stream_name,
                topic_name,
                partition_id,
                [Message(message)],
            )
            await asyncio.sleep(0.01)

        first_consumer = await iggy_client.consumer_group(
            unique_name(),
            stream_name,
            topic_name,
            polling_strategy=PollingStrategy.First(),
            poll_options=POLL_OPTIONS,
        )

        iterator = first_consumer.iter_messages()
        for _ in range(len(test_messages)):
            message = await asyncio.wait_for(iterator.__anext__(), timeout=5)
            initial_messages.append(message)

        start_timestamp = initial_messages[1].timestamp()

        timestamp_consumer = await iggy_client.consumer_group(
            unique_name(),
            stream_name,
            topic_name,
            polling_strategy=PollingStrategy.Timestamp(value=start_timestamp),
            poll_options=POLL_OPTIONS,
        )

        async def take(message: ReceiveMessage) -> None:
            timestamp_messages.append(message)
            if len(timestamp_messages) == len(test_messages) - 1:
                shutdown_event.set()

        await timestamp_consumer.consume_messages(take, shutdown_event)

        assert timestamp_messages[0].timestamp() >= start_timestamp
        assert [
            message.payload().decode() for message in timestamp_messages
        ] == test_messages[1:]
        assert (
            timestamp_consumer.get_last_consumed_offset(partition_id)
            == timestamp_messages[-1].offset()
        )

    @pytest.mark.asyncio
    async def test_iter_messages_updates_last_consumed_offset(
        self, iggy_client: IggyClient, unique_name
    ):
        """Test iter_messages updates the last consumed offset while iterating."""
        consumer_name = unique_name()
        stream_name = unique_name()
        topic_name = unique_name()
        partition_id = 0
        test_messages = [f"Iter offset test {i} - {unique_name()}" for i in range(3)]
        received_messages = []

        await iggy_client.create_stream(stream_name)
        await iggy_client.create_topic(stream=stream_name, name=topic_name)

        consumer = await iggy_client.consumer_group(
            consumer_name,
            stream_name,
            topic_name,
            polling_strategy=PollingStrategy.First(),
            poll_options=POLL_OPTIONS,
        )

        assert consumer.get_last_consumed_offset(partition_id) is None

        await iggy_client.send_messages(
            stream_name,
            topic_name,
            partition_id,
            [Message(message) for message in test_messages],
        )

        iterator = consumer.iter_messages()
        for _ in range(2):
            message = await asyncio.wait_for(iterator.__anext__(), timeout=5)
            received_messages.append(message)

        assert (
            consumer.get_last_consumed_offset(partition_id)
            == received_messages[-1].offset()
        )

        for _ in range(len(test_messages) - len(received_messages)):
            message = await asyncio.wait_for(iterator.__anext__(), timeout=5)
            received_messages.append(message)

        assert (
            consumer.get_last_consumed_offset(partition_id)
            == received_messages[-1].offset()
        )

    @pytest.mark.asyncio
    async def test_iter_messages_reads_across_multiple_batches(
        self, iggy_client: IggyClient, unique_name
    ):
        """Test iter_messages continues across polls when count is small."""
        consumer_name = unique_name()
        stream_name = unique_name()
        topic_name = unique_name()
        partition_id = 0
        test_messages = [f"Batch test {i} - {unique_name()}" for i in range(5)]
        received_messages = []

        await iggy_client.create_stream(stream_name)
        await iggy_client.create_topic(stream=stream_name, name=topic_name)

        await iggy_client.send_messages(
            stream_name,
            topic_name,
            partition_id,
            [Message(message) for message in test_messages],
        )

        consumer = await iggy_client.consumer_group(
            consumer_name,
            stream_name,
            topic_name,
            polling_strategy=PollingStrategy.First(),
            poll_options=PollOptions(count=2, max_wait=POLL_WAIT),
        )

        iterator = consumer.iter_messages()
        for _ in range(len(test_messages)):
            message = await asyncio.wait_for(iterator.__anext__(), timeout=5)
            received_messages.append(message.payload().decode())

        assert received_messages == test_messages

    @pytest.mark.asyncio
    @pytest.mark.parametrize("allow_replay", [False, True])
    async def test_store_offset_moves_the_stored_offset_back(
        self,
        iggy_client: IggyClient,
        unique_name,
        allow_replay: bool,
    ):
        """Test storing a lower offset rewinds the group, with or without replay."""
        consumer_name = unique_name()
        stream_name = unique_name()
        topic_name = unique_name()
        partition_id = 0
        test_messages = [f"Rewind test {i} - {unique_name()}" for i in range(3)]
        received_messages = []
        replayed_messages = []

        await iggy_client.create_stream(stream_name)
        await iggy_client.create_topic(stream=stream_name, name=topic_name)

        consumer = await iggy_client.consumer_group(
            consumer_name,
            stream_name,
            topic_name,
            polling_strategy=PollingStrategy.First(),
            poll_options=POLL_OPTIONS,
            allow_replay=allow_replay,
        )

        await iggy_client.send_messages(
            stream_name,
            topic_name,
            partition_id,
            [Message(message) for message in test_messages],
        )

        iterator = consumer.iter_messages()
        for _ in range(len(test_messages)):
            message = await asyncio.wait_for(iterator.__anext__(), timeout=5)
            received_messages.append(message)

        offsets = [message.offset() for message in received_messages]

        await consumer.store_offset(offsets[-1], partition_id)
        assert consumer.get_last_stored_offset(partition_id) == offsets[-1]

        await consumer.store_offset(offsets[-2], partition_id)
        assert consumer.get_last_stored_offset(partition_id) == offsets[-2]

        replay_consumer = await iggy_client.consumer_group(
            consumer_name,
            stream_name,
            topic_name,
            polling_strategy=PollingStrategy.Next(),
            poll_options=PollOptions(count=1, max_wait=POLL_WAIT),
            allow_replay=allow_replay,
        )

        iterator = replay_consumer.iter_messages()
        message = await asyncio.wait_for(iterator.__anext__(), timeout=5)
        replayed_messages.append(message.payload().decode())
        assert replayed_messages == [test_messages[-1]]

    @pytest.mark.asyncio
    @pytest.mark.parametrize("allow_replay", [False, True])
    async def test_seek_back_reads_again_with_and_without_allow_replay(
        self,
        iggy_client: IggyClient,
        unique_name,
        allow_replay: bool,
    ):
        """Test a seek back reads the sought messages again with any allow_replay."""
        consumer_name = unique_name()
        stream_name = unique_name()
        topic_name = unique_name()
        partition_id = 0
        test_messages = [f"Replay test {i} - {unique_name()}" for i in range(3)]
        final_message = f"Replay test 3 - {unique_name()}"
        received_messages = []

        await iggy_client.create_stream(stream_name)
        await iggy_client.create_topic(stream=stream_name, name=topic_name)

        consumer = await iggy_client.consumer_group(
            consumer_name,
            stream_name,
            topic_name,
            polling_strategy=PollingStrategy.First(),
            poll_options=POLL_OPTIONS,
            allow_replay=allow_replay,
        )

        await iggy_client.send_messages(
            stream_name,
            topic_name,
            partition_id,
            [Message(message) for message in test_messages],
        )

        iterator = consumer.iter_messages()
        for _ in range(len(test_messages)):
            message = await asyncio.wait_for(iterator.__anext__(), timeout=5)
            received_messages.append(message)

        await consumer.seek(
            partition_id,
            PollingStrategy.Offset(value=received_messages[-1].offset()),
        )
        # The replay filter skips only deliveries of the generation that the seek
        # ended, so both cases yield the sought message before the later one.
        await iggy_client.send_messages(
            stream_name,
            topic_name,
            partition_id,
            [Message(final_message)],
        )

        message = await asyncio.wait_for(iterator.__anext__(), timeout=5)
        assert message.payload().decode() == test_messages[-1]

    @pytest.mark.asyncio
    async def test_consumer_group_retries_initialization_until_stream_and_topic_exist(
        self, iggy_client: IggyClient, unique_name
    ):
        """Test init retries allow late stream/topic creation."""
        consumer_name = unique_name()
        stream_name = unique_name()
        topic_name = unique_name()

        consumer_task = asyncio.ensure_future(
            iggy_client.consumer_group(
                consumer_name,
                stream_name,
                topic_name,
                polling_strategy=PollingStrategy.Next(),
                poll_options=POLL_OPTIONS,
                init_retries=10,
                init_retry_interval=timedelta(milliseconds=50),
            )
        )

        await asyncio.sleep(timedelta(milliseconds=50).total_seconds() * 2)

        await iggy_client.create_stream(stream_name)
        await iggy_client.create_topic(stream=stream_name, name=topic_name)

        consumer = await consumer_task

        assert consumer.name() == consumer_name
        assert consumer.stream() == stream_name
        assert consumer.topic() == topic_name

    @pytest.mark.asyncio
    async def test_consumer_group_joins_the_group_on_initialization(
        self, iggy_client: IggyClient, unique_name
    ):
        """Test initialization joins the group before the consumer is returned."""
        consumer_name = unique_name()
        stream_name = unique_name()
        topic_name = unique_name()
        partition_id = 0

        await iggy_client.create_stream(stream_name)
        await iggy_client.create_topic(stream=stream_name, name=topic_name)

        consumer = await iggy_client.consumer_group(
            consumer_name,
            stream_name,
            topic_name,
            polling_strategy=PollingStrategy.Next(),
            poll_options=POLL_OPTIONS,
        )

        assert consumer.name() == consumer_name
        assert consumer.stream() == stream_name
        assert consumer.topic() == topic_name
        assert consumer.partition_id() == partition_id
        group = await iggy_client.get_consumer_group(
            stream_name, topic_name, consumer_name
        )
        assert group is not None
        assert group.members_count == 1

    @pytest.mark.asyncio
    async def test_consumer_group_can_disable_auto_create_when_group_exists(
        self, iggy_client: IggyClient, unique_name
    ):
        """Test disabling auto-create still works for an existing group."""
        consumer_name = unique_name()
        stream_name = unique_name()
        topic_name = unique_name()

        await iggy_client.create_stream(stream_name)
        await iggy_client.create_topic(stream=stream_name, name=topic_name)

        await iggy_client.consumer_group(
            consumer_name,
            stream_name,
            topic_name,
            polling_strategy=PollingStrategy.Next(),
            poll_options=POLL_OPTIONS,
        )

        consumer = await iggy_client.consumer_group(
            consumer_name,
            stream_name,
            topic_name,
            polling_strategy=PollingStrategy.Next(),
            poll_options=POLL_OPTIONS,
            create_consumer_group_if_not_exists=False,
        )

        assert consumer.name() == consumer_name

    @pytest.mark.asyncio
    @pytest.mark.parametrize(
        ("create_stream", "expected_error"),
        [
            (False, "Stream with name:"),
            (True, "Topic with name:"),
        ],
    )
    async def test_consumer_group_missing_stream_or_topic_fails(
        self,
        iggy_client: IggyClient,
        unique_name,
        create_stream: bool,
        expected_error: str,
    ):
        """Test initialization fails when the target stream or topic is missing."""
        consumer_name = unique_name()
        stream_name = unique_name()
        topic_name = unique_name()

        if create_stream:
            await iggy_client.create_stream(stream_name)

        with pytest.raises(RuntimeError, match=expected_error):
            await iggy_client.consumer_group(
                consumer_name,
                stream_name,
                topic_name,
                polling_strategy=PollingStrategy.Next(),
                poll_options=POLL_OPTIONS,
            )

    @pytest.mark.asyncio
    async def test_consumer_group_auto_create_disabled_fails_for_missing_group(
        self, iggy_client: IggyClient, unique_name
    ):
        """Test auto-create disabled fails for a missing group."""
        stream_name = unique_name()
        topic_name = unique_name()

        await iggy_client.create_stream(stream_name)
        await iggy_client.create_topic(stream=stream_name, name=topic_name)

        with pytest.raises(RuntimeError, match="Consumer group with name:"):
            await iggy_client.consumer_group(
                unique_name(),
                stream_name,
                topic_name,
                polling_strategy=PollingStrategy.Next(),
                poll_options=POLL_OPTIONS,
                create_consumer_group_if_not_exists=False,
            )

    @pytest.mark.asyncio
    @pytest.mark.parametrize(
        ("kwargs", "expected_error"),
        [
            (
                {"init_retries": 1},
                "'init_retry_interval' is required if 'init_retries' is set",
            ),
            (
                {"init_retry_interval": timedelta(milliseconds=50)},
                "'init_retries' is required if 'init_retry_interval' is set",
            ),
        ],
    )
    async def test_consumer_group_requires_complete_init_retry_configuration(
        self, iggy_client: IggyClient, unique_name, kwargs, expected_error: str
    ):
        """Test init retry parameters must be configured together."""
        stream_name = unique_name()
        topic_name = unique_name()

        await iggy_client.create_stream(stream_name)
        await iggy_client.create_topic(stream=stream_name, name=topic_name)

        with pytest.raises(RuntimeError, match=expected_error):
            await iggy_client.consumer_group(
                unique_name(),
                stream_name,
                topic_name,
                polling_strategy=PollingStrategy.Next(),
                poll_options=POLL_OPTIONS,
                **kwargs,
            )

    @pytest.mark.asyncio
    async def test_consumer_group_before_connect_fails(self, unique_name):
        """Test consumer_group requires an established connection."""
        host, port = get_server_config()
        client = IggyClient(f"{host}:{port}")

        with pytest.raises(RuntimeError):
            await client.consumer_group(
                unique_name(),
                unique_name(),
                unique_name(),
                polling_strategy=PollingStrategy.Next(),
                poll_options=POLL_OPTIONS,
            )

    @pytest.mark.asyncio
    async def test_consumer_group_before_login_fails(self, unique_name):
        """Test consumer_group requires authentication."""
        host, port = get_server_config()
        wait_for_server(host, port)

        client = IggyClient(f"{host}:{port}")
        await client.connect()
        await wait_for_ping(client)

        with pytest.raises(RuntimeError):
            await client.consumer_group(
                unique_name(),
                unique_name(),
                unique_name(),
                polling_strategy=PollingStrategy.Next(),
                poll_options=POLL_OPTIONS,
            )

    @pytest.mark.asyncio
    @pytest.mark.parametrize(
        "timeout_options",
        [
            pytest.param({}, id="omitted"),
            pytest.param({"offset_drain_timeout": None}, id="none"),
            pytest.param({"offset_drain_timeout": timedelta(seconds=2)}, id="custom"),
            pytest.param({"offset_drain_timeout": timedelta(0)}, id="zero"),
        ],
    )
    async def test_shutdown(
        self, iggy_client: IggyClient, unique_name, timeout_options
    ):
        """Test shutdown accepts the default and configured offset drain timeout."""
        consumer_name = unique_name()
        stream_name = unique_name()
        topic_name = unique_name()
        shutdown_event = asyncio.Event()

        await iggy_client.create_stream(stream_name)
        await iggy_client.create_topic(stream=stream_name, name=topic_name)

        consumer = await iggy_client.consumer_group(
            consumer_name,
            stream_name,
            topic_name,
            polling_strategy=PollingStrategy.Next(),
            poll_options=INTERVAL_COMMIT_OPTIONS,
            **timeout_options,
        )

        async def take(_: ReceiveMessage) -> None:
            pass

        shutdown_event.set()

        await consumer.consume_messages(take, shutdown_event)
        await consumer.shutdown()

        group = await iggy_client.get_consumer_group(
            stream_name, topic_name, consumer_name
        )
        assert group is not None
        assert group.members_count == 0

    @pytest.mark.asyncio
    async def test_negative_offset_drain_timeout_fails(
        self, iggy_client: IggyClient, unique_name
    ):
        """Test a negative offset drain timeout is rejected before initialization."""
        with pytest.raises(ValueError, match="duration must not be negative"):
            await iggy_client.consumer_group(
                unique_name(),
                unique_name(),
                unique_name(),
                offset_drain_timeout=timedelta(microseconds=-1),
            )

    @pytest.mark.asyncio
    async def test_shutdown_with_on_read_moves_the_offset_back(
        self, iggy_client: IggyClient, unique_name
    ):
        """Test OnRead shutdown moves the offset back to the last yielded message."""
        consumer_name = unique_name()
        stream_name = unique_name()
        topic_name = unique_name()
        partition_id = 0
        test_messages = [f"OnRead test {i} - {unique_name()}" for i in range(5)]
        yielded_count = 2
        yielded_messages = []
        resumed_messages = []

        await iggy_client.create_stream(stream_name)
        await iggy_client.create_topic(stream=stream_name, name=topic_name)

        await iggy_client.send_messages(
            stream_name,
            topic_name,
            partition_id,
            [Message(message) for message in test_messages],
        )

        consumer = await iggy_client.consumer_group(
            consumer_name,
            stream_name,
            topic_name,
            polling_strategy=PollingStrategy.Next(),
            poll_options=PollOptions(
                count=len(test_messages),
                max_wait=POLL_WAIT,
                commit=CommitPolicy.OnRead(),
            ),
        )

        iterator = consumer.iter_messages()
        for _ in range(yielded_count):
            message = await asyncio.wait_for(iterator.__anext__(), timeout=5)
            yielded_messages.append(message)

        # OnRead stored the batch end, so the shutdown has an offset to move back.
        stored_offset = consumer.get_last_stored_offset(partition_id)
        assert stored_offset is not None
        assert stored_offset > yielded_messages[-1].offset()

        await consumer.shutdown()

        assert (
            consumer.get_last_stored_offset(partition_id)
            == yielded_messages[-1].offset()
        )

        resuming_consumer = await iggy_client.consumer_group(
            consumer_name,
            stream_name,
            topic_name,
            polling_strategy=PollingStrategy.Next(),
            poll_options=POLL_OPTIONS,
        )

        iterator = resuming_consumer.iter_messages()
        for _ in range(len(test_messages) - yielded_count):
            message = await asyncio.wait_for(iterator.__anext__(), timeout=5)
            resumed_messages.append(message.payload().decode())

        assert resumed_messages == test_messages[yielded_count:]

    @pytest.mark.asyncio
    async def test_cancelled_iterations_lose_and_repeat_no_message(
        self, iggy_client: IggyClient, unique_name
    ):
        """Test iterations cancelled at short deadlines yield each offset once."""
        consumer_name = unique_name()
        stream_name = unique_name()
        topic_name = unique_name()
        partition_id = 0

        await iggy_client.create_stream(stream_name)
        await iggy_client.create_topic(stream=stream_name, name=topic_name)
        await iggy_client.send_messages(
            stream_name,
            topic_name,
            partition_id,
            [Message(f"Cancel test {i}") for i in range(CANCELLED_ITERATIONS_MESSAGES)],
        )

        consumer = await iggy_client.consumer_group(
            consumer_name,
            stream_name,
            topic_name,
            polling_strategy=PollingStrategy.First(),
            poll_options=PollOptions(count=ONE_MESSAGE, max_wait=POLL_WAIT),
            prefetch_messages=ONE_MESSAGE,
        )
        iterator = consumer.iter_messages()
        try:
            offsets = await read_offsets_with_cancels(
                lambda: _message_offset(iterator), CANCELLED_ITERATIONS_MESSAGES - 1
            )
        finally:
            await consumer.shutdown()

        assert offsets == list(range(CANCELLED_ITERATIONS_MESSAGES))

    @pytest.mark.asyncio
    async def test_seek_from_the_consume_messages_callback_raises(
        self, iggy_client: IggyClient, unique_name
    ):
        """Test seek() raises RuntimeError in consume_messages() and works after it."""
        consumer_name = unique_name()
        stream_name = unique_name()
        topic_name = unique_name()
        partition_id = 0
        shutdown_event = asyncio.Event()
        refusals: list[str] = []

        await iggy_client.create_stream(stream_name)
        await iggy_client.create_topic(stream=stream_name, name=topic_name)
        await iggy_client.send_messages(
            stream_name, topic_name, partition_id, [Message("Seek test")]
        )

        consumer = await iggy_client.consumer_group(
            consumer_name,
            stream_name,
            topic_name,
            polling_strategy=PollingStrategy.First(),
            poll_options=POLL_OPTIONS,
        )

        async def take(_: ReceiveMessage) -> None:
            try:
                await consumer.seek(partition_id, PollingStrategy.First())
            except RuntimeError as error:
                refusals.append(str(error))
            shutdown_event.set()

        try:
            await asyncio.wait_for(
                consumer.consume_messages(take, shutdown_event),
                timeout=RUN_TIMEOUT_SECONDS,
            )
            await consumer.seek(partition_id, PollingStrategy.First())
        finally:
            await consumer.shutdown()

        assert refusals == ["seek() cannot run while consume_messages() runs"]

    @pytest.mark.asyncio
    async def test_consume_messages_without_an_event_runs_until_cancelled(
        self, iggy_client: IggyClient, unique_name
    ):
        """Test consume_messages() without an event consumes until a cancel ends it."""
        consumer_name = unique_name()
        stream_name = unique_name()
        topic_name = unique_name()
        partition_id = 0
        all_taken = asyncio.Event()
        offsets: list[int] = []

        await iggy_client.create_stream(stream_name)
        await iggy_client.create_topic(stream=stream_name, name=topic_name)
        await iggy_client.send_messages(
            stream_name,
            topic_name,
            partition_id,
            [Message(f"No event test {i}") for i in range(NO_EVENT_RUN_MESSAGES)],
        )

        consumer = await iggy_client.consumer_group(
            consumer_name,
            stream_name,
            topic_name,
            polling_strategy=PollingStrategy.First(),
            poll_options=POLL_OPTIONS,
        )

        async def take(message: ReceiveMessage) -> None:
            offsets.append(message.offset())
            if len(offsets) == NO_EVENT_RUN_MESSAGES:
                all_taken.set()

        run = asyncio.ensure_future(consumer.consume_messages(take, None))
        try:
            await asyncio.wait_for(all_taken.wait(), timeout=RUN_TIMEOUT_SECONDS)
            assert not run.done(), "The run ended without a cancel"
        finally:
            run.cancel()
            # The run holds the consumer until the cancel ends it.
            await asyncio.wait_for(consumer.shutdown(), timeout=RUN_TIMEOUT_SECONDS)

        assert offsets == list(range(NO_EVENT_RUN_MESSAGES))

    @pytest.mark.asyncio
    async def test_auto_commit_after_delivery_stores_the_yielded_position_on_shutdown(
        self, iggy_client: IggyClient, unique_name
    ):
        """Test CommitAfter.Delivery counts a message when it is yielded."""
        stream_name, topic_name = unique_name(), unique_name()
        await iggy_client.create_stream(stream_name)
        await iggy_client.create_topic(stream=stream_name, name=topic_name)
        await iggy_client.send_messages(
            stream_name, topic_name, 0, [Message(f"Delivery {i}") for i in range(3)]
        )
        consumer = await iggy_client.consumer_group(
            unique_name(),
            stream_name,
            topic_name,
            polling_strategy=PollingStrategy.First(),
            poll_options=PollOptions(
                count=10,
                max_wait=POLL_WAIT,
                commit=CommitPolicy.Auto(
                    AutoCommit(every=CommitEvery.Message(), after=CommitAfter.Delivery)
                ),
            ),
        )
        iterator = consumer.iter_messages()
        first = await asyncio.wait_for(anext(iterator), timeout=RUN_TIMEOUT_SECONDS)
        second = await asyncio.wait_for(anext(iterator), timeout=RUN_TIMEOUT_SECONDS)

        await consumer.shutdown()

        assert consumer.commit_policy == CommitPolicy.Auto(
            AutoCommit(every=CommitEvery.Message(), after=CommitAfter.Delivery)
        )
        assert [first.offset(), second.offset()] == [0, 1]
        assert consumer.get_last_stored_offset(0) == second.offset()

    @pytest.mark.asyncio
    async def test_auto_commit_after_processing_ends_the_run_on_a_raising_callback(
        self, iggy_client: IggyClient, unique_name
    ):
        """Test a raising callback ends the run and leaves its message uncounted."""
        stream_name, topic_name = unique_name(), unique_name()
        await iggy_client.create_stream(stream_name)
        await iggy_client.create_topic(stream=stream_name, name=topic_name)
        await iggy_client.send_messages(
            stream_name, topic_name, 0, [Message(f"Processing {i}") for i in range(3)]
        )
        consumer = await iggy_client.consumer_group(
            unique_name(),
            stream_name,
            topic_name,
            polling_strategy=PollingStrategy.First(),
            poll_options=PollOptions(
                count=10,
                max_wait=POLL_WAIT,
                commit=CommitPolicy.Auto(AutoCommit(every=CommitEvery.Message())),
            ),
        )
        handled: list[int] = []

        async def take(message: ReceiveMessage) -> None:
            if message.offset() == 1:
                raise ValueError("handler failed at offset 1")
            handled.append(message.offset())

        try:
            with pytest.raises(ValueError, match="handler failed at offset 1"):
                await asyncio.wait_for(
                    consumer.consume_messages(take, None), timeout=RUN_TIMEOUT_SECONDS
                )
        finally:
            await consumer.shutdown()

        assert handled == [0]
        assert consumer.get_last_stored_offset(0) == 0

    @pytest.mark.asyncio
    async def test_auto_commit_after_marked_stores_only_the_marked_position(
        self, iggy_client: IggyClient, unique_name
    ):
        """Test mark() counts under CommitAfter.Marked and is refused without Auto."""
        stream_name, topic_name = unique_name(), unique_name()
        await iggy_client.create_stream(stream_name)
        await iggy_client.create_topic(stream=stream_name, name=topic_name)
        await iggy_client.send_messages(
            stream_name, topic_name, 0, [Message(f"Marked {i}") for i in range(3)]
        )
        marked = await iggy_client.consumer_group(
            unique_name(),
            stream_name,
            topic_name,
            polling_strategy=PollingStrategy.First(),
            poll_options=PollOptions(
                count=10,
                max_wait=POLL_WAIT,
                commit=CommitPolicy.Auto(
                    AutoCommit(every=CommitEvery.Message(), after=CommitAfter.Marked)
                ),
            ),
        )
        manual = await iggy_client.consumer_group(
            unique_name(),
            stream_name,
            topic_name,
            polling_strategy=PollingStrategy.First(),
            poll_options=POLL_OPTIONS,
        )
        try:
            iterator = marked.iter_messages()
            first = await asyncio.wait_for(anext(iterator), timeout=RUN_TIMEOUT_SECONDS)
            await asyncio.wait_for(anext(iterator), timeout=RUN_TIMEOUT_SECONDS)
            marked.mark(first.checkpoint())
            unmarked = await asyncio.wait_for(
                anext(manual.iter_messages()), timeout=RUN_TIMEOUT_SECONDS
            )
            with pytest.raises(IggyError) as refused:
                manual.mark(unmarked.checkpoint())
        finally:
            await marked.shutdown()
            await manual.shutdown()

        assert marked.get_last_stored_offset(0) == first.offset()
        assert refused.value.code == INVALID_COMMAND

    @pytest.mark.asyncio
    async def test_calls_that_wait_for_the_run_raise_in_its_callback(
        self, iggy_client: IggyClient, unique_name
    ):
        """Test shutdown(), iteration and a nested run raise in a callback, not hang."""
        consumer_name = unique_name()
        stream_name = unique_name()
        topic_name = unique_name()
        partition_id = 0
        shutdown_event = asyncio.Event()
        refusals: list[str] = []

        await iggy_client.create_stream(stream_name)
        await iggy_client.create_topic(stream=stream_name, name=topic_name)
        await iggy_client.send_messages(
            stream_name, topic_name, partition_id, [Message("Callback test")]
        )

        consumer = await iggy_client.consumer_group(
            consumer_name,
            stream_name,
            topic_name,
            polling_strategy=PollingStrategy.First(),
            poll_options=POLL_OPTIONS,
        )

        async def take(_: ReceiveMessage) -> None:
            calls = [
                consumer.shutdown,
                lambda: anext(consumer.iter_messages()),
                lambda: consumer.consume_messages(take, None),
            ]
            for call in calls:
                try:
                    await call()
                except RuntimeError as error:
                    refusals.append(str(error))
            shutdown_event.set()

        try:
            await asyncio.wait_for(
                consumer.consume_messages(take, shutdown_event),
                timeout=RUN_TIMEOUT_SECONDS,
            )
        finally:
            await asyncio.wait_for(consumer.shutdown(), timeout=RUN_TIMEOUT_SECONDS)

        assert refusals == [
            "shutdown() cannot run in a consume_messages() callback",
            "Iterating the consumer cannot run in a consume_messages() callback",
            "consume_messages() cannot run in a consume_messages() callback",
        ]

    @pytest.mark.asyncio
    async def test_shutdown_ends_a_parked_iteration(
        self, iggy_client: IggyClient, unique_name
    ):
        """Test shutdown() from another task ends an iteration parked while idle."""
        consumer_name = unique_name()
        stream_name = unique_name()
        topic_name = unique_name()

        await iggy_client.create_stream(stream_name)
        await iggy_client.create_topic(stream=stream_name, name=topic_name)

        consumer = await iggy_client.consumer_group(
            consumer_name,
            stream_name,
            topic_name,
            polling_strategy=PollingStrategy.First(),
            poll_options=POLL_OPTIONS,
        )
        parked = asyncio.ensure_future(anext(consumer.iter_messages()))
        # Long enough for the iteration to take the consumer and wait for a message.
        await asyncio.sleep(POLL_WAIT.total_seconds())
        assert not parked.done()

        await asyncio.wait_for(consumer.shutdown(), timeout=RUN_TIMEOUT_SECONDS)

        with pytest.raises(StopAsyncIteration):
            await asyncio.wait_for(parked, timeout=RUN_TIMEOUT_SECONDS)

    @pytest.mark.asyncio
    async def test_cancel_of_a_run_with_an_event_ends_the_run(
        self, iggy_client: IggyClient, unique_name
    ):
        """Test a cancel ends a run whose shutdown event is never set."""
        consumer_name = unique_name()
        stream_name = unique_name()
        topic_name = unique_name()
        partition_id = 0
        taken = asyncio.Event()

        await iggy_client.create_stream(stream_name)
        await iggy_client.create_topic(stream=stream_name, name=topic_name)
        await iggy_client.send_messages(
            stream_name, topic_name, partition_id, [Message("Cancel run test")]
        )

        consumer = await iggy_client.consumer_group(
            consumer_name,
            stream_name,
            topic_name,
            polling_strategy=PollingStrategy.First(),
            poll_options=POLL_OPTIONS,
        )

        async def take(_: ReceiveMessage) -> None:
            taken.set()

        run = asyncio.ensure_future(consumer.consume_messages(take, asyncio.Event()))
        await asyncio.wait_for(taken.wait(), timeout=RUN_TIMEOUT_SECONDS)
        run.cancel()

        # The run holds the consumer, so the shutdown waits for the cancel to end it.
        await asyncio.wait_for(consumer.shutdown(), timeout=RUN_TIMEOUT_SECONDS)

    @pytest.mark.asyncio
    async def test_async_with_shuts_the_consumer_down_on_exit(
        self, iggy_client: IggyClient, unique_name
    ):
        """Test async with yields the consumer and shuts it down when the block ends."""
        consumer_name = unique_name()
        stream_name = unique_name()
        topic_name = unique_name()
        partition_id = 0

        await iggy_client.create_stream(stream_name)
        await iggy_client.create_topic(stream=stream_name, name=topic_name)
        await iggy_client.send_messages(
            stream_name, topic_name, partition_id, [Message("Context test")]
        )

        consumer = await iggy_client.consumer_group(
            consumer_name,
            stream_name,
            topic_name,
            polling_strategy=PollingStrategy.First(),
            poll_options=POLL_OPTIONS,
        )
        async with consumer as entered:
            iterator = entered.iter_messages()
            message = await asyncio.wait_for(anext(iterator), timeout=5)

        assert entered is consumer
        assert message.payload() == b"Context test"
        with pytest.raises(StopAsyncIteration):
            await asyncio.wait_for(anext(iterator), timeout=RUN_TIMEOUT_SECONDS)


class TestStandaloneConsumer:
    """Test the consumer of chosen partitions built by IggyClient.consumer()."""

    @pytest.mark.asyncio
    async def test_standalone_consumer_reads_its_partitions_and_stores_on_shutdown(
        self, iggy_client: IggyClient, unique_name
    ):
        """Test a consumer of two partitions reads both and stores each on shutdown."""
        consumer_name = unique_name()
        stream_name = unique_name()
        topic_name = unique_name()
        partitions = [0, 1]
        await iggy_client.create_stream(stream_name)
        await iggy_client.create_topic(
            stream=stream_name,
            name=topic_name,
            options=TopicOptions(partitions_count=len(partitions)),
        )
        for partition_id in partitions:
            await iggy_client.send_messages(
                stream_name,
                topic_name,
                partition_id,
                [Message(f"Standalone {partition_id} {i}") for i in range(2)],
            )

        consumer = await iggy_client.consumer(
            consumer_name,
            stream_name,
            topic_name,
            partitions=partitions,
            polling_strategy=PollingStrategy.First(),
            poll_options=PollOptions(
                count=10,
                max_wait=POLL_WAIT,
                commit=CommitPolicy.Auto(
                    AutoCommit(every=CommitEvery.Message(), after=CommitAfter.Delivery)
                ),
            ),
        )
        async with consumer:
            iterator = consumer.iter_messages()
            received = [
                await asyncio.wait_for(anext(iterator), timeout=RUN_TIMEOUT_SECONDS)
                for _ in range(4)
            ]

        assert sorted(
            (message.partition_id(), message.payload().decode()) for message in received
        ) == [(p, f"Standalone {p} {i}") for p in partitions for i in range(2)]
        for partition_id in partitions:
            stored = await iggy_client.get_consumer_offset(
                stream_name,
                topic_name,
                consumer=Consumer.Single(consumer_name),
                partition_id=partition_id,
            )
            assert stored is not None
            assert stored.stored_offset == 1
