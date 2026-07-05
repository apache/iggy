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
from datetime import timedelta

import pytest

from apache_iggy import (
    AutoCommit,
    IggyClient,
    PollingStrategy,
    ReceiveMessage,
)
from apache_iggy import SendMessage as Message

from .utils import get_server_config, wait_for_ping, wait_for_server


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
            partitions_count=2,
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

        await iggy_client.create_topic(
            stream=stream_id,
            name=topic_name,
            partitions_count=1,
        )
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
        await iggy_client.create_topic(
            stream=stream_name,
            name=topic_name,
            partitions_count=1,
        )
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

        client = IggyClient(server_address=f"{host}:{port}")
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
        await iggy_client.create_topic(
            stream=stream_name,
            name=topic_name,
            partitions_count=1,
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
        await iggy_client.create_topic(
            stream=stream_name,
            name=topic_name,
            partitions_count=1,
        )

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
        await iggy_client.create_topic(
            stream=stream_name,
            name=topic_name,
            partitions_count=1,
        )

        await iggy_client.consumer_group(
            consumer_name,
            stream_name,
            topic_name,
            partition_id,
            PollingStrategy.Next(),
            10,
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

        client = IggyClient(server_address=f"{host}:{port}")
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
        await iggy_client.create_topic(
            stream=stream_name,
            name=topic_name,
            partitions_count=1,
        )

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
        await iggy_client.create_topic(
            stream=stream_name,
            name=topic_name,
            partitions_count=1,
        )
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

        await iggy_client.create_topic(
            stream=stream_id,
            name=topic_name,
            partitions_count=1,
        )
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
        await iggy_client.create_topic(
            stream=stream_name,
            name=topic_name,
            partitions_count=1,
        )
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

        client = IggyClient(server_address=f"{host}:{port}")
        with pytest.raises(RuntimeError):
            await client.get_consumer_groups(unique_name(), unique_name())

        await client.connect()
        with pytest.raises(RuntimeError):
            await client.get_consumer_groups(unique_name(), unique_name())


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
        await iggy_client.create_topic(
            stream=stream_name,
            name=topic_name,
            partitions_count=1,
        )

        consumer = await iggy_client.consumer_group(
            consumer_name,
            stream_name,
            topic_name,
            partition_id,
            PollingStrategy.Next(),
            10,
            auto_commit=AutoCommit.Interval(timedelta(seconds=5)),
            poll_interval=timedelta(milliseconds=25),
        )

        assert consumer.name() == consumer_name
        assert consumer.stream() == stream_name
        assert consumer.topic() == topic_name
        assert consumer.partition_id() == partition_id
        assert consumer.get_last_consumed_offset(partition_id) is None
        assert consumer.get_last_stored_offset(partition_id) is None

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
        await iggy_client.create_topic(
            stream=stream_name,
            name=topic_name,
            partitions_count=1,
        )

        consumer = await iggy_client.consumer_group(
            consumer_name,
            stream_name,
            topic_name,
            partition_id,
            PollingStrategy.First(),
            10,
            auto_commit=AutoCommit.Disabled(),
            poll_interval=timedelta(milliseconds=25),
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
        await iggy_client.create_topic(
            stream=stream_name,
            name=topic_name,
            partitions_count=1,
        )

        consumer = await iggy_client.consumer_group(
            consumer_name,
            stream_name,
            topic_name,
            partition_id,
            PollingStrategy.First(),
            10,
            auto_commit=AutoCommit.Disabled(),
            poll_interval=timedelta(milliseconds=25),
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
        await iggy_client.create_topic(
            stream=stream_name,
            name=topic_name,
            partitions_count=1,
        )

        consumer = await iggy_client.consumer_group(
            consumer_name,
            stream_name,
            topic_name,
            partition_id,
            PollingStrategy.First(),
            10,
            auto_commit=AutoCommit.Disabled(),
            poll_interval=timedelta(milliseconds=25),
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
        await iggy_client.create_topic(
            stream=stream_name,
            name=topic_name,
            partitions_count=1,
        )

        consumer = await iggy_client.consumer_group(
            consumer_name,
            stream_name,
            topic_name,
            partition_id,
            PollingStrategy.Next(),
            10,
            auto_commit=AutoCommit.Interval(timedelta(seconds=5)),
            poll_interval=timedelta(milliseconds=25),
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
    async def test_iter_messages(self, iggy_client: IggyClient, unique_name):
        """Test that the consumer group can iterate over messages."""
        consumer_name = unique_name()
        stream_name = unique_name()
        topic_name = unique_name()
        partition_id = 0
        test_messages = [f"Consumer group test {i} - {unique_name()}" for i in range(5)]
        received_messages = []

        await iggy_client.create_stream(stream_name)
        await iggy_client.create_topic(
            stream=stream_name,
            name=topic_name,
            partitions_count=1,
        )

        consumer = await iggy_client.consumer_group(
            consumer_name,
            stream_name,
            topic_name,
            partition_id,
            PollingStrategy.Next(),
            10,
            auto_commit=AutoCommit.Interval(timedelta(seconds=5)),
            poll_interval=timedelta(milliseconds=25),
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
        await iggy_client.create_topic(
            stream=stream_name,
            name=topic_name,
            partitions_count=1,
        )

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
            partition_id,
            PollingStrategy.First(),
            10,
            auto_commit=AutoCommit.Disabled(),
            poll_interval=timedelta(milliseconds=25),
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
        await iggy_client.create_topic(
            stream=stream_name,
            name=topic_name,
            partitions_count=1,
        )

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
            partition_id,
            PollingStrategy.Last(),
            1,
            auto_commit=AutoCommit.Disabled(),
            poll_interval=timedelta(milliseconds=25),
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
    async def test_iter_messages_with_last_and_batch_length_two_reads_last_two_messages(
        self, iggy_client: IggyClient, unique_name
    ):
        """Test iter_messages with Last and batch_length=2 reads the last two."""
        consumer_name = unique_name()
        stream_name = unique_name()
        topic_name = unique_name()
        partition_id = 0
        test_messages = [f"Last message test {i} - {unique_name()}" for i in range(4)]
        received_messages = []

        await iggy_client.create_stream(stream_name)
        await iggy_client.create_topic(
            stream=stream_name,
            name=topic_name,
            partitions_count=1,
        )

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
            partition_id,
            PollingStrategy.Last(),
            2,
            auto_commit=AutoCommit.Disabled(),
            poll_interval=timedelta(milliseconds=25),
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
        await iggy_client.create_topic(
            stream=stream_name,
            name=topic_name,
            partitions_count=1,
        )

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
            partition_id,
            PollingStrategy.Last(),
            1,
            auto_commit=AutoCommit.Disabled(),
            poll_interval=timedelta(milliseconds=25),
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
        await iggy_client.create_topic(
            stream=stream_name,
            name=topic_name,
            partitions_count=1,
        )

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
            partition_id,
            PollingStrategy.First(),
            10,
            auto_commit=AutoCommit.Disabled(),
            poll_interval=timedelta(milliseconds=25),
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
            partition_id,
            PollingStrategy.Offset(value=start_offset),
            10,
            auto_commit=AutoCommit.Disabled(),
            poll_interval=timedelta(milliseconds=25),
        )

        iterator = offset_consumer.iter_messages()
        for _ in range(len(test_messages) - 2):
            message = await asyncio.wait_for(iterator.__anext__(), timeout=5)
            offset_messages.append(message)

        assert offset_messages[0].offset() == start_offset
        assert [message.payload().decode() for message in offset_messages] == (
            test_messages[2:]
        )
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
        await iggy_client.create_topic(
            stream=stream_name,
            name=topic_name,
            partitions_count=1,
        )

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
            partition_id,
            PollingStrategy.First(),
            10,
            auto_commit=AutoCommit.Disabled(),
            poll_interval=timedelta(milliseconds=25),
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
            partition_id,
            PollingStrategy.Offset(value=start_offset),
            10,
            auto_commit=AutoCommit.Disabled(),
            poll_interval=timedelta(milliseconds=25),
        )

        async def take(message: ReceiveMessage) -> None:
            offset_messages.append(message)
            if len(offset_messages) == len(test_messages) - 2:
                shutdown_event.set()

        await offset_consumer.consume_messages(take, shutdown_event)

        assert offset_messages[0].offset() == start_offset
        assert [message.payload().decode() for message in offset_messages] == (
            test_messages[2:]
        )
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
        await iggy_client.create_topic(
            stream=stream_name,
            name=topic_name,
            partitions_count=1,
        )

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
            partition_id,
            PollingStrategy.First(),
            10,
            auto_commit=AutoCommit.Disabled(),
            poll_interval=timedelta(milliseconds=25),
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
            partition_id,
            PollingStrategy.Timestamp(value=start_timestamp),
            10,
            auto_commit=AutoCommit.Disabled(),
            poll_interval=timedelta(milliseconds=25),
        )

        iterator = timestamp_consumer.iter_messages()
        for _ in range(len(test_messages) - 1):
            message = await asyncio.wait_for(iterator.__anext__(), timeout=5)
            timestamp_messages.append(message)

        assert timestamp_messages[0].timestamp() >= start_timestamp
        assert [message.payload().decode() for message in timestamp_messages] == (
            test_messages[1:]
        )
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
        await iggy_client.create_topic(
            stream=stream_name,
            name=topic_name,
            partitions_count=1,
        )

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
            partition_id,
            PollingStrategy.First(),
            10,
            auto_commit=AutoCommit.Disabled(),
            poll_interval=timedelta(milliseconds=25),
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
            partition_id,
            PollingStrategy.Timestamp(value=start_timestamp),
            10,
            auto_commit=AutoCommit.Disabled(),
            poll_interval=timedelta(milliseconds=25),
        )

        async def take(message: ReceiveMessage) -> None:
            timestamp_messages.append(message)
            if len(timestamp_messages) == len(test_messages) - 1:
                shutdown_event.set()

        await timestamp_consumer.consume_messages(take, shutdown_event)

        assert timestamp_messages[0].timestamp() >= start_timestamp
        assert [message.payload().decode() for message in timestamp_messages] == (
            test_messages[1:]
        )
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
        await iggy_client.create_topic(
            stream=stream_name,
            name=topic_name,
            partitions_count=1,
        )

        consumer = await iggy_client.consumer_group(
            consumer_name,
            stream_name,
            topic_name,
            partition_id,
            PollingStrategy.First(),
            10,
            auto_commit=AutoCommit.Disabled(),
            poll_interval=timedelta(milliseconds=25),
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
        """Test iter_messages continues across polls when batch_length is small."""
        consumer_name = unique_name()
        stream_name = unique_name()
        topic_name = unique_name()
        partition_id = 0
        test_messages = [f"Batch test {i} - {unique_name()}" for i in range(5)]
        received_messages = []

        await iggy_client.create_stream(stream_name)
        await iggy_client.create_topic(
            stream=stream_name,
            name=topic_name,
            partitions_count=1,
        )

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
            partition_id,
            PollingStrategy.First(),
            2,
            auto_commit=AutoCommit.Disabled(),
            poll_interval=timedelta(milliseconds=25),
        )

        iterator = consumer.iter_messages()
        for _ in range(len(test_messages)):
            message = await asyncio.wait_for(iterator.__anext__(), timeout=5)
            received_messages.append(message.payload().decode())

        assert received_messages == test_messages

    @pytest.mark.asyncio
    @pytest.mark.parametrize("allow_replay", [False, True])
    async def test_store_offset_respects_allow_replay(
        self,
        iggy_client: IggyClient,
        unique_name,
        allow_replay: bool,
    ):
        """Test rewinding offsets only re-consumes when replay is enabled."""
        consumer_name = unique_name()
        stream_name = unique_name()
        topic_name = unique_name()
        partition_id = 0
        test_messages = [f"Replay test {i} - {unique_name()}" for i in range(3)]
        received_messages = []
        replayed_messages = []

        await iggy_client.create_stream(stream_name)
        await iggy_client.create_topic(
            stream=stream_name,
            name=topic_name,
            partitions_count=1,
        )

        consumer = await iggy_client.consumer_group(
            consumer_name,
            stream_name,
            topic_name,
            partition_id,
            PollingStrategy.First(),
            10,
            auto_commit=AutoCommit.Disabled(),
            poll_interval=timedelta(milliseconds=25),
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
        expected_stored_offset = offsets[-2] if allow_replay else offsets[-1]
        assert consumer.get_last_stored_offset(partition_id) == expected_stored_offset

        replay_consumer = await iggy_client.consumer_group(
            consumer_name,
            stream_name,
            topic_name,
            partition_id,
            PollingStrategy.Next(),
            1,
            auto_commit=AutoCommit.Disabled(),
            poll_interval=timedelta(milliseconds=25),
            allow_replay=allow_replay,
        )

        if allow_replay:
            iterator = replay_consumer.iter_messages()
            message = await asyncio.wait_for(iterator.__anext__(), timeout=5)
            replayed_messages.append(message.payload().decode())
            assert replayed_messages == [test_messages[-1]]
        else:
            with pytest.raises(asyncio.TimeoutError):
                await asyncio.wait_for(
                    replay_consumer.iter_messages().__anext__(), timeout=0.2
                )

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
                0,
                PollingStrategy.Next(),
                10,
                auto_commit=AutoCommit.Disabled(),
                poll_interval=timedelta(milliseconds=25),
                init_retries=10,
                init_retry_interval=timedelta(milliseconds=50),
            )
        )

        await asyncio.sleep(timedelta(milliseconds=50).total_seconds() * 2)

        await iggy_client.create_stream(stream_name)
        await iggy_client.create_topic(
            stream=stream_name,
            name=topic_name,
            partitions_count=1,
        )

        consumer = await consumer_task

        assert consumer.name() == consumer_name
        assert consumer.stream() == stream_name
        assert consumer.topic() == topic_name

    @pytest.mark.asyncio
    async def test_consumer_group_can_disable_auto_join(
        self, iggy_client: IggyClient, unique_name
    ):
        """Test initialization can skip automatically joining the group."""
        consumer_name = unique_name()
        stream_name = unique_name()
        topic_name = unique_name()
        partition_id = 0

        await iggy_client.create_stream(stream_name)
        await iggy_client.create_topic(
            stream=stream_name,
            name=topic_name,
            partitions_count=1,
        )

        consumer = await iggy_client.consumer_group(
            consumer_name,
            stream_name,
            topic_name,
            partition_id,
            PollingStrategy.Next(),
            10,
            auto_commit=AutoCommit.Disabled(),
            auto_join_consumer_group=False,
            poll_interval=timedelta(milliseconds=25),
        )

        assert consumer.name() == consumer_name
        assert consumer.stream() == stream_name
        assert consumer.topic() == topic_name
        assert consumer.partition_id() == partition_id

    @pytest.mark.asyncio
    async def test_consumer_group_can_disable_auto_create_when_group_exists(
        self, iggy_client: IggyClient, unique_name
    ):
        """Test disabling auto-create still works for an existing group."""
        consumer_name = unique_name()
        stream_name = unique_name()
        topic_name = unique_name()

        await iggy_client.create_stream(stream_name)
        await iggy_client.create_topic(
            stream=stream_name,
            name=topic_name,
            partitions_count=1,
        )

        await iggy_client.consumer_group(
            consumer_name,
            stream_name,
            topic_name,
            0,
            PollingStrategy.Next(),
            10,
            auto_commit=AutoCommit.Disabled(),
            poll_interval=timedelta(milliseconds=25),
        )

        consumer = await iggy_client.consumer_group(
            consumer_name,
            stream_name,
            topic_name,
            0,
            PollingStrategy.Next(),
            10,
            auto_commit=AutoCommit.Disabled(),
            create_consumer_group_if_not_exists=False,
            poll_interval=timedelta(milliseconds=25),
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
                0,
                PollingStrategy.Next(),
                10,
                auto_commit=AutoCommit.Disabled(),
                poll_interval=timedelta(milliseconds=25),
            )

    @pytest.mark.asyncio
    async def test_consumer_group_auto_create_disabled_fails_for_missing_group(
        self, iggy_client: IggyClient, unique_name
    ):
        """Test auto-create disabled fails for a missing group."""
        stream_name = unique_name()
        topic_name = unique_name()

        await iggy_client.create_stream(stream_name)
        await iggy_client.create_topic(
            stream=stream_name,
            name=topic_name,
            partitions_count=1,
        )

        with pytest.raises(RuntimeError, match="Consumer group with name:"):
            await iggy_client.consumer_group(
                unique_name(),
                stream_name,
                topic_name,
                0,
                PollingStrategy.Next(),
                10,
                auto_commit=AutoCommit.Disabled(),
                create_consumer_group_if_not_exists=False,
                poll_interval=timedelta(milliseconds=25),
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
        await iggy_client.create_topic(
            stream=stream_name,
            name=topic_name,
            partitions_count=1,
        )

        with pytest.raises(RuntimeError, match=expected_error):
            await iggy_client.consumer_group(
                unique_name(),
                stream_name,
                topic_name,
                0,
                PollingStrategy.Next(),
                10,
                auto_commit=AutoCommit.Disabled(),
                poll_interval=timedelta(milliseconds=25),
                **kwargs,
            )

    @pytest.mark.asyncio
    async def test_consumer_group_before_connect_fails(self, unique_name):
        """Test consumer_group requires an established connection."""
        host, port = get_server_config()
        client = IggyClient(server_address=f"{host}:{port}")

        with pytest.raises(RuntimeError):
            await client.consumer_group(
                unique_name(),
                unique_name(),
                unique_name(),
                0,
                PollingStrategy.Next(),
                10,
                auto_commit=AutoCommit.Disabled(),
                poll_interval=timedelta(milliseconds=25),
            )

    @pytest.mark.asyncio
    async def test_consumer_group_before_login_fails(self, unique_name):
        """Test consumer_group requires authentication."""
        host, port = get_server_config()
        wait_for_server(host, port)

        client = IggyClient(server_address=f"{host}:{port}")
        await client.connect()
        await wait_for_ping(client)

        with pytest.raises(RuntimeError):
            await client.consumer_group(
                unique_name(),
                unique_name(),
                unique_name(),
                0,
                PollingStrategy.Next(),
                10,
                auto_commit=AutoCommit.Disabled(),
                poll_interval=timedelta(milliseconds=25),
            )

    @pytest.mark.asyncio
    async def test_shutdown(self, iggy_client: IggyClient, unique_name):
        """Test that the consumer group can be signaled to shutdown."""
        consumer_name = unique_name()
        stream_name = unique_name()
        topic_name = unique_name()
        partition_id = 0
        shutdown_event = asyncio.Event()

        await iggy_client.create_stream(stream_name)
        await iggy_client.create_topic(
            stream=stream_name,
            name=topic_name,
            partitions_count=1,
        )

        consumer = await iggy_client.consumer_group(
            consumer_name,
            stream_name,
            topic_name,
            partition_id,
            PollingStrategy.Next(),
            10,
            auto_commit=AutoCommit.Interval(timedelta(seconds=5)),
            poll_interval=timedelta(milliseconds=25),
        )

        async def take(_: ReceiveMessage) -> None:
            pass

        shutdown_event.set()

        await consumer.consume_messages(take, shutdown_event)
