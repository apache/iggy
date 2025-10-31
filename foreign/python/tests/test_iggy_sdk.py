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

"""
Comprehensive test suite for the Python SDK.

This module contains all the tests for the Iggy Python SDK, organized by functionality.
Tests are marked as either 'unit' or 'integration' based on their requirements.
"""

import asyncio
import uuid
from datetime import timedelta

import pytest
from apache_iggy import AutoCommit, IggyClient, PollingStrategy, ReceiveMessage
from apache_iggy import SendMessage as Message

from .utils import get_server_config, wait_for_ping, wait_for_server


class TestConnectivity:
    """Test basic connectivity and authentication."""

    @pytest.mark.asyncio
    async def test_ping(self, iggy_client: IggyClient):
        """Test server ping functionality."""
        await iggy_client.ping()

    @pytest.mark.asyncio
    async def test_client_not_none(self, iggy_client: IggyClient):
        """Test that client fixture is properly initialized."""
        assert iggy_client is not None

    @pytest.mark.asyncio
    async def test_client_from_connection_string(self):
        """Test that client can be created and set up with a connection string."""
        host, port = get_server_config()
        wait_for_server(host, port)

        client = IggyClient.from_connection_string(
            f"iggy+tcp://iggy:iggy@{host}:{port}"
        )
        await client.connect()
        await wait_for_ping(client)


class TestStreamOperations:
    """Test stream creation, retrieval, and management."""

    @pytest.fixture
    def unique_stream_name(self):
        """Generate unique stream name for each test."""
        return f"test-stream-{uuid.uuid4().hex[:8]}"

    @pytest.mark.asyncio
    async def test_create_and_get_stream(
        self, iggy_client: IggyClient, unique_stream_name
    ):
        """Test stream creation and retrieval."""
        # Create stream
        await iggy_client.create_stream(unique_stream_name)

        # Get stream by name
        stream = await iggy_client.get_stream(unique_stream_name)
        assert stream is not None
        assert stream.name == unique_stream_name
        assert stream.id >= 0

    @pytest.mark.asyncio
    async def test_list_streams(self, iggy_client: IggyClient, unique_stream_name):
        """Test listing streams."""
        # Create a stream first
        await iggy_client.create_stream(unique_stream_name)

        # Get the stream we just created
        stream = await iggy_client.get_stream(unique_stream_name)
        assert stream is not None
        assert stream.name == unique_stream_name

        assert stream.id > 0
        assert stream.topics_count == 0  # New stream has no topics


class TestTopicOperations:
    """Test topic creation, retrieval, and management."""

    @pytest.fixture
    def unique_names(self):
        """Generate unique stream and topic names."""
        unique_id = uuid.uuid4().hex[:8]
        return {
            "stream": f"test-stream-{unique_id}",
            "topic": f"test-topic-{unique_id}",
        }

    @pytest.mark.asyncio
    async def test_create_and_get_topic(self, iggy_client: IggyClient, unique_names):
        """Test topic creation and retrieval."""
        stream_name = unique_names["stream"]
        topic_name = unique_names["topic"]

        # Create stream first
        await iggy_client.create_stream(stream_name)

        # Create topic
        await iggy_client.create_topic(
            stream=stream_name, name=topic_name, partitions_count=2
        )

        # Get topic by name
        topic = await iggy_client.get_topic(stream_name, topic_name)
        assert topic is not None
        assert topic.name == topic_name
        assert topic.id >= 0
        assert topic.partitions_count == 2

    @pytest.mark.asyncio
    async def test_list_topics(self, iggy_client: IggyClient, unique_names):
        """Test listing topics in a stream."""
        stream_name = unique_names["stream"]
        topic_name = unique_names["topic"]

        # Create stream and topic
        await iggy_client.create_stream(stream_name)
        await iggy_client.create_topic(
            stream=stream_name, name=topic_name, partitions_count=1
        )

        # Get the topic we just created
        topic = await iggy_client.get_topic(stream_name, topic_name)
        assert topic is not None
        assert topic.name == topic_name
        assert topic.id >= 0
        assert topic.partitions_count == 1


class TestMessageOperations:
    """Test message sending, polling, and processing."""

    @pytest.fixture
    def message_setup(self):
        """Setup unique names and test data for messaging tests."""
        unique_id = uuid.uuid4().hex[:8]
        return {
            "stream": f"msg-stream-{unique_id}",
            "topic": f"msg-topic-{unique_id}",
            "partition_id": 0,
            "messages": [f"Test message {i} - {unique_id}" for i in range(1, 4)],
        }

    @pytest.mark.asyncio
    async def test_send_and_poll_messages(self, iggy_client: IggyClient, message_setup):
        """Test basic message sending and polling."""
        stream_name = message_setup["stream"]
        topic_name = message_setup["topic"]
        partition_id = message_setup["partition_id"]
        test_messages = message_setup["messages"]

        # Setup stream and topic
        await iggy_client.create_stream(stream_name)
        await iggy_client.create_topic(
            stream=stream_name, name=topic_name, partitions_count=1
        )

        # Send messages
        messages = [Message(msg) for msg in test_messages]
        await iggy_client.send_messages(
            stream=stream_name,
            topic=topic_name,
            partitioning=partition_id,
            messages=messages,
        )

        # Poll messages
        polled_messages = await iggy_client.poll_messages(
            stream=stream_name,
            topic=topic_name,
            partition_id=partition_id,
            polling_strategy=PollingStrategy.First(),
            count=10,
            auto_commit=True,
        )

        # Verify we got our messages
        assert len(polled_messages) >= len(test_messages)

        # Check the first few messages match what we sent
        for i, expected_msg in enumerate(test_messages):
            if i < len(polled_messages):
                actual_payload = polled_messages[i].payload().decode("utf-8")
                assert actual_payload == expected_msg

    @pytest.mark.asyncio
    async def test_send_and_poll_messages_as_bytes(
        self, iggy_client: IggyClient, message_setup
    ):
        """Test basic message sending and polling with message payload as bytes."""
        stream_name = message_setup["stream"]
        topic_name = message_setup["topic"]
        partition_id = message_setup["partition_id"]
        test_messages = message_setup["messages"]

        # Setup stream and topic
        await iggy_client.create_stream(stream_name)
        await iggy_client.create_topic(
            stream=stream_name, name=topic_name, partitions_count=1
        )

        # Send messages
        messages = [Message(msg.encode()) for msg in test_messages]
        await iggy_client.send_messages(
            stream=stream_name,
            topic=topic_name,
            partitioning=partition_id,
            messages=messages,
        )

        # Poll messages
        polled_messages = await iggy_client.poll_messages(
            stream=stream_name,
            topic=topic_name,
            partition_id=partition_id,
            polling_strategy=PollingStrategy.First(),
            count=10,
            auto_commit=True,
        )

        # Verify we got our messages
        assert len(polled_messages) >= len(test_messages)

        # Check the first few messages match what we sent
        for i, expected_msg in enumerate(test_messages):
            if i < len(polled_messages):
                actual_payload = polled_messages[i].payload().decode("utf-8")
                assert actual_payload == expected_msg

    @pytest.mark.asyncio
    async def test_message_properties(self, iggy_client: IggyClient, message_setup):
        """Test access to message properties."""
        stream_name = message_setup["stream"]
        topic_name = message_setup["topic"]
        partition_id = message_setup["partition_id"]

        # Setup
        await iggy_client.create_stream(stream_name)
        await iggy_client.create_topic(
            stream=stream_name, name=topic_name, partitions_count=1
        )

        # Send a test message
        test_payload = f"Property test - {uuid.uuid4().hex[:8]}"
        message = Message(test_payload)
        await iggy_client.send_messages(
            stream=stream_name,
            topic=topic_name,
            partitioning=partition_id,
            messages=[message],
        )

        # Poll and verify properties
        polled_messages = await iggy_client.poll_messages(
            stream=stream_name,
            topic=topic_name,
            partition_id=partition_id,
            polling_strategy=PollingStrategy.Last(),
            count=1,
            auto_commit=True,
        )

        assert len(polled_messages) >= 1
        msg = polled_messages[0]

        # Test all message properties are accessible and have reasonable values
        assert msg.payload().decode("utf-8") == test_payload
        assert isinstance(msg.offset(), int) and msg.offset() >= 0
        assert isinstance(msg.id(), int) and msg.id() > 0
        assert isinstance(msg.timestamp(), int) and msg.timestamp() > 0
        assert isinstance(msg.checksum(), int)
        assert isinstance(msg.length(), int) and msg.length() > 0


class TestPollingStrategies:
    """Test different polling strategies."""

    @pytest.fixture
    def polling_setup(self):
        """Setup for polling strategy tests."""
        unique_id = uuid.uuid4().hex[:8]
        return {
            "stream": f"poll-stream-{unique_id}",
            "topic": f"poll-topic-{unique_id}",
            "partition_id": 0,
            "messages": [f"Polling test {i} - {unique_id}" for i in range(5)],
        }

    @pytest.mark.asyncio
    async def test_polling_strategies(self, iggy_client: IggyClient, polling_setup):
        """Test different polling strategies work correctly."""
        stream_name = polling_setup["stream"]
        topic_name = polling_setup["topic"]
        partition_id = polling_setup["partition_id"]
        test_messages = polling_setup["messages"]

        # Setup
        await iggy_client.create_stream(stream_name)
        await iggy_client.create_topic(
            stream=stream_name, name=topic_name, partitions_count=1
        )

        # Send test messages
        messages = [Message(msg) for msg in test_messages]
        await iggy_client.send_messages(
            stream=stream_name,
            topic=topic_name,
            partitioning=partition_id,
            messages=messages,
        )

        # Test First strategy
        first_messages = await iggy_client.poll_messages(
            stream=stream_name,
            topic=topic_name,
            partition_id=partition_id,
            polling_strategy=PollingStrategy.First(),
            count=1,
            auto_commit=False,
        )
        assert len(first_messages) >= 1

        # Test Last strategy
        last_messages = await iggy_client.poll_messages(
            stream=stream_name,
            topic=topic_name,
            partition_id=partition_id,
            polling_strategy=PollingStrategy.Last(),
            count=1,
            auto_commit=False,
        )
        assert len(last_messages) >= 1

        # Test Next strategy
        next_messages = await iggy_client.poll_messages(
            stream=stream_name,
            topic=topic_name,
            partition_id=partition_id,
            polling_strategy=PollingStrategy.Next(),
            count=2,
            auto_commit=False,
        )
        assert len(next_messages) >= 1

        # Test Offset strategy (if we have messages)
        if first_messages:
            offset_messages = await iggy_client.poll_messages(
                stream=stream_name,
                topic=topic_name,
                partition_id=partition_id,
                polling_strategy=PollingStrategy.Offset(
                    value=first_messages[0].offset()
                ),
                count=1,
                auto_commit=False,
            )
            assert len(offset_messages) >= 1


class TestErrorHandling:
    """Test error handling and edge cases."""

    @pytest.mark.asyncio
    async def test_duplicate_stream_creation(self, iggy_client: IggyClient):
        """Test that creating duplicate streams raises appropriate errors."""
        stream_name = f"duplicate-test-{uuid.uuid4().hex[:8]}"

        # Create stream first time - should succeed
        await iggy_client.create_stream(stream_name)

        # Create same stream again - should fail
        with pytest.raises(RuntimeError) as exc_info:
            await iggy_client.create_stream(stream_name)

        assert "StreamNameAlreadyExists" in str(exc_info.value)

    @pytest.mark.asyncio
    async def test_get_nonexistent_stream(self, iggy_client: IggyClient):
        """Test getting a non-existent stream."""
        nonexistent_name = f"nonexistent-{uuid.uuid4().hex}"

        # get_stream returns None for non-existent streams
        stream = await iggy_client.get_stream(nonexistent_name)
        assert stream is None

    @pytest.mark.asyncio
    async def test_create_topic_in_nonexistent_stream(self, iggy_client: IggyClient):
        """Test creating a topic in a non-existent stream."""
        nonexistent_stream = f"nonexistent-{uuid.uuid4().hex}"
        topic_name = "test-topic"

        with pytest.raises(RuntimeError):
            await iggy_client.create_topic(
                stream=nonexistent_stream, name=topic_name, partitions_count=1
            )


class TestConsumerGroup:
    """Test consumer groups."""

    @pytest.fixture(scope="function")
    def consumer_group_setup(self):
        """Setup for polling strategy tests."""
        unique_id = uuid.uuid4().hex[:8]
        return {
            "consumer": f"consumer-group-consumer-{unique_id}",
            "stream": f"consumer-group-stream-{unique_id}",
            "topic": f"consumer-group-topic-{unique_id}",
            "partition_id": 0,
            "messages": [f"Consumer group test {i} - {unique_id}" for i in range(5)],
        }

    @pytest.mark.asyncio
    async def test_meta(self, iggy_client: IggyClient, consumer_group_setup):
        """Test that meta information can be read about the consumer group."""
        consumer_name = consumer_group_setup["consumer"]
        stream_name = consumer_group_setup["stream"]
        topic_name = consumer_group_setup["topic"]
        partition_id = consumer_group_setup["partition_id"]

        # Setup
        await iggy_client.create_stream(stream_name)
        await iggy_client.create_topic(
            stream=stream_name, name=topic_name, partitions_count=1
        )
        consumer = iggy_client.consumer_group(
            consumer_name,
            stream_name,
            topic_name,
            partition_id,
            PollingStrategy.Next(),
            10,
            auto_commit=AutoCommit.Interval(timedelta(seconds=5)),
            poll_interval=timedelta(seconds=1),
        )

        assert consumer.stream() == stream_name
        assert consumer.topic() == topic_name
        # This internally loads `current_partition_id` which is set to 0 until you start consuming
        assert consumer.partition_id() == 0
        assert consumer.get_last_consumed_offset(partition_id) is None
        assert consumer.get_last_stored_offset(partition_id) is None

    @pytest.mark.asyncio
    async def test_consume_messages(
        self, iggy_client: IggyClient, consumer_group_setup
    ):
        """Test that the consumer group can consume messages."""
        consumer_name = consumer_group_setup["consumer"]
        stream_name = consumer_group_setup["stream"]
        topic_name = consumer_group_setup["topic"]
        partition_id = consumer_group_setup["partition_id"]
        test_messages = consumer_group_setup["messages"]

        # Setup
        received_messages = []
        shutdown_event = asyncio.Event()
        await iggy_client.create_stream(stream_name)
        await iggy_client.create_topic(
            stream=stream_name, name=topic_name, partitions_count=1
        )

        consumer = iggy_client.consumer_group(
            consumer_name,
            stream_name,
            topic_name,
            partition_id,
            PollingStrategy.Next(),
            10,
            auto_commit=AutoCommit.Interval(timedelta(seconds=5)),
            poll_interval=timedelta(seconds=1),
        )

        async def take(message: ReceiveMessage) -> None:
            received_messages.append(message.payload().decode())
            if len(received_messages) == 5:
                shutdown_event.set()

        async def send() -> None:
            await iggy_client.send_messages(
                stream_name,
                topic_name,
                partition_id,
                [Message(m) for m in test_messages],
            )

        await asyncio.gather(consumer.consume_messages(take, shutdown_event), send())

        assert received_messages == test_messages

    @pytest.mark.asyncio
    async def test_shutdown(self, iggy_client: IggyClient, consumer_group_setup):
        """Test that the consumer group can be signaled to shutdown."""
        consumer_name = consumer_group_setup["consumer"]
        stream_name = consumer_group_setup["stream"]
        topic_name = consumer_group_setup["topic"]
        partition_id = consumer_group_setup["partition_id"]

        # Setup
        shutdown_event = asyncio.Event()
        await iggy_client.create_stream(stream_name)
        await iggy_client.create_topic(
            stream=stream_name, name=topic_name, partitions_count=1
        )

        consumer = iggy_client.consumer_group(
            consumer_name,
            stream_name,
            topic_name,
            partition_id,
            PollingStrategy.Next(),
            10,
            auto_commit=AutoCommit.Interval(timedelta(seconds=5)),
            poll_interval=timedelta(seconds=1),
        )

        async def take(_) -> None:
            pass

        shutdown_event.set()

        await consumer.consume_messages(take, shutdown_event)
