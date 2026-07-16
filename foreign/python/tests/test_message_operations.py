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
import uuid

import pytest

from apache_iggy import IggyClient, IggyError, PollingStrategy
from apache_iggy import SendMessage as Message


class TestMessageOperations:
    """Test message sending, polling, and processing."""

    @pytest.mark.asyncio
    async def test_send_and_poll_messages(self, iggy_client: IggyClient, unique_name):
        """Test basic message sending and polling."""
        unique_id = unique_name()
        stream_name = unique_name()
        topic_name = unique_name()
        partition_id = 0
        test_messages = [f"Test message {i} - {unique_id}" for i in range(1, 4)]

        await iggy_client.create_stream(stream_name)
        await iggy_client.create_topic(
            stream=stream_name, name=topic_name, partitions_count=1
        )

        messages = [Message(msg) for msg in test_messages]
        await iggy_client.send_messages(
            stream=stream_name,
            topic=topic_name,
            partitioning=partition_id,
            messages=messages,
        )

        polled_messages = await iggy_client.poll_messages(
            stream=stream_name,
            topic=topic_name,
            partition_id=partition_id,
            polling_strategy=PollingStrategy.First(),
            count=10,
            auto_commit=True,
        )

        assert [message.payload().decode() for message in polled_messages] == (
            test_messages
        )

    @pytest.mark.asyncio
    async def test_send_and_poll_messages_as_bytes(
        self, iggy_client: IggyClient, unique_name
    ):
        """Test basic message sending and polling with message payload as bytes."""
        unique_id = unique_name()
        stream_name = unique_name()
        topic_name = unique_name()
        partition_id = 0
        test_messages = [f"Test message {i} - {unique_id}" for i in range(1, 4)]

        await iggy_client.create_stream(stream_name)
        await iggy_client.create_topic(
            stream=stream_name, name=topic_name, partitions_count=1
        )

        messages = [Message(msg.encode()) for msg in test_messages]
        await iggy_client.send_messages(
            stream=stream_name,
            topic=topic_name,
            partitioning=partition_id,
            messages=messages,
        )

        polled_messages = await iggy_client.poll_messages(
            stream=stream_name,
            topic=topic_name,
            partition_id=partition_id,
            polling_strategy=PollingStrategy.First(),
            count=10,
            auto_commit=True,
        )

        assert [message.payload().decode() for message in polled_messages] == (
            test_messages
        )

    @pytest.mark.asyncio
    async def test_message_properties(self, iggy_client: IggyClient, unique_name):
        """Test access to message properties."""
        stream_name = unique_name()
        topic_name = unique_name()
        partition_id = 0

        await iggy_client.create_stream(stream_name)
        await iggy_client.create_topic(
            stream=stream_name, name=topic_name, partitions_count=1
        )

        test_payload = f"Property test - {uuid.uuid4().hex[:8]}"
        message = Message(test_payload)
        await iggy_client.send_messages(
            stream=stream_name,
            topic=topic_name,
            partitioning=partition_id,
            messages=[message],
        )

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

        assert msg.payload().decode("utf-8") == test_payload
        assert isinstance(msg.offset(), int) and msg.offset() >= 0
        assert isinstance(msg.id(), int) and msg.id() > 0
        assert isinstance(msg.timestamp(), int) and msg.timestamp() > 0
        assert isinstance(msg.checksum(), int)
        assert isinstance(msg.length(), int) and msg.length() > 0

    @pytest.mark.asyncio
    @pytest.mark.parametrize(
        "payload",
        ["", b""],
    )
    async def test_empty_payload_is_rejected(self, payload):
        """Test empty string and bytes payloads are rejected."""
        with pytest.raises(IggyError) as exc_info:
            Message(payload)

        assert exc_info.value.name == "invalid_message_payload_length"
        assert exc_info.value.code == 4025
        assert exc_info.value.message == "Invalid message payload length"

    @pytest.mark.asyncio
    @pytest.mark.parametrize(
        "payload",
        ["Zażółć gęślą jaźń", "こんにちは世界", "emoji 😀"],
    )
    async def test_non_ascii_payload_round_trip(
        self, iggy_client: IggyClient, unique_name, payload
    ):
        """Test UTF-8 payloads preserve bytes and decode back to the original text."""
        stream_name = unique_name()
        topic_name = unique_name()
        partition_id = 0
        expected_payload = payload.encode("utf-8")

        await iggy_client.create_stream(stream_name)
        await iggy_client.create_topic(
            stream=stream_name, name=topic_name, partitions_count=1
        )

        await iggy_client.send_messages(
            stream=stream_name,
            topic=topic_name,
            partitioning=partition_id,
            messages=[Message(payload)],
        )

        polled_messages = await iggy_client.poll_messages(
            stream=stream_name,
            topic=topic_name,
            partition_id=partition_id,
            polling_strategy=PollingStrategy.Last(),
            count=1,
            auto_commit=True,
        )

        assert len(polled_messages) == 1
        message = polled_messages[0]
        assert message.payload() == expected_payload
        assert message.payload().decode("utf-8") == payload
        assert message.length() == len(expected_payload)

    @pytest.mark.asyncio
    @pytest.mark.parametrize(
        "payload",
        ["a", "payload-32-bytes-aaaaaaaaaaaaaaaa", "x" * 256],
    )
    async def test_message_length_matches_payload_size(
        self, iggy_client: IggyClient, unique_name, payload
    ):
        """Test message length matches the number of payload bytes."""
        stream_name = unique_name()
        topic_name = unique_name()
        partition_id = 0
        expected_payload = payload.encode("utf-8")

        await iggy_client.create_stream(stream_name)
        await iggy_client.create_topic(
            stream=stream_name, name=topic_name, partitions_count=1
        )

        await iggy_client.send_messages(
            stream=stream_name,
            topic=topic_name,
            partitioning=partition_id,
            messages=[Message(payload)],
        )

        polled_messages = await iggy_client.poll_messages(
            stream=stream_name,
            topic=topic_name,
            partition_id=partition_id,
            polling_strategy=PollingStrategy.Last(),
            count=1,
            auto_commit=True,
        )

        assert len(polled_messages) == 1
        message = polled_messages[0]
        assert message.payload() == expected_payload
        assert message.length() == len(expected_payload)

    @pytest.mark.asyncio
    @pytest.mark.parametrize(
        "payload",
        [b"\x00", b"\x00\x01\x02hello\xff", bytes(range(256))],
    )
    async def test_bytes_payload_preserves_exact_bytes(
        self, iggy_client: IggyClient, unique_name, payload
    ):
        """Test raw bytes payloads are returned unchanged."""
        stream_name = unique_name()
        topic_name = unique_name()
        partition_id = 0

        await iggy_client.create_stream(stream_name)
        await iggy_client.create_topic(
            stream=stream_name, name=topic_name, partitions_count=1
        )

        await iggy_client.send_messages(
            stream=stream_name,
            topic=topic_name,
            partitioning=partition_id,
            messages=[Message(payload)],
        )

        polled_messages = await iggy_client.poll_messages(
            stream=stream_name,
            topic=topic_name,
            partition_id=partition_id,
            polling_strategy=PollingStrategy.Last(),
            count=1,
            auto_commit=True,
        )

        assert len(polled_messages) == 1
        message = polled_messages[0]
        assert message.payload() == payload
        assert message.length() == len(payload)

    @pytest.mark.asyncio
    async def test_poll_messages_with_count_one_returns_one_message(
        self, iggy_client: IggyClient, unique_name
    ):
        """Test count=1 returns only a single message when more are available."""
        stream_name = unique_name()
        topic_name = unique_name()
        partition_id = 0
        test_messages = [f"Count test {i} - {unique_name()}" for i in range(3)]

        await iggy_client.create_stream(stream_name)
        await iggy_client.create_topic(
            stream=stream_name, name=topic_name, partitions_count=1
        )

        await iggy_client.send_messages(
            stream=stream_name,
            topic=topic_name,
            partitioning=partition_id,
            messages=[Message(message) for message in test_messages],
        )

        polled_messages = await iggy_client.poll_messages(
            stream=stream_name,
            topic=topic_name,
            partition_id=partition_id,
            polling_strategy=PollingStrategy.First(),
            count=1,
            auto_commit=False,
        )

        assert len(polled_messages) == 1
        assert polled_messages[0].payload().decode("utf-8") == test_messages[0]

    @pytest.mark.asyncio
    async def test_poll_messages_with_large_count_returns_all_available_messages(
        self, iggy_client: IggyClient, unique_name
    ):
        """Test count larger than available returns all messages without error."""
        stream_name = unique_name()
        topic_name = unique_name()
        partition_id = 0
        test_messages = [f"Count test {i} - {unique_name()}" for i in range(3)]

        await iggy_client.create_stream(stream_name)
        await iggy_client.create_topic(
            stream=stream_name, name=topic_name, partitions_count=1
        )

        await iggy_client.send_messages(
            stream=stream_name,
            topic=topic_name,
            partitioning=partition_id,
            messages=[Message(message) for message in test_messages],
        )

        polled_messages = await iggy_client.poll_messages(
            stream=stream_name,
            topic=topic_name,
            partition_id=partition_id,
            polling_strategy=PollingStrategy.First(),
            count=10,
            auto_commit=False,
        )

        assert len(polled_messages) == len(test_messages)
        assert [
            message.payload().decode("utf-8") for message in polled_messages
        ] == test_messages

    @pytest.mark.asyncio
    async def test_poll_messages_with_count_zero_is_rejected(
        self, iggy_client: IggyClient, unique_name
    ):
        """Test count=0 is rejected with the expected runtime error."""
        stream_name = unique_name()
        topic_name = unique_name()
        partition_id = 0
        test_messages = [f"Count test {i} - {unique_name()}" for i in range(3)]

        await iggy_client.create_stream(stream_name)
        await iggy_client.create_topic(
            stream=stream_name, name=topic_name, partitions_count=1
        )

        await iggy_client.send_messages(
            stream=stream_name,
            topic=topic_name,
            partitioning=partition_id,
            messages=[Message(message) for message in test_messages],
        )

        with pytest.raises(IggyError) as exc_info:
            await iggy_client.poll_messages(
                stream=stream_name,
                topic=topic_name,
                partition_id=partition_id,
                polling_strategy=PollingStrategy.First(),
                count=0,
                auto_commit=False,
            )

        assert exc_info.value.name == "invalid_messages_count"
        assert exc_info.value.code == 4009
        assert exc_info.value.message == "Invalid messages count"

    @pytest.mark.asyncio
    async def test_poll_messages_with_invalid_partition_id_raises(
        self, iggy_client: IggyClient, unique_name
    ):
        """Test polling a missing partition raises the expected runtime error."""
        stream_name = unique_name()
        topic_name = unique_name()
        partition_id = 0

        await iggy_client.create_stream(stream_name)
        await iggy_client.create_topic(
            stream=stream_name, name=topic_name, partitions_count=1
        )

        await iggy_client.send_messages(
            stream=stream_name,
            topic=topic_name,
            partitioning=partition_id,
            messages=[Message(f"Partition test - {unique_name()}")],
        )

        with pytest.raises(IggyError) as exc_info:
            await iggy_client.poll_messages(
                stream=stream_name,
                topic=topic_name,
                partition_id=1,
                polling_strategy=PollingStrategy.First(),
                count=1,
                auto_commit=False,
            )

        assert exc_info.value.name == "partition_not_found"
        assert exc_info.value.code == 3007
        assert exc_info.value.message.startswith("Partition with ID:")

    @pytest.mark.asyncio
    async def test_polling_strategy_last_with_count_one_returns_last_message(
        self, iggy_client: IggyClient, unique_name
    ):
        """Test Last with count=1 returns exactly the last message."""
        stream_name = unique_name()
        topic_name = unique_name()
        partition_id = 0
        test_messages = [f"Polling test {i} - {unique_name()}" for i in range(5)]

        await iggy_client.create_stream(stream_name)
        await iggy_client.create_topic(
            stream=stream_name, name=topic_name, partitions_count=1
        )

        messages = [Message(msg) for msg in test_messages]
        await iggy_client.send_messages(
            stream=stream_name,
            topic=topic_name,
            partitioning=partition_id,
            messages=messages,
        )

        last_messages = await iggy_client.poll_messages(
            stream=stream_name,
            topic=topic_name,
            partition_id=partition_id,
            polling_strategy=PollingStrategy.Last(),
            count=1,
            auto_commit=False,
        )
        assert len(last_messages) == 1
        assert last_messages[0].payload().decode("utf-8") == test_messages[-1]

    @pytest.mark.asyncio
    async def test_polling_strategy_last_with_count_two_returns_last_two_messages(
        self, iggy_client: IggyClient, unique_name
    ):
        """Test Last with count=2 returns the last two messages in order."""
        stream_name = unique_name()
        topic_name = unique_name()
        partition_id = 0
        test_messages = [f"Polling test {i} - {unique_name()}" for i in range(5)]

        await iggy_client.create_stream(stream_name)
        await iggy_client.create_topic(
            stream=stream_name, name=topic_name, partitions_count=1
        )

        messages = [Message(msg) for msg in test_messages]
        await iggy_client.send_messages(
            stream=stream_name,
            topic=topic_name,
            partitioning=partition_id,
            messages=messages,
        )

        last_messages = await iggy_client.poll_messages(
            stream=stream_name,
            topic=topic_name,
            partition_id=partition_id,
            polling_strategy=PollingStrategy.Last(),
            count=2,
            auto_commit=False,
        )
        assert len(last_messages) == 2
        assert [
            message.payload().decode("utf-8") for message in last_messages
        ] == test_messages[-2:]

    @pytest.mark.asyncio
    async def test_polling_strategy_offset_starts_at_exact_message(
        self, iggy_client: IggyClient, unique_name
    ):
        """Test Offset starts polling exactly from the requested message offset."""
        stream_name = unique_name()
        topic_name = unique_name()
        partition_id = 0
        test_messages = [f"Polling test {i} - {unique_name()}" for i in range(5)]

        await iggy_client.create_stream(stream_name)
        await iggy_client.create_topic(
            stream=stream_name, name=topic_name, partitions_count=1
        )

        messages = [Message(msg) for msg in test_messages]
        await iggy_client.send_messages(
            stream=stream_name,
            topic=topic_name,
            partitioning=partition_id,
            messages=messages,
        )

        first_messages = await iggy_client.poll_messages(
            stream=stream_name,
            topic=topic_name,
            partition_id=partition_id,
            polling_strategy=PollingStrategy.First(),
            count=10,
            auto_commit=False,
        )
        start_offset = first_messages[2].offset()

        offset_messages = await iggy_client.poll_messages(
            stream=stream_name,
            topic=topic_name,
            partition_id=partition_id,
            polling_strategy=PollingStrategy.Offset(value=start_offset),
            count=10,
            auto_commit=False,
        )
        assert len(offset_messages) == len(test_messages[2:])
        assert offset_messages[0].offset() == start_offset
        assert [
            message.payload().decode("utf-8") for message in offset_messages
        ] == test_messages[2:]

    @pytest.mark.asyncio
    async def test_polling_strategy_offset_beyond_newest_returns_no_messages(
        self, iggy_client: IggyClient, unique_name
    ):
        """Test Offset beyond the newest message returns an empty result."""
        stream_name = unique_name()
        topic_name = unique_name()
        partition_id = 0
        test_messages = [f"Polling test {i} - {unique_name()}" for i in range(3)]

        await iggy_client.create_stream(stream_name)
        await iggy_client.create_topic(
            stream=stream_name, name=topic_name, partitions_count=1
        )

        await iggy_client.send_messages(
            stream=stream_name,
            topic=topic_name,
            partitioning=partition_id,
            messages=[Message(msg) for msg in test_messages],
        )

        first_messages = await iggy_client.poll_messages(
            stream=stream_name,
            topic=topic_name,
            partition_id=partition_id,
            polling_strategy=PollingStrategy.First(),
            count=10,
            auto_commit=False,
        )
        offset_beyond_newest = first_messages[-1].offset() + 1

        offset_messages = await iggy_client.poll_messages(
            stream=stream_name,
            topic=topic_name,
            partition_id=partition_id,
            polling_strategy=PollingStrategy.Offset(value=offset_beyond_newest),
            count=10,
            auto_commit=False,
        )

        assert offset_messages == []

    @pytest.mark.asyncio
    async def test_polling_strategy_timestamp_starts_at_or_after_timestamp(
        self, iggy_client: IggyClient, unique_name
    ):
        """Test Timestamp starts at the first message on or after the timestamp."""
        stream_name = unique_name()
        topic_name = unique_name()
        partition_id = 0
        test_messages = [f"Polling test {i} - {unique_name()}" for i in range(5)]

        await iggy_client.create_stream(stream_name)
        await iggy_client.create_topic(
            stream=stream_name, name=topic_name, partitions_count=1
        )

        for message in test_messages:
            await iggy_client.send_messages(
                stream=stream_name,
                topic=topic_name,
                partitioning=partition_id,
                messages=[Message(message)],
            )
            await asyncio.sleep(0.01)

        first_messages = await iggy_client.poll_messages(
            stream=stream_name,
            topic=topic_name,
            partition_id=partition_id,
            polling_strategy=PollingStrategy.First(),
            count=10,
            auto_commit=False,
        )
        start_timestamp = first_messages[2].timestamp()

        timestamp_messages = await iggy_client.poll_messages(
            stream=stream_name,
            topic=topic_name,
            partition_id=partition_id,
            polling_strategy=PollingStrategy.Timestamp(value=start_timestamp),
            count=10,
            auto_commit=False,
        )
        assert len(timestamp_messages) == len(test_messages[2:])
        assert timestamp_messages[0].timestamp() >= start_timestamp
        assert [
            message.payload().decode("utf-8") for message in timestamp_messages
        ] == test_messages[2:]

    @pytest.mark.asyncio
    async def test_polling_strategy_timestamp_after_newest_returns_no_messages(
        self, iggy_client: IggyClient, unique_name
    ):
        """Test Timestamp after the newest message returns an empty result."""
        stream_name = unique_name()
        topic_name = unique_name()
        partition_id = 0
        test_messages = [f"Polling test {i} - {unique_name()}" for i in range(3)]

        await iggy_client.create_stream(stream_name)
        await iggy_client.create_topic(
            stream=stream_name, name=topic_name, partitions_count=1
        )

        for message in test_messages:
            await iggy_client.send_messages(
                stream=stream_name,
                topic=topic_name,
                partitioning=partition_id,
                messages=[Message(message)],
            )
            await asyncio.sleep(0.01)

        first_messages = await iggy_client.poll_messages(
            stream=stream_name,
            topic=topic_name,
            partition_id=partition_id,
            polling_strategy=PollingStrategy.First(),
            count=10,
            auto_commit=False,
        )
        timestamp_after_newest = first_messages[-1].timestamp() + 1

        timestamp_messages = await iggy_client.poll_messages(
            stream=stream_name,
            topic=topic_name,
            partition_id=partition_id,
            polling_strategy=PollingStrategy.Timestamp(value=timestamp_after_newest),
            count=10,
            auto_commit=False,
        )

        assert timestamp_messages == []

    @pytest.mark.asyncio
    async def test_poll_messages_with_auto_commit_true_advances_next(
        self, iggy_client: IggyClient, unique_name
    ):
        """Test auto_commit=True stores progress so Next resumes after it."""
        stream_name = unique_name()
        topic_name = unique_name()
        partition_id = 0
        existing_messages = [
            f"Existing polling test {i} - {unique_name()}" for i in range(2)
        ]
        new_messages = [f"Polling test {i} - {unique_name()}" for i in range(2)]

        await iggy_client.create_stream(stream_name)
        await iggy_client.create_topic(
            stream=stream_name, name=topic_name, partitions_count=1
        )

        await iggy_client.send_messages(
            stream=stream_name,
            topic=topic_name,
            partitioning=partition_id,
            messages=[Message(message) for message in existing_messages],
        )

        first_messages = await iggy_client.poll_messages(
            stream=stream_name,
            topic=topic_name,
            partition_id=partition_id,
            polling_strategy=PollingStrategy.First(),
            count=10,
            auto_commit=True,
        )
        assert [
            message.payload().decode("utf-8")
            for message in first_messages[: len(existing_messages)]
        ] == existing_messages

        await iggy_client.send_messages(
            stream=stream_name,
            topic=topic_name,
            partitioning=partition_id,
            messages=[Message(message) for message in new_messages],
        )

        next_messages = await iggy_client.poll_messages(
            stream=stream_name,
            topic=topic_name,
            partition_id=partition_id,
            polling_strategy=PollingStrategy.Next(),
            count=10,
            auto_commit=True,
        )
        assert len(next_messages) == len(new_messages)
        assert [
            message.payload().decode("utf-8") for message in next_messages
        ] == new_messages

    @pytest.mark.asyncio
    async def test_poll_messages_with_auto_commit_false_does_not_advance_next(
        self, iggy_client: IggyClient, unique_name
    ):
        """Test auto_commit=False leaves no stored offset, so Next starts over."""
        stream_name = unique_name()
        topic_name = unique_name()
        partition_id = 0
        existing_messages = [
            f"Existing polling test {i} - {unique_name()}" for i in range(2)
        ]
        new_messages = [f"Polling test {i} - {unique_name()}" for i in range(2)]

        await iggy_client.create_stream(stream_name)
        await iggy_client.create_topic(
            stream=stream_name, name=topic_name, partitions_count=1
        )

        await iggy_client.send_messages(
            stream=stream_name,
            topic=topic_name,
            partitioning=partition_id,
            messages=[Message(message) for message in existing_messages],
        )

        first_messages = await iggy_client.poll_messages(
            stream=stream_name,
            topic=topic_name,
            partition_id=partition_id,
            polling_strategy=PollingStrategy.First(),
            count=10,
            auto_commit=False,
        )
        assert [
            message.payload().decode("utf-8")
            for message in first_messages[: len(existing_messages)]
        ] == existing_messages

        await iggy_client.send_messages(
            stream=stream_name,
            topic=topic_name,
            partitioning=partition_id,
            messages=[Message(message) for message in new_messages],
        )

        next_messages = await iggy_client.poll_messages(
            stream=stream_name,
            topic=topic_name,
            partition_id=partition_id,
            polling_strategy=PollingStrategy.Next(),
            count=10,
            auto_commit=False,
        )
        assert len(next_messages) == len(existing_messages) + len(new_messages)
        assert [
            message.payload().decode("utf-8") for message in next_messages
        ] == existing_messages + new_messages
