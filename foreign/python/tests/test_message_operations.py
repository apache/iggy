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
import contextlib
import time
import uuid

import pytest

from apache_iggy import (
    AutoCommit,
    CommitPolicy,
    Consumer,
    HeaderKey,
    HeaderValue,
    IggyClient,
    IggyError,
    PartitionContext,
    Partitioning,
    PollingStrategy,
    PollOptions,
    TopicOptions,
    UserHeaders,
)
from apache_iggy import SendMessage as Message

from .utils import wait_for_consumer_group_assignment

INVALID_CONFIGURATION = 2
INVALID_COMMAND = 3
CONSUMER_OFFSET_NOT_FOUND = 3021
HISTORY_UNAVAILABLE = 87
CONSUMER_GROUP_PARTITION_NOT_OWNED = 5009
# Far longer than a local reply takes.
REPLY_WAIT_SECONDS = 1


class TestPartitioning:
    """Test message partitioning strategy construction."""

    @pytest.mark.unit
    def test_balanced_and_partition_id_strategies(self):
        assert isinstance(Partitioning.balanced(), Partitioning)
        assert isinstance(Partitioning.partition_id(1), Partitioning)
        assert isinstance(Partitioning.partition_id(2**32 - 1), Partitioning)

    @pytest.mark.unit
    @pytest.mark.parametrize("key", [b"customer-42", "customer-42", "客户-42"])
    def test_messages_key_accepts_bytes_and_strings(self, key):
        assert isinstance(Partitioning.messages_key(key), Partitioning)

    @pytest.mark.unit
    @pytest.mark.parametrize("key", [b"a" * 255, "a" * 255, "界" * 85])
    def test_messages_key_accepts_255_bytes(self, key):
        assert isinstance(Partitioning.messages_key(key), Partitioning)

    @pytest.mark.unit
    @pytest.mark.parametrize("key", [b"", "", b"a" * 256, "a" * 256, "界" * 86])
    def test_messages_key_rejects_invalid_encoded_length(self, key):
        with pytest.raises(ValueError):
            Partitioning.messages_key(key)

    @pytest.mark.unit
    @pytest.mark.parametrize("partition_id", [-1, 2**32])
    def test_partition_id_rejects_values_outside_u32(self, partition_id):
        with pytest.raises(OverflowError):
            Partitioning.partition_id(partition_id)

    @pytest.mark.unit
    def test_partitioning_rejects_invalid_types(self):
        with pytest.raises(TypeError):
            # pyrefly: ignore  # bad-argument-type
            Partitioning.partition_id("0")
        with pytest.raises(TypeError):
            # pyrefly: ignore  # bad-argument-type
            Partitioning.messages_key(1)


class TestPartitionContext:
    """Test the context, checkpoint and continuation values that fence polls."""

    @pytest.mark.unit
    def test_context_is_a_comparable_value(self):
        context = PartitionContext(incarnation=7, owner_generation=11, metadata_op=13)

        assert context.incarnation == 7
        assert context.owner_generation == 11
        assert context.metadata_op == 13
        assert repr(context) == (
            "PartitionContext(incarnation=7, owner_generation=11, metadata_op=13)"
        )

        equal_context = PartitionContext(7, 11, 13)
        assert context == equal_context
        assert hash(context) == hash(equal_context)
        assert context != PartitionContext(8, 11, 13)

    @pytest.mark.asyncio
    async def test_checkpoint_is_a_comparable_value(
        self, iggy_client: IggyClient, unique_name
    ):
        """Test a checkpoint names its message and hashes as it compares."""
        stream_name = unique_name()
        topic_name = unique_name()
        partition_id = 0

        await iggy_client.create_stream(stream_name)
        await iggy_client.create_topic(stream=stream_name, name=topic_name)
        await iggy_client.send_messages(
            stream=stream_name,
            topic=topic_name,
            partitioning=partition_id,
            messages=[Message("first"), Message("second")],
        )

        polled_messages = await iggy_client.poll_messages(
            stream=stream_name,
            topic=topic_name,
            consumer=Consumer.Single(1),
            partition_id=partition_id,
            polling_strategy=PollingStrategy.First(),
            options=PollOptions(count=10),
        )

        first, second = polled_messages.messages
        checkpoint = first.checkpoint()
        assert checkpoint.partition_id == partition_id
        assert checkpoint.offset == first.offset()
        assert repr(checkpoint) == (
            f"Checkpoint(partition_id={partition_id}, offset={first.offset()})"
        )
        assert checkpoint == polled_messages.checkpoint_at(0)
        assert hash(checkpoint) == hash(polled_messages.checkpoint_at(0))
        assert checkpoint != second.checkpoint()
        assert {checkpoint, polled_messages.checkpoint_at(0), second.checkpoint()} == {
            checkpoint,
            second.checkpoint(),
        }
        assert polled_messages.checkpoint() == second.checkpoint()
        assert polled_messages.checkpoint_at(len(polled_messages.messages)) is None

    @pytest.mark.asyncio
    async def test_client_stores_reads_and_deletes_consumer_offsets(
        self, iggy_client: IggyClient, unique_name
    ):
        """Test the client-level offset calls by partition and by checkpoint."""
        stream_name = unique_name()
        topic_name = unique_name()
        partition_id = 0
        consumer = Consumer.Single(7)
        await iggy_client.create_stream(stream_name)
        await iggy_client.create_topic(stream=stream_name, name=topic_name)
        await iggy_client.send_messages(
            stream=stream_name,
            topic=topic_name,
            partitioning=partition_id,
            messages=[Message(f"offset {i}") for i in range(3)],
        )
        polled = await iggy_client.poll_messages(
            stream=stream_name,
            topic=topic_name,
            consumer=consumer,
            partition_id=partition_id,
            polling_strategy=PollingStrategy.First(),
            options=PollOptions(count=10),
        )

        def offset():
            return iggy_client.get_consumer_offset(
                stream_name, topic_name, consumer=consumer, partition_id=partition_id
            )

        assert await offset() is None
        await iggy_client.store_consumer_offset(
            stream_name,
            topic_name,
            consumer=consumer,
            partition_id=partition_id,
            offset=1,
        )
        stored_by_partition = await offset()
        checkpoint = polled.checkpoint()
        assert checkpoint is not None
        await iggy_client.store_consumer_position(
            stream_name, topic_name, consumer=consumer, checkpoint=checkpoint
        )
        stored_by_checkpoint = await offset()
        await iggy_client.delete_consumer_offset(
            stream_name, topic_name, consumer=consumer, partition_id=partition_id
        )
        deleted = await offset()
        with pytest.raises(IggyError) as missing:
            await iggy_client.delete_consumer_offset(
                stream_name, topic_name, consumer=consumer, partition_id=partition_id
            )

        assert stored_by_partition is not None
        assert (
            stored_by_partition.partition_id,
            stored_by_partition.current_offset,
            stored_by_partition.stored_offset,
        ) == (partition_id, 2, 1)
        assert stored_by_checkpoint is not None
        assert stored_by_checkpoint.stored_offset == 2
        assert deleted is None
        assert missing.value.code == CONSUMER_OFFSET_NOT_FOUND

    @pytest.mark.asyncio
    async def test_continuation_reads_on_after_the_batch(
        self, iggy_client: IggyClient, unique_name
    ):
        """Test a continuation reads from the message after its batch."""
        stream_name = unique_name()
        topic_name = unique_name()
        partition_id = 0
        test_messages = [f"Continuation test {i} - {unique_name()}" for i in range(5)]

        await iggy_client.create_stream(stream_name)
        await iggy_client.create_topic(stream=stream_name, name=topic_name)
        await iggy_client.send_messages(
            stream=stream_name,
            topic=topic_name,
            partitioning=partition_id,
            messages=[Message(message) for message in test_messages],
        )

        first_batch = await iggy_client.poll_messages(
            stream=stream_name,
            topic=topic_name,
            consumer=Consumer.Single(1),
            partition_id=partition_id,
            polling_strategy=PollingStrategy.First(),
            options=PollOptions(count=2),
        )
        next_batch = await iggy_client.poll_messages(
            stream=stream_name,
            topic=topic_name,
            consumer=Consumer.Single(1),
            partition_id=partition_id,
            polling_strategy=first_batch.continuation(),
            options=PollOptions(count=10),
        )

        assert [
            message.payload().decode("utf-8") for message in first_batch.messages
        ] == test_messages[:2]
        assert [
            message.payload().decode("utf-8") for message in next_batch.messages
        ] == test_messages[2:]

    @pytest.mark.asyncio
    async def test_continuation_of_an_empty_batch_keeps_its_start(
        self, iggy_client: IggyClient, unique_name
    ):
        """Test an empty batch continues from the offset its poll started at."""
        stream_name = unique_name()
        topic_name = unique_name()
        partition_id = 0
        existing_messages = [
            f"Existing continuation test {i} - {unique_name()}" for i in range(3)
        ]
        new_messages = [f"Continuation test {i} - {unique_name()}" for i in range(2)]
        # A new partition starts at offset 0.
        start_offset = len(existing_messages)

        await iggy_client.create_stream(stream_name)
        await iggy_client.create_topic(stream=stream_name, name=topic_name)
        await iggy_client.send_messages(
            stream=stream_name,
            topic=topic_name,
            partitioning=partition_id,
            messages=[Message(message) for message in existing_messages],
        )

        empty_batch = await iggy_client.poll_messages(
            stream=stream_name,
            topic=topic_name,
            consumer=Consumer.Single(1),
            partition_id=partition_id,
            polling_strategy=PollingStrategy.Offset(value=start_offset),
            options=PollOptions(count=10),
        )
        assert empty_batch.messages == ()
        assert empty_batch.partition_id == partition_id
        assert empty_batch.checkpoint() is None

        await iggy_client.send_messages(
            stream=stream_name,
            topic=topic_name,
            partitioning=partition_id,
            messages=[Message(message) for message in new_messages],
        )

        resumed_batch = await iggy_client.poll_messages(
            stream=stream_name,
            topic=topic_name,
            consumer=Consumer.Single(1),
            partition_id=partition_id,
            polling_strategy=empty_batch.continuation(),
            options=PollOptions(count=10),
        )
        assert [
            message.payload().decode("utf-8") for message in resumed_batch.messages
        ] == new_messages

    @pytest.mark.asyncio
    async def test_continuation_of_another_partition_is_refused(
        self, iggy_client: IggyClient, unique_name
    ):
        """Test a continuation reads only the partition of its batch."""
        stream_name = unique_name()
        topic_name = unique_name()

        await iggy_client.create_stream(stream_name)
        await iggy_client.create_topic(
            stream=stream_name,
            name=topic_name,
            options=TopicOptions(partitions_count=2),
        )
        await iggy_client.send_messages(
            stream=stream_name,
            topic=topic_name,
            partitioning=0,
            messages=[Message("Partition 0")],
        )
        polled_messages = await iggy_client.poll_messages(
            stream=stream_name,
            topic=topic_name,
            consumer=Consumer.Single(1),
            partition_id=0,
            polling_strategy=PollingStrategy.First(),
            options=PollOptions(count=10),
        )

        with pytest.raises(IggyError) as refusal:
            await iggy_client.poll_messages(
                stream=stream_name,
                topic=topic_name,
                consumer=Consumer.Single(1),
                partition_id=1,
                polling_strategy=polled_messages.continuation(),
                options=PollOptions(count=10),
            )
        assert refusal.value.code == INVALID_COMMAND

    @pytest.mark.asyncio
    async def test_continuation_after_the_partition_was_created_again_is_refused(
        self, iggy_client: IggyClient, unique_name
    ):
        """Test a continuation does not read a partition created again at its id."""
        stream_name = unique_name()
        topic_name = unique_name()
        partition_id = 0

        await iggy_client.create_stream(stream_name)
        await iggy_client.create_topic(stream=stream_name, name=topic_name)
        await iggy_client.send_messages(
            stream=stream_name,
            topic=topic_name,
            partitioning=partition_id,
            messages=[Message("Before the partition was created again")],
        )
        polled_messages = await iggy_client.poll_messages(
            stream=stream_name,
            topic=topic_name,
            consumer=Consumer.Single(1),
            partition_id=partition_id,
            polling_strategy=PollingStrategy.First(),
            options=PollOptions(count=10),
        )

        await iggy_client.delete_partitions(stream_name, topic_name, 1)
        await iggy_client.create_partitions(stream_name, topic_name, 1)

        with pytest.raises(IggyError) as refusal:
            await iggy_client.poll_messages(
                stream=stream_name,
                topic=topic_name,
                consumer=Consumer.Single(1),
                partition_id=partition_id,
                polling_strategy=polled_messages.continuation(),
                options=PollOptions(count=10),
            )
        assert refusal.value.code == HISTORY_UNAVAILABLE


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
        await iggy_client.create_topic(stream=stream_name, name=topic_name)

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
            consumer=Consumer.Single(1),
            partition_id=partition_id,
            polling_strategy=PollingStrategy.First(),
            options=PollOptions(count=10, commit=CommitPolicy.OnRead()),
        )

        assert [message.payload().decode() for message in polled_messages.messages] == (
            test_messages
        )

    @pytest.mark.asyncio
    async def test_poll_refuses_auto_commit(self, iggy_client: IggyClient, unique_name):
        """Test a one-shot poll refuses CommitPolicy.Auto, which it cannot count."""
        stream_name = unique_name()
        topic_name = unique_name()
        await iggy_client.create_stream(stream_name)
        await iggy_client.create_topic(stream=stream_name, name=topic_name)

        with pytest.raises(IggyError) as refused:
            await iggy_client.poll_messages(
                stream=stream_name,
                topic=topic_name,
                consumer=Consumer.Single(1),
                partition_id=0,
                polling_strategy=PollingStrategy.First(),
                options=PollOptions(commit=CommitPolicy.Auto(AutoCommit.default())),
            )

        assert refused.value.code == INVALID_CONFIGURATION

    @pytest.mark.asyncio
    async def test_cancelled_on_read_poll_settles_through_its_one_reply(
        self, iggy_client: IggyClient, unique_name
    ):
        """Test a cancelled OnRead poll settles its partition through its one reply."""
        stream_name = unique_name()
        topic_name = unique_name()
        partition_id = 0
        consumer = Consumer.Single(1)
        on_read = PollOptions(count=1, commit=CommitPolicy.OnRead())

        await iggy_client.create_stream(stream_name)
        await iggy_client.create_topic(stream=stream_name, name=topic_name)
        await iggy_client.send_messages(
            stream=stream_name,
            topic=topic_name,
            partitioning=partition_id,
            messages=[Message("first"), Message("second")],
        )

        def poll():
            return iggy_client.poll_messages(
                stream=stream_name,
                topic=topic_name,
                consumer=consumer,
                partition_id=partition_id,
                polling_strategy=PollingStrategy.Next(),
                options=on_read,
            )

        cancelled = asyncio.ensure_future(poll())
        await asyncio.sleep(0)
        # Blocks the event loop while the reply arrives, so the cancel lands after it.
        time.sleep(REPLY_WAIT_SECONDS)
        cancelled.cancel()
        with contextlib.suppress(asyncio.CancelledError):
            await cancelled

        # The reply of the cancelled poll settles it, so the next poll is not blocked.
        # The cancel wins and nothing is stored, or the store happened and the batch
        # went with the cancelled call.
        polled = await poll()
        assert [message.payload() for message in polled.messages] in (
            [b"first"],
            [b"second"],
        )

    @pytest.mark.asyncio
    async def test_send_messages_reports_committed_confirmation(
        self, iggy_client: IggyClient, unique_name
    ):
        """Test the send response reports the committed partition and offset."""
        stream_name = unique_name()
        topic_name = unique_name()
        partition_id = 0

        await iggy_client.create_stream(stream_name)
        await iggy_client.create_topic(stream=stream_name, name=topic_name)

        payloads = [f"Confirmation test {i}" for i in range(3)]
        response = await iggy_client.send_messages(
            stream=stream_name,
            topic=topic_name,
            partitioning=partition_id,
            messages=[Message(payload) for payload in payloads],
        )

        polled_messages = await iggy_client.poll_messages(
            stream=stream_name,
            topic=topic_name,
            consumer=Consumer.Single(1),
            partition_id=partition_id,
            polling_strategy=PollingStrategy.First(),
            options=PollOptions(count=10, commit=CommitPolicy.OnRead()),
        )

        assert [
            message.payload().decode() for message in polled_messages.messages
        ] == payloads

        # The server confirms committed sends with the partition and the base
        # offset of the batch; the whole send targeted one partition, so a
        # single confirmation covers it.
        assert len(response.confirmations) == 1
        confirmation = response.confirmations[0]
        assert confirmation.partition_id == partition_id
        assert confirmation.base_offset == polled_messages.messages[0].offset()

    @pytest.mark.asyncio
    async def test_send_messages_with_partition_id_strategy(
        self, iggy_client: IggyClient, unique_name
    ):
        stream_name = unique_name()
        topic_name = unique_name()
        partition_id = 2

        await iggy_client.create_stream(stream_name)
        await iggy_client.create_topic(
            stream=stream_name,
            name=topic_name,
            options=TopicOptions(partitions_count=3),
        )

        response = await iggy_client.send_messages(
            stream=stream_name,
            topic=topic_name,
            partitioning=Partitioning.partition_id(partition_id),
            messages=[Message("fixed partition 1"), Message("fixed partition 2")],
        )

        assert len(response.confirmations) == 1
        assert response.confirmations[0].partition_id == partition_id

        with pytest.raises(RuntimeError):
            await iggy_client.send_messages(
                stream=stream_name,
                topic=topic_name,
                partitioning=Partitioning.partition_id(3),
                messages=[Message("missing partition")],
            )

    @pytest.mark.asyncio
    async def test_send_messages_with_balanced_strategy(
        self, iggy_client: IggyClient, unique_name
    ):
        stream_name = unique_name()
        topic_name = unique_name()
        partitions_count = 3

        await iggy_client.create_stream(stream_name)
        await iggy_client.create_topic(
            stream=stream_name,
            name=topic_name,
            options=TopicOptions(partitions_count=partitions_count),
        )

        responses = [
            await iggy_client.send_messages(
                stream=stream_name,
                topic=topic_name,
                partitioning=Partitioning.balanced(),
                messages=[Message(f"balanced {index}")],
            )
            for index in range(partitions_count)
        ]

        assert all(len(response.confirmations) == 1 for response in responses)
        assert (
            len({response.confirmations[0].partition_id for response in responses})
            == partitions_count
        )

    @pytest.mark.asyncio
    @pytest.mark.parametrize("key", [b"customer-42", "customer-42"])
    async def test_send_messages_with_same_key_uses_same_partition(
        self, iggy_client: IggyClient, unique_name, key
    ):
        stream_name = unique_name()
        topic_name = unique_name()

        await iggy_client.create_stream(stream_name)
        await iggy_client.create_topic(
            stream=stream_name,
            name=topic_name,
            options=TopicOptions(partitions_count=3),
        )
        partitioning = Partitioning.messages_key(key)

        first = await iggy_client.send_messages(
            stream=stream_name,
            topic=topic_name,
            partitioning=partitioning,
            messages=[Message("first")],
        )
        second = await iggy_client.send_messages(
            stream=stream_name,
            topic=topic_name,
            partitioning=partitioning,
            messages=[Message("second")],
        )

        assert len(first.confirmations) == 1
        assert len(second.confirmations) == 1
        assert (
            first.confirmations[0].partition_id == second.confirmations[0].partition_id
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
        await iggy_client.create_topic(stream=stream_name, name=topic_name)

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
            consumer=Consumer.Single(1),
            partition_id=partition_id,
            polling_strategy=PollingStrategy.First(),
            options=PollOptions(count=10, commit=CommitPolicy.OnRead()),
        )

        assert [message.payload().decode() for message in polled_messages.messages] == (
            test_messages
        )

    @pytest.mark.asyncio
    async def test_message_properties(self, iggy_client: IggyClient, unique_name):
        """Test access to message properties."""
        stream_name = unique_name()
        topic_name = unique_name()
        partition_id = 0

        await iggy_client.create_stream(stream_name)
        await iggy_client.create_topic(stream=stream_name, name=topic_name)

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
            consumer=Consumer.Single(1),
            partition_id=partition_id,
            polling_strategy=PollingStrategy.Last(),
            options=PollOptions(count=1, commit=CommitPolicy.OnRead()),
        )

        assert len(polled_messages.messages) >= 1
        msg = polled_messages.messages[0]

        assert msg.payload().decode("utf-8") == test_payload
        assert isinstance(msg.offset(), int) and msg.offset() >= 0
        assert isinstance(msg.id(), int) and msg.id() > 0
        assert isinstance(msg.timestamp(), int) and msg.timestamp() > 0
        assert isinstance(msg.origin_timestamp(), int) and msg.origin_timestamp() > 0
        assert isinstance(msg.checksum(), int)
        assert isinstance(msg.length(), int) and msg.length() > 0
        assert msg.user_headers() is None

    @pytest.mark.asyncio
    async def test_message_user_headers_round_trip(
        self, iggy_client: IggyClient, unique_name
    ):
        """Test plain user headers round-trip through the typed representation."""
        stream_name = unique_name()
        topic_name = unique_name()
        partition_id = 0
        message_id = 123456789
        user_headers = {
            "content-type": "application/json",
            "trace-blob": b"\x00\x01",
            "is-retry": False,
            "attempt": 3,
            "score": 0.99,
        }

        await iggy_client.create_stream(stream_name)
        await iggy_client.create_topic(stream=stream_name, name=topic_name)

        await iggy_client.send_messages(
            stream=stream_name,
            topic=topic_name,
            partitioning=partition_id,
            messages=[
                Message(
                    "header round trip",
                    user_headers=user_headers,
                    id=message_id,
                )
            ],
        )

        polled_messages = await iggy_client.poll_messages(
            stream=stream_name,
            topic=topic_name,
            consumer=Consumer.Single(1),
            partition_id=partition_id,
            polling_strategy=PollingStrategy.Last(),
            options=PollOptions(count=1, commit=CommitPolicy.OnRead()),
        )

        assert len(polled_messages.messages) == 1
        message = polled_messages.messages[0]
        assert message.id() == message_id
        typed_headers = message.user_headers()
        assert typed_headers is not None
        assert typed_headers == {
            HeaderKey.String("content-type"): HeaderValue.String("application/json"),
            HeaderKey.String("trace-blob"): HeaderValue.Raw(b"\x00\x01"),
            HeaderKey.String("is-retry"): HeaderValue.Bool(False),
            HeaderKey.String("attempt"): HeaderValue.UnsignedInt8(3),
            HeaderKey.String("score"): HeaderValue.Float64(0.99),
        }
        assert typed_headers.to_scalar_dict() == user_headers
        assert isinstance(message.origin_timestamp(), int)
        assert message.origin_timestamp() > 0

    @pytest.mark.asyncio
    async def test_typed_message_user_headers_round_trip(
        self, iggy_client: IggyClient, unique_name
    ):
        """Test typed user headers preserve explicit header kinds."""
        stream_name = unique_name()
        topic_name = unique_name()
        partition_id = 0
        user_headers: dict[HeaderKey, HeaderValue] = {
            HeaderKey.UnsignedInt128(42): HeaderValue.UnsignedInt128(2**96),
            HeaderKey.String("float32"): HeaderValue.Float32(1.25),
        }

        await iggy_client.create_stream(stream_name)
        await iggy_client.create_topic(stream=stream_name, name=topic_name)

        await iggy_client.send_messages(
            stream=stream_name,
            topic=topic_name,
            partitioning=partition_id,
            messages=[Message("typed headers", user_headers=user_headers)],  # pyright: ignore[reportArgumentType]
        )

        polled_messages = await iggy_client.poll_messages(
            stream=stream_name,
            topic=topic_name,
            consumer=Consumer.Single(1),
            partition_id=partition_id,
            polling_strategy=PollingStrategy.Last(),
            options=PollOptions(count=1, commit=CommitPolicy.OnRead()),
        )

        assert len(polled_messages.messages) == 1
        headers = polled_messages.messages[0].user_headers()
        assert headers == user_headers

    @pytest.mark.asyncio
    async def test_plain_scalars_pick_smallest_lossless_kind(
        self, iggy_client: IggyClient, unique_name
    ):
        """Test plain ints/floats map to the narrowest lossless header kind."""
        stream_name = unique_name()
        topic_name = unique_name()
        partition_id = 0
        plain_headers = {
            "small": 200,
            "negative": -5,
            "large": 2**63,
            "huge": 2**96,
            "exact-float": 1.25,
            "wide-float": 0.1,
        }

        await iggy_client.create_stream(stream_name)
        await iggy_client.create_topic(stream=stream_name, name=topic_name)

        await iggy_client.send_messages(
            stream=stream_name,
            topic=topic_name,
            partitioning=partition_id,
            messages=[Message("plain scalars", user_headers=plain_headers)],
        )

        polled_messages = await iggy_client.poll_messages(
            stream=stream_name,
            topic=topic_name,
            consumer=Consumer.Single(1),
            partition_id=partition_id,
            polling_strategy=PollingStrategy.Last(),
            options=PollOptions(count=1, commit=CommitPolicy.OnRead()),
        )

        headers = polled_messages.messages[0].user_headers()
        assert headers is not None
        assert headers == {
            HeaderKey.String("small"): HeaderValue.UnsignedInt8(200),
            HeaderKey.String("negative"): HeaderValue.Int8(-5),
            HeaderKey.String("large"): HeaderValue.UnsignedInt64(2**63),
            HeaderKey.String("huge"): HeaderValue.UnsignedInt128(2**96),
            HeaderKey.String("exact-float"): HeaderValue.Float32(1.25),
            HeaderKey.String("wide-float"): HeaderValue.Float64(0.1),
        }
        assert headers.to_scalar_dict() == plain_headers

    @pytest.mark.asyncio
    @pytest.mark.parametrize(
        "payload",
        ["", b""],
    )
    async def test_empty_payload_is_rejected(self, payload):
        """Test empty string and bytes payloads are rejected."""
        with pytest.raises(ValueError, match="Invalid message payload length"):
            Message(payload)

    @pytest.mark.asyncio
    @pytest.mark.parametrize(
        ("headers", "error"),
        [
            ({"": "value"}, "Invalid header value"),
            ({"x" * 256: "value"}, "Invalid header value"),
            ({"key": ""}, "Invalid header value"),
            ({"key": b""}, "Invalid header value"),
            ({"key": "x" * 256}, "Invalid header value"),
            (
                {object(): "value"},
                "User header must be str, bytes, bool, int, float, "
                "HeaderKey, or HeaderValue",
            ),
            (
                {"key": object()},
                "User header must be str, bytes, bool, int, float, "
                "HeaderKey, or HeaderValue",
            ),
            ({"key": 2**128}, "128-bit range"),
            ({"key": -(2**200)}, "128-bit range"),
            ({"key": float("inf")}, "finite"),
            ({"key": float("-inf")}, "finite"),
            ({"key": float("nan")}, "finite"),
        ],
    )
    async def test_invalid_user_headers_are_rejected(self, headers, error):
        """Test invalid user header input raises ValueError."""
        with pytest.raises(ValueError, match=error):
            Message("payload", user_headers=headers)

    def test_duplicate_user_header_keys_are_rejected(self):
        """Test a message with duplicate user header keys is rejected."""
        with pytest.raises(ValueError, match="Duplicate user header key"):
            Message(
                "payload",
                user_headers={
                    HeaderKey.String("dup"): HeaderValue.String("first"),
                    "dup": "second",
                },
            )

    def test_user_headers_over_100k_bytes_are_rejected(self):
        """Test user headers exceeding the 100 KB limit are rejected."""
        oversized_headers = {f"key-{index:05d}": "v" * 255 for index in range(1000)}
        with pytest.raises(ValueError, match="Too big headers payload"):
            Message("payload", user_headers=oversized_headers)

    def test_explicit_float32_out_of_range_is_rejected(self):
        """Test an explicit Float32 whose value overflows f32 is rejected."""
        with pytest.raises(ValueError, match="32-bit float"):
            Message("payload", user_headers={"k": HeaderValue.Float32(1e40)})

    @pytest.mark.parametrize(
        "kind,val_str",
        [
            (HeaderValue.Float32, "inf"),
            (HeaderValue.Float32, "-inf"),
            (HeaderValue.Float32, "nan"),
            (HeaderValue.Float64, "inf"),
            (HeaderValue.Float64, "-inf"),
            (HeaderValue.Float64, "nan"),
        ],
    )
    def test_non_finite_typed_header_value_is_rejected(self, kind, val_str):
        """Test typed Float32/Float64 header values reject non-finite values."""
        value = float(val_str)
        with pytest.raises(ValueError, match="finite"):
            UserHeaders({"k": kind(value)})

    @pytest.mark.parametrize(
        "kind,val_str",
        [
            (HeaderKey.Float32, "inf"),
            (HeaderKey.Float32, "-inf"),
            (HeaderKey.Float32, "nan"),
            (HeaderKey.Float64, "inf"),
            (HeaderKey.Float64, "-inf"),
            (HeaderKey.Float64, "nan"),
        ],
    )
    def test_non_finite_typed_header_key_is_rejected(self, kind, val_str):
        """Test typed Float32/Float64 header keys reject non-finite values."""
        value = float(val_str)
        with pytest.raises(ValueError, match="finite"):
            UserHeaders({kind(value): "v"})

    def test_mixed_typed_and_plain_headers_can_be_constructed(self):
        """Test each key/value pair is converted independently and can be mixed."""
        Message(
            "payload",
            user_headers={
                HeaderKey.String("typed-key"): HeaderValue.UnsignedInt16(7),
                HeaderKey.String("plain-value"): "still-a-string",
                "plain-key": HeaderValue.Bool(True),
                "fully-plain": 42,
            },
        )

    def test_typed_user_headers_can_be_constructed(self):
        """Test typed header keys and values cover the full header kind surface."""
        Message(
            "payload",
            user_headers={
                HeaderKey.Raw(b"raw-key"): HeaderValue.Raw(b"raw-value"),
                HeaderKey.String("string-key"): HeaderValue.String("string-value"),
                HeaderKey.Bool(True): HeaderValue.Bool(False),
                HeaderKey.Int8(-8): HeaderValue.Int8(-7),
                HeaderKey.Int16(-16): HeaderValue.Int16(-15),
                HeaderKey.Int32(-32): HeaderValue.Int32(-31),
                HeaderKey.Int64(-64): HeaderValue.Int64(-63),
                HeaderKey.Int128(-(2**80)): HeaderValue.Int128(-(2**79)),
                HeaderKey.UnsignedInt8(8): HeaderValue.UnsignedInt8(9),
                HeaderKey.UnsignedInt16(16): HeaderValue.UnsignedInt16(17),
                HeaderKey.UnsignedInt32(32): HeaderValue.UnsignedInt32(33),
                HeaderKey.UnsignedInt64(64): HeaderValue.UnsignedInt64(65),
                HeaderKey.UnsignedInt128(2**80): HeaderValue.UnsignedInt128(2**79),
                HeaderKey.Float32(1.25): HeaderValue.Float32(2.5),
                HeaderKey.Float64(3.5): HeaderValue.Float64(4.75),
            },
        )

    def test_plain_user_headers_convert_every_kind_losslessly(self):
        """Test every header kind converts to a plain scalar without logging."""
        headers = UserHeaders(
            {
                HeaderKey.String("content-type"): HeaderValue.String(
                    "application/json"
                ),
                HeaderKey.String("trace-blob"): HeaderValue.Raw(b"\x00\x01"),
                HeaderKey.String("is-retry"): HeaderValue.Bool(True),
                HeaderKey.String("attempt"): HeaderValue.Int64(3),
                HeaderKey.String("schema-version"): HeaderValue.UnsignedInt16(1),
                HeaderKey.String("big"): HeaderValue.UnsignedInt128(2**96),
                HeaderKey.String("ratio"): HeaderValue.Float32(1.25),
                HeaderKey.String("score"): HeaderValue.Float64(0.5),
            }
        )

        plain = headers.to_scalar_dict()

        assert plain == {
            "content-type": "application/json",
            "trace-blob": b"\x00\x01",
            "is-retry": True,
            "attempt": 3,
            "schema-version": 1,
            "big": 2**96,
            "ratio": 1.25,
            "score": 0.5,
        }

    def test_plain_user_headers_accepts_plain_dict(self):
        """Test the plain dictionary form passes through unchanged."""
        headers = {"content-type": "application/json", "attempt": 3}
        assert UserHeaders(headers).to_scalar_dict() == headers

    def test_plain_user_headers_preserve_non_string_keys(self):
        """Test non-string typed keys convert back to their scalar Python type."""
        headers = UserHeaders(
            {HeaderKey.UnsignedInt32(7): HeaderValue.String("order-id")}
        )

        plain = headers.to_scalar_dict()

        assert plain == {7: "order-id"}

    def test_to_scalar_dict_raises_on_colliding_keys(self):
        """Test that typed keys mapping to the same plain scalar raise an error."""
        headers = UserHeaders(
            {
                HeaderKey.UnsignedInt8(1): HeaderValue.String("first"),
                HeaderKey.UnsignedInt16(1): HeaderValue.String("second"),
            }
        )
        with pytest.raises(ValueError, match="Distinct typed header keys"):
            headers.to_scalar_dict()

    def test_user_headers_construction_rejects_non_scalar_key(self):
        with pytest.raises(
            ValueError,
            match=(
                "User header must be str, bytes, bool, int, float, "
                "HeaderKey, or HeaderValue"
            ),
        ):
            UserHeaders({object(): "value"})

    def test_user_headers_construction_rejects_non_scalar_value(self):
        with pytest.raises(
            ValueError,
            match=(
                "User header must be str, bytes, bool, int, float, "
                "HeaderKey, or HeaderValue"
            ),
        ):
            UserHeaders({"key": object()})

    def test_user_headers_construction_rejects_invalid_keys_and_values(self):
        with pytest.raises(
            ValueError,
            match=(
                "User header must be str, bytes, bool, int, float, "
                "HeaderKey, or HeaderValue"
            ),
        ):
            UserHeaders({object(): object()})

    def test_user_headers_setitem_rejects_non_scalar_key(self):
        headers = UserHeaders()
        with pytest.raises(
            ValueError,
            match=(
                "User header must be str, bytes, bool, int, float, "
                "HeaderKey, or HeaderValue"
            ),
        ):
            headers[object()] = "value"

    def test_user_headers_setitem_rejects_non_scalar_value(self):
        headers = UserHeaders()
        with pytest.raises(
            ValueError,
            match=(
                "User header must be str, bytes, bool, int, float, "
                "HeaderKey, or HeaderValue"
            ),
        ):
            headers["key"] = object()

    def test_user_headers_to_scalar_dict_rejects_non_scalar_in_stored_data(
        self,
    ):
        headers = UserHeaders()
        dict.__setitem__(headers, object(), object())
        with pytest.raises(
            ValueError,
            match=(
                "User header must be str, bytes, bool, int, float, "
                "HeaderKey, or HeaderValue"
            ),
        ):
            headers.to_scalar_dict()

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
        await iggy_client.create_topic(stream=stream_name, name=topic_name)

        await iggy_client.send_messages(
            stream=stream_name,
            topic=topic_name,
            partitioning=partition_id,
            messages=[Message(payload)],
        )

        polled_messages = await iggy_client.poll_messages(
            stream=stream_name,
            topic=topic_name,
            consumer=Consumer.Single(1),
            partition_id=partition_id,
            polling_strategy=PollingStrategy.Last(),
            options=PollOptions(count=1, commit=CommitPolicy.OnRead()),
        )

        assert len(polled_messages.messages) == 1
        message = polled_messages.messages[0]
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
        await iggy_client.create_topic(stream=stream_name, name=topic_name)

        await iggy_client.send_messages(
            stream=stream_name,
            topic=topic_name,
            partitioning=partition_id,
            messages=[Message(payload)],
        )

        polled_messages = await iggy_client.poll_messages(
            stream=stream_name,
            topic=topic_name,
            consumer=Consumer.Single(1),
            partition_id=partition_id,
            polling_strategy=PollingStrategy.Last(),
            options=PollOptions(count=1, commit=CommitPolicy.OnRead()),
        )

        assert len(polled_messages.messages) == 1
        message = polled_messages.messages[0]
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
        await iggy_client.create_topic(stream=stream_name, name=topic_name)

        await iggy_client.send_messages(
            stream=stream_name,
            topic=topic_name,
            partitioning=partition_id,
            messages=[Message(payload)],
        )

        polled_messages = await iggy_client.poll_messages(
            stream=stream_name,
            topic=topic_name,
            consumer=Consumer.Single(1),
            partition_id=partition_id,
            polling_strategy=PollingStrategy.Last(),
            options=PollOptions(count=1, commit=CommitPolicy.OnRead()),
        )

        assert len(polled_messages.messages) == 1
        message = polled_messages.messages[0]
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
        await iggy_client.create_topic(stream=stream_name, name=topic_name)

        await iggy_client.send_messages(
            stream=stream_name,
            topic=topic_name,
            partitioning=partition_id,
            messages=[Message(message) for message in test_messages],
        )

        polled_messages = await iggy_client.poll_messages(
            stream=stream_name,
            topic=topic_name,
            consumer=Consumer.Single(1),
            partition_id=partition_id,
            polling_strategy=PollingStrategy.First(),
            options=PollOptions(count=1),
        )

        assert len(polled_messages.messages) == 1
        assert polled_messages.messages[0].payload().decode("utf-8") == test_messages[0]

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
        await iggy_client.create_topic(stream=stream_name, name=topic_name)

        await iggy_client.send_messages(
            stream=stream_name,
            topic=topic_name,
            partitioning=partition_id,
            messages=[Message(message) for message in test_messages],
        )

        polled_messages = await iggy_client.poll_messages(
            stream=stream_name,
            topic=topic_name,
            consumer=Consumer.Single(1),
            partition_id=partition_id,
            polling_strategy=PollingStrategy.First(),
            options=PollOptions(count=10),
        )

        assert len(polled_messages.messages) == len(test_messages)
        assert [
            message.payload().decode("utf-8") for message in polled_messages.messages
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
        await iggy_client.create_topic(stream=stream_name, name=topic_name)

        await iggy_client.send_messages(
            stream=stream_name,
            topic=topic_name,
            partitioning=partition_id,
            messages=[Message(message) for message in test_messages],
        )

        with pytest.raises(RuntimeError, match="Invalid messages count"):
            await iggy_client.poll_messages(
                stream=stream_name,
                topic=topic_name,
                consumer=Consumer.Single(1),
                partition_id=partition_id,
                polling_strategy=PollingStrategy.First(),
                options=PollOptions(count=0),
            )

    @pytest.mark.asyncio
    async def test_poll_messages_with_invalid_partition_id_raises(
        self, iggy_client: IggyClient, unique_name
    ):
        """Test polling a missing partition raises the expected runtime error."""
        stream_name = unique_name()
        topic_name = unique_name()
        partition_id = 0

        await iggy_client.create_stream(stream_name)
        await iggy_client.create_topic(stream=stream_name, name=topic_name)

        await iggy_client.send_messages(
            stream=stream_name,
            topic=topic_name,
            partitioning=partition_id,
            messages=[Message(f"Partition test - {unique_name()}")],
        )

        with pytest.raises(
            RuntimeError,
            match=(
                r"Partition with ID: 0 for topic with ID: 0 "
                r"for stream with ID: 0 was not found\."
            ),
        ):
            await iggy_client.poll_messages(
                stream=stream_name,
                topic=topic_name,
                consumer=Consumer.Single(1),
                partition_id=1,
                polling_strategy=PollingStrategy.First(),
                options=PollOptions(count=1),
            )

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
        await iggy_client.create_topic(stream=stream_name, name=topic_name)

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
            consumer=Consumer.Single(1),
            partition_id=partition_id,
            polling_strategy=PollingStrategy.Last(),
            options=PollOptions(count=1),
        )
        assert len(last_messages.messages) == 1
        assert last_messages.messages[0].payload().decode("utf-8") == test_messages[-1]

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
        await iggy_client.create_topic(stream=stream_name, name=topic_name)

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
            consumer=Consumer.Single(1),
            partition_id=partition_id,
            polling_strategy=PollingStrategy.Last(),
            options=PollOptions(count=2),
        )
        assert len(last_messages.messages) == 2
        assert [
            message.payload().decode("utf-8") for message in last_messages.messages
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
        await iggy_client.create_topic(stream=stream_name, name=topic_name)

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
            consumer=Consumer.Single(1),
            partition_id=partition_id,
            polling_strategy=PollingStrategy.First(),
            options=PollOptions(count=10),
        )
        start_offset = first_messages.messages[2].offset()

        offset_messages = await iggy_client.poll_messages(
            stream=stream_name,
            topic=topic_name,
            consumer=Consumer.Single(1),
            partition_id=partition_id,
            polling_strategy=PollingStrategy.Offset(value=start_offset),
            options=PollOptions(count=10),
        )
        assert len(offset_messages.messages) == len(test_messages[2:])
        assert offset_messages.messages[0].offset() == start_offset
        assert [
            message.payload().decode("utf-8") for message in offset_messages.messages
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
        await iggy_client.create_topic(stream=stream_name, name=topic_name)

        await iggy_client.send_messages(
            stream=stream_name,
            topic=topic_name,
            partitioning=partition_id,
            messages=[Message(msg) for msg in test_messages],
        )

        first_messages = await iggy_client.poll_messages(
            stream=stream_name,
            topic=topic_name,
            consumer=Consumer.Single(1),
            partition_id=partition_id,
            polling_strategy=PollingStrategy.First(),
            options=PollOptions(count=10),
        )
        offset_beyond_newest = first_messages.messages[-1].offset() + 1

        offset_messages = await iggy_client.poll_messages(
            stream=stream_name,
            topic=topic_name,
            consumer=Consumer.Single(1),
            partition_id=partition_id,
            polling_strategy=PollingStrategy.Offset(value=offset_beyond_newest),
            options=PollOptions(count=10),
        )

        assert offset_messages.messages == ()

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
        await iggy_client.create_topic(stream=stream_name, name=topic_name)

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
            consumer=Consumer.Single(1),
            partition_id=partition_id,
            polling_strategy=PollingStrategy.First(),
            options=PollOptions(count=10),
        )
        start_timestamp = first_messages.messages[2].timestamp()

        timestamp_messages = await iggy_client.poll_messages(
            stream=stream_name,
            topic=topic_name,
            consumer=Consumer.Single(1),
            partition_id=partition_id,
            polling_strategy=PollingStrategy.Timestamp(value=start_timestamp),
            options=PollOptions(count=10),
        )
        assert len(timestamp_messages.messages) == len(test_messages[2:])
        assert timestamp_messages.messages[0].timestamp() >= start_timestamp
        assert [
            message.payload().decode("utf-8") for message in timestamp_messages.messages
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
        await iggy_client.create_topic(stream=stream_name, name=topic_name)

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
            consumer=Consumer.Single(1),
            partition_id=partition_id,
            polling_strategy=PollingStrategy.First(),
            options=PollOptions(count=10),
        )
        timestamp_after_newest = first_messages.messages[-1].timestamp() + 1

        timestamp_messages = await iggy_client.poll_messages(
            stream=stream_name,
            topic=topic_name,
            consumer=Consumer.Single(1),
            partition_id=partition_id,
            polling_strategy=PollingStrategy.Timestamp(value=timestamp_after_newest),
            options=PollOptions(count=10),
        )

        assert timestamp_messages.messages == ()

    @pytest.mark.asyncio
    async def test_poll_messages_committing_on_read_advances_next(
        self, iggy_client: IggyClient, unique_name
    ):
        """Test CommitPolicy.OnRead() stores progress so Next resumes after it."""
        stream_name = unique_name()
        topic_name = unique_name()
        partition_id = 0
        existing_messages = [
            f"Existing polling test {i} - {unique_name()}" for i in range(2)
        ]
        new_messages = [f"Polling test {i} - {unique_name()}" for i in range(2)]

        await iggy_client.create_stream(stream_name)
        await iggy_client.create_topic(stream=stream_name, name=topic_name)

        await iggy_client.send_messages(
            stream=stream_name,
            topic=topic_name,
            partitioning=partition_id,
            messages=[Message(message) for message in existing_messages],
        )

        first_messages = await iggy_client.poll_messages(
            stream=stream_name,
            topic=topic_name,
            consumer=Consumer.Single(1),
            partition_id=partition_id,
            polling_strategy=PollingStrategy.First(),
            options=PollOptions(count=10, commit=CommitPolicy.OnRead()),
        )
        assert [
            message.payload().decode("utf-8")
            for message in first_messages.messages[: len(existing_messages)]
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
            consumer=Consumer.Single(1),
            partition_id=partition_id,
            polling_strategy=PollingStrategy.Next(),
            options=PollOptions(count=10, commit=CommitPolicy.OnRead()),
        )
        assert len(next_messages.messages) == len(new_messages)
        assert [
            message.payload().decode("utf-8") for message in next_messages.messages
        ] == new_messages

    @pytest.mark.asyncio
    async def test_poll_messages_without_a_commit_does_not_advance_next(
        self, iggy_client: IggyClient, unique_name
    ):
        """Test a poll without a server commit stores no offset, so Next starts over."""
        stream_name = unique_name()
        topic_name = unique_name()
        partition_id = 0
        existing_messages = [
            f"Existing polling test {i} - {unique_name()}" for i in range(2)
        ]
        new_messages = [f"Polling test {i} - {unique_name()}" for i in range(2)]

        await iggy_client.create_stream(stream_name)
        await iggy_client.create_topic(stream=stream_name, name=topic_name)

        await iggy_client.send_messages(
            stream=stream_name,
            topic=topic_name,
            partitioning=partition_id,
            messages=[Message(message) for message in existing_messages],
        )

        first_messages = await iggy_client.poll_messages(
            stream=stream_name,
            topic=topic_name,
            consumer=Consumer.Single(1),
            partition_id=partition_id,
            polling_strategy=PollingStrategy.First(),
            options=PollOptions(count=10),
        )
        assert [
            message.payload().decode("utf-8")
            for message in first_messages.messages[: len(existing_messages)]
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
            consumer=Consumer.Single(1),
            partition_id=partition_id,
            polling_strategy=PollingStrategy.Next(),
            options=PollOptions(count=10),
        )
        assert len(next_messages.messages) == len(existing_messages) + len(new_messages)
        assert [
            message.payload().decode("utf-8") for message in next_messages.messages
        ] == existing_messages + new_messages

    @pytest.mark.asyncio
    async def test_poll_messages_with_distinct_consumers_keeps_offsets_independent(
        self, iggy_client: IggyClient, unique_name
    ):
        """Test each consumer owns its offset, so both see the whole partition."""
        stream_name = unique_name()
        topic_name = unique_name()
        partition_id = 0
        test_messages = [f"Consumer isolation {i} - {unique_name()}" for i in range(3)]

        await iggy_client.create_stream(stream_name)
        await iggy_client.create_topic(stream=stream_name, name=topic_name)
        await iggy_client.send_messages(
            stream=stream_name,
            topic=topic_name,
            partitioning=partition_id,
            messages=[Message(message) for message in test_messages],
        )

        for consumer_name in ("isolation-consumer-a", "isolation-consumer-b"):
            polled_messages = await iggy_client.poll_messages(
                stream=stream_name,
                topic=topic_name,
                consumer=Consumer.Single(consumer_name),
                partition_id=partition_id,
                polling_strategy=PollingStrategy.Next(),
                options=PollOptions(count=10, commit=CommitPolicy.OnRead()),
            )
            assert [
                message.payload().decode("utf-8")
                for message in polled_messages.messages
            ] == test_messages

    @pytest.mark.asyncio
    async def test_poll_messages_with_shared_consumer_shares_the_partition(
        self, iggy_client: IggyClient, unique_name
    ):
        """Test two polls under the same consumer share one stored offset."""
        stream_name = unique_name()
        topic_name = unique_name()
        partition_id = 0
        test_messages = [f"Shared consumer {i} - {unique_name()}" for i in range(3)]

        await iggy_client.create_stream(stream_name)
        await iggy_client.create_topic(stream=stream_name, name=topic_name)
        await iggy_client.send_messages(
            stream=stream_name,
            topic=topic_name,
            partitioning=partition_id,
            messages=[Message(message) for message in test_messages],
        )

        first_poll = await iggy_client.poll_messages(
            stream=stream_name,
            topic=topic_name,
            consumer=Consumer.Single("shared-consumer"),
            partition_id=partition_id,
            polling_strategy=PollingStrategy.Next(),
            options=PollOptions(count=10, commit=CommitPolicy.OnRead()),
        )
        assert [
            message.payload().decode("utf-8") for message in first_poll.messages
        ] == test_messages

        second_poll = await iggy_client.poll_messages(
            stream=stream_name,
            topic=topic_name,
            consumer=Consumer.Single("shared-consumer"),
            partition_id=partition_id,
            polling_strategy=PollingStrategy.Next(),
            options=PollOptions(count=10, commit=CommitPolicy.OnRead()),
        )
        assert second_poll.messages == ()

    @pytest.mark.asyncio
    async def test_poll_messages_without_a_consumer_raises_type_error(
        self, iggy_client: IggyClient, unique_name
    ):
        """Test the consumer argument is required."""
        # Argument binding fails before the call reaches the server, so the
        # stream and topic never have to exist.
        with pytest.raises(TypeError, match="consumer"):
            # pyrefly: ignore  # missing-argument
            await iggy_client.poll_messages(
                stream=unique_name(),
                topic=unique_name(),
                partition_id=0,
                polling_strategy=PollingStrategy.Next(),
                options=PollOptions(count=10, commit=CommitPolicy.OnRead()),
            )

    @pytest.mark.asyncio
    @pytest.mark.parametrize(
        "consumer", ["single", 1, None], ids=["str", "int", "none"]
    )
    async def test_poll_messages_with_a_non_consumer_raises_type_error(
        self, iggy_client: IggyClient, unique_name, consumer
    ):
        """Test the consumer argument only accepts a Consumer."""
        # Argument binding fails before the call reaches the server, so the
        # stream and topic never have to exist.
        with pytest.raises(TypeError, match="not an instance of 'Consumer'"):
            # pyrefly: ignore  # bad-argument-type
            await iggy_client.poll_messages(
                stream=unique_name(),
                topic=unique_name(),
                consumer=consumer,
                partition_id=0,
                polling_strategy=PollingStrategy.Next(),
                options=PollOptions(count=10, commit=CommitPolicy.OnRead()),
            )

    @pytest.mark.parametrize("identifier", [-1, 2**32], ids=["negative", "above-u32"])
    def test_consumer_rejects_numeric_ids_outside_u32(self, identifier):
        """Test an out-of-range numeric id is refused when the consumer is built."""
        with pytest.raises(TypeError):
            Consumer.Single(identifier)
        with pytest.raises(TypeError):
            Consumer.Group(identifier)

    def test_consumer_accepts_valid_numeric_ids(self):
        """Test both consumer kinds keep the numeric id they were given."""
        assert Consumer.Single(1).id == 1
        assert Consumer.Group(4294967295).id == 4294967295

    @pytest.mark.asyncio
    @pytest.mark.parametrize("identifier", ["", "a" * 256], ids=["empty", "too-long"])
    @pytest.mark.parametrize(
        "consumer_kind", [Consumer.Single, Consumer.Group], ids=["single", "group"]
    )
    async def test_poll_messages_with_an_invalid_string_identifier_raises_value_error(
        self, iggy_client: IggyClient, unique_name, identifier, consumer_kind
    ):
        """Test a string id is validated when it is converted, not when it is built."""
        stream_name = unique_name()
        topic_name = unique_name()

        await iggy_client.create_stream(stream_name)
        await iggy_client.create_topic(stream=stream_name, name=topic_name)

        with pytest.raises(ValueError, match="Invalid identifier"):
            await iggy_client.poll_messages(
                stream=stream_name,
                topic=topic_name,
                consumer=consumer_kind(identifier),
                partition_id=0,
                polling_strategy=PollingStrategy.Next(),
                options=PollOptions(count=10, commit=CommitPolicy.OnRead()),
            )

    @pytest.mark.asyncio
    async def test_poll_messages_with_partition_id_reads_only_that_partition(
        self, iggy_client: IggyClient, unique_name
    ):
        """Test a regular consumer reads the given partition, not the whole topic."""
        stream_name = unique_name()
        topic_name = unique_name()
        partitions_count = 3

        await iggy_client.create_stream(stream_name)
        await iggy_client.create_topic(
            stream=stream_name,
            name=topic_name,
            options=TopicOptions(partitions_count=partitions_count),
        )
        for partition_id in range(partitions_count):
            await iggy_client.send_messages(
                stream=stream_name,
                topic=topic_name,
                partitioning=partition_id,
                messages=[Message(f"Partition {partition_id}")],
            )

        polled_messages = await iggy_client.poll_messages(
            stream=stream_name,
            topic=topic_name,
            consumer=Consumer.Single("partition-zero-consumer"),
            partition_id=0,
            polling_strategy=PollingStrategy.Next(),
            options=PollOptions(count=10, commit=CommitPolicy.OnRead()),
        )

        assert [
            (message.partition_id(), message.payload().decode("utf-8"))
            for message in polled_messages.messages
        ] == [(0, "Partition 0")]

    @pytest.mark.asyncio
    async def test_poll_messages_with_consumer_group_reads_every_assigned_partition(
        self, iggy_client: IggyClient, unique_name
    ):
        """Test a sole group member is assigned every partition and reads each one."""
        stream_name = unique_name()
        topic_name = unique_name()
        group_name = unique_name()
        partitions_count = 3

        await iggy_client.create_stream(stream_name)
        await iggy_client.create_topic(
            stream=stream_name,
            name=topic_name,
            options=TopicOptions(partitions_count=partitions_count),
        )
        await iggy_client.create_consumer_group(stream_name, topic_name, group_name)
        await iggy_client.join_consumer_group(stream_name, topic_name, group_name)
        await wait_for_consumer_group_assignment(
            iggy_client, stream_name, topic_name, group_name, members_count=1
        )
        for partition_id in range(partitions_count):
            await iggy_client.send_messages(
                stream=stream_name,
                topic=topic_name,
                partitioning=partition_id,
                messages=[Message(f"Partition {partition_id}")],
            )

        polled = []
        for partition_id in range(partitions_count):
            polled_messages = await iggy_client.poll_messages(
                stream=stream_name,
                topic=topic_name,
                consumer=Consumer.Group(group_name),
                partition_id=partition_id,
                polling_strategy=PollingStrategy.Next(),
                options=PollOptions(count=10, commit=CommitPolicy.OnRead()),
            )
            polled.extend(
                (message.partition_id(), message.payload().decode("utf-8"))
                for message in polled_messages.messages
            )

        assert polled == [
            (partition_id, f"Partition {partition_id}")
            for partition_id in range(partitions_count)
        ]

    @pytest.mark.asyncio
    async def test_poll_messages_with_an_unjoined_consumer_group_fails(
        self, iggy_client: IggyClient, unique_name
    ):
        """Test a group poll needs membership, since it has no assignment without it."""
        stream_name = unique_name()
        topic_name = unique_name()
        group_name = unique_name()

        await iggy_client.create_stream(stream_name)
        await iggy_client.create_topic(stream=stream_name, name=topic_name)
        await iggy_client.create_consumer_group(stream_name, topic_name, group_name)
        await iggy_client.send_messages(
            stream=stream_name,
            topic=topic_name,
            partitioning=0,
            messages=[Message("Unjoined group")],
        )

        # The group exists; only the join is missing.
        with pytest.raises(IggyError) as refusal:
            await iggy_client.poll_messages(
                stream=stream_name,
                topic=topic_name,
                consumer=Consumer.Group(group_name),
                partition_id=0,
                polling_strategy=PollingStrategy.Next(),
                options=PollOptions(count=10, commit=CommitPolicy.OnRead()),
            )
        assert refusal.value.code == CONSUMER_GROUP_PARTITION_NOT_OWNED

    @pytest.mark.asyncio
    async def test_poll_messages_with_consumer_group_reads_its_assigned_partition(
        self, iggy_client: IggyClient, unique_name
    ):
        """Test a group member reads the messages of the partition assigned to it."""
        stream_name = unique_name()
        topic_name = unique_name()
        group_name = unique_name()
        test_messages = [f"Group polling {i} - {unique_name()}" for i in range(3)]

        await iggy_client.create_stream(stream_name)
        await iggy_client.create_topic(stream=stream_name, name=topic_name)
        await iggy_client.create_consumer_group(stream_name, topic_name, group_name)
        await iggy_client.join_consumer_group(stream_name, topic_name, group_name)
        await wait_for_consumer_group_assignment(
            iggy_client, stream_name, topic_name, group_name, members_count=1
        )
        await iggy_client.send_messages(
            stream=stream_name,
            topic=topic_name,
            partitioning=0,
            messages=[Message(message) for message in test_messages],
        )

        polled_messages = await iggy_client.poll_messages(
            stream=stream_name,
            topic=topic_name,
            consumer=Consumer.Group(group_name),
            partition_id=0,
            polling_strategy=PollingStrategy.Next(),
            options=PollOptions(count=10, commit=CommitPolicy.OnRead()),
        )
        assert [
            message.payload().decode("utf-8") for message in polled_messages.messages
        ] == test_messages
