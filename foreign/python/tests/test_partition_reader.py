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
from datetime import timedelta

import pytest

from apache_iggy import (
    Consumer,
    IggyClient,
    IggyError,
    PartitionReader,
    PolledMessages,
    PollingStrategy,
    PollOptions,
    ReadError,
    ReceiveMessage,
    ServerCommitPolicy,
    TopicOptions,
)
from apache_iggy import SendMessage as Message

from .utils import read_offsets_with_cancels, wait_for_consumer_group_assignment

INVALID_COMMAND = 3
FEATURE_UNAVAILABLE = 5
HISTORY_UNAVAILABLE = 87
READ_TIMEOUT_SECONDS = 10
CONSUMER_ID = 1
PARTITION_ID = 0
PARTITIONS = [0, 1, 2]
MESSAGES_PER_PARTITION = 5
# Below MESSAGES_PER_PARTITION, so each partition arrives in several batches.
SMALL_BATCH = 2
# A reader without a wait polls an idle topic in a busy loop.
POLL_WAIT = timedelta(seconds=1)
# One message per batch and one batch of prefetch, so most reads wait for the server.
ONE_MESSAGE = 1
CANCELLED_READS_MESSAGES = 100
LAST_CANCELLED_READS_OFFSET = CANCELLED_READS_MESSAGES - 1


async def _create_topic(
    iggy_client: IggyClient, unique_name, partitions_count: int = 1
) -> tuple[str, str]:
    stream_name = unique_name()
    topic_name = unique_name()
    await iggy_client.create_stream(stream_name)
    await iggy_client.create_topic(
        stream=stream_name,
        name=topic_name,
        options=TopicOptions(partitions_count=partitions_count),
    )
    return stream_name, topic_name


async def _send(
    iggy_client: IggyClient,
    stream_name: str,
    topic_name: str,
    partitions: list[int],
    label: str = "message",
    count: int = MESSAGES_PER_PARTITION,
) -> dict[int, list[tuple[int, str]]]:
    """Sends messages to empty partitions and returns them as `_by_partition` does."""
    sent: dict[int, list[tuple[int, str]]] = {}
    for partition_id in partitions:
        payloads = [
            f"{label} {index} of partition {partition_id}" for index in range(count)
        ]
        await iggy_client.send_messages(
            stream_name,
            topic_name,
            partition_id,
            [Message(payload) for payload in payloads],
        )
        sent[partition_id] = list(enumerate(payloads))
    return sent


async def _next(reader: PartitionReader) -> PolledMessages:
    """`next()` has no time limit, so each read is bounded here."""
    return await asyncio.wait_for(reader.next(), READ_TIMEOUT_SECONDS)


async def _read(reader: PartitionReader, count: int) -> list[ReceiveMessage]:
    """Reads batches until they hold `count` messages, and returns the messages."""
    messages: list[ReceiveMessage] = []
    while len(messages) < count:
        batch = await _next(reader)
        assert batch.messages, "A reader never returns an empty batch"
        messages.extend(batch.messages)
    return messages


async def _batch_offsets(reader: PartitionReader) -> list[int]:
    return [message.offset() for message in (await reader.next()).messages]


def _by_partition(messages: list[ReceiveMessage]) -> dict[int, list[tuple[int, str]]]:
    """The offset and payload of each message by partition, in the order read."""
    received: dict[int, list[tuple[int, str]]] = {}
    for message in messages:
        received.setdefault(message.partition_id(), []).append(
            (message.offset(), message.payload().decode())
        )
    return received


class TestPartitionReader:
    """Test the partition reader built by IggyClient.reader()."""

    @pytest.mark.asyncio
    async def test_reader_reads_every_message_of_its_partitions(
        self, iggy_client: IggyClient, unique_name
    ):
        """Test a reader reads each of its partitions whole and in offset order."""
        stream_name, topic_name = await _create_topic(
            iggy_client, unique_name, partitions_count=len(PARTITIONS)
        )
        sent = await _send(iggy_client, stream_name, topic_name, PARTITIONS)

        reader = await iggy_client.reader(
            stream_name,
            topic_name,
            consumer=Consumer.Single(CONSUMER_ID),
            partitions=PARTITIONS,
            options=PollOptions(count=SMALL_BATCH, max_wait=POLL_WAIT),
        )
        try:
            partitions = await reader.partitions()
            messages = await _read(reader, len(PARTITIONS) * MESSAGES_PER_PARTITION)
        finally:
            await reader.close()

        assert partitions == PARTITIONS
        assert _by_partition(messages) == sent

    @pytest.mark.asyncio
    async def test_reader_resumes_after_the_committed_checkpoint(
        self, iggy_client: IggyClient, unique_name
    ):
        """Test a new reader at Next reads on after the committed checkpoint."""
        committed_offset = 2
        stream_name, topic_name = await _create_topic(iggy_client, unique_name)
        sent = await _send(iggy_client, stream_name, topic_name, [PARTITION_ID])

        reader = await iggy_client.reader(
            stream_name,
            topic_name,
            consumer=Consumer.Single(CONSUMER_ID),
            partitions=[PARTITION_ID],
        )
        try:
            delivered = await _read(reader, MESSAGES_PER_PARTITION)
            await reader.commit(delivered[committed_offset].checkpoint())
        finally:
            await reader.close()
        resumed = await iggy_client.reader(
            stream_name,
            topic_name,
            consumer=Consumer.Single(CONSUMER_ID),
            partitions=[PARTITION_ID],
            start=PollingStrategy.Next(),
        )
        try:
            messages = await _read(
                resumed, MESSAGES_PER_PARTITION - committed_offset - 1
            )
        finally:
            await resumed.close()

        assert _by_partition(messages) == {
            PARTITION_ID: sent[PARTITION_ID][committed_offset + 1 :]
        }

    @pytest.mark.asyncio
    async def test_seek_reads_the_partition_again_from_the_new_start(
        self, iggy_client: IggyClient, unique_name
    ):
        """Test seek moves the next read of a partition back to the new start."""
        seek_offset = 1
        stream_name, topic_name = await _create_topic(iggy_client, unique_name)
        sent = await _send(iggy_client, stream_name, topic_name, [PARTITION_ID])

        reader = await iggy_client.reader(
            stream_name,
            topic_name,
            consumer=Consumer.Single(CONSUMER_ID),
            partitions=[PARTITION_ID],
        )
        try:
            await _read(reader, MESSAGES_PER_PARTITION)
            await reader.seek(PARTITION_ID, PollingStrategy.Offset(value=seek_offset))
            messages = await _read(reader, MESSAGES_PER_PARTITION - seek_offset)
        finally:
            await reader.close()

        assert _by_partition(messages) == {
            PARTITION_ID: sent[PARTITION_ID][seek_offset:]
        }

    @pytest.mark.asyncio
    async def test_seek_refuses_a_partition_the_reader_does_not_read(
        self, iggy_client: IggyClient, unique_name
    ):
        """Test seek of another partition of the topic raises InvalidCommand."""
        other_partition_id = 1
        stream_name, topic_name = await _create_topic(
            iggy_client, unique_name, partitions_count=2
        )
        reader = await iggy_client.reader(
            stream_name,
            topic_name,
            consumer=Consumer.Single(CONSUMER_ID),
            partitions=[PARTITION_ID],
        )
        try:
            with pytest.raises(IggyError) as refusal:
                await reader.seek(other_partition_id, PollingStrategy.First())
        finally:
            await reader.close()

        assert refusal.value.code == INVALID_COMMAND

    @pytest.mark.asyncio
    async def test_close_is_safe_twice_and_ends_the_reader(
        self, iggy_client: IggyClient, unique_name
    ):
        """Test a second close does nothing and a read after close fails."""
        stream_name, topic_name = await _create_topic(iggy_client, unique_name)
        reader = await iggy_client.reader(
            stream_name,
            topic_name,
            consumer=Consumer.Single(CONSUMER_ID),
            partitions=[PARTITION_ID],
        )

        await reader.close()
        await reader.close()

        with pytest.raises(RuntimeError, match="reader has been closed"):
            await _next(reader)

    @pytest.mark.asyncio
    async def test_reader_refuses_an_invalid_configuration(
        self, iggy_client: IggyClient, unique_name
    ):
        """Test reader() refuses wrong partitions and a continuation as start."""
        consumer = Consumer.Single(CONSUMER_ID)
        stream_name, topic_name = await _create_topic(iggy_client, unique_name)
        await _send(iggy_client, stream_name, topic_name, [PARTITION_ID])
        polled = await iggy_client.poll_messages(
            stream_name,
            topic_name,
            partition_id=PARTITION_ID,
            consumer=consumer,
            polling_strategy=PollingStrategy.First(),
        )

        with pytest.raises(IggyError) as without_partitions:
            await iggy_client.reader(stream_name, topic_name, consumer=consumer)
        with pytest.raises(IggyError) as empty_partitions:
            await iggy_client.reader(
                stream_name, topic_name, consumer=consumer, partitions=[]
            )
        with pytest.raises(IggyError) as group_partitions:
            await iggy_client.reader(
                stream_name,
                topic_name,
                consumer=Consumer.Group(unique_name()),
                partitions=[PARTITION_ID],
            )
        with pytest.raises(IggyError) as continuation_start:
            await iggy_client.reader(
                stream_name,
                topic_name,
                consumer=consumer,
                partitions=[PARTITION_ID],
                start=polled.continuation(),
            )

        assert without_partitions.value.code == INVALID_COMMAND
        assert empty_partitions.value.code == INVALID_COMMAND
        assert group_partitions.value.code == INVALID_COMMAND
        assert continuation_start.value.code == INVALID_COMMAND

    @pytest.mark.asyncio
    async def test_commit_refuses_a_checkpoint_it_cannot_store(
        self, iggy_client: IggyClient, unique_name
    ):
        """Test commit refuses a polled or foreign checkpoint, and any under OnRead."""
        stream_name, topic_name = await _create_topic(iggy_client, unique_name)
        await _send(iggy_client, stream_name, topic_name, [PARTITION_ID])
        polled = await iggy_client.poll_messages(
            stream_name,
            topic_name,
            partition_id=PARTITION_ID,
            consumer=Consumer.Single(CONSUMER_ID),
            polling_strategy=PollingStrategy.First(),
        )
        reader = await iggy_client.reader(
            stream_name,
            topic_name,
            consumer=Consumer.Single(CONSUMER_ID),
            partitions=[PARTITION_ID],
        )
        on_read_reader = await iggy_client.reader(
            stream_name,
            topic_name,
            consumer=Consumer.Single(CONSUMER_ID),
            partitions=[PARTITION_ID],
            options=PollOptions(
                max_wait=POLL_WAIT, server_commit=ServerCommitPolicy.OnRead
            ),
        )
        try:
            delivered = (await _read(on_read_reader, 1))[0].checkpoint()
            with pytest.raises(IggyError) as polled_refusal:
                await reader.commit(polled.messages[0].checkpoint())
            with pytest.raises(IggyError) as other_reader_refusal:
                await reader.commit(delivered)
            with pytest.raises(IggyError) as on_read_refusal:
                await on_read_reader.commit(delivered)
        finally:
            await reader.close()
            await on_read_reader.close()

        assert polled_refusal.value.code == INVALID_COMMAND
        assert other_reader_refusal.value.code == INVALID_COMMAND
        assert on_read_refusal.value.code == INVALID_COMMAND

    @pytest.mark.asyncio
    async def test_group_reader_reads_its_assignment_and_leaves_in_close(
        self, iggy_client: IggyClient, unique_name
    ):
        """Test a group reader owns every partition alone and leaves in close."""
        group_name = unique_name()
        stream_name, topic_name = await _create_topic(
            iggy_client, unique_name, partitions_count=len(PARTITIONS)
        )
        await iggy_client.create_consumer_group(stream_name, topic_name, group_name)
        sent = await _send(iggy_client, stream_name, topic_name, PARTITIONS)

        reader = await iggy_client.reader(
            stream_name, topic_name, consumer=Consumer.Group(group_name)
        )
        try:
            await wait_for_consumer_group_assignment(
                iggy_client, stream_name, topic_name, group_name, members_count=1
            )
            joined = await iggy_client.get_consumer_group(
                stream_name, topic_name, group_name
            )
            messages = await _read(reader, len(PARTITIONS) * MESSAGES_PER_PARTITION)
            assigned = await reader.partitions()
        finally:
            await reader.close()
        left = await iggy_client.get_consumer_group(stream_name, topic_name, group_name)

        assert joined is not None
        assert [sorted(member.partitions) for member in joined.members] == [PARTITIONS]
        assert assigned == PARTITIONS
        assert _by_partition(messages) == sent
        assert left is not None
        assert left.members_count == 0
        assert left.members == []

    @pytest.mark.asyncio
    @pytest.mark.parametrize("transport_client", ["http"], indirect=True)
    async def test_group_reader_is_unavailable_over_http(
        self, iggy_client: IggyClient, transport_client: IggyClient, unique_name
    ):
        """Test a group reader over HTTP raises FeatureUnavailable."""
        group_name = unique_name()
        stream_name, topic_name = await _create_topic(iggy_client, unique_name)
        await iggy_client.create_consumer_group(stream_name, topic_name, group_name)

        with pytest.raises(IggyError) as refusal:
            await transport_client.reader(
                stream_name, topic_name, consumer=Consumer.Group(group_name)
            )

        assert refusal.value.code == FEATURE_UNAVAILABLE

    @pytest.mark.asyncio
    async def test_partition_created_again_is_refused_until_seek(
        self, iggy_client: IggyClient, unique_name
    ):
        """Test a partition created again raises HistoryUnavailable until seek."""
        stream_name, topic_name = await _create_topic(iggy_client, unique_name)
        await _send(iggy_client, stream_name, topic_name, [PARTITION_ID], "old")
        reader = await iggy_client.reader(
            stream_name,
            topic_name,
            consumer=Consumer.Single(CONSUMER_ID),
            partitions=[PARTITION_ID],
        )
        refusals: list[ReadError] = []
        messages: list[ReceiveMessage] = []
        try:
            await _read(reader, MESSAGES_PER_PARTITION)
            await iggy_client.delete_partitions(
                stream_name, topic_name, partitions_count=1
            )
            await iggy_client.create_partitions(
                stream_name, topic_name, partitions_count=1
            )
            sent = await _send(
                iggy_client, stream_name, topic_name, [PARTITION_ID], "new"
            )
            while len(messages) < MESSAGES_PER_PARTITION:
                try:
                    batch = await _next(reader)
                except ReadError as error:
                    # A read while the partition did not exist fails with another
                    # error, and the reader retries it.
                    if error.code == HISTORY_UNAVAILABLE:
                        refusals.append(error)
                        await reader.seek(PARTITION_ID, PollingStrategy.First())
                    continue
                assert refusals, "A batch came before the refusal of the old history"
                messages.extend(batch.messages)
        finally:
            await reader.close()

        assert [(refusal.partition_id, refusal.error.code) for refusal in refusals] == [
            (PARTITION_ID, HISTORY_UNAVAILABLE)
        ]
        assert _by_partition(messages) == sent

    @pytest.mark.asyncio
    async def test_cancelled_reads_lose_and_repeat_no_batch(
        self, iggy_client: IggyClient, unique_name
    ):
        """Test reads cancelled at short deadlines return each offset once, in order."""
        stream_name, topic_name = await _create_topic(iggy_client, unique_name)
        await _send(
            iggy_client,
            stream_name,
            topic_name,
            [PARTITION_ID],
            count=CANCELLED_READS_MESSAGES,
        )
        reader = await iggy_client.reader(
            stream_name,
            topic_name,
            consumer=Consumer.Single(CONSUMER_ID),
            partitions=[PARTITION_ID],
            options=PollOptions(count=ONE_MESSAGE, max_wait=POLL_WAIT),
            prefetch_messages=ONE_MESSAGE,
        )
        try:
            offsets = await read_offsets_with_cancels(
                lambda: _batch_offsets(reader), LAST_CANCELLED_READS_OFFSET
            )
        finally:
            await reader.close()

        assert offsets == list(range(CANCELLED_READS_MESSAGES))

    @pytest.mark.asyncio
    async def test_on_read_close_after_a_cancelled_read_resumes_after_the_last_batch(
        self, iggy_client: IggyClient, unique_name
    ):
        """Test a new reader starts after the last batch that an OnRead reader returned
        before a cancelled read and close."""
        stream_name, topic_name = await _create_topic(iggy_client, unique_name)
        await _send(
            iggy_client,
            stream_name,
            topic_name,
            [PARTITION_ID],
            count=CANCELLED_READS_MESSAGES,
        )
        reader = await iggy_client.reader(
            stream_name,
            topic_name,
            consumer=Consumer.Single(CONSUMER_ID),
            partitions=[PARTITION_ID],
            options=PollOptions(
                count=ONE_MESSAGE,
                max_wait=POLL_WAIT,
                server_commit=ServerCommitPolicy.OnRead,
            ),
            prefetch_messages=ONE_MESSAGE,
        )
        try:
            offsets = await read_offsets_with_cancels(
                lambda: _batch_offsets(reader), LAST_CANCELLED_READS_OFFSET // 2
            )
            pending = asyncio.ensure_future(reader.next())
            # One step polls the read: it returns a queued batch or waits for a reply.
            await asyncio.sleep(0)
            pending.cancel()
            await asyncio.wait([pending])
            if not pending.cancelled():
                offsets.extend(
                    message.offset() for message in pending.result().messages
                )
        finally:
            await reader.close()
        resumed = await iggy_client.reader(
            stream_name,
            topic_name,
            consumer=Consumer.Single(CONSUMER_ID),
            partitions=[PARTITION_ID],
        )
        try:
            offsets.extend(
                await read_offsets_with_cancels(
                    lambda: _batch_offsets(resumed), LAST_CANCELLED_READS_OFFSET
                )
            )
        finally:
            await resumed.close()

        assert offsets == list(range(CANCELLED_READS_MESSAGES))
