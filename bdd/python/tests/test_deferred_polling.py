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
Deferred polling BDD test implementation for Python SDK
"""

import asyncio
import concurrent.futures
import socket
import threading
import time
from collections.abc import AsyncIterator, Awaitable, Callable, Iterator
from dataclasses import dataclass
from datetime import timedelta
from typing import Any

import pytest
from apache_iggy import (
    AutoCommit,
    CommitAfter,
    CommitPolicy,
    Consumer,
    IggyClient,
    IggyConsumer,
    PartitionReader,
    PolledMessages,
    PollingStrategy,
    PollOptions,
    ReceiveMessage,
    SendMessage,
    TopicOptions,
)
from pytest_bdd import given, parsers, scenarios, then, when

scenarios("/app/features/deferred_polling.feature")

MESSAGES_COUNT = 10
# A message that reaches the server before the call answers it at once, so a
# start step waits until its call is parked.
PARK_TIME_SECONDS = 0.1
# The wait of a consumer whose step names none.
CONSUMER_MAX_WAIT = timedelta(seconds=10)
DEFAULT_CONSUMER = Consumer.Single(1)


@dataclass
class DeferredState:
    """The loop that runs the calls of a scenario and what they left open."""

    loop: asyncio.AbstractEventLoop
    background_client: IggyClient | None = None
    deferred_poll: concurrent.futures.Future[PolledMessages] | None = None
    deferred_poll_started: float | None = None
    reader: PartitionReader | None = None
    consumer: IggyConsumer | None = None
    consumer_messages: AsyncIterator[ReceiveMessage] | None = None

    def run(
        self, call: Callable[[], Awaitable[Any]], timeout: float | None = None
    ) -> Any:
        """Runs `call` on the scenario loop.

        The SDK binds an awaitable to the loop that runs when the call is made,
        so the call itself runs on that loop.
        """

        async def on_loop():
            return await call()

        future = asyncio.run_coroutine_threadsafe(on_loop(), self.loop)
        return future.result(timeout)


@pytest.fixture
def deferred() -> Iterator[DeferredState]:
    """One event loop for the scenario, so a call can outlive its step."""
    loop = asyncio.new_event_loop()
    thread = threading.Thread(target=loop.run_forever, daemon=True)
    thread.start()
    state = DeferredState(loop=loop)
    yield state
    if state.deferred_poll is not None:
        state.deferred_poll.cancel()
    if state.consumer is not None:
        state.run(state.consumer.shutdown, 30)
    if state.reader is not None:
        state.run(state.reader.close, 30)
    loop.call_soon_threadsafe(loop.stop)
    thread.join(5)
    loop.close()


@given(
    parsers.re(
        r'a stream named "(?P<stream_name>[^"]+)" with a topic named '
        r'"(?P<topic_name>[^"]+)" with (?P<partitions>\d+) partitions exists'
    ),
    converters={"partitions": int},
)
def stream_with_topic(context, deferred, stream_name, topic_name, partitions):
    async def create():
        if await context.client.get_stream(stream_name) is not None:
            await context.client.delete_stream(stream_name)
        await context.client.create_stream(stream_name)
        await context.client.create_topic(
            stream=stream_name,
            name=topic_name,
            options=TopicOptions(partitions_count=partitions),
        )
        stream = await context.client.get_stream(stream_name)
        topic = await context.client.get_topic(stream_name, topic_name)
        context.last_stream_id = stream.id
        context.last_stream_name = stream_name
        context.last_topic_id = topic.id
        context.last_topic_name = topic_name
        context.last_topic_partitions = partitions

    deferred.run(create)


@when(
    parsers.re(
        r"I start a deferred poll of partition (?P<partition>\d+) from offset "
        r"(?P<offset>\d+) with a max wait of (?P<seconds>\d+) seconds?"
    ),
    converters={"partition": int, "offset": int, "seconds": int},
)
def start_deferred_poll(
    context, deferred, root_credentials, partition, offset, seconds
):
    start_poll(
        context,
        deferred,
        root_credentials,
        partition,
        DEFAULT_CONSUMER,
        PollingStrategy.Offset(value=offset),
        PollOptions(count=MESSAGES_COUNT, max_wait=timedelta(seconds=seconds)),
    )


@when(
    parsers.re(
        r"I start a deferred poll of partition (?P<partition>\d+) for consumer "
        r'"(?P<name>[^"]+)" with the on read commit policy, a min count of '
        r"(?P<min_count>\d+) and a max wait of (?P<seconds>\d+) seconds?"
    ),
    converters={"partition": int, "min_count": int, "seconds": int},
)
def start_committing_deferred_poll(
    context, deferred, root_credentials, partition, name, min_count, seconds
):
    start_poll(
        context,
        deferred,
        root_credentials,
        partition,
        Consumer.Single(name),
        PollingStrategy.Next(),
        PollOptions(
            count=MESSAGES_COUNT,
            max_wait=timedelta(seconds=seconds),
            min_count=min_count,
            commit=CommitPolicy.OnRead(),
        ),
    )


@when(
    parsers.re(
        r'I send a message with payload "(?P<payload>[^"]+)" to partition '
        r"(?P<partition>\d+)"
    ),
    converters={"partition": int},
)
def send_to_partition(context, deferred, payload, partition):
    deferred.run(
        lambda: context.client.send_messages(
            stream=context.last_stream_id,
            topic=context.last_topic_id,
            partitioning=partition,
            messages=[SendMessage(payload)],
        )
    )


@then(
    parsers.re(
        r'the deferred poll should return the payload "(?P<payload>[^"]+)" '
        r"within (?P<seconds>\d+) seconds?"
    ),
    converters={"seconds": int},
)
def deferred_poll_returns(deferred, payload, seconds):
    poll = take_deferred_poll(deferred)
    try:
        polled = poll.result(seconds)
    except TimeoutError:
        pytest.fail(f"the deferred poll did not return within {seconds} s")
    assert payloads(polled) == [payload]


@then(
    parsers.re(
        r"the deferred poll should return no messages between (?P<low>\d+) and "
        r"(?P<high>\d+) seconds after it started"
    ),
    converters={"low": int, "high": int},
)
def deferred_poll_returns_nothing(deferred, low, high):
    started = deferred.deferred_poll_started
    poll = take_deferred_poll(deferred)
    try:
        polled = poll.result(max(0.0, started + high - time.monotonic()))
    except TimeoutError:
        pytest.fail(f"the deferred poll did not return within {high} s")
    elapsed = time.monotonic() - started
    assert payloads(polled) == []
    assert elapsed >= low, f"the poll returned after {elapsed:.3f} s, before {low} s"


@when(
    parsers.re(r"I cancel the deferred poll after (?P<milliseconds>\d+) milliseconds"),
    converters={"milliseconds": int},
)
def cancel_deferred_poll(deferred, milliseconds):
    time.sleep(milliseconds / 1000)
    poll = take_deferred_poll(deferred)
    # Cancelling the task drops the parked request, which sends the cancellation.
    assert poll.cancel(), "the deferred poll must still be parked when cancelled"


@when(parsers.re(r"I wait (?P<seconds>\d+) seconds?"), converters={"seconds": int})
def wait_seconds(seconds):
    time.sleep(seconds)


@then(
    parsers.re(
        r'a poll of partition (?P<partition>\d+) for consumer "(?P<name>[^"]+)" '
        r'with the next strategy should return the payload "(?P<payload>[^"]+)"'
    ),
    converters={"partition": int},
)
def next_poll_returns(context, deferred, partition, name, payload):
    poll_next_and_expect(
        context, deferred, partition, name, CommitPolicy.Manual(), payload
    )


@then(
    parsers.re(
        r'a poll of partition (?P<partition>\d+) for consumer "(?P<name>[^"]+)" '
        r"with the next strategy and the on read commit policy should return "
        r'the payload "(?P<payload>[^"]+)"'
    ),
    converters={"partition": int},
)
def on_read_poll_returns(context, deferred, partition, name, payload):
    poll_next_and_expect(
        context, deferred, partition, name, CommitPolicy.OnRead(), payload
    )


@when(
    parsers.re(
        r"I open a partition reader on partitions (?P<first>\d+), "
        r"(?P<second>\d+) and (?P<third>\d+) from offset (?P<offset>\d+) "
        r"with a max wait of (?P<seconds>\d+) seconds?"
    ),
    converters={
        "first": int,
        "second": int,
        "third": int,
        "offset": int,
        "seconds": int,
    },
)
def open_partition_reader(context, deferred, first, second, third, offset, seconds):
    deferred.reader = deferred.run(
        lambda: context.client.reader(
            context.last_stream_id,
            context.last_topic_id,
            consumer=DEFAULT_CONSUMER,
            partitions=[first, second, third],
            start=PollingStrategy.Offset(value=offset),
            options=PollOptions(
                count=MESSAGES_COUNT, max_wait=timedelta(seconds=seconds)
            ),
        )
    )


@then(
    parsers.re(
        r'the partition reader should return the payload "(?P<payload>[^"]+)" '
        r"from partition (?P<partition>\d+) within (?P<seconds>\d+) seconds?"
    ),
    converters={"partition": int, "seconds": int},
)
def partition_reader_returns(deferred, payload, partition, seconds):
    reader = deferred.reader
    assert reader is not None, "partition reader open"
    # The reader never yields an empty batch, so its first batch comes from the
    # first partition that gets a message.
    polled = within(deferred, reader.next, seconds, "the partition reader")
    assert polled.partition_id == partition
    assert payloads(polled) == [payload]


@when(
    parsers.re(
        r'I open a consumer in group "(?P<group>[^"]+)" on the topic with a max '
        r"wait of (?P<seconds>\d+) seconds?"
    ),
    converters={"seconds": int},
)
def open_group_consumer(context, deferred, group, seconds):
    options = PollOptions(count=MESSAGES_COUNT, max_wait=timedelta(seconds=seconds))
    open_consumer(context, deferred, group, options)


@when(
    parsers.re(
        r'I open a consumer in group "(?P<group>[^"]+)" on the topic that commits '
        r"after processing"
    )
)
def open_processing_group_consumer(context, deferred, group):
    defaults = AutoCommit.default()
    auto_commit = AutoCommit(
        interval=defaults.interval,
        every=defaults.every,
        after=CommitAfter.Processing,
    )
    options = PollOptions(
        count=MESSAGES_COUNT,
        max_wait=CONSUMER_MAX_WAIT,
        commit=CommitPolicy.Auto(auto_commit),
    )
    open_consumer(context, deferred, group, options)


@then(
    parsers.re(
        r'the consumer should return the payload "(?P<payload>[^"]+)" within '
        r"(?P<seconds>\d+) seconds?"
    ),
    converters={"seconds": int},
)
def consumer_returns(deferred, payload, seconds):
    messages = deferred.consumer_messages
    assert messages is not None, "consumer open"
    message = within(deferred, lambda: anext(messages), seconds, "the consumer")
    assert message.payload().decode() == payload


@when("I close the consumer")
def close_consumer(deferred):
    consumer = deferred.consumer
    assert consumer is not None, "consumer open"
    deferred.consumer = None
    deferred.consumer_messages = None
    deferred.run(consumer.shutdown)


def start_poll(
    context, deferred, root_credentials, partition, consumer, strategy, options
):
    client = background_client(context, deferred, root_credentials)
    stream, topic = context.last_stream_id, context.last_topic_id
    deferred.deferred_poll_started = time.monotonic()
    deferred.deferred_poll = asyncio.run_coroutine_threadsafe(
        poll_once(client, stream, topic, partition, consumer, strategy, options),
        deferred.loop,
    )
    time.sleep(PARK_TIME_SECONDS)
    assert not deferred.deferred_poll.done(), "the deferred poll must park"


async def poll_once(client, stream, topic, partition, consumer, strategy, options):
    return await client.poll_messages(
        stream=stream,
        topic=topic,
        partition_id=partition,
        consumer=consumer,
        polling_strategy=strategy,
        options=options,
    )


def background_client(context, deferred, root_credentials) -> IggyClient:
    """A parked poll holds its connection, so it runs on a client of its own.

    The state keeps that client, so that a cancelled call still has a link for
    its cancellation.
    """
    if deferred.background_client is None:
        host, port = context.server_addr.split(":")
        client = IggyClient(f"{socket.gethostbyname(host)}:{port}")

        async def connect():
            await client.connect()
            await client.login_user(*root_credentials)

        deferred.run(connect)
        deferred.background_client = client
    return deferred.background_client


def take_deferred_poll(deferred) -> concurrent.futures.Future[PolledMessages]:
    poll = deferred.deferred_poll
    assert poll is not None, "deferred poll started"
    deferred.deferred_poll = None
    return poll


def poll_next_and_expect(context, deferred, partition, name, commit, payload):
    """Polls one message from `Next`, so a poll that commits stores only the
    offset of the payload it returns."""
    polled = deferred.run(
        lambda: context.client.poll_messages(
            stream=context.last_stream_id,
            topic=context.last_topic_id,
            partition_id=partition,
            consumer=Consumer.Single(name),
            polling_strategy=PollingStrategy.Next(),
            options=PollOptions(count=1, commit=commit),
        )
    )
    assert payloads(polled)[:1] == [payload]


def open_consumer(context, deferred, group, options):
    async def open_group():
        consumer = await context.client.consumer_group(
            group,
            context.last_stream_name,
            context.last_topic_name,
            poll_options=options,
        )
        return consumer, consumer.iter_messages()

    deferred.consumer, deferred.consumer_messages = deferred.run(open_group)


def within(deferred, call, seconds, name):
    async def wait():
        return await asyncio.wait_for(call(), seconds)

    try:
        return deferred.run(wait)
    except TimeoutError:
        pytest.fail(f"{name} did not return within {seconds} s")


def payloads(polled: PolledMessages) -> list[str]:
    return [message.payload().decode() for message in polled.messages]
