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
Basic messaging BDD test implementation for Python SDK
"""
import asyncio
import socket
from pytest_bdd import scenarios, given, when, then, parsers
from apache_iggy import IggyClient, SendMessage, PollingStrategy

# Load scenarios from the shared feature file
scenarios('/app/features/basic_messaging.feature')


@given('I have a running Iggy server')
def running_server(context):
    """Ensure we have a running Iggy server and create client"""
    async def _connect():
        # Resolve hostname to IP if needed
        host, port = context.server_addr.split(':')
        try:
            # Try to resolve hostname to IP
            ip_addr = socket.gethostbyname(host)
            resolved_addr = f"{ip_addr}:{port}"
        except socket.gaierror:
            # If resolution fails, use as-is (might be an IP already)
            resolved_addr = context.server_addr

        context.client = IggyClient(resolved_addr)
        await context.client.connect()
        await context.client.ping()  # Health check

    asyncio.run(_connect())


@given('I am authenticated as the root user')
def authenticated_root_user(context):
    """Authenticate as root user"""
    async def _login():
        await context.client.login_user("iggy", "iggy")

    asyncio.run(_login())


@given('I have no streams in the system')
def no_streams_in_system(context):
    """Ensure no streams exist in the system"""
    # With --fresh flag on server, this should already be clean
    # Just verify by attempting to get a stream that shouldn't exist
    pass


@when(parsers.parse('I create a stream with name {stream_name}'))
def create_stream(context, stream_name):
    """Create a stream with specified name"""
    async def _create():
        stream = await context.client.create_stream(name=stream_name)
        context.last_stream_id = stream.id
        context.last_stream_name = stream_name

    asyncio.run(_create())


@then('the stream should be created successfully')
def stream_created_successfully(context):
    """Verify stream was created successfully"""
    async def _verify():
        stream = await context.client.get_stream(context.last_stream_name)
        assert stream is not None

    asyncio.run(_verify())


@then(parsers.parse('the stream should have name {stream_name}'))
def verify_stream_properties(context, stream_name):
    """Verify stream has correct name"""
    async def _verify():
        stream = await context.client.get_stream(stream_name)
        assert stream is not None
        assert stream.name == stream_name

    asyncio.run(_verify())


@when(parsers.parse('I create a topic with name {topic_name} in stream {stream_id:d} with {partitions:d} partitions'))
def create_topic(context, topic_name, stream_id, partitions):
    """Create a topic with specified parameters"""
    async def _create():
        topic = await context.client.create_topic(
            stream=stream_id,
            name=topic_name,
            partitions_count=partitions
        )
        context.last_topic_id = topic.id
        context.last_topic_name = topic_name
        context.last_topic_partitions = partitions

    asyncio.run(_create())


@then('the topic should be created successfully')
def topic_created_successfully(context):
    """Verify topic was created successfully"""
    async def _verify():
        topic = await context.client.get_topic(context.last_stream_id, context.last_topic_name)
        assert topic is not None

    asyncio.run(_verify())


@then(parsers.parse('the topic should have name {topic_name}'))
def verify_topic_properties(context, topic_name):
    """Verify topic has correct name"""
    async def _verify():
        topic = await context.client.get_topic(context.last_stream_id, topic_name)
        assert topic is not None
        assert topic.name == topic_name

    asyncio.run(_verify())


@then(parsers.parse('the topic should have {partitions:d} partitions'))
def verify_topic_partitions(context, partitions):
    """Verify topic has correct number of partitions"""
    async def _verify():
        topic = await context.client.get_topic(context.last_stream_id, context.last_topic_name)
        assert topic is not None
        assert topic.partitions_count == partitions

    asyncio.run(_verify())


@when(parsers.parse('I send {message_count:d} messages to stream {stream_id:d}, topic {topic_id:d}, partition {partition_id:d}'))
def send_messages(context, message_count, stream_id, topic_id, partition_id):
    """Send messages to specified stream, topic, and partition"""
    async def _send():
        messages = []
        for i in range(message_count):
            content = f"test message {i}"
            messages.append(SendMessage(content))

        await context.client.send_messages(
            stream=stream_id,
            topic=topic_id,
            partitioning=partition_id,
            messages=messages
        )

        # Store the last sent message content for comparison
        if messages:
            context.last_sent_message = f"test message {message_count - 1}"

    asyncio.run(_send())


@then('all messages should be sent successfully')
def messages_sent_successfully(context):
    """Verify all messages were sent successfully"""
    # If we got here without exception, messages were sent successfully
    assert context.last_sent_message is not None


@when(parsers.parse('I poll messages from stream {stream_id:d}, topic {topic_id:d}, partition {partition_id:d} starting from offset {start_offset:d}'))
def poll_messages(context, stream_id, topic_id, partition_id, start_offset):
    """Poll messages from specified location"""
    async def _poll():
        context.last_polled_messages = await context.client.poll_messages(
            stream=stream_id,
            topic=topic_id,
            partition_id=partition_id,
            polling_strategy=PollingStrategy.Offset(value=start_offset),
            count=100,  # Poll up to 100 messages
            auto_commit=True
        )

    asyncio.run(_poll())


@then(parsers.parse('I should receive {expected_count:d} messages'))
def verify_message_count(context, expected_count):
    """Verify correct number of messages received"""
    assert context.last_polled_messages is not None
    assert len(context.last_polled_messages) == expected_count


@then(parsers.parse('the messages should have sequential offsets from {start_offset:d} to {end_offset:d}'))
def verify_sequential_offsets(context, start_offset, end_offset):
    """Verify messages have sequential offsets"""
    assert context.last_polled_messages is not None

    for i, message in enumerate(context.last_polled_messages):
        expected_offset = start_offset + i
        assert message.offset() == expected_offset

    last_message = context.last_polled_messages[-1]
    assert last_message.offset() == end_offset


@then('each message should have the expected payload content')
def verify_payload_content(context):
    """Verify each message has expected payload content"""
    assert context.last_polled_messages is not None

    for i, message in enumerate(context.last_polled_messages):
        expected_payload = f"test message {i}"
        actual_payload = message.payload().decode('utf-8')
        assert actual_payload == expected_payload


@then('the last polled message should match the last sent message')
def verify_last_message_match(context):
    """Verify last polled message matches last sent message"""
    assert context.last_sent_message is not None
    assert context.last_polled_messages is not None

    last_polled = context.last_polled_messages[-1]
    last_polled_payload = last_polled.payload().decode('utf-8')

    assert last_polled_payload == context.last_sent_message
