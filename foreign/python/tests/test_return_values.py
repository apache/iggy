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
    AutoCommit,
    IggyClient,
    PollingStrategy,
    ReceiveMessage,
    SendMessage,
    UserStatus,
)

from .utils import get_server_config, unique_credentials, wait_for_ping

PARTITION_ID = 0


@pytest.mark.asyncio
async def test_methods_declared_awaitable_none_resolve_to_none(
    iggy_client: IggyClient, unique_name
):
    """Every method declared as `Awaitable[None]` resolves to `None`, not `()`."""
    stream_name = unique_name()
    renamed_stream = unique_name()
    topic_name = unique_name()
    renamed_topic = unique_name()
    group_name = unique_name()
    consumer_name = unique_name()

    assert await iggy_client.ping() is None

    assert await iggy_client.create_stream(stream_name) is None
    assert await iggy_client.update_stream(stream_name, renamed_stream) is None
    assert (
        await iggy_client.create_topic(
            stream=renamed_stream, name=topic_name, partitions_count=1
        )
        is None
    )
    assert (
        await iggy_client.update_topic(renamed_stream, topic_name, renamed_topic)
        is None
    )
    assert await iggy_client.create_partitions(renamed_stream, renamed_topic, 1) is None
    assert await iggy_client.delete_partitions(renamed_stream, renamed_topic, 1) is None
    assert (
        await iggy_client.delete_segments(
            renamed_stream, renamed_topic, PARTITION_ID, 1
        )
        is None
    )

    assert (
        await iggy_client.create_consumer_group(
            renamed_stream, renamed_topic, group_name
        )
        is None
    )
    assert (
        await iggy_client.join_consumer_group(renamed_stream, renamed_topic, group_name)
        is None
    )
    assert (
        await iggy_client.leave_consumer_group(
            renamed_stream, renamed_topic, group_name
        )
        is None
    )
    assert (
        await iggy_client.delete_consumer_group(
            renamed_stream, renamed_topic, group_name
        )
        is None
    )

    consumer = await iggy_client.consumer_group(
        consumer_name,
        renamed_stream,
        renamed_topic,
        PARTITION_ID,
        PollingStrategy.First(),
        10,
        auto_commit=AutoCommit.Disabled(),
        poll_interval=timedelta(milliseconds=25),
    )
    await iggy_client.send_messages(
        renamed_stream, renamed_topic, PARTITION_ID, [SendMessage(unique_name())]
    )
    received: list[ReceiveMessage] = []
    shutdown_event = asyncio.Event()

    async def take(message: ReceiveMessage) -> None:
        received.append(message)
        shutdown_event.set()

    assert await consumer.consume_messages(take, shutdown_event) is None
    assert len(received) == 1
    assert await consumer.store_offset(received[0].offset(), PARTITION_ID) is None
    assert await consumer.store_position(received[0].position()) is None
    assert await consumer.delete_offset(PARTITION_ID) is None

    producer = await iggy_client.producer(renamed_stream, renamed_topic)
    assert await producer.shutdown() is None

    username, password = unique_credentials(unique_name)
    _, new_password = unique_credentials(unique_name)
    user = await iggy_client.create_user(username, password)
    assert await iggy_client.update_user(user.id, status=UserStatus.Active) is None
    assert await iggy_client.update_permissions(user.id, None) is None
    assert await iggy_client.change_password(user.id, password, new_password) is None

    host, port = get_server_config()
    user_client = IggyClient(f"{host}:{port}")
    assert await user_client.connect() is None
    await wait_for_ping(user_client)
    await user_client.login_user(username, new_password)
    assert await user_client.logout_user() is None
    assert await user_client.disconnect() is None

    other_client = IggyClient(f"{host}:{port}")
    assert await other_client.connect() is None
    assert await other_client.shutdown() is None

    assert await iggy_client.delete_user(user.id) is None
    assert await iggy_client.delete_topic(renamed_stream, renamed_topic) is None
    assert await iggy_client.delete_stream(renamed_stream) is None
