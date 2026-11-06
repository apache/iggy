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

"""The methods of the agent: decode the arguments, call the SDK and return the canonical
form of the result. The keyword parameters of a method are its arguments, so an argument
that the agent does not know fails the call instead of being ignored."""

from typing import Any

from apache_iggy import (
    IggyClient,
    IggyConsumer,
    PartitionReader,
    PollOptions,
    StreamOptions,
    TopicOptions,
)

import args
import canonical
import metadata
from replies import Failure, Unsupported


async def call(
    agent, entity: str, method: str, arguments: dict[str, Any], save_as: str | None
):
    if method == "next":
        if arguments:
            raise Failure(f"next takes no arguments, got {sorted(arguments)}")
        return await _next(agent, entity)
    client = agent.clients.get(entity)
    if client is None:
        raise Failure(f"no client {entity!r}")
    if method in ("build_reader", "build_consumer") and save_as is None:
        raise Failure(f"{method} needs save_as")
    if method == "build_reader":
        agent.readers[save_as] = await build_reader(client, **arguments)
        return None
    if method == "build_consumer":
        agent.consumers[save_as] = await build_consumer(client, **arguments)
        return None
    if method not in METHODS:
        raise Unsupported(f"the method {method}")
    return await METHODS[method](client, **arguments)


async def _next(agent, entity: str):
    reader = agent.readers.get(entity)
    if reader is not None:
        return canonical.polled(await reader.next())
    consumer = agent.consumers.get(entity)
    if consumer is None:
        raise Failure(f"no reader or consumer {entity!r}")
    try:
        received = await anext(consumer.iter_messages())
    except StopAsyncIteration:
        raise Failure("the consumer stream ended") from None
    return {
        "partition_id": received.partition_id(),
        "message": canonical.message(received),
    }


async def connect(client: IggyClient):
    await client.connect()


async def disconnect(client: IggyClient):
    await client.disconnect()


async def ping(client: IggyClient):
    await client.ping()


async def login_user(client: IggyClient, *, username, password):
    identity = await client.login_user(username, password)
    return {"user_id": identity.user_id}


async def logout_user(client: IggyClient):
    await client.logout_user()


async def get_stream(client: IggyClient, *, stream_id):
    stream = await client.get_stream(stream_id)
    return None if stream is None else canonical.stream_details(stream)


async def get_streams(client: IggyClient):
    return [canonical.stream(stream) for stream in await client.get_streams()]


async def create_stream(client: IggyClient, *, name, encryption_key_hex=None):
    options = None
    if encryption_key_hex is not None:
        options = StreamOptions(encryption_key=bytes.fromhex(encryption_key_hex))
    return canonical.stream_details(await client.create_stream(name, options))


async def update_stream(client: IggyClient, *, stream_id, name=None, reset=()):
    await client.update_stream(stream_id, name=name, raw_reset=list(reset))


async def delete_stream(client: IggyClient, *, stream_id):
    await client.delete_stream(stream_id)


async def get_topic(client: IggyClient, *, stream_id, topic_id):
    topic = await client.get_topic(stream_id, topic_id)
    return None if topic is None else canonical.topic_details(topic)


async def get_topics(client: IggyClient, *, stream_id):
    return [canonical.topic(topic) for topic in await client.get_topics(stream_id)]


async def create_topic(
    client: IggyClient,
    *,
    stream_id,
    name,
    partitions_count=None,
    message_expiry=None,
    max_topic_size=None,
):
    options = {}
    if partitions_count is not None:
        options["partitions_count"] = partitions_count
    if message_expiry is not None:
        options["message_expiry"] = args.expiry(message_expiry)
    if max_topic_size is not None:
        options["max_topic_size"] = args.topic_size(max_topic_size)
    topic = await client.create_topic(stream_id, name, TopicOptions(**options))
    # The SDK returns no topic, and the answer says so.
    return None if _no_value(topic) else canonical.topic_details(topic)


async def update_topic(
    client: IggyClient,
    *,
    stream_id,
    topic_id,
    name=None,
    message_expiry=None,
    max_topic_size=None,
    reset=(),
):
    await client.update_topic(
        stream_id,
        topic_id,
        name=name,
        message_expiry=None if message_expiry is None else args.expiry(message_expiry),
        max_topic_size=None
        if max_topic_size is None
        else args.topic_size(max_topic_size),
        raw_reset=list(reset),
    )


async def delete_topic(client: IggyClient, *, stream_id, topic_id):
    await client.delete_topic(stream_id, topic_id)


async def send_messages(
    client: IggyClient, *, stream_id, topic_id, partitioning, messages
):
    batch = [args.message(message) for message in messages]
    await client.send_messages(
        stream_id, topic_id, args.partitioning(partitioning), batch
    )


async def poll_messages(
    client: IggyClient,
    *,
    stream_id,
    topic_id,
    partition_id,
    strategy,
    count,
    consumer=None,
    commit="manual",
    max_wait_ms=None,
    min_count=None,
):
    options = {
        "count": count,
        "max_wait": args.milliseconds(max_wait_ms),
        "commit": args.commit(commit),
    }
    if min_count is not None:
        options["min_count"] = min_count
    polled = await client.poll_messages(
        stream_id,
        topic_id,
        partition_id=partition_id,
        consumer=args.consumer(consumer),
        polling_strategy=args.strategy(strategy),
        options=PollOptions(**options),
    )
    return canonical.polled(polled)


async def create_consumer_group(client: IggyClient, *, stream_id, topic_id, name):
    group = await client.create_consumer_group(stream_id, topic_id, name)
    # The SDK returns no consumer group, and the answer says so.
    return None if _no_value(group) else canonical.consumer_group(group)


async def get_consumer_group(client: IggyClient, *, stream_id, topic_id, group_id):
    group = await client.get_consumer_group(stream_id, topic_id, group_id)
    return None if group is None else canonical.consumer_group(group)


async def delete_consumer_group(client: IggyClient, *, stream_id, topic_id, group_id):
    await client.delete_consumer_group(stream_id, topic_id, group_id)


async def build_reader(
    client: IggyClient,
    *,
    stream_id,
    topic_id,
    consumer=None,
    partitions=None,
    start=None,
    max_wait_ms=None,
    commit="manual",
) -> PartitionReader:
    return await client.reader(
        stream_id,
        topic_id,
        consumer=args.consumer(consumer),
        partitions=partitions,
        start=None if start is None else args.strategy(start),
        options=PollOptions(
            max_wait=args.milliseconds(max_wait_ms), commit=args.commit(commit)
        ),
    )


async def build_consumer(
    client: IggyClient,
    *,
    stream_id,
    topic_id,
    group,
    max_wait_ms=None,
    count=None,
    commit="manual",
) -> IggyConsumer:
    options = {
        "max_wait": args.milliseconds(max_wait_ms),
        "commit": args.commit(commit),
    }
    if count is not None:
        options["count"] = count
    # A consumer names its stream and topic as text, which holds an id or a name.
    return await client.consumer_group(
        group, str(stream_id), str(topic_id), poll_options=PollOptions(**options)
    )


async def resolve_operation(client: IggyClient, *, operation_id, deadline_ms=None):
    await client.resolve_operation(operation_id, timeout=args.milliseconds(deadline_ms))


async def abandon_operation(client: IggyClient, *, operation_id):
    await client.abandon_operation(operation_id)


async def send_binary_request(client: IggyClient, *, code, payload_hex=""):
    answer = await client.send_binary_request(code, bytes.fromhex(payload_hex))
    return {"payload_hex": answer.hex()}


async def send_binary_request_to_replica(
    client: IggyClient, *, code, replica, payload_hex=""
):
    answer = await client.send_binary_request_to_replica(
        code, bytes.fromhex(payload_hex), replica
    )
    return {"payload_hex": answer.hex()}


async def describe_options(client: IggyClient, *, scope):
    return [
        canonical.option_spec(spec) for spec in await client.describe_options(scope)
    ]


async def create_user(
    client: IggyClient, *, username, password, status="active", permissions=None
):
    user = await client.create_user(
        username, password, args.user_status(status), args.permissions(permissions)
    )
    return canonical.user_details(user)


async def get_user(client: IggyClient, *, user_id):
    user = await client.get_user(user_id)
    return None if user is None else canonical.user_details(user)


async def update_user(client: IggyClient, *, user_id, username=None, status=None):
    status = None if status is None else args.user_status(status)
    await client.update_user(user_id, username=username, status=status)


async def delete_user(client: IggyClient, *, user_id):
    await client.delete_user(user_id)


async def get_metadata_changes(client: IggyClient, *, start, groups=(), limit=None):
    page = await client.get_metadata_changes(
        args.metadata_start(start),
        groups=[args.metadata_group(group) for group in groups],
        limit=limit,
    )
    return metadata.changes(page)


METHODS = {
    method.__name__: method
    for method in (
        connect,
        disconnect,
        ping,
        login_user,
        logout_user,
        get_stream,
        get_streams,
        create_stream,
        update_stream,
        delete_stream,
        get_topic,
        get_topics,
        create_topic,
        update_topic,
        delete_topic,
        send_messages,
        poll_messages,
        create_consumer_group,
        get_consumer_group,
        delete_consumer_group,
        resolve_operation,
        abandon_operation,
        send_binary_request,
        send_binary_request_to_replica,
        describe_options,
        create_user,
        get_user,
        update_user,
        delete_user,
        get_metadata_changes,
    )
}


def _no_value(value) -> bool:
    """Whether the SDK returned nothing. pyo3 hands the unit result of a Rust future to
    Python as an empty tuple."""
    return value is None or value == ()
