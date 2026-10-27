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
Resource option update BDD steps for the Python SDK.
"""

import asyncio
import socket
from datetime import timedelta
from pathlib import Path

from apache_iggy import (
    IggyClient,
    IggyExpiry,
    MaxTopicSize,
    OptionKey,
    TopicDetails,
    TopicOptions,
    UserInfoDetails,
    UserOptions,
    UserStatus,
)
from pytest_bdd import given, parsers, scenarios, then, when

FEATURE_FILE = Path("/app/features/resource_options_patch.feature")
if not FEATURE_FILE.exists():
    FEATURE_FILE = (
        Path(__file__).resolve().parents[2] / "scenarios/resource_options_patch.feature"
    )
scenarios(str(FEATURE_FILE))


@given("I have a running Iggy server")
def running_server(context):
    async def connect():
        host, port = context.server_addr.split(":")
        try:
            address = f"{socket.gethostbyname(host)}:{port}"
        except socket.gaierror:
            address = context.server_addr

        context.client = IggyClient(address)
        await context.client.connect()
        await context.client.ping()

    asyncio.run(connect())


@given("I am authenticated as the root user")
def authenticated_root_user(context, root_credentials):
    async def login():
        await context.client.login_user(*root_credentials)

    asyncio.run(login())


@given(parsers.re(r'a stream named "(?P<name>[^"]+)" exists'))
def stream_exists(context, name):
    async def create():
        stream = await context.client.create_stream(name)
        context.last_stream_id = stream.id
        context.last_stream_name = stream.name

    asyncio.run(create())


@given(
    parsers.re(
        r'a topic named "(?P<name>[^"]+)" exists in the stream with message expiry '
        r"(?P<expiry>\d+) microseconds and max topic size (?P<max_size>\d+) bytes"
    ),
    converters={"expiry": int, "max_size": int},
)
def topic_exists_with_settings(context, name, expiry, max_size):
    async def create():
        await context.client.create_topic(
            context.last_stream_id,
            name,
            TopicOptions(
                message_expiry=IggyExpiry.ExpireDuration(
                    duration=timedelta(microseconds=expiry)
                ),
                max_topic_size=MaxTopicSize.Custom(bytes=max_size),
            ),
        )
        topic = await context.client.get_topic(context.last_stream_id, name)
        assert topic is not None
        context.last_topic_id = topic.id
        context.last_topic_name = topic.name

    asyncio.run(create())


@when(
    parsers.re(
        r"I update the topic message expiry to (?P<expiry>\d+) microseconds "
        r"without a name"
    ),
    converters={"expiry": int},
)
def update_topic_expiry(context, expiry):
    update_topic(
        context,
        message_expiry=IggyExpiry.ExpireDuration(
            duration=timedelta(microseconds=expiry)
        ),
    )


@when(parsers.re(r'I reset the topic option "(?P<key>[^"]+)"'))
def reset_topic_option(context, key):
    update_topic(context, reset=[option_key(key)])


@when(parsers.re(r'I rename the topic to "(?P<name>[^"]+)"'))
def rename_topic(context, name):
    update_topic(context, name=name)


@then(parsers.re(r'the topic name should be "(?P<name>[^"]+)"'))
def topic_name_is(context, name):
    assert fetch_topic(context).name == name


@then(
    parsers.re(
        r"the topic message expiry should be (?P<expiry>\d+) microseconds "
        r"set explicitly"
    ),
    converters={"expiry": int},
)
def topic_expiry_explicit(context, expiry):
    topic = fetch_topic(context)
    assert isinstance(topic.message_expiry, IggyExpiry.ExpireDuration)
    assert topic.message_expiry.duration == timedelta(microseconds=expiry)
    assert topic.options.to_scalar_dict()["message_expiry"] == expiry


@then(
    parsers.re(
        r"the topic max topic size should be (?P<max_size>\d+) bytes set explicitly"
    ),
    converters={"max_size": int},
)
def topic_max_size_explicit(context, max_size):
    topic = fetch_topic(context)
    assert isinstance(topic.max_topic_size, MaxTopicSize.Custom)
    assert topic.max_topic_size.bytes == max_size
    assert topic.options.to_scalar_dict()["max_topic_size"] == max_size


@then("the topic max topic size should be derived from the server default")
def topic_max_size_derived(context):
    topic = fetch_topic(context)
    assert "max_topic_size" not in topic.options.to_scalar_dict()
    assert "max_topic_size" in topic.derived_options.to_scalar_dict()


@given(
    parsers.re(
        r'a user named "(?P<username>[^"]+)" with password "(?P<password>[^"]+)" exists'
    )
)
def user_exists(context, username, password):
    async def create():
        user = await context.client.create_user(
            username, password, options=UserOptions()
        )
        context.last_user_id = user.id

    asyncio.run(create())


@when(
    parsers.re(
        r'I update the user username to "(?P<username>[^"]+)" and status to '
        r'"(?P<status>[^"]+)"'
    )
)
def update_user(context, username, status):
    async def update():
        await context.client.update_user(
            context.last_user_id, username=username, status=user_status(status)
        )

    asyncio.run(update())


@then(parsers.re(r'the user should have username "(?P<username>[^"]+)"'))
def user_has_username(context, username):
    assert fetch_user(context).username == username


@then(parsers.re(r'the user should have status "(?P<status>[^"]+)"'))
def user_has_status(context, status):
    assert fetch_user(context).status == user_status(status)


def update_topic(context, **changes) -> None:
    async def update():
        await context.client.update_topic(
            context.last_stream_id, context.last_topic_id, **changes
        )

    asyncio.run(update())


def fetch_topic(context) -> TopicDetails:
    async def fetch():
        return await context.client.get_topic(
            context.last_stream_id, context.last_topic_id
        )

    topic = asyncio.run(fetch())
    assert topic is not None
    return topic


def fetch_user(context) -> UserInfoDetails:
    async def fetch():
        return await context.client.get_user(context.last_user_id)

    user = asyncio.run(fetch())
    assert user is not None
    return user


def option_key(key: str) -> OptionKey:
    return getattr(OptionKey, "".join(part.title() for part in key.split("_")))


def user_status(status: str) -> UserStatus:
    return getattr(UserStatus, status.title())
