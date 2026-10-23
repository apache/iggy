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
Stream options BDD steps for the Python SDK.
"""

import asyncio
import base64
import socket
from pathlib import Path

from apache_iggy import (
    Consumer,
    IggyClient,
    IggyExpiry,
    OptionKey,
    PollingStrategy,
    SendMessage,
    StreamDetails,
    StreamOptions,
    TopicOptions,
    encryption_key_fingerprint,
)
from pytest_bdd import given, parsers, scenarios, then, when

FEATURE_FILE = Path("/app/features/stream_options.feature")
if not FEATURE_FILE.exists():
    FEATURE_FILE = (
        Path(__file__).resolve().parents[2] / "scenarios/stream_options.feature"
    )
scenarios(str(FEATURE_FILE))

KEY_LENGTH = 32
FINGERPRINT_LENGTH = 16
PARTITION_ID = 0
NEVER_RETURNED_KEYS = {"encryption_key", "encryption_key_wrapped", "critical_keys"}


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


@when(
    parsers.re(
        r'I create a stream named "(?P<name>[^"]+)" with an encryption key '
        r"of 32 bytes valued (?P<byte>\d+)"
    ),
    converters={"byte": int},
)
def create_stream_with_key(context, name, byte):
    create_keyed_stream(context, name, byte)


@given(
    parsers.re(
        r'a stream named "(?P<name>[^"]+)" with an encryption key '
        r"of 32 bytes valued (?P<byte>\d+) exists"
    ),
    converters={"byte": int},
)
def keyed_stream_exists(context, name, byte):
    create_keyed_stream(context, name, byte)


@when(
    parsers.re(r'I create a stream named "(?P<name>[^"]+)" without an encryption key')
)
def create_stream_without_key(context, name):
    async def create():
        stream = await context.client.create_stream(name)
        context.last_stream_id = stream.id
        context.last_stream_name = stream.name
        context.last_stream_key = None

    asyncio.run(create())


@then(parsers.re(r'the stream should report encryption key source "(?P<source>[^"]+)"'))
def stream_reports_source(context, source):
    assert fetch_stream(context).encryption_key_source == source


@then("the stream encryption key fingerprint should match the key")
def stream_fingerprint_matches_key(context):
    assert context.last_stream_key is not None
    expected = encryption_key_fingerprint(context.last_stream_key)
    assert fetch_stream(context).encryption_key_fingerprint == expected


@then("the stream should report an encryption key fingerprint")
def stream_reports_fingerprint(context):
    fingerprint = fetch_stream(context).encryption_key_fingerprint
    assert fingerprint is not None
    assert len(fingerprint) == FINGERPRINT_LENGTH


@then("the stream should report at least one KEK fingerprint")
def stream_reports_kek_fingerprint(context):
    assert fetch_stream(context).encryption_kek_fingerprints


@then("the stream should expose no encryption key material")
def stream_exposes_no_key_material(context):
    stream = fetch_stream(context)
    explicit = stream.options.to_scalar_dict()
    derived = stream.derived_options.to_scalar_dict()
    assert not NEVER_RETURNED_KEYS & (set(explicit) | set(derived))
    if context.last_stream_key is not None:
        values = [*explicit.values(), *derived.values()]
        assert context.last_stream_key not in values
        assert base64.b64encode(context.last_stream_key).decode() not in values


@when(
    parsers.re(
        r'I create a topic named "(?P<name>[^"]+)" with (?P<partitions>\d+) '
        r"partitions? in the stream"
    ),
    converters={"partitions": int},
)
def create_topic_in_stream(context, name, partitions):
    async def create():
        await context.client.create_topic(
            context.last_stream_id,
            name,
            TopicOptions(
                partitions_count=partitions,
                message_expiry=IggyExpiry.NeverExpire(),
            ),
        )
        topic = await context.client.get_topic(context.last_stream_id, name)
        assert topic is not None
        context.last_topic_id = topic.id
        context.last_topic_name = topic.name
        context.last_topic_partitions = partitions

    asyncio.run(create())


@when(parsers.re(r'I send a message with payload "(?P<payload>[^"]+)" to the topic'))
def send_payload(context, payload):
    send_message(context, SendMessage(payload))


@when(
    parsers.re(
        r'I send a message with payload "(?P<payload>[^"]+)" and header '
        r'"(?P<header>[^"]+)" set to "(?P<value>[^"]+)" to the topic'
    )
)
def send_payload_with_header(context, payload, header, value):
    send_message(context, SendMessage(payload, user_headers={header: value}))


@then(parsers.re(r'polling the topic should return the payload "(?P<payload>[^"]+)"'))
def poll_returns_payload(context, payload):
    message = poll_first(context)
    assert message.payload() == payload.encode()


@then(
    parsers.re(
        r'polling the topic should return the payload "(?P<payload>[^"]+)" '
        r'and header "(?P<header>[^"]+)" set to "(?P<value>[^"]+)"'
    )
)
def poll_returns_payload_and_header(context, payload, header, value):
    message = poll_first(context)
    assert message.payload() == payload.encode()
    headers = message.user_headers()
    assert headers is not None
    assert headers.to_scalar_dict()[header] == value


@when(parsers.re(r'I rename the stream to "(?P<name>[^"]+)"'))
def rename_stream(context, name):
    async def rename():
        await context.client.update_stream(context.last_stream_id, name=name)

    asyncio.run(rename())


@then(parsers.re(r'the stream name should be "(?P<name>[^"]+)"'))
def stream_name_is(context, name):
    assert fetch_stream(context).name == name


@when(parsers.re(r'I request a reset of the stream option "(?P<key>[^"]+)"'))
def request_stream_reset(context, key):
    async def reset():
        try:
            await context.client.update_stream(
                context.last_stream_id, reset=[option_key(key)]
            )
            context.last_update_error = None
        except RuntimeError as error:
            context.last_update_error = error

    asyncio.run(reset())


@then("the stream update should be rejected")
def stream_update_rejected(context):
    assert context.last_update_error is not None


@when("I describe the stream options")
def describe_stream_options(context):
    async def describe():
        context.last_option_specs = await context.client.describe_options("stream")

    asyncio.run(describe())


@then(
    parsers.re(r'the stream option "(?P<key>[^"]+)" should be updatable and not secret')
)
def option_updatable(context, key):
    spec = option_spec(context, key)
    assert spec.updatable and not spec.secret and not spec.read_only


@then(
    parsers.re(r'the stream option "(?P<key>[^"]+)" should be create only and secret')
)
def option_create_only_secret(context, key):
    spec = option_spec(context, key)
    assert not spec.updatable and not spec.resettable
    assert spec.secret and not spec.read_only


@then(parsers.re(r'the stream option "(?P<key>[^"]+)" should be read only'))
def option_read_only(context, key):
    spec = option_spec(context, key)
    assert spec.read_only and not spec.updatable and not spec.secret


def create_keyed_stream(context, name: str, byte: int) -> None:
    key = bytes([byte]) * KEY_LENGTH

    async def create():
        stream = await context.client.create_stream(
            name, StreamOptions(encryption_key=key)
        )
        context.last_stream_id = stream.id
        context.last_stream_name = stream.name
        context.last_stream_key = key

    asyncio.run(create())


def fetch_stream(context) -> StreamDetails:
    async def fetch():
        return await context.client.get_stream(context.last_stream_id)

    stream = asyncio.run(fetch())
    assert stream is not None
    return stream


def send_message(context, message: SendMessage) -> None:
    async def send():
        await context.client.send_messages(
            stream=context.last_stream_id,
            topic=context.last_topic_id,
            partitioning=PARTITION_ID,
            messages=[message],
        )

    asyncio.run(send())


def poll_first(context):
    async def poll():
        return await context.client.poll_messages(
            stream=context.last_stream_id,
            topic=context.last_topic_id,
            consumer=Consumer.Single(1),
            partition_id=PARTITION_ID,
            polling_strategy=PollingStrategy.Offset(value=0),
            count=1,
            auto_commit=False,
        )

    messages = asyncio.run(poll())
    assert messages
    return messages[0]


def option_key(key: str) -> OptionKey:
    return getattr(OptionKey, "".join(part.title() for part in key.split("_")))


def option_spec(context, key: str):
    assert context.last_option_specs is not None
    return next(spec for spec in context.last_option_specs if spec.key == key)
