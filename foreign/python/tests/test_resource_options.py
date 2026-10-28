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
Tests for typed stream, topic and user options.

The server cases run once per transport through `transport_client`, against a
server with encryption enabled. The vector cases read the fixture the Rust
encoders generated and need no server.
"""

import base64
import inspect
import json
import re
import secrets
from datetime import timedelta
from pathlib import Path

import pytest

from apache_iggy import (
    AutoLogin,
    Consumer,
    Durability,
    GlobalPermissions,
    IggyClient,
    IggyExpiry,
    MaxTopicSize,
    MetadataChangeGroup,
    MetadataChangeKind,
    MetadataStreamChange,
    OptionError,
    OptionKey,
    PartitionResizePolicy,
    Permissions,
    PollingStrategy,
    QuicConfig,
    QuicReconnectionConfig,
    Stream,
    StreamDetails,
    StreamOptions,
    TcpConfig,
    TcpReconnectionConfig,
    Topic,
    TopicDetails,
    TopicOptions,
    UserInfo,
    UserInfoDetails,
    UserOptions,
    UserStatus,
    WebSocketConfig,
    encryption_key_fingerprint,
)
from apache_iggy import SendMessage as Message

from .utils import (
    follow_stream_changes,
    get_quic_server_config,
    get_server_config,
    get_websocket_server_config,
    latest_metadata_cursor,
    stream_records,
    unique_credentials,
    wait_for_ping,
)

FIXTURE = json.loads(
    (Path(__file__).parent / "fixtures" / "resource_options.json").read_text()
)
# The binding raises the server's error text, so each fixture error name maps
# to the text it carries.
ERROR_MESSAGES = {
    "invalid_command": "Invalid command",
    "invalid_encryption_key": "Invalid encryption key",
    "invalid_stream_name": "Invalid stream name",
    "invalid_topic_name": "Invalid topic name",
    "invalid_username": "Invalid username",
    "option_not_resettable": "Option cannot be reset",
    "option_not_updatable": "Option cannot be updated after creation",
    "read_only_option": "Option is read-only",
    "unsupported_option_key": "Unsupported option key",
}
# Option refusals raise `OptionError`, which carries the code on every transport.
OPTION_ERROR_CODES = {
    vector["error"]["name"]: vector["error"]["code"]
    for section in ("update_resource_errors", "http_update")
    for vector in FIXTURE[section]
    if vector.get("error") and vector["error"]["code"] >= 4000
}
STREAM_KEY_LENGTH = FIXTURE["constants"]["stream_key_length"]
NEVER_RETURNED_KEYS = set(FIXTURE["constants"]["never_returned_keys"])
FINGERPRINT_LENGTH = 16
U64_MAX = 2**64 - 1
MICROS_PER_HOUR = 3_600_000_000
GIB = 1024**3


@pytest.mark.usefixtures("iggy_client")
class TestStreamOptions:
    """Typed stream create options, encryption projections and updates."""

    @pytest.mark.asyncio
    async def test_given_stream_key_when_creating_should_read_matching_projection(
        self, transport_client: IggyClient, unique_name
    ):
        """A client key comes back only as its source and fingerprints."""
        key = secrets.token_bytes(STREAM_KEY_LENGTH)
        fingerprint = encryption_key_fingerprint(key)

        created = await transport_client.create_stream(
            unique_name(), StreamOptions(encryption_key=key)
        )
        fetched = await transport_client.get_stream(created.id)
        listed = next(
            stream
            for stream in await transport_client.get_streams()
            if stream.id == created.id
        )

        assert fetched is not None
        for stream in (created, fetched, listed):
            assert_encryption_projection(stream, "stream", fingerprint)
            assert_no_key_material(stream, key)
        assert listed.encryption_kek_fingerprints == fetched.encryption_kek_fingerprints

    @pytest.mark.asyncio
    async def test_given_keyless_stream_when_reading_should_report_server_generated_key(  # noqa: E501
        self, transport_client: IggyClient, unique_name
    ):
        """A server with encryption enabled generates a key for a keyless stream."""
        created = await transport_client.create_stream(unique_name())
        fetched = await transport_client.get_stream(created.id)

        assert fetched is not None
        fingerprint = fetched.encryption_key_fingerprint
        assert fingerprint is not None
        assert re.fullmatch(f"[0-9a-f]{{{FINGERPRINT_LENGTH}}}", fingerprint)
        for stream in (created, fetched):
            assert_encryption_projection(stream, "server", fingerprint)

    @pytest.mark.asyncio
    async def test_given_stream_rename_when_updating_should_preserve_encryption_projection(  # noqa: E501
        self, transport_client: IggyClient, unique_name
    ):
        """A rename keeps the stream's key, so data written before still reads."""
        key = secrets.token_bytes(STREAM_KEY_LENGTH)
        topic_name = unique_name()
        new_name = unique_name()
        payload = f"before-rename-{unique_name()}"
        created = await transport_client.create_stream(
            unique_name(),
            StreamOptions(encryption_key=base64.b64encode(key).decode()),
        )
        await transport_client.create_topic(created.id, topic_name)
        await transport_client.send_messages(
            stream=created.id,
            topic=topic_name,
            partitioning=0,
            messages=[Message(payload)],
        )

        await transport_client.update_stream(created.id, name=new_name)

        renamed = await transport_client.get_stream(created.id)
        assert renamed is not None
        assert renamed.id == created.id
        assert renamed.name == new_name
        assert await transport_client.get_stream(created.name) is None
        assert_encryption_projection(renamed, "stream", encryption_key_fingerprint(key))
        assert (
            renamed.encryption_kek_fingerprints == created.encryption_kek_fingerprints
        )
        polled = await transport_client.poll_messages(
            stream=new_name,
            topic=topic_name,
            consumer=Consumer.Single("stream-rename"),
            partition_id=0,
            polling_strategy=PollingStrategy.First(),
            count=1,
            auto_commit=False,
        )
        assert [message.payload().decode() for message in polled] == [payload]

    @pytest.mark.asyncio
    async def test_given_stream_key_reset_when_updating_should_reject_without_changing_state(  # noqa: E501
        self, transport_client: IggyClient, unique_name
    ):
        """No stream key is resettable, so a reset is refused before it is sent."""
        key = secrets.token_bytes(STREAM_KEY_LENGTH)
        created = await transport_client.create_stream(
            unique_name(), StreamOptions(encryption_key=key)
        )

        with pytest.raises(RuntimeError, match=ERROR_MESSAGES["option_not_updatable"]):
            await transport_client.update_stream(
                created.id, reset=[OptionKey.EncryptionKey]
            )
        with pytest.raises(RuntimeError, match=ERROR_MESSAGES["option_not_resettable"]):
            await transport_client.update_stream(created.id, reset=[OptionKey.Name])

        stream = await transport_client.get_stream(created.id)
        assert stream is not None
        assert stream.name == created.name
        assert_encryption_projection(stream, "stream", encryption_key_fingerprint(key))

    @pytest.mark.asyncio
    async def test_given_stream_key_when_following_metadata_changes_should_expose_fingerprints_without_key(  # noqa: E501
        self, transport_client: IggyClient, unique_name
    ):
        """A change record carries the key projections a stream read does."""
        key = secrets.token_bytes(STREAM_KEY_LENGTH)
        streams = [MetadataChangeGroup.Streams]
        cursor = await latest_metadata_cursor(transport_client, streams)

        created = await transport_client.create_stream(
            unique_name(), StreamOptions(encryption_key=key)
        )

        followed = await follow_stream_changes(
            transport_client, cursor, streams, created.id, 1
        )
        records = stream_records(followed, created.id)
        assert [record.kind for record in records] == [MetadataChangeKind.StreamCreated]
        change = records[0].change
        assert isinstance(change, MetadataStreamChange)
        assert change.options is not None
        assert change.derived_options is not None
        explicit = change.options.to_scalar_dict()
        derived = change.derived_options.to_scalar_dict()
        assert derived["encryption_key_source"] == "stream"
        assert derived["encryption_key_fingerprint"] == encryption_key_fingerprint(key)
        assert derived["encryption_kek_fingerprint"] == ",".join(
            created.encryption_kek_fingerprints
        )
        assert not NEVER_RETURNED_KEYS & (set(explicit) | set(derived))
        for value in (*explicit.values(), *derived.values()):
            assert_secret_absent(str(value), key)
        assert_secret_absent(repr(records[0]), key)


@pytest.mark.usefixtures("iggy_client")
class TestTopicOptions:
    """Typed topic create options and generic topic updates."""

    @pytest.mark.asyncio
    async def test_given_keyword_topic_options_when_creating_should_preserve_all_ten_settings(  # noqa: E501
        self, transport_client: IggyClient, unique_name
    ):
        """Every known create setting has a typed field and reads back explicit."""
        stream_name = unique_name()
        topic_name = unique_name()
        await transport_client.create_stream(stream_name)

        await transport_client.create_topic(
            stream_name,
            topic_name,
            TopicOptions(
                partitions_count=2,
                compression_algorithm="gzip",
                message_expiry=IggyExpiry.ExpireDuration(duration=timedelta(hours=1)),
                max_topic_size=MaxTopicSize.Custom(bytes=GIB),
                segment_size=1024 * 1024,
                durability=Durability.PERSISTED,
                consumer_offset_durability=Durability.PERSISTED,
                partition_resize_policy=PartitionResizePolicy.Fixed,
                messages_required_to_save=7,
                size_of_messages_required_to_save=4096,
                preallocate_segments=True,
            ),
        )

        topic = await transport_client.get_topic(stream_name, topic_name)
        assert topic is not None
        assert topic.partitions_count == 2
        assert topic.options.to_scalar_dict() == {
            "compression_algorithm": "gzip",
            "message_expiry": MICROS_PER_HOUR,
            "max_topic_size": GIB,
            "segment_size": 1024 * 1024,
            "durability": "persisted",
            "consumer_offset_durability": "persisted",
            "partition_resize_policy": "fixed",
            "messages_required_to_save": 7,
            "size_of_messages_required_to_save": 4096,
            "preallocate_segments": True,
        }
        assert topic.derived_options.to_scalar_dict() == {}
        assert topic.compression_algorithm == "gzip"
        assert isinstance(topic.max_topic_size, MaxTopicSize.Custom)
        assert topic.max_topic_size.bytes == GIB

    @pytest.mark.asyncio
    async def test_given_topic_reset_when_updating_should_preserve_unmentioned_values(  # noqa: E501
        self, transport_client: IggyClient, unique_name
    ):
        """An update without a name changes only the keys it names or resets."""
        stream_name = unique_name()
        topic_name = unique_name()
        await transport_client.create_stream(stream_name)
        await transport_client.create_topic(
            stream_name,
            topic_name,
            TopicOptions(
                compression_algorithm="gzip",
                message_expiry=IggyExpiry.ExpireDuration(duration=timedelta(hours=1)),
                max_topic_size=MaxTopicSize.Custom(bytes=GIB),
            ),
        )

        await transport_client.update_topic(
            stream_name,
            topic_name,
            message_expiry=IggyExpiry.ExpireDuration(duration=timedelta(hours=2)),
            reset=[OptionKey.MaxTopicSize],
        )

        topic = await transport_client.get_topic(stream_name, topic_name)
        assert topic is not None
        assert topic.name == topic_name
        explicit = topic.options.to_scalar_dict()
        derived = topic.derived_options.to_scalar_dict()
        assert explicit["message_expiry"] == 2 * MICROS_PER_HOUR
        assert explicit["compression_algorithm"] == "gzip"
        assert "max_topic_size" not in explicit
        assert derived["max_topic_size"] == U64_MAX
        assert topic.compression_algorithm == "gzip"
        assert isinstance(topic.max_topic_size, MaxTopicSize.Unlimited)
        # Compression is create-only, so the update has no field for it.
        with pytest.raises(TypeError):
            await transport_client.update_topic(
                stream_name,
                topic_name,
                # pyrefly: ignore  # unexpected-keyword
                compression_algorithm="none",
            )


@pytest.mark.usefixtures("iggy_client")
class TestUserOptions:
    """Typed user create options and generic user updates."""

    @pytest.mark.asyncio
    async def test_given_user_options_when_creating_and_updating_should_preserve_user_behavior(  # noqa: E501
        self, transport_client: IggyClient, unique_name
    ):
        """A username and status patch applies together or not at all."""
        username, password = unique_credentials(unique_name)
        other_username, other_password = unique_credentials(unique_name)
        new_username, _ = unique_credentials(unique_name)
        created = await transport_client.create_user(
            username, password, options=UserOptions()
        )
        other = await transport_client.create_user(other_username, other_password)
        try:
            assert created.options.to_scalar_dict() == {}
            assert created.derived_options.to_scalar_dict() == {}

            await transport_client.update_user(
                created.id, username=new_username, status=UserStatus.Inactive
            )

            user = await transport_client.get_user(created.id)
            assert user is not None
            assert user.username == new_username
            assert user.status == UserStatus.Inactive
            assert user.options.to_scalar_dict() == {}
            listed = next(
                entry
                for entry in await transport_client.get_users()
                if entry.id == created.id
            )
            assert listed.username == new_username
            assert listed.derived_options.to_scalar_dict() == {}

            with pytest.raises(RuntimeError):
                await transport_client.update_user(
                    other.id, username=new_username, status=UserStatus.Inactive
                )
            unchanged = await transport_client.get_user(other.id)
            assert unchanged is not None
            assert unchanged.username == other_username
            assert unchanged.status == UserStatus.Active
        finally:
            await transport_client.delete_user(created.id)
            await transport_client.delete_user(other.id)

    @pytest.mark.asyncio
    @pytest.mark.parametrize("transport", ["tcp", "quic", "websocket"])
    async def test_given_logged_in_user_rename_when_reconnecting_should_keep_new_username(  # noqa: E501
        self, iggy_client: IggyClient, unique_name, transport: str
    ):
        """A self-rename refreshes the credentials a reconnect replays.

        HTTP holds no session and replays no credentials, so only the binary
        transports reconnect.
        """
        username, password = unique_credentials(unique_name)
        new_username, _ = unique_credentials(unique_name)
        permissions = Permissions(
            GlobalPermissions(manage_users=True, manage_streams=True)
        )
        user = await iggy_client.create_user(
            username, password, permissions=permissions
        )
        client = IggyClient(
            auto_login_config(
                transport, AutoLogin.username_password(username, password)
            )
        )
        try:
            await client.connect()
            await wait_for_ping(client)

            with pytest.raises(RuntimeError):
                await client.update_user(user.id, username="iggy")
            stream = await client.create_stream(unique_name())
            await client.update_stream(stream.id, name=unique_name())
            await reconnect(client)

            await client.update_user(user.id, username=new_username)
            await reconnect(client)

            renamed = await iggy_client.get_user(user.id)
            assert renamed is not None
            assert renamed.username == new_username
        finally:
            await client.shutdown()
            await iggy_client.delete_user(user.id)


@pytest.mark.usefixtures("iggy_client")
class TestServerOptionContract:
    """Descriptors and refusals that come back from the server."""

    @pytest.mark.asyncio
    async def test_given_every_scope_when_describing_options_should_match_fixture(  # noqa: E501
        self, transport_client: IggyClient
    ):
        """Every transport decodes the same descriptors and flags."""
        flags = FIXTURE["constants"]["option_flags"]
        for vector in FIXTURE["describe_options"]:
            specs = await transport_client.describe_options(vector["scope"])
            assert [spec.key for spec in specs] == [
                entry["key"] for entry in vector["entries"]
            ]
            for spec, entry in zip(specs, vector["entries"], strict=True):
                for flag, mask in flags.items():
                    assert getattr(spec, flag) == bool(entry["flags"] & mask), flag

    @pytest.mark.asyncio
    async def test_given_raw_reset_of_internal_key_when_updating_should_get_server_refusal(  # noqa: E501
        self, transport_client: IggyClient, unique_name
    ):
        """A raw reset name travels to the server, which decides it."""
        vector = fixture_case("update_resource_errors", "user_reset_internal_key")
        username, password = unique_credentials(unique_name)
        created = await transport_client.create_user(username, password)
        try:
            with pytest.raises(
                OptionError, match=ERROR_MESSAGES[vector["error"]["name"]]
            ) as refusal:
                await transport_client.update_user(
                    created.id, raw_reset=vector["input"]["reset"]
                )
            assert_option_error(refusal.value, vector["error"]["name"])
        finally:
            await transport_client.delete_user(created.id)

    @pytest.mark.asyncio
    async def test_given_raw_set_key_refused_by_admission_when_updating_should_get_server_refusal(  # noqa: E501
        self, transport_client: IggyClient, unique_name
    ):
        """Raw set keys reach the server, which refuses them by category."""
        stream_name = unique_name()
        topic_name = unique_name()
        await transport_client.create_stream(stream_name)
        await transport_client.create_topic(stream_name, topic_name)
        for options, name in (
            ({"compression_algorithm": "gzip"}, "option_not_updatable"),
            ({"future_key": "on"}, "unsupported_option_key"),
        ):
            with pytest.raises(OptionError, match=ERROR_MESSAGES[name]) as refusal:
                await transport_client.update_topic(
                    stream_name, topic_name, options=options
                )
            assert_option_error(refusal.value, name)
        with pytest.raises(
            OptionError, match=ERROR_MESSAGES["read_only_option"]
        ) as refusal:
            await transport_client.update_stream(
                stream_name, raw_reset=["encryption_key_source"]
            )
        assert_option_error(refusal.value, "read_only_option")


class TestSharedVectors:
    """The binding against the vectors the Rust encoders generated."""

    @pytest.mark.unit
    def test_given_shared_vectors_when_encoding_should_match_server_contract(
        self,
    ):
        """Fingerprints, key members and typed refusals match the server."""
        vectors = FIXTURE["stream_key"]
        assert (
            encryption_key_fingerprint(bytes.fromhex(vectors["key_hex"]))
            == vectors["fingerprint"]
        )
        assert (
            encryption_key_fingerprint(vectors["key_base64"]) == vectors["fingerprint"]
        )

        described = {row["key"] for row in FIXTURE["catalog"] if row["described"]}
        internal = {row["key"] for row in FIXTURE["catalog"] if not row["described"]}
        members = {
            str(value)
            for value in vars(OptionKey).values()
            if isinstance(value, OptionKey)
        }
        assert members == described
        assert all(str(option_key(key)) == key for key in described)
        assert not any(hasattr(OptionKey, member_name(key)) for key in internal)

        # A client that never connects proves the refusal happens before I/O:
        # the call raises before it returns an awaitable.
        client = IggyClient("127.0.0.1:1")
        for vector in FIXTURE["update_resource_errors"]:
            if raw_reset_names(vector):
                # The server decides raw names, see the transport refusal tests.
                continue
            name = vector["error"]["name"]
            with pytest.raises(RuntimeError, match=ERROR_MESSAGES[name]) as refusal:
                typed_update(client, vector)
            if name in OPTION_ERROR_CODES:
                assert_option_error(refusal.value, name)
            else:
                assert not isinstance(refusal.value, OptionError), name

    @pytest.mark.unit
    def test_given_fixture_catalog_when_reading_typed_surface_should_cover_every_key(  # noqa: E501
        self,
    ):
        """Every public catalog key has its typed field, reset member or accessor."""
        create_fields = {
            "stream": parameters(StreamOptions),
            "topic": parameters(TopicOptions),
            "user": parameters(UserOptions),
        }
        update_fields = {
            "stream": parameters(IggyClient.update_stream),
            "topic": parameters(IggyClient.update_topic),
            "user": parameters(IggyClient.update_user),
        }
        readers = {
            "stream": (Stream, StreamDetails),
            "topic": (Topic, TopicDetails),
            "user": (UserInfo, UserInfoDetails),
        }
        resettable = FIXTURE["constants"]["option_flags"]["resettable"]
        for scope in create_fields:
            assert "raw" in create_fields[scope], scope
            assert {"reset", "raw_reset", "options"} <= update_fields[scope], scope
        for row in FIXTURE["catalog"]:
            if row["visibility"] == "internal":
                continue
            key = row["key"]
            for scope in row["scopes"]:
                if row["storage"] == "map" and row["mutability"] != "read_only":
                    assert key in create_fields[scope], f"{scope} create {key}"
                if row["mutability"] == "updatable":
                    assert key in update_fields[scope], f"{scope} update {key}"
                if row["flags"] & resettable:
                    assert hasattr(OptionKey, member_name(key)), f"reset {key}"
                if row["mutability"] == "read_only":
                    for reader in readers[scope]:
                        # A multi-valued read-only key reads through a plural.
                        assert hasattr(reader, key) or hasattr(reader, f"{key}s"), (
                            f"{reader.__name__} {key}"
                        )

    @pytest.mark.unit
    def test_given_invalid_key_when_encoding_should_reject_before_network_io(
        self,
    ):
        """Only exactly 32 bytes or their canonical base64 form are accepted."""
        for vector in FIXTURE["stream_key"]["invalid_keys"]:
            key = key_input(vector["input"])
            with pytest.raises(
                ValueError, match=ERROR_MESSAGES[vector["error"]["name"]]
            ) as refusal:
                StreamOptions(encryption_key=key)
            assert_secret_absent(str(refusal.value), key)
            with pytest.raises(
                ValueError, match=ERROR_MESSAGES[vector["error"]["name"]]
            ):
                encryption_key_fingerprint(key)
        with pytest.raises(TypeError):
            # pyrefly: ignore  # bad-argument-type
            StreamOptions(encryption_key=7)

        key = bytes.fromhex(FIXTURE["stream_key"]["key_hex"])
        for accepted in (key, FIXTURE["stream_key"]["key_base64"]):
            options = StreamOptions(encryption_key=accepted, raw={"future": "on"})
            assert options.has_encryption_key
            assert_secret_absent(repr(options), key)
            assert "future" in repr(options)
        assert not StreamOptions().has_encryption_key


@pytest.mark.asyncio
@pytest.mark.usefixtures("iggy_client")
async def test_given_invalid_key_when_creating_should_send_no_request(
    transport_client: IggyClient, unique_name
):
    """An invalid key never reaches the server on any transport."""
    name = unique_name()
    for vector in FIXTURE["stream_key"]["invalid_keys"]:
        with pytest.raises(ValueError):
            await transport_client.create_stream(
                name, StreamOptions(encryption_key=key_input(vector["input"]))
            )

    assert await transport_client.get_stream(name) is None


def assert_encryption_projection(
    stream: Stream | StreamDetails, source: str, fingerprint: str
) -> None:
    """Check the typed accessors and the derived block report the same key."""
    assert stream.encryption_key_source == source
    assert stream.encryption_key_fingerprint == fingerprint
    kek_fingerprints = stream.encryption_kek_fingerprints
    assert kek_fingerprints
    assert all(len(kek) == FINGERPRINT_LENGTH for kek in kek_fingerprints)
    derived = stream.derived_options.to_scalar_dict()
    assert derived["encryption_key_source"] == source
    assert derived["encryption_key_fingerprint"] == fingerprint
    assert derived["encryption_kek_fingerprint"] == ",".join(kek_fingerprints)
    returned = set(stream.options.to_scalar_dict()) | set(derived)
    assert not NEVER_RETURNED_KEYS & returned


def assert_no_key_material(stream: Stream | StreamDetails, key: bytes) -> None:
    """Check no returned option value carries the key, raw or encoded."""
    values = [
        *stream.options.to_scalar_dict().values(),
        *stream.derived_options.to_scalar_dict().values(),
    ]
    assert key not in values
    assert base64.b64encode(key).decode() not in values
    assert key.hex() not in values


def assert_option_error(error: BaseException, name: str) -> None:
    """Check a refusal carries the code and kind of a fixture error name."""
    assert isinstance(error, OptionError), repr(error)
    assert (error.code, error.kind) == (OPTION_ERROR_CODES[name], name)


def assert_secret_absent(text: str, key: bytes | str) -> None:
    """Check a diagnostic carries none of the forms a key can be given in."""
    raw = key if isinstance(key, bytes) else key.encode()
    for secret in (raw.hex(), base64.b64encode(raw).decode(), repr(raw)):
        assert secret not in text
    if isinstance(key, str) and key.strip():
        assert key.strip() not in text


def key_input(value: dict) -> bytes | str:
    """Return a fixture key input as the bytes or text the API takes."""
    if "raw_hex" in value:
        return bytes.fromhex(value["raw_hex"])
    return value["base64"]


def fixture_case(section: str, case: str) -> dict:
    """Return the fixture vector of a section by its case name."""
    return next(vector for vector in FIXTURE[section] if vector["case"] == case)


def parameters(callable_object) -> set[str]:
    """Return the keyword names a binding constructor or method takes."""
    return set(inspect.signature(callable_object).parameters)


def raw_reset_names(vector: dict) -> list[str]:
    """Return the reset names of a vector that have no `OptionKey` member."""
    return [
        key
        for key in vector["input"].get("reset", [])
        if not hasattr(OptionKey, member_name(key))
    ]


def member_name(key: str) -> str:
    """Return the `OptionKey` member name for a catalog key."""
    return "".join(part.title() for part in key.split("_"))


def option_key(key: str) -> OptionKey:
    """Return the `OptionKey` member for a catalog key."""
    return getattr(OptionKey, member_name(key))


def typed_update(client: IggyClient, vector: dict):
    """Issue the typed update a fixture vector describes."""
    target = vector["target"]
    values = vector["input"]
    reset = [option_key(key) for key in values.get("reset", [])] or None
    if vector["scope"] == "topic":
        expiry = values.get("message_expiry")
        max_size = values.get("max_topic_size")
        return client.update_topic(
            target["stream_id"],
            target["topic_id"],
            name=values.get("name"),
            message_expiry=None
            if expiry is None
            else IggyExpiry.ExpireDuration(
                duration=timedelta(microseconds=int(expiry))
            ),
            max_topic_size=None
            if max_size is None
            else MaxTopicSize.Custom(bytes=int(max_size)),
            reset=reset,
        )
    if vector["scope"] == "stream":
        return client.update_stream(
            target["stream_id"], name=values.get("name"), reset=reset
        )
    status = values.get("status")
    return client.update_user(
        target["user_id"],
        username=values.get("username"),
        status=None if status is None else getattr(UserStatus, status.title()),
        reset=reset,
    )


def auto_login_config(
    transport: str, auto_login: AutoLogin
) -> TcpConfig | QuicConfig | WebSocketConfig:
    """Return a binary transport config that signs in on every connect."""
    if transport == "tcp":
        host, port = get_server_config()
        return TcpConfig(
            server_address=f"{host}:{port}",
            auto_login=auto_login,
            reconnection=TcpReconnectionConfig(reestablish_after=timedelta(0)),
        )
    if transport == "quic":
        host, port = get_quic_server_config()
        return QuicConfig(
            server_address=f"{host}:{port}",
            auto_login=auto_login,
            reconnection=QuicReconnectionConfig(reestablish_after=timedelta(0)),
        )
    host, port = get_websocket_server_config()
    return WebSocketConfig(server_address=f"{host}:{port}", auto_login=auto_login)


async def reconnect(client: IggyClient) -> None:
    """Drop the connection, sign in again and prove the session is privileged."""
    await client.disconnect()
    await client.connect()
    await wait_for_ping(client)
    await client.get_users()
