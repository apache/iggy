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
Tests for reading the metadata change log.

The server cases run once per transport through `transport_client`. The vector
cases read the fixture the Rust encoders generated and need no server. The
keyed stream case needs a server key and lives in `test_resource_options.py`.
"""

import json
from pathlib import Path

import pytest

from apache_iggy import (
    GlobalPermissions,
    IggyClient,
    IggyError,
    MetadataChangeCursor,
    MetadataChangeGroup,
    MetadataChangeKind,
    MetadataChangePhase,
    MetadataChangeStart,
    MetadataChangeStatus,
    MetadataCurrentPageToken,
    MetadataMemberLeftReason,
    MetadataPermissionsUpdatedReason,
    MetadataSessionClosedReason,
    MetadataStreamChange,
    MetadataStreamDeleted,
    MetadataTokenDeletedReason,
    MetadataTopicChange,
    Permissions,
    TopicOptions,
)

from .utils import (
    METADATA_CHANGES_PAGE_LIMIT,
    follow_stream_changes,
    latest_metadata_cursor,
    stream_records,
    unique_credentials,
)

FIXTURE = json.loads(
    (Path(__file__).parent / "fixtures" / "metadata_changes.json").read_text()
)
CONSTANTS = FIXTURE["constants"]
INVALID_COMMAND = 3
UNAUTHORIZED = 41
ROOT_USER_ID = 0
STREAMS = MetadataChangeGroup.Streams
TOPICS = MetadataChangeGroup.Topics
ENUM_SECTIONS = (
    (MetadataChangeGroup, "groups"),
    (MetadataChangeKind, "kinds"),
    (MetadataChangeStatus, "statuses"),
    (MetadataChangePhase, "phases"),
    (MetadataMemberLeftReason, "member_left_reasons"),
    (MetadataTokenDeletedReason, "token_deleted_reasons"),
    (MetadataSessionClosedReason, "session_closed_reasons"),
    (MetadataPermissionsUpdatedReason, "permissions_updated_reasons"),
)


@pytest.mark.usefixtures("iggy_client")
class TestMetadataChangesFollow:
    """Following the change log on every transport."""

    @pytest.mark.asyncio
    async def test_given_latest_cursor_when_creating_renaming_and_deleting_stream_should_follow_each_change(  # noqa: E501
        self, transport_client: IggyClient, unique_name
    ):
        """Each change comes back typed, in order and with the root actor."""
        name = unique_name()
        renamed = unique_name()
        cursor = await latest_metadata_cursor(transport_client, [STREAMS])

        stream = await transport_client.create_stream(name)
        await transport_client.update_stream(stream.id, name=renamed)
        await transport_client.delete_stream(stream.id)

        followed = await follow_stream_changes(
            transport_client, cursor, [STREAMS], stream.id, 3
        )
        assert all(record.group == STREAMS for record in followed)
        records = stream_records(followed, stream.id)
        assert [record.kind for record in records] == [
            MetadataChangeKind.StreamCreated,
            MetadataChangeKind.StreamUpdated,
            MetadataChangeKind.StreamDeleted,
        ]
        assert all(record.actor_user_id == ROOT_USER_ID for record in records)
        positions = [(record.op, record.ordinal) for record in records]
        assert positions == sorted(set(positions))
        assert all(record.timestamp > 0 and record.checksum > 0 for record in records)

        created, updated, deleted = (record.change for record in records)
        assert isinstance(created, MetadataStreamChange)
        assert isinstance(updated, MetadataStreamChange)
        assert isinstance(deleted, MetadataStreamDeleted)
        assert (created.name, updated.name, deleted.name) == (name, renamed, renamed)
        assert created.stream_created_op == records[0].op
        assert (
            updated.stream_created_op
            == deleted.stream_created_op
            == created.stream_created_op
        )
        assert created.created_at == stream.created_at
        assert not created.options_unreadable
        assert created.options is not None

    @pytest.mark.asyncio
    async def test_given_topics_group_when_following_should_return_only_topic_records(  # noqa: E501
        self, transport_client: IggyClient, unique_name
    ):
        """A group filter leaves out the stream created on the way."""
        topic_name = unique_name()
        cursor = await latest_metadata_cursor(transport_client, [TOPICS])

        stream = await transport_client.create_stream(unique_name())
        await transport_client.create_topic(
            stream.id, topic_name, TopicOptions(partitions_count=2)
        )

        followed = await follow_stream_changes(
            transport_client, cursor, [TOPICS], stream.id, 1
        )
        assert all(record.group == TOPICS for record in followed)
        records = stream_records(followed, stream.id)
        assert [record.kind for record in records] == [MetadataChangeKind.TopicCreated]
        topic = records[0].change
        assert isinstance(topic, MetadataTopicChange)
        assert (topic.name, topic.partitions_count) == (topic_name, 2)
        assert topic.topic_created_op == records[0].op

    @pytest.mark.asyncio
    async def test_given_limit_one_when_following_should_page_every_record_in_order(
        self, transport_client: IggyClient, unique_name
    ):
        """Pages of one record continue through `next` until `has_more` drops."""
        cursor = await latest_metadata_cursor(transport_client, [STREAMS])
        created = [
            (await transport_client.create_stream(unique_name())).id for _ in range(3)
        ]

        records = []
        while True:
            page = await transport_client.get_metadata_changes(
                MetadataChangeStart.After(cursor), groups=[STREAMS], limit=1
            )
            assert page.status == MetadataChangeStatus.Ok
            assert page.phase == MetadataChangePhase.Changes
            assert len(page.records) <= 1
            assert page.next is not None
            assert (page.next.op, page.next.ordinal) > (cursor.op, cursor.ordinal)
            cursor = page.next
            records.extend(page.records)
            if not page.has_more:
                break

        assert [
            record.change.stream_id
            for record in records
            if record.kind == MetadataChangeKind.StreamCreated
            and isinstance(record.change, MetadataStreamChange)
        ] == created
        assert (cursor.op, cursor.ordinal) == (records[-1].op, records[-1].ordinal)

    @pytest.mark.asyncio
    async def test_given_time_range_when_reading_earliest_should_return_only_records_in_range(  # noqa: E501
        self, transport_client: IggyClient, unique_name
    ):
        """`since` and `until` keep the records prepared inside the range."""
        cursor = await latest_metadata_cursor(transport_client, [STREAMS])
        stream = await transport_client.create_stream(unique_name())
        followed = await follow_stream_changes(
            transport_client, cursor, [STREAMS], stream.id, 1
        )
        timestamp = stream_records(followed, stream.id)[0].timestamp

        page = await transport_client.get_metadata_changes(
            MetadataChangeStart.Earliest(),
            groups=[STREAMS],
            since=timestamp,
            until=timestamp,
            limit=METADATA_CHANGES_PAGE_LIMIT,
        )

        assert page.status == MetadataChangeStatus.Ok
        assert page.records
        assert all(record.timestamp == timestamp for record in page.records)
        assert stream_records(page.records, stream.id)

    @pytest.mark.asyncio
    async def test_given_foreign_and_future_cursors_when_following_should_answer_reset_and_behind(  # noqa: E501
        self, transport_client: IggyClient
    ):
        """A cursor of another history resets, one past the head is behind."""
        latest = await transport_client.get_metadata_changes(
            MetadataChangeStart.Latest()
        )
        assert latest.log_id is not None
        foreign = MetadataChangeCursor(latest.log_id ^ 1, 1, 1, 0)
        ahead = MetadataChangeCursor(latest.log_id, latest.head_op + 1_000_000, 1, 0)

        reset = await transport_client.get_metadata_changes(
            MetadataChangeStart.After(foreign)
        )
        behind = await transport_client.get_metadata_changes(
            MetadataChangeStart.After(ahead)
        )

        assert reset.status == MetadataChangeStatus.Reset
        assert (reset.next, reset.records) == (None, [])
        assert behind.status == MetadataChangeStatus.Behind
        assert (behind.next, behind.records) == (ahead, [])


@pytest.mark.usefixtures("iggy_client")
class TestMetadataChangesCurrentState:
    """Current state pages on every transport."""

    @pytest.mark.asyncio
    async def test_given_current_scan_when_paging_should_match_stream_list(
        self, transport_client: IggyClient, unique_name
    ):
        """Pages of two streams add up to the stream list, then hand an anchor."""
        for _ in range(3):
            await transport_client.create_stream(unique_name())

        items = []
        page_token = None
        anchor = None
        pages = 0
        while True:
            page = await transport_client.get_metadata_changes(
                MetadataChangeStart.Current(page_token), groups=[STREAMS], limit=2
            )
            pages += 1
            assert page.status == MetadataChangeStatus.Ok
            assert page.phase == MetadataChangePhase.Current
            assert page.records == []
            assert len(page.items) <= 2
            items.extend(page.items)
            page_token = page.current_page_token
            if page_token is None:
                anchor = page.next
                break
            assert page.has_more
            assert page_token.groups == [STREAMS]
            assert MetadataCurrentPageToken.parse(str(page_token)) == page_token

        listed = {
            (stream.id, stream.name) for stream in await transport_client.get_streams()
        }
        projected = set()
        for item in items:
            assert item.group == STREAMS
            body = item.body
            assert isinstance(body, MetadataStreamChange)
            projected.add((body.stream_id, body.name))
        assert projected == listed
        assert pages >= 2
        assert anchor is not None
        following = await transport_client.get_metadata_changes(
            MetadataChangeStart.After(anchor), groups=[STREAMS]
        )
        assert following.status == MetadataChangeStatus.Ok


@pytest.mark.usefixtures("iggy_client")
class TestMetadataChangesRefusals:
    """Refused queries and callers on every transport."""

    @pytest.mark.asyncio
    async def test_given_refused_query_vectors_when_reading_should_raise_before_sending(  # noqa: E501
        self, transport_client: IggyClient
    ):
        """The client refuses what the server would, before any I/O."""
        for vector in FIXTURE["refused"]["queries"]:
            with pytest.raises(IggyError) as refusal:
                read_vector(transport_client, vector["input"])
            assert refusal.value.code == INVALID_COMMAND, vector["case"]

    @pytest.mark.asyncio
    async def test_given_user_without_manage_servers_when_reading_should_raise_unauthorized(  # noqa: E501
        self, transport_client: IggyClient, iggy_client: IggyClient, unique_name
    ):
        """Read permissions do not authorize the change log."""
        username, password = unique_credentials(unique_name)
        user = await iggy_client.create_user(
            username,
            password,
            permissions=Permissions(
                global_permissions=GlobalPermissions(read_servers=True)
            ),
        )
        try:
            await transport_client.logout_user()
            await transport_client.login_user(username, password)
            with pytest.raises(IggyError) as refusal:
                await transport_client.get_metadata_changes(
                    MetadataChangeStart.Earliest()
                )
            assert refusal.value.code == UNAUTHORIZED
        finally:
            await iggy_client.delete_user(user.id)


class TestMetadataChangesVectors:
    """The binding against the vectors the Rust encoders generated."""

    @pytest.mark.unit
    def test_given_cursor_vectors_when_parsing_should_round_trip_every_field(self):
        """Cursor text and fields convert both ways without losing a bit."""
        for vector in FIXTURE["cursors"]:
            cursor = MetadataChangeCursor.parse(vector["text"])
            fields = (
                int(vector["log_id"], 16),
                int(vector["op"]),
                int(vector["checksum"], 16),
                vector["ordinal"],
            )
            assert (cursor.log_id, cursor.op, cursor.checksum, cursor.ordinal) == fields
            assert str(cursor) == vector["text"]
            assert len(vector["text"]) == CONSTANTS["cursor_text_length"]
            assert MetadataChangeCursor(*fields) == cursor
            assert hash(MetadataChangeCursor(*fields)) == hash(cursor)

    @pytest.mark.unit
    def test_given_page_token_vectors_when_parsing_should_expose_every_field(self):
        """A token names its anchor, head, groups and last key."""
        codes = {name: group["code"] for name, group in CONSTANTS["groups"].items()}
        for vector in FIXTURE["page_tokens"]:
            token = MetadataCurrentPageToken.parse(vector["text"])
            assert str(token) == vector["text"]
            assert str(token.anchor) == vector["anchor"]
            assert token.required_head == vector["required_head"]
            assert str(token.last_group) == vector["group"]
            assert token.last_key.hex() == vector["last_key_hex"]
            masked = sorted(
                (code, name)
                for name, code in codes.items()
                if vector["groups_mask"] & (1 << (code - 1))
            )
            assert [str(group) for group in token.groups] == [
                name for _, name in masked
            ]

    @pytest.mark.unit
    def test_given_refused_cursor_and_token_texts_when_parsing_should_raise_value_error(  # noqa: E501
        self,
    ):
        """Prefix, length, padding, alphabet and canonical form are strict."""
        for vector in FIXTURE["refused"]["cursors"]:
            with pytest.raises(ValueError, match="invalid metadata change cursor"):
                MetadataChangeCursor.parse(vector["text"])
        for vector in FIXTURE["refused"]["page_tokens"]:
            with pytest.raises(ValueError, match="invalid metadata current page token"):
                MetadataCurrentPageToken.parse(vector["text"])

    @pytest.mark.unit
    def test_given_refused_query_vectors_when_reading_should_raise_before_network_io(  # noqa: E501
        self,
    ):
        """A client that never connects proves the refusal happens before I/O."""
        client = IggyClient("127.0.0.1:1")
        for vector in FIXTURE["refused"]["queries"]:
            with pytest.raises(IggyError, match="Invalid command") as refusal:
                read_vector(client, vector["input"])
            assert refusal.value.code == INVALID_COMMAND, vector["case"]

    @pytest.mark.unit
    @pytest.mark.asyncio
    async def test_given_request_vectors_when_reading_should_pass_validation(self):
        """Accepted queries get past validation and fail only on the transport."""
        client = IggyClient("127.0.0.1:1")
        for vector in FIXTURE["requests"]:
            with pytest.raises(IggyError) as failure:
                await read_vector(client, vector["input"])
            assert failure.value.code != INVALID_COMMAND, vector["case"]

    @pytest.mark.unit
    def test_given_fixture_constants_when_reading_enums_should_cover_every_name(self):
        """Every group, kind, status, phase and reason has its member."""
        for enum_type, section in ENUM_SECTIONS:
            members = {
                str(value)
                for value in vars(enum_type).values()
                if isinstance(value, enum_type)
            }
            assert members == set(CONSTANTS[section]), section

    @pytest.mark.unit
    def test_given_enum_members_when_used_as_keys_should_hash_by_value(self):
        """Members key dicts and sets, and equal members hash alike."""
        for enum_type, section in ENUM_SECTIONS:
            members = [
                value
                for value in vars(enum_type).values()
                if isinstance(value, enum_type)
            ]
            names = {member: str(member) for member in members}
            assert len(set(members)) == len(names) == len(members), section
            assert all(names[member] == str(member) for member in members), section
        for vector in FIXTURE["page_tokens"]:
            last_group = MetadataCurrentPageToken.parse(vector["text"]).last_group
            assert last_group in {group(vector["group"])}
            assert hash(last_group) == hash(group(vector["group"]))


def read_vector(client: IggyClient, query: dict):
    """Start `get_metadata_changes` with a fixture query input."""
    start = {
        "earliest": lambda: MetadataChangeStart.Earliest(),
        "latest": lambda: MetadataChangeStart.Latest(),
        "after": lambda: MetadataChangeStart.After(
            MetadataChangeCursor.parse(query["cursor"])
        ),
        "current": lambda: MetadataChangeStart.Current(
            MetadataCurrentPageToken.parse(query["page_token"])
            if query["page_token"]
            else None
        ),
    }[query["start"]]()
    return client.get_metadata_changes(
        start,
        groups=[group(name) for name in query["groups"]],
        since=query["since"],
        until=query["until"],
        limit=query["limit"],
    )


def group(name: str) -> MetadataChangeGroup:
    return getattr(
        MetadataChangeGroup, "".join(part.title() for part in name.split("_"))
    )
