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
Metadata changes BDD steps for the Python SDK.

The server, authentication, stream, topic and rename steps are shared through
conftest.py.
"""

import asyncio
import base64
from pathlib import Path

from apache_iggy import (
    GlobalPermissions,
    IggyError,
    MetadataChangeGroup,
    MetadataChangeKind,
    MetadataChangeRecord,
    MetadataChanges,
    MetadataChangeStart,
    MetadataChangeStatus,
    MetadataStreamChange,
    MetadataStreamDeleted,
    Permissions,
    encryption_key_fingerprint,
)
from pytest_bdd import given, parsers, scenarios, then, when

FEATURE_FILE = Path("/app/features/metadata_changes.feature")
if not FEATURE_FILE.exists():
    FEATURE_FILE = (
        Path(__file__).resolve().parents[2] / "scenarios/metadata_changes.feature"
    )
scenarios(str(FEATURE_FILE))

ROOT_USER_ID = 0
UNAUTHORIZED = 41
PAGE_LIMIT = 1000
# A two phase delete commits its record after the call returns.
FOLLOW_ATTEMPTS = 100
FOLLOW_INTERVAL_SECONDS = 0.1
NEVER_RETURNED_KEYS = {"encryption_key", "encryption_key_wrapped", "critical_keys"}


@given(
    parsers.re(
        r'I hold the latest metadata change cursor for the groups "(?P<groups>[^"]+)"'
    )
)
def hold_latest_cursor(context, groups):
    context.last_change_groups = parse_groups(groups)
    page = asyncio.run(read_changes(context, MetadataChangeStart.Latest()))
    assert page.status == MetadataChangeStatus.Ok
    assert page.next is not None
    context.last_change_cursor = page.next


@when("I delete the stream")
def delete_stream(context):
    async def delete():
        await context.client.delete_stream(context.last_stream_id)

    asyncio.run(delete())


@when(
    parsers.re(r'I create and delete (?P<count>\d+) streams named "(?P<prefix>[^"]+)"'),
    converters={"count": int},
)
def churn_streams(context, count, prefix):
    async def churn():
        for index in range(count):
            stream = await context.client.create_stream(f"{prefix}-{index}")
            await context.client.delete_stream(stream.id)

    asyncio.run(churn())


@then(
    parsers.re(
        r"following the metadata changes should return the kinds "
        r'"(?P<kinds>[^"]+)" for the stream'
    )
)
def followed_kinds(context, kinds):
    expected = [parse_member(MetadataChangeKind, name) for name in kinds.split(",")]

    async def follow() -> list[MetadataChangeRecord]:
        records: list[MetadataChangeRecord] = []
        cursor = context.last_change_cursor
        for _ in range(FOLLOW_ATTEMPTS):
            page = await read_changes(context, MetadataChangeStart.After(cursor))
            assert page.status == MetadataChangeStatus.Ok
            assert page.next is not None
            cursor = page.next
            records.extend(page.records)
            if page.has_more:
                continue
            if len(stream_records(context, records)) >= len(expected):
                break
            await asyncio.sleep(FOLLOW_INTERVAL_SECONDS)
        return records

    records = asyncio.run(follow())
    assert all(record.group in context.last_change_groups for record in records)
    mine = stream_records(context, records)
    assert [record.kind for record in mine] == expected
    context.last_changes = mine


@then("each followed change should name the root user as its actor")
def root_actor(context):
    assert context.last_changes
    for record in context.last_changes:
        assert record.actor_user_id == ROOT_USER_ID, record


@then(
    parsers.re(
        r'the followed "(?P<kind>[^"]+)" change should carry a requested op '
        r"before its own op"
    )
)
def requested_op_before_own(context, kind):
    record = followed(context, kind)
    deleted = record.change
    assert isinstance(deleted, MetadataStreamDeleted)
    assert deleted.requested_op is not None
    assert deleted.requested_op < record.op


@then(
    parsers.re(
        r'the followed "(?P<kind>[^"]+)" change should report encryption key '
        r'source "(?P<source>[^"]+)"'
    )
)
def change_key_source(context, kind, source):
    derived = followed_stream(context, kind).derived_options
    assert derived is not None
    assert derived.to_scalar_dict()["encryption_key_source"] == source


@then(
    parsers.re(
        r'the followed "(?P<kind>[^"]+)" change should carry the stream key '
        r"fingerprint"
    )
)
def change_key_fingerprint(context, kind):
    assert context.last_stream_key is not None
    derived = followed_stream(context, kind).derived_options
    assert derived is not None
    assert derived.to_scalar_dict()[
        "encryption_key_fingerprint"
    ] == encryption_key_fingerprint(context.last_stream_key)


@then("the followed changes should expose no encryption key material")
def changes_hide_keys(context):
    assert context.last_changes
    for record in context.last_changes:
        stream = record.change
        if not isinstance(stream, MetadataStreamChange):
            continue
        assert stream.options is not None
        assert stream.derived_options is not None
        explicit = stream.options.to_scalar_dict()
        derived = stream.derived_options.to_scalar_dict()
        assert not NEVER_RETURNED_KEYS & (set(explicit) | set(derived))
        if context.last_stream_key is not None:
            values = [*explicit.values(), *derived.values()]
            assert context.last_stream_key not in values
            assert base64.b64encode(context.last_stream_key).decode() not in values


@then(parsers.re(r'following the metadata changes should answer "(?P<status>[^"]+)"'))
def follow_status(context, status):
    page = asyncio.run(
        read_changes(context, MetadataChangeStart.After(context.last_change_cursor))
    )
    assert page.status == parse_member(MetadataChangeStatus, status)
    if page.status != MetadataChangeStatus.Ok:
        assert page.records == []
        assert page.next is None


@when(parsers.re(r'I read the current state for the groups "(?P<groups>[^"]+)"'))
def read_current_state(context, groups):
    context.last_change_groups = parse_groups(groups)

    async def scan():
        start = MetadataChangeStart.Current()
        items = []
        while True:
            page = await read_changes(context, start)
            assert page.status == MetadataChangeStatus.Ok
            items.extend(page.items)
            if page.current_page_token is None:
                assert page.next is not None
                context.last_change_cursor = page.next
                return items
            start = MetadataChangeStart.Current(page.current_page_token)

    context.last_current_items = asyncio.run(scan())


@then("the current state should list the same streams as the stream list")
def current_matches_list(context):
    async def list_streams():
        return await context.client.get_streams()

    listed = {(stream.id, stream.name) for stream in asyncio.run(list_streams())}
    projected = {
        (item.body.stream_id, item.body.name)
        for item in context.last_current_items
        if isinstance(item.body, MetadataStreamChange)
    }
    assert projected == listed


@given(
    parsers.re(
        r'a user named "(?P<username>[^"]+)" with password "(?P<password>[^"]+)" '
        r"and no server permissions exists"
    )
)
def user_without_server_permissions(context, username, password):
    async def create():
        user = await context.client.create_user(
            username,
            password,
            permissions=Permissions(
                global_permissions=GlobalPermissions(read_servers=True)
            ),
        )
        context.last_user_id = user.id

    asyncio.run(create())


@when(
    parsers.re(
        r'I authenticate as "(?P<username>[^"]+)" with password "(?P<password>[^"]+)"'
    )
)
def authenticate_as(context, username, password):
    async def login():
        await context.client.logout_user()
        await context.client.login_user(username, password)

    asyncio.run(login())


@then("reading the metadata changes should be refused as unauthorized")
def changes_refused(context):
    context.last_change_groups = []
    try:
        asyncio.run(read_changes(context, MetadataChangeStart.Earliest()))
    except IggyError as error:
        assert error.code == UNAUTHORIZED
    else:
        raise AssertionError("manage_servers is required")


async def read_changes(context, start: MetadataChangeStart) -> MetadataChanges:
    return await context.client.get_metadata_changes(
        start, groups=context.last_change_groups, limit=PAGE_LIMIT
    )


def stream_records(
    context, records: list[MetadataChangeRecord]
) -> list[MetadataChangeRecord]:
    return [
        record
        for record in records
        if getattr(record.change, "stream_id", None) == context.last_stream_id
    ]


def followed(context, kind: str) -> MetadataChangeRecord:
    wanted = parse_member(MetadataChangeKind, kind)
    return next(record for record in context.last_changes if record.kind == wanted)


def followed_stream(context, kind: str) -> MetadataStreamChange:
    stream = followed(context, kind).change
    assert isinstance(stream, MetadataStreamChange)
    assert not stream.options_unreadable
    return stream


def parse_groups(names: str) -> list[MetadataChangeGroup]:
    return [parse_member(MetadataChangeGroup, name) for name in names.split(",")]


def parse_member(enum_type, name: str):
    return getattr(enum_type, "".join(part.title() for part in name.strip().split("_")))
