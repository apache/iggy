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
Tests for reading the list commands one page at a time.

The server cases run once per transport through `transport_client`. Other
tests share the server, so each case filters by a name of its own. A page can
hold fewer items than its limit and still have a `next`, so each case reads
every page of a query and never expects a page count. The vector cases read
the fixture the Rust encoders generated and need no server.
"""

import functools
import json
from pathlib import Path

import pytest

from apache_iggy import IggyClient, IggyError, PageOrder, TopicOptions

from .utils import unique_credentials

FIXTURE = json.loads(
    (Path(__file__).parent / "fixtures" / "list_pages.json").read_text()
)
INVALID_COMMAND = FIXTURE["constants"]["errors"]["invalid_command"]
DEFAULT_LIMIT = FIXTURE["constants"]["default_limit"]
# Over HTTP the SDK reports a refusal it has no typed error for as this code,
# with the error body of the server in the message.
HTTP_RESPONSE_ERROR = 300
HTTP_TRANSPORT = "http"
ID_CURSOR = next(
    vector["text"] for vector in FIXTURE["cursors"] if vector["case"] == "id_seven"
)
UNIQUE_NAME_BYTES = 32
SMALL_PAGE = 2
STREAMS_COUNT = 3
PARTITIONS_COUNT = 3
TOPIC = "events"
OTHER_NAME = "other"
RESERVED_CHARACTERS = " a+b&c=d %e#f/?"
NO_SERVER_ADDRESS = "127.0.0.1:1"
TRANSPORT_SCHEMES = ("tcp", "quic", "http", "ws")
LIMITS_OUTSIDE_U32 = (-1, 2**32)
PARENT_ID = 1


@pytest.mark.usefixtures("iggy_client")
class TestListPages:
    """Page reads on every transport."""

    @pytest.mark.asyncio
    async def test_given_three_streams_when_walking_pages_of_two_should_read_them_in_each_order(  # noqa: E501
        self, transport_client: IggyClient, unique_name
    ):
        """Pages of at most two streams cannot hold all three in one page."""
        name = unique_name(min_bytes=UNIQUE_NAME_BYTES, max_bytes=UNIQUE_NAME_BYTES)
        streams = [
            await transport_client.create_stream(f"{name}-{index}")
            for index in range(STREAMS_COUNT)
        ]
        by_id = sorted(stream.id for stream in streams)
        by_created = [
            stream.id
            for stream in sorted(
                streams, key=lambda stream: (stream.created_at, stream.id)
            )
        ]
        read = transport_client.get_streams_page

        assert await walk_ids(read, limit=SMALL_PAGE, name=name) == by_id
        assert (
            await walk_ids(read, limit=SMALL_PAGE, name=name, descending=True)
            == by_id[::-1]
        )
        assert (
            await walk_ids(read, limit=SMALL_PAGE, name=name, order=PageOrder.Created)
            == by_created
        )

    @pytest.mark.asyncio
    async def test_given_topic_with_three_partitions_when_reading_pages_of_two_should_return_them_by_id(  # noqa: E501
        self, transport_client: IggyClient, unique_name
    ):
        """The cursors lead through every partition in both directions."""
        stream = await transport_client.create_stream(unique_name())
        await transport_client.create_topic(
            stream.id, TOPIC, TopicOptions(partitions_count=PARTITIONS_COUNT)
        )
        read = functools.partial(transport_client.get_partitions_page, stream.id, TOPIC)
        ids = list(range(PARTITIONS_COUNT))

        assert await walk_ids(read, limit=SMALL_PAGE) == ids
        assert await walk_ids(read, limit=SMALL_PAGE, descending=True) == ids[::-1]

    @pytest.mark.asyncio
    async def test_given_a_name_filter_when_reading_topics_groups_and_users_should_keep_only_matching_names(  # noqa: E501
        self, transport_client: IggyClient, unique_name
    ):
        """Only the entries whose name holds the filter text come back."""
        name, password = unique_credentials(unique_name)
        topic = f"{name}-topic"
        stream = await transport_client.create_stream(f"{name}-stream")
        await transport_client.create_topic(stream.id, topic)
        await transport_client.create_topic(stream.id, OTHER_NAME)
        await transport_client.create_consumer_group(stream.id, topic, f"{name}-group")
        await transport_client.create_consumer_group(stream.id, topic, OTHER_NAME)
        user = await transport_client.create_user(name, password)
        try:
            topics = await walk(
                functools.partial(transport_client.get_topics_page, stream.id),
                name=name,
            )
            groups = await walk(
                functools.partial(
                    transport_client.get_consumer_groups_page, stream.id, topic
                ),
                name=name,
            )
            users = await walk(transport_client.get_users_page, name=name)
        finally:
            await transport_client.delete_user(user.id)

        assert [found.name for found in topics] == [topic]
        assert [found.name for found in groups] == [f"{name}-group"]
        assert [found.username for found in users] == [name]

    @pytest.mark.asyncio
    async def test_given_a_name_with_reserved_characters_when_filtering_streams_should_match_it_exactly(  # noqa: E501
        self, transport_client: IggyClient, unique_name
    ):
        """The HTTP server decodes the query as a form, so each character must arrive escaped."""  # noqa: E501
        name = unique_name(min_bytes=UNIQUE_NAME_BYTES, max_bytes=UNIQUE_NAME_BYTES)
        stream = await transport_client.create_stream(f"{name}{RESERVED_CHARACTERS}")

        found = await walk_ids(transport_client.get_streams_page, name=stream.name)

        assert found == [stream.id]

    @pytest.mark.asyncio
    async def test_given_a_cursor_the_server_cannot_use_when_reading_should_refuse_it_and_keep_working(  # noqa: E501
        self,
        transport_client: IggyClient,
        unique_name,
        request: pytest.FixtureRequest,
    ):
        """An id cursor in a creation walk is refused.

        Over HTTP the refusal has the HTTP response error code, and its error
        body holds the server code.
        """
        assert isinstance(request.node, pytest.Function)
        transport = request.node.callspec.params["transport_client"]
        code = HTTP_RESPONSE_ERROR if transport == HTTP_TRANSPORT else INVALID_COMMAND
        stream = await transport_client.create_stream(unique_name())

        with pytest.raises(IggyError) as refusal:
            await transport_client.get_streams_page(
                order=PageOrder.Created, cursor=ID_CURSOR
            )
        assert refusal.value.code == code
        assert server_code(refusal.value) == INVALID_COMMAND

        found = await walk_ids(transport_client.get_streams_page, name=stream.name)
        assert found == [stream.id]


class TestListPagesVectors:
    """Page reads on a client that never connects."""

    @pytest.mark.unit
    @pytest.mark.parametrize("scheme", TRANSPORT_SCHEMES)
    def test_given_refused_and_opaque_cursor_texts_when_reading_should_raise_before_network_io(  # noqa: E501
        self, scheme: str
    ):
        """A client that never connects proves the refusal happens before I/O."""
        client = never_connected(scheme)
        for vector in FIXTURE["refused"]["cursors"] + FIXTURE["opaque_cursors"]:
            for read in page_reads(client):
                with pytest.raises(IggyError, match="Invalid command") as refusal:
                    read(cursor=vector["text"])
                assert refusal.value.code == INVALID_COMMAND, vector["case"]

    @pytest.mark.unit
    @pytest.mark.parametrize("scheme", TRANSPORT_SCHEMES)
    def test_given_refused_query_vectors_when_reading_should_raise_before_network_io(  # noqa: E501
        self, scheme: str
    ):
        """A limit or a name out of range fails at the call."""
        client = never_connected(scheme)
        for vector in FIXTURE["refused"]["queries"]:
            for read in page_reads(client, vector["input"]):
                with pytest.raises(IggyError, match="Invalid command") as refusal:
                    read(**page_arguments(vector["input"]))
                assert refusal.value.code == INVALID_COMMAND, vector["case"]

    @pytest.mark.unit
    def test_given_a_limit_outside_the_u32_range_when_reading_should_raise_overflow_error(  # noqa: E501
        self,
    ):
        """The limit fails in the argument conversion, as in `create_partitions`."""
        client = IggyClient(NO_SERVER_ADDRESS)
        for limit in LIMITS_OUTSIDE_U32:
            for read in page_reads(client):
                with pytest.raises(OverflowError):
                    read(limit=limit)

    @pytest.mark.unit
    @pytest.mark.asyncio
    async def test_given_request_vectors_when_reading_should_pass_validation(self):
        """Accepted queries get past validation and fail only on the transport."""
        client = IggyClient(NO_SERVER_ADDRESS)
        for vector in FIXTURE["requests"]:
            for read in page_reads(client, vector["input"]):
                with pytest.raises(IggyError) as failure:
                    await read(**page_arguments(vector["input"]))
                assert failure.value.code != INVALID_COMMAND, vector["case"]


async def walk(read_page, **query) -> list:
    """The items of every page of a query, read until a page has no `next`."""
    items = []
    cursor: str | None = None
    while True:
        page = await read_page(**query, cursor=cursor)
        assert len(page.items) <= query.get("limit", DEFAULT_LIMIT)
        items.extend(page.items)
        cursor = page.next
        if cursor is None:
            return items


async def walk_ids(read_page, **query) -> list[int]:
    """The ids of every page of a query."""
    return [item.id for item in await walk(read_page, **query)]


def server_code(error: IggyError) -> int:
    """The code of a server refusal, read from the HTTP error body when needed."""
    if error.code != HTTP_RESPONSE_ERROR:
        return error.code
    return json.loads(str(error).split("body: ", 1)[1])["id"]


def never_connected(scheme: str) -> IggyClient:
    """A client of the transport, for an address that has no server."""
    return IggyClient.from_connection_string(
        f"iggy+{scheme}://iggy:iggy@{NO_SERVER_ADDRESS}"
    )


def page_reads(client: IggyClient, query: dict | None = None):
    """Each page method that takes `query`, with the parent ids it needs.

    `get_partitions_page` takes no `name`, so a query with a name skips it.
    """
    named = (
        client.get_streams_page,
        functools.partial(client.get_topics_page, PARENT_ID),
        functools.partial(client.get_consumer_groups_page, PARENT_ID, PARENT_ID),
        client.get_users_page,
    )
    if query is not None and query["name"] is not None:
        return named
    return (*named, functools.partial(client.get_partitions_page, PARENT_ID, PARENT_ID))


def page_arguments(query: dict) -> dict:
    """The keyword arguments of a fixture query input, without a null name."""
    arguments = {
        "order": getattr(PageOrder, query["order"].title()),
        "descending": query["descending"],
        "limit": query["limit"],
        "cursor": query["cursor"],
    }
    if query["name"] is not None:
        arguments["name"] = query["name"]
    return arguments
