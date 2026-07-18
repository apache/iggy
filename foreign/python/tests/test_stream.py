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

import pytest

from apache_iggy import IggyClient, SendMessage

from .utils import get_server_config, wait_for_ping, wait_for_server


class TestStreamOperations:
    """Test stream creation, retrieval, and management."""

    @pytest.mark.asyncio
    @pytest.mark.parametrize(
        ("prefix", "min_bytes", "max_bytes"),
        [
            ("a", 8, 255),
            ("stream-name", 8, 255),
            ("stream_name.with.mixed-CHARS123", 8, 255),
            (" leading-space", 8, 255),
            ("trailing-space ", 8, 255),
            ("multiple  spaces  inside", 8, 255),
            ("name/with/slash", 8, 255),
            ("name:with:colons", 8, 255),
            ("name.with.dots", 8, 255),
            ("   ", 8, 255),
            ("a" * 247, 255, 255),
            (("é" * 122) + "abc", 255, 255),
            (("한" * 81) + "abc", 255, 255),
            (("漢" * 81) + "abc", 255, 255),
            (("あ" * 81) + "abc", 255, 255),
            (("😀" * 60) + "abcdefg", 255, 255),
        ],
    )
    async def test_create_and_get_stream(
        self,
        iggy_client: IggyClient,
        unique_name,
        prefix: str,
        min_bytes: int,
        max_bytes: int,
    ):
        """Test stream creation and retrieval."""
        stream_name = unique_name(prefix, min_bytes=min_bytes, max_bytes=max_bytes)

        await iggy_client.create_stream(stream_name)

        stream = await iggy_client.get_stream(stream_name)
        assert stream is not None
        assert stream.name == stream_name
        assert stream.topics_count == 0

    @pytest.mark.asyncio
    @pytest.mark.parametrize(
        ("prefix", "min_bytes", "max_bytes"),
        [
            ("", 0, 0),
            ("a" * 248, 256, 256),
            ("é" * 124, 256, 256),
            (("é" * 123) + "ab", 256, 256),
            (("한" * 82) + "ab", 256, 256),
            (("漢" * 82) + "ab", 256, 256),
            (("あ" * 82) + "ab", 256, 256),
            ("😀" * 62, 256, 256),
            (("😀" * 61) + "abcd", 256, 256),
        ],
    )
    async def test_create_stream_invalid_names(
        self,
        iggy_client: IggyClient,
        unique_name,
        prefix: str,
        min_bytes: int,
        max_bytes: int,
    ):
        """Test create_stream enforces byte-length validation."""
        stream_name = unique_name(prefix, min_bytes=min_bytes, max_bytes=max_bytes)

        with pytest.raises(RuntimeError):
            await iggy_client.create_stream(stream_name)

    @pytest.mark.asyncio
    async def test_get_stream_by_name_and_id(
        self, iggy_client: IggyClient, unique_name
    ):
        """Test repeated stream lookup works by both name and numeric id."""
        stream_name = unique_name()
        await iggy_client.create_stream(stream_name)

        stream_by_name = await iggy_client.get_stream(stream_name)
        assert stream_by_name is not None

        stream_by_name_again = await iggy_client.get_stream(stream_name)
        assert stream_by_name_again is not None
        assert stream_by_name_again.id == stream_by_name.id
        assert stream_by_name_again.name == stream_by_name.name

        stream_by_id = await iggy_client.get_stream(stream_by_name.id)
        assert stream_by_id is not None
        assert stream_by_id.id == stream_by_name.id
        assert stream_by_id.name == stream_by_name.name

    @pytest.mark.asyncio
    async def test_create_stream_then_reconnect_then_get_stream(
        self, iggy_client: IggyClient, unique_name
    ):
        """Test a stream remains retrievable after reconnecting with a fresh client."""
        stream_name = unique_name()

        await iggy_client.create_stream(stream_name)

        host, port = get_server_config()
        wait_for_server(host, port)

        client = IggyClient(f"{host}:{port}")
        await client.connect()
        await wait_for_ping(client)
        await client.login_user("iggy", "iggy")

        stream = await client.get_stream(stream_name)
        assert stream is not None
        assert stream.name == stream_name
        assert stream.topics_count == 0

    @pytest.mark.asyncio
    async def test_duplicate_stream_creation(
        self, iggy_client: IggyClient, unique_name
    ):
        """Test that creating duplicate streams raises appropriate errors."""
        stream_name = unique_name()

        await iggy_client.create_stream(stream_name)

        with pytest.raises(RuntimeError) as exc_info:
            await iggy_client.create_stream(stream_name)

        assert "already exists" in str(exc_info.value)

    @pytest.mark.asyncio
    async def test_get_nonexistent_stream(self, iggy_client: IggyClient, unique_name):
        """Test getting a non-existent stream by name or numeric id."""
        nonexistent_name = unique_name()

        stream_by_name = await iggy_client.get_stream(nonexistent_name)
        assert stream_by_name is None

        stream_by_id = await iggy_client.get_stream(999999)
        assert stream_by_id is None

    @pytest.mark.asyncio
    async def test_get_stream_before_connect_fails(self, unique_name):
        """Test get_stream requires an established connection."""
        host, port = get_server_config()
        client = IggyClient(f"{host}:{port}")

        with pytest.raises(RuntimeError):
            await client.get_stream(unique_name())

    @pytest.mark.asyncio
    async def test_get_stream_before_login_fails(self, unique_name):
        """Test get_stream requires authentication."""
        host, port = get_server_config()
        wait_for_server(host, port)

        client = IggyClient(f"{host}:{port}")
        await client.connect()

        with pytest.raises(RuntimeError):
            await client.get_stream(unique_name())

    @pytest.mark.asyncio
    async def test_create_stream_before_connect_fails(self, unique_name):
        """Test create_stream requires an established connection."""
        host, port = get_server_config()
        client = IggyClient(f"{host}:{port}")

        with pytest.raises(RuntimeError):
            await client.create_stream(unique_name())

    @pytest.mark.asyncio
    async def test_create_stream_before_login_fails(self, unique_name):
        """Test create_stream requires authentication."""
        host, port = get_server_config()
        wait_for_server(host, port)

        client = IggyClient(f"{host}:{port}")
        await client.connect()

        with pytest.raises(RuntimeError):
            await client.create_stream(unique_name())


class TestGetStreams:
    """Test listing streams via get_streams."""

    @pytest.mark.asyncio
    async def test_get_streams_returns_created_streams(
        self, iggy_client: IggyClient, unique_name
    ):
        """Test get_streams returns every stream created during the test."""
        # Reverse-alphabetical names so that id-ascending (creation) order
        # and name order disagree, proving the list isn't accidentally
        # name-sorted. The client fixture is session-scoped, so other tests
        # may have created streams; assert on the ones created here instead
        # of the full server view.
        created = [unique_name(f"z{index}") for index in range(3, 0, -1)]
        for name in created:
            await iggy_client.create_stream(name)

        streams = await iggy_client.get_streams()
        by_name = {stream.name: stream for stream in streams}
        assert set(created).issubset(by_name)

        mine = [by_name[name] for name in created]
        assert [stream.id for stream in mine] == sorted(stream.id for stream in mine)
        assert all(stream.messages_count == 0 for stream in mine)
        assert all(stream.topics_count == 0 for stream in mine)

    @pytest.mark.asyncio
    async def test_get_streams_reflects_topic_count(
        self, iggy_client: IggyClient, unique_name
    ):
        """Test get_streams reports the topic count for a listed stream."""
        stream_name = unique_name()
        topic_name = unique_name()

        await iggy_client.create_stream(stream_name)
        await iggy_client.create_topic(
            stream=stream_name, name=topic_name, partitions_count=1
        )

        streams = await iggy_client.get_streams()
        listed = next(
            (stream for stream in streams if stream.name == stream_name), None
        )
        assert listed is not None
        assert listed.topics_count == 1

    @pytest.mark.asyncio
    async def test_get_streams_returns_same_result_when_called_repeatedly(
        self, iggy_client: IggyClient, unique_name
    ):
        """Test repeated get_streams calls return an identically ordered view."""
        await iggy_client.create_stream(unique_name())

        first = await iggy_client.get_streams()
        second = await iggy_client.get_streams()
        assert [stream.id for stream in first] == [stream.id for stream in second]
        assert [stream.name for stream in first] == [stream.name for stream in second]

    @pytest.mark.asyncio
    async def test_get_streams_before_connect_fails(self):
        """Test get_streams requires an established connection."""
        host, port = get_server_config()
        client = IggyClient(f"{host}:{port}")

        with pytest.raises(RuntimeError):
            await client.get_streams()

    @pytest.mark.asyncio
    async def test_get_streams_before_login_fails(self):
        """Test get_streams requires authentication."""
        host, port = get_server_config()
        wait_for_server(host, port)

        client = IggyClient(f"{host}:{port}")
        await client.connect()

        with pytest.raises(RuntimeError):
            await client.get_streams()


class TestUpdateStream:
    """Test updating streams via update_stream."""

    @pytest.mark.asyncio
    async def test_update_stream_renames_stream(
        self, iggy_client: IggyClient, unique_name
    ):
        """Test update_stream renames a stream; old name no longer resolves."""
        stream_name = unique_name()
        new_name = unique_name()

        await iggy_client.create_stream(stream_name)

        await iggy_client.update_stream(stream_id=stream_name, name=new_name)

        renamed = await iggy_client.get_stream(new_name)
        assert renamed is not None
        assert renamed.name == new_name

        old = await iggy_client.get_stream(stream_name)
        assert old is None

    @pytest.mark.asyncio
    async def test_update_stream_preserves_id(
        self, iggy_client: IggyClient, unique_name
    ):
        """Test update_stream keeps the same numeric id after a rename."""
        stream_name = unique_name()
        new_name = unique_name()

        await iggy_client.create_stream(stream_name)
        before = await iggy_client.get_stream(stream_name)
        assert before is not None

        await iggy_client.update_stream(stream_id=stream_name, name=new_name)

        after = await iggy_client.get_stream(new_name)
        assert after is not None
        assert after.id == before.id

    @pytest.mark.asyncio
    async def test_update_stream_by_numeric_id(
        self, iggy_client: IggyClient, unique_name
    ):
        """Test update_stream accepts a numeric stream id."""
        stream_name = unique_name()
        new_name = unique_name()

        await iggy_client.create_stream(stream_name)
        stream = await iggy_client.get_stream(stream_name)
        assert stream is not None

        await iggy_client.update_stream(stream_id=stream.id, name=new_name)

        renamed = await iggy_client.get_stream(new_name)
        assert renamed is not None
        assert renamed.name == new_name

    @pytest.mark.asyncio
    async def test_update_stream_applies_repeated_updates(
        self, iggy_client: IggyClient, unique_name
    ):
        """Test successive update_stream calls each take effect."""
        stream_name = unique_name()
        first_rename = unique_name()
        second_rename = unique_name()

        await iggy_client.create_stream(stream_name)

        await iggy_client.update_stream(stream_id=stream_name, name=first_rename)
        after_first = await iggy_client.get_stream(first_rename)
        assert after_first is not None
        assert after_first.name == first_rename
        assert await iggy_client.get_stream(stream_name) is None

        await iggy_client.update_stream(stream_id=first_rename, name=second_rename)
        after_second = await iggy_client.get_stream(second_rename)
        assert after_second is not None
        assert after_second.name == second_rename
        assert await iggy_client.get_stream(first_rename) is None

    @pytest.mark.asyncio
    async def test_update_nonexistent_stream_fails(
        self, iggy_client: IggyClient, unique_name
    ):
        """Test update_stream raises for a non-existent stream."""
        with pytest.raises(RuntimeError):
            await iggy_client.update_stream(stream_id=unique_name(), name=unique_name())

    @pytest.mark.asyncio
    async def test_update_stream_to_existing_name_fails(
        self, iggy_client: IggyClient, unique_name
    ):
        """Test update_stream rejects renaming a stream to a name already in use."""
        first_stream = unique_name()
        second_stream = unique_name()

        await iggy_client.create_stream(first_stream)
        await iggy_client.create_stream(second_stream)

        with pytest.raises(RuntimeError):
            await iggy_client.update_stream(stream_id=second_stream, name=first_stream)

    @pytest.mark.asyncio
    async def test_update_stream_requires_connection_and_auth(self, unique_name):
        """Test update_stream fails both before connecting and before logging in."""
        host, port = get_server_config()
        wait_for_server(host, port)

        client = IggyClient(f"{host}:{port}")
        with pytest.raises(RuntimeError):
            await client.update_stream(stream_id=unique_name(), name=unique_name())

        await client.connect()
        with pytest.raises(RuntimeError):
            await client.update_stream(stream_id=unique_name(), name=unique_name())


class TestDeleteStream:
    """Test deleting streams via delete_stream."""

    @pytest.mark.asyncio
    async def test_delete_stream_removes_stream(
        self, iggy_client: IggyClient, unique_name
    ):
        """Test delete_stream removes the stream so it no longer resolves."""
        stream_name = unique_name()

        await iggy_client.create_stream(stream_name)

        await iggy_client.delete_stream(stream_name)

        assert await iggy_client.get_stream(stream_name) is None

    @pytest.mark.asyncio
    async def test_delete_stream_by_numeric_id(
        self, iggy_client: IggyClient, unique_name
    ):
        """Test delete_stream accepts a numeric stream id."""
        stream_name = unique_name()

        await iggy_client.create_stream(stream_name)
        stream = await iggy_client.get_stream(stream_name)
        assert stream is not None

        await iggy_client.delete_stream(stream.id)

        assert await iggy_client.get_stream(stream_name) is None

    @pytest.mark.asyncio
    async def test_delete_stream_leaves_other_streams(
        self, iggy_client: IggyClient, unique_name
    ):
        """Test delete_stream removes only the targeted stream."""
        stream_to_delete = unique_name()
        stream_to_keep = unique_name()

        await iggy_client.create_stream(stream_to_delete)
        await iggy_client.create_stream(stream_to_keep)

        await iggy_client.delete_stream(stream_to_delete)

        assert await iggy_client.get_stream(stream_to_delete) is None
        kept = await iggy_client.get_stream(stream_to_keep)
        assert kept is not None
        assert kept.name == stream_to_keep

    @pytest.mark.asyncio
    async def test_delete_nonexistent_stream_fails(
        self, iggy_client: IggyClient, unique_name
    ):
        """Test delete_stream raises for a non-existent stream."""
        with pytest.raises(RuntimeError):
            await iggy_client.delete_stream(unique_name())

    @pytest.mark.asyncio
    async def test_delete_stream_twice_fails_second_time(
        self, iggy_client: IggyClient, unique_name
    ):
        """Test deleting an already-deleted stream raises on the second call."""
        stream_name = unique_name()

        await iggy_client.create_stream(stream_name)

        await iggy_client.delete_stream(stream_name)
        with pytest.raises(RuntimeError):
            await iggy_client.delete_stream(stream_name)

    @pytest.mark.asyncio
    async def test_delete_stream_requires_connection_and_auth(self, unique_name):
        """Test delete_stream fails both before connecting and before logging in."""
        host, port = get_server_config()
        wait_for_server(host, port)

        client = IggyClient(f"{host}:{port}")
        with pytest.raises(RuntimeError):
            await client.delete_stream(unique_name())

        await client.connect()
        with pytest.raises(RuntimeError):
            await client.delete_stream(unique_name())


class TestPurgeStream:
    """Test purging stream messages via purge_stream."""

    @pytest.mark.asyncio
    async def test_purge_stream_clears_messages_but_keeps_stream(
        self, iggy_client: IggyClient, unique_name
    ):
        """Test purge_stream empties the stream while leaving it in place."""
        stream_name = unique_name()
        topic_name = unique_name()

        await iggy_client.create_stream(stream_name)
        await iggy_client.create_topic(
            stream=stream_name, name=topic_name, partitions_count=1
        )

        messages = [SendMessage(f"payload-{index}") for index in range(5)]
        await iggy_client.send_messages(stream_name, topic_name, 0, messages)

        before = await iggy_client.get_stream(stream_name)
        assert before is not None
        assert before.messages_count == 5

        await iggy_client.purge_stream(stream_name)

        after = await iggy_client.get_stream(stream_name)
        # Purging clears messages only; the stream itself survives (purge is
        # not delete) and keeps its identity and topics.
        assert after is not None
        assert after.messages_count == 0
        assert after.id == before.id
        assert after.name == before.name
        assert after.topics_count == before.topics_count

    @pytest.mark.asyncio
    async def test_purge_empty_stream_succeeds(
        self, iggy_client: IggyClient, unique_name
    ):
        """Test purge_stream is a no-op on a stream with no messages."""
        stream_name = unique_name()

        await iggy_client.create_stream(stream_name)

        await iggy_client.purge_stream(stream_name)

        stream = await iggy_client.get_stream(stream_name)
        assert stream is not None
        assert stream.messages_count == 0

    @pytest.mark.asyncio
    async def test_purge_stream_is_idempotent_when_called_repeatedly(
        self, iggy_client: IggyClient, unique_name
    ):
        """Test purge_stream succeeds when called repeatedly on the same stream."""
        stream_name = unique_name()
        topic_name = unique_name()

        await iggy_client.create_stream(stream_name)
        await iggy_client.create_topic(
            stream=stream_name, name=topic_name, partitions_count=1
        )

        messages = [SendMessage(f"payload-{index}") for index in range(5)]
        await iggy_client.send_messages(stream_name, topic_name, 0, messages)

        await iggy_client.purge_stream(stream_name)
        await iggy_client.purge_stream(stream_name)

        stream = await iggy_client.get_stream(stream_name)
        assert stream is not None
        assert stream.messages_count == 0

    @pytest.mark.asyncio
    async def test_purge_nonexistent_stream_fails(
        self, iggy_client: IggyClient, unique_name
    ):
        """Test purge_stream raises for a non-existent stream."""
        with pytest.raises(RuntimeError):
            await iggy_client.purge_stream(unique_name())

    @pytest.mark.asyncio
    async def test_purge_stream_requires_connection_and_auth(self, unique_name):
        """Test purge_stream fails both before connecting and before logging in."""
        host, port = get_server_config()
        wait_for_server(host, port)

        client = IggyClient(f"{host}:{port}")
        with pytest.raises(RuntimeError):
            await client.purge_stream(unique_name())

        await client.connect()
        with pytest.raises(RuntimeError):
            await client.purge_stream(unique_name())
