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

from apache_iggy import IggyClient, IggyError

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

        with pytest.raises(IggyError) as exc_info:
            await iggy_client.create_stream(stream_name)

        assert exc_info.value.name == "invalid_format"
        assert exc_info.value.code == 4
        assert exc_info.value.message == "Invalid format"

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

        with pytest.raises(IggyError) as exc_info:
            await iggy_client.create_stream(stream_name)

        assert exc_info.value.name == "stream_name_already_exists"
        assert exc_info.value.code == 1012
        assert exc_info.value.message.startswith("Stream with name:")

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

        with pytest.raises(IggyError) as exc_info:
            await client.get_stream(unique_name())

        assert exc_info.value.name == "disconnected"
        assert exc_info.value.code == 8
        assert exc_info.value.message == "Disconnected"

    @pytest.mark.asyncio
    async def test_get_stream_before_login_fails(self, unique_name):
        """Test get_stream requires authentication."""
        host, port = get_server_config()
        wait_for_server(host, port)

        client = IggyClient(f"{host}:{port}")
        await client.connect()

        with pytest.raises(IggyError) as exc_info:
            await client.get_stream(unique_name())

        assert exc_info.value.name == "unauthenticated"
        assert exc_info.value.code == 40
        assert exc_info.value.message == "Unauthenticated"

    @pytest.mark.asyncio
    async def test_create_stream_before_connect_fails(self, unique_name):
        """Test create_stream requires an established connection."""
        host, port = get_server_config()
        client = IggyClient(f"{host}:{port}")

        with pytest.raises(IggyError) as exc_info:
            await client.create_stream(unique_name())

        assert exc_info.value.name == "disconnected"
        assert exc_info.value.code == 8
        assert exc_info.value.message == "Disconnected"

    @pytest.mark.asyncio
    async def test_create_stream_before_login_fails(self, unique_name):
        """Test create_stream requires authentication."""
        host, port = get_server_config()
        wait_for_server(host, port)

        client = IggyClient(f"{host}:{port}")
        await client.connect()

        with pytest.raises(IggyError) as exc_info:
            await client.create_stream(unique_name())

        assert exc_info.value.name == "unauthenticated"
        assert exc_info.value.code == 40
        assert exc_info.value.message == "Unauthenticated"
