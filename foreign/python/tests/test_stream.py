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

from apache_iggy import IggyClient

from .utils import get_server_config, wait_for_ping, wait_for_server


class TestStreamOperations:
    """Test stream creation, retrieval, and management."""

    @pytest.mark.asyncio
    @pytest.mark.parametrize(
        "stream_name",
        [
            "a",
            "stream-name",
            "stream_name.with.mixed-CHARS123",
            " leading-space",
            "trailing-space ",
            "multiple  spaces  inside",
            "name/with/slash",
            "name:with:colons",
            "name.with.dots",
            "   ",
            "a" * 255,
            ("é" * 126) + "abc",
            ("한" * 84) + "abc",
            ("漢" * 84) + "abc",
            ("あ" * 84) + "abc",
            ("😀" * 62) + "abcdefg",
        ],
    )
    async def test_create_and_get_stream(
        self, iggy_client: IggyClient, stream_name: str
    ):
        """Test stream creation and retrieval."""
        await iggy_client.create_stream(stream_name)

        stream = await iggy_client.get_stream(stream_name)
        assert stream is not None
        assert stream.name == stream_name
        assert stream.id >= 0
        assert stream.topics_count == 0

    @pytest.mark.asyncio
    @pytest.mark.parametrize(
        "stream_name",
        [
            "",
            "a" * 256,
            "é" * 128,
            ("é" * 127) + "ab",
            ("한" * 85) + "a",
            ("漢" * 85) + "a",
            ("あ" * 85) + "a",
            "😀" * 64,
            ("😀" * 63) + "abcd",
        ],
    )
    async def test_create_stream_invalid_names(
        self, iggy_client: IggyClient, stream_name: str
    ):
        """Test create_stream enforces byte-length validation."""
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

        assert "StreamNameAlreadyExists" in str(exc_info.value)

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
