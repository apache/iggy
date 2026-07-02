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

from datetime import timedelta

import pytest

from apache_iggy import IggyClient, SendMessage

from .utils import get_server_config, wait_for_ping, wait_for_server


class TestCreateTopic:
    """Test topic creation via create_topic."""

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
    async def test_create_and_get_topic(
        self,
        iggy_client: IggyClient,
        unique_name,
        prefix: str,
        min_bytes: int,
        max_bytes: int,
    ):
        """Test topic creation and retrieval."""
        stream_name = unique_name()
        topic_name = unique_name(prefix, min_bytes=min_bytes, max_bytes=max_bytes)

        await iggy_client.create_stream(stream_name)
        await iggy_client.create_topic(
            stream=stream_name, name=topic_name, partitions_count=2
        )

        topic = await iggy_client.get_topic(stream_name, topic_name)
        assert topic is not None
        assert topic.name == topic_name
        assert topic.partitions_count == 2

        stream = await iggy_client.get_stream(stream_name)
        assert stream is not None
        assert stream.topics_count > 0

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
    async def test_create_topic_invalid_names(
        self,
        iggy_client: IggyClient,
        unique_name,
        prefix: str,
        min_bytes: int,
        max_bytes: int,
    ):
        """Test create_topic enforces byte-length validation."""
        stream_name = unique_name()
        topic_name = unique_name(prefix, min_bytes=min_bytes, max_bytes=max_bytes)

        await iggy_client.create_stream(stream_name)

        with pytest.raises(RuntimeError):
            await iggy_client.create_topic(
                stream=stream_name, name=topic_name, partitions_count=1
            )

    @pytest.mark.asyncio
    async def test_create_and_get_topic_with_numeric_stream_id(
        self, iggy_client: IggyClient, unique_name
    ):
        """Test topic APIs accept a numeric stream id for create and get operations."""
        stream_name = unique_name()
        topic_name = unique_name()

        await iggy_client.create_stream(stream_name)
        stream = await iggy_client.get_stream(stream_name)
        assert stream is not None

        await iggy_client.create_topic(
            stream=stream.id, name=topic_name, partitions_count=2
        )

        topic_by_name = await iggy_client.get_topic(stream.id, topic_name)
        assert topic_by_name is not None
        assert topic_by_name.name == topic_name
        assert topic_by_name.partitions_count == 2

        topic_by_id = await iggy_client.get_topic(stream.id, topic_by_name.id)
        assert topic_by_id is not None
        assert topic_by_id.id == topic_by_name.id
        assert topic_by_id.name == topic_by_name.name

    @pytest.mark.asyncio
    async def test_duplicate_topic_creation(self, iggy_client: IggyClient, unique_name):
        """Test that creating duplicate topics raises appropriate errors."""
        stream_name = unique_name()
        topic_name = unique_name()

        await iggy_client.create_stream(stream_name)
        await iggy_client.create_topic(
            stream=stream_name, name=topic_name, partitions_count=1
        )

        with pytest.raises(RuntimeError) as exc_info:
            await iggy_client.create_topic(
                stream=stream_name, name=topic_name, partitions_count=1
            )

        assert "already exists" in str(exc_info.value)

    @pytest.mark.asyncio
    async def test_topic_names_can_repeat_across_different_streams(
        self, iggy_client: IggyClient, unique_name
    ):
        """Test the same topic name can be created in different streams."""
        first_stream_name = unique_name()
        second_stream_name = unique_name()
        topic_name = unique_name()

        await iggy_client.create_stream(first_stream_name)
        await iggy_client.create_stream(second_stream_name)

        await iggy_client.create_topic(
            stream=first_stream_name, name=topic_name, partitions_count=1
        )
        await iggy_client.create_topic(
            stream=second_stream_name, name=topic_name, partitions_count=1
        )

        first_topic = await iggy_client.get_topic(first_stream_name, topic_name)
        second_topic = await iggy_client.get_topic(second_stream_name, topic_name)

        assert first_topic is not None
        assert second_topic is not None
        assert first_topic.name == topic_name
        assert second_topic.name == topic_name
        assert first_topic.partitions_count == 1
        assert second_topic.partitions_count == 1

    @pytest.mark.asyncio
    @pytest.mark.parametrize("compression_algorithm", ["gzip", "Gzip", "none", "None"])
    async def test_create_topic_with_valid_compression_algorithm(
        self, iggy_client: IggyClient, unique_name, compression_algorithm: str
    ):
        """Test create_topic accepts a supported compression algorithm value."""
        stream_name = unique_name()
        topic_name = unique_name()

        await iggy_client.create_stream(stream_name)
        await iggy_client.create_topic(
            stream=stream_name,
            name=topic_name,
            partitions_count=1,
            compression_algorithm=compression_algorithm,
        )

        topic = await iggy_client.get_topic(stream_name, topic_name)
        assert topic is not None
        assert topic.name == topic_name
        assert topic.partitions_count == 1

    @pytest.mark.asyncio
    @pytest.mark.parametrize(
        "compression_algorithm", ["brotli", "deflate", "gzipp", "", " gzip "]
    )
    async def test_create_topic_invalid_compression_algorithm(
        self, iggy_client: IggyClient, unique_name, compression_algorithm: str
    ):
        """Test create_topic rejects unsupported compression algorithm values."""
        stream_name = unique_name()
        topic_name = unique_name()

        await iggy_client.create_stream(stream_name)

        with pytest.raises(RuntimeError, match="Unknown compression type"):
            await iggy_client.create_topic(
                stream=stream_name,
                name=topic_name,
                partitions_count=1,
                compression_algorithm=compression_algorithm,
            )

    @pytest.mark.asyncio
    @pytest.mark.parametrize(
        "message_expiry",
        [
            timedelta(0),  # value for server default message expiry
            timedelta(microseconds=1),
            timedelta(seconds=1),
            timedelta(minutes=10),
            timedelta(days=1, seconds=2, microseconds=3),
        ],
    )
    async def test_create_topic_with_message_expiry(
        self, iggy_client: IggyClient, unique_name, message_expiry: timedelta
    ):
        """Test create_topic accepts an explicit message expiry."""
        stream_name = unique_name()
        topic_name = unique_name()

        await iggy_client.create_stream(stream_name)
        await iggy_client.create_topic(
            stream=stream_name,
            name=topic_name,
            partitions_count=1,
            message_expiry=message_expiry,
        )

        topic = await iggy_client.get_topic(stream_name, topic_name)
        assert topic is not None
        assert topic.name == topic_name

    @pytest.mark.asyncio
    @pytest.mark.parametrize("invalid_message_expiry", [1, "1s", object()])
    async def test_create_topic_invalid_message_expiry(
        self, iggy_client: IggyClient, unique_name, invalid_message_expiry
    ):
        """Test create_topic rejects message_expiry values that are not timedeltas."""
        stream_name = unique_name()
        topic_name = unique_name()

        await iggy_client.create_stream(stream_name)

        with pytest.raises(TypeError):
            await iggy_client.create_topic(
                stream=stream_name,
                name=topic_name,
                partitions_count=1,
                message_expiry=invalid_message_expiry,
            )

    @pytest.mark.asyncio
    @pytest.mark.parametrize(
        "max_topic_size",
        [
            0,  # value for server default max topic size
            2**64 - 1,
            2_000_000_000,
        ],
    )
    async def test_create_topic_with_valid_max_topic_size(
        self, iggy_client: IggyClient, unique_name, max_topic_size: int
    ):
        """Test create_topic accepts supported maximum topic size values."""
        stream_name = unique_name()
        topic_name = unique_name()

        await iggy_client.create_stream(stream_name)
        await iggy_client.create_topic(
            stream=stream_name,
            name=topic_name,
            partitions_count=1,
            max_topic_size=max_topic_size,
        )

        topic = await iggy_client.get_topic(stream_name, topic_name)
        assert topic is not None
        assert topic.name == topic_name

    @pytest.mark.asyncio
    @pytest.mark.parametrize(
        ("max_topic_size", "expected_exception"),
        [
            (4563, RuntimeError),
            (-1, OverflowError),
            (2e64, TypeError),
        ],
    )
    async def test_create_topic_invalid_max_topic_size(
        self,
        iggy_client: IggyClient,
        unique_name,
        max_topic_size,
        expected_exception,
    ):
        """Test create_topic rejects invalid maximum topic size values."""
        stream_name = unique_name()
        topic_name = unique_name()

        await iggy_client.create_stream(stream_name)

        with pytest.raises(expected_exception):
            await iggy_client.create_topic(
                stream=stream_name,
                name=topic_name,
                partitions_count=1,
                max_topic_size=max_topic_size,
            )

    @pytest.mark.asyncio
    @pytest.mark.parametrize(
        "replication_factor",
        [
            0,  # value for server default replication factor
            1,
            42,
            255,
        ],
    )
    async def test_create_topic_with_valid_replication_factor(
        self, iggy_client: IggyClient, unique_name, replication_factor: int
    ):
        """Test create_topic accepts a supported replication factor."""
        stream_name = unique_name()
        topic_name = unique_name()

        await iggy_client.create_stream(stream_name)
        await iggy_client.create_topic(
            stream=stream_name,
            name=topic_name,
            partitions_count=1,
            replication_factor=replication_factor,
        )

        topic = await iggy_client.get_topic(stream_name, topic_name)
        assert topic is not None
        assert topic.name == topic_name

    @pytest.mark.asyncio
    @pytest.mark.parametrize(
        ("replication_factor", "expected_exception"),
        [
            (-1, OverflowError),
            (256, OverflowError),
            ("1", TypeError),
            (1.0, TypeError),
        ],
    )
    async def test_create_topic_invalid_replication_factor(
        self,
        iggy_client: IggyClient,
        unique_name,
        replication_factor,
        expected_exception,
    ):
        """Test create_topic rejects invalid replication factor values."""
        stream_name = unique_name()
        topic_name = unique_name()

        await iggy_client.create_stream(stream_name)

        with pytest.raises(expected_exception):
            await iggy_client.create_topic(
                stream=stream_name,
                name=topic_name,
                partitions_count=1,
                replication_factor=replication_factor,
            )

    @pytest.mark.asyncio
    @pytest.mark.parametrize("partitions_count", [1001, 10000])
    async def test_create_topic_invalid_partitions_count(
        self, iggy_client: IggyClient, unique_name, partitions_count: int
    ):
        """Test create_topic rejects partition counts above the supported limit."""
        stream_name = unique_name()
        topic_name = unique_name()

        await iggy_client.create_stream(stream_name)

        with pytest.raises(RuntimeError) as exc_info:
            await iggy_client.create_topic(
                stream=stream_name, name=topic_name, partitions_count=partitions_count
            )

        assert "Too many partitions" in str(exc_info.value)

    @pytest.mark.asyncio
    @pytest.mark.parametrize("partitions_count", [0, 1, 1000])
    async def test_create_topic_with_valid_partitions_count(
        self, iggy_client: IggyClient, unique_name, partitions_count: int
    ):
        """Test create_topic accepts the maximum supported partitions count."""
        stream_name = unique_name()
        topic_name = unique_name()

        await iggy_client.create_stream(stream_name)
        await iggy_client.create_topic(
            stream=stream_name, name=topic_name, partitions_count=partitions_count
        )

        topic = await iggy_client.get_topic(stream_name, topic_name)
        assert topic is not None
        assert topic.name == topic_name
        assert topic.partitions_count == partitions_count

    @pytest.mark.asyncio
    async def test_stream_topics_count_increases_after_multiple_topic_creations(
        self, iggy_client: IggyClient, unique_name
    ):
        """Test a stream reports additional topics after multiple creations."""
        stream_name = unique_name()
        first_topic_name = unique_name()
        second_topic_name = unique_name()

        await iggy_client.create_stream(stream_name)

        stream_before = await iggy_client.get_stream(stream_name)
        assert stream_before is not None
        assert stream_before.topics_count == 0

        await iggy_client.create_topic(
            stream=stream_name, name=first_topic_name, partitions_count=1
        )
        await iggy_client.create_topic(
            stream=stream_name, name=second_topic_name, partitions_count=1
        )

        stream_after = await iggy_client.get_stream(stream_name)
        assert stream_after is not None
        assert stream_after.topics_count == 2

    @pytest.mark.asyncio
    async def test_create_topic_then_reconnect_then_get_topic(
        self, iggy_client: IggyClient, unique_name
    ):
        """Test a topic remains retrievable after reconnecting with a fresh client."""
        stream_name = unique_name()
        topic_name = unique_name()

        await iggy_client.create_stream(stream_name)
        await iggy_client.create_topic(
            stream=stream_name, name=topic_name, partitions_count=1
        )

        host, port = get_server_config()
        wait_for_server(host, port)

        client = IggyClient(f"{host}:{port}")
        await client.connect()
        await wait_for_ping(client)
        await client.login_user("iggy", "iggy")

        topic = await client.get_topic(stream_name, topic_name)
        assert topic is not None
        assert topic.name == topic_name
        assert topic.partitions_count == 1

    @pytest.mark.asyncio
    async def test_create_topic_in_nonexistent_stream(
        self, iggy_client: IggyClient, unique_name
    ):
        """Test creating a topic in a non-existent stream."""
        nonexistent_stream = unique_name()
        topic_name = unique_name()

        with pytest.raises(RuntimeError):
            await iggy_client.create_topic(
                stream=nonexistent_stream, name=topic_name, partitions_count=1
            )

    @pytest.mark.asyncio
    async def test_create_topic_requires_connection_and_auth(self, unique_name):
        """Test create_topic fails both before connecting and before logging in."""
        host, port = get_server_config()
        wait_for_server(host, port)

        client = IggyClient(f"{host}:{port}")
        with pytest.raises(RuntimeError):
            await client.create_topic(
                stream=unique_name(), name=unique_name(), partitions_count=1
            )

        await client.connect()
        with pytest.raises(RuntimeError):
            await client.create_topic(
                stream=unique_name(), name=unique_name(), partitions_count=1
            )


class TestGetTopic:
    """Test topic retrieval via get_topic."""

    @pytest.mark.asyncio
    async def test_get_topic_by_name_and_id(self, iggy_client: IggyClient, unique_name):
        """Test repeated topic lookup works by both name and numeric id."""
        stream_name = unique_name()
        topic_name = unique_name()

        await iggy_client.create_stream(stream_name)
        await iggy_client.create_topic(
            stream=stream_name, name=topic_name, partitions_count=1
        )

        topic_by_name = await iggy_client.get_topic(stream_name, topic_name)
        assert topic_by_name is not None

        topic_by_name_again = await iggy_client.get_topic(stream_name, topic_name)
        assert topic_by_name_again is not None
        assert topic_by_name_again.id == topic_by_name.id
        assert topic_by_name_again.name == topic_by_name.name

        topic_by_id = await iggy_client.get_topic(stream_name, topic_by_name.id)
        assert topic_by_id is not None
        assert topic_by_id.id == topic_by_name.id
        assert topic_by_id.name == topic_by_name.name

    @pytest.mark.asyncio
    async def test_get_nonexistent_topic(self, iggy_client: IggyClient, unique_name):
        """Test getting a non-existent topic by name or numeric id."""
        stream_name = unique_name()
        nonexistent_topic_name = unique_name()

        await iggy_client.create_stream(stream_name)

        topic_by_name = await iggy_client.get_topic(stream_name, nonexistent_topic_name)
        assert topic_by_name is None

        topic_by_id = await iggy_client.get_topic(stream_name, 999999)
        assert topic_by_id is None

    @pytest.mark.asyncio
    async def test_get_topic_in_nonexistent_stream(
        self, iggy_client: IggyClient, unique_name
    ):
        """Test getting a topic from a non-existent stream returns no topic."""
        nonexistent_stream = unique_name()
        topic_name = unique_name()

        topic = await iggy_client.get_topic(nonexistent_stream, topic_name)
        assert topic is None

    @pytest.mark.asyncio
    async def test_get_topic_requires_connection_and_auth(self, unique_name):
        """Test get_topic fails both before connecting and before logging in."""
        host, port = get_server_config()
        wait_for_server(host, port)

        client = IggyClient(f"{host}:{port}")
        with pytest.raises(RuntimeError):
            await client.get_topic(unique_name(), unique_name())

        await client.connect()
        with pytest.raises(RuntimeError):
            await client.get_topic(unique_name(), unique_name())


class TestGetTopics:
    """Test listing topics in a stream via get_topics."""

    @pytest.mark.asyncio
    async def test_get_topics_in_empty_stream_returns_empty_list(
        self, iggy_client: IggyClient, unique_name
    ):
        """Test get_topics returns an empty list for a stream with no topics."""
        stream_name = unique_name()

        await iggy_client.create_stream(stream_name)

        topics = await iggy_client.get_topics(stream_name)
        assert topics == []

    @pytest.mark.asyncio
    @pytest.mark.parametrize("topic_count", [1, 3, 5])
    async def test_get_topics_returns_all_created_topics(
        self, iggy_client: IggyClient, unique_name, topic_count: int
    ):
        """Test get_topics returns every created topic, ordered by id ascending."""
        stream_name = unique_name()
        # Reverse-alphabetical names so that id-ascending (creation) order
        # and name order disagree, proving the list isn't accidentally
        # name-sorted.
        topic_names = [f"{unique_name()}-{i}" for i in range(topic_count, 0, -1)]

        await iggy_client.create_stream(stream_name)
        for name in topic_names:
            await iggy_client.create_topic(
                stream=stream_name, name=name, partitions_count=1
            )

        topics = await iggy_client.get_topics(stream_name)
        assert len(topics) == len(topic_names)
        assert [topic.name for topic in topics] == topic_names
        assert [topic.id for topic in topics] == sorted(topic.id for topic in topics)
        # Only assert fields supplied at creation or deterministically set by
        # the server: each topic keeps its partition count and a fresh topic
        # holds no messages. The numeric id is server-assigned and not checked.
        assert all(topic.partitions_count == 1 for topic in topics)
        assert all(topic.messages_count == 0 for topic in topics)

    @pytest.mark.asyncio
    async def test_get_topics_returns_same_result_when_called_repeatedly(
        self, iggy_client: IggyClient, unique_name
    ):
        """Test repeated get_topics calls return an identically ordered view."""
        stream_name = unique_name()
        topic_names = [unique_name() for _ in range(3)]

        await iggy_client.create_stream(stream_name)
        for name in topic_names:
            await iggy_client.create_topic(
                stream=stream_name, name=name, partitions_count=1
            )

        first = await iggy_client.get_topics(stream_name)
        second = await iggy_client.get_topics(stream_name)
        assert [topic.name for topic in first] == topic_names
        assert [topic.id for topic in first] == [topic.id for topic in second]
        assert [topic.name for topic in first] == [topic.name for topic in second]

    @pytest.mark.asyncio
    async def test_get_topics_accepts_numeric_stream_id(
        self, iggy_client: IggyClient, unique_name
    ):
        """Test get_topics works when the stream is referenced by numeric id."""
        stream_name = unique_name()
        topic_name = unique_name()

        await iggy_client.create_stream(stream_name)
        await iggy_client.create_topic(
            stream=stream_name, name=topic_name, partitions_count=1
        )
        stream = await iggy_client.get_stream(stream_name)
        assert stream is not None

        topics_by_name = await iggy_client.get_topics(stream_name)
        topics_by_id = await iggy_client.get_topics(stream.id)
        assert len(topics_by_name) == 1
        assert len(topics_by_id) == 1
        assert topics_by_id[0].name == topic_name

    @pytest.mark.asyncio
    async def test_get_topics_in_nonexistent_stream_returns_empty(
        self, iggy_client: IggyClient, unique_name
    ):
        """Test get_topics returns an empty list for a non-existent stream."""
        topics = await iggy_client.get_topics(unique_name())
        assert topics == []

    @pytest.mark.asyncio
    async def test_get_topics_requires_connection_and_auth(self, unique_name):
        """Test get_topics fails both before connecting and before logging in."""
        host, port = get_server_config()
        wait_for_server(host, port)

        client = IggyClient(f"{host}:{port}")
        with pytest.raises(RuntimeError):
            await client.get_topics(unique_name())

        await client.connect()
        with pytest.raises(RuntimeError):
            await client.get_topics(unique_name())


class TestUpdateTopic:
    """Test updating topics via update_topic."""

    @pytest.mark.asyncio
    async def test_update_topic_renames_topic(
        self, iggy_client: IggyClient, unique_name
    ):
        """Test update_topic renames a topic; old name no longer resolves."""
        stream_name = unique_name()
        topic_name = unique_name()
        new_name = unique_name()

        await iggy_client.create_stream(stream_name)
        await iggy_client.create_topic(
            stream=stream_name, name=topic_name, partitions_count=1
        )

        await iggy_client.update_topic(
            stream_id=stream_name, topic_id=topic_name, name=new_name
        )

        renamed = await iggy_client.get_topic(stream_name, new_name)
        assert renamed is not None
        assert renamed.name == new_name

        old = await iggy_client.get_topic(stream_name, topic_name)
        assert old is None

    @pytest.mark.asyncio
    async def test_update_topic_preserves_id(
        self, iggy_client: IggyClient, unique_name
    ):
        """Test update_topic keeps the same numeric id after a rename."""
        stream_name = unique_name()
        topic_name = unique_name()
        new_name = unique_name()

        await iggy_client.create_stream(stream_name)
        await iggy_client.create_topic(
            stream=stream_name, name=topic_name, partitions_count=1
        )
        before = await iggy_client.get_topic(stream_name, topic_name)
        assert before is not None

        await iggy_client.update_topic(
            stream_id=stream_name, topic_id=topic_name, name=new_name
        )

        after = await iggy_client.get_topic(stream_name, new_name)
        assert after is not None
        assert after.id == before.id

    @pytest.mark.asyncio
    async def test_update_topic_by_numeric_id(
        self, iggy_client: IggyClient, unique_name
    ):
        """Test update_topic accepts a numeric topic id."""
        stream_name = unique_name()
        topic_name = unique_name()
        new_name = unique_name()

        await iggy_client.create_stream(stream_name)
        await iggy_client.create_topic(
            stream=stream_name, name=topic_name, partitions_count=1
        )
        topic = await iggy_client.get_topic(stream_name, topic_name)
        assert topic is not None

        await iggy_client.update_topic(
            stream_id=stream_name, topic_id=topic.id, name=new_name
        )

        renamed = await iggy_client.get_topic(stream_name, new_name)
        assert renamed is not None
        assert renamed.name == new_name

    @pytest.mark.asyncio
    @pytest.mark.parametrize("compression_algorithm", ["gzip", "none"])
    async def test_update_topic_with_valid_compression_algorithm(
        self, iggy_client: IggyClient, unique_name, compression_algorithm: str
    ):
        """Test update_topic accepts a supported compression algorithm."""
        stream_name = unique_name()
        topic_name = unique_name()

        await iggy_client.create_stream(stream_name)
        await iggy_client.create_topic(
            stream=stream_name, name=topic_name, partitions_count=1
        )

        await iggy_client.update_topic(
            stream_id=stream_name,
            topic_id=topic_name,
            name=topic_name,
            compression_algorithm=compression_algorithm,
        )

        topic = await iggy_client.get_topic(stream_name, topic_name)
        assert topic is not None
        assert topic.name == topic_name
        assert topic.compression_algorithm == compression_algorithm

    @pytest.mark.asyncio
    @pytest.mark.parametrize("compression_algorithm", ["brotli", "gzipp", ""])
    async def test_update_topic_invalid_compression_algorithm(
        self, iggy_client: IggyClient, unique_name, compression_algorithm: str
    ):
        """Test update_topic rejects unsupported compression algorithm values."""
        stream_name = unique_name()
        topic_name = unique_name()

        await iggy_client.create_stream(stream_name)
        await iggy_client.create_topic(
            stream=stream_name, name=topic_name, partitions_count=1
        )

        with pytest.raises(RuntimeError, match="Unknown compression type"):
            await iggy_client.update_topic(
                stream_id=stream_name,
                topic_id=topic_name,
                name=topic_name,
                compression_algorithm=compression_algorithm,
            )

    @pytest.mark.asyncio
    @pytest.mark.parametrize("invalid_message_expiry", [1, "1s", object()])
    async def test_update_topic_invalid_message_expiry(
        self, iggy_client: IggyClient, unique_name, invalid_message_expiry
    ):
        """Test update_topic rejects message_expiry values that are not timedeltas."""
        stream_name = unique_name()
        topic_name = unique_name()

        await iggy_client.create_stream(stream_name)
        await iggy_client.create_topic(
            stream=stream_name, name=topic_name, partitions_count=1
        )

        with pytest.raises(TypeError):
            await iggy_client.update_topic(
                stream_id=stream_name,
                topic_id=topic_name,
                name=topic_name,
                message_expiry=invalid_message_expiry,
            )

    @pytest.mark.asyncio
    @pytest.mark.parametrize("replication_factor", [0, 1, 255])
    async def test_update_topic_with_valid_replication_factor(
        self, iggy_client: IggyClient, unique_name, replication_factor: int
    ):
        """Test update_topic accepts a supported replication factor."""
        stream_name = unique_name()
        topic_name = unique_name()

        await iggy_client.create_stream(stream_name)
        await iggy_client.create_topic(
            stream=stream_name, name=topic_name, partitions_count=1
        )

        await iggy_client.update_topic(
            stream_id=stream_name,
            topic_id=topic_name,
            name=topic_name,
            replication_factor=replication_factor,
        )

        topic = await iggy_client.get_topic(stream_name, topic_name)
        assert topic is not None
        # The server normalizes a replication factor of 0 to 1.
        assert topic.replication_factor == (replication_factor or 1)

    @pytest.mark.asyncio
    @pytest.mark.parametrize(
        ("replication_factor", "expected_exception"),
        [
            (-1, OverflowError),
            (256, OverflowError),
            ("1", TypeError),
            (1.0, TypeError),
        ],
    )
    async def test_update_topic_invalid_replication_factor(
        self,
        iggy_client: IggyClient,
        unique_name,
        replication_factor,
        expected_exception,
    ):
        """Test update_topic rejects invalid replication factor values."""
        stream_name = unique_name()
        topic_name = unique_name()

        await iggy_client.create_stream(stream_name)
        await iggy_client.create_topic(
            stream=stream_name, name=topic_name, partitions_count=1
        )

        with pytest.raises(expected_exception):
            await iggy_client.update_topic(
                stream_id=stream_name,
                topic_id=topic_name,
                name=topic_name,
                replication_factor=replication_factor,
            )

    @pytest.mark.asyncio
    @pytest.mark.parametrize(
        ("max_topic_size", "expected_exception"),
        [
            (-1, OverflowError),
            (2e64, TypeError),
        ],
    )
    async def test_update_topic_invalid_max_topic_size(
        self,
        iggy_client: IggyClient,
        unique_name,
        max_topic_size,
        expected_exception,
    ):
        """Test update_topic rejects invalid maximum topic size values."""
        stream_name = unique_name()
        topic_name = unique_name()

        await iggy_client.create_stream(stream_name)
        await iggy_client.create_topic(
            stream=stream_name, name=topic_name, partitions_count=1
        )

        with pytest.raises(expected_exception):
            await iggy_client.update_topic(
                stream_id=stream_name,
                topic_id=topic_name,
                name=topic_name,
                max_topic_size=max_topic_size,
            )

    @pytest.mark.asyncio
    async def test_update_topic_with_message_expiry(
        self, iggy_client: IggyClient, unique_name
    ):
        """Test update_topic accepts an explicit message expiry."""
        stream_name = unique_name()
        topic_name = unique_name()

        await iggy_client.create_stream(stream_name)
        await iggy_client.create_topic(
            stream=stream_name, name=topic_name, partitions_count=1
        )

        await iggy_client.update_topic(
            stream_id=stream_name,
            topic_id=topic_name,
            name=topic_name,
            message_expiry=timedelta(minutes=10),
        )

        topic = await iggy_client.get_topic(stream_name, topic_name)
        assert topic is not None
        assert topic.name == topic_name

    @pytest.mark.asyncio
    @pytest.mark.parametrize("max_topic_size", [0, 2_000_000_000, 2**64 - 1])
    async def test_update_topic_with_valid_max_topic_size(
        self, iggy_client: IggyClient, unique_name, max_topic_size: int
    ):
        """Test update_topic accepts supported maximum topic size values."""
        stream_name = unique_name()
        topic_name = unique_name()

        await iggy_client.create_stream(stream_name)
        await iggy_client.create_topic(
            stream=stream_name, name=topic_name, partitions_count=1
        )

        await iggy_client.update_topic(
            stream_id=stream_name,
            topic_id=topic_name,
            name=topic_name,
            max_topic_size=max_topic_size,
        )

        topic = await iggy_client.get_topic(stream_name, topic_name)
        assert topic is not None
        assert topic.name == topic_name

    @pytest.mark.asyncio
    async def test_update_topic_applies_repeated_updates(
        self, iggy_client: IggyClient, unique_name
    ):
        """Test successive update_topic calls each take effect."""
        stream_name = unique_name()
        topic_name = unique_name()
        first_rename = unique_name()
        second_rename = unique_name()

        await iggy_client.create_stream(stream_name)
        await iggy_client.create_topic(
            stream=stream_name, name=topic_name, partitions_count=1
        )

        await iggy_client.update_topic(
            stream_id=stream_name, topic_id=topic_name, name=first_rename
        )
        after_first = await iggy_client.get_topic(stream_name, first_rename)
        assert after_first is not None
        assert after_first.name == first_rename
        assert await iggy_client.get_topic(stream_name, topic_name) is None

        await iggy_client.update_topic(
            stream_id=stream_name, topic_id=first_rename, name=second_rename
        )
        after_second = await iggy_client.get_topic(stream_name, second_rename)
        assert after_second is not None
        assert after_second.name == second_rename
        assert await iggy_client.get_topic(stream_name, first_rename) is None

    @pytest.mark.asyncio
    async def test_update_nonexistent_topic_fails(
        self, iggy_client: IggyClient, unique_name
    ):
        """Test update_topic raises for a non-existent topic."""
        stream_name = unique_name()

        await iggy_client.create_stream(stream_name)

        with pytest.raises(RuntimeError):
            await iggy_client.update_topic(
                stream_id=stream_name, topic_id=unique_name(), name=unique_name()
            )

    @pytest.mark.asyncio
    async def test_update_topic_in_nonexistent_stream_fails(
        self, iggy_client: IggyClient, unique_name
    ):
        """Test update_topic raises when the stream does not exist."""
        with pytest.raises(RuntimeError):
            await iggy_client.update_topic(
                stream_id=unique_name(), topic_id=unique_name(), name=unique_name()
            )

    @pytest.mark.asyncio
    async def test_update_topic_to_existing_name_fails(
        self, iggy_client: IggyClient, unique_name
    ):
        """Test update_topic rejects renaming a topic to a name already in use."""
        stream_name = unique_name()
        first_topic = unique_name()
        second_topic = unique_name()

        await iggy_client.create_stream(stream_name)
        await iggy_client.create_topic(
            stream=stream_name, name=first_topic, partitions_count=1
        )
        await iggy_client.create_topic(
            stream=stream_name, name=second_topic, partitions_count=1
        )

        with pytest.raises(RuntimeError):
            await iggy_client.update_topic(
                stream_id=stream_name, topic_id=second_topic, name=first_topic
            )

    @pytest.mark.asyncio
    async def test_update_topic_requires_connection_and_auth(self, unique_name):
        """Test update_topic fails both before connecting and before logging in."""
        host, port = get_server_config()
        wait_for_server(host, port)

        client = IggyClient(f"{host}:{port}")
        with pytest.raises(RuntimeError):
            await client.update_topic(
                stream_id=unique_name(), topic_id=unique_name(), name=unique_name()
            )

        await client.connect()
        with pytest.raises(RuntimeError):
            await client.update_topic(
                stream_id=unique_name(), topic_id=unique_name(), name=unique_name()
            )


class TestDeleteTopic:
    """Test deleting topics via delete_topic."""

    @pytest.mark.asyncio
    async def test_delete_topic_removes_topic(
        self, iggy_client: IggyClient, unique_name
    ):
        """Test delete_topic removes the topic and drops the stream count."""
        stream_name = unique_name()
        topic_name = unique_name()

        await iggy_client.create_stream(stream_name)
        await iggy_client.create_topic(
            stream=stream_name, name=topic_name, partitions_count=1
        )

        await iggy_client.delete_topic(stream_name, topic_name)

        assert await iggy_client.get_topic(stream_name, topic_name) is None
        assert await iggy_client.get_topics(stream_name) == []

        stream = await iggy_client.get_stream(stream_name)
        assert stream is not None
        assert stream.topics_count == 0

    @pytest.mark.asyncio
    async def test_delete_topic_by_numeric_id(
        self, iggy_client: IggyClient, unique_name
    ):
        """Test delete_topic accepts a numeric topic id."""
        stream_name = unique_name()
        topic_name = unique_name()

        await iggy_client.create_stream(stream_name)
        await iggy_client.create_topic(
            stream=stream_name, name=topic_name, partitions_count=1
        )
        topic = await iggy_client.get_topic(stream_name, topic_name)
        assert topic is not None

        await iggy_client.delete_topic(stream_name, topic.id)

        assert await iggy_client.get_topic(stream_name, topic_name) is None

    @pytest.mark.asyncio
    async def test_delete_topic_leaves_other_topics(
        self, iggy_client: IggyClient, unique_name
    ):
        """Test delete_topic removes only the targeted topic."""
        stream_name = unique_name()
        topic_to_delete = unique_name()
        topic_to_keep = unique_name()

        await iggy_client.create_stream(stream_name)
        await iggy_client.create_topic(
            stream=stream_name, name=topic_to_delete, partitions_count=1
        )
        await iggy_client.create_topic(
            stream=stream_name, name=topic_to_keep, partitions_count=1
        )

        await iggy_client.delete_topic(stream_name, topic_to_delete)

        remaining = await iggy_client.get_topics(stream_name)
        assert len(remaining) == 1
        assert remaining[0].name == topic_to_keep

    @pytest.mark.asyncio
    async def test_delete_nonexistent_topic_fails(
        self, iggy_client: IggyClient, unique_name
    ):
        """Test delete_topic raises for a non-existent topic."""
        stream_name = unique_name()

        await iggy_client.create_stream(stream_name)

        with pytest.raises(RuntimeError):
            await iggy_client.delete_topic(stream_name, unique_name())

    @pytest.mark.asyncio
    async def test_delete_topic_twice_fails_second_time(
        self, iggy_client: IggyClient, unique_name
    ):
        """Test deleting an already-deleted topic raises on the second call."""
        stream_name = unique_name()
        topic_name = unique_name()

        await iggy_client.create_stream(stream_name)
        await iggy_client.create_topic(
            stream=stream_name, name=topic_name, partitions_count=1
        )

        await iggy_client.delete_topic(stream_name, topic_name)
        with pytest.raises(RuntimeError):
            await iggy_client.delete_topic(stream_name, topic_name)

    @pytest.mark.asyncio
    async def test_delete_topic_requires_connection_and_auth(self, unique_name):
        """Test delete_topic fails both before connecting and before logging in."""
        host, port = get_server_config()
        wait_for_server(host, port)

        client = IggyClient(f"{host}:{port}")
        with pytest.raises(RuntimeError):
            await client.delete_topic(unique_name(), unique_name())

        await client.connect()
        with pytest.raises(RuntimeError):
            await client.delete_topic(unique_name(), unique_name())


class TestPurgeTopic:
    """Test purging topic messages via purge_topic."""

    @pytest.mark.asyncio
    async def test_purge_topic_clears_messages_but_keeps_topic(
        self, iggy_client: IggyClient, unique_name
    ):
        """Test purge_topic empties the topic while leaving it in place."""
        stream_name = unique_name()
        topic_name = unique_name()

        await iggy_client.create_stream(stream_name)
        await iggy_client.create_topic(
            stream=stream_name, name=topic_name, partitions_count=1
        )

        messages = [SendMessage(f"payload-{index}") for index in range(5)]
        await iggy_client.send_messages(stream_name, topic_name, 0, messages)

        before = await iggy_client.get_topic(stream_name, topic_name)
        assert before is not None
        assert before.messages_count == 5

        await iggy_client.purge_topic(stream_name, topic_name)

        after = await iggy_client.get_topic(stream_name, topic_name)
        assert after is not None
        assert after.messages_count == 0
        # Purging clears messages only; every other field is left unchanged.
        assert after.id == before.id
        assert after.name == before.name
        assert after.partitions_count == before.partitions_count
        assert after.compression_algorithm == before.compression_algorithm
        assert after.replication_factor == before.replication_factor

    @pytest.mark.asyncio
    async def test_purge_empty_topic_succeeds(
        self, iggy_client: IggyClient, unique_name
    ):
        """Test purge_topic is a no-op on a topic with no messages."""
        stream_name = unique_name()
        topic_name = unique_name()

        await iggy_client.create_stream(stream_name)
        await iggy_client.create_topic(
            stream=stream_name, name=topic_name, partitions_count=1
        )

        await iggy_client.purge_topic(stream_name, topic_name)

        topic = await iggy_client.get_topic(stream_name, topic_name)
        assert topic is not None
        assert topic.messages_count == 0

    @pytest.mark.asyncio
    async def test_purge_topic_is_idempotent_when_called_repeatedly(
        self, iggy_client: IggyClient, unique_name
    ):
        """Test purge_topic succeeds when called repeatedly on the same topic."""
        stream_name = unique_name()
        topic_name = unique_name()

        await iggy_client.create_stream(stream_name)
        await iggy_client.create_topic(
            stream=stream_name, name=topic_name, partitions_count=1
        )

        messages = [SendMessage(f"payload-{index}") for index in range(5)]
        await iggy_client.send_messages(stream_name, topic_name, 0, messages)

        await iggy_client.purge_topic(stream_name, topic_name)
        await iggy_client.purge_topic(stream_name, topic_name)

        topic = await iggy_client.get_topic(stream_name, topic_name)
        assert topic is not None
        assert topic.messages_count == 0

    @pytest.mark.asyncio
    async def test_purge_nonexistent_topic_fails(
        self, iggy_client: IggyClient, unique_name
    ):
        """Test purge_topic raises for a non-existent topic in an existing stream."""
        stream_name = unique_name()

        await iggy_client.create_stream(stream_name)

        with pytest.raises(RuntimeError):
            await iggy_client.purge_topic(stream_name, unique_name())

    @pytest.mark.asyncio
    async def test_purge_topic_in_nonexistent_stream_fails(
        self, iggy_client: IggyClient, unique_name
    ):
        """Test purge_topic raises when the stream does not exist."""
        with pytest.raises(RuntimeError):
            await iggy_client.purge_topic(unique_name(), unique_name())

    @pytest.mark.asyncio
    async def test_purge_topic_requires_connection_and_auth(self, unique_name):
        """Test purge_topic fails both before connecting and before logging in."""
        host, port = get_server_config()
        wait_for_server(host, port)

        client = IggyClient(f"{host}:{port}")
        with pytest.raises(RuntimeError):
            await client.purge_topic(unique_name(), unique_name())

        await client.connect()
        with pytest.raises(RuntimeError):
            await client.purge_topic(unique_name(), unique_name())
