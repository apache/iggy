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

from typing import Any

import pytest

from apache_iggy import (
    GlobalPermissions,
    Permissions,
    StreamPermissions,
    TopicPermissions,
)

GLOBAL_PERMISSION_FLAGS = [
    "manage_servers",
    "read_servers",
    "manage_users",
    "read_users",
    "manage_streams",
    "read_streams",
    "manage_topics",
    "read_topics",
    "poll_messages",
    "send_messages",
]

STREAM_PERMISSION_FLAGS = [
    "manage_stream",
    "read_stream",
    "manage_topics",
    "read_topics",
    "poll_messages",
    "send_messages",
]

TOPIC_PERMISSION_FLAGS = [
    "manage_topic",
    "read_topic",
    "poll_messages",
    "send_messages",
]


class TestPermissionsModel:
    """Test constructing the Permissions classes without a server."""

    def test_default_permissions_deny_everything(self):
        """Test Permissions() has every global flag off and no streams."""
        permissions = Permissions()

        assert permissions.streams is None
        global_permissions = permissions.global_
        assert global_permissions.manage_servers is False
        assert global_permissions.read_servers is False
        assert global_permissions.manage_users is False
        assert global_permissions.read_users is False
        assert global_permissions.manage_streams is False
        assert global_permissions.read_streams is False
        assert global_permissions.manage_topics is False
        assert global_permissions.read_topics is False
        assert global_permissions.poll_messages is False
        assert global_permissions.send_messages is False

    @pytest.mark.parametrize("flag", GLOBAL_PERMISSION_FLAGS)
    def test_each_global_permission_flag_round_trips_alone(self, flag):
        """Test setting one flag flips only that flag."""
        global_permissions = GlobalPermissions(**{flag: True})

        for name in GLOBAL_PERMISSION_FLAGS:
            assert getattr(global_permissions, name) is (name == flag)

    def test_all_global_permission_flags_set_together(self):
        """Test all flags can be enabled at once."""
        global_permissions = GlobalPermissions(
            **dict.fromkeys(GLOBAL_PERMISSION_FLAGS, True)
        )

        for name in GLOBAL_PERMISSION_FLAGS:
            assert getattr(global_permissions, name) is True

    @pytest.mark.parametrize("flag", STREAM_PERMISSION_FLAGS)
    def test_each_stream_permission_flag_round_trips_alone(self, flag):
        """Test setting one flag flips only that flag."""
        flags: dict[str, Any] = {flag: True}
        stream_permissions = StreamPermissions(**flags)

        for name in STREAM_PERMISSION_FLAGS:
            assert getattr(stream_permissions, name) is (name == flag)

    def test_all_stream_permission_flags_set_together(self):
        """Test all flags can be enabled at once."""
        flags: dict[str, Any] = dict.fromkeys(STREAM_PERMISSION_FLAGS, True)
        stream_permissions = StreamPermissions(**flags)

        for name in STREAM_PERMISSION_FLAGS:
            assert getattr(stream_permissions, name) is True

    @pytest.mark.parametrize("flag", TOPIC_PERMISSION_FLAGS)
    def test_each_topic_permission_flag_round_trips_alone(self, flag):
        """Test setting one flag flips only that flag."""
        topic_permissions = TopicPermissions(**{flag: True})

        for name in TOPIC_PERMISSION_FLAGS:
            assert getattr(topic_permissions, name) is (name == flag)

    def test_all_topic_permission_flags_set_together(self):
        """Test all flags can be enabled at once."""
        topic_permissions = TopicPermissions(
            **dict.fromkeys(TOPIC_PERMISSION_FLAGS, True)
        )

        for name in TOPIC_PERMISSION_FLAGS:
            assert getattr(topic_permissions, name) is True

    def test_nested_stream_and_topic_permissions_are_preserved(self):
        """Test stream and topic permission dicts survive construction."""
        permissions = Permissions(
            global_=GlobalPermissions(read_servers=True),
            streams={
                1: StreamPermissions(
                    read_stream=True,
                    topics={7: TopicPermissions(poll_messages=True)},
                ),
                42: StreamPermissions(manage_stream=True),
            },
        )

        streams = permissions.streams
        assert streams is not None
        assert set(streams) == {1, 42}
        assert streams[1].read_stream is True
        assert streams[1].manage_stream is False
        topics = streams[1].topics
        assert topics is not None
        assert set(topics) == {7}
        assert topics[7].poll_messages is True
        assert topics[7].manage_topic is False
        assert streams[42].manage_stream is True
        assert streams[42].topics is None

    def test_stream_id_above_u32_is_rejected(self):
        """Test dict keys outside the u32 wire range fail at construction."""
        with pytest.raises(OverflowError):
            Permissions(streams={2**32: StreamPermissions(read_stream=True)})
        with pytest.raises(OverflowError):
            StreamPermissions(topics={2**32: TopicPermissions(read_topic=True)})

    def test_permissions_equality(self):
        """Test structurally identical Permissions compare equal."""
        first = Permissions(
            global_=GlobalPermissions(read_streams=True),
            streams={3: StreamPermissions(poll_messages=True)},
        )
        second = Permissions(
            global_=GlobalPermissions(read_streams=True),
            streams={3: StreamPermissions(poll_messages=True)},
        )
        different = Permissions(global_=GlobalPermissions(manage_streams=True))

        assert first == second
        assert first != different
