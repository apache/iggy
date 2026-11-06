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

"""The canonical form of a metadata changes answer: the HTTP JSON that the fixture
core/common/fixtures/metadata_changes.json names "expected"."""

import base64
from typing import Any

from apache_iggy import (
    MetadataConsumerGroupCreated,
    MetadataConsumerGroupDeleted,
    MetadataConsumerGroupItem,
    MetadataMemberItem,
    MetadataMemberJoined,
    MetadataMemberLeft,
    MetadataPartitionHandedOver,
    MetadataPartitionsChanged,
    MetadataPartitionsItem,
    MetadataSession,
    MetadataSessionClosed,
    MetadataStreamChange,
    MetadataStreamDeleted,
    MetadataTokenChange,
    MetadataTokenDeleted,
    MetadataTopicChange,
    MetadataTopicDeleted,
    MetadataTopicItem,
    MetadataUnknownChange,
    MetadataUnknownItem,
    MetadataUserChange,
    MetadataUserIdentity,
    MetadataUserPermissionsUpdated,
)

from canonical import global_permissions, options, status_name

_STREAM_KEY = ("stream_id", "stream_created_op")
_TOPIC_KEY = (*_STREAM_KEY, "topic_id", "topic_created_op")
_USER_KEY = ("user_id", "user_created_at")

# The fields of each body, as its JSON form in core/common/src/types/metadata_changes
# names them.
_BODY_FIELDS = {
    MetadataStreamChange: (*_STREAM_KEY, "created_at", "name", "options"),
    MetadataStreamDeleted: (*_STREAM_KEY, "created_at", "name", "requested_op"),
    MetadataTopicChange: (
        *_TOPIC_KEY,
        "created_at",
        "name",
        "partitions_count",
        "options",
    ),
    MetadataTopicItem: (*_TOPIC_KEY, "created_at", "name", "options"),
    MetadataTopicDeleted: (*_TOPIC_KEY, "created_at", "name", "requested_op"),
    MetadataPartitionsChanged: (
        *_TOPIC_KEY,
        "partitions_count",
        "removed_partition_ids",
        "requested_op",
    ),
    MetadataPartitionsItem: (*_TOPIC_KEY, "partitions_count"),
    MetadataConsumerGroupCreated: (
        *_TOPIC_KEY,
        "group_id",
        "name",
        "consumer_groups_count",
    ),
    MetadataConsumerGroupDeleted: (
        *_TOPIC_KEY,
        "group_id",
        "name",
        "consumer_groups_count",
        "requested_op",
    ),
    MetadataConsumerGroupItem: (*_TOPIC_KEY, "group_id", "name"),
    MetadataMemberJoined: (
        *_TOPIC_KEY,
        "group_id",
        "member_id",
        "client_id",
        "members_count",
        "partition_ids",
    ),
    MetadataMemberLeft: (
        *_TOPIC_KEY,
        "group_id",
        "member_id",
        "client_id",
        "members_count",
        "reason",
    ),
    MetadataPartitionHandedOver: (*_TOPIC_KEY, "group_id", "partition_id", "member_id"),
    MetadataMemberItem: (
        *_TOPIC_KEY,
        "group_id",
        "member_id",
        "client_id",
        "partition_ids",
    ),
    MetadataUserChange: (*_USER_KEY, "username", "status", "permissions", "options"),
    MetadataUserIdentity: (*_USER_KEY, "username"),
    MetadataUserPermissionsUpdated: (*_USER_KEY, "username", "permissions", "reason"),
    MetadataTokenChange: (*_USER_KEY, "name", "expiry"),
    MetadataTokenDeleted: (*_USER_KEY, "name", "reason"),
    MetadataSession: (*_USER_KEY, "client_id", "session"),
    MetadataSessionClosed: (*_USER_KEY, "client_id", "session", "reason"),
}

_STREAM_PERMISSIONS = (
    "manage_stream",
    "read_stream",
    "manage_topics",
    "read_topics",
    "poll_messages",
    "send_messages",
)
_TOPIC_PERMISSIONS = ("manage_topic", "read_topic", "poll_messages", "send_messages")


def changes(page) -> dict[str, Any]:
    return {
        "status": str(page.status),
        "phase": str(page.phase),
        "log_id": _optional(page.log_id, _hex128),
        "oldest_op": page.oldest_op,
        "retained_since": page.retained_since,
        "head_op": page.head_op,
        "next": _optional(page.next, str),
        "has_more": page.has_more,
        "records": [_record(record) for record in page.records],
        "items": [_item(item) for item in page.items],
        "current_page_token": _optional(page.current_page_token, str),
    }


def _record(record) -> dict[str, Any]:
    view = {
        "op": record.op,
        "ordinal": record.ordinal,
        "checksum": _hex128(record.checksum),
        "timestamp": record.timestamp,
    }
    if record.actor_user_id is not None:
        view["actor_user_id"] = record.actor_user_id
    change = record.change
    if isinstance(change, MetadataUnknownChange):
        # A record of an unknown group or kind keeps its codes and its raw body.
        return view | {
            "group": str(change.group),
            "kind": str(change.kind),
            "body": base64.b64encode(change.body).decode(),
        }
    return view | {"group": str(record.group), "kind": str(record.kind)} | _body(change)


def _item(item) -> dict[str, Any]:
    body = item.body
    if isinstance(body, MetadataUnknownItem):
        return {"group": str(body.group), "body": base64.b64encode(body.body).decode()}
    return {"group": str(item.group)} | _body(body)


def _body(body) -> dict[str, Any]:
    view = {}
    for name in _BODY_FIELDS[type(body)]:
        value = getattr(body, name)
        match name:
            case "options" if body.options_unreadable:
                view |= {"options": None, "options_unreadable": True}
            case "options":
                view["options"] = options(body)
            case "permissions":
                view[name] = _permissions(value)
            case "client_id":
                view[name] = _hex128(value)
            case "status":
                view[name] = status_name(value)
            case "reason":
                view[name] = str(value)
            case _ if value is not None:
                view[name] = value
    return view


def _permissions(value) -> dict[str, Any] | None:
    """Permissions in the get_user JSON shape, with stream permissions."""
    if value is None:
        return None
    return {
        "global": global_permissions(value.global_permissions),
        "streams": _optional(
            value.streams,
            lambda streams: {
                str(stream_id): _stream_permissions(stream)
                for stream_id, stream in streams.items()
            },
        ),
    }


def _stream_permissions(value) -> dict[str, Any]:
    view: dict[str, Any] = {name: getattr(value, name) for name in _STREAM_PERMISSIONS}
    view["topics"] = _optional(
        value.topics,
        lambda topics: {
            str(topic_id): {name: getattr(topic, name) for name in _TOPIC_PERMISSIONS}
            for topic_id, topic in topics.items()
        },
    )
    return view


def _hex128(value: int) -> str:
    """A 128-bit value as 32 lowercase hex characters."""
    return f"{value:032x}"


def _optional(value, convert):
    return None if value is None else convert(value)
