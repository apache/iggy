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

"""The argument forms of conformance/README.md as SDK values. Identifiers need no
conversion: the SDK takes a number as an id and text as a name, as the arguments carry
them."""

from datetime import timedelta
from typing import Any

from apache_iggy import (
    AutoCommit,
    CommitPolicy,
    Consumer,
    GlobalPermissions,
    HeaderKey,
    IggyExpiry,
    MaxTopicSize,
    MetadataChangeCursor,
    MetadataChangeGroup,
    MetadataChangeStart,
    MetadataCurrentPageToken,
    Partitioning,
    Permissions,
    PollingStrategy,
    SendMessage,
    UserStatus,
)

from canonical import header_value
from replies import Failure

# The SDK has no default consumer, so a call without one names the default consumer of
# the Rust SDK, which the contract refers to.
DEFAULT_CONSUMER = Consumer.Single(0)

METADATA_GROUPS = {
    str(group): group
    for group in vars(MetadataChangeGroup).values()
    if isinstance(group, MetadataChangeGroup)
}


def keyed(value: Any) -> tuple[str, Any]:
    """A bare keyword such as "first", which has no value, or a one-key object such as
    {"offset": 3}."""
    if isinstance(value, str):
        return value, None
    if isinstance(value, dict) and len(value) == 1:
        return next(iter(value.items()))
    raise Failure(f"expected a keyword or a one-key object, got {value!r}")


def strategy(value: Any) -> PollingStrategy:
    match keyed(value):
        case "first", None:
            return PollingStrategy.First()
        case "last", None:
            return PollingStrategy.Last()
        case "next", None:
            return PollingStrategy.Next()
        case "offset", int(offset):
            return PollingStrategy.Offset(offset)
        case "timestamp", int(micros):
            return PollingStrategy.Timestamp(micros)
    raise Failure(f"unknown strategy {value!r}")


def partitioning(value: Any) -> Partitioning:
    match keyed(value):
        case "balanced", None:
            return Partitioning.balanced()
        case "partition_id", int(partition_id):
            return Partitioning.partition_id(partition_id)
        case "messages_key", str(key):
            return Partitioning.messages_key(key)
    raise Failure(f"unknown partitioning {value!r}")


def consumer(value: Any) -> Consumer:
    if value is None:
        return DEFAULT_CONSUMER
    match keyed(value):
        case "consumer", int() | str() as consumer_id:
            return Consumer.Single(consumer_id)
        case "group", int() | str() as group_id:
            return Consumer.Group(group_id)
    raise Failure(f"unknown consumer {value!r}")


def expiry(value: Any) -> IggyExpiry:
    match value:
        case "never":
            return IggyExpiry.NeverExpire()
        case "server_default":
            return IggyExpiry.ServerDefault()
        case int(micros):
            return IggyExpiry.ExpireDuration(timedelta(microseconds=micros))
    raise Failure(f"unknown message expiry {value!r}")


def topic_size(value: Any) -> MaxTopicSize:
    match value:
        case "unlimited":
            return MaxTopicSize.Unlimited()
        case "server_default":
            return MaxTopicSize.ServerDefault()
        case int(size):
            return MaxTopicSize.Custom(size)
    raise Failure(f"unknown max topic size {value!r}")


def commit(value: str) -> CommitPolicy:
    match value:
        case "manual":
            return CommitPolicy.Manual()
        case "on_read":
            return CommitPolicy.OnRead()
        case "after_processing":
            # The default automatic commit counts a message after its processing.
            return CommitPolicy.Auto(AutoCommit.default())
    raise Failure(f"unknown commit policy {value!r}")


def milliseconds(value: int | None) -> timedelta | None:
    return None if value is None else timedelta(milliseconds=value)


def user_status(value: str) -> UserStatus:
    match value:
        case "active":
            return UserStatus.Active
        case "inactive":
            return UserStatus.Inactive
    raise Failure(f"unknown user status {value!r}")


def permissions(value: dict[str, Any] | None) -> Permissions | None:
    """Global permissions only, as format 1 has them."""
    if value is None:
        return None
    unknown = value.keys() - {"global"}
    if unknown:
        raise Failure(f"unknown permissions {sorted(unknown)}")
    return Permissions(global_permissions=GlobalPermissions(**value.get("global", {})))


def metadata_start(value: Any) -> MetadataChangeStart:
    match keyed(value):
        case "earliest", None:
            return MetadataChangeStart.Earliest()
        case "latest", None:
            return MetadataChangeStart.Latest()
        case "after", str(cursor):
            return MetadataChangeStart.After(MetadataChangeCursor.parse(cursor))
        case "current", None:
            return MetadataChangeStart.Current()
        case "current", str(token):
            return MetadataChangeStart.Current(MetadataCurrentPageToken.parse(token))
    raise Failure(f"unknown metadata start {value!r}")


def metadata_group(name: str) -> MetadataChangeGroup:
    if name not in METADATA_GROUPS:
        raise Failure(f"unknown metadata group {name!r}")
    return METADATA_GROUPS[name]


def message(value: dict[str, Any]) -> SendMessage:
    return _message(**value)


def _message(*, id=None, payload=None, payload_hex=None, headers=None) -> SendMessage:
    if (payload is None) == (payload_hex is None):
        raise Failure("a message needs exactly one of payload and payload_hex")
    data = payload.encode() if payload is not None else bytes.fromhex(payload_hex)
    user_headers = None
    if headers:
        user_headers = {
            HeaderKey.String(key): header_value(**header)
            for key, header in headers.items()
        }
    return SendMessage(data, user_headers, None if id is None else message_id(id))


def message_id(text: str) -> int:
    """A u128 id from its decimal text."""
    if not (text.isascii() and text.isdigit()) or int(text) >= 1 << 128:
        raise Failure(f"the message id {text!r} is not a u128")
    return int(text)
