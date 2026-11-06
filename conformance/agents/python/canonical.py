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

"""The canonical result forms of core/conformance/src/canonical.rs. The orchestrator
decodes each result into its form and refuses any field that the form does not have."""

import base64
import contextlib
import struct
from typing import Any

from apache_iggy import HeaderKey, HeaderValue, UserStatus

from replies import Failure

# Each typed header variant with its kind name, and the struct format of a fixed-size
# value.
_VARIANTS = {
    "Raw": ("raw", None),
    "String": ("string", None),
    "Bool": ("bool", "<?"),
    "Int8": ("int8", "<b"),
    "Int16": ("int16", "<h"),
    "Int32": ("int32", "<i"),
    "Int64": ("int64", "<q"),
    "Int128": ("int128", None),
    "UnsignedInt8": ("uint8", "<B"),
    "UnsignedInt16": ("uint16", "<H"),
    "UnsignedInt32": ("uint32", "<I"),
    "UnsignedInt64": ("uint64", "<Q"),
    "UnsignedInt128": ("uint128", None),
    "Float32": ("float32", "<f"),
    "Float64": ("float64", "<d"),
}
_KEY_KINDS = {
    getattr(HeaderKey, variant): kind for variant, (kind, _) in _VARIANTS.items()
}
_VALUE_KINDS = {
    getattr(HeaderValue, variant): kind for variant, (kind, _) in _VARIANTS.items()
}
_VALUE_VARIANTS = {
    kind: getattr(HeaderValue, variant) for variant, (kind, _) in _VARIANTS.items()
}
_FORMATS = dict(_VARIANTS.values())

GLOBAL_PERMISSIONS = (
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
)


def stream(value) -> dict[str, Any]:
    return {
        "id": value.id,
        "name": value.name,
        "created_at": value.created_at,
        "size_bytes": value.size,
        "messages_count": value.messages_count,
        "topics_count": value.topics_count,
        "options": options(value),
    }


def stream_details(value) -> dict[str, Any]:
    return stream(value) | {"topics": [topic(item) for item in value.topics]}


def topic(value) -> dict[str, Any]:
    return {
        "id": value.id,
        "name": value.name,
        "created_at": value.created_at,
        "size_bytes": value.size,
        "messages_count": value.messages_count,
        "partitions_count": value.partitions_count,
        "options": options(value),
    }


def topic_details(value) -> dict[str, Any]:
    return topic(value) | {"partitions": [partition(item) for item in value.partitions]}


def partition(value) -> dict[str, Any]:
    return {
        "id": value.id,
        "created_at": value.created_at,
        "segments_count": value.segments_count,
        "current_offset": value.current_offset,
        "size_bytes": value.size,
        "messages_count": value.messages_count,
    }


def polled(value) -> dict[str, Any]:
    messages = value.messages
    # The SDK keeps the messages of a batch, not their count.
    return {
        "partition_id": value.partition_id,
        "current_offset": value.current_offset,
        "count": len(messages),
        "messages": [message(item) for item in messages],
    }


def message(value) -> dict[str, Any]:
    payload = value.payload()
    view = {
        "offset": value.offset(),
        "id": str(value.id()),
        "timestamp": value.timestamp(),
        "origin_timestamp": value.origin_timestamp(),
        "checksum": value.checksum(),
        "payload_hex": payload.hex(),
    }
    with contextlib.suppress(UnicodeDecodeError):
        view["payload"] = payload.decode()
    headers = value.user_headers()
    if headers:
        view["headers"] = {
            key_text(key): header_entry(header) for key, header in headers.items()
        }
    return view


def header_entry(value: HeaderValue) -> dict[str, str]:
    kind, text = value_text(value)
    return {"kind": kind, "value": text}


def consumer_group(value) -> dict[str, Any]:
    return {
        "id": value.id,
        "name": value.name,
        "partitions_count": value.partitions_count,
        "members_count": value.members_count,
        "members": [
            {
                "id": member.id,
                "partitions_count": member.partitions_count,
                "partitions": list(member.partitions),
            }
            for member in value.members
        ],
    }


def option_spec(value) -> dict[str, Any]:
    return {
        "key": value.key,
        "kind": value.kind,
        "updatable": value.updatable,
        "resettable": value.resettable,
        "secret": value.secret,
        "read_only": value.read_only,
        "critical": value.critical,
    }


def user_details(value) -> dict[str, Any]:
    permissions = value.permissions
    return {
        "id": value.id,
        "username": value.username,
        "status": status_name(value.status),
        "created_at": value.created_at,
        "permissions": None
        if permissions is None
        else {"global": global_permissions(permissions.global_permissions)},
        "options": options(value),
    }


def global_permissions(value) -> dict[str, bool]:
    return {name: getattr(value, name) for name in GLOBAL_PERMISSIONS}


def status_name(status: UserStatus) -> str:
    return "inactive" if status == UserStatus.Inactive else "active"


def options(resource) -> dict[str, dict[str, Any]]:
    """Joins the options that the client set and those that the server derived into one
    map with the explicit flag of each, as the HTTP JSON form does."""
    view = {}
    for headers, explicit in (
        (resource.derived_options, False),
        (resource.options, True),
    ):
        for key, value in headers.items():
            kind, text = value_text(value)
            view[key_text(key)] = {"value": text, "kind": kind, "explicit": explicit}
    return view


def value_text(value: HeaderValue) -> tuple[str, str]:
    """The kind of a header value and its reversible text: UTF-8 for strings, true or
    false, base-10 integers, padded standard base64 for raw bytes, and 0x with the
    IEEE-754 bits in fixed-width lowercase hex for floats."""
    kind = _VALUE_KINDS[type(value)]
    raw = value.value
    match kind:
        case "raw":
            return kind, base64.b64encode(raw).decode()
        case "string":
            return kind, raw
        case "bool":
            return kind, "true" if raw else "false"
        case "float32" | "float64":
            bits = struct.pack(_FORMATS[kind], raw)
            return kind, f"0x{int.from_bytes(bits, 'little'):0{2 * len(bits)}x}"
        case _:
            return kind, str(raw)


def header_value(*, kind: str, value: str) -> HeaderValue:
    """Reverses value_text."""
    if kind not in _VALUE_VARIANTS:
        raise Failure(f"unknown header kind {kind!r}")
    variant = _VALUE_VARIANTS[kind]
    match kind:
        case "raw":
            return variant(base64.b64decode(value, validate=True))
        case "string":
            return variant(value)
        case "bool" if value in ("true", "false"):
            return variant(value == "true")
        case "float32" | "float64" if value.startswith("0x"):
            size = struct.calcsize(_FORMATS[kind])
            bits = int(value[2:], 16).to_bytes(size, "little")
            return variant(struct.unpack(_FORMATS[kind], bits)[0])
        case "bool" | "float32" | "float64":
            raise Failure(f"{value!r} is not the text of a {kind} value")
        case _:
            return variant(int(value))


def key_text(key: HeaderKey) -> str:
    """A header key as text: the UTF-8 of its bytes, with invalid bytes replaced."""
    kind = _KEY_KINDS[type(key)]
    raw = key.value
    match kind:
        case "string":
            return raw
        case "raw":
            data = raw
        case "int128" | "uint128":
            data = raw.to_bytes(16, "little", signed=kind == "int128")
        case _:
            data = struct.pack(_FORMATS[kind], raw)
    return data.decode(errors="replace")
