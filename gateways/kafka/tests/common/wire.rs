// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

//! Kafka wire request builders aligned with SCOPE.md / protocol spec.
//!
//! Callers must also declare `#[path = "common/codec.rs"] mod codec;` at their own crate root -
//! this file borrows that module via `super::codec` rather than redeclaring it, since `rustc`
//! rejects loading the same file as two distinct modules in one crate.
//!
//! `#![allow(dead_code)]` is load-bearing, not vestigial: each test binary `#[path]`-includes
//! this whole file but only calls a subset of it, so per-binary dead-code analysis would flag
//! every function some *other* binary uses as unused here.
#![allow(dead_code)]

use bytes::Bytes;

use super::codec::Encoder;

/// Consumer-group and admin keys explicitly out of scope in SCOPE.md.
pub const OUT_OF_SCOPE_API_KEYS: &[(i16, &str)] = &[
    (8, "OffsetCommit"),
    (9, "OffsetFetch"),
    (13, "LeaveGroup"),
    (15, "DescribeGroups"),
    (16, "ListGroups"),
    (20, "DeleteTopics"),
];

/// Append Metadata request fields that follow the topics array for `version`.
fn write_metadata_request_trailer(enc: &mut Encoder, version: i16) {
    if version >= 4 {
        enc.write_bool(true); // allow_auto_topic_creation
    }
    // include_cluster_authorized_operations exists on v8–v10 only (removed in v11).
    if (8..=10).contains(&version) {
        enc.write_bool(false);
    }
    if version >= 8 {
        enc.write_bool(false); // include_topic_authorized_operations
    }
    if version >= 9 {
        enc.write_empty_tagged_fields();
    }
}

/// Metadata v9 flexible request listing topic names (compact strings).
///
/// Each topic entry is its own tagged struct per the Kafka protocol schema
/// (`topics => name TAG_BUFFER`), so the per-topic tag buffer is written
/// right after each name, not once for the whole array.
pub fn build_metadata_flexible_request(topic_names: &[&str]) -> Bytes {
    build_metadata_flexible_request_for_version(9, topic_names)
}

/// Metadata flexible request for a specific version (v9+).
fn build_metadata_flexible_request_for_version(version: i16, topic_names: &[&str]) -> Bytes {
    let mut enc = Encoder::with_capacity(96);
    enc.write_varint((topic_names.len() + 1) as u64);
    for name in topic_names {
        if version >= 10 {
            enc.write_bytes(&[0u8; 16]);
        }
        enc.write_compact_nullable_string(Some(name));
        enc.write_empty_tagged_fields();
    }
    write_metadata_request_trailer(&mut enc, version);
    enc.freeze()
}

/// Metadata v10+ flexible request: each topic entry includes a 16-byte `topic_id` before `name`.
pub fn build_metadata_flexible_request_v10(topic_names: &[&str]) -> Bytes {
    build_metadata_flexible_request_for_version(10, topic_names)
}

/// `ApiVersions` v3+ flexible body (`ClientSoftwareName`, `ClientSoftwareVersion`, tagged fields).
pub fn build_api_versions_flexible_request(software_name: &str, software_version: &str) -> Bytes {
    let mut enc = Encoder::with_capacity(64);
    enc.write_compact_nullable_string(Some(software_name));
    enc.write_compact_nullable_string(Some(software_version));
    enc.write_empty_tagged_fields();
    enc.freeze()
}

/// Legacy Metadata request body for a specific version (v0–v8).
pub fn build_metadata_legacy_request_for_version(version: i16, topic_names: &[&str]) -> Bytes {
    let mut enc = Encoder::with_capacity(64);
    enc.write_i32(i32::try_from(topic_names.len()).expect("topic name count fits i32"));
    for name in topic_names {
        enc.write_nullable_string(Some(name))
            .expect("topic name fits");
    }
    write_metadata_request_trailer(&mut enc, version);
    enc.freeze()
}

/// Legacy Metadata "all topics" body (`topics = null` / `-1`) for a specific version.
pub fn build_metadata_all_topics_legacy(version: i16) -> Bytes {
    let mut enc = Encoder::with_capacity(16);
    enc.write_i32(-1);
    write_metadata_request_trailer(&mut enc, version);
    enc.freeze()
}

/// Flexible Metadata "all topics" body (`topics` compact null / varint `0`).
pub fn build_metadata_all_topics_flexible(version: i16) -> Bytes {
    let mut enc = Encoder::with_capacity(16);
    enc.write_varint(0); // null compact array → all topics
    write_metadata_request_trailer(&mut enc, version);
    enc.freeze()
}

/// Minimal `ListOffsets` request for supported versions (v1–v6).
pub fn build_list_offsets_request(version: i16, topic: &str, partition: i32) -> Bytes {
    let flexible = version >= 6;
    let mut enc = Encoder::with_capacity(128);
    enc.write_i32(-1); // replica_id
    if version >= 2 {
        enc.write_i8(0); // isolation_level
    }

    if flexible {
        enc.write_varint(2); // one topic (N+1)
        enc.write_compact_nullable_string(Some(topic));
        enc.write_varint(2); // one partition
    } else {
        enc.write_i32(1);
        enc.write_nullable_string(Some(topic))
            .expect("topic name fits");
        enc.write_i32(1);
    }

    enc.write_i32(partition);
    if version >= 4 {
        enc.write_i32(-1); // current_leader_epoch
    }
    enc.write_i64(-1); // latest timestamp

    if flexible {
        enc.write_empty_tagged_fields(); // partition tagged fields
        enc.write_empty_tagged_fields(); // topic tagged fields
        enc.write_empty_tagged_fields(); // request tagged fields
    }

    enc.freeze()
}

/// `CreateTopics` v2+ with zero topics (valid empty create).
pub fn build_create_topics_empty_request(version: i16) -> Bytes {
    let flexible = version >= 5;
    let mut enc = Encoder::with_capacity(32);

    if flexible {
        enc.write_varint(1); // empty topics compact array (N+1 = 1)
    } else {
        enc.write_i32(0);
    }
    enc.write_i32(5_000); // timeout_ms
    if version >= 1 {
        enc.write_bool(false); // validate_only
    }
    if flexible {
        enc.write_empty_tagged_fields();
    }

    enc.freeze()
}

/// Produce v2–v8 legacy request with optional transactional id and topic.
pub fn build_produce_legacy_request(
    version: i16,
    acks: i16,
    transactional_id: Option<&str>,
    topic: Option<&str>,
) -> Bytes {
    let mut enc = Encoder::with_capacity(128);
    if version >= 3 {
        enc.write_nullable_string(transactional_id)
            .expect("transactional id fits");
    }
    enc.write_i16(acks);
    enc.write_i32(1_000);
    enc.write_i32(i32::from(topic.is_some()));
    if let Some(name) = topic {
        enc.write_nullable_string(Some(name))
            .expect("topic name fits");
        enc.write_i32(1);
        enc.write_i32(0);
        enc.write_nullable_bytes(Some(&[0x00, 0x00, 0x00, 0x00]))
            .expect("records fit");
    }
    enc.freeze()
}

/// Produce v9+ flexible request with empty topics array.
pub fn build_produce_flexible_empty_request(acks: i16) -> Bytes {
    let mut enc = Encoder::with_capacity(32);
    enc.write_compact_nullable_string(None); // null transactional_id
    enc.write_i16(acks);
    enc.write_i32(1_000); // timeout_ms
    enc.write_varint(1); // empty topics compact array (N+1)
    enc.write_empty_tagged_fields();
    enc.freeze()
}

/// Fetch v4+ minimal empty-topic request.
pub fn build_fetch_empty_topics_request(version: i16) -> Bytes {
    let flexible = version >= 12;
    let mut enc = Encoder::with_capacity(64);

    enc.write_i32(-1); // replica_id
    enc.write_i32(100); // max_wait_ms
    enc.write_i32(1); // min_bytes
    if version >= 3 {
        enc.write_i32(i32::MAX); // max_bytes
    }
    if version >= 4 {
        enc.write_i8(0); // isolation_level
    }
    if version >= 7 {
        enc.write_i32(0); // session_id
        enc.write_i32(0); // session_epoch
    }

    if flexible {
        enc.write_varint(1); // empty topics compact array
    } else {
        enc.write_i32(0);
    }

    if version >= 7 {
        if flexible {
            enc.write_varint(1); // empty forgotten_topics_data
        } else {
            enc.write_i32(0);
        }
    }

    if flexible {
        enc.write_empty_tagged_fields();
    }

    enc.freeze()
}

/// Fetch request with one topic/partition and optional forgotten topics / rack id.
pub fn build_fetch_request_with_sections(
    version: i16,
    topic: &str,
    partition: i32,
    forgotten_topic: Option<&str>,
    rack_id: Option<&str>,
) -> Bytes {
    let flexible = version >= 12;
    let mut enc = Encoder::with_capacity(256);

    enc.write_i32(-1); // replica_id
    enc.write_i32(100); // max_wait_ms
    enc.write_i32(1); // min_bytes
    if version >= 3 {
        enc.write_i32(i32::MAX); // max_bytes
    }
    if version >= 4 {
        enc.write_i8(0); // isolation_level
    }
    if version >= 7 {
        enc.write_i32(7); // session_id
        enc.write_i32(1); // session_epoch
    }

    if flexible {
        enc.write_varint(2); // one topic
        enc.write_compact_nullable_string(Some(topic));
        enc.write_varint(2); // one partition
    } else {
        enc.write_i32(1);
        enc.write_nullable_string(Some(topic))
            .expect("topic name fits");
        enc.write_i32(1);
    }

    enc.write_i32(partition);
    if version >= 9 {
        enc.write_i32(-1); // current_leader_epoch
    }
    enc.write_i64(42); // fetch_offset
    if version >= 12 {
        enc.write_i32(-1); // last_fetched_epoch
    }
    if version >= 5 {
        enc.write_i64(0); // log_start_offset
    }
    enc.write_i32(1024); // partition_max_bytes
    if flexible {
        enc.write_empty_tagged_fields(); // partition tagged fields
        enc.write_empty_tagged_fields(); // topic tagged fields
    }

    if version >= 7 {
        let forgotten_count = usize::from(forgotten_topic.is_some());
        if flexible {
            enc.write_varint((forgotten_count + 1) as u64);
        } else {
            enc.write_i32(i32::try_from(forgotten_count).expect("count fits i32"));
        }
        if let Some(name) = forgotten_topic {
            if flexible {
                enc.write_compact_nullable_string(Some(name));
                enc.write_varint(2); // one partition
                enc.write_i32(partition);
                enc.write_empty_tagged_fields();
            } else {
                enc.write_nullable_string(Some(name))
                    .expect("topic name fits");
                enc.write_i32(1);
                enc.write_i32(partition);
            }
        }
    }

    if version >= 11 {
        if flexible {
            enc.write_compact_nullable_string(rack_id);
        } else {
            enc.write_nullable_string(rack_id).expect("rack id fits");
        }
    }

    if flexible {
        enc.write_empty_tagged_fields();
    }

    enc.freeze()
}

/// `ListOffsets` request covering legacy v0 `max_num_offsets` and newer leader-epoch branches.
pub fn build_list_offsets_branch_request(version: i16, topic: &str, partition: i32) -> Bytes {
    let flexible = version >= 6;
    let mut enc = Encoder::with_capacity(128);
    enc.write_i32(-1); // replica_id
    if version >= 2 {
        enc.write_i8(1); // isolation_level
    }

    if flexible {
        enc.write_varint(2); // one topic
        enc.write_compact_nullable_string(Some(topic));
        enc.write_varint(2); // one partition
    } else {
        enc.write_i32(1);
        enc.write_nullable_string(Some(topic)).expect("topic fits");
        enc.write_i32(1);
    }

    enc.write_i32(partition);
    if version >= 4 {
        enc.write_i32(-1); // current_leader_epoch
    }
    enc.write_i64(-2); // earliest
    if version == 0 {
        enc.write_i32(1); // max_num_offsets
    }
    if flexible {
        enc.write_empty_tagged_fields();
        enc.write_empty_tagged_fields();
        enc.write_empty_tagged_fields();
    }

    enc.freeze()
}

/// `CreateTopics` request with one topic, one assignment, and one config.
pub fn build_create_topics_request_with_sections(version: i16, topic: &str) -> Bytes {
    let flexible = version >= 5;
    let mut enc = Encoder::with_capacity(256);

    if flexible {
        enc.write_varint(2); // one topic
        enc.write_compact_nullable_string(Some(topic));
    } else {
        enc.write_i32(1);
        enc.write_nullable_string(Some(topic)).expect("topic fits");
    }
    enc.write_i32(3); // num_partitions
    enc.write_i16(1); // replication_factor

    if flexible {
        enc.write_varint(2); // one assignment
    } else {
        enc.write_i32(1);
    }
    enc.write_i32(0); // partition_index
    if flexible {
        enc.write_varint(2); // one replica
    } else {
        enc.write_i32(1);
    }
    enc.write_i32(1); // broker_id
    if flexible {
        enc.write_empty_tagged_fields();
    }

    if flexible {
        enc.write_varint(2); // one config
        enc.write_compact_nullable_string(Some("cleanup.policy"));
        enc.write_compact_nullable_string(Some("delete"));
        enc.write_empty_tagged_fields();
    } else {
        enc.write_i32(1);
        enc.write_nullable_string(Some("cleanup.policy"))
            .expect("config key fits");
        enc.write_nullable_string(Some("delete"))
            .expect("config value fits");
    }

    if flexible {
        enc.write_empty_tagged_fields(); // topic tagged fields
    }

    enc.write_i32(5_000); // timeout_ms
    if version >= 1 {
        enc.write_bool(true); // validate_only
    }
    if flexible {
        enc.write_empty_tagged_fields();
    }

    enc.freeze()
}

// ── Consumer group coordination (keys 10, 11, 12, 14) ───────────────────────

/// Write a Kafka string, compact or legacy by `flexible`.
fn write_string(enc: &mut Encoder, flexible: bool, value: Option<&str>) {
    if flexible {
        enc.write_compact_nullable_string(value);
    } else {
        enc.write_nullable_string(value).expect("string fits");
    }
}

/// Write a Kafka bytes field, compact or legacy by `flexible`.
fn write_bytes_field(enc: &mut Encoder, flexible: bool, value: &[u8]) {
    if flexible {
        enc.write_compact_nullable_bytes(Some(value));
    } else {
        enc.write_nullable_bytes(Some(value)).expect("bytes fit");
    }
}

fn write_array_count(enc: &mut Encoder, flexible: bool, count: usize) {
    if flexible {
        enc.write_varint((count + 1) as u64);
    } else {
        enc.write_i32(i32::try_from(count).expect("count fits i32"));
    }
}

/// `FindCoordinator` request. `keys` carries one entry below v4 and any number from v4.
pub fn build_find_coordinator_request(version: i16, keys: &[&str], key_type: i8) -> Bytes {
    let flexible = version >= 3;
    let mut enc = Encoder::with_capacity(64);

    if version <= 3 {
        write_string(
            &mut enc,
            flexible,
            Some(keys.first().copied().unwrap_or("")),
        );
    }
    if version >= 1 {
        enc.write_i8(key_type);
    }
    if version >= 4 {
        enc.write_varint((keys.len() + 1) as u64);
        for key in keys {
            enc.write_compact_nullable_string(Some(key));
        }
    }
    if flexible {
        enc.write_empty_tagged_fields();
    }
    enc.freeze()
}

pub const DEFAULT_JOIN_PROTOCOLS: &[(&str, &[u8])] = &[("range", b"subscription")];

/// Everything a `JoinGroup` body carries, so callers vary one field at a time.
pub struct JoinGroupParams<'a> {
    pub group_id: &'a str,
    pub session_timeout_ms: i32,
    pub rebalance_timeout_ms: i32,
    pub member_id: &'a str,
    pub group_instance_id: Option<&'a str>,
    pub protocol_type: &'a str,
    pub protocols: &'a [(&'a str, &'a [u8])],
}

impl Default for JoinGroupParams<'_> {
    fn default() -> Self {
        Self {
            group_id: "test-group",
            session_timeout_ms: 10_000,
            rebalance_timeout_ms: 20_000,
            member_id: "",
            group_instance_id: None,
            protocol_type: "consumer",
            protocols: DEFAULT_JOIN_PROTOCOLS,
        }
    }
}

pub fn build_join_group_request(version: i16, params: &JoinGroupParams<'_>) -> Bytes {
    let flexible = version >= 6;
    let mut enc = Encoder::with_capacity(256);

    write_string(&mut enc, flexible, Some(params.group_id));
    enc.write_i32(params.session_timeout_ms);
    if version >= 1 {
        enc.write_i32(params.rebalance_timeout_ms);
    }
    write_string(&mut enc, flexible, Some(params.member_id));
    if version >= 5 {
        write_string(&mut enc, flexible, params.group_instance_id);
    }
    write_string(&mut enc, flexible, Some(params.protocol_type));

    write_array_count(&mut enc, flexible, params.protocols.len());
    for (name, metadata) in params.protocols {
        write_string(&mut enc, flexible, Some(name));
        write_bytes_field(&mut enc, flexible, metadata);
        if flexible {
            enc.write_empty_tagged_fields();
        }
    }

    if version >= 8 {
        enc.write_compact_nullable_string(None); // reason
    }
    if flexible {
        enc.write_empty_tagged_fields();
    }
    enc.freeze()
}

pub fn build_heartbeat_request(
    version: i16,
    group_id: &str,
    generation_id: i32,
    member_id: &str,
) -> Bytes {
    let flexible = version >= 4;
    let mut enc = Encoder::with_capacity(64);

    write_string(&mut enc, flexible, Some(group_id));
    enc.write_i32(generation_id);
    write_string(&mut enc, flexible, Some(member_id));
    if version >= 3 {
        write_string(&mut enc, flexible, None); // group_instance_id
    }
    if flexible {
        enc.write_empty_tagged_fields();
    }
    enc.freeze()
}

/// Everything a `SyncGroup` body carries. `protocol_type`/`protocol_name` are written from v5.
pub struct SyncGroupParams<'a> {
    pub group_id: &'a str,
    pub generation_id: i32,
    pub member_id: &'a str,
    pub protocol_type: Option<&'a str>,
    pub protocol_name: Option<&'a str>,
    pub assignments: &'a [(&'a str, &'a [u8])],
}

impl Default for SyncGroupParams<'_> {
    fn default() -> Self {
        Self {
            group_id: "test-group",
            generation_id: 1,
            member_id: "",
            protocol_type: None,
            protocol_name: None,
            assignments: &[],
        }
    }
}

pub fn build_sync_group_request(version: i16, params: &SyncGroupParams<'_>) -> Bytes {
    let flexible = version >= 4;
    let mut enc = Encoder::with_capacity(256);

    write_string(&mut enc, flexible, Some(params.group_id));
    enc.write_i32(params.generation_id);
    write_string(&mut enc, flexible, Some(params.member_id));
    if version >= 3 {
        write_string(&mut enc, flexible, None); // group_instance_id
    }
    if version >= 5 {
        enc.write_compact_nullable_string(params.protocol_type);
        enc.write_compact_nullable_string(params.protocol_name);
    }

    write_array_count(&mut enc, flexible, params.assignments.len());
    for (member_id, assignment) in params.assignments {
        write_string(&mut enc, flexible, Some(member_id));
        write_bytes_field(&mut enc, flexible, assignment);
        if flexible {
            enc.write_empty_tagged_fields();
        }
    }

    if flexible {
        enc.write_empty_tagged_fields();
    }
    enc.freeze()
}
