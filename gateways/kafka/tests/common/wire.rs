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
#![allow(dead_code)]

use bytes::Bytes;

use iggy_gateway_kafka::protocol::codec::Encoder;

/// Consumer-group and admin keys explicitly out of scope in SCOPE.md.
pub const OUT_OF_SCOPE_API_KEYS: &[(i16, &str)] = &[
    (8, "OffsetCommit"),
    (9, "OffsetFetch"),
    (10, "FindCoordinator"),
    (11, "JoinGroup"),
    (12, "Heartbeat"),
    (13, "LeaveGroup"),
    (14, "SyncGroup"),
    (15, "DescribeGroups"),
    (16, "ListGroups"),
    (17, "SaslHandshake"),
    (20, "DeleteTopics"),
];

/// Flexible-encoding boundary per SCOPE.md valid-versions table.
pub const FLEXIBLE_FROM_VERSION: &[(i16, i16)] = &[
    (0, 9),  // Produce
    (1, 12), // Fetch
    (2, 6),  // ListOffsets
    (3, 9),  // Metadata
    (18, 3), // ApiVersions
    (19, 5), // CreateTopics
];

/// Metadata v9+ flexible request listing topic names (compact strings).
///
/// Each topic entry is its own tagged struct per the Kafka protocol schema
/// (`topics => name TAG_BUFFER`), so the per-topic tag buffer is written
/// right after each name, not once for the whole array.
pub fn build_metadata_flexible_request(topic_names: &[&str]) -> Bytes {
    let mut enc = Encoder::with_capacity(64);
    enc.write_varint((topic_names.len() + 1) as u64);
    for name in topic_names {
        enc.write_compact_nullable_string(Some(name));
        enc.write_empty_tagged_fields();
    }
    enc.write_empty_tagged_fields();
    enc.freeze()
}

/// Metadata v10+ flexible request: each topic entry includes a 16-byte `topic_id` before `name`.
pub fn build_metadata_flexible_request_v10(topic_names: &[&str]) -> Bytes {
    let mut enc = Encoder::with_capacity(96);
    enc.write_varint((topic_names.len() + 1) as u64);
    for name in topic_names {
        enc.write_bytes(&[0u8; 16]);
        enc.write_compact_nullable_string(Some(name));
        enc.write_empty_tagged_fields();
    }
    enc.write_empty_tagged_fields();
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

/// Fetch v2 body without `max_bytes` field (defaults to 50 MiB).
pub fn build_fetch_v2_default_max_bytes_request() -> Bytes {
    let mut enc = Encoder::with_capacity(32);
    enc.write_i32(-1);
    enc.write_i32(100);
    enc.write_i32(1);
    enc.write_i32(0);
    enc.freeze()
}

/// Produce v9+ flexible request with one topic/partition and tagged fields.
pub fn build_produce_flexible_request_with_topic(topic: &str) -> Bytes {
    let mut enc = Encoder::with_capacity(128);
    enc.write_compact_nullable_string(Some("txn-1"));
    enc.write_i16(1);
    enc.write_i32(500);
    enc.write_varint(2);
    enc.write_compact_nullable_string(Some(topic));
    enc.write_varint(2);
    enc.write_i32(0);
    enc.write_compact_nullable_bytes(Some(&[0x00, 0x00, 0x00, 0x00]));
    enc.write_empty_tagged_fields();
    enc.write_empty_tagged_fields();
    enc.write_empty_tagged_fields();
    enc.freeze()
}

/// Fetch v3 body without `isolation_level` field (defaults to 0).
pub fn build_fetch_v3_no_isolation_request() -> Bytes {
    let mut enc = Encoder::with_capacity(32);
    enc.write_i32(-1);
    enc.write_i32(100);
    enc.write_i32(1);
    enc.write_i32(1024);
    enc.write_i32(0);
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
