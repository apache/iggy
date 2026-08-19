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

#[path = "common/codec.rs"]
mod codec;
#[path = "common/fixtures.rs"]
mod fixtures;
#[path = "common/scope.rs"]
mod scope;
#[path = "common/tcp.rs"]
mod tcp;
#[path = "common/wire.rs"]
mod wire;

use bytes::Bytes;

use iggy_gateway_kafka::protocol::api::{
    API_KEY_API_VERSIONS, API_KEY_CREATE_TOPICS, API_KEY_FETCH, API_KEY_LIST_OFFSETS,
    API_KEY_METADATA, API_KEY_PRODUCE, ERROR_INVALID_REQUEST, ERROR_NONE, ERROR_NOT_CONTROLLER,
    ERROR_NOT_LEADER_OR_FOLLOWER, ERROR_UNKNOWN_TOPIC_OR_PARTITION, ERROR_UNSUPPORTED_VERSION,
    handle_request, is_supported_version, supported_api_ranges,
};

use codec::Decoder;
use fixtures::load_fixture_body_or_skip;
use scope::default_broker;
use tcp::{build_metadata_legacy_request, build_produce_v3_body};
use wire::{
    build_api_versions_flexible_request, build_metadata_all_topics_legacy,
    build_metadata_flexible_request, build_metadata_flexible_request_v10,
    build_metadata_legacy_request_for_version, build_produce_legacy_request,
};

// ── ApiVersions ─────────────────────────────────────────────────────────────

#[test]
fn api_versions_v1_response_non_flexible_format() {
    let body = handle_request(API_KEY_API_VERSIONS, 1, Bytes::new(), &default_broker())
        .expect_response("test request has acks != 0 and expects a response");
    let mut d = Decoder::new(body);

    assert_eq!(d.read_i16().unwrap(), 0); // error_code

    // Non-flexible: i32 array count
    let count = d.read_i32().unwrap();
    assert!(count >= 2);
    let mut keys = Vec::new();
    for _ in 0..count {
        keys.push(d.read_i16().unwrap());
        d.read_i16().unwrap(); // min
        d.read_i16().unwrap(); // max
    }
    assert_eq!(d.read_i32().unwrap(), 0); // throttle_time_ms

    let expected_keys: Vec<i16> = supported_api_ranges().iter().map(|r| r.api_key).collect();
    for k in expected_keys {
        assert!(keys.contains(&k));
    }
}

#[test]
fn api_versions_v3_response_flexible_format() {
    let body = handle_request(
        API_KEY_API_VERSIONS,
        3,
        build_api_versions_flexible_request("apache-iggy", "0.1.0"),
        &default_broker(),
    )
    .expect_response("test request has acks != 0 and expects a response");
    let mut d = Decoder::new(body);

    assert_eq!(d.read_i16().unwrap(), 0); // error_code

    // Flexible: varint(len+1) compact array
    let count_plus_one = d.read_varint().unwrap();
    assert!(count_plus_one >= 3); // at least 2 entries → varint = 3+
    let count = i32::try_from(count_plus_one - 1).expect("api count fits i32");

    let mut keys = Vec::new();
    for _ in 0..count {
        keys.push(d.read_i16().unwrap());
        d.read_i16().unwrap(); // min
        d.read_i16().unwrap(); // max
        d.read_tagged_fields().unwrap(); // per-entry tagged fields
    }
    assert_eq!(d.read_i32().unwrap(), 0); // throttle_time_ms
    d.read_tagged_fields().unwrap(); // top-level tagged fields

    let expected_keys: Vec<i16> = supported_api_ranges().iter().map(|r| r.api_key).collect();
    for k in expected_keys {
        assert!(keys.contains(&k));
    }
}

// ── Metadata ─────────────────────────────────────────────────────────────────

#[test]
fn metadata_response_has_broker_array_and_topic_array() {
    let body = handle_request(
        API_KEY_METADATA,
        0,
        build_metadata_all_topics_legacy(0),
        &default_broker(),
    )
    .expect_response("test request has acks != 0 and expects a response");
    let mut d = Decoder::new(body);

    let broker_count = d.read_i32().unwrap();
    assert_eq!(broker_count, 1);
    let node_id = d.read_i32().unwrap();
    assert_eq!(node_id, 1);
    let host = d.read_nullable_string().unwrap().unwrap();
    assert_eq!(host, "127.0.0.1");
    let port = d.read_i32().unwrap();
    assert_eq!(port, 9093);

    let topic_count = d.read_i32().unwrap();
    assert_eq!(topic_count, 0);
}

#[test]
fn metadata_empty_body_closes_connection() {
    assert!(
        handle_request(API_KEY_METADATA, 0, Bytes::new(), &default_broker()).is_close(),
        "empty Metadata body is malformed, not all-topics"
    );
}

#[test]
fn api_versions_v3_empty_body_returns_invalid_request() {
    let body = handle_request(API_KEY_API_VERSIONS, 3, Bytes::new(), &default_broker())
        .expect_response("ApiVersions has a top-level error_code");
    let mut d = Decoder::new(body);
    assert_eq!(d.read_i16().unwrap(), ERROR_INVALID_REQUEST);
}

#[test]
fn unsupported_metadata_version_closes_connection() {
    // Above max: a clamped v9 body is unparsable to a v99 client, so Close is the honest contract.
    assert!(
        handle_request(
            API_KEY_METADATA,
            99,
            build_metadata_flexible_request_v10(&["orders"]),
            &default_broker(),
        )
        .is_close(),
        "Metadata above supported max must close rather than return a clamped body"
    );
}

// ── Misc ────────────────────────────────────────────────────────────────────

#[test]
fn unknown_api_key_closes_connection() {
    let outcome = handle_request(999, 0, Bytes::new(), &default_broker());
    assert!(
        outcome.is_close(),
        "unknown api_key must close (no parseable response schema)"
    );
}

#[test]
fn version_support_table_is_applied() {
    assert!(is_supported_version(API_KEY_API_VERSIONS, 3));
    assert!(!is_supported_version(API_KEY_API_VERSIONS, 10));
    assert!(is_supported_version(API_KEY_METADATA, 1));
    assert!(!is_supported_version(API_KEY_METADATA, -1));
}

#[test]
fn apiversions_unsupported_version_uses_v0_encoding_without_throttle() {
    let body = handle_request(API_KEY_API_VERSIONS, 99, Bytes::new(), &default_broker())
        .expect_response("test request has acks != 0 and expects a response");
    // v0: error_code(2) + api_keys i32 count(4) + 6 entries × 6 bytes = 42 - no throttle_time_ms.
    assert_eq!(body.len(), 42);
    let mut d = Decoder::new(body);
    assert_eq!(d.read_i16().unwrap(), ERROR_UNSUPPORTED_VERSION);
    assert_eq!(d.read_i32().unwrap(), 6);
    assert_eq!(d.remaining(), 36);
}

#[test]
fn produce_malformed_body_with_acks_one_stays_silent() {
    // `kafka_protocol` decodes Produce in one shot, so a decode failure never exposes `acks`
    // (unlike the pre-migration field-by-field decoder, which could still answer with
    // INVALID_REQUEST once it knew acks was nonzero). Every Produce decode failure now stays
    // silent rather than risk desyncing an acks=0 fire-and-forget client's correlation stream.
    let body = Bytes::from_static(&[
        0xff, 0xff, // null transactional_id
        0x00, 0x01, // acks = 1
        0x00, 0x00, 0x03, 0xe8, // timeout_ms
        0x00, 0x00, 0x00, 0x01, // one topic
    ]);
    assert!(
        handle_request(API_KEY_PRODUCE, 3, body, &default_broker()).is_no_response(),
        "malformed Produce body must stay silent regardless of acks"
    );
}

#[test]
fn fetch_malformed_body_returns_invalid_request() {
    let response = handle_request(
        API_KEY_FETCH,
        12,
        Bytes::from_static(&[
            0xff, 0xff, 0xff, 0xff, // replica_id
            0x00, 0x00, 0x00, 0x64, // max_wait_ms
            0x00, 0x00, 0x00, 0x01, // min_bytes
        ]),
        &default_broker(),
    )
    .expect_response("fetch must return error response");
    let mut d = Decoder::new(response);
    assert_eq!(d.read_i32().unwrap(), 0); // throttle
    assert_eq!(d.read_i16().unwrap(), ERROR_INVALID_REQUEST);
}

#[test]
fn list_offsets_malformed_body_returns_invalid_request() {
    let response = handle_request(
        API_KEY_LIST_OFFSETS,
        6,
        Bytes::from_static(&[
            0xff, 0xff, 0xff, 0xff, // replica_id
            0x01, // isolation_level
            0x02, // compact topics count = one
            0x00, // null topic name
        ]),
        &default_broker(),
    )
    .expect_response("list offsets must return error response");
    let mut d = Decoder::new(response);
    assert_eq!(d.read_i32().unwrap(), 0); // throttle
    assert_eq!(d.read_varint().unwrap(), 2);
    assert_eq!(
        d.read_compact_nullable_string().unwrap(),
        Some(String::new())
    );
    assert_eq!(d.read_varint().unwrap(), 2);
    assert_eq!(d.read_i32().unwrap(), 0);
    assert_eq!(d.read_i16().unwrap(), ERROR_INVALID_REQUEST);
}

#[test]
fn create_topics_malformed_body_returns_invalid_request() {
    let response = handle_request(
        API_KEY_CREATE_TOPICS,
        5,
        Bytes::from_static(&[
            0x02, // compact topics count = one
            0x00, // null compact topic name
        ]),
        &default_broker(),
    )
    .expect_response("create topics must return error response");
    let mut d = Decoder::new(response);
    assert_eq!(d.read_i32().unwrap(), 0);
    assert_eq!(d.read_varint().unwrap(), 2);
    assert_eq!(
        d.read_compact_nullable_string().unwrap(),
        Some(String::new())
    );
    assert_eq!(d.read_i16().unwrap(), ERROR_INVALID_REQUEST);
}

#[test]
fn metadata_null_topic_name_closes_connection() {
    assert!(
        handle_request(
            API_KEY_METADATA,
            0,
            Bytes::from_static(&[
                0x00, 0x00, 0x00, 0x01, // one topic
                0xff, 0xff, // null topic name
            ]),
            &default_broker(),
        )
        .is_close(),
        "null topic name cannot be echoed in a Metadata response"
    );
}

// ── Full handler regression - every scoped API key x version through `handle_request` ──

#[test]
fn handle_request_succeeds_for_every_supported_version_with_fixture() {
    for &(api_key, name, min_ver, max_ver) in scope::SCOPED_API_KEYS {
        if api_key == API_KEY_METADATA {
            for version in min_ver..=max_ver {
                let body = if version >= 9 {
                    wire::build_metadata_all_topics_flexible(version)
                } else {
                    build_metadata_all_topics_legacy(version)
                };
                let resp = handle_request(api_key, version, body, &default_broker())
                    .expect_response("test request has acks != 0 and expects a response");
                assert!(
                    !resp.is_empty(),
                    "{name} v{version} returned empty response"
                );
            }
            continue;
        }
        if api_key == API_KEY_API_VERSIONS {
            for version in min_ver..=max_ver {
                let body = if version >= 3 {
                    build_api_versions_flexible_request("iggy-test", "0.1.0")
                } else {
                    Bytes::new()
                };
                let resp = handle_request(api_key, version, body, &default_broker())
                    .expect_response("test request has acks != 0 and expects a response");
                assert!(
                    !resp.is_empty(),
                    "{name} v{version} returned empty response"
                );
            }
            continue;
        }

        for version in min_ver..=max_ver {
            let Some(body) = load_fixture_body_or_skip(api_key, name, version) else {
                continue;
            };
            let resp = handle_request(api_key, version, body, &default_broker())
                .expect_response("test request has acks != 0 and expects a response");
            assert!(
                !resp.is_empty(),
                "{name} v{version} returned empty response"
            );
        }
    }
}

#[test]
fn produce_stub_response_returns_retriable_not_leader() {
    for version in 3i16..=9 {
        let Some(body) = load_fixture_body_or_skip(0, "Produce", version) else {
            continue;
        };
        let resp = handle_request(API_KEY_PRODUCE, version, body, &default_broker())
            .expect_response("test request has acks != 0 and expects a response");
        let flexible = version >= 9;
        let mut d = Decoder::new(resp);
        if flexible {
            let _topics = d.read_varint().unwrap();
            let _topic = d.read_compact_nullable_string().unwrap();
            let _parts = d.read_varint().unwrap();
        } else {
            let _topics = d.read_i32().unwrap();
            let _topic = d.read_nullable_string().unwrap();
            let _parts = d.read_i32().unwrap();
        }
        let _partition = d.read_i32().unwrap();
        assert_eq!(
            d.read_i16().unwrap(),
            ERROR_NOT_LEADER_OR_FOLLOWER,
            "Produce v{version}"
        );
    }
}

#[test]
fn fetch_stub_response_returns_retriable_not_leader() {
    for version in 4i16..=12 {
        let Some(body) = load_fixture_body_or_skip(1, "Fetch", version) else {
            continue;
        };
        let resp = handle_request(API_KEY_FETCH, version, body, &default_broker())
            .expect_response("test request has acks != 0 and expects a response");
        let flexible = version >= 12;
        let mut d = Decoder::new(resp);
        if version >= 1 {
            let _throttle = d.read_i32().unwrap();
        }
        if version >= 7 {
            assert_eq!(d.read_i16().unwrap(), ERROR_NONE);
            let _session = d.read_i32().unwrap();
        }
        if flexible {
            let _topics = d.read_varint().unwrap();
            let _topic = d.read_compact_nullable_string().unwrap();
            let _parts = d.read_varint().unwrap();
        } else {
            let _topics = d.read_i32().unwrap();
            let _topic = d.read_nullable_string().unwrap();
            let _parts = d.read_i32().unwrap();
        }
        let _partition = d.read_i32().unwrap();
        assert_eq!(
            d.read_i16().unwrap(),
            ERROR_NOT_LEADER_OR_FOLLOWER,
            "Fetch v{version} partition error"
        );
    }
}

/// Exercises the optional Fetch fields (`forgotten_topics`, `rack_id`) that
/// `fetch_stub_response_returns_retriable_not_leader`'s fixture-derived bodies leave empty:
/// v9 legacy encoding with a populated forgotten-topics list, and v12 flexible encoding adding a
/// rack id (introduced at v11) on top of that. Both must decode and reach the same stub outcome
/// as an empty-topics request.
#[test]
fn fetch_decodes_request_with_forgotten_topics_and_rack_id() {
    for (version, rack_id) in [(9i16, None), (12i16, Some("rack-a"))] {
        let body =
            wire::build_fetch_request_with_sections(version, "orders", 2, Some("stale"), rack_id);
        let resp = handle_request(API_KEY_FETCH, version, body, &default_broker())
            .expect_response("test request has acks != 0 and expects a response");
        let flexible = version >= 12;
        let mut d = Decoder::new(resp);
        let _throttle = d.read_i32().unwrap();
        assert_eq!(d.read_i16().unwrap(), ERROR_NONE, "Fetch v{version}");
        let _session = d.read_i32().unwrap();
        if flexible {
            let _topics = d.read_varint().unwrap();
            let _topic = d.read_compact_nullable_string().unwrap();
            let _parts = d.read_varint().unwrap();
        } else {
            let _topics = d.read_i32().unwrap();
            let _topic = d.read_nullable_string().unwrap();
            let _parts = d.read_i32().unwrap();
        }
        let _partition = d.read_i32().unwrap();
        assert_eq!(
            d.read_i16().unwrap(),
            ERROR_NOT_LEADER_OR_FOLLOWER,
            "Fetch v{version} partition error"
        );
    }
}

#[test]
fn list_offsets_stub_response_returns_retriable_not_leader() {
    for version in 1i16..=6 {
        let Some(body) = load_fixture_body_or_skip(2, "ListOffsets", version) else {
            continue;
        };
        let resp = handle_request(API_KEY_LIST_OFFSETS, version, body, &default_broker())
            .expect_response("test request has acks != 0 and expects a response");
        let flexible = version >= 6;
        let mut d = Decoder::new(resp);
        if version >= 2 {
            let _throttle = d.read_i32().unwrap();
        }
        if flexible {
            let _topics = d.read_varint().unwrap();
            let _topic = d.read_compact_nullable_string().unwrap();
            let _parts = d.read_varint().unwrap();
        } else {
            let _topics = d.read_i32().unwrap();
            let _topic = d.read_nullable_string().unwrap();
            let _parts = d.read_i32().unwrap();
        }
        let _partition = d.read_i32().unwrap();
        assert_eq!(
            d.read_i16().unwrap(),
            ERROR_NOT_LEADER_OR_FOLLOWER,
            "ListOffsets v{version}"
        );
    }
}

// ── Metadata regression - all supported versions, broker advertise, topic counts ──

/// Topic name assigned to slot `i` by [`metadata_request_legacy`] / [`metadata_request_flexible`].
fn synthetic_topic_name(i: i32) -> String {
    format!("topic-{i}")
}

fn metadata_request_legacy(version: i16, topic_count: i32) -> Bytes {
    let names: Vec<String> = (0..topic_count).map(synthetic_topic_name).collect();
    let refs: Vec<&str> = names.iter().map(String::as_str).collect();
    build_metadata_legacy_request_for_version(version, &refs)
}

fn metadata_request_flexible(topic_count: usize) -> Bytes {
    let names: Vec<String> = (0..topic_count)
        .map(|i| synthetic_topic_name(i32::try_from(i).unwrap()))
        .collect();
    let refs: Vec<&str> = names.iter().map(String::as_str).collect();
    build_metadata_flexible_request(&refs)
}

fn read_broker_legacy(d: &mut Decoder) -> (String, i32) {
    let count = d.read_i32().unwrap();
    assert_eq!(count, 1);
    let _node = d.read_i32().unwrap();
    let host = d.read_nullable_string().unwrap().unwrap();
    let port = d.read_i32().unwrap();
    (host, port)
}

fn read_broker_flexible(d: &mut Decoder) -> (String, i32) {
    let count_plus_one = d.read_varint().unwrap();
    assert_eq!(count_plus_one, 2); // one broker
    let _node = d.read_i32().unwrap();
    let host = d.read_compact_nullable_string().unwrap().unwrap();
    let port = d.read_i32().unwrap();
    let _rack = d.read_compact_nullable_string().unwrap();
    d.read_tagged_fields().unwrap();
    (host, port)
}

#[test]
fn metadata_corrupt_partial_body_closes_connection() {
    assert!(
        handle_request(
            API_KEY_METADATA,
            0,
            Bytes::from_static(&[0x00, 0x00]),
            &default_broker(),
        )
        .is_close(),
        "truncated Metadata body must close; there is no top-level error field"
    );
}

#[test]
fn metadata_v0_empty_topics_stub_broker() {
    let body = handle_request(
        API_KEY_METADATA,
        0,
        metadata_request_legacy(0, 0),
        &default_broker(),
    )
    .expect_response("test request has acks != 0 and expects a response");
    let mut d = Decoder::new(body);
    let (host, port) = read_broker_legacy(&mut d);
    assert_eq!(host, "127.0.0.1");
    assert_eq!(port, 9093);
    assert_eq!(d.read_i32().unwrap(), 0);
    assert_eq!(
        d.remaining(),
        0,
        "empty request must produce no trailing bytes"
    );
}

#[test]
fn metadata_v0_three_topics_each_unknown() {
    let body = handle_request(
        API_KEY_METADATA,
        0,
        metadata_request_legacy(0, 3),
        &default_broker(),
    )
    .expect_response("test request has acks != 0 and expects a response");
    let mut d = Decoder::new(body);
    let _ = read_broker_legacy(&mut d);
    assert_eq!(d.read_i32().unwrap(), 3);
    for i in 0..3 {
        assert_eq!(d.read_i16().unwrap(), ERROR_UNKNOWN_TOPIC_OR_PARTITION);
        assert_eq!(
            d.read_nullable_string().unwrap().unwrap(),
            synthetic_topic_name(i)
        );
        assert_eq!(d.read_i32().unwrap(), 0);
    }
}

#[test]
fn metadata_v1_includes_controller_id() {
    let body = handle_request(
        API_KEY_METADATA,
        1,
        metadata_request_legacy(1, 0),
        &default_broker(),
    )
    .expect_response("test request has acks != 0 and expects a response");
    let mut d = Decoder::new(body);
    // Metadata v1 has no throttle_time_ms (added in v3).
    let _ = read_broker_legacy(&mut d);
    let _rack = d.read_nullable_string().unwrap();
    let controller = d.read_i32().unwrap();
    assert_eq!(controller, 1);
}

#[test]
fn metadata_v2_includes_cluster_id_field() {
    let body = handle_request(
        API_KEY_METADATA,
        2,
        metadata_request_legacy(2, 0),
        &default_broker(),
    )
    .expect_response("test request has acks != 0 and expects a response");
    let mut d = Decoder::new(body);
    let _ = read_broker_legacy(&mut d);
    let _rack = d.read_nullable_string().unwrap();
    let _cluster_id = d.read_nullable_string().unwrap();
    let _controller = d.read_i32().unwrap();
    assert_eq!(d.read_i32().unwrap(), 0);
}

#[test]
fn metadata_all_legacy_versions_produce_valid_response() {
    for version in 0i16..=8 {
        let body = handle_request(
            API_KEY_METADATA,
            version,
            metadata_request_legacy(version, 1),
            &default_broker(),
        )
        .expect_response("test request has acks != 0 and expects a response");
        let mut d = Decoder::new(body);
        if version >= 3 {
            let _throttle = d.read_i32().unwrap();
        }
        let _ = read_broker_legacy(&mut d);
        if version >= 1 {
            let _rack = d.read_nullable_string().unwrap();
        }
        if version >= 2 {
            let _cluster = d.read_nullable_string().unwrap();
        }
        if version >= 1 {
            let _controller = d.read_i32().unwrap();
        }
        assert_eq!(d.read_i32().unwrap(), 1);
        assert_eq!(d.read_i16().unwrap(), ERROR_UNKNOWN_TOPIC_OR_PARTITION);
    }
}

#[test]
fn metadata_v9_flexible_encoding() {
    let body = handle_request(
        API_KEY_METADATA,
        9,
        metadata_request_flexible(2),
        &default_broker(),
    )
    .expect_response("test request has acks != 0 and expects a response");
    let mut d = Decoder::new(body);
    let _throttle = d.read_i32().unwrap();
    let (host, port) = read_broker_flexible(&mut d);
    assert_eq!(host, "127.0.0.1");
    assert_eq!(port, 9093);
    let _cluster = d.read_compact_nullable_string().unwrap();
    let controller = d.read_i32().unwrap();
    assert_eq!(controller, 1);

    let topics_plus_one = d.read_varint().unwrap();
    assert_eq!(topics_plus_one, 3); // 2 topics
    for i in 0..2 {
        assert_eq!(d.read_i16().unwrap(), ERROR_UNKNOWN_TOPIC_OR_PARTITION);
        assert_eq!(
            d.read_compact_nullable_string().unwrap().unwrap(),
            synthetic_topic_name(i)
        );
        let _internal = d.read_bool().unwrap();
        let parts_plus_one = d.read_varint().unwrap();
        assert_eq!(parts_plus_one, 1); // empty partitions
        assert_eq!(d.read_i32().unwrap(), i32::MIN); // topic_authorized_operations (v8+)
        d.read_tagged_fields().unwrap();
    }
    assert_eq!(d.read_i32().unwrap(), i32::MIN); // cluster_authorized_operations (v8+)
    d.read_tagged_fields().unwrap();
    assert_eq!(d.remaining(), 0);
}

#[test]
fn metadata_v8_includes_authorized_operations_legacy() {
    let body = handle_request(
        API_KEY_METADATA,
        8,
        metadata_request_legacy(8, 1),
        &default_broker(),
    )
    .expect_response("test request has acks != 0 and expects a response");
    let mut d = Decoder::new(body);
    let _throttle = d.read_i32().unwrap();
    let _ = read_broker_legacy(&mut d);
    let _rack = d.read_nullable_string().unwrap();
    let _cluster = d.read_nullable_string().unwrap();
    let _controller = d.read_i32().unwrap();
    assert_eq!(d.read_i32().unwrap(), 1);
    let _topic_error = d.read_i16().unwrap();
    let _topic = d.read_nullable_string().unwrap();
    let _internal = d.read_bool().unwrap();
    assert_eq!(d.read_i32().unwrap(), 0); // empty partitions
    assert_eq!(d.read_i32().unwrap(), i32::MIN); // topic_authorized_operations
    assert_eq!(d.read_i32().unwrap(), i32::MIN); // cluster_authorized_operations
    assert_eq!(d.remaining(), 0);
}

#[test]
fn create_topics_stub_response_returns_not_controller() {
    for version in 2i16..=5 {
        let Some(body) = load_fixture_body_or_skip(19, "CreateTopics", version) else {
            continue;
        };
        let resp = handle_request(API_KEY_CREATE_TOPICS, version, body, &default_broker())
            .expect_response("test request has acks != 0 and expects a response");
        let flexible = version >= 5;
        let mut d = Decoder::new(resp);
        if version >= 2 {
            let _throttle = d.read_i32().unwrap();
        }
        if flexible {
            let _topics = d.read_varint().unwrap();
            let _topic = d.read_compact_nullable_string().unwrap();
        } else {
            let _topics = d.read_i32().unwrap();
            let _topic = d.read_nullable_string().unwrap();
        }
        assert_eq!(
            d.read_i16().unwrap(),
            ERROR_NOT_CONTROLLER,
            "CreateTopics v{version}"
        );
    }
}

// ── Produce acks=0 (broker must stay silent even on a malformed body) ──────

#[test]
fn produce_acks_zero_malformed_body_stays_silent() {
    // topics array declares 1 element but no topic data follows - decode fails after acks=0
    // was on the wire, though `kafka_protocol`'s one-shot decode no longer exposes that acks
    // was read. The handler must stay silent regardless.
    let body = build_produce_v3_body(0, 1);
    assert!(
        handle_request(API_KEY_PRODUCE, 3, body, &default_broker()).is_no_response(),
        "handler must not respond when acks=0 even if decode fails after acks"
    );
}

#[test]
fn produce_acks_zero_stays_silent_on_advertised_but_unsupported_version() {
    // ApiVersions advertises Produce min=0 (KAFKA-18659) while the firewall's real floor is 3
    // (SUPPORTED_RANGES). A spec-compliant client can legitimately send v0-2 with acks=0; the
    // firewall must not run before acks is decoded, or this silence contract breaks for exactly
    // the versions ApiVersions told the client were fine to use.
    for version in 0i16..3 {
        let body = build_produce_legacy_request(version, 0, None, None);
        assert!(
            handle_request(API_KEY_PRODUCE, version, body, &default_broker()).is_no_response(),
            "Produce v{version} acks=0 must stay silent even though the firewall doesn't accept v{version}"
        );
    }
}

// ── Metadata topic name echo (must not hardcode a placeholder topic name) ──

fn read_metadata_v1_topics(d: &mut Decoder, expected_count: i32) -> Vec<String> {
    let _brokers_count = d.read_i32().unwrap();
    d.read_i32().unwrap(); // node_id
    d.read_nullable_string().unwrap(); // host
    d.read_i32().unwrap(); // port
    d.read_nullable_string().unwrap(); // rack (v1+)
    d.read_i32().unwrap(); // controller_id (v1+)

    assert_eq!(d.read_i32().unwrap(), expected_count);
    let mut names = Vec::with_capacity(usize::try_from(expected_count).unwrap_or(0));
    for _ in 0..expected_count {
        assert_eq!(
            d.read_i16().unwrap(),
            ERROR_UNKNOWN_TOPIC_OR_PARTITION,
            "stub has no topic catalog; unknown topic must surface error 3"
        );
        names.push(d.read_nullable_string().unwrap().expect("topic name"));
        d.read_bool().unwrap(); // is_internal (v1+)
        assert_eq!(d.read_i32().unwrap(), 0, "empty partitions array");
    }
    names
}

#[test]
fn metadata_v1_echoes_requested_topic_name_in_response() {
    let topic = "orders";
    let request = build_metadata_legacy_request(&[topic]);
    let body = handle_request(API_KEY_METADATA, 1, request, &default_broker())
        .expect_response("test request has acks != 0 and expects a response");
    let mut d = Decoder::new(body);

    // `read_metadata_v1_topics` asserts the per-topic error code (unknown-topic-or-partition, the
    // stub's only topic outcome) as it walks the response; the name check below is on top of that.
    let names = read_metadata_v1_topics(&mut d, 1);
    assert_eq!(
        names,
        vec![topic.to_string()],
        "response must echo requested topic name, not a placeholder"
    );
    assert_eq!(d.remaining(), 0);
}

// ── Metadata (spec + SCOPE.md coverage) ─────────────────────────────────────

#[test]
fn metadata_v0_empty_topics_returns_zero_length_topic_array() {
    let body = handle_request(
        API_KEY_METADATA,
        0,
        build_metadata_legacy_request(&[]),
        &default_broker(),
    )
    .expect_response("test request has acks != 0 and expects a response");
    let mut d = Decoder::new(body);
    let _brokers = d.read_i32().unwrap();
    d.read_i32().unwrap();
    d.read_nullable_string().unwrap();
    d.read_i32().unwrap();
    assert_eq!(d.read_i32().unwrap(), 0, "empty request → zero topics");
    assert_eq!(d.remaining(), 0);
}

#[test]
fn metadata_v3_includes_throttle_time_ms_before_brokers() {
    let body = handle_request(
        API_KEY_METADATA,
        3,
        build_metadata_legacy_request_for_version(3, &[]),
        &default_broker(),
    )
    .expect_response("metadata v3 empty topics must succeed");
    let mut d = Decoder::new(body);
    assert_eq!(d.read_i32().unwrap(), 0, "throttle_time_ms");
}

#[test]
fn metadata_v9_flexible_empty_topics_returns_zero_topics() {
    let body = handle_request(
        API_KEY_METADATA,
        9,
        build_metadata_flexible_request(&[]),
        &default_broker(),
    )
    .expect_response("test request has acks != 0 and expects a response");
    let mut d = Decoder::new(body);
    d.read_i32().unwrap(); // throttle
    let broker_count = usize::try_from(d.read_varint().unwrap())
        .unwrap()
        .saturating_sub(1);
    for _ in 0..broker_count {
        d.read_i32().unwrap();
        d.read_compact_nullable_string().unwrap();
        d.read_i32().unwrap();
        d.read_compact_nullable_string().unwrap();
        d.read_tagged_fields().unwrap();
    }
    d.read_compact_nullable_string().unwrap(); // cluster_id
    d.read_i32().unwrap(); // controller_id
    let topic_count = usize::try_from(d.read_varint().unwrap())
        .unwrap()
        .saturating_sub(1);
    assert_eq!(topic_count, 0);
}

#[test]
fn metadata_v9_flexible_echoes_each_requested_topic_name() {
    let topics = ["orders", "payments", "inventory"];
    let body = handle_request(
        API_KEY_METADATA,
        9,
        build_metadata_flexible_request(&topics),
        &default_broker(),
    )
    .expect_response("test request has acks != 0 and expects a response");

    let mut d = Decoder::new(body);
    d.read_i32().unwrap();
    let broker_count = usize::try_from(d.read_varint().unwrap())
        .unwrap()
        .saturating_sub(1);
    for _ in 0..broker_count {
        d.read_i32().unwrap();
        d.read_compact_nullable_string().unwrap();
        d.read_i32().unwrap();
        d.read_compact_nullable_string().unwrap();
        d.read_tagged_fields().unwrap();
    }
    d.read_compact_nullable_string().unwrap();
    d.read_i32().unwrap();

    let topic_count = usize::try_from(d.read_varint().unwrap())
        .unwrap()
        .saturating_sub(1);
    assert_eq!(topic_count, topics.len());

    let mut names = Vec::new();
    for _ in 0..topic_count {
        assert_eq!(
            d.read_i16().unwrap(),
            ERROR_UNKNOWN_TOPIC_OR_PARTITION,
            "stub gateway returns unknown topic error per topic"
        );
        names.push(
            d.read_compact_nullable_string()
                .unwrap()
                .expect("topic name"),
        );
        d.read_bool().unwrap();
        assert_eq!(
            usize::try_from(d.read_varint().unwrap())
                .unwrap()
                .saturating_sub(1),
            0,
            "empty partitions array"
        );
        d.read_i32().unwrap(); // topic_authorized_operations (v8+)
        d.read_tagged_fields().unwrap();
    }

    assert_eq!(
        names,
        topics
            .iter()
            .map(|topic| (*topic).to_string())
            .collect::<Vec<_>>(),
        "metadata must echo requested topic names for client matching"
    );
}

#[test]
fn metadata_v1_legacy_multiple_topics_echo_names() {
    let topics = ["alpha", "beta"];
    let body = handle_request(
        API_KEY_METADATA,
        1,
        build_metadata_legacy_request(&topics),
        &default_broker(),
    )
    .expect_response("test request has acks != 0 and expects a response");

    let mut d = Decoder::new(body);
    d.read_i32().unwrap();
    d.read_i32().unwrap();
    d.read_nullable_string().unwrap();
    d.read_i32().unwrap();
    d.read_nullable_string().unwrap();
    d.read_i32().unwrap();
    assert_eq!(d.read_i32().unwrap(), 2);

    for expected in topics {
        assert_eq!(d.read_i16().unwrap(), ERROR_UNKNOWN_TOPIC_OR_PARTITION);
        assert_eq!(
            d.read_nullable_string().unwrap().as_deref(),
            Some(expected),
            "metadata v1 must echo {expected}"
        );
        d.read_bool().unwrap();
        assert_eq!(d.read_i32().unwrap(), 0);
    }
}

// `metadata_v9_flexible_echoes_each_requested_topic_name` above already sends a 3-topic v9
// request through this exact decode path and asserts response topic_count == request topic
// count, plus per-topic name/error-code/partition-count - a strict superset of what a
// response-slot-count-only test here would add.
