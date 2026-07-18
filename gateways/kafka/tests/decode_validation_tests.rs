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

//! Validates request decoders and response encoders against the binary fixtures
//! produced by tools/kafka-tool.
//!
//! Frame layout written by kafka-tool (all versions):
//!   [4-byte length prefix]
//!   [`api_key` i16][`api_version` i16][`correlation_id` i32]
//!   header v1: [`client_id`] `NULLABLE_STRING`
//!   header v2: [`client_id`] `COMPACT_NULLABLE_STRING` + request-header tagged fields
//!   [request body]                             ← properly encoded per spec (flexible or not)

use std::path::PathBuf;

use bytes::Bytes;

use iggy_gateway_kafka::protocol::codec::Decoder;
use iggy_gateway_kafka::protocol::header::{RequestHeader, request_header_version};
use iggy_gateway_kafka::protocol::requests::{
    decode_create_topics_request, decode_fetch_request, decode_list_offsets_request,
    decode_produce_request,
};
use iggy_gateway_kafka::protocol::responses::{
    encode_create_topics_response, encode_fetch_response, encode_list_offsets_response,
    encode_produce_response,
};

#[path = "common/wire.rs"]
mod wire;

// ── helpers ───────────────────────────────────────────────────────────────────

fn fixtures_dir() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("tools/kafka-tool/kafka_messages")
}

/// Load a kafka-tool `.bin` file and return just the request body bytes, or `None`
/// (after a standardized skip note) when the gitignored fixture is absent, so a fresh
/// clone skips rather than panicking.
fn load_body(api_key: i16, api_name: &str, version: i16) -> Option<Bytes> {
    let filename = format!("{api_key:03}_{api_name}_v{version}.bin");
    let path = fixtures_dir().join(&filename);
    let Ok(data) = std::fs::read(&path) else {
        eprintln!(
            "skipping {filename}: wire fixture missing — generate with \
             `gateways/kafka/scripts/ci-wire-fixtures.sh generate` (or the kafka-tool \
             `generate` subcommand)"
        );
        return None;
    };

    let frame = Bytes::copy_from_slice(&data[4..]);
    let hdr_ver = request_header_version(api_key, version);
    let mut decoder = Decoder::new(frame);
    RequestHeader::decode_from(&mut decoder, hdr_ver).expect("fixture request header must decode");
    Some(
        decoder
            .read_bytes(decoder.remaining())
            .expect("fixture request body must decode"),
    )
}

// ── Produce (API key 0) ───────────────────────────────────────────────────────

#[test]
fn produce_all_supported_versions_decode() {
    for version in 3i16..=9 {
        let Some(body) = load_body(0, "Produce", version) else {
            continue;
        };
        let req = decode_produce_request(version, body)
            .into_request()
            .unwrap_or_else(|e| panic!("Produce v{version} decode failed: {e}"));

        assert_eq!(req.acks, -1, "Produce v{version}: unexpected acks");
        assert_eq!(
            req.timeout_ms, 5000,
            "Produce v{version}: unexpected timeout_ms"
        );
        assert_eq!(req.topics.len(), 1, "Produce v{version}: expected 1 topic");
        assert_eq!(
            req.topics[0].topic, "test-topic",
            "Produce v{version}: wrong topic name"
        );
        assert_eq!(
            req.topics[0].partitions.len(),
            1,
            "Produce v{version}: expected 1 partition"
        );
        assert_eq!(
            req.topics[0].partitions[0].partition, 0,
            "Produce v{version}: wrong partition index"
        );
        assert!(
            req.topics[0].partitions[0].records.is_some(),
            "Produce v{version}: records should be present"
        );
    }
}

#[test]
fn produce_response_encodes_for_all_supported_versions() {
    for version in 3i16..=9 {
        let Some(body) = load_body(0, "Produce", version) else {
            continue;
        };
        let req = decode_produce_request(version, body)
            .into_request()
            .unwrap_or_else(|e| panic!("Produce v{version} decode failed: {e}"));
        let resp = encode_produce_response(version, &req);
        assert!(
            !resp.is_empty(),
            "Produce v{version}: response must not be empty"
        );
    }
}

#[test]
fn produce_response_v3_roundtrip() {
    use iggy_gateway_kafka::protocol::codec::Decoder;
    let Some(body) = load_body(0, "Produce", 3) else {
        return;
    };
    let req = decode_produce_request(3, body).into_request().unwrap();
    let resp = encode_produce_response(3, &req);

    let mut d = Decoder::new(resp);
    let topic_count = d.read_i32().unwrap();
    assert_eq!(topic_count, 1);
    let topic_name = d.read_nullable_string().unwrap().unwrap();
    assert_eq!(topic_name, "test-topic");
    let partition_count = d.read_i32().unwrap();
    assert_eq!(partition_count, 1);
    let partition = d.read_i32().unwrap();
    assert_eq!(partition, 0);
    let error_code = d.read_i16().unwrap();
    assert_eq!(error_code, 6); // NOT_LEADER_OR_FOLLOWER — stub until Iggy bridge
    let base_offset = d.read_i64().unwrap();
    assert_eq!(base_offset, 0);
    // log_append_time_ms (v2+)
    let _log_append = d.read_i64().unwrap();
    // log_start_offset (v5+) — not present for v3
    let throttle = d.read_i32().unwrap();
    assert_eq!(throttle, 0);
}

#[test]
fn produce_response_v8_includes_record_errors() {
    use iggy_gateway_kafka::protocol::codec::Decoder;
    let Some(body) = load_body(0, "Produce", 8) else {
        return;
    };
    let req = decode_produce_request(8, body).into_request().unwrap();
    let resp = encode_produce_response(8, &req);

    let mut d = Decoder::new(resp);
    let topic_count = d.read_i32().unwrap();
    assert_eq!(topic_count, 1);
    let _topic_name = d.read_nullable_string().unwrap();
    let partition_count = d.read_i32().unwrap();
    assert_eq!(partition_count, 1);
    let _partition = d.read_i32().unwrap();
    let error_code = d.read_i16().unwrap();
    assert_eq!(error_code, 6); // NOT_LEADER_OR_FOLLOWER — stub until Iggy bridge
    let _base_offset = d.read_i64().unwrap();
    let _log_append_time = d.read_i64().unwrap(); // v2+
    let _log_start_offset = d.read_i64().unwrap(); // v5+
    let record_errors_count = d.read_i32().unwrap(); // v8+: should be 0
    assert_eq!(
        record_errors_count, 0,
        "v8 must emit empty record_errors array"
    );
    let error_message = d.read_nullable_string().unwrap(); // v8+: should be null
    assert!(error_message.is_none(), "v8 error_message must be null");
}

#[test]
fn produce_v9_flexible_empty_topics_decode() {
    let req = decode_produce_request(9, wire::build_produce_flexible_empty_request(0))
        .into_request()
        .expect("flexible produce request should decode");
    assert_eq!(req.acks, 0);
    assert_eq!(req.topics.len(), 0);
}

// ── Fetch (API key 1) ─────────────────────────────────────────────────────────

#[test]
fn fetch_all_supported_versions_decode() {
    for version in 4i16..=12 {
        let Some(body) = load_body(1, "Fetch", version) else {
            continue;
        };
        let req = decode_fetch_request(version, body)
            .unwrap_or_else(|e| panic!("Fetch v{version} decode failed: {e}"));

        assert_eq!(
            req.max_wait_ms, 500,
            "Fetch v{version}: unexpected max_wait_ms"
        );
        assert_eq!(req.min_bytes, 1, "Fetch v{version}: unexpected min_bytes");
        assert_eq!(req.topics.len(), 1, "Fetch v{version}: expected 1 topic");
        assert_eq!(
            req.topics[0].topic, "test-topic",
            "Fetch v{version}: wrong topic name"
        );
        assert_eq!(
            req.topics[0].partitions.len(),
            1,
            "Fetch v{version}: expected 1 partition"
        );
        assert_eq!(
            req.topics[0].partitions[0].partition, 0,
            "Fetch v{version}: wrong partition index"
        );
        assert_eq!(
            req.topics[0].partitions[0].fetch_offset, 0,
            "Fetch v{version}: wrong fetch_offset"
        );
    }
}

#[test]
fn fetch_response_encodes_for_all_supported_versions() {
    for version in 4i16..=12 {
        let Some(body) = load_body(1, "Fetch", version) else {
            continue;
        };
        let req = decode_fetch_request(version, body)
            .unwrap_or_else(|e| panic!("Fetch v{version} decode failed: {e}"));
        let resp = encode_fetch_response(version, &req);
        assert!(
            !resp.is_empty(),
            "Fetch v{version}: response must not be empty"
        );
    }
}

#[test]
fn fetch_response_v7_roundtrip() {
    use iggy_gateway_kafka::protocol::codec::Decoder;
    let Some(body) = load_body(1, "Fetch", 7) else {
        return;
    };
    let req = decode_fetch_request(7, body).unwrap();
    let resp = encode_fetch_response(7, &req);

    let mut d = Decoder::new(resp);
    let throttle_ms = d.read_i32().unwrap(); // v1+
    assert_eq!(throttle_ms, 0);
    let error_code = d.read_i16().unwrap(); // v7+
    assert_eq!(error_code, 0);
    let session_id = d.read_i32().unwrap(); // v7+
    assert_eq!(session_id, 0);
    let topic_count = d.read_i32().unwrap();
    assert_eq!(topic_count, 1);
    let topic_name = d.read_nullable_string().unwrap().unwrap();
    assert_eq!(topic_name, "test-topic");
    let partition_count = d.read_i32().unwrap();
    assert_eq!(partition_count, 1);
    let partition = d.read_i32().unwrap();
    assert_eq!(partition, 0);
    let partition_error = d.read_i16().unwrap();
    assert_eq!(partition_error, 0);
    let high_watermark = d.read_i64().unwrap();
    assert_eq!(high_watermark, 0);
}

#[test]
fn fetch_v12_decodes_forgotten_topics_and_rack_id_sections() {
    let req = decode_fetch_request(
        12,
        wire::build_fetch_request_with_sections(12, "test-topic", 2, Some("forgotten"), Some("r1")),
    )
    .expect("fetch request should decode");
    assert_eq!(req.max_wait_ms, 100);
    assert_eq!(req.min_bytes, 1);
    assert_eq!(req.max_bytes, i32::MAX);
    assert_eq!(req.isolation_level, 0);
    assert_eq!(req.topics.len(), 1);
    assert_eq!(req.topics[0].topic, "test-topic");
    assert_eq!(req.topics[0].partitions.len(), 1);
    assert_eq!(req.topics[0].partitions[0].partition, 2);
    assert_eq!(req.topics[0].partitions[0].fetch_offset, 42);
    assert_eq!(req.topics[0].partitions[0].partition_max_bytes, 1024);
}

// ── ListOffsets (API key 2) ───────────────────────────────────────────────────

#[test]
fn list_offsets_all_supported_versions_decode() {
    for version in 1i16..=6 {
        let Some(body) = load_body(2, "ListOffsets", version) else {
            continue;
        };
        let req = decode_list_offsets_request(version, body)
            .unwrap_or_else(|e| panic!("ListOffsets v{version} decode failed: {e}"));

        assert_eq!(
            req.topics.len(),
            1,
            "ListOffsets v{version}: expected 1 topic"
        );
        assert_eq!(
            req.topics[0].topic, "test-topic",
            "ListOffsets v{version}: wrong topic name"
        );
        assert_eq!(
            req.topics[0].partitions.len(),
            1,
            "ListOffsets v{version}: expected 1 partition"
        );
        assert_eq!(
            req.topics[0].partitions[0].partition, 0,
            "ListOffsets v{version}: wrong partition index"
        );
    }
}

#[test]
fn list_offsets_v0_decodes_legacy_max_num_offsets_branch() {
    let req =
        decode_list_offsets_request(0, wire::build_list_offsets_branch_request(0, "legacy", 4))
            .expect("v0 list offsets should decode");
    assert_eq!(req.isolation_level, 0);
    assert_eq!(req.topics.len(), 1);
    assert_eq!(req.topics[0].topic, "legacy");
    assert_eq!(req.topics[0].partitions[0].partition, 4);
    assert_eq!(req.topics[0].partitions[0].timestamp, -2);
}

#[test]
fn list_offsets_v6_decodes_flexible_leader_epoch_branch() {
    let req = decode_list_offsets_request(6, wire::build_list_offsets_branch_request(6, "flex", 5))
        .expect("v6 list offsets should decode");
    assert_eq!(req.isolation_level, 1);
    assert_eq!(req.topics[0].topic, "flex");
    assert_eq!(req.topics[0].partitions[0].partition, 5);
    assert_eq!(req.topics[0].partitions[0].timestamp, -2);
}

#[test]
fn list_offsets_response_encodes_for_all_supported_versions() {
    for version in 1i16..=6 {
        let Some(body) = load_body(2, "ListOffsets", version) else {
            continue;
        };
        let req = decode_list_offsets_request(version, body)
            .unwrap_or_else(|e| panic!("ListOffsets v{version} decode failed: {e}"));
        let resp = encode_list_offsets_response(version, &req);
        assert!(
            !resp.is_empty(),
            "ListOffsets v{version}: response must not be empty"
        );
    }
}

#[test]
fn list_offsets_response_v1_no_leader_epoch() {
    use iggy_gateway_kafka::protocol::codec::Decoder;
    let Some(body) = load_body(2, "ListOffsets", 1) else {
        return;
    };
    let req = decode_list_offsets_request(1, body).unwrap();
    let resp = encode_list_offsets_response(1, &req);

    let mut d = Decoder::new(resp);
    // v1: no throttle_time_ms
    let topic_count = d.read_i32().unwrap();
    assert_eq!(topic_count, 1);
    let _topic_name = d.read_nullable_string().unwrap();
    let partition_count = d.read_i32().unwrap();
    assert_eq!(partition_count, 1);
    let _partition = d.read_i32().unwrap();
    let error_code = d.read_i16().unwrap();
    assert_eq!(error_code, 0);
    let _timestamp = d.read_i64().unwrap(); // v1+
    let _offset = d.read_i64().unwrap();
    // v1 must NOT have a leader_epoch field — assert all bytes consumed
    assert_eq!(
        d.remaining(),
        0,
        "v1 response must have no trailing bytes (leader_epoch must NOT be written)"
    );
}

#[test]
fn list_offsets_response_v4_has_leader_epoch() {
    use iggy_gateway_kafka::protocol::codec::Decoder;
    let Some(body) = load_body(2, "ListOffsets", 4) else {
        return;
    };
    let req = decode_list_offsets_request(4, body).unwrap();
    let resp = encode_list_offsets_response(4, &req);

    let mut d = Decoder::new(resp);
    let _throttle = d.read_i32().unwrap(); // v2+
    let topic_count = d.read_i32().unwrap();
    assert_eq!(topic_count, 1);
    let _topic_name = d.read_nullable_string().unwrap();
    let partition_count = d.read_i32().unwrap();
    assert_eq!(partition_count, 1);
    let _partition = d.read_i32().unwrap();
    let error_code = d.read_i16().unwrap();
    assert_eq!(error_code, 0);
    let _timestamp = d.read_i64().unwrap();
    let _offset = d.read_i64().unwrap();
    let leader_epoch = d.read_i32().unwrap(); // v4+
    assert_eq!(leader_epoch, -1, "v4 must have leader_epoch = -1");
    assert_eq!(d.remaining(), 0);
}

// ── CreateTopics (API key 19) ─────────────────────────────────────────────────

#[test]
fn create_topics_all_supported_versions_decode() {
    for version in 2i16..=5 {
        let Some(body) = load_body(19, "CreateTopics", version) else {
            continue;
        };
        let req = decode_create_topics_request(version, body)
            .unwrap_or_else(|e| panic!("CreateTopics v{version} decode failed: {e}"));

        assert_eq!(
            req.topics.len(),
            1,
            "CreateTopics v{version}: expected 1 topic"
        );
        assert_eq!(
            req.topics[0].num_partitions, 1,
            "CreateTopics v{version}: wrong num_partitions"
        );
        assert_eq!(
            req.topics[0].replication_factor, 1,
            "CreateTopics v{version}: wrong replication_factor"
        );
        assert!(
            !req.topics[0].name.is_empty(),
            "CreateTopics v{version}: topic name must not be empty"
        );
        assert_eq!(
            req.timeout_ms, 30000,
            "CreateTopics v{version}: unexpected timeout_ms"
        );
    }
}

#[test]
fn create_topics_v0_defaults_validate_only_to_false() {
    let req = decode_create_topics_request(
        0,
        wire::build_create_topics_request_with_sections(0, "legacy-topic"),
    )
    .expect("create topics v0 should decode");
    assert_eq!(req.timeout_ms, 5_000);
    assert!(!req.validate_only);
    assert_eq!(req.topics.len(), 1);
    assert_eq!(req.topics[0].name, "legacy-topic");
    assert_eq!(req.topics[0].num_partitions, 3);
    assert_eq!(req.topics[0].replication_factor, 1);
}

#[test]
fn create_topics_v5_decodes_flexible_assignments_and_configs() {
    let req = decode_create_topics_request(
        5,
        wire::build_create_topics_request_with_sections(5, "flex-topic"),
    )
    .expect("create topics v5 should decode");
    assert_eq!(req.timeout_ms, 5_000);
    assert!(req.validate_only);
    assert_eq!(req.topics.len(), 1);
    assert_eq!(req.topics[0].name, "flex-topic");
    assert_eq!(req.topics[0].num_partitions, 3);
    assert_eq!(req.topics[0].replication_factor, 1);
}

#[test]
fn create_topics_response_encodes_for_all_supported_versions() {
    for version in 2i16..=5 {
        let Some(body) = load_body(19, "CreateTopics", version) else {
            continue;
        };
        let req = decode_create_topics_request(version, body)
            .unwrap_or_else(|e| panic!("CreateTopics v{version} decode failed: {e}"));
        let resp = encode_create_topics_response(version, &req);
        assert!(
            !resp.is_empty(),
            "CreateTopics v{version}: response must not be empty"
        );
    }
}

#[test]
fn create_topics_response_v2_roundtrip() {
    use iggy_gateway_kafka::protocol::codec::Decoder;
    let Some(body) = load_body(19, "CreateTopics", 2) else {
        return;
    };
    let req = decode_create_topics_request(2, body).unwrap();
    let topic_name = req.topics[0].name.clone();
    let resp = encode_create_topics_response(2, &req);

    let mut d = Decoder::new(resp);
    let _throttle = d.read_i32().unwrap(); // v2+
    let topic_count = d.read_i32().unwrap();
    assert_eq!(topic_count, 1);
    let resp_topic = d.read_nullable_string().unwrap().unwrap();
    assert_eq!(resp_topic, topic_name);
    let error_code = d.read_i16().unwrap();
    assert_eq!(error_code, 41); // NOT_CONTROLLER — stub until Iggy bridge
    let error_msg = d.read_nullable_string().unwrap(); // v1+
    assert!(error_msg.is_none());
    assert_eq!(d.remaining(), 0);
}

#[test]
fn create_topics_response_v5_roundtrip() {
    use iggy_gateway_kafka::protocol::codec::Decoder;
    let Some(body) = load_body(19, "CreateTopics", 5) else {
        return;
    };
    let req = decode_create_topics_request(5, body).unwrap();
    let resp = encode_create_topics_response(5, &req);

    let mut d = Decoder::new(resp);
    let _throttle = d.read_i32().unwrap(); // v2+
    let topic_count_plus_one = d.read_varint().unwrap(); // flexible compact array
    assert_eq!(topic_count_plus_one, 2); // 1 topic → varint = 2

    let _topic_name = d.read_compact_nullable_string().unwrap();
    let error_code = d.read_i16().unwrap();
    assert_eq!(error_code, 41); // NOT_CONTROLLER — stub until Iggy bridge
    let _error_msg = d.read_compact_nullable_string().unwrap(); // v1+
    let num_partitions = d.read_i32().unwrap();
    assert_eq!(num_partitions, 1);
    let replication_factor = d.read_i16().unwrap();
    assert_eq!(replication_factor, 1);
    let configs_count_plus_one = d.read_varint().unwrap(); // empty compact array
    assert_eq!(configs_count_plus_one, 1); // empty = varint(1)
    d.read_tagged_fields().unwrap(); // per-entry tagged_fields
    d.read_tagged_fields().unwrap(); // top-level tagged_fields
    assert_eq!(d.remaining(), 0);
}
