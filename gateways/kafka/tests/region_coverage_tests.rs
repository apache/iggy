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

//! Decoder and listener paths that exist only for region coverage of version branches.

use std::time::Duration;

use bytes::BytesMut;
use iggy_gateway_kafka::error::KafkaProtocolError;
use iggy_gateway_kafka::protocol::codec::Encoder;
use iggy_gateway_kafka::protocol::requests::{
    ProduceDecodeResult, decode_create_topics_request, decode_fetch_request,
    decode_list_offsets_request, decode_produce_request,
};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;
use tokio::time;

#[path = "common/server.rs"]
mod server;
#[path = "common/tcp.rs"]
mod tcp;
#[path = "common/wire.rs"]
mod wire;

use tcp::{ByteRead, build_request_frame, read_byte_with_timeout, read_response_frame};

// ── Produce version branches ──────────────────────────────────────────────────

#[test]
fn produce_v2_skips_transactional_id_branch() {
    let req = decode_produce_request(2, wire::build_produce_legacy_request(2, 1, None, None))
        .into_request()
        .expect("produce v2 should decode");
    assert_eq!(req.acks, 1);
    assert!(req.transactional_id.is_none());
    assert!(req.topics.is_empty());
}

#[test]
fn produce_v3_legacy_transactional_id_and_topic_decode() {
    let req = decode_produce_request(
        3,
        wire::build_produce_legacy_request(3, -1, Some("txn-1"), Some("legacy-topic")),
    )
    .into_request()
    .expect("produce v3 legacy should decode");
    assert_eq!(req.transactional_id.as_deref(), Some("txn-1"));
    assert_eq!(req.topics.len(), 1);
    assert_eq!(req.topics[0].topic, "legacy-topic");
    assert!(req.topics[0].partitions[0].records.is_some());
}

#[test]
fn produce_v8_legacy_null_records_decode() {
    let mut enc = Encoder::with_capacity(64);
    enc.write_nullable_string(None::<&str>).unwrap();
    enc.write_i16(1);
    enc.write_i32(500);
    enc.write_i32(1);
    enc.write_nullable_string(Some("topic")).unwrap();
    enc.write_i32(1);
    enc.write_i32(0);
    enc.write_nullable_bytes(None).unwrap();

    let req = decode_produce_request(8, enc.freeze())
        .into_request()
        .expect("produce v8 with null records should decode");
    assert!(req.topics[0].partitions[0].records.is_none());
}

#[test]
fn produce_null_topic_name_preserves_acks_on_error() {
    let mut enc = Encoder::with_capacity(32);
    enc.write_nullable_string(None::<&str>).unwrap();
    enc.write_i16(1);
    enc.write_i32(500);
    enc.write_i32(1);
    enc.write_nullable_string(None::<&str>).unwrap();

    match decode_produce_request(3, enc.freeze()) {
        ProduceDecodeResult::Err { acks, error } => {
            assert_eq!(acks, Some(1));
            assert!(matches!(error, KafkaProtocolError::NullTopicName));
        }
        ProduceDecodeResult::Ok(_) => panic!("expected NullTopicName"),
    }
}

#[test]
fn produce_v9_flexible_transactional_id_and_tagged_fields_decode() {
    let req = decode_produce_request(
        9,
        wire::build_produce_flexible_request_with_topic("flex-topic"),
    )
    .into_request()
    .expect("produce v9 flexible should decode");
    assert_eq!(req.transactional_id.as_deref(), Some("txn-1"));
    assert_eq!(req.topics[0].topic, "flex-topic");
    assert!(req.topics[0].partitions[0].records.is_some());
}

#[test]
fn produce_v3_error_before_acks_has_none_acks() {
    let mut enc = Encoder::with_capacity(8);
    enc.write_i16(1);
    match decode_produce_request(3, enc.freeze()) {
        ProduceDecodeResult::Err { acks, .. } => assert_eq!(acks, None),
        ProduceDecodeResult::Ok(_) => panic!("expected decode error before acks"),
    }
}

#[test]
fn produce_v3_error_after_acks_preserves_acks() {
    let mut enc = Encoder::with_capacity(16);
    enc.write_nullable_string(None::<&str>).unwrap();
    enc.write_i16(7);
    match decode_produce_request(3, enc.freeze()) {
        ProduceDecodeResult::Err { acks, .. } => assert_eq!(acks, Some(7)),
        ProduceDecodeResult::Ok(_) => panic!("expected decode error after acks"),
    }
}

#[test]
fn produce_v3_error_after_timeout_preserves_acks() {
    let mut enc = Encoder::with_capacity(16);
    enc.write_nullable_string(None::<&str>).unwrap();
    enc.write_i16(1);
    enc.write_i32(500);
    match decode_produce_request(3, enc.freeze()) {
        ProduceDecodeResult::Err { acks, .. } => assert_eq!(acks, Some(1)),
        ProduceDecodeResult::Ok(_) => panic!("expected decode error after timeout"),
    }
}

#[test]
fn produce_v9_error_on_null_topic_preserves_acks() {
    let mut enc = Encoder::with_capacity(32);
    enc.write_compact_nullable_string(None);
    enc.write_i16(2);
    enc.write_i32(500);
    enc.write_varint(2);
    enc.write_compact_nullable_string(None);
    match decode_produce_request(9, enc.freeze()) {
        ProduceDecodeResult::Err { acks, error } => {
            assert_eq!(acks, Some(2));
            assert!(matches!(error, KafkaProtocolError::NullTopicName));
        }
        ProduceDecodeResult::Ok(_) => panic!("expected NullTopicName"),
    }
}

#[test]
fn produce_v9_error_on_partition_count_preserves_acks() {
    let mut enc = Encoder::with_capacity(64);
    enc.write_compact_nullable_string(None);
    enc.write_i16(3);
    enc.write_i32(500);
    enc.write_varint(2);
    enc.write_compact_nullable_string(Some("topic"));
    match decode_produce_request(9, enc.freeze()) {
        ProduceDecodeResult::Err { acks, .. } => assert_eq!(acks, Some(3)),
        ProduceDecodeResult::Ok(_) => panic!("expected decode error in partition count"),
    }
}

#[test]
fn produce_v9_error_on_partition_records_preserves_acks() {
    let mut enc = Encoder::with_capacity(64);
    enc.write_compact_nullable_string(None);
    enc.write_i16(4);
    enc.write_i32(500);
    enc.write_varint(2);
    enc.write_compact_nullable_string(Some("topic"));
    enc.write_varint(2);
    enc.write_i32(0);
    match decode_produce_request(9, enc.freeze()) {
        ProduceDecodeResult::Err { acks, .. } => assert_eq!(acks, Some(4)),
        ProduceDecodeResult::Ok(_) => panic!("expected decode error in records"),
    }
}

// ── Fetch version branches ────────────────────────────────────────────────────

#[test]
fn fetch_v2_uses_default_max_bytes_when_field_absent() {
    let req = decode_fetch_request(2, wire::build_fetch_v2_default_max_bytes_request())
        .expect("fetch v2 should decode");
    assert_eq!(req.max_bytes, 52_428_800);
    assert_eq!(req.isolation_level, 0);
    assert!(req.topics.is_empty());
}

#[test]
fn fetch_v7_legacy_forgotten_topics_and_rack_id_decode() {
    let req = decode_fetch_request(
        7,
        wire::build_fetch_request_with_sections(7, "topic-a", 1, Some("forgotten"), Some("rack-1")),
    )
    .expect("fetch v7 legacy sections should decode");
    assert_eq!(req.topics[0].topic, "topic-a");
    assert_eq!(req.topics[0].partitions[0].partition, 1);
}

#[test]
fn fetch_v9_leader_epoch_without_v12_fields_decode() {
    let req = decode_fetch_request(
        9,
        wire::build_fetch_request_with_sections(9, "topic-b", 2, None, None),
    )
    .expect("fetch v9 should decode");
    assert_eq!(req.topics[0].partitions[0].fetch_offset, 42);
}

#[test]
fn fetch_v11_legacy_rack_id_decode() {
    let req = decode_fetch_request(
        11,
        wire::build_fetch_request_with_sections(11, "topic-c", 3, None, Some("rack-z")),
    )
    .expect("fetch v11 legacy rack id should decode");
    assert_eq!(req.max_wait_ms, 100);
}

#[test]
fn fetch_v3_skips_isolation_level_field() {
    let req = decode_fetch_request(3, wire::build_fetch_v3_no_isolation_request())
        .expect("fetch v3 should decode");
    assert_eq!(req.isolation_level, 0);
    assert_eq!(req.max_bytes, 1024);
}

#[test]
fn fetch_v4_truncated_after_replica_id_returns_error() {
    let mut enc = Encoder::with_capacity(4);
    enc.write_i32(-1);
    let err = decode_fetch_request(4, enc.freeze()).unwrap_err();
    assert!(matches!(err, KafkaProtocolError::BufferUnderflow { .. }));
}

#[test]
fn fetch_v7_truncated_in_forgotten_topics_returns_error() {
    let body = wire::build_fetch_request_with_sections(7, "topic", 0, Some("forgot"), None);
    let truncated = body.slice(..body.len() - 2);
    let err = decode_fetch_request(7, truncated).unwrap_err();
    assert!(matches!(err, KafkaProtocolError::BufferUnderflow { .. }));
}

#[test]
fn fetch_v12_flexible_truncated_in_topic_tagged_fields_returns_error() {
    let body = wire::build_fetch_request_with_sections(12, "topic", 0, None, None);
    let truncated = body.slice(..body.len() - 1);
    let err = decode_fetch_request(12, truncated).unwrap_err();
    assert!(matches!(err, KafkaProtocolError::BufferUnderflow { .. }));
}

#[test]
fn fetch_v12_flexible_null_topic_name_returns_error() {
    let mut enc = Encoder::with_capacity(64);
    enc.write_i32(-1);
    enc.write_i32(100);
    enc.write_i32(1);
    enc.write_i32(1024);
    enc.write_i8(0);
    enc.write_i32(0);
    enc.write_i32(0);
    enc.write_varint(2);
    enc.write_compact_nullable_string(None);
    let err = decode_fetch_request(12, enc.freeze()).unwrap_err();
    assert!(matches!(err, KafkaProtocolError::NullTopicName));
}

#[test]
fn fetch_null_topic_name_returns_error() {
    let mut enc = Encoder::with_capacity(64);
    enc.write_i32(-1);
    enc.write_i32(100);
    enc.write_i32(1);
    enc.write_i32(i32::MAX);
    enc.write_i8(0);
    enc.write_i32(1);
    enc.write_nullable_string(None::<&str>).unwrap();

    let err = decode_fetch_request(4, enc.freeze()).unwrap_err();
    assert!(matches!(err, KafkaProtocolError::NullTopicName));
}

// ── ListOffsets version branches ──────────────────────────────────────────────

#[test]
fn list_offsets_v1_skips_isolation_level_field() {
    let req = decode_list_offsets_request(1, wire::build_list_offsets_branch_request(1, "v1", 0))
        .expect("list offsets v1 should decode");
    assert_eq!(req.isolation_level, 0);
}

#[test]
fn list_offsets_v2_reads_isolation_level() {
    let req = decode_list_offsets_request(2, wire::build_list_offsets_branch_request(2, "v2", 1))
        .expect("list offsets v2 should decode");
    assert_eq!(req.isolation_level, 1);
}

#[test]
fn list_offsets_v3_skips_leader_epoch_branch() {
    let req = decode_list_offsets_request(3, wire::build_list_offsets_branch_request(3, "v3", 2))
        .expect("list offsets v3 should decode");
    assert_eq!(req.topics[0].partitions[0].partition, 2);
}

#[test]
fn list_offsets_v5_leader_epoch_without_flexible_encoding() {
    let req = decode_list_offsets_request(5, wire::build_list_offsets_branch_request(5, "v5", 3))
        .expect("list offsets v5 should decode");
    assert_eq!(req.topics[0].topic, "v5");
}

#[test]
fn list_offsets_v4_truncated_in_leader_epoch_returns_error() {
    let body = wire::build_list_offsets_branch_request(4, "topic", 1);
    let truncated = body.slice(..body.len() - 4);
    let err = decode_list_offsets_request(4, truncated).unwrap_err();
    assert!(matches!(err, KafkaProtocolError::BufferUnderflow { .. }));
}

#[test]
fn list_offsets_v6_flexible_null_topic_name_returns_error() {
    let mut enc = Encoder::with_capacity(32);
    enc.write_i32(-1);
    enc.write_i8(0);
    enc.write_varint(2);
    enc.write_compact_nullable_string(None);
    let err = decode_list_offsets_request(6, enc.freeze()).unwrap_err();
    assert!(matches!(err, KafkaProtocolError::NullTopicName));
}

#[test]
fn list_offsets_null_topic_name_returns_error() {
    let mut enc = Encoder::with_capacity(32);
    enc.write_i32(-1);
    enc.write_i8(0);
    enc.write_i32(1);
    enc.write_nullable_string(None::<&str>).unwrap();
    let err = decode_list_offsets_request(2, enc.freeze()).unwrap_err();
    assert!(matches!(err, KafkaProtocolError::NullTopicName));
}

// ── CreateTopics version branches ─────────────────────────────────────────────

#[test]
fn create_topics_v3_legacy_assignments_decode() {
    let req = decode_create_topics_request(
        3,
        wire::build_create_topics_request_with_sections(3, "v3-topic"),
    )
    .expect("create topics v3 should decode");
    assert_eq!(req.topics[0].name, "v3-topic");
    assert!(req.validate_only);
}

#[test]
fn create_topics_v4_legacy_configs_decode() {
    let req = decode_create_topics_request(
        4,
        wire::build_create_topics_request_with_sections(4, "v4-topic"),
    )
    .expect("create topics v4 should decode");
    assert_eq!(req.topics[0].replication_factor, 1);
}

#[test]
fn create_topics_v2_truncated_in_config_value_returns_error() {
    let body = wire::build_create_topics_request_with_sections(2, "topic");
    let truncated = body.slice(..body.len() - 3);
    let err = decode_create_topics_request(2, truncated).unwrap_err();
    assert!(matches!(err, KafkaProtocolError::BufferUnderflow { .. }));
}

#[test]
fn create_topics_v5_flexible_null_topic_name_returns_error() {
    let mut enc = Encoder::with_capacity(16);
    enc.write_varint(2);
    enc.write_compact_nullable_string(None);
    let err = decode_create_topics_request(5, enc.freeze()).unwrap_err();
    assert!(matches!(err, KafkaProtocolError::NullTopicName));
}

#[test]
fn create_topics_null_topic_name_returns_error() {
    let mut enc = Encoder::with_capacity(32);
    enc.write_i32(1);
    enc.write_nullable_string(None::<&str>).unwrap();
    let err = decode_create_topics_request(2, enc.freeze()).unwrap_err();
    assert!(matches!(err, KafkaProtocolError::NullTopicName));
}

// ── Server handle_connection branches (e2e) ───────────────────────────────────

#[tokio::test]
async fn e2e_frame_shorter_than_kafka_header_returns_buffer_underflow() {
    let (addr, _shutdown) = server::spawn_test_server().await;
    let mut stream = TcpStream::connect(addr).await.expect("connect");

    let mut frame = BytesMut::new();
    frame.extend_from_slice(&7_i32.to_be_bytes());
    frame.extend_from_slice(&[0x00, 0x12, 0x00, 0x01, 0x00, 0x00, 0x00]);
    stream.write_all(&frame).await.expect("short header write");

    assert_eq!(
        read_byte_with_timeout(&mut stream, Duration::from_secs(2)).await,
        ByteRead::Closed,
        "frame payload shorter than 8-byte Kafka header must close connection"
    );
}

#[tokio::test]
async fn e2e_client_eof_after_valid_frame_closes_connection_cleanly() {
    let (addr, _shutdown) = server::spawn_test_server().await;
    let mut stream = TcpStream::connect(addr).await.expect("connect");

    let frame = build_request_frame(18, 1, 901, Some("eof-test"), &[]);
    stream.write_all(&frame).await.expect("api versions write");
    let _payload = read_response_frame(&mut stream, 8 * 1024 * 1024).await;

    stream.shutdown().await.expect("client shutdown");
    time::sleep(Duration::from_millis(100)).await;

    let mut buf = [0u8; 1];
    let n = stream.read(&mut buf).await.expect("read after shutdown");
    assert_eq!(n, 0, "server should close after client EOF");
}
