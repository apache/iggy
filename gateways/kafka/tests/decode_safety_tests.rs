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

//! Adversarial wire-input tests for #3421 - malformed lengths must return errors, never panic.

#[path = "common/wire.rs"]
mod wire;

use bytes::Bytes;

use iggy_gateway_kafka::error::KafkaProtocolError;
use iggy_gateway_kafka::protocol::codec::{Decoder, Encoder, MAX_COLLECTION_LEN};
use iggy_gateway_kafka::protocol::requests::{
    ProduceDecodeResult, decode_create_topics_request, decode_fetch_request,
    decode_list_offsets_request, decode_produce_request,
};

#[test]
fn compact_array_varint_zero_rejected_on_non_nullable_array() {
    // Compact-array varint=0 is Kafka's null encoding; required (non-nullable) arrays reject it.
    let mut d = Decoder::new(Bytes::from_static(&[0x00]));
    let err = d.read_compact_array_count().unwrap_err();
    assert!(matches!(err, KafkaProtocolError::NullCompactArray));
}

#[test]
fn compact_array_varint_zero_nullable_decodes_as_empty() {
    let mut d = Decoder::new(Bytes::from_static(&[0x00]));
    assert_eq!(d.read_compact_array_count_nullable().unwrap(), 0);
}

#[test]
fn produce_decoder_rejects_trailing_bytes_after_valid_body() {
    let mut body = Vec::new();
    body.extend_from_slice(&(-1_i16).to_be_bytes()); // null transactional_id (legacy)
    body.extend_from_slice(&1_i16.to_be_bytes()); // acks
    body.extend_from_slice(&1000_i32.to_be_bytes()); // timeout_ms
    body.extend_from_slice(&0_i32.to_be_bytes()); // empty topics
    body.push(0xFF); // trailing garbage
    let err = decode_produce_request(3, Bytes::from(body))
        .into_request()
        .unwrap_err();
    assert!(matches!(err, KafkaProtocolError::UnexpectedTrailingBytes));
}

#[test]
fn negative_i32_array_length_returns_error_not_panic() {
    let mut raw = Vec::new();
    raw.extend_from_slice(&(-1_i32).to_be_bytes());
    let mut d = Decoder::new(Bytes::from(raw));
    let err = d.read_i32_array_count().unwrap_err();
    assert!(matches!(err, KafkaProtocolError::InvalidArrayLength(-1)));
}

#[test]
fn i32_array_length_above_max_returns_collection_too_large() {
    let mut raw = Vec::new();
    let oversized = i32::try_from(MAX_COLLECTION_LEN + 1).expect("test value fits i32");
    raw.extend_from_slice(&oversized.to_be_bytes());
    let mut d = Decoder::new(Bytes::from(raw));
    let err = d.read_i32_array_count().unwrap_err();
    assert!(matches!(err, KafkaProtocolError::CollectionTooLarge { .. }));
}

#[test]
fn fetch_max_declared_topics_count_with_empty_body_returns_error_not_large_alloc() {
    // Declares the maximum allowed topics_count (65_536) but supplies no element bytes at
    // all. Guards against pre-reserving a Vec directly off the wire count before validating
    // any element bytes are present - decode must fail fast on the first missing byte, not
    // attempt a large upfront allocation.
    let mut body = Vec::new();
    body.extend_from_slice(&0_i32.to_be_bytes()); // replica_id
    body.extend_from_slice(&0_i32.to_be_bytes()); // max_wait_ms
    body.extend_from_slice(&0_i32.to_be_bytes()); // min_bytes
    body.extend_from_slice(&0_i32.to_be_bytes()); // max_bytes (version >= 3)
    body.push(0); // isolation_level (version >= 4)
    let topics_count = i32::try_from(MAX_COLLECTION_LEN).expect("fits i32");
    body.extend_from_slice(&topics_count.to_be_bytes());
    // no topic bytes follow

    let err = decode_fetch_request(4, Bytes::from(body)).unwrap_err();
    assert!(matches!(err, KafkaProtocolError::BufferUnderflow { .. }));
}

#[test]
fn produce_decoder_rejects_truncated_flexible_body() {
    let mut body = Vec::new();
    body.push(0x00); // transactional_id null (compact)
    body.extend_from_slice(&1_i16.to_be_bytes()); // acks
    body.extend_from_slice(&1000_i32.to_be_bytes()); // timeout
    body.push(0x02); // topics compact array: 1 element (varint = count+1)
    // truncated before topic name

    let err = decode_produce_request(9, Bytes::from(body))
        .into_request()
        .unwrap_err();
    assert!(matches!(err, KafkaProtocolError::BufferUnderflow { .. }));
}

#[test]
fn write_nullable_string_rejects_oversized_length() {
    let mut enc = Encoder::with_capacity(8);
    let long = "x".repeat(i16::MAX as usize + 1);
    let err = enc.write_nullable_string(Some(&long)).unwrap_err();
    assert!(matches!(err, KafkaProtocolError::StringTooLong { .. }));
}

#[test]
fn varint_terminal_byte_with_extra_bits_at_shift_63_is_rejected() {
    // Nine continuation bytes then terminal 0x7E at shift 63 (bits 1-6 set, bit 7 clear).
    let mut d = Decoder::new(Bytes::from_static(&[
        0x80, 0x80, 0x80, 0x80, 0x80, 0x80, 0x80, 0x80, 0x80, 0x7E,
    ]));
    let err = d.read_varint().unwrap_err();
    assert!(matches!(err, KafkaProtocolError::InvalidVarint));
}

// ── Produce: error preserves acks (so a retry decision can honor acks=0) ────

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

// ── Fetch: truncated / null-topic inputs return errors, never panic ────────

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

// ── ListOffsets: truncated / null-topic inputs return errors, never panic ──

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

// ── CreateTopics: truncated / null-topic inputs return errors, never panic ─

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
