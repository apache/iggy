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

//! Adversarial wire-input tests for #3421 — malformed lengths must return errors, never panic.

use bytes::Bytes;

use iggy_gateway_kafka::error::KafkaProtocolError;
use iggy_gateway_kafka::protocol::codec::{Decoder, Encoder, MAX_COLLECTION_LEN};
use iggy_gateway_kafka::protocol::requests::decode_produce_request;

#[test]
fn compact_array_varint_zero_decodes_as_empty_without_panic() {
    // Per Kafka spec, compact-array varint=0 means null/absent → 0 elements (not an error).
    let mut d = Decoder::new(Bytes::from_static(&[0x00]));
    assert_eq!(d.read_compact_array_count().unwrap(), 0);
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
fn produce_decoder_rejects_truncated_flexible_body() {
    let mut body = Vec::new();
    body.push(0x00); // transactional_id null (compact)
    body.extend_from_slice(&1_i16.to_be_bytes()); // acks
    body.extend_from_slice(&1000_i32.to_be_bytes()); // timeout
    body.push(0x02); // topics compact array: 1 element (varint = count+1)
    // truncated before topic name

    let err = decode_produce_request(9, Bytes::from(body)).unwrap_err();
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
