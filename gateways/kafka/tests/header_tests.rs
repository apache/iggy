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

use iggy_gateway_kafka::protocol::codec::Encoder;
use iggy_gateway_kafka::protocol::header::{
    RequestHeader, ResponseHeader, request_header_version, response_header_version,
};

// ── Request header v1 (non-flexible) ───────────────────────────────────────

#[test]
fn request_header_v1_decodes() {
    let mut enc = Encoder::with_capacity(64);
    enc.write_i16(18); // api_key: ApiVersions
    enc.write_i16(2); // api_version
    enc.write_i32(101);
    enc.write_nullable_string(Some("kafka-cli")).unwrap();
    let bytes = enc.freeze();

    let header = RequestHeader::decode(bytes, 1).expect("decode should succeed");
    assert_eq!(header.api_key, 18);
    assert_eq!(header.api_version, 2);
    assert_eq!(header.correlation_id, 101);
    assert_eq!(header.client_id.as_deref(), Some("kafka-cli"));
}

#[test]
fn request_header_v1_null_client_id() {
    let mut enc = Encoder::with_capacity(32);
    enc.write_i16(18);
    enc.write_i16(1);
    enc.write_i32(5);
    enc.write_nullable_string(None).unwrap();
    let bytes = enc.freeze();

    let header = RequestHeader::decode(bytes, 1).unwrap();
    assert_eq!(header.client_id, None);
}

// ── Request header v2 (flexible — compact client_id + tagged fields) ───────

#[test]
fn request_header_v2_decodes() {
    let mut enc = Encoder::with_capacity(64);
    enc.write_i16(18); // api_key: ApiVersions
    enc.write_i16(3); // api_version (flexible threshold for ApiVersions is 3)
    enc.write_i32(202);
    enc.write_compact_nullable_string(Some("my-client"));
    enc.write_empty_tagged_fields();
    let bytes = enc.freeze();

    let header = RequestHeader::decode(bytes, 2).expect("flexible decode should succeed");
    assert_eq!(header.api_key, 18);
    assert_eq!(header.api_version, 3);
    assert_eq!(header.correlation_id, 202);
    assert_eq!(header.client_id.as_deref(), Some("my-client"));
}

#[test]
fn request_header_v2_null_client_id() {
    let mut enc = Encoder::with_capacity(32);
    enc.write_i16(18);
    enc.write_i16(3);
    enc.write_i32(303);
    enc.write_compact_nullable_string(None);
    enc.write_empty_tagged_fields();
    let bytes = enc.freeze();

    let header = RequestHeader::decode(bytes, 2).unwrap();
    assert_eq!(header.client_id, None);
}

// ── Response header encode ──────────────────────────────────────────────────

#[test]
fn response_header_v0_encodes_correlation_id_only() {
    let header = ResponseHeader { correlation_id: 77 };
    let bytes = header.encode(0);
    assert_eq!(bytes.as_ref(), &[0, 0, 0, 77]);
}

#[test]
fn response_header_v1_encodes_correlation_id_plus_tagged_fields() {
    let header = ResponseHeader { correlation_id: 1 };
    let bytes = header.encode(1);
    // [0,0,0,1] correlation_id + [0x00] empty tagged fields
    assert_eq!(bytes.as_ref(), &[0, 0, 0, 1, 0x00]);
}

// ── Header version lookup ───────────────────────────────────────────────────

/// Flexible-encoding threshold per API key (mirrors `protocol/header.rs`).
const API_KEY_FLEXIBLE_FROM: &[(i16, i16)] = &[
    (0, 9),
    (1, 12),
    (2, 6),
    (3, 9),
    (4, 4),
    (5, 2),
    (6, 6),
    (7, 3),
    (8, 8),
    (9, 6),
    (10, 3),
    (11, 6),
    (12, 4),
    (13, 4),
    (14, 4),
    (15, 5),
    (16, 3),
    (17, i16::MAX),
    (18, 3),
    (19, 5),
    (20, 4),
    (21, 2),
    (22, 2),
    (23, 4),
    (24, 3),
    (25, 3),
    (26, 3),
    (27, 1),
    (28, 3),
    (29, 2),
    (30, 2),
    (31, 2),
    (32, 4),
    (33, 2),
    (34, 2),
    (35, 2),
    (36, 2),
    (37, 2),
    (38, 2),
    (39, 2),
    (40, 2),
    (41, 2),
    (42, 2),
    (43, 2),
    (44, 1),
    (45, 0),
    (46, 0),
    (47, i16::MAX),
    (48, 1),
    (49, 1),
    (50, 0),
    (51, 0),
    (55, 0),
    (56, 0),
    (57, 1),
    (60, 0),
    (61, 0),
    (64, 0),
    (65, 0),
    (66, 0),
    (67, 0),
    (68, 0),
    (69, 0),
    (71, 0),
    (72, 0),
    (74, 0),
    (75, 0),
    (76, 0),
    (77, 0),
    (78, 0),
    (79, 0),
    (80, 0),
];

#[test]
fn request_header_version_hits_every_api_key_match_arm() {
    for &(api_key, flexible_from) in API_KEY_FLEXIBLE_FROM {
        match flexible_from {
            0 => {
                assert_eq!(request_header_version(api_key, 0), 2);
                assert_eq!(request_header_version(api_key, i16::MAX), 2);
            }
            i16::MAX => {
                assert_eq!(request_header_version(api_key, 0), 1);
                assert_eq!(request_header_version(api_key, i16::MAX - 1), 1);
            }
            threshold => {
                assert_eq!(request_header_version(api_key, threshold - 1), 1);
                assert_eq!(request_header_version(api_key, threshold), 2);
            }
        }
    }
}

#[test]
fn response_header_version_hits_every_api_key_match_arm() {
    for &(api_key, _) in API_KEY_FLEXIBLE_FROM {
        if api_key == 18 {
            assert_eq!(response_header_version(api_key, 0), 0);
            assert_eq!(response_header_version(api_key, 99), 0);
            continue;
        }
        let req_hdr_at_v0 = request_header_version(api_key, 0);
        let expected_at_v0 = if req_hdr_at_v0 >= 2 { 1 } else { 0 };
        assert_eq!(response_header_version(api_key, 0), expected_at_v0);

        let req_hdr_at_max = request_header_version(api_key, i16::MAX - 1);
        let expected_at_max = if req_hdr_at_max >= 2 { 1 } else { 0 };
        assert_eq!(
            response_header_version(api_key, i16::MAX - 1),
            expected_at_max
        );
    }
    assert_eq!(response_header_version(999, 0), 0);
}

#[test]
fn request_header_version_non_flexible_below_threshold() {
    // ApiVersions v0-2 → header v1
    assert_eq!(request_header_version(18, 0), 1);
    assert_eq!(request_header_version(18, 2), 1);
    // Metadata v0-8 → header v1
    assert_eq!(request_header_version(3, 0), 1);
    assert_eq!(request_header_version(3, 8), 1);
}

#[test]
fn request_header_version_flexible_at_threshold() {
    // ApiVersions v3 → header v2
    assert_eq!(request_header_version(18, 3), 2);
    // Metadata v9 → header v2
    assert_eq!(request_header_version(3, 9), 2);
    // ConsumerGroupHeartbeat (68) always flexible
    assert_eq!(request_header_version(68, 0), 2);
}

#[test]
fn response_header_version_apiversions_always_zero() {
    // ApiVersions is a special case: response header is always v0
    assert_eq!(response_header_version(18, 0), 0);
    assert_eq!(response_header_version(18, 3), 0); // even flexible request → v0 response
}

#[test]
fn share_group_api_keys_use_flexible_header_from_v0() {
    for key in [77, 78, 79, 80] {
        assert_eq!(
            request_header_version(key, 0),
            2,
            "api_key {key} must use flexible header v2"
        );
    }
}

#[test]
fn response_header_version_flexible_non_apiversions() {
    // Metadata v9+ is flexible → response header v1
    assert_eq!(response_header_version(3, 9), 1);
    // Metadata v0 is non-flexible → response header v0
    assert_eq!(response_header_version(3, 0), 0);
}

#[test]
fn request_header_version_never_flexible_keys_stay_v1() {
    for key in [17, 47] {
        assert_eq!(
            request_header_version(key, i16::MAX - 1),
            1,
            "api_key {key} must stay on header v1"
        );
    }
}

#[test]
fn request_header_version_always_flexible_keys_use_v2() {
    for key in [
        45, 46, 50, 51, 55, 60, 61, 64, 65, 66, 67, 68, 69, 71, 72, 74, 75, 76,
    ] {
        assert_eq!(
            request_header_version(key, 0),
            2,
            "api_key {key} must use flexible header v2"
        );
    }
}

#[test]
fn request_header_version_unknown_api_defaults_to_v1() {
    assert_eq!(request_header_version(999, 0), 1);
    assert_eq!(request_header_version(-1, 12), 1);
}

#[test]
fn request_header_decode_rejects_unsupported_version() {
    let bytes = Encoder::with_capacity(0).freeze();
    let err = RequestHeader::decode(bytes, 99).unwrap_err();
    assert!(matches!(
        err,
        iggy_gateway_kafka::error::KafkaProtocolError::UnsupportedHeaderVersion(99)
    ));
}

#[test]
fn request_header_v1_truncated_payload_fails() {
    let mut enc = Encoder::with_capacity(8);
    enc.write_i16(18);
    enc.write_i16(1);
    let err = RequestHeader::decode(enc.freeze(), 1).unwrap_err();
    assert!(err.to_string().contains("buffer underflow"));
}

#[test]
fn request_header_v2_truncated_before_tagged_fields_fails() {
    let mut enc = Encoder::with_capacity(16);
    enc.write_i16(18);
    enc.write_i16(3);
    enc.write_i32(303);
    enc.write_compact_nullable_string(Some("c"));
    let err = RequestHeader::decode(enc.freeze(), 2).unwrap_err();
    assert!(err.to_string().contains("buffer underflow"));
}

#[test]
fn response_header_encode_into_matches_encode() {
    let header = ResponseHeader {
        correlation_id: 1234,
    };
    let encoded = header.encode(1);
    let mut buf = bytes::BytesMut::new();
    header.encode_into(&mut buf, 1);
    assert_eq!(buf.freeze(), encoded);
}

#[test]
fn response_header_encoded_size_matches_versions() {
    assert_eq!(ResponseHeader::encoded_size(0), 4);
    assert_eq!(ResponseHeader::encoded_size(1), 5);
    assert_eq!(ResponseHeader::encoded_size(2), 5);
}
