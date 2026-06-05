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
    enc.write_nullable_string(Some("kafka-cli"));
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
    enc.write_nullable_string(None);
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
fn response_header_version_flexible_non_apiversions() {
    // Metadata v9+ is flexible → response header v1
    assert_eq!(response_header_version(3, 9), 1);
    // Metadata v0 is non-flexible → response header v0
    assert_eq!(response_header_version(3, 0), 0);
}
