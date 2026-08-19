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

//! `request_header_version` / `response_header_version` are thin wrappers around
//! `kafka_protocol::messages::ApiKey` (see `src/protocol/header.rs`); decoding/encoding the
//! header bytes themselves is `kafka_protocol::messages::RequestHeader`/`ResponseHeader`'s own
//! tested responsibility, not re-tested here. These tests cover the gateway-specific policy
//! layered on top: the unknown-API-key fallback and the `ApiVersions` response-header special case.

use kafka_protocol::messages::ApiKey;

use iggy_gateway_kafka::protocol::api::{
    API_KEY_API_VERSIONS, API_KEY_CREATE_TOPICS, API_KEY_FETCH, API_KEY_LIST_OFFSETS,
    API_KEY_METADATA, API_KEY_PRODUCE,
};
use iggy_gateway_kafka::protocol::header::{request_header_version, response_header_version};

/// Flexible-encoding threshold per API key (mirrors `protocol/header.rs`; cross-checked against
/// the independent `kafka-protocol` crate below rather than trusted on its own).
///
/// Keys 4-7 (LeaderAndIsr/StopReplica/UpdateMetadata/ControlledShutdown) are inter-broker-only
/// APIs `kafka_protocol` 0.17 does not implement (`ApiKey::try_from` fails for them), so this
/// gateway's wrapper always falls back to header v1 for them - `i16::MAX`, not their legacy
/// threshold from the pre-migration hand-rolled table.
const API_KEY_FLEXIBLE_FROM: &[(i16, i16)] = &[
    (0, 9),
    (1, 12),
    (2, 6),
    (3, 9),
    (4, i16::MAX),
    (5, i16::MAX),
    (6, i16::MAX),
    (7, i16::MAX),
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
    // WriteTxnMarkers' only valid versions are 1-2 (no v0 on the real wire) and both are
    // flexible; `kafka_protocol` encodes this as an unconditional header v2, matching the `0`
    // ("always flexible") arm below rather than a real threshold.
    (27, 0),
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
    (57, 0),
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

/// `request_header_version(api_key, version)` is exactly
/// `ApiKey::try_from(api_key).map_or(1, |key| key.request_header_version(version))` - so for
/// any `api_key` where `try_from` succeeds, this expands to `external.request_header_version
/// (version) == external.request_header_version(version)`. That is true by construction, not by
/// anything this test checks; it would pass unchanged if `header.rs` swapped `map_or(1, ...)`
/// for `map_or(2, ...)`, or if `kafka_protocol` itself shipped a wrong threshold. There is no
/// independent source of the right answer to compare against without hand-maintaining the same
/// threshold table the migration to `kafka_protocol` deliberately eliminated (see
/// `src/protocol/header.rs`'s module doc).
///
/// The one place this wrapper adds real (non-tautological) behavior is the `map_or` **fallback**
/// - what happens when `try_from` fails. That's the only assertion worth writing here.
#[test]
fn request_header_version_falls_back_to_v1_when_kafka_protocol_does_not_recognize_the_key() {
    // Keys 4-7 (LeaderAndIsr/StopReplica/UpdateMetadata/ControlledShutdown) are the only ones in
    // API_KEY_FLEXIBLE_FROM where `ApiKey::try_from` genuinely fails in kafka-protocol 0.17 -
    // verified directly against the crate's `ApiKey` enum, not inferred from this table. Keys
    // 17 and 47 (SaslHandshake, OffsetDelete) are recognized by the crate; they're marked
    // `i16::MAX` here because the crate's own `request_header_version` never returns 2 for them,
    // which `request_header_version_never_flexible_keys_stay_v1` already covers.
    for key in [4, 5, 6, 7] {
        assert!(
            ApiKey::try_from(key).is_err(),
            "api_key {key} must be one kafka_protocol::messages::ApiKey does not recognize, \
             for this test to exercise the fallback"
        );
        assert_eq!(request_header_version(key, 0), 1, "api_key {key}");
        assert_eq!(request_header_version(key, i16::MAX), 1, "api_key {key}");
    }
}

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
                assert_eq!(
                    request_header_version(api_key, threshold - 1),
                    1,
                    "api_key={api_key} threshold={threshold}"
                );
                assert_eq!(
                    request_header_version(api_key, threshold),
                    2,
                    "api_key={api_key} threshold={threshold}"
                );
            }
        }
    }
}

/// The ApiVersions-always-v0 special case is already independently covered by
/// `response_header_version_apiversions_always_zero` below. Deleted from here: a loop that
/// derived its own "expected" value by calling `request_header_version` (i.e. compared the
/// wrapper against itself, not against the gateway's actual policy) - the same tautology as
/// `request_header_version_falls_back_to_v1_when_kafka_protocol_does_not_recognize_the_key`
/// above, and it would still pass if `response_header_version` and `request_header_version`
/// both drifted together.
#[test]
fn response_header_version_unknown_api_defaults_to_v0() {
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

// ── Flexible-encoding boundaries (SCOPE.md) ─────────────────────────────────

#[test]
fn request_header_version_switches_at_scope_flexible_boundaries() {
    assert_eq!(request_header_version(API_KEY_PRODUCE, 8), 1);
    assert_eq!(request_header_version(API_KEY_PRODUCE, 9), 2);
    assert_eq!(request_header_version(API_KEY_FETCH, 11), 1);
    assert_eq!(request_header_version(API_KEY_FETCH, 12), 2);
    assert_eq!(request_header_version(API_KEY_LIST_OFFSETS, 5), 1);
    assert_eq!(request_header_version(API_KEY_LIST_OFFSETS, 6), 2);
    assert_eq!(request_header_version(API_KEY_METADATA, 8), 1);
    assert_eq!(request_header_version(API_KEY_METADATA, 9), 2);
    assert_eq!(request_header_version(API_KEY_API_VERSIONS, 2), 1);
    assert_eq!(request_header_version(API_KEY_API_VERSIONS, 3), 2);
    assert_eq!(request_header_version(API_KEY_CREATE_TOPICS, 4), 1);
    assert_eq!(request_header_version(API_KEY_CREATE_TOPICS, 5), 2);
}
