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

//! Version negotiation firewall - boundary tests for every scoped API key.

#[path = "common/codec.rs"]
mod codec;
#[path = "common/fixtures.rs"]
mod fixtures;
#[path = "common/scope.rs"]
mod scope;
#[path = "common/server.rs"]
mod server;
#[path = "common/tcp.rs"]
mod tcp;
#[path = "common/wire.rs"]
mod wire;

use std::time::Duration;

use bytes::Bytes;
use tokio::io::AsyncWriteExt;
use tokio::net::TcpStream;

use iggy_gateway_kafka::protocol::api::{
    API_KEY_API_VERSIONS, API_KEY_CREATE_TOPICS, API_KEY_FETCH, API_KEY_LIST_OFFSETS,
    API_KEY_METADATA, API_KEY_PRODUCE, ERROR_INVALID_REQUEST, ERROR_NONE,
    ERROR_UNSUPPORTED_VERSION, advertised_min_version, handle_request, is_supported_version,
    supported_api_ranges,
};

use codec::Decoder;
use fixtures::load_fixture_body_or_skip;
use scope::{SCOPED_API_KEYS, default_broker};
use server::spawn_test_server;
use tcp::{
    ByteRead, build_list_offsets_v0_request_with_topic_t, build_metadata_legacy_request,
    build_produce_flexible_body, build_produce_v2_body, build_produce_v3_body, build_request_frame,
    parse_response_payload, read_byte_with_timeout, round_trip,
};
use wire::{
    OUT_OF_SCOPE_API_KEYS, build_api_versions_flexible_request, build_create_topics_empty_request,
    build_fetch_empty_topics_request, build_list_offsets_request,
    build_metadata_all_topics_flexible, build_metadata_all_topics_legacy,
    build_metadata_flexible_request_v10,
};

#[test]
fn supported_ranges_table_has_six_entries() {
    assert_eq!(supported_api_ranges().len(), 6);
}

#[test]
fn is_supported_version_matches_scope_table() {
    for &(api_key, _, min_ver, max_ver) in SCOPED_API_KEYS {
        assert!(
            !is_supported_version(api_key, min_ver - 1),
            "key {api_key} must reject v{}",
            min_ver - 1
        );
        assert!(
            is_supported_version(api_key, min_ver),
            "key {api_key} must accept min v{min_ver}"
        );
        assert!(
            is_supported_version(api_key, max_ver),
            "key {api_key} must accept max v{max_ver}"
        );
        assert!(
            !is_supported_version(api_key, max_ver + 1),
            "key {api_key} must reject v{}",
            max_ver + 1
        );
    }
}

/// Checked against `SCOPED_API_KEYS` - a hand-maintained table mirroring `SCOPE.md`'s published
/// contract, not `supported_api_ranges()` (the function under test). Comparing the wire response
/// against `supported_api_ranges()` would only prove the encoder faithfully serializes whatever
/// `SUPPORTED_RANGES` happens to hold today, even if that table itself regressed. The one
/// deliberate min-version override (Produce advertises 0, not its real firewall floor of 3, per
/// KIP-511 compatibility) is hardcoded here rather than obtained by calling
/// `advertised_min_version` - the same reasoning applies to it as the function under test.
///
/// Relies on `SUPPORTED_RANGES` (src) and `SCOPED_API_KEYS` (test) sharing declaration order
/// (Produce, Fetch, `ListOffsets`, Metadata, `ApiVersions`, `CreateTopics`) -
/// `supported_ranges_table_has_six_entries` plus `is_supported_version_matches_scope_table`
/// already pin that both tables cover the same six keys.
#[test]
fn apiversions_advertises_exact_supported_ranges_v1() {
    let body = handle_request(API_KEY_API_VERSIONS, 1, Bytes::new(), &default_broker())
        .expect_response("test request has acks != 0 and expects a response");
    let mut d = Decoder::new(body);
    assert_eq!(d.read_i16().unwrap(), 0);
    let count = usize::try_from(d.read_i32().unwrap()).expect("api count fits usize");
    assert_eq!(count, SCOPED_API_KEYS.len());

    for &(api_key, name, min_ver, max_ver) in SCOPED_API_KEYS {
        let key = d.read_i16().unwrap();
        let min = d.read_i16().unwrap();
        let max = d.read_i16().unwrap();
        assert_eq!(key, api_key, "{name}");
        let expected_min = if api_key == API_KEY_PRODUCE {
            0
        } else {
            min_ver
        };
        assert_eq!(min, expected_min, "{name} advertised min");
        assert_eq!(max, max_ver, "{name} advertised max");
    }
    assert_eq!(d.read_i32().unwrap(), 0); // throttle
    assert_eq!(d.remaining(), 0);
}

/// See `apiversions_advertises_exact_supported_ranges_v1` for why this checks against
/// `SCOPED_API_KEYS`, not `supported_api_ranges()`.
#[test]
fn apiversions_advertises_exact_supported_ranges_v3_flexible() {
    let body = handle_request(
        API_KEY_API_VERSIONS,
        3,
        build_api_versions_flexible_request("iggy-test", "0.1.0"),
        &default_broker(),
    )
    .expect_response("test request has acks != 0 and expects a response");
    let mut d = Decoder::new(body);
    assert_eq!(d.read_i16().unwrap(), 0);
    let count = usize::try_from(d.read_varint().unwrap() - 1).expect("api count fits usize");
    assert_eq!(count, SCOPED_API_KEYS.len());

    for &(api_key, name, min_ver, max_ver) in SCOPED_API_KEYS {
        let key = d.read_i16().unwrap();
        let min = d.read_i16().unwrap();
        let max = d.read_i16().unwrap();
        d.read_tagged_fields().unwrap();
        assert_eq!(key, api_key, "{name}");
        let expected_min = if api_key == API_KEY_PRODUCE {
            0
        } else {
            min_ver
        };
        assert_eq!(min, expected_min, "{name} advertised min");
        assert_eq!(max, max_ver, "{name} advertised max");
    }
    assert_eq!(d.read_i32().unwrap(), 0);
    d.read_tagged_fields().unwrap();
    assert_eq!(d.remaining(), 0);
}

// Produce-advertises-min-zero-but-firewall-stays-three coverage lives in
// `produce_advertises_min_zero_but_firewall_rejects_below_v3` below - checked against
// `SCOPED_API_KEYS` (independent of `supported_api_ranges()`, the function under test) plus the
// acks=0/acks!=0 behavioral split this shorter version never covered.

#[test]
fn apiversions_all_versions_return_success() {
    for version in 0i16..=3 {
        let request = if version >= 3 {
            build_api_versions_flexible_request("iggy-test", "0.1.0")
        } else {
            Bytes::new()
        };
        let body = handle_request(API_KEY_API_VERSIONS, version, request, &default_broker())
            .expect_response("test request has acks != 0 and expects a response");
        let mut d = Decoder::new(body);
        assert_eq!(d.read_i16().unwrap(), 0, "ApiVersions v{version}");
    }
}

#[test]
fn apiversions_out_of_range_returns_unsupported_in_body() {
    let body = handle_request(API_KEY_API_VERSIONS, 99, Bytes::new(), &default_broker())
        .expect_response("test request has acks != 0 and expects a response");
    let mut d = Decoder::new(body);
    assert_eq!(d.read_i16().unwrap(), ERROR_UNSUPPORTED_VERSION);
}

fn metadata_request_one_topic() -> Bytes {
    build_metadata_legacy_request(&["test-topic"])
}

#[test]
fn metadata_below_min_version_closes_connection() {
    assert!(
        handle_request(
            API_KEY_METADATA,
            -1,
            metadata_request_one_topic(),
            &default_broker(),
        )
        .is_close(),
        "Metadata below supported min must close rather than return a clamped body"
    );
}

#[test]
fn metadata_above_max_version_closes_connection() {
    // v10 request uses flexible encoding; a clamped v9 reply would not survive client parsing.
    assert!(
        handle_request(
            API_KEY_METADATA,
            10,
            build_metadata_flexible_request_v10(&["test-topic"]),
            &default_broker(),
        )
        .is_close(),
        "Metadata above supported max must close rather than return a clamped body"
    );
}

#[tokio::test]
async fn e2e_metadata_above_max_version_closes_tcp_connection() {
    let (addr, _shutdown) = spawn_test_server().await;
    let mut stream = TcpStream::connect(addr).await.expect("connect");

    let body = build_metadata_flexible_request_v10(&["orders"]);
    let frame = build_request_frame(API_KEY_METADATA, 10, 44, Some("n9-test"), &body);
    stream.write_all(&frame).await.expect("write metadata v10");

    assert_eq!(
        read_byte_with_timeout(&mut stream, Duration::from_secs(2)).await,
        ByteRead::Closed,
        "unsupported Metadata version must close the connection"
    );
}

#[test]
fn produce_below_min_version_with_nonzero_acks_closes_connection() {
    // Produce v2 is below both the firewall min (3) and `kafka_protocol`'s schema floor (3-13)
    // - no encodable response exists at this version, so a client expecting a reply (acks != 0)
    // gets a close instead of the pre-migration downgraded error response. acks=0 still keeps
    // the connection open - see `produce_advertises_min_zero_but_firewall_rejects_below_v3` and
    // `api::handle_produce_request`'s hand-peeked acks path.
    let body = handle_request(
        API_KEY_PRODUCE,
        2,
        build_produce_v2_body(1, 0),
        &default_broker(),
    );
    assert!(
        body.is_close(),
        "Produce v2 with acks != 0 has no encodable response shape and must close"
    );
}

#[test]
fn fetch_below_min_version_closes_connection() {
    // Fetch v3 is below both the firewall min (4) and `kafka_protocol`'s schema floor (4-18) -
    // no encodable response exists at this version, so this closes instead of the
    // pre-migration downgraded error response.
    assert!(
        handle_request(API_KEY_FETCH, 3, Bytes::new(), &default_broker()).is_close(),
        "Fetch v3 has no encodable response shape and must close"
    );
}

#[test]
fn fetch_unsupported_version_above_max_closes_connection() {
    // Fetch v13+ response shape differs from the v12 encoder; a clamped body is unparsable.
    assert!(
        handle_request(API_KEY_FETCH, 13, Bytes::new(), &default_broker()).is_close(),
        "Fetch above encoder max must close rather than return a clamped body"
    );
}

#[test]
fn produce_unsupported_version_above_max_closes_connection() {
    assert!(
        handle_request(API_KEY_PRODUCE, 13, Bytes::new(), &default_broker()).is_close(),
        "Produce above encoder max must close rather than return a clamped body"
    );
}

#[test]
fn create_topics_unsupported_version_above_max_closes_connection() {
    assert!(
        handle_request(API_KEY_CREATE_TOPICS, 7, Bytes::new(), &default_broker()).is_close(),
        "CreateTopics above encoder max must close rather than return a clamped body"
    );
}

#[test]
fn list_offsets_unsupported_version_above_max_closes_connection() {
    assert!(
        handle_request(API_KEY_LIST_OFFSETS, 7, Bytes::new(), &default_broker()).is_close(),
        "ListOffsets above encoder max must close rather than return a clamped body"
    );
}

#[test]
fn list_offsets_v0_closes_connection() {
    // `kafka_protocol` has no encoder for ListOffsets v0's legacy `old_style_offsets` shape (it
    // predates the schema the crate generates from), so a v0 request - already below the
    // firewall's min=1 - now closes instead of getting the pre-migration downgraded response.
    assert!(
        handle_request(API_KEY_LIST_OFFSETS, 0, Bytes::new(), &default_broker()).is_close(),
        "ListOffsets v0 has no encodable response shape and must close"
    );
}

#[test]
fn create_topics_below_min_version_closes_connection() {
    // CreateTopics v1 is below both the firewall min (2) and `kafka_protocol`'s schema floor
    // (2-7) - no encodable response exists at this version, so this closes instead of the
    // pre-migration downgraded error response.
    assert!(
        handle_request(API_KEY_CREATE_TOPICS, 1, Bytes::new(), &default_broker()).is_close(),
        "CreateTopics v1 has no encodable response shape and must close"
    );
}

#[test]
fn unsupported_api_keys_close_connection() {
    for key in [8, 9, 10, 11, 17, 20, 42, 999] {
        let outcome = handle_request(key, 0, Bytes::new(), &default_broker());
        assert!(
            outcome.is_close(),
            "unknown api_key {key} must close (no parseable response schema)"
        );
    }
}

// Produce/Fetch fixture-driven "supported version returns non-empty response" coverage lives in
// `api_handler_tests.rs::handle_request_succeeds_for_every_supported_version_with_fixture` -
// that generic per-scoped-API loop already exercises the exact same fixtures and version ranges.

#[test]
fn corrupt_produce_body_with_acks_stays_silent() {
    // `kafka_protocol` decodes Produce in one shot, so a decode failure never exposes `acks`
    // (unlike the pre-migration field-by-field decoder, which could still answer with
    // INVALID_REQUEST once it knew acks was nonzero). Every Produce decode failure now stays
    // silent regardless of whether acks was readable before the truncation.
    let body = Bytes::from_static(&[
        0xFF, 0xFF, // null transactional_id
        0x00, 0x01, // acks = 1
        0x00, 0x00, 0x00, 0x00, // timeout_ms = 0
        0xFF, 0xFF, 0xFF, // truncated topics count
    ]);
    assert!(
        handle_request(API_KEY_PRODUCE, 3, body, &default_broker()).is_no_response(),
        "malformed Produce body must stay silent regardless of acks"
    );
}

#[test]
fn corrupt_produce_body_before_acks_is_silent() {
    // Decode fails before acks is read: the client's response expectation is unknowable, and an
    // error response could desync an acks=0 fire-and-forget client, so the server stays silent.
    let body = Bytes::from_static(&[0xFF, 0xFF]); // null transactional_id, then EOF
    let outcome = handle_request(API_KEY_PRODUCE, 3, body, &default_broker());
    assert!(
        outcome.is_no_response(),
        "produce decode failure before acks must be silent"
    );
}

#[test]
fn corrupt_fetch_body_returns_invalid_request_error() {
    let body = Bytes::from_static(&[0xFF, 0xFF, 0xFF]);
    let resp = handle_request(API_KEY_FETCH, 4, body, &default_broker())
        .expect_response("test request has acks != 0 and expects a response");
    let mut d = Decoder::new(resp);
    assert_eq!(d.read_i32().unwrap(), 0);
    assert_eq!(d.read_i32().unwrap(), 1);
    assert_eq!(d.read_nullable_string().unwrap(), Some(String::new()));
    assert_eq!(d.read_i32().unwrap(), 1);
    assert_eq!(d.read_i32().unwrap(), 0);
    assert_eq!(d.read_i16().unwrap(), ERROR_INVALID_REQUEST);
}

// ── ListOffsets v0 (no encodable representation in kafka_protocol) ─────────

#[test]
fn list_offsets_v0_with_topic_closes_connection() {
    let request_body = build_list_offsets_v0_request_with_topic_t();
    assert!(
        handle_request(API_KEY_LIST_OFFSETS, 0, request_body, &default_broker()).is_close(),
        "ListOffsets v0 has no encodable response shape and must close, even with a well-formed body"
    );
}

// ── Comprehensive scoped-API coverage (correlation id, boundary versions) ──

fn request_body_for_scoped_api(api_key: i16, name: &str, version: i16) -> Bytes {
    match api_key {
        API_KEY_METADATA => {
            if version >= 9 {
                build_metadata_all_topics_flexible(version)
            } else {
                build_metadata_all_topics_legacy(version)
            }
        }
        API_KEY_API_VERSIONS => {
            if version >= 3 {
                build_api_versions_flexible_request("iggy-test", "0.1.0")
            } else {
                Bytes::new()
            }
        }
        API_KEY_PRODUCE => {
            // `_or_skip` (not bare `load_fixture_body`) so `KAFKA_FIXTURES_REQUIRED=1` panics on
            // a missing fixture instead of this falling through to a synthetic body and CI
            // silently never exercising the real generated fixture.
            load_fixture_body_or_skip(api_key, name, version).unwrap_or_else(|| {
                if version >= 9 {
                    build_produce_flexible_body(1, 0)
                } else if version >= 3 {
                    build_produce_v3_body(1, 0)
                } else {
                    build_produce_v2_body(1, 0)
                }
            })
        }
        API_KEY_FETCH => load_fixture_body_or_skip(api_key, name, version)
            .unwrap_or_else(|| build_fetch_empty_topics_request(version)),
        API_KEY_LIST_OFFSETS => load_fixture_body_or_skip(api_key, name, version)
            .unwrap_or_else(|| build_list_offsets_request(version, "scope-topic", 0)),
        API_KEY_CREATE_TOPICS => build_create_topics_empty_request(version),
        _ => Bytes::new(),
    }
}

#[tokio::test]
async fn each_scoped_api_min_version_preserves_correlation_id_e2e() {
    let (addr, _shutdown) = spawn_test_server().await;

    for &(api_key, name, min_ver, _max_ver) in SCOPED_API_KEYS {
        let correlation_id = 10_000 + i32::from(api_key);
        let body = request_body_for_scoped_api(api_key, name, min_ver);
        let (corr, resp_body) = round_trip(addr, api_key, min_ver, correlation_id, &body).await;
        assert_eq!(
            corr, correlation_id,
            "{name} v{min_ver} correlation id must round-trip"
        );
        assert!(
            !resp_body.is_empty(),
            "{name} v{min_ver} must return non-empty body"
        );
    }
}

#[tokio::test]
async fn each_scoped_api_max_version_preserves_correlation_id_e2e() {
    let (addr, _shutdown) = spawn_test_server().await;

    for &(api_key, name, _min_ver, max_ver) in SCOPED_API_KEYS {
        let correlation_id = 20_000 + i32::from(api_key);
        let body = request_body_for_scoped_api(api_key, name, max_ver);
        let (corr, resp_body) = round_trip(addr, api_key, max_ver, correlation_id, &body).await;
        assert_eq!(
            corr, correlation_id,
            "{name} v{max_ver} correlation id must round-trip"
        );
        assert!(
            !resp_body.is_empty(),
            "{name} v{max_ver} must return non-empty body"
        );
    }
}

#[tokio::test]
async fn apiversions_v0_and_v2_e2e_return_success() {
    let (addr, _shutdown) = spawn_test_server().await;

    for version in [0i16, 2] {
        let correlation_id = 300 + i32::from(version);
        let (corr, body) =
            round_trip(addr, API_KEY_API_VERSIONS, version, correlation_id, &[]).await;
        assert_eq!(corr, correlation_id);
        let mut d = Decoder::new(body);
        assert_eq!(
            d.read_i16().unwrap(),
            ERROR_NONE,
            "ApiVersions v{version} must succeed"
        );
    }
}

#[tokio::test]
async fn apiversions_v4_out_of_range_e2e_returns_unsupported() {
    let (addr, _shutdown) = spawn_test_server().await;
    let (corr, body) = round_trip(addr, API_KEY_API_VERSIONS, 4, 350, &[]).await;
    assert_eq!(corr, 350);
    let mut d = Decoder::new(body);
    assert_eq!(
        d.read_i16().unwrap(),
        ERROR_UNSUPPORTED_VERSION,
        "ApiVersions v4 must return UNSUPPORTED_VERSION per KIP-511"
    );
}

// ── Out-of-scope API keys (SCOPE.md unsupported list) ───────────────────────

#[test]
fn out_of_scope_api_keys_close_without_panic() {
    for &(api_key, name) in OUT_OF_SCOPE_API_KEYS {
        let outcome = handle_request(api_key, 0, Bytes::new(), &default_broker());
        assert!(outcome.is_close(), "{name} (key {api_key}) must close");
    }
}

// ── Boundary versions keep the TCP session (except Metadata) ───────────────

#[tokio::test]
async fn each_scoped_api_above_max_version_e2e_closes_or_kip511() {
    let (addr, _shutdown) = spawn_test_server().await;
    let mut stream = TcpStream::connect(addr).await.expect("connect");

    for &(api_key, name, _min_ver, max_ver) in SCOPED_API_KEYS {
        let above = max_ver + 1;
        // Produce is decoded before the firewall check, so it needs a body with a readable acks
        // (built for the flexible wire version); the other APIs reject on version pre-decode.
        let body = request_body_for_scoped_api(api_key, name, above);
        let frame = build_request_frame(
            api_key,
            above,
            50_000 + i32::from(api_key),
            Some("scope-test"),
            &body,
        );
        stream
            .write_all(&frame)
            .await
            .unwrap_or_else(|_| panic!("write {name} v{above}"));
        if api_key == API_KEY_API_VERSIONS {
            // KIP-511: ApiVersions above max still answers with a v0 UNSUPPORTED_VERSION body.
            let payload = tcp::read_response_frame(&mut stream, 8 * 1024 * 1024).await;
            assert!(
                !payload.is_empty(),
                "{name} v{above} must still respond on wire"
            );
            continue;
        }
        // Other APIs: encoding at the client's raw version above our encoder max would omit
        // later-version fields, so Close is the honest contract (same as Metadata).
        assert_eq!(
            read_byte_with_timeout(&mut stream, Duration::from_secs(2)).await,
            ByteRead::Closed,
            "{name} v{above} must close the connection"
        );
        stream = TcpStream::connect(addr)
            .await
            .unwrap_or_else(|_| panic!("reconnect after {name} close"));
    }

    let ok = build_request_frame(API_KEY_API_VERSIONS, 1, 89_999, Some("scope-test"), &[]);
    stream.write_all(&ok).await.expect("recovery request");
    let payload = tcp::read_response_frame(&mut stream, 8 * 1024 * 1024).await;
    assert_eq!(
        parse_response_payload(API_KEY_API_VERSIONS, 1, payload).0,
        89_999
    );
}

#[tokio::test]
async fn each_scoped_api_below_min_version_e2e_closes_except_api_versions() {
    // Every scoped API's firewall min was chosen at or above `kafka_protocol`'s own schema
    // floor for that message (Produce 3, Fetch 4, ListOffsets 1's encoder actually starts at 1
    // but the crate's schema floor is 1 too - see below - Metadata 0, CreateTopics 2), so
    // `min_ver - 1` has no encodable response under the crate and closes for every API except
    // ApiVersions, which always answers per KIP-511 regardless of version validity.
    let (addr, _shutdown) = spawn_test_server().await;
    let mut stream = TcpStream::connect(addr).await.expect("connect");

    for &(api_key, name, min_ver, _max_ver) in SCOPED_API_KEYS {
        let below = min_ver - 1;
        let body = request_body_for_scoped_api(api_key, name, below);
        let frame = build_request_frame(
            api_key,
            below,
            40_000 + i32::from(api_key),
            Some("scope-test"),
            &body,
        );
        stream
            .write_all(&frame)
            .await
            .unwrap_or_else(|_| panic!("write {name} v{below}"));

        if api_key == API_KEY_API_VERSIONS {
            let payload = tcp::read_response_frame(&mut stream, 8 * 1024 * 1024).await;
            assert!(
                !payload.is_empty(),
                "ApiVersions v{below} must still respond per KIP-511"
            );
            continue;
        }

        assert_eq!(
            read_byte_with_timeout(&mut stream, Duration::from_secs(2)).await,
            ByteRead::Closed,
            "{name} v{below} has no encodable response shape and must close"
        );
        stream = TcpStream::connect(addr)
            .await
            .unwrap_or_else(|_| panic!("reconnect after {name} close"));
    }

    let ok = build_request_frame(API_KEY_API_VERSIONS, 1, 88_888, Some("scope-test"), &[]);
    stream.write_all(&ok).await.expect("recovery request");
    let payload = tcp::read_response_frame(&mut stream, 8 * 1024 * 1024).await;
    assert_eq!(
        parse_response_payload(API_KEY_API_VERSIONS, 1, payload).0,
        88_888
    );
}

#[test]
fn produce_advertises_min_zero_but_firewall_rejects_below_v3() {
    let range = SCOPED_API_KEYS
        .iter()
        .find(|(k, _, _, _)| *k == API_KEY_PRODUCE)
        .expect("produce in scope");
    let (_, _, firewall_min, _) = *range;
    assert_eq!(firewall_min, 3);
    assert_eq!(advertised_min_version(API_KEY_PRODUCE, firewall_min), 0);
    assert!(!is_supported_version(API_KEY_PRODUCE, 0));
    assert!(!is_supported_version(API_KEY_PRODUCE, 2));

    // acks=0 must keep the connection open even though no response is encodable at v2 - the
    // fire-and-forget wire-protocol rule outranks "no parseable response" here.
    assert!(
        handle_request(
            API_KEY_PRODUCE,
            2,
            build_produce_v2_body(0, 0),
            &default_broker(),
        )
        .is_no_response(),
        "Produce v2 acks=0 must stay silent, not close"
    );

    // acks != 0 has no encodable response at v2 and must close instead.
    assert!(
        handle_request(
            API_KEY_PRODUCE,
            2,
            build_produce_v2_body(1, 0),
            &default_broker(),
        )
        .is_close(),
        "Produce v2 acks != 0 has no encodable response shape and must close"
    );
}

// ── Unsupported-version e2e paths for remaining scoped APIs ─────────────────

#[tokio::test]
async fn list_offsets_v7_unsupported_e2e_closes_connection() {
    let (addr, _shutdown) = spawn_test_server().await;
    let mut stream = TcpStream::connect(addr).await.expect("connect");
    let frame = build_request_frame(
        API_KEY_LIST_OFFSETS,
        7,
        370,
        Some("scope-test"),
        &[0x00, 0x00, 0x00, 0x00],
    );
    stream.write_all(&frame).await.expect("write");
    assert_eq!(
        read_byte_with_timeout(&mut stream, Duration::from_secs(2)).await,
        ByteRead::Closed,
        "ListOffsets v7 is above encoder max and must close"
    );
}

#[tokio::test]
async fn create_topics_v1_unsupported_e2e_closes_connection() {
    // CreateTopics v1 is below both the firewall min (2) and `kafka_protocol`'s schema floor
    // (2-7) - no encodable response exists at this version.
    let (addr, _shutdown) = spawn_test_server().await;
    let mut stream = TcpStream::connect(addr).await.expect("connect");
    let frame = build_request_frame(API_KEY_CREATE_TOPICS, 1, 380, Some("scope-test"), &[]);
    stream
        .write_all(&frame)
        .await
        .expect("write create topics v1");
    assert_eq!(
        read_byte_with_timeout(&mut stream, Duration::from_secs(2)).await,
        ByteRead::Closed,
        "CreateTopics v1 has no encodable response shape and must close"
    );
}

// ── Corrupt decode paths for remaining scoped APIs ──────────────────────────

/// v1 (legacy, non-flexible) - the shape `api_handler_tests.rs:216`/`:234`'s structural decodes
/// don't cover (those exercise Fetch v12 and `ListOffsets` v6, both flexible). A version that's
/// `is_supported_version` (v1 is, `ListOffsets`' firewall floor) always decode-fails to
/// `ERROR_INVALID_REQUEST` specifically - never `ERROR_UNSUPPORTED_VERSION`, which
/// `handle_versioned_request` only reaches on the *unsupported-version* branch, not a decode
/// failure - so this is a precise decode, not a scan for either.
#[test]
fn corrupt_list_offsets_body_returns_invalid_request_error() {
    let body = Bytes::from_static(&[0xFF, 0xFF, 0xFF]);
    let resp = handle_request(API_KEY_LIST_OFFSETS, 1, body, &default_broker())
        .expect_response("test request has acks != 0 and expects a response");
    let mut d = Decoder::new(resp);
    // v1 has no throttle_time_ms (added v2+).
    assert_eq!(d.read_i32().unwrap(), 1, "topics len");
    assert_eq!(d.read_nullable_string().unwrap(), Some(String::new()));
    assert_eq!(d.read_i32().unwrap(), 1, "partitions len");
    assert_eq!(d.read_i32().unwrap(), 0, "partition_index");
    assert_eq!(d.read_i16().unwrap(), ERROR_INVALID_REQUEST);
}

/// v2 (legacy, non-flexible) - see `corrupt_list_offsets_body_returns_invalid_request_error`
/// above for why this is a precise decode of `ERROR_INVALID_REQUEST`, not a scan for either code.
#[test]
fn corrupt_create_topics_body_returns_invalid_request_error() {
    let body = Bytes::from_static(&[0xFF, 0xFF, 0xFF]);
    let resp = handle_request(API_KEY_CREATE_TOPICS, 2, body, &default_broker())
        .expect_response("test request has acks != 0 and expects a response");
    let mut d = Decoder::new(resp);
    assert_eq!(d.read_i32().unwrap(), 0, "throttle");
    assert_eq!(d.read_i32().unwrap(), 1, "topics len");
    assert_eq!(d.read_nullable_string().unwrap(), Some(String::new()));
    assert_eq!(d.read_i16().unwrap(), ERROR_INVALID_REQUEST);
}
