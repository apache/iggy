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

use bytes::Bytes;
use iggy::prelude::*;
use iggy_binary_protocol::WireEncode;
use iggy_binary_protocol::codes::{GET_STATS_CODE, LOGIN_USER_CODE, PING_CODE};
use iggy_binary_protocol::requests::system::{GetStatsRequest, PingRequest};
use integration::iggy_harness;

/// A code no server registers a handler for, well past every range the protocol
/// assigns, so it stays a vendor code as the command table grows.
const VENDOR_CODE: u32 = 60_001;

const KINDS: [BinaryRequestKind; 2] = [
    BinaryRequestKind::NonReplicated,
    BinaryRequestKind::Replicated,
];

#[cfg(not(feature = "vsr"))]
#[iggy_harness(test_client_transport = [Tcp, Quic, Http, WebSocket])]
async fn given_authenticated_client_when_sending_raw_request_should_round_trip(
    harness: &TestHarness,
) {
    let client = harness.root_client().await.unwrap();
    assert_raw_round_trip(&client).await;
}

#[cfg(feature = "vsr")]
#[iggy_harness(test_client_transport = [Tcp, WebSocket, Quic])]
async fn given_authenticated_client_when_sending_raw_request_should_round_trip(
    harness: &TestHarness,
) {
    let client = harness.new_client().await.unwrap();
    client
        .login_user(DEFAULT_ROOT_USERNAME, DEFAULT_ROOT_PASSWORD)
        .await
        .unwrap();
    assert_raw_round_trip(&client).await;
}

/// Each transport answers its own raw method and returns `FeatureUnavailable`
/// for the other's.
async fn assert_raw_round_trip(client: &IggyClient) {
    match client.get_connection_info().await.protocol {
        TransportProtocol::Http => {
            client
                .send_http_request(HttpMethod::Get, "/ping", None)
                .await
                .expect("HTTP ping request should succeed");

            let stats = client
                .send_http_request(HttpMethod::Get, "/stats", None)
                .await
                .expect("authenticated HTTP request should return a body");
            assert!(!stats.is_empty());

            for kind in KINDS {
                let error = client
                    .send_binary_request(kind, PING_CODE, PingRequest.to_bytes())
                    .await
                    .expect_err("binary command must be unavailable on HTTP");
                assert_eq!(error, IggyError::FeatureUnavailable);
            }
        }
        _ => {
            assert_standard_codes_round_trip(client).await;
            assert_session_control_codes_are_rejected(client).await;
            assert_vendor_code_reaches_the_server(client).await;
            assert_replicated_declaration_is_honored(client).await;
            assert_conflicting_declaration_is_honored(client).await;

            let error = client
                .send_http_request(HttpMethod::Get, "/ping", None)
                .await
                .expect_err("HTTP request must be unavailable on binary transports");
            assert_eq!(error, IggyError::FeatureUnavailable);
        }
    }
}

async fn assert_standard_codes_round_trip(client: &IggyClient) {
    let response = client
        .send_binary_request(
            BinaryRequestKind::NonReplicated,
            PING_CODE,
            PingRequest.to_bytes(),
        )
        .await
        .expect("binary ping request should succeed");
    assert!(response.is_empty());

    let stats = client
        .send_binary_request(
            BinaryRequestKind::NonReplicated,
            GET_STATS_CODE,
            GetStatsRequest.to_bytes(),
        )
        .await
        .expect("authenticated binary command should return a body");
    assert!(!stats.is_empty());
}

/// Rejected before the wire whatever the declaration, which also guards the VSR
/// consensus-session panic a raw login on a bound client used to trigger.
async fn assert_session_control_codes_are_rejected(client: &IggyClient) {
    for kind in KINDS {
        let error = client
            .send_binary_request(kind, LOGIN_USER_CODE, Bytes::new())
            .await
            .expect_err("session-control codes must be rejected by the raw path");
        assert_eq!(error, IggyError::InvalidCommand);
    }
}

/// A non-replicated vendor code leaves the SDK, so the rejection comes from the
/// server rather than the encoder, and the connection survives it.
async fn assert_vendor_code_reaches_the_server(client: &IggyClient) {
    let error = client
        .send_binary_request(
            BinaryRequestKind::NonReplicated,
            VENDOR_CODE,
            Bytes::from_static(b"vendor-body"),
        )
        .await
        .expect_err("no server registers a handler for the vendor code");
    assert_eq!(error, IggyError::InvalidCommand);

    client
        .ping()
        .await
        .expect("connection must stay usable after a request-level rejection");
}

/// Under VSR a replicated vendor code has no deterministic handler registry, so
/// it fails in the encoder. Classic framing has no operation field, so the same
/// call reaches the server and is rejected there.
async fn assert_replicated_declaration_is_honored(client: &IggyClient) {
    let error = client
        .send_binary_request(
            BinaryRequestKind::Replicated,
            VENDOR_CODE,
            Bytes::from_static(b"vendor-body"),
        )
        .await
        .expect_err("a replicated vendor code has no handler on either protocol");

    #[cfg(feature = "vsr")]
    assert_eq!(error, IggyError::FeatureUnavailable);
    #[cfg(not(feature = "vsr"))]
    assert_eq!(error, IggyError::InvalidCommand);

    client
        .ping()
        .await
        .expect("connection must stay usable after a rejected declaration");
}

/// A declaration that disagrees with a standard code cannot redirect it. Under
/// VSR the encoder rejects the pair; classic framing ignores the declaration and
/// emits the same bytes it would for the matching one.
async fn assert_conflicting_declaration_is_honored(client: &IggyClient) {
    let result = client
        .send_binary_request(
            BinaryRequestKind::Replicated,
            GET_STATS_CODE,
            GetStatsRequest.to_bytes(),
        )
        .await;

    #[cfg(feature = "vsr")]
    assert_eq!(
        result.expect_err("a read declared replicated must be rejected"),
        IggyError::InvalidCommand
    );
    #[cfg(not(feature = "vsr"))]
    assert!(
        !result
            .expect("classic framing ignores the declaration")
            .is_empty()
    );
}
