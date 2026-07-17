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

            let error = client
                .send_binary_request(PING_CODE, PingRequest.to_bytes())
                .await
                .expect_err("binary command must be unavailable on HTTP");
            assert_eq!(error, IggyError::FeatureUnavailable);
        }
        _ => {
            let response = client
                .send_binary_request(PING_CODE, PingRequest.to_bytes())
                .await
                .expect("binary ping request should succeed");
            assert!(response.is_empty());

            let stats = client
                .send_binary_request(GET_STATS_CODE, GetStatsRequest.to_bytes())
                .await
                .expect("authenticated binary command should return a body");
            assert!(!stats.is_empty());

            // Rejected before the wire (guards the VSR consensus-session panic
            // when a login code is sent raw on a bound client).
            let error = client
                .send_binary_request(LOGIN_USER_CODE, Bytes::new())
                .await
                .expect_err("session-control codes must be rejected by the raw path");
            assert_eq!(error, IggyError::InvalidCommand);

            // VSR encoder is closed-world: unknown code rejected at encode time.
            #[cfg(feature = "vsr")]
            {
                let error = client
                    .send_binary_request(60_000, Bytes::new())
                    .await
                    .expect_err("unknown code must be rejected under VSR");
                assert_eq!(error, IggyError::InvalidCommand);
            }

            let error = client
                .send_http_request(HttpMethod::Get, "/ping", None)
                .await
                .expect_err("HTTP request must be unavailable on binary transports");
            assert_eq!(error, IggyError::FeatureUnavailable);
        }
    }
}
