/* Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

//! Regression test for HTTP handlers not broadcasting PAT events to other shards.
//! TCP connections may land on any shard via SO_REUSEPORT, so without broadcast
//! they won't see PATs created via HTTP (which runs only on shard 0).

use iggy::prelude::*;
use iggy_common::TransportProtocol;
use integration::harness::TestHarness;
use integration::iggy_harness;

const PAT_NAME: &str = "cross-protocol-test-pat";
const TCP_CLIENT_COUNT: usize = 20;

async fn create_root_client(harness: &TestHarness, transport: TransportProtocol) -> IggyClient {
    harness
        .root_client_for(transport)
        .await
        .expect("Failed to create root client")
}

/// Create PAT via HTTP, list via multiple TCP connections.
#[iggy_harness(server(
    tcp.socket.override_defaults = true,
    tcp.socket.nodelay = true
))]
pub async fn should_see_pat_created_via_http_when_listing_via_tcp(harness: &TestHarness) {
    let http_client = create_root_client(harness, TransportProtocol::Http).await;

    let created_pat = http_client
        .create_personal_access_token(PAT_NAME, IggyExpiry::NeverExpire)
        .await
        .expect("Failed to create PAT via HTTP");

    assert!(!created_pat.token.is_empty());

    let http_pats = http_client
        .get_personal_access_tokens()
        .await
        .expect("Failed to get PATs via HTTP");
    assert_eq!(http_pats.len(), 1);
    assert_eq!(http_pats[0].name, PAT_NAME);

    let mut failures = Vec::new();

    for i in 0..TCP_CLIENT_COUNT {
        let tcp_client = create_root_client(harness, TransportProtocol::Tcp).await;

        let tcp_pats = tcp_client
            .get_personal_access_tokens()
            .await
            .expect("Failed to get PATs via TCP");

        if tcp_pats.is_empty() {
            failures.push(format!("TCP client {} saw 0 PATs (expected 1)", i));
        } else if tcp_pats.len() != 1 {
            failures.push(format!(
                "TCP client {} saw {} PATs (expected 1)",
                i,
                tcp_pats.len()
            ));
        } else if tcp_pats[0].name != PAT_NAME {
            failures.push(format!(
                "TCP client {} saw wrong PAT name: {}",
                i, tcp_pats[0].name
            ));
        }
    }

    let _ = http_client.delete_personal_access_token(PAT_NAME).await;

    assert!(
        failures.is_empty(),
        "Cross-protocol PAT visibility failures ({}/{} clients failed):\n{}",
        failures.len(),
        TCP_CLIENT_COUNT,
        failures.join("\n")
    );
}

/// Create PAT via TCP, list via HTTP. Should work since TCP handlers broadcast.
#[iggy_harness(server(
    tcp.socket.override_defaults = true,
    tcp.socket.nodelay = true
))]
pub async fn should_see_pat_created_via_tcp_when_listing_via_http(harness: &TestHarness) {
    let tcp_client = create_root_client(harness, TransportProtocol::Tcp).await;

    let created_pat = tcp_client
        .create_personal_access_token(PAT_NAME, IggyExpiry::NeverExpire)
        .await
        .expect("Failed to create PAT via TCP");

    assert!(!created_pat.token.is_empty());

    let http_client = create_root_client(harness, TransportProtocol::Http).await;

    let http_pats = http_client
        .get_personal_access_tokens()
        .await
        .expect("Failed to get PATs via HTTP");

    assert_eq!(http_pats.len(), 1);
    assert_eq!(http_pats[0].name, PAT_NAME);

    let _ = tcp_client.delete_personal_access_token(PAT_NAME).await;
}

/// Create via TCP, delete via HTTP, verify deletion visible via TCP.
#[iggy_harness(server(
    tcp.socket.override_defaults = true,
    tcp.socket.nodelay = true
))]
pub async fn should_not_see_pat_deleted_via_http_when_listing_via_tcp(harness: &TestHarness) {
    let tcp_client = create_root_client(harness, TransportProtocol::Tcp).await;

    tcp_client
        .create_personal_access_token(PAT_NAME, IggyExpiry::NeverExpire)
        .await
        .expect("Failed to create PAT via TCP");

    let http_client = create_root_client(harness, TransportProtocol::Http).await;

    http_client
        .delete_personal_access_token(PAT_NAME)
        .await
        .expect("Failed to delete PAT via HTTP");

    let mut still_visible_count = 0;
    for _ in 0..TCP_CLIENT_COUNT {
        let tcp_check_client = create_root_client(harness, TransportProtocol::Tcp).await;

        let tcp_pats = tcp_check_client
            .get_personal_access_tokens()
            .await
            .expect("Failed to get PATs via TCP");

        if !tcp_pats.is_empty() {
            still_visible_count += 1;
        }
    }

    assert_eq!(
        still_visible_count, 0,
        "Deleted PAT still visible to {} TCP clients",
        still_visible_count
    );
}
