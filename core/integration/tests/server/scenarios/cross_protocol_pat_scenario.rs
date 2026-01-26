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
use integration::{
    http_client::HttpClientFactory,
    tcp_client::TcpClientFactory,
    test_server::{ClientFactory, IpAddrKind, TestServer, login_root},
};
use std::collections::HashMap;

const PAT_NAME: &str = "cross-protocol-test-pat";
const TCP_CLIENT_COUNT: usize = 20;

/// Create PAT via HTTP, list via multiple TCP connections.
pub async fn run() {
    let mut extra_envs = HashMap::new();
    extra_envs.insert(
        "IGGY_TCP_SOCKET_OVERRIDE_DEFAULTS".to_string(),
        "true".to_string(),
    );
    extra_envs.insert("IGGY_TCP_SOCKET_NODELAY".to_string(), "true".to_string());

    let mut test_server = TestServer::new(Some(extra_envs), true, None, IpAddrKind::V4);
    test_server.start();

    let http_addr = test_server
        .get_http_api_addr()
        .expect("HTTP address not available");
    let tcp_addr = test_server
        .get_raw_tcp_addr()
        .expect("TCP address not available");

    let http_factory = HttpClientFactory {
        server_addr: http_addr,
    };
    let http_client = create_client(&http_factory).await;
    login_root(&http_client).await;

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

    let tcp_factory = TcpClientFactory {
        server_addr: tcp_addr,
        ..Default::default()
    };

    let mut failures = Vec::new();

    for i in 0..TCP_CLIENT_COUNT {
        let tcp_client = create_client(&tcp_factory).await;
        login_root(&tcp_client).await;

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
pub async fn run_tcp_to_http() {
    let mut extra_envs = HashMap::new();
    extra_envs.insert(
        "IGGY_TCP_SOCKET_OVERRIDE_DEFAULTS".to_string(),
        "true".to_string(),
    );
    extra_envs.insert("IGGY_TCP_SOCKET_NODELAY".to_string(), "true".to_string());

    let mut test_server = TestServer::new(Some(extra_envs), true, None, IpAddrKind::V4);
    test_server.start();

    let http_addr = test_server
        .get_http_api_addr()
        .expect("HTTP address not available");
    let tcp_addr = test_server
        .get_raw_tcp_addr()
        .expect("TCP address not available");

    let tcp_factory = TcpClientFactory {
        server_addr: tcp_addr,
        ..Default::default()
    };
    let tcp_client = create_client(&tcp_factory).await;
    login_root(&tcp_client).await;

    let created_pat = tcp_client
        .create_personal_access_token(PAT_NAME, IggyExpiry::NeverExpire)
        .await
        .expect("Failed to create PAT via TCP");

    assert!(!created_pat.token.is_empty());

    let http_factory = HttpClientFactory {
        server_addr: http_addr,
    };
    let http_client = create_client(&http_factory).await;
    login_root(&http_client).await;

    let http_pats = http_client
        .get_personal_access_tokens()
        .await
        .expect("Failed to get PATs via HTTP");

    assert_eq!(http_pats.len(), 1);
    assert_eq!(http_pats[0].name, PAT_NAME);

    let _ = tcp_client.delete_personal_access_token(PAT_NAME).await;
}

/// Create via TCP, delete via HTTP, verify deletion visible via TCP.
pub async fn run_delete_visibility() {
    let mut extra_envs = HashMap::new();
    extra_envs.insert(
        "IGGY_TCP_SOCKET_OVERRIDE_DEFAULTS".to_string(),
        "true".to_string(),
    );
    extra_envs.insert("IGGY_TCP_SOCKET_NODELAY".to_string(), "true".to_string());

    let mut test_server = TestServer::new(Some(extra_envs), true, None, IpAddrKind::V4);
    test_server.start();

    let http_addr = test_server
        .get_http_api_addr()
        .expect("HTTP address not available");
    let tcp_addr = test_server
        .get_raw_tcp_addr()
        .expect("TCP address not available");

    let tcp_factory = TcpClientFactory {
        server_addr: tcp_addr,
        ..Default::default()
    };
    let tcp_client = create_client(&tcp_factory).await;
    login_root(&tcp_client).await;

    tcp_client
        .create_personal_access_token(PAT_NAME, IggyExpiry::NeverExpire)
        .await
        .expect("Failed to create PAT via TCP");

    let http_factory = HttpClientFactory {
        server_addr: http_addr,
    };
    let http_client = create_client(&http_factory).await;
    login_root(&http_client).await;

    http_client
        .delete_personal_access_token(PAT_NAME)
        .await
        .expect("Failed to delete PAT via HTTP");

    let mut still_visible_count = 0;
    for _ in 0..TCP_CLIENT_COUNT {
        let tcp_check_client = create_client(&tcp_factory).await;
        login_root(&tcp_check_client).await;

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

async fn create_client(client_factory: &dyn ClientFactory) -> IggyClient {
    let client = client_factory.create_client().await;
    IggyClient::create(client, None, None)
}
