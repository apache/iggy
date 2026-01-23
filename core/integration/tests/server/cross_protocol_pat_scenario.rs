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
use integration::iggy_harness;
use test_case::test_matrix;

const PAT_NAME: &str = "cross-protocol-test-pat";
const CLIENT_COUNT: usize = 20;

fn http_to_tcp() -> bool {
    true
}
fn tcp_to_http() -> bool {
    false
}

/// Test full PAT lifecycle with cross-protocol operations.
/// Verifies cross-shard event broadcasting works for both create and delete.
///
/// - `http_to_tcp`: Create via HTTP → Verify via TCP (20 clients) → Delete via TCP → Verify via HTTP
/// - `tcp_to_http`: Create via TCP → Verify via HTTP → Delete via HTTP → Verify via TCP (20 clients)
#[iggy_harness(server(tcp.socket.nodelay = true))]
#[test_matrix([http_to_tcp(), tcp_to_http()])]
async fn cross_protocol_pat_lifecycle(create_via_http: bool, harness: &TestHarness) {
    if create_via_http {
        // Create via HTTP → Verify via TCP (20 clients) → Delete via TCP → Verify via HTTP
        let http_client = harness.http_root_client().await.unwrap();

        let created_pat = http_client
            .create_personal_access_token(PAT_NAME, IggyExpiry::NeverExpire)
            .await
            .expect("Failed to create PAT via HTTP");
        assert!(!created_pat.token.is_empty());

        // Verify creation visible via multiple TCP clients (they may land on different shards)
        let mut create_failures = Vec::new();
        for i in 0..CLIENT_COUNT {
            let tcp_client = harness.tcp_root_client().await.unwrap();
            let tcp_pats = tcp_client
                .get_personal_access_tokens()
                .await
                .expect("Failed to get PATs via TCP");

            if tcp_pats.len() != 1 {
                create_failures.push(format!(
                    "TCP client {} saw {} PATs (expected 1)",
                    i,
                    tcp_pats.len()
                ));
            } else if tcp_pats[0].name != PAT_NAME {
                create_failures.push(format!(
                    "TCP client {} saw wrong PAT name: {}",
                    i, tcp_pats[0].name
                ));
            }
        }
        assert!(
            create_failures.is_empty(),
            "PAT creation visibility failures ({}/{} clients):\n{}",
            create_failures.len(),
            CLIENT_COUNT,
            create_failures.join("\n")
        );

        // Delete via TCP (cross-protocol)
        let tcp_client = harness.tcp_root_client().await.unwrap();
        tcp_client
            .delete_personal_access_token(PAT_NAME)
            .await
            .expect("Failed to delete PAT via TCP");

        // Verify deletion visible via HTTP
        let http_pats = http_client
            .get_personal_access_tokens()
            .await
            .expect("Failed to get PATs via HTTP after delete");
        assert!(
            http_pats.is_empty(),
            "PAT still visible via HTTP after TCP deletion"
        );
    } else {
        // Create via TCP → Verify via HTTP → Delete via HTTP → Verify via TCP (20 clients)
        let tcp_client = harness.tcp_root_client().await.unwrap();

        let created_pat = tcp_client
            .create_personal_access_token(PAT_NAME, IggyExpiry::NeverExpire)
            .await
            .expect("Failed to create PAT via TCP");
        assert!(!created_pat.token.is_empty());

        // Verify creation visible via HTTP
        let http_client = harness.http_root_client().await.unwrap();
        let http_pats = http_client
            .get_personal_access_tokens()
            .await
            .expect("Failed to get PATs via HTTP");
        assert_eq!(
            http_pats.len(),
            1,
            "PAT not visible via HTTP after TCP creation"
        );
        assert_eq!(http_pats[0].name, PAT_NAME);

        // Delete via HTTP (cross-protocol)
        http_client
            .delete_personal_access_token(PAT_NAME)
            .await
            .expect("Failed to delete PAT via HTTP");

        // Verify deletion visible via multiple TCP clients (they may land on different shards)
        let mut delete_failures = Vec::new();
        for i in 0..CLIENT_COUNT {
            let tcp_check_client = harness.tcp_root_client().await.unwrap();
            let tcp_pats = tcp_check_client
                .get_personal_access_tokens()
                .await
                .expect("Failed to get PATs via TCP after delete");

            if !tcp_pats.is_empty() {
                delete_failures.push(format!(
                    "TCP client {} still sees {} PATs after deletion",
                    i,
                    tcp_pats.len()
                ));
            }
        }
        assert!(
            delete_failures.is_empty(),
            "PAT deletion visibility failures ({}/{} clients):\n{}",
            delete_failures.len(),
            CLIENT_COUNT,
            delete_failures.join("\n")
        );
    }
}
