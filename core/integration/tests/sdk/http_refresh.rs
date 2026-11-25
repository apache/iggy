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

//! End-to-end coverage for the SDK HTTP client's `refresh_access_token`
//! against a live the server listener: the reissued token must replace the one
//! the client holds and keep it authenticated.

use iggy::http::http_client::HttpClient;
use iggy::prelude::*;
use integration::iggy_harness;
use std::time::Duration;

#[iggy_harness]
async fn given_logged_in_http_client_when_refreshing_should_swap_to_a_working_token(
    harness: &TestHarness,
) {
    let addr = harness
        .server()
        .http_addr()
        .expect("HTTP transport not configured on test server");
    let client = HttpClient::new(&format!("http://{addr}")).expect("build SDK HTTP client");

    let logged_in = client
        .login_user(DEFAULT_ROOT_USERNAME, DEFAULT_ROOT_PASSWORD)
        .await
        .expect("login as root");
    let login_token = logged_in.access_token.expect("login returns a token");

    let refreshed = client
        .refresh_access_token()
        .await
        .expect("refresh the access token");
    let refreshed_token = refreshed.access_token.expect("refresh returns a token");

    assert_eq!(
        refreshed.user_id, logged_in.user_id,
        "refresh must re-issue for the same user"
    );
    assert_ne!(
        refreshed_token.token, login_token.token,
        "refresh must mint a token distinct from the one it replaced"
    );

    // The client swapped its stored bearer to the reissued token; an authed
    // call carries that stored token, so success proves the swap landed.
    client
        .get_stats()
        .await
        .expect("authed call after refresh must use the reissued token");
}

#[iggy_harness]
async fn given_a_personal_access_token_exchange_when_refreshed_should_keep_the_source_expiry_and_revocation(
    harness: &TestHarness,
) {
    let owner = harness.tcp_root_client().await.expect("root");
    let source = owner
        .create_personal_access_token(
            "refresh-source",
            PersonalAccessTokenExpiry::ExpireDuration(IggyDuration::from(Duration::from_secs(120))),
        )
        .await
        .expect("create an expiring PAT");
    let expiry = owner
        .get_personal_access_tokens()
        .await
        .expect("list tokens")
        .into_iter()
        .find(|token| token.name == "refresh-source")
        .expect("source PAT")
        .expiry_at
        .expect("source deadline")
        .to_secs();
    let address = harness.server().http_addr().expect("HTTP endpoint");
    let client = HttpClient::new(&format!("http://{address}")).expect("HTTP client");
    let exchanged = client
        .login_with_personal_access_token(&source.token)
        .await
        .expect("exchange the PAT")
        .access_token
        .expect("access token");
    assert_eq!(exchanged.expiry, expiry);
    let refreshed = client
        .refresh_access_token()
        .await
        .expect("refresh")
        .access_token
        .expect("access token");
    assert_eq!(refreshed.expiry, expiry);
    assert!(
        refreshed.token != exchanged.token,
        "refresh issues a new token"
    );
    client.get_stats().await.expect("refreshed token is usable");

    owner
        .delete_personal_access_token("refresh-source")
        .await
        .expect("delete the source PAT");
    assert!(matches!(
        client.refresh_access_token().await,
        Err(IggyError::Unauthenticated)
    ));
    assert!(matches!(
        client.get_stats().await,
        Err(IggyError::Unauthenticated)
    ));
    owner
        .get_stats()
        .await
        .expect("the password session stays usable");
}
