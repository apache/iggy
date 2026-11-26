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

use iggy::prelude::locking::IggyRwLockFn;
use iggy::prelude::*;
use iggy_common::VsrSessionControl;
use integration::harness::TestHarness;
use integration::iggy_harness;
use std::time::Duration;
use tokio::time::{Instant, sleep};

const USERNAME: &str = "alice";
const PASSWORD: &str = "first-password";
const NEW_PASSWORD: &str = "second-password";
const EXPIRY: Duration = Duration::from_secs(5);
const EXPIRY_BUDGET: Duration = Duration::from_secs(20);
const RETRY_PAUSE: Duration = Duration::from_millis(200);

#[iggy_harness(test_client_transport = [Tcp, Http, Quic, WebSocket])]
async fn given_two_sessions_when_an_admin_changes_the_password_should_end_both(
    harness: &TestHarness,
) {
    let root = harness.root_client().await.expect("root client");
    create_user(&root).await;
    let first = signed_in(harness, PASSWORD).await;
    let second = signed_in(harness, PASSWORD).await;

    root.change_password(&alice(), PASSWORD, NEW_PASSWORD)
        .await
        .expect("an admin changes the password");

    for client in [&first, &second] {
        assert_refused(client.get_user(&alice()).await);
    }
    root.get_streams()
        .await
        .expect("the administrator's session survives");
    assert_sign_in_refused(sign_in(harness, PASSWORD).await.map(|_| ()));
    signed_in(harness, NEW_PASSWORD)
        .await
        .get_user(&alice())
        .await
        .expect("the new password signs in");
}

#[iggy_harness(test_client_transport = [Tcp, Http, Quic, WebSocket])]
async fn given_a_signed_in_user_when_changing_its_own_password_should_commit_and_need_the_new_one(
    harness: &TestHarness,
) {
    let root = harness.root_client().await.expect("root client");
    create_user(&root).await;
    let client = signed_in(harness, PASSWORD).await;
    let session = binary_session(&client).await;

    client
        .change_password(&alice(), PASSWORD, NEW_PASSWORD)
        .await
        .expect("a self change answers its committed result");

    let next = client.get_user(&alice()).await;
    if let Some(original) = session {
        next.expect("the submitting binary session stays usable");
        assert_eq!(binary_session(&client).await, Some(original));
    } else {
        assert_refused(next);
    }
    assert_sign_in_refused(sign_in(harness, PASSWORD).await.map(|_| ()));
    signed_in(harness, NEW_PASSWORD).await;
}

#[iggy_harness(test_client_transport = [Tcp, Http, Quic, WebSocket])]
async fn given_sessions_of_two_tokens_when_one_is_deleted_should_end_only_its_sessions(
    harness: &TestHarness,
) {
    let root = harness.root_client().await.expect("root client");
    create_user(&root).await;
    let owner = signed_in(harness, PASSWORD).await;
    let ci = create_token(&owner, "ci-token", PersonalAccessTokenExpiry::NeverExpire).await;
    let deploy = create_token(&owner, "deploy", PersonalAccessTokenExpiry::NeverExpire).await;
    let ci_client = token_client(harness, &ci).await;
    let deploy_client = token_client(harness, &deploy).await;

    owner
        .delete_personal_access_token("ci-token")
        .await
        .expect("delete the ci-token token");

    assert_refused(ci_client.get_user(&alice()).await);
    deploy_client
        .get_user(&alice())
        .await
        .expect("another token's session survives");
    owner
        .get_user(&alice())
        .await
        .expect("the password session survives");
    let replaced = create_token(&owner, "ci-token", PersonalAccessTokenExpiry::NeverExpire).await;
    assert_refused(
        harness
            .new_client()
            .await
            .expect("client")
            .login_with_personal_access_token(&ci)
            .await
            .map(|_| ()),
    );
    token_client(harness, &replaced)
        .await
        .get_user(&alice())
        .await
        .expect("a token created under the same name is a new token");
}

#[iggy_harness(test_client_transport = [Tcp, Http, Quic, WebSocket])]
async fn given_token_session_when_the_password_changes_should_end_it_and_sign_in_again(
    harness: &TestHarness,
) {
    let root = harness.root_client().await.expect("root client");
    create_user(&root).await;
    let owner = signed_in(harness, PASSWORD).await;
    let token = create_token(&owner, "service", PersonalAccessTokenExpiry::NeverExpire).await;
    let service = token_client(harness, &token).await;
    let session = binary_session(&service).await;

    root.change_password(&alice(), PASSWORD, NEW_PASSWORD)
        .await
        .expect("an admin changes the password");

    // A binary SDK signs in again with the token, which survives the change.
    // An HTTP client holds a token exchanged before the change, which is refused.
    let next = service.get_user(&alice()).await;
    if let Some(original) = session {
        next.expect("the binary SDK signs in again with the token");
        assert_ne!(binary_session(&service).await, Some(original));
    } else {
        assert_refused(next);
    }
    token_client(harness, &token)
        .await
        .get_user(&alice())
        .await
        .expect("the token signs in after the change");
}

#[iggy_harness(
    test_client_transport = [Tcp, Http, Quic, WebSocket],
    server(personal_access_token.cleaner.enabled = false)
)]
async fn given_expiring_token_when_no_cleaner_runs_should_stop_serving_at_expiry(
    harness: &TestHarness,
) {
    let root = harness.root_client().await.expect("root client");
    create_user(&root).await;
    let owner = signed_in(harness, PASSWORD).await;
    let token = create_token(
        &owner,
        "short-lived",
        PersonalAccessTokenExpiry::ExpireDuration(IggyDuration::from(EXPIRY)),
    )
    .await;
    let created = Instant::now();
    let client = token_client(harness, &token).await;
    client
        .get_user(&alice())
        .await
        .expect("the token serves before its expiry");

    let deadline = created + EXPIRY_BUDGET;
    let refused_at = loop {
        match client.get_user(&alice()).await {
            Ok(_) => {
                assert!(
                    Instant::now() < deadline,
                    "the session still served {EXPIRY_BUDGET:?} after its token was created"
                );
                sleep(RETRY_PAUSE).await;
            }
            Err(error) => {
                assert_refused::<()>(Err(error));
                break Instant::now();
            }
        }
    };

    assert!(
        refused_at >= created + EXPIRY - Duration::from_secs(1),
        "the token was refused {:?} after its creation, before its expiry",
        refused_at - created
    );
    owner
        .get_user(&alice())
        .await
        .expect("the password session survives the token's expiry");
}

#[iggy_harness(test_client_transport = [Tcp, Http, Quic, WebSocket])]
async fn given_session_from_before_a_deactivation_when_reactivated_should_stay_ended(
    harness: &TestHarness,
) {
    let root = harness.root_client().await.expect("root client");
    create_user(&root).await;
    let client = signed_in(harness, PASSWORD).await;
    let session = binary_session(&client).await;
    let token = create_token(&client, "service", PersonalAccessTokenExpiry::NeverExpire).await;
    let service = token_client(harness, &token).await;
    let status = |status| UserUpdateOptions {
        status: Some(status),
        ..UserUpdateOptions::default()
    };

    root.update_user(&alice(), &status(UserStatus::Inactive))
        .await
        .expect("deactivate");
    root.update_user(&alice(), &status(UserStatus::Active))
        .await
        .expect("reactivate");

    // A binary SDK signs in again with the unchanged password. An HTTP client
    // holds a token issued before the deactivation, which is refused.
    let next = client.get_user(&alice()).await;
    if let Some(original) = session {
        next.expect("the binary SDK signs in again after the reactivation");
        assert_ne!(binary_session(&client).await, Some(original));
    } else {
        assert_refused(next);
    }
    assert_refused(service.get_user(&alice()).await);
    assert_refused(
        harness
            .new_client()
            .await
            .expect("client")
            .login_with_personal_access_token(&token)
            .await
            .map(|_| ()),
    );
    signed_in(harness, PASSWORD).await;
}

#[iggy_harness(test_client_transport = [Tcp], cluster_nodes = [3])]
async fn given_sessions_on_other_nodes_when_the_password_changes_should_end_them_on_every_node(
    harness: &TestHarness,
) {
    let root = harness.root_client().await.expect("root client");
    create_user(&root).await;
    let mut followers = Vec::new();
    for node in [1, 2] {
        let client = harness
            .node(node)
            .tcp_client()
            .expect("tcp client builder")
            .connect()
            .await
            .expect("connect to the node");
        // The node verifies the password against the users it has applied.
        let deadline = Instant::now() + EXPIRY_BUDGET;
        while let Err(error) = client.login_user(USERNAME, PASSWORD).await {
            assert!(
                Instant::now() < deadline,
                "node {node} did not apply the new user: {error}"
            );
            sleep(RETRY_PAUSE).await;
        }
        followers.push(client);
    }

    root.change_password(&alice(), PASSWORD, NEW_PASSWORD)
        .await
        .expect("an admin changes the password through node 0");

    for client in &followers {
        let deadline = Instant::now() + EXPIRY_BUDGET;
        loop {
            match client.get_user(&alice()).await {
                Ok(_) => {
                    assert!(
                        Instant::now() < deadline,
                        "a node still served the old session {EXPIRY_BUDGET:?} after the change"
                    );
                    sleep(RETRY_PAUSE).await;
                }
                Err(error) => {
                    assert_refused::<()>(Err(error));
                    break;
                }
            }
        }
    }
}

async fn binary_session(client: &IggyClient) -> Option<(u128, u64)> {
    let handle = client.client();
    let transport = handle.read().await;
    let identity = match &*transport {
        ClientWrapper::Tcp(tcp) => tcp.session_identity().await,
        ClientWrapper::Quic(quic) => quic.session_identity().await,
        ClientWrapper::WebSocket(websocket) => websocket.session_identity().await,
        ClientWrapper::Http(_) => return None,
        ClientWrapper::Iggy(_) => panic!("the harness builds a transport client"),
    }
    .expect("bound binary session");
    Some((identity.client_id, identity.session))
}

fn alice() -> Identifier {
    Identifier::named(USERNAME).expect("valid username")
}

async fn create_user(root: &IggyClient) {
    root.create_user(
        USERNAME,
        PASSWORD,
        UserStatus::Active,
        None,
        &UserCreateOptions::default(),
    )
    .await
    .expect("create alice");
}

async fn sign_in(harness: &TestHarness, password: &str) -> Result<IggyClient, IggyError> {
    let client = harness.new_client().await.expect("client");
    client.login_user(USERNAME, password).await?;
    Ok(client)
}

async fn signed_in(harness: &TestHarness, password: &str) -> IggyClient {
    sign_in(harness, password).await.expect("sign in alice")
}

async fn create_token(
    client: &IggyClient,
    name: &str,
    expiry: PersonalAccessTokenExpiry,
) -> String {
    client
        .create_personal_access_token(name, expiry)
        .await
        .expect("create a personal access token")
        .token
}

async fn token_client(harness: &TestHarness, token: &str) -> IggyClient {
    let client = harness.new_client().await.expect("client");
    client
        .login_with_personal_access_token(token)
        .await
        .expect("sign in with the token");
    client
}

/// A wrong password over HTTP answers 400 with the typed error body.
fn assert_sign_in_refused(result: Result<(), IggyError>) {
    match result {
        Err(IggyError::InvalidCredentials) => {}
        Err(IggyError::HttpResponseError(400, body)) if body.contains("invalid_credentials") => {}
        other => panic!("expected a refused sign-in, got {other:?}"),
    }
}

fn assert_refused<T: std::fmt::Debug>(result: Result<T, IggyError>) {
    match result {
        Err(
            IggyError::Unauthenticated
            | IggyError::InvalidCredentials
            | IggyError::InvalidPersonalAccessToken,
        ) => {}
        other => panic!("expected a refused credential, got {other:?}"),
    }
}
