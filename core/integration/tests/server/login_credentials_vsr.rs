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

//! Credential rejections on the register handshake against the server (vsr).
//!
//! A username/password body that fails verification falls through to the PAT
//! decode attempt, so the terminal rejection has to carry the credential
//! failure that actually happened. Reporting the payload shape instead
//! (`MalformedLogin` -> `InvalidFormat`) tells a client its request was
//! malformed when the request was fine and the password was not.

use iggy::prelude::*;
use integration::iggy_harness;

#[iggy_harness(test_client_transport = [Tcp])]
async fn given_wrong_password_when_logging_in_should_reject_invalid_credentials(
    harness: &TestHarness,
) {
    let client = harness
        .new_client_for(TransportProtocol::Tcp)
        .await
        .expect("tcp client");

    let result = client
        .login_user("iggy", "definitely-not-the-password")
        .await;

    let expected = IggyError::InvalidCredentials.as_code();
    assert!(
        matches!(&result, Err(error) if error.as_code() == expected),
        "a wrong password must surface Err(InvalidCredentials), got {result:?}"
    );
}

#[iggy_harness(test_client_transport = [Tcp])]
async fn given_unknown_username_when_logging_in_should_reject_invalid_credentials(
    harness: &TestHarness,
) {
    let client = harness
        .new_client_for(TransportProtocol::Tcp)
        .await
        .expect("tcp client");

    let result = client.login_user("no-such-user", "irrelevant").await;

    // Same rejection as a wrong password: an unknown username must not be
    // distinguishable from a bad password.
    let expected = IggyError::InvalidCredentials.as_code();
    assert!(
        matches!(&result, Err(error) if error.as_code() == expected),
        "an unknown username must surface Err(InvalidCredentials), got {result:?}"
    );
}

#[iggy_harness(test_client_transport = [Tcp])]
async fn given_rejected_login_when_retrying_with_valid_credentials_should_succeed(
    harness: &TestHarness,
) {
    let client = harness
        .new_client_for(TransportProtocol::Tcp)
        .await
        .expect("tcp client");

    let rejected = client.login_user("iggy", "wrong").await;
    assert!(rejected.is_err(), "the first login must be rejected");

    // The rejection evicts the session, so a usable client has to reconnect;
    // the point is that a bad password does not poison the account.
    let retry = harness
        .new_client_for(TransportProtocol::Tcp)
        .await
        .expect("tcp client");
    retry
        .login_user("iggy", "iggy")
        .await
        .expect("valid credentials must still authenticate after a rejection");
}
