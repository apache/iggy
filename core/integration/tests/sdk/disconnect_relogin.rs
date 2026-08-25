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

//! An explicit disconnect ends the session for good: the credentials a manual
//! sign-in remembered (for reconnecting across involuntary drops and
//! failovers) must not resurrect it. Pins at the Rust layer the contract the
//! C++ e2e suite asserts through the FFI (`DisconnectThenReconnectWithoutRelogin`,
//! `GetStatsBeforeLoginThrows`), so a regression fails here first instead of
//! three suites downstream.

use iggy::prelude::*;
use integration::iggy_harness;

#[iggy_harness]
async fn given_a_logged_in_client_when_explicitly_disconnected_should_require_a_fresh_login(
    harness: &TestHarness,
) {
    let client = harness.new_client().await.unwrap();
    client
        .login_user(DEFAULT_ROOT_USERNAME, DEFAULT_ROOT_PASSWORD)
        .await
        .unwrap();
    client.get_me().await.expect("authenticated get_me works");

    client.disconnect().await.unwrap();
    client.connect().await.unwrap();
    assert!(
        matches!(client.get_me().await, Err(IggyError::Unauthenticated)),
        "an explicit disconnect is caller intent, like a logout: the sign-in it ended \
         must not be silently replayed by the reconnect, so the server sees an \
         unauthenticated request"
    );

    client.disconnect().await.unwrap();
    assert!(
        matches!(client.get_stats().await, Err(IggyError::NotConnected)),
        "an operation after an explicit disconnect must fail on the dead transport \
         instead of reconnecting into a resurrected session"
    );

    // The remembered sign-in exists for involuntary drops; a fresh manual
    // login after the disconnect works exactly as before.
    client.connect().await.unwrap();
    client
        .login_user(DEFAULT_ROOT_USERNAME, DEFAULT_ROOT_PASSWORD)
        .await
        .unwrap();
    client
        .get_me()
        .await
        .expect("a fresh login restores service");
}
