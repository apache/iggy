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

//! The expired-token cleaner deletes through consensus under the reserved
//! client id 0, which no session ever registers. Every replica must commit
//! that delete like any other op and keep committing afterwards.

use std::time::Duration;

use iggy::prelude::*;
use integration::iggy_harness;
use tokio::time::{sleep, timeout};

const TOKEN_NAME: &str = "short-lived";
const STREAM_NAME: &str = "after-token-cleanup";
const CLEANUP_BUDGET: Duration = Duration::from_secs(30);
const WRITE_BUDGET: Duration = Duration::from_secs(10);
const RETRY_PAUSE: Duration = Duration::from_millis(100);

// A solo node is the reported topology. A cluster also runs the delete through
// the backups' commit walk. A replica that panics on it fails the test when
// the harness drops its handle.
#[iggy_harness(
    test_client_transport = [Tcp],
    cluster_nodes = [1, 3],
    server(personal_access_token.cleaner.interval = "1 s")
)]
async fn given_expired_token_when_cleaner_deletes_it_should_keep_committing_writes(
    harness: &TestHarness,
) {
    let client = harness.tcp_root_client().await.expect("tcp root client");
    client
        .create_personal_access_token(
            TOKEN_NAME,
            PersonalAccessTokenExpiry::ExpireDuration(IggyDuration::ONE_SECOND),
        )
        .await
        .expect("create token");

    // A replica that dies committing the delete leaves the client retrying
    // without an answer, so the wait is bounded, not open-ended.
    timeout(CLEANUP_BUDGET, wait_for_no_tokens(&client))
        .await
        .unwrap_or_else(|_| {
            panic!(
                "the expired token was still listed, or no node answered, after {CLEANUP_BUDGET:?}"
            )
        });

    // The delete applies before its commit completes, so an empty list alone
    // does not prove the commit went through. A later write must commit
    // behind it.
    timeout(WRITE_BUDGET, client.create_stream(STREAM_NAME))
        .await
        .unwrap_or_else(|_| {
            panic!("a write after the cleaner's delete did not commit within {WRITE_BUDGET:?}")
        })
        .expect("a write after the cleaner's delete must commit");
}

async fn wait_for_no_tokens(client: &IggyClient) {
    while !client
        .get_personal_access_tokens()
        .await
        .expect("the node must keep serving while the cleaner runs")
        .is_empty()
    {
        sleep(RETRY_PAUSE).await;
    }
}
