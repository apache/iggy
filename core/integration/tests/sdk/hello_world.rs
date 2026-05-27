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

use iggy::prelude::*;
use integration::iggy_harness;

#[cfg(not(feature = "vsr"))]
#[iggy_harness]
async fn hello_world(harness: &TestHarness) {
    let client = harness.root_client().await.unwrap();
    client.ping().await.unwrap();
}

#[cfg(feature = "vsr")]
#[iggy_harness(test_client_transport = [Tcp, WebSocket])]
async fn hello_world(harness: &TestHarness) {
    let client = harness.new_client().await.unwrap();
    client
        .login_user(DEFAULT_ROOT_USERNAME, DEFAULT_ROOT_PASSWORD)
        .await
        .unwrap();
}

#[cfg(feature = "vsr")]
#[iggy_harness(test_client_transport = [Tcp, WebSocket, Quic])]
async fn hello_world_ping(harness: &TestHarness) {
    let client = harness.new_client().await.unwrap();
    client
        .login_user(DEFAULT_ROOT_USERNAME, DEFAULT_ROOT_PASSWORD)
        .await
        .unwrap();
    client.ping().await.unwrap();
    client.logout_user().await.unwrap();
}

/// VSR replicated-mutation smoke. Exercises the consensus path under the
/// new header framing (CreateStream + CreateTopic + SendMessages all
/// replicate via `Operation::*` -> `prepare_request` -> `on_ack`). Without
/// this, the hello_world / ping cases only cover `Operation::Register` and
/// `Operation::NonReplicated`, leaving the actual subject of the PR
/// uncovered.
#[cfg(feature = "vsr")]
#[iggy_harness(test_client_transport = [Tcp, WebSocket, Quic])]
async fn replicated_create_stream_round_trip(harness: &TestHarness) {
    use iggy::prelude::*;
    let client = harness.new_client().await.unwrap();
    client
        .login_user(DEFAULT_ROOT_USERNAME, DEFAULT_ROOT_PASSWORD)
        .await
        .unwrap();
    let stream = client
        .create_stream("vsr-smoke")
        .await
        .expect("create_stream must commit through VSR");
    let topic = client
        .create_topic(
            &stream.id.try_into().unwrap(),
            "vsr-topic",
            1,
            CompressionAlgorithm::None,
            None,
            IggyExpiry::NeverExpire,
            MaxTopicSize::ServerDefault,
        )
        .await
        .expect("create_topic must commit through VSR (CreateTopicWithAssignments transform)");
    client
        .create_consumer_group(
            &stream.id.try_into().unwrap(),
            &topic.id.try_into().unwrap(),
            "vsr-group",
        )
        .await
        .expect("create_consumer_group must commit through VSR");
    client.logout_user().await.unwrap();
}

/// VSR raw-PAT return path. The token is minted non-deterministically on the
/// home shard (never replicated), so the committed reply body is empty and the
/// home shard must inject the raw token before answering the client. A blank
/// token here means that injection regressed.
#[cfg(feature = "vsr")]
#[iggy_harness(test_client_transport = [Tcp])]
async fn replicated_create_pat_returns_raw_token(harness: &TestHarness) {
    use iggy::prelude::*;
    let client = harness.new_client().await.unwrap();
    client
        .login_user(DEFAULT_ROOT_USERNAME, DEFAULT_ROOT_PASSWORD)
        .await
        .unwrap();
    let raw = client
        .create_personal_access_token("vsr-pat", PersonalAccessTokenExpiry::NeverExpire)
        .await
        .expect("create_personal_access_token must commit through VSR");
    assert!(
        !raw.token.is_empty(),
        "home shard must return the minted raw token, not the empty committed body"
    );
    client.logout_user().await.unwrap();
}
