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

//! A server with the `gcs` tiered storage backend, against the
//! fake-gcs-server bucket that the harness makes for it. Docker must run.
//!
//! The emulator checks no token, so the tests of the token sources only
//! show that the server fetched a token from them and went on to use the
//! store.

use std::collections::HashMap;
use std::time::Duration;

use iggy_object_store::gcs::GcsStore;
use iggy_object_store::{IdentityRead, KeyLayout, StoreIdentity};
use integration::harness::fake_gcs::FakeGcsBucket;
use integration::harness::{TestHarness, TestServerConfig};
use integration::iggy_harness;
use tokio::time::{Instant, sleep};
use wiremock::matchers::{body_string_contains, header, method, path};
use wiremock::{Mock, MockServer, ResponseTemplate};

use crate::server::scenarios::geo_mode_faults_scenario::{
    Answer, Bucket, Ledger, Topics, assert_settled,
};

/// How long the store identity check of a new server takes at most to claim
/// its bucket.
const CLAIM_DEADLINE: Duration = Duration::from_secs(30);
const POLL_INTERVAL: Duration = Duration::from_millis(200);
const MESSAGES: u64 = 20;
const MESSAGE_BYTES: usize = 1024;
const TOKEN_PATH: &str = "/computeMetadata/v1/instance/service-accounts/default/token";
/// A 2048-bit RSA key that the TLS and JWT tests of the repository use.
const TEST_KEY: &str = include_str!("../../../certs/iggy_key.pem");

#[iggy_harness(server(tiered_storage.backend = "gcs"))]
async fn given_gcs_tiered_storage_when_the_server_boots_should_claim_its_bucket(
    harness: &TestHarness,
) {
    wait_for_claim(harness).await;
}

/// A geo topic stores its sends in fresh objects, its leader rewrites them
/// into long-term segments, and a poll reads each message once, from both.
#[tokio::test]
async fn given_a_geo_topic_on_gcs_when_producing_should_store_the_sends_and_read_each_once_from_long_term_segments()
 {
    let mut harness = server_with_gcs_store(HashMap::from([(
        // A geo topic takes messages once the metadata primary stored its
        // object, and a node without an origin stores none.
        "IGGY_GEO_ORIGIN".to_owned(),
        "dc-a".to_owned(),
    )]));
    harness.start().await.expect("the server boots");
    let bucket = Bucket::Gcs(
        harness
            .gcs_bucket()
            .expect("the harness makes a bucket for the gcs backend")
            .clone(),
    );
    let client = harness.tcp_root_client().await.expect("tcp root client");
    let topics = Topics::create(&client, "geo-gcs").await;

    let mut geo = Ledger::default();
    for _ in 0..MESSAGES {
        assert_eq!(
            geo.send(&client, &topics.stream, &topics.geo, MESSAGE_BYTES)
                .await,
            Answer::Acked
        );
    }

    assert_settled(&client, &topics, &geo, &bucket).await;
}

/// A server on a Compute Engine instance takes the token of its service
/// account from the metadata server, a mock here, and claims its bucket.
#[tokio::test]
async fn given_the_metadata_server_when_the_server_boots_should_fetch_a_token_and_claim_its_bucket()
{
    let metadata = MockServer::start().await;
    Mock::given(method("GET"))
        .and(path(TOKEN_PATH))
        .and(header("metadata-flavor", "Google"))
        .respond_with(ResponseTemplate::new(200).set_body_string(
            r#"{"access_token":"ya29.instance","expires_in":3599,"token_type":"Bearer"}"#,
        ))
        .mount(&metadata)
        .await;
    let host = metadata.address().to_string();
    let mut harness = server_with_gcs_store(HashMap::from([
        ("IGGY_TIERED_STORAGE_GCS_AUTH".to_owned(), "auto".to_owned()),
        ("GCE_METADATA_HOST".to_owned(), host),
        ("GOOGLE_APPLICATION_CREDENTIALS".to_owned(), String::new()),
    ]));

    harness.start().await.expect("the server boots");

    wait_for_claim(&harness).await;
    let requests = metadata
        .received_requests()
        .await
        .expect("the mock records requests");
    assert!(
        requests
            .iter()
            .any(|request| request.url.path() == TOKEN_PATH),
        "the server never asked the metadata server for a token"
    );
}

/// A server with a service account key file signs a JWT with the key and
/// exchanges it at the key's token endpoint, a mock here, for a token.
#[tokio::test]
async fn given_a_service_account_key_when_the_server_boots_should_exchange_a_jwt_and_claim_its_bucket()
 {
    let endpoint = MockServer::start().await;
    Mock::given(method("POST"))
        .and(path("/token"))
        .and(body_string_contains(
            "grant_type=urn%3Aietf%3Aparams%3Aoauth%3Agrant-type%3Ajwt-bearer",
        ))
        .respond_with(ResponseTemplate::new(200).set_body_string(
            r#"{"access_token":"ya29.account","expires_in":3600,"token_type":"Bearer"}"#,
        ))
        .mount(&endpoint)
        .await;
    let directory = tempfile::tempdir().expect("a directory for the key file");
    let key_file = directory.path().join("key.json");
    let key = serde_json::json!({
        "type": "service_account",
        "project_id": "iggy-test",
        "private_key_id": "key-1",
        "private_key": TEST_KEY,
        "client_email": "iggy@iggy-test.iam.gserviceaccount.com",
        "token_uri": format!("{}/token", endpoint.uri()),
    });
    std::fs::write(&key_file, key.to_string()).expect("the key file");
    let mut harness = server_with_gcs_store(HashMap::from([
        ("IGGY_TIERED_STORAGE_GCS_AUTH".to_owned(), "auto".to_owned()),
        (
            "IGGY_TIERED_STORAGE_GCS_SERVICE_ACCOUNT_KEY".to_owned(),
            key_file.to_string_lossy().into_owned(),
        ),
    ]));

    harness.start().await.expect("the server boots");

    wait_for_claim(&harness).await;
    let requests = endpoint
        .received_requests()
        .await
        .expect("the mock records requests");
    let assertion = requests
        .iter()
        .find_map(|request| {
            String::from_utf8_lossy(&request.body)
                .split_once("&assertion=")
                .map(|(_, assertion)| assertion.to_owned())
        })
        .expect("the server never exchanged a JWT");
    assert_eq!(assertion.split('.').count(), 3, "{assertion}");
}

/// A harness whose one server uses the `gcs` backend, with `envs` over the
/// harness's own settings for the bucket.
fn server_with_gcs_store(envs: HashMap<String, String>) -> TestHarness {
    let mut extra_envs = HashMap::from([
        ("IGGY_TIERED_STORAGE_BACKEND".to_owned(), "gcs".to_owned()),
        (
            "IGGY_DATA_MAINTENANCE_MESSAGES_CLEANER_ENABLED".to_owned(),
            "true".to_owned(),
        ),
        (
            "IGGY_DATA_MAINTENANCE_MESSAGES_INTERVAL".to_owned(),
            "100ms".to_owned(),
        ),
    ]);
    extra_envs.extend(envs);
    TestHarness::builder()
        .cluster_nodes(1)
        .server(TestServerConfig::builder().extra_envs(extra_envs).build())
        .build()
        .expect("the harness")
}

/// Waits until the server of `harness` claims its bucket.
async fn wait_for_claim(harness: &TestHarness) {
    let bucket = harness
        .gcs_bucket()
        .expect("the harness makes a bucket for the gcs backend")
        .clone();
    let deadline = Instant::now() + CLAIM_DEADLINE;
    loop {
        let read = read_identity(bucket.clone()).await;
        if matches!(read, IdentityRead::Found(_)) {
            return;
        }
        assert!(
            Instant::now() < deadline,
            "the server did not claim {bucket:?}: {read:?}"
        );
        sleep(POLL_INTERVAL).await;
    }
}

/// The GCS provider runs on compio, not on the harness's runtime.
async fn read_identity(bucket: FakeGcsBucket) -> IdentityRead {
    tokio::task::spawn_blocking(move || {
        compio::runtime::Runtime::new()
            .expect("a compio runtime")
            .block_on(async {
                let store = GcsStore::new(bucket.gcs_config()).expect("the GCS store");
                let layout = KeyLayout::new("iggy").expect("the default prefix");
                StoreIdentity::read(&store, &layout)
                    .await
                    .expect("the read of the identity object")
            })
    })
    .await
    .expect("the identity task")
}
