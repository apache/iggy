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

//! A server with the `s3` tiered storage backend, against the Floci bucket
//! that the harness makes for it, and a server that reads an `s3` geo peer
//! in another Floci bucket. Docker must run.

use std::collections::HashMap;
use std::path::Path;
use std::time::{Duration, SystemTime};

use iggy_object_store::s3::S3Store;
use iggy_object_store::{IdentityRead, KeyLayout, StoreIdentity};
use integration::harness::floci::{self, ACCESS_KEY_ID, FlociBucket, REGION, SECRET_ACCESS_KEY};
use integration::harness::{TestHarness, TestServerConfig};
use integration::iggy_harness;
use tokio::time::{Instant, sleep};
use wiremock::matchers::{body_string_contains, method, path};
use wiremock::{Mock, MockServer, ResponseTemplate};

use super::geo_peers::wait_for_gauges;
use super::http_client::HttpClient;

/// How long the store identity check of a new server takes at most to claim
/// its bucket.
const CLAIM_DEADLINE: Duration = Duration::from_secs(30);
const POLL_INTERVAL: Duration = Duration::from_millis(200);
const ROLE: &str = "iggy-node";
const ROLE_ARN: &str = "arn:aws:iam::000000000000:role/iggy";
const TOKEN: &str = "web-identity-token";
const PEER_CLUSTER_UID: u128 = 0x0b0b_0b0b_0b0b_0b0b_0b0b_0b0b_0b0b_0b0b;
const CREDENTIALS_PATH: &str = "/latest/meta-data/iam/security-credentials/";

#[iggy_harness(server(tiered_storage.backend = "s3"))]
async fn given_s3_tiered_storage_when_the_server_boots_should_claim_its_bucket(
    harness: &TestHarness,
) {
    wait_for_claim(harness).await;
}

/// A server without static keys takes the credentials of its instance's role
/// from the instance metadata service, a mock here, and claims its bucket
/// with them.
#[tokio::test]
async fn given_instance_metadata_credentials_when_the_server_boots_should_claim_its_bucket_with_them()
 {
    let metadata = instance_metadata().await;
    let mut harness =
        server_without_static_keys([("AWS_EC2_METADATA_SERVICE_ENDPOINT", metadata.uri())]);

    harness.start().await.expect("the server boots");

    wait_for_claim(&harness).await;
    let requests = metadata
        .received_requests()
        .await
        .expect("the mock records requests");
    assert!(
        requests
            .iter()
            .any(|request| request.url.path().ends_with(&format!("/{ROLE}"))),
        "the server never asked for the credentials of {ROLE}"
    );
}

/// A server with web identity exchanges its token at STS, a mock here, for
/// credentials, and claims its bucket with them.
#[tokio::test]
async fn given_web_identity_credentials_when_the_server_boots_should_claim_its_bucket_with_them() {
    let sts = web_identity_sts().await;
    let directory = tempfile::tempdir().expect("a directory for the token");
    let token_file = directory.path().join("token");
    std::fs::write(&token_file, format!("{TOKEN}\n")).expect("the token file");
    let mut harness = server_without_static_keys([
        (
            "AWS_WEB_IDENTITY_TOKEN_FILE",
            token_file.to_string_lossy().into_owned(),
        ),
        (
            "AWS_ROLE_ARN",
            "arn:aws:iam::000000000000:role/iggy".to_owned(),
        ),
        ("AWS_ENDPOINT_URL_STS", sts.uri()),
    ]);

    harness.start().await.expect("the server boots");

    wait_for_claim(&harness).await;
    let requests = sts
        .received_requests()
        .await
        .expect("the mock records requests");
    assert!(
        requests.iter().any(|request| {
            String::from_utf8_lossy(&request.body).contains(&format!("WebIdentityToken={TOKEN}"))
        }),
        "the server never exchanged its token"
    );
}

/// A server with web identity reads an `s3` peer that has no keys with the
/// credentials that STS, a mock here, hands out for its token. The server's
/// own store is a directory, so only the peer's store signs anything: Floci
/// checks every signature, and the keys it accepts reach this server from
/// STS alone. So a verified peer signed with what STS handed out for it.
#[tokio::test]
async fn given_an_s3_peer_without_keys_when_the_server_boots_should_verify_it_with_web_identity_credentials()
 {
    let peer = floci::new_bucket()
        .await
        .expect("a Floci bucket for the peer, which needs Docker");
    claim(peer.clone(), PEER_CLUSTER_UID).await;
    let sts = web_identity_sts().await;
    let directory = tempfile::tempdir().expect("a directory for the token and the store");
    let own_store = directory.path().join("store");
    std::fs::create_dir(&own_store).expect("a directory for the server's own store");
    let extra_envs = HashMap::from(
        [
            ("IGGY_TIERED_STORAGE_BACKEND", "fs".to_owned()),
            (
                "IGGY_TIERED_STORAGE_PATH",
                own_store.to_string_lossy().into_owned(),
            ),
            ("AWS_WEB_IDENTITY_TOKEN_FILE", token_file(directory.path())),
            ("AWS_ROLE_ARN", ROLE_ARN.to_owned()),
            ("AWS_ENDPOINT_URL_STS", sts.uri()),
            ("IGGY_GEO_ORIGIN", "dc-a".to_owned()),
            ("IGGY_GEO_PEERS_0_ORIGIN", "dc-b".to_owned()),
            ("IGGY_GEO_PEERS_0_BACKEND", "s3".to_owned()),
            ("IGGY_GEO_PEERS_0_BUCKET", peer.bucket.clone()),
            ("IGGY_GEO_PEERS_0_REGION", REGION.to_owned()),
            ("IGGY_GEO_PEERS_0_ENDPOINT", peer.endpoint.clone()),
        ]
        .map(|(name, value)| (name.to_owned(), value)),
    );
    let mut harness = TestHarness::builder()
        .server(TestServerConfig::builder().extra_envs(extra_envs).build())
        .build()
        .expect("the harness");

    harness.start().await.expect("the server boots");

    let http = HttpClient::login_root(&harness).await;
    wait_for_gauges(&http, &[("dc-b", true, false)]).await;
    let exchanges = sts
        .received_requests()
        .await
        .expect("the mock records requests")
        .len();
    assert!(
        exchanges >= 1,
        "the peer's store exchanges the token, but STS got no request"
    );
}

/// A harness whose one server uses the `s3` backend without static keys, so
/// it takes credentials from the sources that `envs` set up.
fn server_without_static_keys<const N: usize>(envs: [(&str, String); N]) -> TestHarness {
    let mut extra_envs = HashMap::from([
        ("IGGY_TIERED_STORAGE_BACKEND".to_owned(), "s3".to_owned()),
        // Empty static keys keep the harness from setting its own.
        (
            "IGGY_TIERED_STORAGE_ACCESS_KEY_ID".to_owned(),
            String::new(),
        ),
        (
            "IGGY_TIERED_STORAGE_SECRET_ACCESS_KEY".to_owned(),
            String::new(),
        ),
    ]);
    extra_envs.extend(envs.map(|(name, value)| (name.to_owned(), value)));
    TestHarness::builder()
        .server(TestServerConfig::builder().extra_envs(extra_envs).build())
        .build()
        .expect("the harness")
}

/// Waits until the server of `harness` claims its bucket.
async fn wait_for_claim(harness: &TestHarness) {
    let bucket = harness
        .tiered_storage_bucket()
        .expect("the harness makes a bucket for the s3 backend")
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

/// A mock of the instance metadata service that hands out Floci's keys as
/// the credentials of [`ROLE`], valid for an hour.
async fn instance_metadata() -> MockServer {
    let server = MockServer::start().await;
    let expires_at = humantime::format_rfc3339_seconds(SystemTime::now() + Duration::from_hours(1));
    Mock::given(method("PUT"))
        .and(path("/latest/api/token"))
        .respond_with(ResponseTemplate::new(200).set_body_string("session-token"))
        .mount(&server)
        .await;
    Mock::given(method("GET"))
        .and(path(CREDENTIALS_PATH))
        .respond_with(ResponseTemplate::new(200).set_body_string(ROLE))
        .mount(&server)
        .await;
    Mock::given(method("GET"))
        .and(path(format!("{CREDENTIALS_PATH}{ROLE}")))
        .respond_with(ResponseTemplate::new(200).set_body_string(format!(
            r#"{{"Code":"Success","Type":"AWS-HMAC","AccessKeyId":"{ACCESS_KEY_ID}","SecretAccessKey":"{SECRET_ACCESS_KEY}","Token":"instance-session","Expiration":"{expires_at}"}}"#
        )))
        .mount(&server)
        .await;
    server
}

/// A mock of STS that hands out Floci's keys for the web identity token
/// [`TOKEN`], valid for an hour.
async fn web_identity_sts() -> MockServer {
    let server = MockServer::start().await;
    let expires_at = humantime::format_rfc3339_seconds(SystemTime::now() + Duration::from_hours(1));
    Mock::given(method("POST"))
        .and(path("/"))
        .and(body_string_contains("Action=AssumeRoleWithWebIdentity"))
        .respond_with(ResponseTemplate::new(200).set_body_string(format!(
            "<AssumeRoleWithWebIdentityResponse><AssumeRoleWithWebIdentityResult><Credentials>\
             <AccessKeyId>{ACCESS_KEY_ID}</AccessKeyId>\
             <SecretAccessKey>{SECRET_ACCESS_KEY}</SecretAccessKey>\
             <SessionToken>web-identity-session</SessionToken>\
             <Expiration>{expires_at}</Expiration>\
             </Credentials></AssumeRoleWithWebIdentityResult></AssumeRoleWithWebIdentityResponse>"
        )))
        .mount(&server)
        .await;
    server
}

/// Writes the web identity token [`TOKEN`] into `directory`, and returns
/// the path of the file.
fn token_file(directory: &Path) -> String {
    let token_file = directory.join("token");
    std::fs::write(&token_file, format!("{TOKEN}\n")).expect("the token file");
    token_file.to_string_lossy().into_owned()
}

/// Claims the root `iggy` of `bucket` for `cluster_uid`, as the replica 0 of
/// that cluster does. The S3 provider runs on compio, not on the harness's
/// runtime.
async fn claim(bucket: FlociBucket, cluster_uid: u128) {
    let claimed = tokio::task::spawn_blocking(move || {
        compio::runtime::Runtime::new()
            .expect("a compio runtime")
            .block_on(async {
                let store = S3Store::new(bucket.s3_config()).expect("the S3 store");
                let layout = KeyLayout::new("iggy").expect("the default prefix");
                StoreIdentity { cluster_uid }
                    .claim(&store, &layout)
                    .await
                    .expect("the claim")
            })
    })
    .await
    .expect("the claim task");
    assert!(claimed, "the root of the peer was free");
}

/// The S3 provider runs on compio, not on the harness's runtime.
async fn read_identity(bucket: FlociBucket) -> IdentityRead {
    tokio::task::spawn_blocking(move || {
        compio::runtime::Runtime::new()
            .expect("a compio runtime")
            .block_on(async {
                let store = S3Store::new(bucket.s3_config()).expect("the S3 store");
                let layout = KeyLayout::new("iggy").expect("the default prefix");
                StoreIdentity::read(&store, &layout)
                    .await
                    .expect("the read of the identity object")
            })
    })
    .await
    .expect("the identity task")
}
