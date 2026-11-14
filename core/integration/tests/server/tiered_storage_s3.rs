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
//! that the harness makes for it. Docker must run.

use std::collections::HashMap;
use std::time::{Duration, SystemTime};

use iggy_object_store::s3::S3Store;
use iggy_object_store::{IdentityRead, KeyLayout, StoreIdentity};
use integration::harness::floci::{ACCESS_KEY_ID, FlociBucket, SECRET_ACCESS_KEY};
use integration::harness::{TestHarness, TestServerConfig};
use integration::iggy_harness;
use tokio::time::{Instant, sleep};
use wiremock::matchers::{body_string_contains, method, path};
use wiremock::{Mock, MockServer, ResponseTemplate};

/// How long the store identity check of a new server takes at most to claim
/// its bucket.
const CLAIM_DEADLINE: Duration = Duration::from_secs(30);
const POLL_INTERVAL: Duration = Duration::from_millis(200);
const ROLE: &str = "iggy-node";
const TOKEN: &str = "web-identity-token";
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
