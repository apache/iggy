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

use super::{POLL_ATTEMPTS, POLL_INTERVAL_MS};
use crate::connectors::fixtures::{
    GITHUB_ENDPOINT_ID, GITHUB_HMAC_HEADER, GITHUB_INSTANCE, HttpSourceFixture, MANAGEMENT_TOKEN,
    PARTNER_BEARER_TOKEN, PARTNER_ENDPOINT_ID, PARTNER_INSTANCE,
};
use iggy::prelude::IggyClient;
use iggy_common::MessageClient;
use iggy_common::{Consumer, Identifier, PollingStrategy};
use iggy_connector_sdk::api::{ConnectorStatus, SourceInfoResponse};
use integration::harness::seeds;
use integration::iggy_harness;
use reqwest::{Client, StatusCode};
use serde_json::{Value, json};
use std::time::Duration;
use tokio::time::sleep;

const API_KEY: &str = "test-api-key";
const RESTORED_TOKEN: &str = "token-that-must-survive-a-restart";

#[iggy_harness(
    server(connectors_runtime(config_path = "tests/connectors/http/source.toml")),
    seed = seeds::connector_multi_topic_stream
)]
async fn webhook_post_produces_message_to_iggy(harness: &TestHarness, fixture: HttpSourceFixture) {
    let client = harness.root_client().await.unwrap();
    let http = webhook_client();
    wait_for_gateway(&http, &fixture).await;

    let body = r#"{"event":"push","repository":"apache/iggy"}"#;
    let response = http
        .post(fixture.webhook_url(GITHUB_ENDPOINT_ID))
        .header(
            GITHUB_HMAC_HEADER,
            fixture.github_signature(body.as_bytes()),
        )
        .header("X-GitHub-Delivery", "72d3162e-cc78-11e3-81ab-4c9367dc0958")
        .body(body)
        .send()
        .await
        .expect("Failed to POST the webhook");

    assert_eq!(response.status(), StatusCode::OK);

    let messages = poll_payloads(&client, seeds::names::TOPIC, "http_source_cg_1", 1).await;
    assert_eq!(
        String::from_utf8_lossy(&messages[0].0),
        body,
        "the raw request body must reach Iggy byte for byte"
    );
    let headers = messages[0]
        .1
        .as_ref()
        .expect("HTTP metadata forwarding is enabled");
    assert_eq!(
        header_value(headers, "iggy_source_instance").as_deref(),
        Some(GITHUB_INSTANCE)
    );
    assert_eq!(
        header_value(headers, "X-GitHub-Delivery").as_deref(),
        Some("72d3162e-cc78-11e3-81ab-4c9367dc0958"),
        "the delivery id is the consumer's dedup key and must survive the FFI hop"
    );
}

/// The secret path is covered above; nothing exercised the named one, which is
/// the route an operator gets by setting `topic_path` and the one a typo in
/// `auth_bearer_token` would leave open.
#[iggy_harness(
    server(connectors_runtime(config_path = "tests/connectors/http/source.toml")),
    seed = seeds::connector_multi_topic_stream
)]
async fn named_path_post_produces_message_to_iggy(
    harness: &TestHarness,
    fixture: HttpSourceFixture,
) {
    let client = harness.root_client().await.unwrap();
    let http = webhook_client();
    wait_for_gateway(&http, &fixture).await;

    let body = r#"{"event":"deployment","environment":"staging"}"#;
    let response = http
        .post(fixture.named_url(seeds::names::TOPIC))
        .body(body)
        .send()
        .await
        .expect("Failed to POST to the named path");

    assert_eq!(response.status(), StatusCode::OK);

    let messages = poll_payloads(&client, seeds::names::TOPIC, "http_source_cg_named", 1).await;
    assert_eq!(
        String::from_utf8_lossy(&messages[0].0),
        body,
        "the named path must deliver the raw body byte for byte, same as the secret path"
    );
    let headers = messages[0]
        .1
        .as_ref()
        .expect("HTTP metadata forwarding is enabled");
    assert_eq!(
        header_value(headers, "iggy_source_instance").as_deref(),
        Some(GITHUB_INSTANCE),
        "and must attribute the message to the instance that owns the topic_path"
    );
}

#[iggy_harness(
    server(connectors_runtime(config_path = "tests/connectors/http/source.toml")),
    seed = seeds::connector_multi_topic_stream
)]
async fn two_instances_share_one_listener(harness: &TestHarness, fixture: HttpSourceFixture) {
    let client = harness.root_client().await.unwrap();
    let http = webhook_client();
    wait_for_gateway(&http, &fixture).await;

    let github_body = r#"{"from":"github"}"#;
    let github = http
        .post(fixture.webhook_url(GITHUB_ENDPOINT_ID))
        .header(
            GITHUB_HMAC_HEADER,
            fixture.github_signature(github_body.as_bytes()),
        )
        .body(github_body)
        .send()
        .await
        .expect("Failed to POST to the GitHub endpoint");
    let partner_body = r#"{"from":"partner"}"#;
    let partner = http
        .post(fixture.webhook_url(PARTNER_ENDPOINT_ID))
        .header("Authorization", format!("Bearer {PARTNER_BEARER_TOKEN}"))
        .body(partner_body)
        .send()
        .await
        .expect("Failed to POST to the partner endpoint");

    assert_eq!(github.status(), StatusCode::OK);
    assert_eq!(
        partner.status(),
        StatusCode::OK,
        "the second instance must be reachable on the port the first one bound"
    );

    let first = poll_payloads(&client, seeds::names::TOPIC, "http_source_cg_2a", 1).await;
    let second = poll_payloads(&client, seeds::names::TOPIC_2, "http_source_cg_2b", 1).await;
    assert_eq!(String::from_utf8_lossy(&first[0].0), github_body);
    assert_eq!(
        String::from_utf8_lossy(&second[0].0),
        partner_body,
        "one listener, but each instance produces to its own topic"
    );
    assert_eq!(
        header_value(
            second[0].1.as_ref().expect("metadata is on by default"),
            "iggy_source_instance"
        )
        .as_deref(),
        Some(PARTNER_INSTANCE)
    );
}

#[iggy_harness(
    server(connectors_runtime(config_path = "tests/connectors/http/source.toml")),
    seed = seeds::connector_multi_topic_stream
)]
async fn management_registered_endpoint_accepts_until_revoked(
    harness: &TestHarness,
    fixture: HttpSourceFixture,
) {
    let client = harness.root_client().await.unwrap();
    let http = webhook_client();
    wait_for_gateway(&http, &fixture).await;

    let endpoint_id = register_endpoint(&http, &fixture, GITHUB_INSTANCE).await;
    let body = r#"{"event":"dynamic"}"#;
    let accepted = http
        .post(fixture.webhook_url(&endpoint_id))
        .body(body)
        .send()
        .await
        .expect("Failed to POST to the registered endpoint");
    assert_eq!(
        accepted.status(),
        StatusCode::OK,
        "an endpoint registered through the API must serve without a restart"
    );

    let messages = poll_payloads(&client, seeds::names::TOPIC, "http_source_cg_3", 1).await;
    assert_eq!(String::from_utf8_lossy(&messages[0].0), body);

    let revoked = http
        .delete(format!(
            "{}/admin/endpoints/{endpoint_id}",
            fixture.admin_url()
        ))
        .header("Authorization", format!("Bearer {MANAGEMENT_TOKEN}"))
        .json(&json!({"reason": "compromised"}))
        .send()
        .await
        .expect("Failed to revoke the endpoint");
    assert_eq!(revoked.status(), StatusCode::ACCEPTED);

    let after = http
        .post(fixture.webhook_url(&endpoint_id))
        .body(body)
        .send()
        .await
        .expect("Failed to POST to the revoked endpoint");
    assert_eq!(
        after.status(),
        StatusCode::NOT_FOUND,
        "a revocation must take effect immediately, and must not leak that the URL once worked"
    );
}

#[iggy_harness(
    server(connectors_runtime(config_path = "tests/connectors/http/source.toml")),
    seed = seeds::connector_multi_topic_stream
)]
async fn dynamic_endpoint_survives_connector_restart(
    harness: &TestHarness,
    fixture: HttpSourceFixture,
) {
    let http = webhook_client();
    let api_url = harness
        .connectors_runtime()
        .expect("connector runtime should be available")
        .http_url();
    wait_for_gateway(&http, &fixture).await;

    // Registered with a secret so the restart proves the credential survives
    // the state round trip, not merely the endpoint id.
    let endpoint_id = register_secured_endpoint(&http, &fixture, GITHUB_INSTANCE).await;
    // The registry only reaches the runtime once a poll carrying it has been
    // sent, so wait for the connector to report it submitted before pulling
    // the rug out from under it.
    wait_for_submitted(&http, &fixture, &endpoint_id).await;

    let restarted = http
        .post(format!("{api_url}/sources/{GITHUB_INSTANCE}/restart"))
        .header("api-key", API_KEY)
        .send()
        .await
        .expect("Failed to restart the source connector");
    assert!(
        restarted.status().is_success(),
        "Restart request failed: {}",
        restarted.status()
    );
    wait_for_source_status(&http, &api_url, ConnectorStatus::Running).await;
    wait_for_gateway(&http, &fixture).await;

    let authorized = http
        .post(fixture.webhook_url(&endpoint_id))
        .header("Authorization", format!("Bearer {RESTORED_TOKEN}"))
        .body(r#"{"event":"after-restart"}"#)
        .send()
        .await
        .expect("Failed to POST after the restart");
    let unauthorized = http
        .post(fixture.webhook_url(&endpoint_id))
        .body(r#"{"event":"after-restart"}"#)
        .send()
        .await
        .expect("Failed to POST after the restart");

    assert_eq!(
        authorized.status(),
        StatusCode::OK,
        "an endpoint that only ever existed in ConnectorState must come back with the connector"
    );
    assert_eq!(
        unauthorized.status(),
        StatusCode::UNAUTHORIZED,
        "and its secret must come back with it, not be dropped to an open endpoint"
    );
}

/// The README's strongest security claim: a revoked endpoint must not be
/// resurrected by a restart that re-reads the TOML still declaring it.
#[iggy_harness(
    server(connectors_runtime(config_path = "tests/connectors/http/source.toml")),
    seed = seeds::connector_multi_topic_stream
)]
async fn revoked_static_endpoint_stays_revoked_across_restart(
    harness: &TestHarness,
    fixture: HttpSourceFixture,
) {
    let http = webhook_client();
    let api_url = harness
        .connectors_runtime()
        .expect("connector runtime should be available")
        .http_url();
    wait_for_gateway(&http, &fixture).await;

    let body = r#"{"event":"push"}"#;
    let before = http
        .post(fixture.webhook_url(GITHUB_ENDPOINT_ID))
        .header(
            GITHUB_HMAC_HEADER,
            fixture.github_signature(body.as_bytes()),
        )
        .body(body)
        .send()
        .await
        .expect("Failed to POST before revocation");
    assert_eq!(before.status(), StatusCode::OK);

    let revoked = http
        .delete(format!(
            "{}/admin/endpoints/{GITHUB_ENDPOINT_ID}",
            fixture.admin_url()
        ))
        .header("Authorization", format!("Bearer {MANAGEMENT_TOKEN}"))
        .json(&json!({"reason": "compromised"}))
        .send()
        .await
        .expect("Failed to revoke the static endpoint");
    assert_eq!(revoked.status(), StatusCode::ACCEPTED);
    wait_for_submitted(&http, &fixture, GITHUB_ENDPOINT_ID).await;

    let restarted = http
        .post(format!("{api_url}/sources/{GITHUB_INSTANCE}/restart"))
        .header("api-key", API_KEY)
        .send()
        .await
        .expect("Failed to restart the source connector");
    assert!(restarted.status().is_success());
    wait_for_source_status(&http, &api_url, ConnectorStatus::Running).await;
    wait_for_gateway(&http, &fixture).await;

    let after = http
        .post(fixture.webhook_url(GITHUB_ENDPOINT_ID))
        .header(
            GITHUB_HMAC_HEADER,
            fixture.github_signature(body.as_bytes()),
        )
        .body(body)
        .send()
        .await
        .expect("Failed to POST after the restart");
    assert_eq!(
        after.status(),
        StatusCode::NOT_FOUND,
        "the tombstone must outlive the restart even though http_github.toml still declares this endpoint"
    );
}

/// Keep-alive is off on purpose. The gateway shuts down gracefully, so an idle
/// pooled socket holds the listener open until its shutdown timeout expires,
/// stalling harness teardown for seconds per test.
fn webhook_client() -> Client {
    Client::builder()
        .pool_max_idle_per_host(0)
        .build()
        .expect("Failed to build the webhook client")
}

/// The listener binds during the connector's `open()`, which happens after the
/// runtime's own HTTP API is already answering.
///
/// Waits for *both* instances by name. The public `/health` answers as soon as
/// one instance is serving, and the two open independently, so returning on the
/// first left every later request racing the second: a post to
/// `PARTNER_ENDPOINT_ID` would 404, and a registration against `GITHUB_INSTANCE`
/// would come back "unknown instance", depending on which won.
async fn wait_for_gateway(http: &Client, fixture: &HttpSourceFixture) {
    for _ in 0..POLL_ATTEMPTS {
        if let Ok(response) = http
            .get(format!("{}/admin/health", fixture.admin_url()))
            .send()
            .await
            && response.status() == StatusCode::OK
            && let Ok(health) = response.json::<Value>().await
        {
            let joined: Vec<&str> = health["instances"]
                .as_array()
                .map(|instances| {
                    instances
                        .iter()
                        .filter_map(|instance| instance["instance"].as_str())
                        .collect()
                })
                .unwrap_or_default();
            if joined.contains(&GITHUB_INSTANCE) && joined.contains(&PARTNER_INSTANCE) {
                return;
            }
        }
        sleep(Duration::from_millis(POLL_INTERVAL_MS)).await;
    }
    panic!("Both webhook gateway instances did not join in time");
}

/// Registers a bearer-guarded endpoint, so a test can prove the secret and not
/// just the id survives whatever it does next.
async fn register_secured_endpoint(
    http: &Client,
    fixture: &HttpSourceFixture,
    instance: &str,
) -> String {
    let response = http
        .post(format!("{}/admin/endpoints", fixture.admin_url()))
        .header("Authorization", format!("Bearer {MANAGEMENT_TOKEN}"))
        .json(&json!({
            "instance": instance,
            "auth_type": "bearer",
            "auth_secret": RESTORED_TOKEN,
        }))
        .send()
        .await
        .expect("Failed to register a secured endpoint");
    assert_eq!(response.status(), StatusCode::CREATED);
    let body: Value = response
        .json()
        .await
        .expect("Registration must return JSON");
    body["endpoint_id"]
        .as_str()
        .expect("Registration must return an endpoint id")
        .to_string()
}

async fn register_endpoint(http: &Client, fixture: &HttpSourceFixture, instance: &str) -> String {
    let response = http
        .post(format!("{}/admin/endpoints", fixture.admin_url()))
        .header("Authorization", format!("Bearer {MANAGEMENT_TOKEN}"))
        .json(&json!({"instance": instance}))
        .send()
        .await
        .expect("Failed to register an endpoint");
    assert_eq!(response.status(), StatusCode::CREATED);
    let body: Value = response
        .json()
        .await
        .expect("Registration must return JSON");
    body["endpoint_id"]
        .as_str()
        .expect("Registration must return an endpoint id")
        .to_string()
}

async fn wait_for_submitted(http: &Client, fixture: &HttpSourceFixture, endpoint_id: &str) {
    for _ in 0..POLL_ATTEMPTS {
        if let Ok(response) = http
            .get(format!(
                "{}/admin/endpoints/{endpoint_id}",
                fixture.admin_url()
            ))
            .header("Authorization", format!("Bearer {MANAGEMENT_TOKEN}"))
            .send()
            .await
            && let Ok(body) = response.json::<Value>().await
            && body["submitted"] == json!(true)
        {
            return;
        }
        sleep(Duration::from_millis(POLL_INTERVAL_MS)).await;
    }
    panic!("The registered endpoint was never handed to the runtime for persistence");
}

/// Compares the typed status rather than a string: `ConnectorStatus`
/// serializes lowercase, so a `{:?}` comparison would silently never match.
async fn wait_for_source_status(http: &Client, api_url: &str, expected: ConnectorStatus) {
    for _ in 0..POLL_ATTEMPTS {
        if let Ok(response) = http
            .get(format!("{api_url}/sources/{GITHUB_INSTANCE}"))
            .header("api-key", API_KEY)
            .send()
            .await
            && let Ok(info) = response.json::<SourceInfoResponse>().await
            && info.status == expected
        {
            return;
        }
        sleep(Duration::from_millis(POLL_INTERVAL_MS)).await;
    }
    panic!("The source connector did not reach {expected:?} status in time");
}

type PolledMessage = (Vec<u8>, Option<Vec<(String, String)>>);

async fn poll_payloads(
    client: &IggyClient,
    topic: &str,
    consumer: &str,
    expected: usize,
) -> Vec<PolledMessage> {
    let stream_id: Identifier = seeds::names::STREAM.try_into().unwrap();
    let topic_id: Identifier = topic.try_into().unwrap();
    let consumer_id: Identifier = consumer.try_into().unwrap();

    let mut collected: Vec<PolledMessage> = Vec::new();
    for _ in 0..POLL_ATTEMPTS {
        if let Ok(polled) = client
            .poll_messages(
                &stream_id,
                &topic_id,
                None,
                &Consumer::new(consumer_id.clone()),
                &PollingStrategy::next(),
                10,
                true,
            )
            .await
        {
            for message in polled.messages {
                // `Display` on a header field renders "<kind>: <value>", so
                // the bare name has to come from `as_str`.
                let headers = message.user_headers_map().ok().flatten().map(|headers| {
                    headers
                        .into_iter()
                        .map(|(key, value)| {
                            (
                                key.as_str().unwrap_or_default().to_string(),
                                value.as_str().unwrap_or_default().to_string(),
                            )
                        })
                        .collect()
                });
                collected.push((message.payload.to_vec(), headers));
            }
            if collected.len() >= expected {
                return collected;
            }
        }
        sleep(Duration::from_millis(POLL_INTERVAL_MS)).await;
    }
    panic!(
        "Expected {expected} messages on {topic}, got {}",
        collected.len()
    );
}

fn header_value(headers: &[(String, String)], key: &str) -> Option<String> {
    headers
        .iter()
        .find(|(name, _)| name == key)
        .map(|(_, value)| value.clone())
}
