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

use std::sync::Arc;

use iggy::http::http_client::HttpClient;
use iggy::prelude::{
    GlobalPermissions, HttpClientConfig, Identifier, IggyClientBuilder, IggyError, Permissions,
    UserStatus, UserUpdateOptions,
};
use iggy_common::{StreamClient, UserClient};
use integration::harness::config::JWKS_ISSUER_USERNAME;
use integration::iggy_harness;
use jsonwebtoken::{Algorithm, EncodingKey, Header, encode};
use serde::{Deserialize, Serialize};

const TEST_ISSUER: &str = "https://test-issuer.com";
const TEST_AUDIENCE: &str = "iggy";
const TEST_KEY_ID: &str = "iggy-jwt-key-1";
const TEST_PRIVATE_KEY: &[u8] = include_bytes!("../../../../certs/iggy_key.pem");
const A2A_PASSWORD: &str = "a2a-test-password";

/// Seed function to create the A2A user with proper permissions
async fn seed_a2a_user(
    client: &iggy::prelude::IggyClient,
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    // Grant read_streams permission so the user can call get_streams().
    match client
        .create_user(
            JWKS_ISSUER_USERNAME,
            A2A_PASSWORD,
            UserStatus::Active,
            Some(streams_permissions(false)),
        )
        .await
    {
        Ok(user) => {
            println!("A2A user created successfully with ID: {}", user.id);
        }
        Err(e) => {
            println!(
                "Note: Could not create A2A user (may already exist): {:?}",
                e
            );
        }
    }
    Ok(())
}

/// The `aud` claim as RFC 7519 allows it on the wire: one string, or an array
/// of them. Untagged serialization emits each shape verbatim, which is what the
/// server's own audience parser reads back.
#[derive(Debug, Serialize, Deserialize)]
#[serde(untagged)]
enum Audience {
    Single(String),
    Multiple(Vec<String>),
}

/// Test claims structure for JWT tokens
/// Supports both single string and array audience per RFC 7519
#[derive(Debug, Serialize, Deserialize)]
struct TestClaims {
    jti: String,
    iss: String,
    aud: Audience,
    sub: String,
    exp: u64,
    iat: u64,
    nbf: u64,
}

/// Get current timestamp in seconds since Unix epoch
fn now_timestamp() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_secs()
}

/// Creates a valid JWT token with specified expiration time
fn create_valid_jwt(exp_seconds: u64) -> String {
    let now = now_timestamp();
    let claims = TestClaims {
        jti: uuid::Uuid::now_v7().to_string(),
        iss: TEST_ISSUER.to_string(),
        aud: Audience::Single(TEST_AUDIENCE.to_string()),
        sub: "external-a2a-user-123".to_string(),
        exp: now + exp_seconds,
        iat: now,
        nbf: now,
    };

    let mut header = Header::new(Algorithm::RS256);
    header.kid = Some(TEST_KEY_ID.to_string());
    let encoding_key = EncodingKey::from_rsa_pem(TEST_PRIVATE_KEY).unwrap();

    encode(&header, &claims, &encoding_key).unwrap()
}

/// Creates a valid JWT token with audience as array
fn create_valid_jwt_with_array_aud(exp_seconds: u64) -> String {
    let now = now_timestamp();
    let claims = TestClaims {
        jti: uuid::Uuid::now_v7().to_string(),
        iss: TEST_ISSUER.to_string(),
        aud: Audience::Multiple(vec![
            "some-other-service".to_string(),
            TEST_AUDIENCE.to_string(),
            "another-service".to_string(),
        ]),
        sub: "external-a2a-user-123".to_string(),
        exp: now + exp_seconds,
        iat: now,
        nbf: now,
    };

    let mut header = Header::new(Algorithm::RS256);
    header.kid = Some(TEST_KEY_ID.to_string());
    let encoding_key = EncodingKey::from_rsa_pem(TEST_PRIVATE_KEY).unwrap();

    encode(&header, &claims, &encoding_key).unwrap()
}

/// Creates an expired JWT token (expired 1 hour ago)
fn create_expired_jwt() -> String {
    let now = now_timestamp();
    let claims = TestClaims {
        jti: uuid::Uuid::now_v7().to_string(),
        iss: TEST_ISSUER.to_string(),
        aud: Audience::Single(TEST_AUDIENCE.to_string()),
        sub: "external-a2a-user-123".to_string(),
        exp: now.saturating_sub(3600),
        iat: now.saturating_sub(7200),
        nbf: now.saturating_sub(7200),
    };

    let mut header = Header::new(Algorithm::RS256);
    header.kid = Some(TEST_KEY_ID.to_string());
    let encoding_key = EncodingKey::from_rsa_pem(TEST_PRIVATE_KEY).unwrap();

    encode(&header, &claims, &encoding_key).unwrap()
}

/// Creates a JWT token with unknown issuer
fn create_unknown_issuer_jwt() -> String {
    let now = now_timestamp();
    let claims = TestClaims {
        jti: uuid::Uuid::now_v7().to_string(),
        iss: "https://unknown-issuer.com".to_string(),
        aud: Audience::Single(TEST_AUDIENCE.to_string()),
        sub: "external-a2a-user-123".to_string(),
        exp: now + 3600,
        iat: now,
        nbf: now,
    };

    let mut header = Header::new(Algorithm::RS256);
    header.kid = Some(TEST_KEY_ID.to_string());
    let encoding_key = EncodingKey::from_rsa_pem(TEST_PRIVATE_KEY).unwrap();

    encode(&header, &claims, &encoding_key).unwrap()
}

/// Craft an algorithm-confusion token: it claims the trusted issuer and carries
/// the real JWKS `kid`, but is HS256 (symmetric) and signed with an
/// attacker-controlled secret the server never shares. The issuer publishes only
/// an RSA key, so verifying an HMAC signature against it must fail closed rather
/// than treat the RSA public-key bytes as an HMAC secret.
fn create_algorithm_confusion_jwt() -> String {
    let now = now_timestamp();
    let claims = TestClaims {
        jti: uuid::Uuid::now_v7().to_string(),
        iss: TEST_ISSUER.to_string(),
        aud: Audience::Single(TEST_AUDIENCE.to_string()),
        sub: "external-a2a-user-123".to_string(),
        exp: now + 3600,
        iat: now,
        nbf: now,
    };

    let mut header = Header::new(Algorithm::HS256);
    header.kid = Some(TEST_KEY_ID.to_string());
    let encoding_key = EncodingKey::from_secret(b"attacker-controlled-hmac-secret");

    encode(&header, &claims, &encoding_key).unwrap()
}

/// Creates a valid-shape RS256 token stamped with an arbitrary `kid`. Used to
/// drive the unknown-key-id path: the issuer is trusted but no JWKS entry
/// matches, so verification reaches the fetch/rate-limit path and then fails.
fn create_jwt_with_kid(kid: &str) -> String {
    let now = now_timestamp();
    let claims = TestClaims {
        jti: uuid::Uuid::now_v7().to_string(),
        iss: TEST_ISSUER.to_string(),
        aud: Audience::Single(TEST_AUDIENCE.to_string()),
        sub: "external-a2a-user-123".to_string(),
        exp: now + 3600,
        iat: now,
        nbf: now,
    };

    let mut header = Header::new(Algorithm::RS256);
    header.kid = Some(kid.to_string());
    let encoding_key = EncodingKey::from_rsa_pem(TEST_PRIVATE_KEY).unwrap();

    encode(&header, &claims, &encoding_key).unwrap()
}

/// Create an IggyClient with the provided JWT token
async fn create_client_with_jwt(http_addr: &str, token: String) -> iggy::prelude::IggyClient {
    IggyClientBuilder::new()
        .with_http()
        .with_api_url(format!("http://{}", http_addr))
        .with_jwt(token)
        .build()
        .expect("failed to build client")
}

fn streams_permissions(manage_streams: bool) -> Permissions {
    Permissions {
        global: GlobalPermissions {
            manage_streams,
            read_streams: true,
            ..GlobalPermissions::default()
        },
        streams: None,
    }
}

/// Test that valid A2A JWT token allows access to API
#[iggy_harness(
    server(config_path = "tests/server/a2a_jwt/config.toml"),
    jwks_server(store_path = "tests/server/a2a_jwt/wiremock/__files/jwks.json"),
    seed = seed_a2a_user
)]
async fn test_a2a_jwt_valid_token(harness: &TestHarness) {
    let server = harness
        .all_servers()
        .first()
        .expect("server should be available");
    let http_addr = server
        .http_addr()
        .expect("http address should be available");

    let token = create_valid_jwt(3600);
    let client = create_client_with_jwt(&http_addr.to_string(), token).await;

    // get_streams() should succeed with valid JWT token
    let result = client.get_streams().await;
    assert!(result.is_ok(), "Expected Ok, got {:?}", result);
}

/// Test that valid A2A JWT token with array audience allows access to API
#[iggy_harness(
    server(config_path = "tests/server/a2a_jwt/config.toml"),
    jwks_server(store_path = "tests/server/a2a_jwt/wiremock/__files/jwks.json"),
    seed = seed_a2a_user
)]
async fn test_a2a_jwt_array_audience(harness: &TestHarness) {
    let server = harness
        .all_servers()
        .first()
        .expect("server should be available");
    let http_addr = server
        .http_addr()
        .expect("http address should be available");

    let token = create_valid_jwt_with_array_aud(3600);
    let client = create_client_with_jwt(&http_addr.to_string(), token).await;

    // get_streams() should succeed with valid JWT token
    let result = client.get_streams().await;
    assert!(result.is_ok(), "Expected Ok, got {:?}", result);
}

/// Test that expired A2A JWT token is rejected
#[iggy_harness(
    server(config_path = "tests/server/a2a_jwt/config.toml"),
    jwks_server(store_path = "tests/server/a2a_jwt/wiremock/__files/jwks.json")
)]
async fn test_a2a_jwt_expired_token(harness: &TestHarness) {
    let server = harness
        .all_servers()
        .first()
        .expect("server should be available");
    let http_addr = server
        .http_addr()
        .expect("http address should be available");

    let token = create_expired_jwt();
    let client = create_client_with_jwt(&http_addr.to_string(), token).await;

    // get_streams() should fail with Unauthenticated error
    let result = client.get_streams().await;
    assert!(
        result.is_err(),
        "Expected Unauthenticated error, got {:?}",
        result
    );
    let err = result.unwrap_err();
    assert_eq!(
        err.as_code(),
        iggy::prelude::IggyError::Unauthenticated.as_code(),
        "Expected Unauthenticated error, got {:?}",
        err
    );
}

/// Test that JWT token with unknown issuer is rejected
#[iggy_harness(
    server(config_path = "tests/server/a2a_jwt/config.toml"),
    jwks_server(store_path = "tests/server/a2a_jwt/wiremock/__files/jwks.json")
)]
async fn test_a2a_jwt_unknown_issuer(harness: &TestHarness) {
    let server = harness
        .all_servers()
        .first()
        .expect("server should be available");
    let http_addr = server
        .http_addr()
        .expect("http address should be available");

    let token = create_unknown_issuer_jwt();
    let client = create_client_with_jwt(&http_addr.to_string(), token).await;

    // get_streams() should fail with Unauthenticated error
    let result = client.get_streams().await;
    assert!(
        result.is_err(),
        "Expected Unauthenticated error, got {:?}",
        result
    );
    let err = result.unwrap_err();
    assert_eq!(
        err.as_code(),
        iggy::prelude::IggyError::Unauthenticated.as_code(),
        "Expected Unauthenticated error, got {:?}",
        err
    );
}

/// Test that an algorithm-confusion token is rejected: HS256-signed but claiming
/// the trusted issuer and the real JWKS `kid`. The issuer's key set holds only an
/// RSA key, so an HMAC signature must never verify against it - proving end to end
/// that the RSA public key can never be misused as an HMAC secret.
#[iggy_harness(
    server(config_path = "tests/server/a2a_jwt/config.toml"),
    jwks_server(store_path = "tests/server/a2a_jwt/wiremock/__files/jwks.json"),
    seed = seed_a2a_user
)]
async fn test_a2a_jwt_algorithm_confusion(harness: &TestHarness) {
    let server = harness
        .all_servers()
        .first()
        .expect("server should be available");
    let http_addr = server
        .http_addr()
        .expect("http address should be available");

    // First prove the JWKS path resolves this issuer and kid: a valid RS256 token
    // carrying the same kid must authenticate, warming the RSA key into cache.
    // Only then can the rejection below be attributed to the HMAC-vs-RSA
    // algorithm check rather than an incidental JWKS-fetch failure, which would
    // also surface as Unauthenticated and pass the assertion for the wrong reason.
    let valid_client = create_client_with_jwt(&http_addr.to_string(), create_valid_jwt(3600)).await;
    assert!(
        valid_client.get_streams().await.is_ok(),
        "valid RS256 token must authenticate (JWKS reachable, kid resolvable)"
    );

    let token = create_algorithm_confusion_jwt();
    let client = create_client_with_jwt(&http_addr.to_string(), token).await;

    // get_streams() should fail with Unauthenticated error
    let result = client.get_streams().await;
    assert!(
        result.is_err(),
        "algorithm-confusion token must be rejected, got {:?}",
        result
    );
    let err = result.unwrap_err();
    assert_eq!(
        err.as_code(),
        iggy::prelude::IggyError::Unauthenticated.as_code(),
        "Expected Unauthenticated error, got {:?}",
        err
    );
}

/// Test that a valid trusted-issuer (A2A) token cannot be refreshed. The same
/// token authenticates reads (see `test_a2a_jwt_valid_token`), but exchanging it
/// for a self-issued token would mint a bearer outliving the external grant the
/// issuer controls, so `POST /users/refresh-token` must reject it and issue
/// nothing.
#[iggy_harness(
    server(config_path = "tests/server/a2a_jwt/config.toml"),
    jwks_server(store_path = "tests/server/a2a_jwt/wiremock/__files/jwks.json"),
    seed = seed_a2a_user
)]
async fn test_a2a_jwt_refresh_rejected(harness: &TestHarness) {
    let server = harness
        .all_servers()
        .first()
        .expect("server should be available");
    let http_addr = server
        .http_addr()
        .expect("http address should be available");

    // Pre-set the A2A token as the client's bearer. `HttpClient`'s retry
    // middleware bridges the HTTP-listener warmup the readiness gate does not.
    let client = HttpClient::create(Arc::new(HttpClientConfig {
        api_url: format!("http://{}", http_addr),
        jwt: Some(create_valid_jwt(3600)),
        ..HttpClientConfig::default()
    }))
    .expect("failed to build HTTP client");

    // Prove the token authenticates a normal request first: a successful read
    // runs the same trusted-issuer JWKS verify, so the refresh rejection below is
    // the trusted-issuer refusal and not an incidental JWKS-fetch failure (both
    // collapse to Unauthenticated on the wire).
    assert!(
        client.get_streams().await.is_ok(),
        "A2A token must authenticate a read before refresh is attempted"
    );

    // An `Ok` would mean a self-issued token was handed back - the regression.
    let result = client.refresh_access_token().await;
    assert!(
        result.is_err(),
        "trusted-issuer token must not be refreshable, got {:?}",
        result
    );
    let err = result.unwrap_err();
    assert_eq!(
        err.as_code(),
        iggy::prelude::IggyError::Unauthenticated.as_code(),
        "Expected Unauthenticated error, got {:?}",
        err
    );
}

/// Test that missing JWT token results in authentication failure
#[iggy_harness(
    server(config_path = "tests/server/a2a_jwt/config.toml"),
    jwks_server(store_path = "tests/server/a2a_jwt/wiremock/__files/jwks.json")
)]
async fn test_a2a_jwt_missing_token(harness: &TestHarness) {
    let server = harness
        .all_servers()
        .first()
        .expect("server should be available");
    let http_addr = server
        .http_addr()
        .expect("http address should be available");

    // Create client without JWT token
    let client = IggyClientBuilder::new()
        .with_http()
        .with_api_url(format!("http://{}", http_addr))
        .build()
        .expect("failed to build client");

    // get_streams() should fail with Unauthenticated error
    let result = client.get_streams().await;
    assert!(
        result.is_err(),
        "Expected Unauthenticated error, got {:?}",
        result
    );
    let err = result.unwrap_err();
    assert_eq!(
        err.as_code(),
        iggy::prelude::IggyError::Unauthenticated.as_code(),
        "Expected Unauthenticated error, got {:?}",
        err
    );
}

/// A token that names the trusted issuer but carries an unknown `kid` reaches the
/// JWKS fetch path pre-signature. Replaying it must not amplify into one upstream
/// fetch per request: the per-issuer rate-limit / negative-cache window collapses
/// an unknown-key-id flood to at most one outbound JWKS request.
#[iggy_harness(
    server(config_path = "tests/server/a2a_jwt/config.toml"),
    jwks_server(store_path = "tests/server/a2a_jwt/wiremock/__files/jwks.json")
)]
async fn test_a2a_jwt_unknown_kid_does_not_amplify_jwks_fetch(harness: &TestHarness) {
    let server = harness
        .all_servers()
        .first()
        .expect("server should be available");
    let http_addr = server
        .http_addr()
        .expect("http address should be available");

    // The same unknown kid replayed: each request is a cache miss that classifies
    // the trusted issuer and would fetch its JWKS on every request if unbounded.
    let token = create_jwt_with_kid("novel-unknown-kid");
    for _ in 0..8 {
        let client = create_client_with_jwt(&http_addr.to_string(), token.clone()).await;
        let result = client.get_streams().await;
        assert!(
            result.is_err(),
            "unknown-kid token must be rejected, got {:?}",
            result
        );
    }

    // Exactly one fetch: the first miss reads the issuer's key set, and every
    // replay inside the refresh window is then served as a known-absent kid
    // without touching the network. Without the fix this would be one fetch per
    // request. The `== 1` also proves the path was exercised (not a vacuous 0).
    let fetches = harness.jwks_request_count().await;
    assert_eq!(
        fetches, 1,
        "unknown-kid replay must collapse to a single JWKS fetch, got {fetches}"
    );
}

/// The issuer maps onto a username, not a user id: once that user is deleted,
/// its tokens must stop working, also after another account takes the freed id.
#[iggy_harness(
    server(config_path = "tests/server/a2a_jwt/config.toml"),
    jwks_server(store_path = "tests/server/a2a_jwt/wiremock/__files/jwks.json"),
    seed = seed_a2a_user
)]
async fn test_a2a_jwt_rejected_after_mapped_user_deleted(harness: &TestHarness) {
    let http_addr = harness
        .all_servers()
        .first()
        .expect("server should be available")
        .http_addr()
        .expect("http address should be available");
    let client = create_client_with_jwt(&http_addr.to_string(), create_valid_jwt(3600)).await;
    assert!(
        client.get_streams().await.is_ok(),
        "A2A token must authenticate before the delete"
    );

    let root = harness.tcp_root_client().await.expect("TCP root client");
    let mapped = Identifier::named(JWKS_ISSUER_USERNAME).expect("valid username");
    let mapped_id = root
        .get_user(&mapped)
        .await
        .expect("get user")
        .expect("seeded user")
        .id;
    root.delete_user(&mapped).await.expect("delete user");
    let successor = root
        .create_user(
            "a2a-successor",
            A2A_PASSWORD,
            UserStatus::Active,
            Some(streams_permissions(false)),
        )
        .await
        .expect("create successor");
    assert_eq!(
        successor.id, mapped_id,
        "the successor must take the freed id, or this test proves nothing"
    );

    let error = client
        .get_streams()
        .await
        .expect_err("an A2A token must not act as the account that took the id");
    assert_eq!(error.as_code(), IggyError::Unauthenticated.as_code());
}

/// A write runs on a session cached per credential. After a rename moves the
/// username to another account, the token's writes must act as that account,
/// not as the renamed one its first write registered a session for.
#[iggy_harness(
    server(config_path = "tests/server/a2a_jwt/config.toml"),
    jwks_server(store_path = "tests/server/a2a_jwt/wiremock/__files/jwks.json"),
    seed = seed_a2a_user
)]
async fn test_a2a_jwt_writes_follow_username_after_rename(harness: &TestHarness) {
    let http_addr = harness
        .all_servers()
        .first()
        .expect("server should be available")
        .http_addr()
        .expect("http address should be available");
    let root = harness.tcp_root_client().await.expect("TCP root client");
    let mapped = Identifier::named(JWKS_ISSUER_USERNAME).expect("valid username");
    root.update_permissions(&mapped, Some(streams_permissions(true)))
        .await
        .expect("grant manage_streams");

    let client = create_client_with_jwt(&http_addr.to_string(), create_valid_jwt(3600)).await;
    client
        .create_stream("a2a-before-rename")
        .await
        .expect("A2A token must write as the mapped user");

    root.update_user(
        &mapped,
        Some("a2a-renamed"),
        None,
        &UserUpdateOptions::default(),
    )
    .await
    .expect("rename the mapped user");
    root.create_user(
        JWKS_ISSUER_USERNAME,
        A2A_PASSWORD,
        UserStatus::Active,
        Some(streams_permissions(false)),
    )
    .await
    .expect("create a new account under the mapped username");

    assert!(
        client.get_streams().await.is_ok(),
        "reads must act as the account that holds the username now"
    );
    let error = client
        .create_stream("a2a-after-rename")
        .await
        .expect_err("writes must not act as the renamed account");
    assert_eq!(error.as_code(), IggyError::Unauthorized.as_code());
}
