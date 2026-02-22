/*
 * Licensed to the Apache Software Foundation (ASF) under one
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

use integration::iggy_harness;
use jsonwebtoken::{Algorithm, EncodingKey, Header, encode};
use reqwest::Client;
use serde::{Deserialize, Serialize};

const TEST_ISSUER: &str = "https://test-issuer.com";
const TEST_AUDIENCE: &str = "iggy";
const TEST_KEY_ID: &str = "test-key-1";

/// Test claims structure for JWT tokens
#[derive(Debug, Serialize, Deserialize)]
struct TestClaims {
    jti: String,
    iss: String,
    aud: String,
    sub: String,
    exp: u64,
    iat: u64,
    nbf: u64,
}

/// Get current timestamp in seconds since Unix epoch
fn now_timestamp() -> i64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_secs() as i64
}

/// Creates a valid JWT token with specified expiration time
fn create_valid_jwt(exp_seconds: i64) -> String {
    let now = now_timestamp();
    let claims = TestClaims {
        jti: uuid::Uuid::now_v7().to_string(),
        iss: TEST_ISSUER.to_string(),
        aud: TEST_AUDIENCE.to_string(),
        sub: "0".to_string(),
        exp: (now + exp_seconds) as u64,
        iat: now as u64,
        nbf: now as u64,
    };

    let mut header = Header::new(Algorithm::RS256);
    header.kid = Some(TEST_KEY_ID.to_string());
    let private_key = include_bytes!("test_private_key.pem");
    let encoding_key = EncodingKey::from_rsa_pem(private_key).unwrap();

    encode(&header, &claims, &encoding_key).unwrap()
}

/// Creates an expired JWT token (expired 1 hour ago)
fn create_expired_jwt() -> String {
    let now = now_timestamp();
    let claims = TestClaims {
        jti: uuid::Uuid::now_v7().to_string(),
        iss: TEST_ISSUER.to_string(),
        aud: TEST_AUDIENCE.to_string(),
        sub: "0".to_string(),
        exp: (now - 3600) as u64,
        iat: (now - 7200) as u64,
        nbf: (now - 7200) as u64,
    };

    let mut header = Header::new(Algorithm::RS256);
    header.kid = Some(TEST_KEY_ID.to_string());
    let private_key = include_bytes!("test_private_key.pem");
    let encoding_key = EncodingKey::from_rsa_pem(private_key).unwrap();

    encode(&header, &claims, &encoding_key).unwrap()
}

/// Creates a JWT token with unknown issuer
fn create_unknown_issuer_jwt() -> String {
    let now = now_timestamp();
    let claims = TestClaims {
        jti: uuid::Uuid::now_v7().to_string(),
        iss: "https://unknown-issuer.com".to_string(),
        aud: TEST_AUDIENCE.to_string(),
        sub: "0".to_string(),
        exp: (now + 3600) as u64,
        iat: now as u64,
        nbf: now as u64,
    };

    let mut header = Header::new(Algorithm::RS256);
    header.kid = Some(TEST_KEY_ID.to_string());
    let private_key = include_bytes!("test_private_key.pem");
    let encoding_key = EncodingKey::from_rsa_pem(private_key).unwrap();

    encode(&header, &claims, &encoding_key).unwrap()
}

/// Test that valid A2A JWT token allows access to API
#[iggy_harness(
    server(config_path = "tests/server/a2a_jwt/config.toml"),
    jwks_server(store_path = "tests/server/a2a_jwt/wiremock/__files/jwks.json")
)]
async fn test_a2a_jwt_valid_token(harness: &TestHarness) {
    let server = harness
        .all_servers()
        .first()
        .expect("server should be available");
    let http_addr = server
        .http_addr()
        .expect("http address should be available");

    let client = Client::new();
    let token = create_valid_jwt(3600);

    let response = client
        .get(format!("http://{}/streams", http_addr))
        .header("Authorization", format!("Bearer {}", token))
        .send()
        .await
        .unwrap();

    assert_eq!(response.status(), 200);
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

    let client = Client::new();
    let token = create_expired_jwt();

    let response = client
        .get(format!("http://{}/streams", http_addr))
        .header("Authorization", format!("Bearer {}", token))
        .send()
        .await
        .unwrap();

    assert_eq!(response.status(), 401);
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

    let client = Client::new();
    let token = create_unknown_issuer_jwt();

    let response = client
        .get(format!("http://{}/streams", http_addr))
        .header("Authorization", format!("Bearer {}", token))
        .send()
        .await
        .unwrap();

    assert_eq!(response.status(), 401);
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

    let client = Client::new();

    // Send request without Authorization header
    let response = client
        .get(format!("http://{}/streams", http_addr))
        .send()
        .await
        .unwrap();

    assert_eq!(response.status(), 401);
}
