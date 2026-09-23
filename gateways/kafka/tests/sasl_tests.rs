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

//! SASL authentication over a real socket.
//!
//! Drives the whole exchange against a running `KafkaGateway` with a stub verifier standing in
//! for Iggy, so these cover the listener's state machine rather than Iggy's credential checking.

use std::net::SocketAddr;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::Duration;

use async_trait::async_trait;
use bytes::{BufMut, Bytes, BytesMut};
use tokio::io::AsyncWriteExt;
use tokio::net::TcpStream;

use iggy_gateway_kafka::GatewayConfig;
use iggy_gateway_kafka::auth::{AuthError, AuthenticatedPrincipal, SaslAuthenticator};
use iggy_gateway_kafka::protocol::acl::PrincipalPermissions;
use iggy_gateway_kafka::protocol::sasl::PlainCredentials;

#[path = "common/codec.rs"]
mod codec;
#[path = "common/server.rs"]
mod server;
#[path = "common/tcp.rs"]
mod tcp;

use server::spawn_test_server_with_authenticator;
use tcp::{ByteRead, build_request_frame, parse_response_payload, read_byte_with_timeout};

const API_KEY_PRODUCE: i16 = 0;
const API_KEY_METADATA: i16 = 3;
const API_KEY_SASL_HANDSHAKE: i16 = 17;
const API_KEY_API_VERSIONS: i16 = 18;
const API_KEY_SASL_AUTHENTICATE: i16 = 36;

const ERROR_NONE: i16 = 0;
const ERROR_UNSUPPORTED_SASL_MECHANISM: i16 = 33;
const ERROR_ILLEGAL_SASL_STATE: i16 = 34;
const ERROR_UNSUPPORTED_VERSION: i16 = 35;
const ERROR_SASL_AUTHENTICATION_FAILED: i16 = 58;
const ERROR_UNKNOWN_SERVER_ERROR: i16 = -1;

const HANDSHAKE_VERSION: i16 = 1;
const AUTHENTICATE_VERSION: i16 = 1;

/// Accepts one specific credential pair and rejects everything else.
#[derive(Debug)]
struct FixedCredentialAuthenticator {
    username: &'static str,
    password: &'static str,
    permissions: PrincipalPermissions,
}

#[async_trait]
impl SaslAuthenticator for FixedCredentialAuthenticator {
    async fn authenticate(
        &self,
        credentials: &PlainCredentials,
    ) -> Result<AuthenticatedPrincipal, AuthError> {
        use secrecy::ExposeSecret;
        let matches = credentials.username == self.username
            && credentials.password.expose_secret() == self.password;
        if matches {
            Ok(AuthenticatedPrincipal {
                username: credentials.username.clone(),
                permissions: self.permissions,
                permissions_known: true,
            })
        } else {
            Err(AuthError::Rejected)
        }
    }
}

fn sasl_config() -> GatewayConfig {
    GatewayConfig {
        sasl_enabled: true,
        idle_timeout: Duration::from_secs(5),
        read_timeout: Duration::from_secs(5),
        write_timeout: Duration::from_secs(5),
        shutdown_drain_timeout: Duration::from_secs(5),
        ..GatewayConfig::default()
    }
}

async fn spawn_sasl_gateway() -> SocketAddr {
    let authenticator = Arc::new(FixedCredentialAuthenticator {
        username: "alice",
        password: "s3cret",
        permissions: PrincipalPermissions::default(),
    });
    let (addr, shutdown) = spawn_test_server_with_authenticator(sasl_config(), authenticator).await;
    // Held for the whole test: dropping the sender shuts the gateway down mid-exchange.
    std::mem::forget(shutdown);
    addr
}

/// `SaslHandshake` body: one legacy (non-compact) string, at both v0 and v1.
fn handshake_body(mechanism: &str) -> Bytes {
    let mut buf = BytesMut::new();
    buf.put_i16(i16::try_from(mechanism.len()).expect("test mechanism fits i16"));
    buf.extend_from_slice(mechanism.as_bytes());
    buf.freeze()
}

/// `SaslAuthenticate` body at v0/v1: one legacy length-prefixed bytes field.
fn authenticate_body(auth_bytes: &[u8]) -> Bytes {
    let mut buf = BytesMut::new();
    buf.put_i32(i32::try_from(auth_bytes.len()).expect("test token fits i32"));
    buf.extend_from_slice(auth_bytes);
    buf.freeze()
}

/// `SaslAuthenticate` body at v2, where the token is a compact bytes field and a tagged-fields
/// byte follows it. Modern clients negotiate v2, so this framing is the one they actually send.
fn authenticate_body_v2(auth_bytes: &[u8]) -> Bytes {
    let mut buf = BytesMut::new();
    // Compact bytes: unsigned varint of len + 1. Single byte for anything under 127.
    let len = u8::try_from(auth_bytes.len() + 1).expect("test token is short");
    buf.put_u8(len);
    buf.extend_from_slice(auth_bytes);
    buf.put_u8(0);
    buf.freeze()
}

fn plain_token(username: &str, password: &str) -> Vec<u8> {
    let mut token = vec![0u8];
    token.extend_from_slice(username.as_bytes());
    token.push(0);
    token.extend_from_slice(password.as_bytes());
    token
}

/// Sends one request on an existing stream and returns the decoded response body.
async fn send(
    stream: &mut TcpStream,
    api_key: i16,
    api_version: i16,
    correlation_id: i32,
    body: &[u8],
) -> Bytes {
    let frame = build_request_frame(
        api_key,
        api_version,
        correlation_id,
        Some("sasl-test"),
        body,
    );
    stream.write_all(&frame).await.expect("write request");
    let payload = tcp::read_response_frame(stream, 8 * 1024 * 1024).await;
    let (echoed, response) = parse_response_payload(api_key, api_version, payload);
    assert_eq!(echoed, correlation_id, "correlation id must be echoed");
    response
}

/// `DescribeAcls` puts a `throttle_time_ms` (`i32`) *before* its error code, unlike the SASL
/// responses. Reading offset 0 there returns the always-zero high half of the throttle field, so
/// every assertion comparing it against 0 passes no matter what the gateway answered.
fn acl_error_code(body: &Bytes) -> i16 {
    assert!(body.len() >= 6, "DescribeAcls response is too short");
    i16::from_be_bytes([body[4], body[5]])
}

/// Decodes a `DescribeAcls` v1 response into
/// `(resource_type, resource_name, pattern_type, operation, permission_type)` tuples.
///
/// Pattern type and permission type are decoded rather than stepped over deliberately. They carry
/// the security claim of the whole surface: a response that said DENY, or a prefixed pattern,
/// would otherwise pass every test in this crate while meaning something entirely different.
fn parse_acl_bindings(body: &Bytes) -> Vec<(i8, String, i8, i8, i8)> {
    // Every read is bounds-checked by field name. A decoder that walks a response with raw
    // indexing fails as an index-out-of-bounds naming no field, and the more dangerous case is
    // quieter still: drift by a few bytes and it returns plausible-looking bindings decoded from
    // the wrong offsets. That already happened once in this suite, where `error_code` read the
    // first two bytes of a body whose first field is `throttle_time_ms`, so every assertion
    // compared 0 against 0 and passed.
    let need = |at: usize, count: usize, field: &str| {
        assert!(
            at + count <= body.len(),
            "ran past the end of the response reading {field}: wanted {count} byte(s) at {at}, \
             body holds {}. The decoder is out of step with the response shape.",
            body.len()
        );
    };

    let mut at = 6; // throttle_time_ms + error_code
    // error_message: nullable string
    need(at, 2, "error_message length");
    let len = i16::from_be_bytes([body[at], body[at + 1]]);
    at += 2;
    if len >= 0 {
        at += usize::try_from(len).expect("non-negative string length");
    }
    need(at, 4, "resource count");
    let resource_count = i32::from_be_bytes([body[at], body[at + 1], body[at + 2], body[at + 3]]);
    at += 4;

    let mut out = Vec::new();
    for _ in 0..resource_count.max(0) {
        need(at, 3, "resource type and name length");
        let resource_type = body[at].cast_signed();
        at += 1;
        let name_len = usize::try_from(i16::from_be_bytes([body[at], body[at + 1]]))
            .expect("non-negative name length");
        at += 2;
        need(at, name_len, "resource name");
        let name = String::from_utf8_lossy(&body[at..at + name_len]).to_string();
        at += name_len;
        need(at, 5, "pattern type and acl count");
        let pattern_type = body[at].cast_signed();
        at += 1;
        let acl_count = i32::from_be_bytes([body[at], body[at + 1], body[at + 2], body[at + 3]]);
        at += 4;
        for _ in 0..acl_count.max(0) {
            need(at, 2, "principal length");
            let principal_len = usize::try_from(i16::from_be_bytes([body[at], body[at + 1]]))
                .expect("non-negative principal length");
            at += 2;
            need(at, principal_len + 2, "principal and host length");
            at += principal_len;
            let host_len = usize::try_from(i16::from_be_bytes([body[at], body[at + 1]]))
                .expect("non-negative host length");
            at += 2;
            need(at, host_len + 2, "host, operation and permission type");
            at += host_len;
            let operation = body[at].cast_signed();
            at += 1;
            let permission_type = body[at].cast_signed();
            at += 1;
            out.push((
                resource_type,
                name.clone(),
                pattern_type,
                operation,
                permission_type,
            ));
        }
    }

    // Trailing bytes mean the walk is out of step even though every individual read fit, which is
    // the failure that returns plausible bindings rather than panicking.
    assert_eq!(
        at,
        body.len(),
        "decoder stopped {} byte(s) short of the end, so the bindings above were read at the \
         wrong offsets",
        body.len() - at
    );
    out
}

/// First `i16` of a `SaslHandshake` or `SaslAuthenticate` response body is its error code.
fn error_code(body: &Bytes) -> i16 {
    assert!(body.len() >= 2, "response body is too short to hold a code");
    i16::from_be_bytes([body[0], body[1]])
}

async fn assert_closed(stream: &mut TcpStream) {
    let read = read_byte_with_timeout(stream, Duration::from_secs(5)).await;
    assert!(
        matches!(read, ByteRead::Closed),
        "connection should have been closed, got {read:?}"
    );
}

async fn handshake_ok(stream: &mut TcpStream) {
    let body = send(
        stream,
        API_KEY_SASL_HANDSHAKE,
        HANDSHAKE_VERSION,
        1,
        &handshake_body("PLAIN"),
    )
    .await;
    assert_eq!(error_code(&body), ERROR_NONE, "PLAIN must be accepted");
}

#[tokio::test]
async fn given_valid_credentials_when_authenticating_should_serve_normal_requests() {
    let addr = spawn_sasl_gateway().await;
    let mut stream = TcpStream::connect(addr).await.expect("connect");

    // ApiVersions is legal before the handshake, and must advertise the SASL keys so the client
    // knows which token framing to use.
    let advertised = send(&mut stream, API_KEY_API_VERSIONS, 1, 1, &[]).await;
    assert!(
        advertised
            .windows(2)
            .any(|w| i16::from_be_bytes([w[0], w[1]]) == API_KEY_SASL_AUTHENTICATE),
        "SaslAuthenticate must be advertised while SASL is enabled"
    );

    handshake_ok(&mut stream).await;

    let token = plain_token("alice", "s3cret");
    let body = send(
        &mut stream,
        API_KEY_SASL_AUTHENTICATE,
        AUTHENTICATE_VERSION,
        2,
        &authenticate_body(&token),
    )
    .await;
    assert_eq!(error_code(&body), ERROR_NONE, "valid credentials accepted");

    // The connection now serves ordinary traffic, and survives it.
    let metadata = send(&mut stream, API_KEY_METADATA, 0, 3, &[0, 0, 0, 0]).await;
    assert!(
        !metadata.is_empty(),
        "Metadata must answer once authenticated"
    );

    // A real Java client sends ApiVersions a second time after authenticating.
    let second = send(&mut stream, API_KEY_API_VERSIONS, 1, 4, &[]).await;
    assert!(!second.is_empty(), "ApiVersions must stay legal after auth");
}

#[tokio::test]
async fn given_wrong_credentials_when_authenticating_should_fail_then_close() {
    let addr = spawn_sasl_gateway().await;
    let mut stream = TcpStream::connect(addr).await.expect("connect");
    handshake_ok(&mut stream).await;

    let token = plain_token("alice", "wrong-password");
    let body = send(
        &mut stream,
        API_KEY_SASL_AUTHENTICATE,
        AUTHENTICATE_VERSION,
        2,
        &authenticate_body(&token),
    )
    .await;
    assert_eq!(error_code(&body), ERROR_SASL_AUTHENTICATION_FAILED);
    assert_closed(&mut stream).await;
}

#[tokio::test]
async fn given_a_malformed_plain_token_when_authenticating_should_fail_like_a_bad_password() {
    let addr = spawn_sasl_gateway().await;
    let mut stream = TcpStream::connect(addr).await.expect("connect");
    handshake_ok(&mut stream).await;

    // Two fields instead of three. Indistinguishable from a rejection, on purpose.
    let body = send(
        &mut stream,
        API_KEY_SASL_AUTHENTICATE,
        AUTHENTICATE_VERSION,
        2,
        &authenticate_body(b"alice\0s3cret"),
    )
    .await;
    assert_eq!(error_code(&body), ERROR_SASL_AUTHENTICATION_FAILED);
    assert_closed(&mut stream).await;
}

#[tokio::test]
async fn given_an_unknown_mechanism_when_handshaking_should_report_the_supported_list() {
    let addr = spawn_sasl_gateway().await;
    let mut stream = TcpStream::connect(addr).await.expect("connect");

    let body = send(
        &mut stream,
        API_KEY_SASL_HANDSHAKE,
        HANDSHAKE_VERSION,
        1,
        &handshake_body("SCRAM-SHA-256"),
    )
    .await;
    assert_eq!(error_code(&body), ERROR_UNSUPPORTED_SASL_MECHANISM);
    assert!(
        body.windows(5).any(|w| w == b"PLAIN"),
        "the refusal must still name what is supported, or the operator cannot act on it"
    );
    assert_closed(&mut stream).await;
}

#[tokio::test]
async fn given_handshake_v0_when_offered_should_be_refused_before_any_headerless_token() {
    let addr = spawn_sasl_gateway().await;
    let mut stream = TcpStream::connect(addr).await.expect("connect");

    // v0 selects the pre-KIP-152 framing, where tokens arrive with no request header at all.
    let body = send(
        &mut stream,
        API_KEY_SASL_HANDSHAKE,
        0,
        1,
        &handshake_body("PLAIN"),
    )
    .await;
    assert_eq!(error_code(&body), ERROR_UNSUPPORTED_VERSION);
    assert_closed(&mut stream).await;
}

#[tokio::test]
async fn given_an_unauthenticated_connection_when_a_normal_request_arrives_should_refuse_it() {
    let addr = spawn_sasl_gateway().await;
    let mut stream = TcpStream::connect(addr).await.expect("connect");

    // Metadata carries no top-level error field at this version, so there is nowhere well-formed
    // to put the code and the connection closes without a body.
    let frame = build_request_frame(API_KEY_METADATA, 0, 1, Some("sasl-test"), &[0, 0, 0, 0]);
    stream.write_all(&frame).await.expect("write request");
    assert_closed(&mut stream).await;
}

#[tokio::test]
async fn given_an_unauthenticated_connection_when_produce_arrives_should_close_without_answering() {
    let addr = spawn_sasl_gateway().await;
    let mut stream = TcpStream::connect(addr).await.expect("connect");

    // Produce never gets a body on this path: acks=0 forbids a response and the acks value is not
    // knowable without decoding a body this connection has not earned.
    let mut body = BytesMut::new();
    body.put_i16(-1);
    body.put_i16(1);
    body.put_i32(1000);
    body.put_i32(0);
    let frame = build_request_frame(API_KEY_PRODUCE, 3, 1, Some("sasl-test"), &body.freeze());
    stream.write_all(&frame).await.expect("write request");
    assert_closed(&mut stream).await;
}

#[tokio::test]
async fn given_a_token_before_a_handshake_should_be_an_illegal_state() {
    let addr = spawn_sasl_gateway().await;
    let mut stream = TcpStream::connect(addr).await.expect("connect");

    let token = plain_token("alice", "s3cret");
    let body = send(
        &mut stream,
        API_KEY_SASL_AUTHENTICATE,
        AUTHENTICATE_VERSION,
        1,
        &authenticate_body(&token),
    )
    .await;
    assert_eq!(
        error_code(&body),
        ERROR_ILLEGAL_SASL_STATE,
        "a token with no negotiated mechanism is an ordering violation, not a bad password"
    );
    assert_closed(&mut stream).await;
}

#[tokio::test]
async fn given_a_second_handshake_after_authenticating_should_be_refused_without_closing() {
    let addr = spawn_sasl_gateway().await;
    let mut stream = TcpStream::connect(addr).await.expect("connect");
    handshake_ok(&mut stream).await;

    let token = plain_token("alice", "s3cret");
    let authenticated = send(
        &mut stream,
        API_KEY_SASL_AUTHENTICATE,
        AUTHENTICATE_VERSION,
        2,
        &authenticate_body(&token),
    )
    .await;
    assert_eq!(error_code(&authenticated), ERROR_NONE);

    let body = send(
        &mut stream,
        API_KEY_SASL_HANDSHAKE,
        HANDSHAKE_VERSION,
        3,
        &handshake_body("PLAIN"),
    )
    .await;
    assert_eq!(error_code(&body), ERROR_ILLEGAL_SASL_STATE);

    // Still usable: a real broker does not drop an authenticated connection over this.
    let metadata = send(&mut stream, API_KEY_METADATA, 0, 4, &[0, 0, 0, 0]).await;
    assert!(!metadata.is_empty(), "connection must survive the refusal");
}

#[tokio::test]
async fn given_sasl_disabled_when_a_client_connects_should_serve_without_authenticating() {
    // The default configuration, which is what every existing deployment runs.
    let (addr, shutdown) = server::spawn_test_server().await;
    std::mem::forget(shutdown);
    let mut stream = TcpStream::connect(addr).await.expect("connect");

    let metadata = send(&mut stream, API_KEY_METADATA, 0, 1, &[0, 0, 0, 0]).await;
    assert!(!metadata.is_empty(), "Metadata must answer with SASL off");

    let advertised = send(&mut stream, API_KEY_API_VERSIONS, 1, 2, &[]).await;
    assert!(
        !advertised
            .windows(2)
            .any(|w| i16::from_be_bytes([w[0], w[1]]) == API_KEY_SASL_HANDSHAKE),
        "the SASL keys must not be advertised while the feature is off"
    );
}

#[tokio::test]
async fn given_sasl_disabled_when_a_handshake_arrives_should_refuse_it_without_closing() {
    let (addr, shutdown) = server::spawn_test_server().await;
    std::mem::forget(shutdown);
    let mut stream = TcpStream::connect(addr).await.expect("connect");

    // The keys are unadvertised with SASL off, so no conformant client sends this. One that does
    // gets the same answer a real broker gives on a PLAINTEXT listener: a parseable refusal, not a
    // dropped connection. A well-formed response exists here, unlike a genuinely unknown key.
    let body = send(
        &mut stream,
        API_KEY_SASL_HANDSHAKE,
        HANDSHAKE_VERSION,
        1,
        &handshake_body("PLAIN"),
    )
    .await;
    assert_eq!(error_code(&body), ERROR_ILLEGAL_SASL_STATE);

    let metadata = send(&mut stream, API_KEY_METADATA, 0, 2, &[0, 0, 0, 0]).await;
    assert!(!metadata.is_empty(), "connection must survive the refusal");
}

#[tokio::test]
async fn given_authenticate_v2_when_authenticating_should_accept_the_compact_framing() {
    let addr = spawn_sasl_gateway().await;
    let mut stream = TcpStream::connect(addr).await.expect("connect");
    handshake_ok(&mut stream).await;

    // v2 is the highest version advertised, uses compact bytes plus tagged fields in the request,
    // and shifts the response header to v1. None of that is exercised by the v0/v1 framing.
    let token = plain_token("alice", "s3cret");
    let body = send(
        &mut stream,
        API_KEY_SASL_AUTHENTICATE,
        2,
        2,
        &authenticate_body_v2(&token),
    )
    .await;
    assert_eq!(error_code(&body), ERROR_NONE, "v2 credentials accepted");

    let metadata = send(&mut stream, API_KEY_METADATA, 0, 3, &[0, 0, 0, 0]).await;
    assert!(
        !metadata.is_empty(),
        "connection serves traffic after a v2 login"
    );
}

#[tokio::test]
async fn given_authenticate_v2_with_bad_credentials_should_fail_then_close() {
    let addr = spawn_sasl_gateway().await;
    let mut stream = TcpStream::connect(addr).await.expect("connect");
    handshake_ok(&mut stream).await;

    let token = plain_token("alice", "wrong");
    let body = send(
        &mut stream,
        API_KEY_SASL_AUTHENTICATE,
        2,
        2,
        &authenticate_body_v2(&token),
    )
    .await;
    assert_eq!(error_code(&body), ERROR_SASL_AUTHENTICATION_FAILED);
    assert_closed(&mut stream).await;
}

/// Parses an `ApiVersions` v1 response body into `(api_key, min_version, max_version)` rows.
fn parse_api_versions(body: &Bytes) -> Vec<(i16, i16, i16)> {
    assert!(body.len() >= 6, "ApiVersions response is too short");
    let count = i32::from_be_bytes([body[2], body[3], body[4], body[5]]);
    let mut rows = Vec::new();
    let mut at = 6;
    for _ in 0..count {
        assert!(at + 6 <= body.len(), "truncated ApiVersions entry");
        rows.push((
            i16::from_be_bytes([body[at], body[at + 1]]),
            i16::from_be_bytes([body[at + 2], body[at + 3]]),
            i16::from_be_bytes([body[at + 4], body[at + 5]]),
        ));
        at += 6;
    }
    rows
}

#[tokio::test]
async fn given_sasl_enabled_when_advertising_should_offer_handshake_from_version_zero() {
    // Regression. Advertising SaslHandshake as v1-only is what a version firewall pinned to v1
    // suggests, and it breaks librdkafka outright: its SASL-handshake feature detection depends on
    // key 17 appearing at version 0, so it reports "SASL Handshake not supported by broker" and
    // never sends one. Advertising from 0 while still refusing a v0 handshake at runtime is the
    // same split Produce already uses. Caught by a real client, not by a stub.
    let addr = spawn_sasl_gateway().await;
    let mut stream = TcpStream::connect(addr).await.expect("connect");

    let body = send(&mut stream, API_KEY_API_VERSIONS, 1, 1, &[]).await;
    let rows = parse_api_versions(&body);

    let handshake = rows
        .iter()
        .find(|(key, _, _)| *key == API_KEY_SASL_HANDSHAKE)
        .expect("SaslHandshake must be advertised while SASL is enabled");
    assert_eq!(
        handshake.1, 0,
        "librdkafka needs SaslHandshake advertised from v0 or it sees no SASL at all"
    );
    assert_eq!(
        handshake.2, 1,
        "v0 framing is refused, so v1 is the ceiling"
    );

    let authenticate = rows
        .iter()
        .find(|(key, _, _)| *key == API_KEY_SASL_AUTHENTICATE)
        .expect("SaslAuthenticate must be advertised while SASL is enabled");
    assert_eq!((authenticate.1, authenticate.2), (0, 2));
}

#[tokio::test]
async fn given_sasl_disabled_when_advertising_should_offer_neither_sasl_key() {
    let (addr, shutdown) = server::spawn_test_server().await;
    std::mem::forget(shutdown);
    let mut stream = TcpStream::connect(addr).await.expect("connect");

    let body = send(&mut stream, API_KEY_API_VERSIONS, 1, 1, &[]).await;
    let rows = parse_api_versions(&body);
    assert!(
        !rows
            .iter()
            .any(|(key, _, _)| *key == API_KEY_SASL_HANDSHAKE || *key == API_KEY_SASL_AUTHENTICATE),
        "advertising SASL while it is off would invite clients into an exchange that cannot finish"
    );
}

#[tokio::test]
async fn given_an_unauthenticated_connection_when_it_goes_quiet_should_be_dropped_on_the_pre_auth_budget()
 {
    // An unauthenticated connection holds a `max_connections` permit while proving nothing, so it
    // gets a budget measured in seconds rather than the ten-minute idle timeout.
    let config = GatewayConfig {
        pre_auth_timeout: Duration::from_millis(400),
        idle_timeout: Duration::from_secs(30),
        ..sasl_config()
    };
    let authenticator = Arc::new(FixedCredentialAuthenticator {
        username: "alice",
        password: "s3cret",
        permissions: PrincipalPermissions::default(),
    });
    let (addr, shutdown) = spawn_test_server_with_authenticator(config, authenticator).await;
    std::mem::forget(shutdown);

    let mut stream = TcpStream::connect(addr).await.expect("connect");
    // Say nothing at all. The idle timeout is 30s, so a drop can only come from the pre-auth one.
    assert_closed(&mut stream).await;
}

#[tokio::test]
async fn given_an_authenticated_connection_when_it_goes_quiet_should_keep_the_longer_budget() {
    // The mirror image: once authenticated, the short budget must no longer apply, or every idle
    // client would be disconnected seconds after logging in.
    let config = GatewayConfig {
        pre_auth_timeout: Duration::from_millis(300),
        idle_timeout: Duration::from_secs(30),
        ..sasl_config()
    };
    let authenticator = Arc::new(FixedCredentialAuthenticator {
        username: "alice",
        password: "s3cret",
        permissions: PrincipalPermissions::default(),
    });
    let (addr, shutdown) = spawn_test_server_with_authenticator(config, authenticator).await;
    std::mem::forget(shutdown);

    let mut stream = TcpStream::connect(addr).await.expect("connect");
    handshake_ok(&mut stream).await;
    let token = plain_token("alice", "s3cret");
    let body = send(
        &mut stream,
        API_KEY_SASL_AUTHENTICATE,
        AUTHENTICATE_VERSION,
        2,
        &authenticate_body(&token),
    )
    .await;
    assert_eq!(error_code(&body), ERROR_NONE);

    tokio::time::sleep(Duration::from_millis(900)).await;
    let metadata = send(&mut stream, API_KEY_METADATA, 0, 3, &[0, 0, 0, 0]).await;
    assert!(
        !metadata.is_empty(),
        "an authenticated connection must not inherit the pre-auth budget"
    );
}

#[tokio::test]
async fn given_an_oversized_token_when_authenticating_should_be_refused_without_decoding_it() {
    let addr = spawn_sasl_gateway().await;
    let mut stream = TcpStream::connect(addr).await.expect("connect");
    handshake_ok(&mut stream).await;

    // Well past the guard's cap. This is the one frame an unauthenticated connection can send
    // repeatedly, so it must be bounded well below `max_frame_size`.
    let mut oversized = plain_token("alice", "s3cret");
    oversized.extend(std::iter::repeat_n(b'A', 8192));
    let body = send(
        &mut stream,
        API_KEY_SASL_AUTHENTICATE,
        AUTHENTICATE_VERSION,
        2,
        &authenticate_body(&oversized),
    )
    .await;
    assert_eq!(
        error_code(&body),
        ERROR_SASL_AUTHENTICATION_FAILED,
        "an oversized token is refused as a credential failure, telling the client nothing extra"
    );
    assert_closed(&mut stream).await;
}

/// Authenticator that always reports Iggy as unreachable.
#[derive(Debug)]
struct UnavailableAuthenticator;

#[async_trait]
impl SaslAuthenticator for UnavailableAuthenticator {
    async fn authenticate(
        &self,
        _credentials: &PlainCredentials,
    ) -> Result<AuthenticatedPrincipal, AuthError> {
        Err(AuthError::Unavailable)
    }
}

#[tokio::test]
async fn given_iggy_is_unreachable_when_authenticating_should_not_send_a_terminal_auth_error() {
    // 58 is fatal to a Kafka client: Java raises it to the application rather than retrying. An
    // Iggy outage must therefore not borrow it, or a momentary blip becomes a permanent
    // authentication error for credentials that were always correct. Closing reads as a transport
    // failure, which is retriable, and still says nothing about whether the account exists.
    let (addr, shutdown) =
        spawn_test_server_with_authenticator(sasl_config(), Arc::new(UnavailableAuthenticator))
            .await;
    std::mem::forget(shutdown);
    let mut stream = TcpStream::connect(addr).await.expect("connect");
    handshake_ok(&mut stream).await;

    let token = plain_token("alice", "s3cret");
    let frame = build_request_frame(
        API_KEY_SASL_AUTHENTICATE,
        AUTHENTICATE_VERSION,
        2,
        Some("sasl-test"),
        &authenticate_body(&token),
    );
    stream.write_all(&frame).await.expect("write request");
    assert_closed(&mut stream).await;
}

#[tokio::test]
async fn given_a_second_pre_auth_api_versions_should_be_refused_and_close() {
    // Without this the pre-auth read budget resets on every frame, so a client that never
    // authenticates holds a connection permit indefinitely by sending ApiVersions on a timer.
    let addr = spawn_sasl_gateway().await;
    let mut stream = TcpStream::connect(addr).await.expect("connect");

    let first = send(&mut stream, API_KEY_API_VERSIONS, 1, 1, &[]).await;
    assert!(!first.is_empty(), "the first ApiVersions is answered");

    let second = send(&mut stream, API_KEY_API_VERSIONS, 1, 2, &[]).await;
    assert_eq!(
        error_code(&second),
        ERROR_ILLEGAL_SASL_STATE,
        "a real broker allows exactly one ApiVersions before the handshake"
    );
    assert_closed(&mut stream).await;
}

#[tokio::test]
async fn given_an_illegal_state_handshake_should_not_disclose_the_mechanism_list() {
    // With SASL off, an unauthenticated scanner must not learn the deployment can speak PLAIN.
    // A real broker answers ILLEGAL_SASL_STATE with an empty mechanism list.
    let (addr, shutdown) = server::spawn_test_server().await;
    std::mem::forget(shutdown);
    let mut stream = TcpStream::connect(addr).await.expect("connect");

    let body = send(
        &mut stream,
        API_KEY_SASL_HANDSHAKE,
        HANDSHAKE_VERSION,
        1,
        &handshake_body("PLAIN"),
    )
    .await;
    assert_eq!(error_code(&body), ERROR_ILLEGAL_SASL_STATE);
    assert!(
        !body.windows(5).any(|w| w == b"PLAIN"),
        "an out-of-order request must not be answered with the mechanism list"
    );
}

#[tokio::test]
async fn given_sasl_authenticate_after_authenticating_should_be_refused_without_closing() {
    let addr = spawn_sasl_gateway().await;
    let mut stream = TcpStream::connect(addr).await.expect("connect");
    handshake_ok(&mut stream).await;
    let token = plain_token("alice", "s3cret");
    let ok = send(
        &mut stream,
        API_KEY_SASL_AUTHENTICATE,
        AUTHENTICATE_VERSION,
        2,
        &authenticate_body(&token),
    )
    .await;
    assert_eq!(error_code(&ok), ERROR_NONE);

    // The sibling of the second-handshake case, on the other SASL key.
    let body = send(
        &mut stream,
        API_KEY_SASL_AUTHENTICATE,
        AUTHENTICATE_VERSION,
        3,
        &authenticate_body(&token),
    )
    .await;
    assert_eq!(error_code(&body), ERROR_ILLEGAL_SASL_STATE);

    let metadata = send(&mut stream, API_KEY_METADATA, 0, 4, &[0, 0, 0, 0]).await;
    assert!(!metadata.is_empty(), "connection must survive the refusal");
}

#[tokio::test]
async fn given_an_unauthenticated_fetch_should_get_a_parseable_body_before_the_close() {
    // `encode_error_for_key`'s whole reason to exist: an unauthenticated client gets a body shaped
    // for the version it asked for, then the drop. Nothing exercised these encoders before.
    let addr = spawn_sasl_gateway().await;

    for (api_key, api_version, body) in [
        (1i16, 4i16, vec![0u8; 0]),
        (2, 1, vec![0u8; 0]),
        (19, 2, vec![0u8; 0]),
    ] {
        let mut stream = TcpStream::connect(addr).await.expect("connect");
        let frame = build_request_frame(api_key, api_version, 1, Some("sasl-test"), &body);
        stream.write_all(&frame).await.expect("write request");
        let payload = tcp::read_response_frame(&mut stream, 8 * 1024 * 1024).await;
        let (_, response) = parse_response_payload(api_key, api_version, payload);
        assert!(
            !response.is_empty(),
            "api_key {api_key} must get a parseable refusal body"
        );
        assert!(
            tcp::scan_for_error_code(&response, ERROR_ILLEGAL_SASL_STATE),
            "api_key {api_key} refusal must carry ILLEGAL_SASL_STATE"
        );
        assert_closed(&mut stream).await;
    }
}

#[tokio::test]
async fn given_ascending_order_when_advertising_should_match_what_a_real_broker_sends() {
    let addr = spawn_sasl_gateway().await;
    let mut stream = TcpStream::connect(addr).await.expect("connect");
    let body = send(&mut stream, API_KEY_API_VERSIONS, 1, 1, &[]).await;
    let keys: Vec<i16> = parse_api_versions(&body)
        .into_iter()
        .map(|(key, _, _)| key)
        .collect();
    let mut sorted = keys.clone();
    sorted.sort_unstable();
    assert_eq!(
        keys, sorted,
        "every real broker emits api_key ascending; anything binary-searching this would break"
    );
}

#[tokio::test]
async fn given_sasl_enabled_without_an_authenticator_should_refuse_to_start() {
    // Driven through `run` directly: the shared spawn helper discards the Result, so a test
    // written against it would pass whatever this guard did.
    use iggy_gateway_kafka::KafkaGateway;
    use iggy_gateway_kafka::server::bind_listener;

    let listener = bind_listener("127.0.0.1:0").expect("bind ephemeral port");
    let (_tx, rx) = tokio::sync::broadcast::channel(1);
    let result = KafkaGateway::new(sasl_config()).run(listener, rx).await;
    assert!(
        result.is_err(),
        "a gateway demanding SASL with nothing to verify against would reject every client"
    );
}

#[tokio::test]
async fn given_an_authenticator_without_sasl_enabled_should_refuse_to_start() {
    // The quieter half: a verifier attached while the flag is off serves everything
    // unauthenticated, with nothing in the log to say so.
    use iggy_gateway_kafka::KafkaGateway;
    use iggy_gateway_kafka::server::bind_listener;

    let listener = bind_listener("127.0.0.1:0").expect("bind ephemeral port");
    let (_tx, rx) = tokio::sync::broadcast::channel(1);
    let authenticator = Arc::new(FixedCredentialAuthenticator {
        username: "alice",
        password: "s3cret",
        permissions: PrincipalPermissions::default(),
    });
    let config = GatewayConfig {
        sasl_enabled: false,
        ..sasl_config()
    };
    let result = KafkaGateway::new(config)
        .with_authenticator(authenticator)
        .run(listener, rx)
        .await;
    assert!(
        result.is_err(),
        "an attached verifier with the flag off must not silently serve unauthenticated traffic"
    );
}

/// Blocks until released, so a test can pin an authentication slot.
#[derive(Debug)]
struct StallingAuthenticator {
    release: tokio::sync::Semaphore,
}

#[async_trait]
impl SaslAuthenticator for StallingAuthenticator {
    async fn authenticate(
        &self,
        credentials: &PlainCredentials,
    ) -> Result<AuthenticatedPrincipal, AuthError> {
        let _held = self.release.acquire().await;
        Ok(AuthenticatedPrincipal {
            username: credentials.username.clone(),
            permissions: PrincipalPermissions::default(),
            permissions_known: true,
        })
    }
}

#[tokio::test]
async fn given_all_authentication_slots_are_busy_when_waiting_too_long_should_close_not_reject() {
    // The permit wait shares the pre-authentication budget. Without that bound a connection sits
    // in the queue holding a `max_connections` permit for as long as the backlog takes to drain,
    // which is the invariant `pre_auth_timeout` is documented to enforce. It must close rather
    // than answer 58, which a Kafka client treats as fatal even though nothing was rejected.
    let config = GatewayConfig {
        max_concurrent_authentications: 1,
        pre_auth_timeout: Duration::from_millis(400),
        ..sasl_config()
    };
    // Zero permits available, so the single slot is occupied for the whole test.
    let authenticator = Arc::new(StallingAuthenticator {
        release: tokio::sync::Semaphore::new(0),
    });
    let (addr, shutdown) = spawn_test_server_with_authenticator(config, authenticator).await;
    std::mem::forget(shutdown);

    // First connection takes the slot and never finishes.
    let mut holder = TcpStream::connect(addr).await.expect("connect");
    handshake_ok(&mut holder).await;
    let token = plain_token("alice", "s3cret");
    let frame = build_request_frame(
        API_KEY_SASL_AUTHENTICATE,
        AUTHENTICATE_VERSION,
        2,
        Some("sasl-test"),
        &authenticate_body(&token),
    );
    holder.write_all(&frame).await.expect("write request");

    // Second connection can never get a slot and must be let go, not left queued.
    let mut queued = TcpStream::connect(addr).await.expect("connect");
    handshake_ok(&mut queued).await;
    queued.write_all(&frame).await.expect("write request");
    assert_closed(&mut queued).await;
}

/// Records the most verifications in flight at once, so a test can pin what bounds them.
#[derive(Debug, Default)]
struct ConcurrencyRecordingAuthenticator {
    in_flight: AtomicUsize,
    peak: AtomicUsize,
}

#[async_trait]
impl SaslAuthenticator for ConcurrencyRecordingAuthenticator {
    async fn authenticate(
        &self,
        credentials: &PlainCredentials,
    ) -> Result<AuthenticatedPrincipal, AuthError> {
        let in_flight = self.in_flight.fetch_add(1, Ordering::SeqCst) + 1;
        self.peak.fetch_max(in_flight, Ordering::SeqCst);
        // Long enough that unbounded verifications would visibly overlap, short enough that a
        // bounded run still finishes well inside the pre-authentication budget.
        tokio::time::sleep(Duration::from_millis(50)).await;
        self.in_flight.fetch_sub(1, Ordering::SeqCst);
        Ok(AuthenticatedPrincipal {
            username: credentials.username.clone(),
            permissions: PrincipalPermissions::default(),
            permissions_known: true,
        })
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn given_a_single_authentication_slot_should_verify_one_credential_at_a_time() {
    // Iggy hashes with Argon2 inline on its shard threads, which have no blocking pool, so
    // unbounded concurrent verification is a denial-of-service vector rather than a throughput
    // question. What pins that here is an authenticator that *completes*: the sibling test above
    // stalls forever, so its client times out whether or not this semaphore holds, and it passes
    // just as happily with the permit dropped the moment it is taken.
    const CLIENTS: i32 = 6;
    let config = GatewayConfig {
        max_concurrent_authentications: 1,
        pre_auth_timeout: Duration::from_secs(10),
        ..sasl_config()
    };
    let authenticator = Arc::new(ConcurrencyRecordingAuthenticator::default());
    let (addr, shutdown) =
        spawn_test_server_with_authenticator(config, authenticator.clone()).await;
    std::mem::forget(shutdown);

    let clients: Vec<_> = (0..CLIENTS)
        .map(|client| {
            tokio::spawn(async move {
                let mut stream = TcpStream::connect(addr).await.expect("connect");
                handshake_ok(&mut stream).await;
                let token = plain_token("alice", "s3cret");
                let body = send(
                    &mut stream,
                    API_KEY_SASL_AUTHENTICATE,
                    AUTHENTICATE_VERSION,
                    client + 2,
                    &authenticate_body(&token),
                )
                .await;
                assert_eq!(
                    error_code(&body),
                    ERROR_NONE,
                    "a queued credential must still be verified, not rejected"
                );
            })
        })
        .collect();
    for client in clients {
        client.await.expect("client task");
    }

    assert_eq!(
        authenticator.peak.load(Ordering::SeqCst),
        1,
        "one slot must admit one verification at a time; anything higher means the limit bounds \
         nothing and a burst of connections reaches Iggy's shard threads unthrottled"
    );
}

#[tokio::test]
async fn given_a_pre_auth_request_above_the_firewall_should_close_rather_than_answer_it() {
    // `kafka_protocol`'s encoders reach further than this gateway's firewall, so encoding at the
    // requested version would hand an unauthenticated client a well-formed body for a version the
    // dispatch path refuses. The least-trusted path must not be the permissive one.
    let addr = spawn_sasl_gateway().await;
    let mut stream = TcpStream::connect(addr).await.expect("connect");

    // Fetch v13: inside the crate's encoder range, outside this gateway's.
    let frame = build_request_frame(1, 13, 1, Some("sasl-test"), &[]);
    stream.write_all(&frame).await.expect("write request");
    assert_closed(&mut stream).await;
}

#[tokio::test]
async fn given_a_refused_first_api_versions_should_not_spend_the_single_allowance() {
    // A client refused at one version is entitled to retry lower, which is what the KIP-511
    // downgrade does. Spending the allowance on the refusal strands it on the retry.
    let addr = spawn_sasl_gateway().await;
    let mut stream = TcpStream::connect(addr).await.expect("connect");

    // v99 is out of range, so this is answered with an error rather than a usable version list.
    let refused = send(&mut stream, API_KEY_API_VERSIONS, 99, 1, &[]).await;
    assert_ne!(
        error_code(&refused),
        ERROR_NONE,
        "an out-of-range ApiVersions must not be reported as a success"
    );

    // The retry at a supported version must still be allowed through.
    let retried = send(&mut stream, API_KEY_API_VERSIONS, 1, 2, &[]).await;
    assert_eq!(
        error_code(&retried),
        ERROR_NONE,
        "the downgrade retry must not be refused for having spent the allowance"
    );
}

const API_KEY_DESCRIBE_ACLS: i16 = 29;
const DESCRIBE_ACLS_VERSION: i16 = 1;

/// `DescribeAcls` v1 filter body: an all-matching filter (`ANY` everywhere, no names).
fn any_acl_filter_body() -> Bytes {
    let mut buf = BytesMut::new();
    buf.put_i8(1); // resource_type_filter = ANY
    buf.put_i16(-1); // resource_name_filter = null
    buf.put_i8(1); // pattern_type_filter = ANY
    buf.put_i16(-1); // principal_filter = null
    buf.put_i16(-1); // host_filter = null
    buf.put_i8(1); // operation = ANY
    buf.put_i8(1); // permission_type = ANY
    buf.freeze()
}

/// `DescribeAcls` v3 filter body: flexible framing, so compact nullable strings and a trailing
/// tagged-fields byte. This is what a real `AdminClient` negotiates against the advertised range,
/// and it exercises request header v2 and response header v1 as well.
fn any_acl_filter_body_v3() -> Bytes {
    let mut buf = BytesMut::new();
    buf.put_i8(1); // resource_type_filter = ANY
    buf.put_u8(0); // resource_name_filter = null (compact: varint 0)
    buf.put_i8(1); // pattern_type_filter = ANY
    buf.put_u8(0); // principal_filter = null
    buf.put_u8(0); // host_filter = null
    buf.put_i8(1); // operation = ANY
    buf.put_i8(1); // permission_type = ANY
    buf.put_u8(0); // empty tagged fields
    buf.freeze()
}

async fn spawn_gateway_for(permissions: PrincipalPermissions) -> SocketAddr {
    let authenticator = Arc::new(FixedCredentialAuthenticator {
        username: "alice",
        password: "s3cret",
        permissions,
    });
    let (addr, shutdown) = spawn_test_server_with_authenticator(sasl_config(), authenticator).await;
    std::mem::forget(shutdown);
    addr
}

async fn authenticate(stream: &mut TcpStream) {
    handshake_ok(stream).await;
    let token = plain_token("alice", "s3cret");
    let body = send(
        stream,
        API_KEY_SASL_AUTHENTICATE,
        AUTHENTICATE_VERSION,
        2,
        &authenticate_body(&token),
    )
    .await;
    assert_eq!(error_code(&body), ERROR_NONE);
}

#[tokio::test]
async fn given_a_principal_with_permissions_when_describing_acls_should_report_them() {
    let addr = spawn_gateway_for(PrincipalPermissions {
        poll_messages: true,
        send_messages: true,
        read_topics: true,
        ..PrincipalPermissions::default()
    })
    .await;
    let mut stream = TcpStream::connect(addr).await.expect("connect");
    authenticate(&mut stream).await;

    let body = send(
        &mut stream,
        API_KEY_DESCRIBE_ACLS,
        DESCRIBE_ACLS_VERSION,
        3,
        &any_acl_filter_body(),
    )
    .await;
    assert_eq!(
        acl_error_code(&body),
        ERROR_NONE,
        "an ACL view is not an error"
    );
    // Decoded, not byte-scanned: the resource type, name and operation of every binding are the
    // thing under test, and a substring scan asserts none of them.
    let bindings = parse_acl_bindings(&body);
    assert!(
        bindings.contains(&(2, "*".to_string(), 3, 3, 3)),
        "poll_messages must render TOPIC * READ, got {bindings:?}"
    );
    // Iggy has no deny rules and nothing here is prefix-scoped, so every binding must say ALLOW on
    // a LITERAL pattern. Without asserting these two, a response meaning the opposite would pass.
    assert!(
        bindings
            .iter()
            .all(|(_, _, pattern, _, permission)| *pattern == 3 && *permission == 3),
        "every binding must be an ALLOW on a LITERAL pattern, got {bindings:?}"
    );
    assert!(
        bindings.contains(&(2, "*".to_string(), 3, 4, 3)),
        "send_messages must render TOPIC * WRITE, got {bindings:?}"
    );
    assert!(
        bindings.contains(&(3, "*".to_string(), 3, 3, 3)),
        "a principal that may read topics must get the derived GROUP * READ, got {bindings:?}"
    );
    assert!(
        body.windows(10).any(|w| w == b"User:alice"),
        "every binding names the authenticated principal"
    );
}

#[tokio::test]
async fn given_only_poll_messages_should_not_render_a_group_binding() {
    // Regression. Consumer-group operations route through Iggy's topic rule, which never consults
    // `poll_messages`, so deriving the group binding from polling advertised access Iggy denies.
    let addr = spawn_gateway_for(PrincipalPermissions {
        poll_messages: true,
        ..PrincipalPermissions::default()
    })
    .await;
    let mut stream = TcpStream::connect(addr).await.expect("connect");
    authenticate(&mut stream).await;

    let body = send(
        &mut stream,
        API_KEY_DESCRIBE_ACLS,
        DESCRIBE_ACLS_VERSION,
        3,
        &any_acl_filter_body(),
    )
    .await;
    let bindings = parse_acl_bindings(&body);
    assert!(
        bindings.contains(&(2, "*".to_string(), 3, 3, 3)),
        "polling still grants TOPIC READ, got {bindings:?}"
    );
    assert!(
        !bindings.iter().any(|(resource, ..)| *resource == 3),
        "no group binding without a topic read grant, got {bindings:?}"
    );
}

#[tokio::test]
async fn given_manage_servers_should_not_claim_an_unusable_cluster_alter() {
    // A server flag renders DESCRIBE and nothing else. `manage_servers` cannot be set here on
    // purpose: Iggy reads it in exactly one rule, as an alias for `read_servers`, so the
    // projection folds the two together and this layer never sees it apart. Rendering CLUSTER
    // ALTER from it advertised an ability with nowhere to be used, and the two write ACL APIs it
    // implies are not even advertised.
    let addr = spawn_gateway_for(PrincipalPermissions {
        read_servers: true,
        ..PrincipalPermissions::default()
    })
    .await;
    let mut stream = TcpStream::connect(addr).await.expect("connect");
    authenticate(&mut stream).await;

    let body = send(
        &mut stream,
        API_KEY_DESCRIBE_ACLS,
        DESCRIBE_ACLS_VERSION,
        3,
        &any_acl_filter_body(),
    )
    .await;
    let bindings = parse_acl_bindings(&body);
    assert!(
        bindings.contains(&(4, "kafka-cluster".to_string(), 3, 8, 3)),
        "server permissions must still render CLUSTER DESCRIBE, got {bindings:?}"
    );
    assert!(
        !bindings.iter().any(|(.., operation, _)| *operation == 7),
        "nothing may claim ALTER, got {bindings:?}"
    );
}

#[tokio::test]
async fn given_a_principal_with_no_permissions_should_report_an_empty_view_not_an_error() {
    // Kafka draws a firm line between "nothing matched" and "the request failed", and an admin
    // tool prints them very differently.
    let addr = spawn_gateway_for(PrincipalPermissions::default()).await;
    let mut stream = TcpStream::connect(addr).await.expect("connect");
    authenticate(&mut stream).await;

    let body = send(
        &mut stream,
        API_KEY_DESCRIBE_ACLS,
        DESCRIBE_ACLS_VERSION,
        3,
        &any_acl_filter_body(),
    )
    .await;
    assert_eq!(
        acl_error_code(&body),
        ERROR_NONE,
        "an empty ACL set is a successful answer"
    );
    assert!(
        parse_acl_bindings(&body).is_empty(),
        "no permissions means no bindings at all"
    );
}

/// Accepts `alice` but reports that her permissions could not be read, the shape
/// `IggyAuthenticator` produces when the login succeeds and the follow-up `get_user` does not.
#[derive(Debug)]
struct UnreadPermissionsAuthenticator;

#[async_trait]
impl SaslAuthenticator for UnreadPermissionsAuthenticator {
    async fn authenticate(
        &self,
        credentials: &PlainCredentials,
    ) -> Result<AuthenticatedPrincipal, AuthError> {
        Ok(AuthenticatedPrincipal {
            username: credentials.username.clone(),
            permissions: PrincipalPermissions::default(),
            permissions_known: false,
        })
    }
}

#[tokio::test]
async fn given_unread_permissions_when_describing_acls_should_answer_an_error_and_stay_open() {
    // The fallback permissions are empty, so answering from them would render the same zero
    // bindings as a principal that genuinely holds nothing. Every other stub reports its
    // permissions as known, so without this case the two answers could swap unnoticed.
    let (addr, shutdown) = spawn_test_server_with_authenticator(
        sasl_config(),
        Arc::new(UnreadPermissionsAuthenticator),
    )
    .await;
    std::mem::forget(shutdown);
    let mut stream = TcpStream::connect(addr).await.expect("connect");
    authenticate(&mut stream).await;

    let body = send(
        &mut stream,
        API_KEY_DESCRIBE_ACLS,
        DESCRIBE_ACLS_VERSION,
        3,
        &any_acl_filter_body(),
    )
    .await;
    assert_eq!(
        acl_error_code(&body),
        ERROR_UNKNOWN_SERVER_ERROR,
        "a view that was never read must not be reported as an empty one"
    );
    assert!(
        parse_acl_bindings(&body).is_empty(),
        "an error answer carries no bindings"
    );

    // Kept open: the login was valid, and only the ACL view is missing.
    let metadata = send(&mut stream, API_KEY_METADATA, 0, 4, &[0, 0, 0, 0]).await;
    assert!(
        !metadata.is_empty(),
        "the connection must keep serving after the ACL error"
    );
}

#[tokio::test]
async fn given_an_unauthenticated_connection_when_describing_acls_should_be_answered_then_closed() {
    let addr = spawn_gateway_for(PrincipalPermissions::default()).await;
    let mut stream = TcpStream::connect(addr).await.expect("connect");

    // Answered with a parseable body rather than dropped silently. The key is outside the firewall
    // table, so the generic error path closes without one, which leaves the client guessing on the
    // one path an unauthenticated peer can actually reach.
    let body = send(
        &mut stream,
        API_KEY_DESCRIBE_ACLS,
        DESCRIBE_ACLS_VERSION,
        1,
        &any_acl_filter_body(),
    )
    .await;
    assert_eq!(acl_error_code(&body), ERROR_ILLEGAL_SASL_STATE);
    assert!(
        parse_acl_bindings(&body).is_empty(),
        "a refusal must disclose no bindings"
    );
    assert_closed(&mut stream).await;
}

#[tokio::test]
async fn given_sasl_enabled_should_advertise_describe_acls() {
    let addr = spawn_gateway_for(PrincipalPermissions::default()).await;
    let mut stream = TcpStream::connect(addr).await.expect("connect");
    let body = send(&mut stream, API_KEY_API_VERSIONS, 1, 1, &[]).await;
    let rows = parse_api_versions(&body);
    let acls = rows
        .iter()
        .find(|(key, _, _)| *key == API_KEY_DESCRIBE_ACLS)
        .expect("DescribeAcls must be advertised while SASL is enabled");
    assert_eq!((acls.1, acls.2), (1, 3));
}

#[tokio::test]
async fn given_sasl_disabled_should_not_advertise_describe_acls() {
    // With no principal there is nothing it could truthfully describe.
    let (addr, shutdown) = server::spawn_test_server().await;
    std::mem::forget(shutdown);
    let mut stream = TcpStream::connect(addr).await.expect("connect");
    let body = send(&mut stream, API_KEY_API_VERSIONS, 1, 1, &[]).await;
    assert!(
        !parse_api_versions(&body)
            .iter()
            .any(|(key, _, _)| *key == API_KEY_DESCRIBE_ACLS)
    );
}

#[tokio::test]
async fn given_describe_acls_v3_should_answer_over_the_flexible_framing() {
    // v3 is the version a real AdminClient negotiates, and the only flexible one: compact strings,
    // tagged fields, request header v2 and response header v1. v1 exercises none of that.
    let addr = spawn_gateway_for(PrincipalPermissions {
        poll_messages: true,
        read_topics: true,
        ..PrincipalPermissions::default()
    })
    .await;
    let mut stream = TcpStream::connect(addr).await.expect("connect");
    authenticate(&mut stream).await;

    let body = send(
        &mut stream,
        API_KEY_DESCRIBE_ACLS,
        3,
        3,
        &any_acl_filter_body_v3(),
    )
    .await;
    // v3 is flexible, so the decoder above (legacy framing) does not apply; the error code still
    // sits after the throttle field.
    assert_eq!(acl_error_code(&body), ERROR_NONE, "v3 must answer cleanly");
    assert!(
        body.windows(10).any(|w| w == b"User:alice"),
        "the v3 response must carry the rendered bindings, not just a header"
    );

    // Still usable afterwards, which proves the flexible response framing did not desync the
    // connection's correlation stream.
    let again = send(&mut stream, API_KEY_METADATA, 0, 4, &[0, 0, 0, 0]).await;
    assert!(!again.is_empty());
}

#[tokio::test]
async fn given_describe_acls_above_the_advertised_range_should_be_refused() {
    let addr = spawn_gateway_for(PrincipalPermissions::default()).await;
    let mut stream = TcpStream::connect(addr).await.expect("connect");
    authenticate(&mut stream).await;

    // v4 has no schema at either end, so there is no parseable body to answer with.
    let frame = build_request_frame(
        API_KEY_DESCRIBE_ACLS,
        4,
        3,
        Some("sasl-test"),
        &any_acl_filter_body_v3(),
    );
    stream.write_all(&frame).await.expect("write request");
    assert_closed(&mut stream).await;
}

#[tokio::test]
async fn given_a_malformed_acl_filter_should_answer_an_error_and_stay_open() {
    // The handler's two error branches differ in liveness: a malformed filter is answered and the
    // connection kept, while an out-of-range version closes. Only the second was covered, so the
    // reachable one went untested.
    let addr = spawn_gateway_for(PrincipalPermissions::default()).await;
    let mut stream = TcpStream::connect(addr).await.expect("connect");
    authenticate(&mut stream).await;

    // Declares a 64-byte resource name in a body that holds two.
    let mut truncated = BytesMut::new();
    truncated.put_i8(1);
    truncated.put_i16(64);
    truncated.put_slice(b"xy");
    let body = send(
        &mut stream,
        API_KEY_DESCRIBE_ACLS,
        DESCRIBE_ACLS_VERSION,
        3,
        &truncated.freeze(),
    )
    .await;
    assert_eq!(
        acl_error_code(&body),
        42,
        "a malformed filter is INVALID_REQUEST"
    );

    // Still usable: the client may send a well-formed filter next.
    let good = send(
        &mut stream,
        API_KEY_DESCRIBE_ACLS,
        DESCRIBE_ACLS_VERSION,
        4,
        &any_acl_filter_body(),
    )
    .await;
    assert_eq!(acl_error_code(&good), ERROR_NONE);
}
