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
use std::time::Duration;

use async_trait::async_trait;
use bytes::{BufMut, Bytes, BytesMut};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;

use iggy_gateway_kafka::GatewayConfig;
use iggy_gateway_kafka::auth::{AuthError, SaslAuthenticator};
use iggy_gateway_kafka::protocol::sasl::{MAX_PRE_AUTH_API_VERSIONS, PlainCredentials};

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

const HANDSHAKE_VERSION: i16 = 1;
const AUTHENTICATE_VERSION: i16 = 1;

/// Accepts one specific credential pair and rejects everything else.
#[derive(Debug)]
struct FixedCredentialAuthenticator {
    username: &'static str,
    password: &'static str,
}

#[async_trait]
impl SaslAuthenticator for FixedCredentialAuthenticator {
    async fn authenticate(&self, credentials: &PlainCredentials) -> Result<(), AuthError> {
        use secrecy::ExposeSecret;
        let matches = credentials.username == self.username
            && credentials.password.expose_secret() == self.password;
        if matches {
            Ok(())
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
async fn given_pipelined_bytes_behind_a_rejected_token_should_close_with_a_fin_not_a_reset() {
    // Unread input at close makes Linux send RST instead of FIN, and an RST lets the client's
    // stack discard the 58 before the client reads it. Loopback delivers the 58 ahead of the RST
    // either way, so what this pins is the close itself: a clean EOF, not a reset.
    let addr = spawn_sasl_gateway().await;
    let mut stream = TcpStream::connect(addr).await.expect("connect");
    handshake_ok(&mut stream).await;

    let rejected = build_request_frame(
        API_KEY_SASL_AUTHENTICATE,
        AUTHENTICATE_VERSION,
        2,
        Some("sasl-test"),
        &authenticate_body(&plain_token("alice", "wrong-password")),
    );
    let pipelined = build_request_frame(API_KEY_METADATA, 0, 3, Some("sasl-test"), &[0, 0, 0, 0]);
    let mut both = BytesMut::from(&rejected[..]);
    both.extend_from_slice(&pipelined);
    stream.write_all(&both).await.expect("write requests");
    // Let the server answer and close before the client reads anything.
    tokio::time::sleep(Duration::from_millis(200)).await;

    let payload = tcp::read_response_frame(&mut stream, 8 * 1024 * 1024).await;
    let (echoed, body) =
        parse_response_payload(API_KEY_SASL_AUTHENTICATE, AUTHENTICATE_VERSION, payload);
    assert_eq!(echoed, 2);
    assert_eq!(error_code(&body), ERROR_SASL_AUTHENTICATION_FAILED);
    let mut rest = [0u8; 1];
    let read = tokio::time::timeout(Duration::from_secs(5), stream.read(&mut rest))
        .await
        .expect("the server must close the connection");
    assert!(
        matches!(read, Ok(0)),
        "the close must be a FIN the client reads as EOF, not a reset: {read:?}"
    );
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
async fn given_a_handshake_above_the_version_ceiling_after_authenticating_should_close() {
    // The keep-open promise stops where the schemas do. SaslHandshake has no v2, so there is no
    // body this client could parse and one shaped for v1 would be misparsed; the version is
    // checked before the encoder rather than left to fail inside it.
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

    let frame = build_request_frame(
        API_KEY_SASL_HANDSHAKE,
        HANDSHAKE_VERSION + 1,
        3,
        Some("sasl-test"),
        &handshake_body("PLAIN"),
    );
    stream.write_all(&frame).await.expect("write request");
    assert_closed(&mut stream).await;
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
    async fn authenticate(&self, _credentials: &PlainCredentials) -> Result<(), AuthError> {
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
async fn given_pre_auth_api_versions_past_the_allowance_should_be_refused_and_close() {
    // Without this the pre-auth read budget resets on every frame, so a client that never
    // authenticates holds a connection permit indefinitely by sending ApiVersions on a timer.
    let addr = spawn_sasl_gateway().await;
    let mut stream = TcpStream::connect(addr).await.expect("connect");

    for correlation_id in 1..=i32::from(MAX_PRE_AUTH_API_VERSIONS) {
        let answered = send(&mut stream, API_KEY_API_VERSIONS, 1, correlation_id, &[]).await;
        assert_eq!(
            error_code(&answered),
            ERROR_NONE,
            "ApiVersions {correlation_id} is inside the allowance"
        );
    }

    let refused = send(&mut stream, API_KEY_API_VERSIONS, 1, 99, &[]).await;
    assert_eq!(
        error_code(&refused),
        ERROR_ILLEGAL_SASL_STATE,
        "a real broker allows one ApiVersions before the handshake, and the allowance adds only \
         the KIP-511 downgrade retry"
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
    async fn authenticate(&self, _credentials: &PlainCredentials) -> Result<(), AuthError> {
        let _held = self.release.acquire().await;
        Ok(())
    }
}

#[tokio::test]
async fn given_all_authentication_slots_are_busy_when_waiting_too_long_should_close_not_reject() {
    // The permit wait is bounded by the pre-authentication budget. Without that a connection sits
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

#[tokio::test]
async fn given_a_verification_in_flight_when_shutting_down_should_close_without_waiting_it_out() {
    // The connection loop only watches the shutdown token between frames. A verification that
    // did not watch it too would hold the drain for the whole pre-authentication budget.
    let config = GatewayConfig {
        pre_auth_timeout: Duration::from_secs(30),
        shutdown_drain_timeout: Duration::from_secs(30),
        ..sasl_config()
    };
    let authenticator = Arc::new(StallingAuthenticator {
        release: tokio::sync::Semaphore::new(0),
    });
    let (addr, shutdown) = spawn_test_server_with_authenticator(config, authenticator).await;

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
    tokio::time::sleep(Duration::from_millis(100)).await;

    shutdown.send(()).expect("signal shutdown");
    assert_eq!(
        read_byte_with_timeout(&mut stream, Duration::from_secs(2)).await,
        ByteRead::Closed,
        "shutdown must end a verification in flight"
    );
}

#[tokio::test]
async fn given_a_rejected_login_when_the_peer_retries_at_once_should_close_until_the_delay_passes()
{
    let addr = spawn_sasl_gateway().await;

    let mut rejected = TcpStream::connect(addr).await.expect("connect");
    handshake_ok(&mut rejected).await;
    let body = send(
        &mut rejected,
        API_KEY_SASL_AUTHENTICATE,
        AUTHENTICATE_VERSION,
        2,
        &authenticate_body(&plain_token("alice", "wrong-password")),
    )
    .await;
    assert_eq!(error_code(&body), ERROR_SASL_AUTHENTICATION_FAILED);
    assert_closed(&mut rejected).await;

    // Even the right password is not checked while the peer is throttled, and it gets a close
    // rather than 58, which a Kafka client would treat as fatal.
    let mut throttled = TcpStream::connect(addr).await.expect("connect");
    handshake_ok(&mut throttled).await;
    let frame = build_request_frame(
        API_KEY_SASL_AUTHENTICATE,
        AUTHENTICATE_VERSION,
        2,
        Some("sasl-test"),
        &authenticate_body(&plain_token("alice", "s3cret")),
    );
    throttled.write_all(&frame).await.expect("write request");
    assert_closed(&mut throttled).await;

    tokio::time::sleep(Duration::from_millis(600)).await;
    let mut retried = TcpStream::connect(addr).await.expect("connect");
    handshake_ok(&mut retried).await;
    let body = send(
        &mut retried,
        API_KEY_SASL_AUTHENTICATE,
        AUTHENTICATE_VERSION,
        2,
        &authenticate_body(&plain_token("alice", "s3cret")),
    )
    .await;
    assert_eq!(
        error_code(&body),
        ERROR_NONE,
        "the delay must run out on its own"
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
async fn given_a_refused_api_versions_when_retried_lower_should_still_be_answered() {
    // A client refused at one version is entitled to retry lower, which is what the KIP-511
    // downgrade does. An allowance that did not cover the retry would strand it.
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

#[tokio::test]
async fn given_a_repeated_refused_api_versions_should_run_the_allowance_out() {
    // The half a success-only allowance misses: a version this gateway refuses is one the peer
    // can repeat, and every frame resets the pre-authentication read budget, so refusals that
    // cost nothing hold a `max_connections` permit for as long as the client keeps typing.
    let addr = spawn_sasl_gateway().await;
    let mut stream = TcpStream::connect(addr).await.expect("connect");

    for correlation_id in 1..=i32::from(MAX_PRE_AUTH_API_VERSIONS) {
        let refused = send(&mut stream, API_KEY_API_VERSIONS, 99, correlation_id, &[]).await;
        assert_ne!(
            error_code(&refused),
            ERROR_NONE,
            "v99 is out of range at every attempt"
        );
    }

    // Past the allowance there is nothing left to answer with: ILLEGAL_SASL_STATE has no shape at
    // a version outside the firewall, so this closes without a body.
    let frame = build_request_frame(API_KEY_API_VERSIONS, 99, 99, Some("sasl-test"), &[]);
    stream.write_all(&frame).await.expect("write request");
    assert_closed(&mut stream).await;
}
