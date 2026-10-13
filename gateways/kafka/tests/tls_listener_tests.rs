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

//! The TLS listeners over a real socket: `SSL` and `SASL_SSL` beside `PLAINTEXT`, each
//! advertising its own address, client certificates as Iggy identities, and certificate reload
//! without a restart.
//!
//! A stub [`PrincipalResolver`] stands in for the bridge, so no Iggy server is needed; the
//! certificate path against a real one is in `kafka_client_e2e_tests.rs`.

use std::collections::HashMap;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use async_trait::async_trait;
use bytes::Bytes;
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt};
use tokio::net::{TcpSocket, TcpStream};
use tokio::sync::SemaphorePermit;

use iggy_gateway_kafka::auth::{
    AuthError, AuthenticatedPrincipal, PrincipalResolver, SaslAuthenticator,
};
use iggy_gateway_kafka::listener::SecurityProtocol;
use iggy_gateway_kafka::protocol::acl::PrincipalPermissions;
use iggy_gateway_kafka::protocol::sasl::PlainCredentials;
use iggy_gateway_kafka::server::{BoundListener, bind_listener};
use iggy_gateway_kafka::tls::{ClientAuth, TlsConfig};
use iggy_gateway_kafka::{GatewayConfig, KafkaGateway};

#[path = "common/certs.rs"]
mod certs;
#[path = "common/codec.rs"]
mod codec;
#[path = "common/server.rs"]
mod server;
#[path = "common/tcp.rs"]
mod tcp;

use certs::{Issued, TestPki, connect_tls, connect_tls_over};
use codec::{Decoder, Encoder};
use server::{listener, spawn_gateway};
use tcp::{
    ByteRead, build_metadata_legacy_request, build_request_frame, parse_response_payload,
    read_byte_with_timeout, read_response_frame,
};

const API_KEY_METADATA: i16 = 3;
const API_KEY_SASL_HANDSHAKE: i16 = 17;
const API_KEY_API_VERSIONS: i16 = 18;
const API_KEY_DESCRIBE_ACLS: i16 = 29;
const API_KEY_SASL_AUTHENTICATE: i16 = 36;

const ERROR_NONE: i16 = 0;
const MAX_FRAME: usize = 8 * 1024 * 1024;

const USERNAME: &str = "alice";
const PASSWORD: &str = "s3cret";

/// Every certificate a test needs, written where the gateway reads them.
struct Pki {
    authority: TestPki,
    dir: tempfile::TempDir,
    cert_file: std::path::PathBuf,
    key_file: std::path::PathBuf,
    ca_file: std::path::PathBuf,
}

impl Pki {
    fn new() -> Self {
        let pki = TestPki::new();
        let dir = tempfile::tempdir().expect("tempdir");
        let (cert_file, key_file) = pki.server_cert("gateway").write(dir.path(), "server");
        let ca_file = pki.write_ca(dir.path());
        Self {
            authority: pki,
            dir,
            cert_file,
            key_file,
            ca_file,
        }
    }

    fn tls(&self) -> TlsConfig {
        TlsConfig::new(&self.cert_file, &self.key_file)
    }

    fn tls_with_client_auth(&self, mode: ClientAuth) -> TlsConfig {
        self.tls().with_client_auth(mode, &self.ca_file)
    }
}

/// Knows a fixed set of users, and remembers every name it was asked about.
#[derive(Debug, Default)]
struct KnownUsers {
    users: HashMap<String, PrincipalPermissions>,
    asked: Mutex<Vec<String>>,
}

impl KnownUsers {
    fn with(username: &str, permissions: PrincipalPermissions) -> Arc<Self> {
        let mut users = HashMap::new();
        users.insert(username.to_string(), permissions);
        Arc::new(Self {
            users,
            asked: Mutex::default(),
        })
    }

    fn asked(&self) -> Vec<String> {
        self.asked.lock().expect("not poisoned").clone()
    }
}

#[async_trait]
impl PrincipalResolver for KnownUsers {
    async fn resolve(&self, username: &str) -> Result<AuthenticatedPrincipal, AuthError> {
        self.asked
            .lock()
            .expect("not poisoned")
            .push(username.to_string());
        self.users
            .get(username)
            .map(|permissions| AuthenticatedPrincipal {
                username: username.to_string(),
                permissions: *permissions,
                permissions_known: true,
            })
            .ok_or(AuthError::Rejected)
    }
}

/// Answers every lookup the same way, or never answers.
#[derive(Debug)]
enum BrokenResolver {
    Unavailable,
    Hangs,
}

#[async_trait]
impl PrincipalResolver for BrokenResolver {
    async fn resolve(&self, _username: &str) -> Result<AuthenticatedPrincipal, AuthError> {
        match self {
            Self::Unavailable => Err(AuthError::Unavailable),
            Self::Hangs => std::future::pending().await,
        }
    }
}

/// Knows every user, after a fixed delay, and counts the lookups it started.
#[derive(Debug)]
struct SlowUsers {
    delay: Duration,
    started: AtomicUsize,
}

#[async_trait]
impl PrincipalResolver for SlowUsers {
    async fn resolve(&self, username: &str) -> Result<AuthenticatedPrincipal, AuthError> {
        self.started.fetch_add(1, Ordering::SeqCst);
        tokio::time::sleep(self.delay).await;
        Ok(AuthenticatedPrincipal {
            username: username.to_string(),
            permissions: topic_reader(),
            permissions_known: true,
        })
    }
}

/// Accepts `USERNAME`/`PASSWORD` and nothing else.
#[derive(Debug)]
struct FixedCredentials;

#[async_trait]
impl SaslAuthenticator for FixedCredentials {
    async fn authenticate(
        &self,
        credentials: &PlainCredentials,
        _slot: SemaphorePermit<'_>,
    ) -> Result<AuthenticatedPrincipal, AuthError> {
        use secrecy::ExposeSecret;
        if credentials.username == USERNAME && credentials.password.expose_secret() == PASSWORD {
            Ok(AuthenticatedPrincipal {
                username: credentials.username.clone(),
                permissions: topic_reader(),
                permissions_known: true,
            })
        } else {
            Err(AuthError::Rejected)
        }
    }
}

fn topic_reader() -> PrincipalPermissions {
    PrincipalPermissions {
        read_topics: true,
        poll_messages: true,
        ..PrincipalPermissions::default()
    }
}

fn quick_config(listeners: Vec<SecurityProtocol>, tls: Option<TlsConfig>) -> GatewayConfig {
    GatewayConfig {
        listeners: listeners.into_iter().map(listener).collect(),
        tls,
        idle_timeout: Duration::from_secs(5),
        read_timeout: Duration::from_secs(5),
        write_timeout: Duration::from_secs(5),
        pre_auth_timeout: Duration::from_secs(5),
        shutdown_drain_timeout: Duration::from_secs(5),
        ..GatewayConfig::default()
    }
}

/// Sends one request and returns the response body.
async fn send<S: AsyncRead + AsyncWrite + Unpin>(
    stream: &mut S,
    api_key: i16,
    api_version: i16,
    body: &[u8],
) -> Bytes {
    let frame = build_request_frame(api_key, api_version, 7, Some("tls-test"), body);
    stream.write_all(&frame).await.expect("write request");
    let payload = read_response_frame(stream, MAX_FRAME).await;
    let (_, body) = parse_response_payload(api_key, api_version, payload);
    body
}

/// The advertised `(api_key, min, max)` rows of an `ApiVersions` v1 answer.
async fn api_versions<S: AsyncRead + AsyncWrite + Unpin>(stream: &mut S) -> Vec<(i16, i16, i16)> {
    let body = send(stream, API_KEY_API_VERSIONS, 1, &[]).await;
    let mut decoder = Decoder::new(body);
    assert_eq!(decoder.read_i16().expect("error code"), ERROR_NONE);
    let count = decoder.read_i32().expect("count");
    (0..count)
        .map(|_| {
            (
                decoder.read_i16().expect("api key"),
                decoder.read_i16().expect("min"),
                decoder.read_i16().expect("max"),
            )
        })
        .collect()
}

fn advertises(rows: &[(i16, i16, i16)], api_key: i16) -> bool {
    rows.iter().any(|row| row.0 == api_key)
}

/// The broker `(host, port)` a Metadata v0 answer advertises.
async fn advertised_broker<S: AsyncRead + AsyncWrite + Unpin>(stream: &mut S) -> (String, i32) {
    let request = build_metadata_legacy_request(&[]);
    let body = send(stream, API_KEY_METADATA, 0, &request).await;
    let mut decoder = Decoder::new(body);
    assert_eq!(decoder.read_i32().expect("broker count"), 1);
    decoder.read_i32().expect("node id");
    let host = decoder
        .read_nullable_string()
        .expect("host")
        .expect("non-null host");
    let port = decoder.read_i32().expect("port");
    (host, port)
}

/// Runs the SASL/PLAIN exchange as `username`, returning the `SaslAuthenticate` error code.
async fn sasl_plain<S: AsyncRead + AsyncWrite + Unpin>(
    stream: &mut S,
    username: &str,
    password: &str,
) -> i16 {
    let mut handshake = Encoder::with_capacity(16);
    handshake
        .write_nullable_string(Some("PLAIN"))
        .expect("mechanism");
    let body = send(stream, API_KEY_SASL_HANDSHAKE, 1, &handshake.freeze()).await;
    assert_eq!(
        i16::from_be_bytes([body[0], body[1]]),
        ERROR_NONE,
        "handshake must accept PLAIN"
    );

    let mut token = vec![0u8];
    token.extend_from_slice(username.as_bytes());
    token.push(0);
    token.extend_from_slice(password.as_bytes());
    let mut authenticate = Encoder::with_capacity(64);
    authenticate
        .write_nullable_bytes(Some(&token))
        .expect("token");
    let body = send(stream, API_KEY_SASL_AUTHENTICATE, 1, &authenticate.freeze()).await;
    i16::from_be_bytes([body[0], body[1]])
}

/// `DescribeAcls` v1 with a match-everything filter: the error code and the raw body.
async fn describe_acls<S: AsyncRead + AsyncWrite + Unpin>(stream: &mut S) -> (i16, Bytes) {
    let mut filter = Encoder::with_capacity(16);
    filter.write_i8(1); // resource type ANY
    filter.write_nullable_string(None).expect("name");
    filter.write_i8(1); // pattern type ANY
    filter.write_nullable_string(None).expect("principal");
    filter.write_nullable_string(None).expect("host");
    filter.write_i8(1); // operation ANY
    filter.write_i8(1); // permission ANY
    let body = send(stream, API_KEY_DESCRIBE_ACLS, 1, &filter.freeze()).await;
    let mut decoder = Decoder::new(body.clone());
    decoder.read_i32().expect("throttle");
    (decoder.read_i16().expect("error code"), body)
}

fn mentions(body: &Bytes, text: &str) -> bool {
    body.windows(text.len())
        .any(|window| window == text.as_bytes())
}

#[tokio::test]
async fn given_an_ssl_listener_when_a_tls_client_connects_should_be_served_without_sasl_keys() {
    let pki = Pki::new();
    let (addrs, _shutdown) = spawn_gateway(
        KafkaGateway::new,
        quick_config(vec![SecurityProtocol::Ssl], Some(pki.tls())),
    );

    let mut stream = connect_tls(addrs[0], pki.authority.client_config(None))
        .await
        .expect("TLS handshake against the gateway's certificate");
    let rows = api_versions(&mut stream).await;
    assert!(advertises(&rows, API_KEY_METADATA));
    assert!(
        !advertises(&rows, API_KEY_SASL_HANDSHAKE) && !advertises(&rows, API_KEY_SASL_AUTHENTICATE),
        "an SSL listener runs no SASL exchange, got: {rows:?}"
    );
    assert!(
        !advertises(&rows, API_KEY_DESCRIBE_ACLS),
        "without client certificates an SSL listener names no principal, got: {rows:?}"
    );
}

#[tokio::test]
async fn given_several_listeners_when_asking_metadata_should_advertise_the_port_the_client_used() {
    let pki = Pki::new();
    let (addrs, _shutdown) = spawn_gateway(
        |config| KafkaGateway::new(config).with_authenticator(Arc::new(FixedCredentials)),
        quick_config(
            vec![
                SecurityProtocol::Plaintext,
                SecurityProtocol::Ssl,
                SecurityProtocol::SaslSsl,
            ],
            Some(pki.tls()),
        ),
    );
    let [plaintext, ssl, sasl_ssl] = addrs[..] else {
        panic!("three listeners")
    };
    assert!(plaintext != ssl && ssl != sasl_ssl);

    let mut plain = TcpStream::connect(plaintext).await.expect("connect");
    assert_eq!(
        advertised_broker(&mut plain).await,
        ("127.0.0.1".to_string(), i32::from(plaintext.port()))
    );

    let mut tls = connect_tls(ssl, pki.authority.client_config(None))
        .await
        .expect("TLS handshake");
    assert_eq!(
        advertised_broker(&mut tls).await,
        ("127.0.0.1".to_string(), i32::from(ssl.port())),
        "a TLS client pointed at the plaintext port would fail its next connection"
    );

    let mut authenticated = connect_tls(sasl_ssl, pki.authority.client_config(None))
        .await
        .expect("TLS handshake");
    assert_eq!(
        sasl_plain(&mut authenticated, USERNAME, PASSWORD).await,
        ERROR_NONE
    );
    assert_eq!(
        advertised_broker(&mut authenticated).await,
        ("127.0.0.1".to_string(), i32::from(sasl_ssl.port()))
    );
}

#[tokio::test]
async fn given_an_ssl_listener_when_a_plaintext_client_connects_should_be_closed() {
    let pki = Pki::new();
    let (addrs, _shutdown) = spawn_gateway(
        KafkaGateway::new,
        quick_config(vec![SecurityProtocol::Ssl], Some(pki.tls())),
    );

    let mut stream = TcpStream::connect(addrs[0]).await.expect("connect");
    let frame = build_request_frame(API_KEY_API_VERSIONS, 1, 1, Some("plaintext"), &[]);
    stream.write_all(&frame).await.expect("write");
    // A Kafka frame is not a ClientHello. rustls answers with a TLS alert record (content type
    // 21) before closing, and either is a refusal; a Kafka frame starts with a length prefix
    // whose first byte is 0 for any frame under 16 MiB.
    let first = read_byte_with_timeout(&mut stream, Duration::from_secs(5)).await;
    assert!(
        matches!(first, ByteRead::Closed | ByteRead::Byte(21)),
        "the gateway must not speak Kafka on the TLS port, got: {first:?}"
    );
}

#[tokio::test]
async fn given_a_sasl_ssl_listener_should_run_the_sasl_exchange_inside_tls() {
    let pki = Pki::new();
    let (addrs, _shutdown) = spawn_gateway(
        |config| KafkaGateway::new(config).with_authenticator(Arc::new(FixedCredentials)),
        quick_config(vec![SecurityProtocol::SaslSsl], Some(pki.tls())),
    );

    let mut stream = connect_tls(addrs[0], pki.authority.client_config(None))
        .await
        .expect("TLS handshake");
    let rows = api_versions(&mut stream).await;
    assert!(advertises(&rows, API_KEY_SASL_HANDSHAKE));
    assert!(advertises(&rows, API_KEY_SASL_AUTHENTICATE));
    assert!(advertises(&rows, API_KEY_DESCRIBE_ACLS));
    assert_eq!(
        sasl_plain(&mut stream, USERNAME, PASSWORD).await,
        ERROR_NONE
    );
    let (code, body) = describe_acls(&mut stream).await;
    assert_eq!(code, ERROR_NONE);
    assert!(
        mentions(&body, &format!("User:{USERNAME}")),
        "the SASL principal is the one described"
    );

    let mut rejected = connect_tls(addrs[0], pki.authority.client_config(None))
        .await
        .expect("TLS handshake");
    assert_ne!(
        sasl_plain(&mut rejected, USERNAME, "wrong").await,
        ERROR_NONE,
        "a wrong password is still refused over TLS"
    );
}

#[tokio::test]
async fn given_required_client_auth_when_the_certificate_names_a_user_should_authenticate_as_it() {
    let pki = Pki::new();
    let users = KnownUsers::with(USERNAME, topic_reader());
    let resolver = Arc::clone(&users);
    let (addrs, _shutdown) = spawn_gateway(
        move |config| KafkaGateway::new(config).with_principal_resolver(resolver),
        quick_config(
            vec![SecurityProtocol::Ssl],
            Some(
                pki.tls_with_client_auth(ClientAuth::Required)
                    .with_principal_mapping("RULE:^CN=([^,]+).*$/$1/L".parse().expect("valid")),
            ),
        ),
    );

    let certificate = pki.authority.client_cert("Alice", "Example");
    let mut stream = connect_tls(addrs[0], pki.authority.client_config(Some(&certificate)))
        .await
        .expect("mTLS handshake");
    let rows = api_versions(&mut stream).await;
    assert!(
        advertises(&rows, API_KEY_DESCRIBE_ACLS),
        "a listener that names principals advertises DescribeAcls, got: {rows:?}"
    );
    assert!(!advertises(&rows, API_KEY_SASL_HANDSHAKE));
    let (code, body) = describe_acls(&mut stream).await;
    assert_eq!(code, ERROR_NONE);
    assert!(
        mentions(&body, &format!("User:{USERNAME}")),
        "the mapped, lowercased common name is the principal described"
    );
    assert_eq!(
        users.asked(),
        vec![USERNAME.to_string()],
        "the mapping rule runs before the lookup, and the lookup happens once"
    );
}

#[tokio::test]
async fn given_required_client_auth_when_the_certificate_names_no_user_should_close() {
    let pki = Pki::new();
    let users = KnownUsers::with(USERNAME, topic_reader());
    let (addrs, _shutdown) = spawn_gateway(
        move |config| KafkaGateway::new(config).with_principal_resolver(users),
        quick_config(
            vec![SecurityProtocol::Ssl],
            Some(
                pki.tls_with_client_auth(ClientAuth::Required)
                    .with_principal_mapping("RULE:^CN=([^,]+).*$/$1/".parse().expect("valid")),
            ),
        ),
    );

    let stranger = pki.authority.client_cert("mallory", "Example");
    let mut stream = connect_tls(addrs[0], pki.authority.client_config(Some(&stranger)))
        .await
        .expect("the handshake itself succeeds: the CA signed the certificate");
    let frame = build_request_frame(API_KEY_API_VERSIONS, 1, 1, Some("stranger"), &[]);
    // The write may or may not land before the close; the read is the assertion.
    let _ = stream.write_all(&frame).await;
    assert_eq!(
        read_byte_with_timeout(&mut stream, Duration::from_secs(5)).await,
        ByteRead::Closed,
        "a certificate for an account that does not exist admits nothing"
    );
}

#[tokio::test]
async fn given_required_client_auth_when_no_certificate_is_presented_should_fail_the_handshake() {
    let pki = Pki::new();
    let users = KnownUsers::with(USERNAME, topic_reader());
    let (addrs, _shutdown) = spawn_gateway(
        move |config| KafkaGateway::new(config).with_principal_resolver(users),
        quick_config(
            vec![SecurityProtocol::Ssl],
            Some(pki.tls_with_client_auth(ClientAuth::Required)),
        ),
    );

    // Under TLS 1.3 the client's side of the handshake completes before the server's
    // `certificate_required` alert arrives, so the refusal shows on the first read.
    let Ok(mut stream) = connect_tls(addrs[0], pki.authority.client_config(None)).await else {
        return;
    };
    let frame = build_request_frame(API_KEY_API_VERSIONS, 1, 1, Some("anonymous"), &[]);
    let _ = stream.write_all(&frame).await;
    assert_eq!(
        read_byte_with_timeout(&mut stream, Duration::from_secs(5)).await,
        ByteRead::Closed,
        "a client without a certificate must not be served on a listener that requires one"
    );
}

#[tokio::test]
async fn given_requested_client_auth_when_no_certificate_is_presented_should_serve_anonymously() {
    let pki = Pki::new();
    let users = KnownUsers::with(USERNAME, topic_reader());
    let (addrs, _shutdown) = spawn_gateway(
        move |config| KafkaGateway::new(config).with_principal_resolver(users),
        quick_config(
            vec![SecurityProtocol::Ssl],
            Some(pki.tls_with_client_auth(ClientAuth::Requested)),
        ),
    );

    let mut stream = connect_tls(addrs[0], pki.authority.client_config(None))
        .await
        .expect("a requested certificate may be withheld");
    let rows = api_versions(&mut stream).await;
    assert!(advertises(&rows, API_KEY_DESCRIBE_ACLS));
    let (code, body) = describe_acls(&mut stream).await;
    assert_eq!(
        code, ERROR_NONE,
        "Kafka's anonymous principal is described, not refused"
    );
    assert!(
        !mentions(&body, "User:"),
        "and it holds nothing, got a binding in: {body:?}"
    );
}

#[tokio::test]
async fn given_client_auth_when_a_client_without_a_certificate_uses_sasl_ssl_should_authenticate_by_sasl()
 {
    let pki = Pki::new();
    let users = KnownUsers::with("certificate-user", topic_reader());
    let resolver = Arc::clone(&users);
    let (addrs, _shutdown) = spawn_gateway(
        move |config| {
            KafkaGateway::new(config)
                .with_authenticator(Arc::new(FixedCredentials))
                .with_principal_resolver(resolver)
        },
        quick_config(
            vec![SecurityProtocol::Ssl, SecurityProtocol::SaslSsl],
            Some(
                pki.tls_with_client_auth(ClientAuth::Required)
                    .with_principal_mapping("RULE:^CN=([^,]+).*$/$1/".parse().expect("valid")),
            ),
        ),
    );
    let [ssl, sasl_ssl] = addrs[..] else {
        panic!("two listeners")
    };

    let mut stream = connect_tls(sasl_ssl, pki.authority.client_config(None))
        .await
        .expect("SASL_SSL asks for no client certificate, as on a Kafka broker");
    assert_eq!(
        sasl_plain(&mut stream, USERNAME, PASSWORD).await,
        ERROR_NONE
    );
    let (code, body) = describe_acls(&mut stream).await;
    assert_eq!(code, ERROR_NONE);
    assert!(
        mentions(&body, &format!("User:{USERNAME}")),
        "the SASL principal is the one described"
    );
    assert!(
        users.asked().is_empty(),
        "nothing is looked up by certificate on a SASL listener"
    );

    let Ok(mut refused) = connect_tls(ssl, pki.authority.client_config(None)).await else {
        return;
    };
    let frame = build_request_frame(API_KEY_API_VERSIONS, 1, 1, Some("anonymous"), &[]);
    let _ = refused.write_all(&frame).await;
    assert_eq!(
        read_byte_with_timeout(&mut refused, Duration::from_secs(5)).await,
        ByteRead::Closed,
        "the SSL listener beside it still requires a certificate"
    );
}

#[tokio::test]
async fn given_a_large_response_when_the_tls_client_reads_slowly_should_deliver_all_of_it() {
    let pki = Pki::new();
    // Small fixed buffers on both ends turn off autotuning, so the gateway's socket is full
    // whenever rustls hands it the end of the response.
    let socket = bind_listener("127.0.0.1:0").expect("bind");
    socket2::SockRef::from(&socket)
        .set_send_buffer_size(16 * 1024)
        .expect("send buffer");
    let addr = socket.local_addr().expect("local addr");
    let (_shutdown, rx) = tokio::sync::broadcast::channel(1);
    let config = GatewayConfig {
        write_timeout: Duration::from_secs(30),
        ..quick_config(vec![SecurityProtocol::Ssl], Some(pki.tls()))
    };
    tokio::spawn(KafkaGateway::new(config).run(
        vec![BoundListener::new(listener(SecurityProtocol::Ssl), socket)],
        rx,
    ));
    let client = TcpSocket::new_v4().expect("socket");
    client
        .set_recv_buffer_size(16 * 1024)
        .expect("receive buffer");
    let tcp = client.connect(addr).await.expect("connect");
    let mut stream = connect_tls_over(tcp, pki.authority.client_config(None))
        .await
        .expect("TLS handshake");

    // The stub echoes every requested topic name back, so the answer is about as large as the
    // request.
    let name = "t".repeat(16 * 1024);
    let names: Vec<&str> = std::iter::repeat_n(name.as_str(), 128).collect();
    let request = build_metadata_legacy_request(&names);
    let frame = build_request_frame(API_KEY_METADATA, 0, 7, Some("tls-test"), &request);
    stream.write_all(&frame).await.expect("write request");

    let read = tokio::time::timeout(Duration::from_secs(20), async {
        let mut prefix = [0u8; 4];
        stream.read_exact(&mut prefix).await.expect("length prefix");
        let frame_len = usize::try_from(i32::from_be_bytes(prefix)).expect("positive length");
        let mut chunk = vec![0u8; 16 * 1024];
        let mut received = 0;
        while received < frame_len {
            let read = stream.read(&mut chunk).await.expect("read");
            assert_ne!(read, 0, "closed after {received} of {frame_len} bytes");
            received += read;
            tokio::time::sleep(Duration::from_millis(1)).await;
        }
        frame_len
    })
    .await;
    let frame_len = read.expect("the whole response arrives; its tail must not wait for a flush");
    assert!(frame_len > 2 * 1024 * 1024, "got only {frame_len} bytes");
}

#[tokio::test]
async fn given_required_client_auth_when_no_rule_maps_the_subject_should_close() {
    let pki = Pki::new();
    let users = KnownUsers::with(USERNAME, topic_reader());
    let resolver = Arc::clone(&users);
    let (addrs, _shutdown) = spawn_gateway(
        move |config| KafkaGateway::new(config).with_principal_resolver(resolver),
        quick_config(
            vec![SecurityProtocol::Ssl],
            Some(
                pki.tls_with_client_auth(ClientAuth::Required)
                    .with_principal_mapping(
                        "RULE:^CN=([^,]+),O=Allowed$/$1/".parse().expect("valid"),
                    ),
            ),
        ),
    );

    let unmapped = pki.authority.client_cert(USERNAME, "Example");
    let mut stream = connect_tls(addrs[0], pki.authority.client_config(Some(&unmapped)))
        .await
        .expect("the CA signed the certificate, so the handshake succeeds");
    let frame = build_request_frame(API_KEY_API_VERSIONS, 1, 1, Some("unmapped"), &[]);
    let _ = stream.write_all(&frame).await;
    assert_eq!(
        read_byte_with_timeout(&mut stream, Duration::from_secs(5)).await,
        ByteRead::Closed,
        "a subject no rule maps names nobody, and is not served as anonymous"
    );
    assert!(users.asked().is_empty(), "nothing was looked up");
}

#[tokio::test]
async fn given_a_client_that_never_starts_the_handshake_should_close_it_within_the_budget() {
    let pki = Pki::new();
    let (addrs, _shutdown) = spawn_gateway(
        KafkaGateway::new,
        GatewayConfig {
            pre_auth_timeout: Duration::from_millis(300),
            ..quick_config(vec![SecurityProtocol::Ssl], Some(pki.tls()))
        },
    );

    let mut stream = TcpStream::connect(addrs[0]).await.expect("connect");
    assert_eq!(
        read_byte_with_timeout(&mut stream, Duration::from_secs(3)).await,
        ByteRead::Closed,
        "a silent client must not hold a connection slot past the pre-authentication budget"
    );
}

#[tokio::test]
async fn given_a_lookup_that_fails_or_hangs_should_close_the_certificate_connection() {
    for resolver in [BrokenResolver::Unavailable, BrokenResolver::Hangs] {
        let pki = Pki::new();
        let label = format!("{resolver:?}");
        let resolver = Arc::new(resolver);
        let (addrs, _shutdown) = spawn_gateway(
            move |config| KafkaGateway::new(config).with_principal_resolver(resolver),
            GatewayConfig {
                pre_auth_timeout: Duration::from_millis(300),
                ..quick_config(
                    vec![SecurityProtocol::Ssl],
                    Some(
                        pki.tls_with_client_auth(ClientAuth::Required)
                            .with_principal_mapping(
                                "RULE:^CN=([^,]+).*$/$1/".parse().expect("valid"),
                            ),
                    ),
                )
            },
        );

        let certificate = pki.authority.client_cert(USERNAME, "Example");
        let mut stream = connect_tls(addrs[0], pki.authority.client_config(Some(&certificate)))
            .await
            .expect("mTLS handshake");
        let frame = build_request_frame(API_KEY_API_VERSIONS, 1, 1, Some("lookup"), &[]);
        let _ = stream.write_all(&frame).await;
        assert_eq!(
            read_byte_with_timeout(&mut stream, Duration::from_secs(3)).await,
            ByteRead::Closed,
            "{label}: an identity that cannot be confirmed admits nothing"
        );
    }
}

#[tokio::test]
async fn given_rotated_certificate_files_should_serve_the_new_certificate_without_a_restart() {
    let pki = Pki::new();
    let first = pki.authority.server_cert("first");
    first.write(pki.dir.path(), "server");
    let (addrs, _shutdown) = spawn_gateway(
        KafkaGateway::new,
        quick_config(
            vec![SecurityProtocol::Ssl],
            Some(pki.tls().with_reload_interval(Duration::from_millis(100))),
        ),
    );

    let addr = addrs[0];
    let served = |expected: &Issued| {
        let config = pki.authority.client_config(None);
        let expected = expected.der.clone();
        async move {
            let stream = connect_tls(addr, config).await.expect("TLS handshake");
            let (_, connection) = stream.get_ref();
            let leaf = connection
                .peer_certificates()
                .and_then(|chain| chain.first())
                .expect("the server presented a certificate");
            leaf.as_ref() == expected.as_slice()
        }
    };
    assert!(served(&first).await, "the certificate loaded at startup");

    let second = pki.authority.server_cert("second");
    second.write(pki.dir.path(), "server");
    let deadline = tokio::time::Instant::now() + Duration::from_secs(10);
    while !served(&second).await {
        assert!(
            tokio::time::Instant::now() < deadline,
            "the rotated certificate never went live"
        );
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
}

#[tokio::test]
async fn given_unusable_material_should_refuse_to_start() {
    let pki = Pki::new();
    let other = TestPki::new().server_cert("other");
    // The key belongs to a different certificate.
    std::fs::write(&pki.key_file, other.key_pem).expect("swap key");
    let socket = bind_listener("127.0.0.1:0").expect("bind");
    let (_tx, rx) = tokio::sync::broadcast::channel(1);
    let error = KafkaGateway::new(quick_config(vec![SecurityProtocol::Ssl], Some(pki.tls())))
        .run(
            vec![BoundListener::new(listener(SecurityProtocol::Ssl), socket)],
            rx,
        )
        .await
        .unwrap_err()
        .to_string();
    assert!(error.contains("pair"), "got: {error}");
}

#[tokio::test]
async fn given_certificate_identities_without_a_lookup_should_refuse_to_start() {
    let pki = Pki::new();
    let socket = bind_listener("127.0.0.1:0").expect("bind");
    let (_tx, rx) = tokio::sync::broadcast::channel(1);
    let config = quick_config(
        vec![SecurityProtocol::Ssl],
        Some(pki.tls_with_client_auth(ClientAuth::Required)),
    );
    let error = KafkaGateway::new(config)
        .run(
            vec![BoundListener::new(listener(SecurityProtocol::Ssl), socket)],
            rx,
        )
        .await
        .unwrap_err()
        .to_string();
    assert!(
        error.contains("IGGY_KAFKA_BRIDGE_ENABLED"),
        "a certificate names an Iggy user only a bridge session can look up, got: {error}"
    );
}

#[tokio::test]
async fn given_a_lookup_queued_behind_another_should_get_its_own_budget_once_it_holds_the_slot() {
    // Each lookup fits the budget alone, but the queued one's wait plus its own lookup does not:
    // it is served only if the budget restarts when the slot is acquired.
    let budget = Duration::from_millis(1000);
    let pki = Pki::new();
    let users = Arc::new(SlowUsers {
        delay: Duration::from_millis(800),
        started: AtomicUsize::new(0),
    });
    let resolver = Arc::clone(&users);
    let (addrs, _shutdown) = spawn_gateway(
        move |config| KafkaGateway::new(config).with_principal_resolver(resolver),
        GatewayConfig {
            pre_auth_timeout: budget,
            max_concurrent_authentications: 1,
            ..quick_config(
                vec![SecurityProtocol::Ssl],
                Some(
                    pki.tls_with_client_auth(ClientAuth::Required)
                        .with_principal_mapping("RULE:^CN=([^,]+).*$/$1/".parse().expect("valid")),
                ),
            )
        },
    );
    let certificate = pki.authority.client_cert(USERNAME, "Example");
    let client_config = pki.authority.client_config(Some(&certificate));

    let mut first = connect_tls(addrs[0], Arc::clone(&client_config))
        .await
        .expect("first mTLS handshake");
    while users.started.load(Ordering::SeqCst) == 0 {
        tokio::time::sleep(Duration::from_millis(5)).await;
    }
    let mut queued = connect_tls(addrs[0], client_config)
        .await
        .expect("queued mTLS handshake");

    let (first_rows, queued_rows) =
        tokio::join!(api_versions(&mut first), api_versions(&mut queued));
    assert!(advertises(&first_rows, API_KEY_DESCRIBE_ACLS));
    assert!(
        advertises(&queued_rows, API_KEY_DESCRIBE_ACLS),
        "the queued lookup must not inherit what the wait left of a shared deadline"
    );
}

#[tokio::test]
async fn given_one_listener_at_its_connection_limit_should_still_serve_another_listener() {
    let (addrs, _shutdown) = spawn_gateway(
        KafkaGateway::new,
        GatewayConfig {
            max_connections: 1,
            ..quick_config(
                vec![SecurityProtocol::Plaintext, SecurityProtocol::Plaintext],
                None,
            )
        },
    );

    let mut held = TcpStream::connect(addrs[0]).await.expect("connect");
    assert!(advertises(&api_versions(&mut held).await, API_KEY_METADATA));
    let mut refused = TcpStream::connect(addrs[0]).await.expect("connect");
    assert_eq!(
        read_byte_with_timeout(&mut refused, Duration::from_secs(3)).await,
        ByteRead::Closed,
        "the first listener is full"
    );

    let mut other = TcpStream::connect(addrs[1]).await.expect("connect");
    assert!(
        advertises(&api_versions(&mut other).await, API_KEY_METADATA),
        "each listener has its own connection pool"
    );
}

#[tokio::test]
async fn given_client_auth_without_an_ssl_listener_should_refuse_to_start() {
    let pki = Pki::new();
    let socket = bind_listener("127.0.0.1:0").expect("bind");
    let (_tx, rx) = tokio::sync::broadcast::channel(1);
    let config = quick_config(
        vec![SecurityProtocol::SaslSsl],
        Some(pki.tls_with_client_auth(ClientAuth::Required)),
    );
    let error = KafkaGateway::new(config)
        .with_authenticator(Arc::new(FixedCredentials))
        .run(
            vec![BoundListener::new(
                listener(SecurityProtocol::SaslSsl),
                socket,
            )],
            rx,
        )
        .await
        .unwrap_err()
        .to_string();
    assert!(error.contains("no SSL listener"), "got: {error}");
}

/// Kept for the suites that pin the single-listener shape: a `SASL_PLAINTEXT` listener on an
/// ephemeral port is what `IGGY_KAFKA_SASL_ENABLED=true` used to mean.
#[tokio::test]
async fn given_a_sasl_plaintext_listener_should_still_run_the_exchange_in_the_clear() {
    let (addrs, _shutdown) = spawn_gateway(
        |config| KafkaGateway::new(config).with_authenticator(Arc::new(FixedCredentials)),
        quick_config(vec![SecurityProtocol::SaslPlaintext], None),
    );
    let mut stream = TcpStream::connect(addrs[0]).await.expect("connect");
    assert_eq!(
        sasl_plain(&mut stream, USERNAME, PASSWORD).await,
        ERROR_NONE
    );
}
