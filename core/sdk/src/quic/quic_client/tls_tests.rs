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

use super::*;
use quinn::{Connection, ConnectionError, Endpoint, ServerConfig};
use rustls::RootCertStore;
use rustls::pki_types::{CertificateDer, PrivatePkcs8KeyDer};
use tokio::time::timeout;

fn server(name: &str) -> (Endpoint, CertificateDer<'static>) {
    let _ = rustls::crypto::ring::default_provider().install_default();
    let certified = rcgen::generate_simple_self_signed(vec![name.to_owned()]).unwrap();
    let certificate = certified.cert.der().clone();
    let config = ServerConfig::with_single_cert(
        vec![certificate.clone()],
        PrivatePkcs8KeyDer::from(certified.signing_key.serialize_der()).into(),
    )
    .unwrap();
    (
        Endpoint::server(config, "127.0.0.1:0".parse().unwrap()).unwrap(),
        certificate,
    )
}

async fn handshake(
    server: &Endpoint,
    config: &QuicClientConfig,
    tls: ClientConfig,
) -> Result<Connection, ConnectionError> {
    let mut client = Endpoint::client("127.0.0.1:0".parse().unwrap()).unwrap();
    client.set_default_client_config(tls);
    let (result, peer) = timeout(Duration::from_secs(5), async {
        tokio::join!(
            client
                .connect(server.local_addr().unwrap(), &config.server_name)
                .unwrap(),
            async { server.accept().await.unwrap().await },
        )
    })
    .await
    .expect("local QUIC handshake must finish within the test budget");
    match (&result, &peer) {
        (Ok(_), Ok(_)) | (Err(_), Err(_)) => {}
        _ => panic!("client and server TLS results disagree: {result:?}, {peer:?}"),
    }
    // Close both ends only after both handshakes complete, so a success cannot
    // race with the fixture shutting down its endpoint.
    client.close(0u32.into(), b"test complete");
    timeout(Duration::from_secs(5), client.wait_idle())
        .await
        .unwrap();
    result
}

fn assert_certificate_failure(result: Result<Connection, ConnectionError>) {
    match result {
        Err(ConnectionError::TransportError(error)) => {
            let code: u64 = error.code.into();
            assert!(
                (0x100..=0x1ff).contains(&code),
                "expected a TLS crypto alert: {error}"
            );
            assert!(
                error.reason.to_lowercase().contains("certificate"),
                "expected a certificate verdict: {error}"
            );
        }
        other => panic!(
            "expected a certificate failure rather than a DNS, login or timeout error: {other:?}"
        ),
    }
}

#[tokio::test]
async fn connection_string_verification_flags_control_quic_certificate_acceptance() {
    for (options, accepted) in [
        ("tls_validate_certificate=true", false),
        ("validate_certificate=true", false),
        ("tls_validate_certificate=false", true),
        ("validate_certificate=false", true),
        (
            "validate_certificate=false&tls_validate_certificate=true",
            false,
        ),
        (
            "tls_validate_certificate=true&validate_certificate=false",
            true,
        ),
    ] {
        let (server, _) = server("localhost");
        let client = QuicClient::from_connection_string(&format!(
            "iggy+quic://user:secret@{}?{options}",
            server.local_addr().unwrap()
        ))
        .unwrap();
        let result = handshake(&server, &client.config, configure(&client.config).unwrap()).await;
        if accepted {
            assert!(result.is_ok(), "{options}: {result:?}");
        } else {
            assert_certificate_failure(result);
        }
        server.close(0u32.into(), b"test complete");
    }
}

#[test]
fn invalid_booleans_fail_before_quic_transport_initialization() {
    for key in ["tls_validate_certificate", "validate_certificate"] {
        for value in [
            concat!("tu", "re"),
            "invalid",
            "yes",
            "1",
            "",
            "TRUE",
            "False",
            "0",
        ] {
            assert!(matches!(
                QuicClient::from_connection_string(&format!(
                    "iggy+quic://user:secret@127.0.0.1:8080?{key}={value}"
                )),
                Err(IggyError::InvalidConnectionString)
            ));
        }
    }
}

#[tokio::test]
async fn explicit_quic_server_name_controls_hostname_verification() {
    let (server, certificate) = server("remote.example");
    // Trust only this fixture to isolate hostname matching from trust-chain
    // rejection. Production verification above still uses platform roots.
    let mut roots = RootCertStore::empty();
    roots.add(certificate).unwrap();
    let tls = ClientConfig::with_root_certificates(Arc::new(roots)).unwrap();
    let connection = ConnectionString::<QuicConnectionStringOptions>::new(&format!(
        "iggy+quic://user:secret@{}?tls_validate_certificate=true",
        server.local_addr().unwrap()
    ))
    .unwrap();
    let mut config = QuicClientConfig::from(connection);
    assert_eq!(config.server_name, "localhost");
    assert_certificate_failure(handshake(&server, &config, tls.clone()).await);
    config.server_name = "remote.example".into();
    assert!(handshake(&server, &config, tls).await.is_ok());
    server.close(0u32.into(), b"test complete");
}
