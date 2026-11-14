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
use crate::tcp::tcp_client::TcpClient;
use iggy_common::{ConnectionString, TcpConnectionStringOptions};
use rustls::pki_types::PrivatePkcs8KeyDer;
use std::fs::{self, OpenOptions};
use std::io::Write;
use std::path::PathBuf;
use tokio::net::TcpListener;
use tokio_rustls::TlsAcceptor;

struct CaFile(PathBuf);

impl CaFile {
    fn new(pem: &str) -> Self {
        let path = std::env::temp_dir().join(format!("iggy-tcp-ca-{}.pem", rand::random::<u128>()));
        let mut file = OpenOptions::new()
            .write(true)
            .create_new(true)
            .open(&path)
            .unwrap();
        file.write_all(pem.as_bytes()).unwrap();
        Self(path)
    }
}

impl Drop for CaFile {
    fn drop(&mut self) {
        fs::remove_file(&self.0).unwrap();
    }
}

async fn handshake(options: &str, trusted: bool, expected: Result<(), IggyError>) {
    let _ = rustls::crypto::ring::default_provider().install_default();
    let certified =
        rcgen::generate_simple_self_signed(vec!["localhost".into(), "127.0.0.1".into()]).unwrap();
    let ca = CaFile::new(&certified.cert.pem());
    let server_config = rustls::ServerConfig::builder()
        .with_no_client_auth()
        .with_single_cert(
            vec![certified.cert.der().clone()],
            PrivatePkcs8KeyDer::from(certified.signing_key.serialize_der()).into(),
        )
        .unwrap();
    let acceptor = TlsAcceptor::from(Arc::new(server_config));
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap().to_string();
    let ca_option = if trusted {
        format!("&tls_ca_file={}", ca.0.display())
    } else {
        String::new()
    };
    let url = format!("iggy+tcp://user:secret@{address}?tls=true&{options}{ca_option}");
    let client = TcpClient::from_connection_string(&url).unwrap();
    let dialer = Dialer::Tcp(Arc::clone(&client.config));
    let (result, accepted) = timeout(Duration::from_secs(5), async {
        tokio::join!(dialer.dial(&address), async {
            let (stream, _) = listener.accept().await.unwrap();
            acceptor.accept(stream).await
        })
    })
    .await
    .expect("local TCP TLS handshake must finish within the test budget");
    assert_eq!(
        result.as_ref().map(|_| ()).map_err(Clone::clone),
        expected,
        "{url}"
    );
    assert_eq!(
        accepted.is_ok(),
        expected.is_ok(),
        "server handshake: {accepted:?}"
    );
}

#[tokio::test]
async fn connection_string_tls_trust_and_domain_reach_tcp_handshake() {
    handshake(
        "tls_validate_certificate=true&tls_domain=localhost",
        true,
        Ok(()),
    )
    .await;
    handshake("tls_validate_certificate=true", true, Ok(())).await;
    handshake(
        "tls_validate_certificate=true&tls_domain=localhost",
        false,
        Err(IggyError::CannotEstablishConnection),
    )
    .await;
    handshake(
        "tls_validate_certificate=true&tls_domain=other.example",
        true,
        Err(IggyError::CannotEstablishConnection),
    )
    .await;
    handshake(
        "tls_validate_certificate=false&tls_domain=other.example",
        false,
        Ok(()),
    )
    .await;
    handshake("tls_domain=localhost", true, Ok(())).await;
}

#[test]
fn malformed_tls_connection_strings_fail_before_client_creation() {
    for key in ["tls", "tls_validate_certificate"] {
        for value in [
            concat!("tu", "re"),
            "invalid",
            "yes",
            "1",
            "",
            "TRUE",
            "False",
        ] {
            assert!(matches!(
                TcpClient::from_connection_string(&format!(
                    "iggy+tcp://user:secret@127.0.0.1:8090?{key}={value}"
                )),
                Err(IggyError::InvalidConnectionString)
            ));
        }
    }
}

#[tokio::test]
async fn connection_string_invalid_ca_path_is_a_configuration_error() {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap().to_string();
    let connection = ConnectionString::<TcpConnectionStringOptions>::new(&format!("iggy+tcp://user:secret@{address}?tls=true&tls_validate_certificate=true&tls_ca_file={}/Cargo.toml/ca.pem", env!("CARGO_MANIFEST_DIR"))).unwrap();
    let dialer = Dialer::Tcp(Arc::new(connection.into()));
    assert_eq!(
        dialer.dial(&address).await.err(),
        Some(IggyError::InvalidTlsCertificatePath)
    );
}
