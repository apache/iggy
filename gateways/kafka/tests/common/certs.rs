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

//! A throwaway PKI for the TLS suites, minted per test so nothing checked in can expire.
//!
//! Compiled into each integration test binary via `#[path]`.
#![allow(dead_code)]

use std::net::SocketAddr;
use std::path::{Path, PathBuf};
use std::sync::Arc;

use rcgen::{
    BasicConstraints, CertificateParams, DistinguishedName, DnType, ExtendedKeyUsagePurpose, IsCa,
    Issuer, KeyPair,
};
use rustls::pki_types::pem::PemObject;
use rustls::pki_types::{CertificateDer, PrivateKeyDer, ServerName};
use rustls::{ClientConfig, RootCertStore};
use tokio::net::TcpStream;
use tokio_rustls::TlsConnector;
use tokio_rustls::client::TlsStream;

/// A certificate and the key that goes with it, as PEM and as DER.
pub struct Issued {
    pub cert_pem: String,
    pub key_pem: String,
    pub der: Vec<u8>,
}

impl Issued {
    /// Writes `<name>.pem` and `<name>.key` into `dir` and returns their paths.
    pub fn write(&self, dir: &Path, name: &str) -> (PathBuf, PathBuf) {
        let cert = dir.join(format!("{name}.pem"));
        let key = dir.join(format!("{name}.key"));
        std::fs::write(&cert, &self.cert_pem).expect("write certificate");
        std::fs::write(&key, &self.key_pem).expect("write key");
        (cert, key)
    }

    /// Both halves in one PEM file, the shape the Java client's PEM keystore reads.
    pub fn write_bundle(&self, dir: &Path, name: &str) -> PathBuf {
        let path = dir.join(format!("{name}.pem"));
        std::fs::write(&path, format!("{}{}", self.key_pem, self.cert_pem)).expect("write bundle");
        path
    }
}

/// One CA that signs every certificate a test needs.
pub struct TestPki {
    pub ca_pem: String,
    issuer: Issuer<'static, KeyPair>,
}

impl Default for TestPki {
    fn default() -> Self {
        Self::new()
    }
}

impl TestPki {
    pub fn new() -> Self {
        let key = KeyPair::generate().expect("generate a CA key");
        let mut params = CertificateParams::new(Vec::<String>::new()).expect("CA params");
        params.is_ca = IsCa::Ca(BasicConstraints::Unconstrained);
        params
            .distinguished_name
            .push(DnType::CommonName, "iggy kafka gateway test CA");
        let certificate = params.self_signed(&key).expect("self-sign the CA");
        Self {
            ca_pem: certificate.pem(),
            issuer: Issuer::new(params, key),
        }
    }

    /// A server certificate for `localhost` and `127.0.0.1`, which is what every client here
    /// dials and what a hostname-verifying Java client checks.
    pub fn server_cert(&self, common_name: &str) -> Issued {
        let mut params =
            CertificateParams::new(vec!["localhost".to_string(), "127.0.0.1".to_string()])
                .expect("server params");
        params
            .distinguished_name
            .push(DnType::CommonName, common_name);
        params.extended_key_usages = vec![ExtendedKeyUsagePurpose::ServerAuth];
        self.issue(&params)
    }

    /// A client certificate whose subject is `CN=<common_name>,O=<organization>`.
    pub fn client_cert(&self, common_name: &str, organization: &str) -> Issued {
        self.client_cert_with_subject(|subject| {
            subject.push(DnType::OrganizationName, organization);
            subject.push(DnType::CommonName, common_name);
        })
    }

    /// A client certificate with whatever subject `build` assembles, in certificate order: least
    /// specific first, the way a CA writes it. rcgen seeds every subject with a common name,
    /// which would otherwise land first.
    pub fn client_cert_with_subject(&self, build: impl FnOnce(&mut DistinguishedName)) -> Issued {
        let mut params = CertificateParams::new(Vec::<String>::new()).expect("client params");
        params.distinguished_name = DistinguishedName::new();
        build(&mut params.distinguished_name);
        params.extended_key_usages = vec![ExtendedKeyUsagePurpose::ClientAuth];
        self.issue(&params)
    }

    fn issue(&self, params: &CertificateParams) -> Issued {
        let key = KeyPair::generate().expect("generate a key");
        let certificate = params.signed_by(&key, &self.issuer).expect("sign");
        Issued {
            cert_pem: certificate.pem(),
            key_pem: key.serialize_pem(),
            der: certificate.der().to_vec(),
        }
    }

    /// Writes the CA certificate into `dir` and returns its path.
    pub fn write_ca(&self, dir: &Path) -> PathBuf {
        let path = dir.join("ca.pem");
        std::fs::write(&path, &self.ca_pem).expect("write CA");
        path
    }

    /// A rustls client that trusts this CA, presenting `client` when given one.
    pub fn client_config(&self, client: Option<&Issued>) -> Arc<ClientConfig> {
        let mut roots = RootCertStore::empty();
        for certificate in CertificateDer::pem_slice_iter(self.ca_pem.as_bytes()) {
            roots
                .add(certificate.expect("CA PEM parses"))
                .expect("CA is a root");
        }
        let builder = ClientConfig::builder_with_provider(Arc::new(
            rustls::crypto::aws_lc_rs::default_provider(),
        ))
        .with_safe_default_protocol_versions()
        .expect("default protocol versions")
        .with_root_certificates(roots);
        let config = match client {
            Some(issued) => {
                let chain = CertificateDer::pem_slice_iter(issued.cert_pem.as_bytes())
                    .collect::<Result<Vec<_>, _>>()
                    .expect("client certificate parses");
                let key = PrivateKeyDer::from_pem_slice(issued.key_pem.as_bytes())
                    .expect("client key parses");
                builder
                    .with_client_auth_cert(chain, key)
                    .expect("client certificate and key match")
            }
            None => builder.with_no_client_auth(),
        };
        Arc::new(config)
    }
}

/// Opens a TLS connection to `addr` as `localhost`, with `config`.
pub async fn connect_tls(
    addr: SocketAddr,
    config: Arc<ClientConfig>,
) -> Result<TlsStream<TcpStream>, std::io::Error> {
    let tcp = TcpStream::connect(addr).await?;
    connect_tls_over(tcp, config).await
}

/// Runs the client handshake over a socket the caller already connected.
pub async fn connect_tls_over(
    tcp: TcpStream,
    config: Arc<ClientConfig>,
) -> Result<TlsStream<TcpStream>, std::io::Error> {
    let server_name = ServerName::try_from("localhost").expect("valid server name");
    TlsConnector::from(config).connect(server_name, tcp).await
}
