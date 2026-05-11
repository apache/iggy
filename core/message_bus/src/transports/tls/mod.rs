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

//! Shared TLS plumbing for the in-process TCP-TLS and WSS transports.
//!
//! Hosts the credentials newtypes, PEM loaders, and the idempotent
//! rustls/ring crypto provider initializer. The hand-rolled
//! `UnbufferedConnection` driver was retired in favour of `compio-tls`;
//! the per-transport handshake + steady-state pump now lives directly
//! in `transports/tcp_tls.rs` and `transports/wss.rs`.
//!
//! # Why an explicit `install_default_crypto_provider`
//!
//! `rustls` 0.23 requires a crypto provider to be installed before any
//! `ServerConfig::builder()` / `ClientConfig::builder()` call. The
//! workspace pins `rustls = { features = ["ring"] }`, so the
//! provider is `rustls::crypto::ring::default_provider()`. Multiple
//! call sites (transports, tests, server-ng bootstrap) may attempt to
//! install it; the wrapper here is idempotent and safe under races
//! between concurrent first-use sites.

use rustls::pki_types::{CertificateDer, PrivateKeyDer, ServerName};
use std::io::{self, BufReader};
use std::path::Path;
use std::sync::Arc;

/// Local-end role on a TLS connection. Selects which rustls state
/// machine the per-transport handshake helper drives and is shared by
/// the in-process TCP-TLS and WSS transports.
pub enum TlsRole {
    /// Server side: drives the rustls server state machine against the
    /// supplied [`rustls::ServerConfig`].
    Server(Arc<rustls::ServerConfig>),
    /// Client side: drives the rustls client state machine against the
    /// supplied [`rustls::ClientConfig`] + a pre-validated server name.
    Client {
        /// Client-role rustls configuration.
        config: Arc<rustls::ClientConfig>,
        /// Server name presented in SNI and validated against the leaf
        /// certificate's SANs / CN. Owned `'static` because the
        /// `compio_tls::TlsConnector::connect` path retains it for the
        /// lifetime of the underlying rustls `ClientConnection`.
        server_name: ServerName<'static>,
    },
}

/// Server-side TLS credentials: a certificate chain and the matching
/// private key, both in DER form.
///
/// Construct via [`load_pem`] for production deployments or
/// [`self_signed_for_loopback`] for tests / local development.
pub struct TlsServerCredentials {
    /// Leaf certificate first, intermediates in order, root LAST or
    /// omitted (matches the on-wire order rustls expects).
    pub cert_chain: Vec<CertificateDer<'static>>,
    /// PKCS#8, RSA, or SEC1-encoded private key matching the leaf
    /// certificate's public key.
    pub key_der: PrivateKeyDer<'static>,
}

impl std::fmt::Debug for TlsServerCredentials {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        // Don't print key bytes. cert_chain is public material so
        // length is fine to disclose.
        f.debug_struct("TlsServerCredentials")
            .field("cert_chain_len", &self.cert_chain.len())
            .field("key_der", &"<redacted>")
            .finish()
    }
}

/// Load a server certificate chain + private key from on-disk PEM
/// files.
///
/// `cert_path` must contain one or more `BEGIN CERTIFICATE` blocks in
/// chain order (leaf first). `key_path` must contain a single
/// `BEGIN ... PRIVATE KEY` block (PKCS#8, RSA, or SEC1).
///
/// # Errors
///
/// Returns [`io::Error`] when either file cannot be read, the
/// certificate file contains no certificates, or the key file
/// contains no recognized private key.
pub fn load_pem(cert_path: &Path, key_path: &Path) -> io::Result<TlsServerCredentials> {
    let cert_file = std::fs::File::open(cert_path)?;
    let mut cert_reader = BufReader::new(cert_file);
    let cert_chain: Vec<CertificateDer<'static>> =
        rustls_pemfile::certs(&mut cert_reader).collect::<Result<_, _>>()?;
    if cert_chain.is_empty() {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            format!("no certificates found in {}", cert_path.display()),
        ));
    }

    let key_file = std::fs::File::open(key_path)?;
    let mut key_reader = BufReader::new(key_file);
    let key_der = rustls_pemfile::private_key(&mut key_reader)?.ok_or_else(|| {
        io::Error::new(
            io::ErrorKind::InvalidData,
            format!("no private key found in {}", key_path.display()),
        )
    })?;

    Ok(TlsServerCredentials {
        cert_chain,
        key_der,
    })
}

/// Generate a fresh self-signed certificate valid for `localhost`.
///
/// Intended for tests and local-only development. Production
/// deployments must call [`load_pem`] with credentials provisioned by
/// the operator's PKI.
///
/// # Panics
///
/// Panics if `rcgen` fails to generate a key pair. This indicates a
/// platform-level RNG failure and would not be a recoverable error
/// in any caller this helper is intended for (tests, dev loops).
#[must_use]
pub fn self_signed_for_loopback() -> TlsServerCredentials {
    let (cert_chain, key_der) = iggy_common::generate_self_signed_certificate("localhost")
        .expect("rcgen self-signed certificate generation must not fail in test/dev paths");
    TlsServerCredentials {
        cert_chain,
        key_der,
    }
}

/// Install the rustls/ring crypto provider as the process-wide
/// default. Idempotent and safe to call from multiple sites.
///
/// rustls 0.23 requires a default provider before any
/// `ServerConfig::builder()` or `ClientConfig::builder()` call. This
/// helper centralises the install so transports, tests, and
/// server-ng bootstrap converge on the same provider without
/// per-call-site `let _ = ...install_default()`.
pub fn install_default_crypto_provider() {
    // Race-safe: rustls's `install_default` returns Err if a provider
    // is already installed (either by us on a prior call or by another
    // crate in the workspace). Either outcome is a successful no-op
    // for the caller.
    let _ = rustls::crypto::ring::default_provider().install_default();
}

#[cfg(test)]
mod tests {
    use super::*;
    use rustls::RootCertStore;
    use std::io::Write;
    use std::sync::Arc;

    #[test]
    fn pem_roundtrip_load_yields_usable_server_config() {
        install_default_crypto_provider();

        // Generate a real cert/key pair via rcgen and serialise to PEM,
        // then verify load_pem reconstitutes credentials that build a
        // valid rustls::ServerConfig.
        let cert = rcgen::generate_simple_self_signed(vec!["localhost".to_owned()]).expect("rcgen");
        let cert_pem = cert.cert.pem();
        let key_pem = cert.signing_key.serialize_pem();

        let cert_file = tempfile::NamedTempFile::new().expect("tempfile cert");
        let key_file = tempfile::NamedTempFile::new().expect("tempfile key");
        std::fs::File::create(cert_file.path())
            .unwrap()
            .write_all(cert_pem.as_bytes())
            .unwrap();
        std::fs::File::create(key_file.path())
            .unwrap()
            .write_all(key_pem.as_bytes())
            .unwrap();

        let loaded = load_pem(cert_file.path(), key_file.path()).expect("load_pem");
        assert_eq!(loaded.cert_chain.len(), 1);

        let _server_cfg = rustls::ServerConfig::builder()
            .with_no_client_auth()
            .with_single_cert(loaded.cert_chain, loaded.key_der)
            .expect("ServerConfig::with_single_cert");
    }

    #[test]
    fn self_signed_validates_via_rustls_server_config() {
        install_default_crypto_provider();

        let creds = self_signed_for_loopback();
        let _server_cfg = rustls::ServerConfig::builder()
            .with_no_client_auth()
            .with_single_cert(creds.cert_chain.clone(), creds.key_der)
            .expect("ServerConfig builds from self-signed cert");

        // Same cert must be installable into a client RootCertStore for
        // the test client side to dial in.
        let mut roots = RootCertStore::empty();
        roots
            .add(creds.cert_chain[0].clone())
            .expect("RootCertStore::add");
        let _client_cfg = rustls::ClientConfig::builder()
            .with_root_certificates(Arc::new(roots))
            .with_no_client_auth();
    }

    #[test]
    fn crypto_provider_install_is_idempotent() {
        // Multiple calls must not panic. First call may install, all
        // subsequent calls are no-ops.
        install_default_crypto_provider();
        install_default_crypto_provider();
        install_default_crypto_provider();

        // Sanity: a builder call after install must succeed (proves
        // the provider is actually present).
        let _ = rustls::ServerConfig::builder().with_no_client_auth();
    }

    #[test]
    fn load_pem_rejects_empty_cert_file() {
        let cert_file = tempfile::NamedTempFile::new().expect("tempfile cert");
        let key_file = tempfile::NamedTempFile::new().expect("tempfile key");
        // Both files exist but are empty.
        let err = load_pem(cert_file.path(), key_file.path()).expect_err("must fail");
        assert_eq!(err.kind(), io::ErrorKind::InvalidData);
    }

    #[test]
    fn load_pem_rejects_missing_key() {
        let cert = rcgen::generate_simple_self_signed(vec!["localhost".to_owned()]).expect("rcgen");
        let cert_pem = cert.cert.pem();
        let cert_file = tempfile::NamedTempFile::new().expect("tempfile cert");
        let key_file = tempfile::NamedTempFile::new().expect("tempfile key");
        std::fs::File::create(cert_file.path())
            .unwrap()
            .write_all(cert_pem.as_bytes())
            .unwrap();
        // Key file is empty.
        let err = load_pem(cert_file.path(), key_file.path()).expect_err("must fail");
        assert_eq!(err.kind(), io::ErrorKind::InvalidData);
    }
}
