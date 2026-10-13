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

//! TLS on the Kafka-facing listeners: the certificate, client authentication, reload without a
//! restart, and the principal a client certificate carries.

use std::fmt;
use std::fs;
use std::path::{Path, PathBuf};
use std::str::FromStr;
use std::sync::{Arc, Mutex, PoisonError};
use std::time::Duration;

use arc_swap::ArcSwap;
use rustls::pki_types::pem::PemObject;
use rustls::pki_types::{CertificateDer, PrivateKeyDer};
use rustls::server::WebPkiClientVerifier;
use rustls::{RootCertStore, ServerConfig, ServerConnection};
use tokio_rustls::TlsAcceptor;
use tokio_util::sync::CancellationToken;
use tracing::{error, info};

use crate::error::{KafkaProtocolError, Result};
use crate::listener::SecurityProtocol;

pub mod identity;

pub use identity::{
    IdentityError, MappingRuleError, PrincipalMappingRules, ReplacementError,
    subject_distinguished_name,
};

/// How often the certificate files are re-read when nothing else is configured.
///
/// A minute is short against any rotation schedule and costs two small file reads.
pub const DEFAULT_RELOAD_INTERVAL: Duration = Duration::from_secs(60);

/// Kafka's `ssl.client.auth`.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum ClientAuth {
    /// No certificate is requested.
    #[default]
    None,
    /// A certificate is requested and verified when presented; a client without one is admitted
    /// anonymously.
    Requested,
    /// A client without a valid certificate fails the handshake.
    Required,
}

impl ClientAuth {
    #[must_use]
    pub const fn is_enabled(self) -> bool {
        !matches!(self, Self::None)
    }

    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::None => "none",
            Self::Requested => "requested",
            Self::Required => "required",
        }
    }
}

impl FromStr for ClientAuth {
    type Err = KafkaProtocolError;

    fn from_str(raw: &str) -> Result<Self> {
        match raw {
            "none" => Ok(Self::None),
            "requested" => Ok(Self::Requested),
            "required" => Ok(Self::Required),
            other => Err(KafkaProtocolError::InvalidConfig(format!(
                "unknown client authentication mode '{other}'; expected none, requested or \
                 required"
            ))),
        }
    }
}

impl fmt::Display for ClientAuth {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.as_str())
    }
}

/// TLS material and policy shared by every `SSL` and `SASL_SSL` listener.
///
/// `client_auth` applies to `SSL` listeners only. A `SASL_SSL` listener never asks for a client
/// certificate, as Kafka leaves `ssl.client.auth` at `none` there unless that listener
/// overrides it.
#[derive(Debug, Clone)]
pub struct TlsConfig {
    /// PEM certificate chain, leaf first.
    pub cert_file: PathBuf,
    /// PEM private key, PKCS#8, PKCS#1 or SEC1.
    pub key_file: PathBuf,
    pub client_auth: ClientAuth,
    /// PEM roots client certificates must chain to. Required unless `client_auth` is `none`.
    pub client_ca_file: Option<PathBuf>,
    /// How a client certificate's subject becomes an Iggy username on an `SSL` listener.
    pub principal_mapping: PrincipalMappingRules,
    /// How often the files are re-read. Zero never re-reads them.
    pub reload_interval: Duration,
}

impl TlsConfig {
    #[must_use]
    pub fn new(cert_file: impl Into<PathBuf>, key_file: impl Into<PathBuf>) -> Self {
        Self {
            cert_file: cert_file.into(),
            key_file: key_file.into(),
            client_auth: ClientAuth::None,
            client_ca_file: None,
            principal_mapping: PrincipalMappingRules::default(),
            reload_interval: DEFAULT_RELOAD_INTERVAL,
        }
    }

    #[must_use]
    pub fn with_client_auth(mut self, mode: ClientAuth, ca_file: impl Into<PathBuf>) -> Self {
        self.client_auth = mode;
        self.client_ca_file = Some(ca_file.into());
        self
    }

    #[must_use]
    pub fn with_principal_mapping(mut self, rules: PrincipalMappingRules) -> Self {
        self.principal_mapping = rules;
        self
    }

    #[must_use]
    pub const fn with_reload_interval(mut self, interval: Duration) -> Self {
        self.reload_interval = interval;
        self
    }
}

/// The bytes last read from disk, kept so a reload can tell whether anything changed.
#[derive(Clone, PartialEq, Eq)]
struct Material {
    certificate: Vec<u8>,
    key: Vec<u8>,
    client_ca: Option<Vec<u8>>,
}

impl Material {
    fn read(config: &TlsConfig) -> Result<Self> {
        Ok(Self {
            certificate: read_file(&config.cert_file)?,
            key: read_file(&config.key_file)?,
            client_ca: config
                .client_ca_file
                .as_deref()
                .map(read_file)
                .transpose()?,
        })
    }

    async fn read_async(config: &TlsConfig) -> Result<Self> {
        let client_ca = match config.client_ca_file.as_deref() {
            Some(path) => Some(read_file_async(path).await?),
            None => None,
        };
        Ok(Self {
            certificate: read_file_async(&config.cert_file).await?,
            key: read_file_async(&config.key_file).await?,
            client_ca,
        })
    }
}

fn read_file(path: &Path) -> Result<Vec<u8>> {
    fs::read(path).map_err(|error| unreadable(path, &error))
}

async fn read_file_async(path: &Path) -> Result<Vec<u8>> {
    tokio::fs::read(path)
        .await
        .map_err(|error| unreadable(path, &error))
}

fn unreadable(path: &Path, error: &std::io::Error) -> KafkaProtocolError {
    KafkaProtocolError::InvalidConfig(format!("cannot read {}: {error}", path.display()))
}

/// The two server configurations one set of material builds: `SSL` listeners verify client
/// certificates as `client_auth` says, `SASL_SSL` listeners never ask for one.
struct ServerConfigs {
    ssl: Arc<ServerConfig>,
    sasl_ssl: Arc<ServerConfig>,
}

impl ServerConfigs {
    fn build(config: &TlsConfig, material: &Material) -> Result<Self> {
        let ssl = Arc::new(build_server_config(config, config.client_auth, material)?);
        let sasl_ssl = if config.client_auth.is_enabled() {
            Arc::new(build_server_config(config, ClientAuth::None, material)?)
        } else {
            Arc::clone(&ssl)
        };
        Ok(Self { ssl, sasl_ssl })
    }
}

/// The live TLS configuration, swapped under running listeners when the files change.
///
/// Every accept takes the configuration current at that moment, so a connection in flight keeps
/// the material it handshook with and the next one gets the new material. Nothing waits on a
/// lock in the accept path: the swap is an atomic pointer exchange.
pub struct TlsState {
    config: TlsConfig,
    current: ArcSwap<ServerConfigs>,
    loaded: Mutex<Material>,
}

impl fmt::Debug for TlsState {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("TlsState")
            .field("config", &self.config)
            .finish_non_exhaustive()
    }
}

impl TlsState {
    /// Reads the files and builds the first configuration.
    ///
    /// # Errors
    ///
    /// Returns `InvalidConfig` when client authentication is on without a CA file, a file
    /// cannot be read, or the material does not build a usable server configuration: no
    /// certificate, an unparsable key, a key that does not match the certificate, or an
    /// empty root store.
    pub fn load(config: TlsConfig) -> Result<Self> {
        if config.client_auth.is_enabled() && config.client_ca_file.is_none() {
            return Err(KafkaProtocolError::InvalidConfig(format!(
                "client authentication is '{}' but no client CA file is configured to verify \
                 certificates against",
                config.client_auth
            )));
        }
        let material = Material::read(&config)?;
        let server_configs = ServerConfigs::build(&config, &material)?;
        Ok(Self {
            config,
            current: ArcSwap::from_pointee(server_configs),
            loaded: Mutex::new(material),
        })
    }

    #[must_use]
    pub const fn config(&self) -> &TlsConfig {
        &self.config
    }

    /// An acceptor over the configuration current right now, for a listener speaking
    /// `protocol`. Only an `SSL` listener asks for a client certificate.
    #[must_use]
    pub fn acceptor(&self, protocol: SecurityProtocol) -> TlsAcceptor {
        let current = self.current.load();
        let config = if protocol.authenticates_by_certificate() {
            &current.ssl
        } else {
            &current.sasl_ssl
        };
        TlsAcceptor::from(Arc::clone(config))
    }

    /// Re-reads the files and swaps the configuration if they changed.
    ///
    /// Returns `Ok(true)` when new material went live. A read or build failure leaves the current
    /// material serving: a rotation that is half written, or a certificate that does not match
    /// its key, must not take the listener down. Material that failed to build is retried on every
    /// call, so each tick reports the failure until the files are fixed.
    ///
    /// # Errors
    ///
    /// The same conditions as [`Self::load`].
    pub async fn reload(&self) -> Result<bool> {
        let material = Material::read_async(&self.config).await?;
        if *self.loaded.lock().unwrap_or_else(PoisonError::into_inner) == material {
            return Ok(false);
        }
        let server_configs = ServerConfigs::build(&self.config, &material)?;
        // Recorded only after a successful build, so a failed build is retried every tick.
        *self.loaded.lock().unwrap_or_else(PoisonError::into_inner) = material;
        self.current.store(Arc::new(server_configs));
        Ok(true)
    }

    /// Reloads on the configured interval until `cancel` fires.
    pub async fn reload_until_cancelled(self: Arc<Self>, cancel: CancellationToken) {
        let interval = self.config.reload_interval;
        if interval.is_zero() {
            return;
        }
        loop {
            tokio::select! {
                () = cancel.cancelled() => return,
                () = tokio::time::sleep(interval) => {}
            }
            match self.reload().await {
                Ok(true) => info!(
                    cert_file = %self.config.cert_file.display(),
                    "TLS certificate reloaded; new connections use the new material"
                ),
                Ok(false) => {}
                // error!, not warn!: an operator who rotated a certificate and sees this is
                // about to serve an expired one, and nothing else will say so.
                Err(error) => error!(
                    %error,
                    cert_file = %self.config.cert_file.display(),
                    "TLS certificate reload failed; the previous material stays in use"
                ),
            }
        }
    }

    /// The Iggy username the client certificate on `connection` names, or `None` when the client
    /// presented no certificate.
    ///
    /// Only meaningful on an `SSL` listener. The chain was already verified by the handshake, so
    /// what is read here is trusted; what can still fail is the subject naming no principal.
    ///
    /// # Errors
    ///
    /// Returns [`IdentityError`] when the leaf does not parse or no mapping rule matches it.
    pub fn principal_name(
        &self,
        connection: &ServerConnection,
    ) -> std::result::Result<Option<String>, IdentityError> {
        let Some(leaf) = connection
            .peer_certificates()
            .and_then(|chain| chain.first())
        else {
            return Ok(None);
        };
        let subject = subject_distinguished_name(leaf)?;
        self.config
            .principal_mapping
            .map(&subject)
            .map(Some)
            .ok_or(IdentityError::NoMatchingRule { subject })
    }
}

fn build_server_config(
    config: &TlsConfig,
    client_auth: ClientAuth,
    material: &Material,
) -> Result<ServerConfig> {
    // Named explicitly rather than taken from the process default: the Iggy SDK installs its own
    // provider lazily on its first TLS dial, and a listener that came up first would otherwise
    // decide for it, or panic because neither had chosen yet.
    let provider = Arc::new(rustls::crypto::aws_lc_rs::default_provider());
    let certificates = pem_certificates(&material.certificate, &config.cert_file)?;
    if certificates.is_empty() {
        return Err(KafkaProtocolError::InvalidConfig(format!(
            "{} holds no certificate",
            config.cert_file.display()
        )));
    }
    let key = PrivateKeyDer::from_pem_slice(&material.key).map_err(|error| {
        KafkaProtocolError::InvalidConfig(format!(
            "{} holds no usable private key: {error}",
            config.key_file.display()
        ))
    })?;
    let builder = ServerConfig::builder_with_provider(Arc::clone(&provider))
        .with_safe_default_protocol_versions()
        .map_err(|error| KafkaProtocolError::InvalidConfig(error.to_string()))?;
    let builder = match (client_auth, &material.client_ca, &config.client_ca_file) {
        (ClientAuth::None, _, _) => builder.with_no_client_auth(),
        (mode, Some(ca_pem), Some(ca_file)) => {
            let mut roots = RootCertStore::empty();
            for certificate in pem_certificates(ca_pem, ca_file)? {
                roots.add(certificate).map_err(|error| {
                    KafkaProtocolError::InvalidConfig(format!(
                        "{} holds a certificate that cannot be a root: {error}",
                        ca_file.display()
                    ))
                })?;
            }
            if roots.is_empty() {
                return Err(KafkaProtocolError::InvalidConfig(format!(
                    "{} holds no certificate",
                    ca_file.display()
                )));
            }
            let mut verifier =
                WebPkiClientVerifier::builder_with_provider(Arc::new(roots), provider);
            if mode == ClientAuth::Requested {
                verifier = verifier.allow_unauthenticated();
            }
            let verifier = verifier
                .build()
                .map_err(|error| KafkaProtocolError::InvalidConfig(error.to_string()))?;
            builder.with_client_cert_verifier(verifier)
        }
        // `load` refuses this combination before any material is read.
        (mode, _, _) => {
            return Err(KafkaProtocolError::InvalidConfig(format!(
                "client authentication is '{mode}' but no client CA file is configured"
            )));
        }
    };
    builder
        .with_single_cert(certificates, key)
        .map_err(|error| {
            KafkaProtocolError::InvalidConfig(format!(
                "{} and {} do not form a usable certificate and key pair: {error}",
                config.cert_file.display(),
                config.key_file.display()
            ))
        })
}

fn pem_certificates(pem: &[u8], path: &Path) -> Result<Vec<CertificateDer<'static>>> {
    CertificateDer::pem_slice_iter(pem)
        .collect::<std::result::Result<Vec<_>, _>>()
        .map_err(|error| {
            KafkaProtocolError::InvalidConfig(format!(
                "{} is not a PEM certificate file: {error}",
                path.display()
            ))
        })
}

#[cfg(test)]
mod tests {
    use rcgen::{CertificateParams, DnType, KeyPair};

    use super::*;

    fn self_signed(common_name: &str) -> (String, String) {
        let key = KeyPair::generate().expect("generate a key");
        let mut params =
            CertificateParams::new(vec!["localhost".to_string()]).expect("valid params");
        params
            .distinguished_name
            .push(DnType::CommonName, common_name);
        let certificate = params.self_signed(&key).expect("self-sign");
        (certificate.pem(), key.serialize_pem())
    }

    fn write(dir: &Path, name: &str, contents: &str) -> PathBuf {
        let path = dir.join(name);
        fs::write(&path, contents).expect("write test file");
        path
    }

    #[test]
    fn client_auth_parses_kafkas_spellings_only() {
        assert_eq!("none".parse::<ClientAuth>().unwrap(), ClientAuth::None);
        assert_eq!(
            "requested".parse::<ClientAuth>().unwrap(),
            ClientAuth::Requested
        );
        assert_eq!(
            "required".parse::<ClientAuth>().unwrap(),
            ClientAuth::Required
        );
        for raw in ["Required", "true", "", "optional"] {
            assert!(
                raw.parse::<ClientAuth>().is_err(),
                "'{raw}' must be rejected"
            );
        }
    }

    #[tokio::test]
    async fn given_a_certificate_and_key_should_load_and_report_no_change_on_reload() {
        let dir = tempfile::tempdir().expect("tempdir");
        let (cert, key) = self_signed("first");
        let cert_file = write(dir.path(), "cert.pem", &cert);
        let key_file = write(dir.path(), "key.pem", &key);
        let state = TlsState::load(TlsConfig::new(cert_file, key_file)).expect("loads");
        assert!(!state.reload().await.expect("reload reads the same files"));
    }

    #[tokio::test]
    async fn given_rotated_files_should_swap_and_keep_the_old_material_on_a_broken_rotation() {
        let dir = tempfile::tempdir().expect("tempdir");
        let (cert, key) = self_signed("first");
        let cert_file = write(dir.path(), "cert.pem", &cert);
        let key_file = write(dir.path(), "key.pem", &key);
        let state = TlsState::load(TlsConfig::new(&cert_file, &key_file)).expect("loads");
        let before = state.current.load_full();

        // The certificate is rewritten first and the key later, as a non-atomic rotation does.
        let (next_cert, next_key) = self_signed("second");
        fs::write(&cert_file, &next_cert).expect("rotate certificate");
        let error = state
            .reload()
            .await
            .expect_err("a certificate that does not match the key must not go live");
        assert!(error.to_string().contains("pair"), "got: {error}");
        assert!(
            Arc::ptr_eq(&before, &state.current.load_full()),
            "the previous material keeps serving"
        );

        fs::write(&key_file, &next_key).expect("rotate key");
        assert!(state.reload().await.expect("the completed rotation loads"));
        assert!(!Arc::ptr_eq(&before, &state.current.load_full()));
        assert!(!state.reload().await.expect("nothing changed since"));
    }

    #[tokio::test]
    async fn given_a_broken_rotation_should_report_it_on_every_reload_until_fixed() {
        let dir = tempfile::tempdir().expect("tempdir");
        let (cert, key) = self_signed("first");
        let cert_file = write(dir.path(), "cert.pem", &cert);
        let key_file = write(dir.path(), "key.pem", &key);
        let state = TlsState::load(TlsConfig::new(&cert_file, &key_file)).expect("loads");

        let (next_cert, next_key) = self_signed("second");
        fs::write(&cert_file, &next_cert).expect("rotate certificate");
        for attempt in 1..=2 {
            let error = state
                .reload()
                .await
                .expect_err("a mismatched pair must fail on every reload, not only the first");
            assert!(
                error.to_string().contains("pair"),
                "attempt {attempt}: {error}"
            );
        }

        fs::write(&key_file, &next_key).expect("rotate key");
        assert!(state.reload().await.expect("the fixed rotation loads"));
    }

    #[test]
    fn given_client_auth_without_a_ca_should_refuse_to_load() {
        let dir = tempfile::tempdir().expect("tempdir");
        let (cert, key) = self_signed("first");
        let cert_file = write(dir.path(), "cert.pem", &cert);
        let key_file = write(dir.path(), "key.pem", &key);
        let mut config = TlsConfig::new(cert_file, key_file);
        config.client_auth = ClientAuth::Required;
        let error = TlsState::load(config).unwrap_err().to_string();
        assert!(error.contains("required"), "got: {error}");
    }

    #[test]
    fn given_unreadable_or_empty_files_should_name_the_file() {
        let dir = tempfile::tempdir().expect("tempdir");
        let (cert, key) = self_signed("first");
        let key_file = write(dir.path(), "key.pem", &key);
        let missing = dir.path().join("missing.pem");
        let error = TlsState::load(TlsConfig::new(&missing, &key_file))
            .unwrap_err()
            .to_string();
        assert!(error.contains("missing.pem"), "got: {error}");

        let empty = write(dir.path(), "empty.pem", "");
        let error = TlsState::load(TlsConfig::new(&empty, &key_file))
            .unwrap_err()
            .to_string();
        assert!(error.contains("empty.pem"), "got: {error}");

        let cert_file = write(dir.path(), "cert.pem", &cert);
        let error = TlsState::load(TlsConfig::new(&cert_file, &empty))
            .unwrap_err()
            .to_string();
        assert!(error.contains("empty.pem"), "got: {error}");
    }
}
