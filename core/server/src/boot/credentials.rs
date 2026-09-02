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

//! Root user credentials and TLS/PSK material loaded at boot.

use crate::boot::topology::TcpTopology;
use crate::server_error::ServerError;
use crate::shell::ServerMuxStateMachine;
use configs::server::ServerConfig;
use iggy_common::defaults::{
    DEFAULT_ROOT_PASSWORD, DEFAULT_ROOT_USERNAME, MAX_PASSWORD_LENGTH, MAX_USERNAME_LENGTH,
    MIN_PASSWORD_LENGTH, MIN_USERNAME_LENGTH,
};
use message_bus::replica::auth::ReplicaAuth;
use message_bus::replica::handshake::ReplicaTlsCtx;
use message_bus::replica::io as replica_io;
use message_bus::transports::tls::{
    AcceptAnyServerCert, REPLICA_ALPN, TlsServerCredentials, load_ca_pem, load_pem,
    self_signed_for_loopback,
};
use metadata::impls::metadata::StreamsFrontend;
use rustls::pki_types::ServerName;
use server_common::crypto;
use std::collections::HashMap;
use std::env;
use std::path::Path;
use std::sync::Arc;
use tracing::{info, warn};

const IGGY_ROOT_USERNAME_ENV: &str = "IGGY_ROOT_USERNAME";

const IGGY_ROOT_PASSWORD_ENV: &str = "IGGY_ROOT_PASSWORD";

pub(in crate::boot) fn ensure_default_root_user(mux_stm: &ServerMuxStateMachine) {
    if !mux_stm.users().read(|users| users.items.is_empty()) {
        return;
    }

    let (username, password_hash) = create_root_credentials();
    mux_stm.users().ensure_root_user(&username, &password_hash);
}

/// Apply `--with-default-root-credentials`.
///
/// Fills in whichever of `IGGY_ROOT_USERNAME_ENV` /
/// `IGGY_ROOT_PASSWORD_ENV` the operator did not export, so the flag is
/// exactly the sugar for setting both by hand and the environment keeps
/// winning over it.
///
/// # Safety
///
/// Mutates the process environment, so the caller must still be
/// single-threaded.
pub unsafe fn apply_default_root_credentials(enabled: bool) {
    if !enabled {
        return;
    }

    let username_set = env::var(IGGY_ROOT_USERNAME_ENV).is_ok();
    let password_set = env::var(IGGY_ROOT_PASSWORD_ENV).is_ok();
    if username_set && password_set {
        warn!(
            "--with-default-root-credentials ignored: {IGGY_ROOT_USERNAME_ENV} and \
             {IGGY_ROOT_PASSWORD_ENV} are already set"
        );
        return;
    }

    // SAFETY: single-threaded caller, per this function's contract.
    unsafe {
        if !username_set {
            env::set_var(IGGY_ROOT_USERNAME_ENV, DEFAULT_ROOT_USERNAME);
        }
        if !password_set {
            env::set_var(IGGY_ROOT_PASSWORD_ENV, DEFAULT_ROOT_PASSWORD);
        }
    }
    warn!(
        "--with-default-root-credentials: a newly created root user will use the \
         well-known development credentials; INSECURE outside development"
    );
}

/// Resolve the root user credentials from `IGGY_ROOT_USERNAME` /
/// `IGGY_ROOT_PASSWORD`, falling back to the default username with a
/// generated password.
///
/// Returns `(username, password_hash)`; the plaintext password never
/// leaves this function.
fn create_root_credentials() -> (String, String) {
    if let Some((username, password)) = root_credentials_from_env() {
        info!("Using the custom root user credentials.");
        return (username, crypto::hash_password(&password));
    }

    info!("Using the default root user credentials...");
    let password = crypto::generate_secret(20..40);
    // Through tracing, not stdout: this is the only time the operator can read
    // the password, so it has to reach the log file too.
    warn!("Generated root user password: {password}");
    (
        DEFAULT_ROOT_USERNAME.to_string(),
        crypto::hash_password(&password),
    )
}

/// The credentials the operator supplied, `None` when neither variable is
/// set. A half-set pair never reaches here: [`validate_root_credentials`]
/// rejects it at boot.
fn root_credentials_from_env() -> Option<(String, String)> {
    match (
        env::var(IGGY_ROOT_USERNAME_ENV),
        env::var(IGGY_ROOT_PASSWORD_ENV),
    ) {
        (Ok(username), Ok(password)) => Some((username, password)),
        _ => None,
    }
}

/// Reject root-credential misconfiguration before any shard thread exists.
///
/// Shard 0 seeds the root user from inside `recover`'s baseline closure,
/// which cannot fail, so every operator-facing check has to run here or it
/// would have to panic a shard thread instead.
pub(in crate::boot) fn validate_root_credentials_env(
    config: &ServerConfig,
) -> Result<(), ServerError> {
    // `recover` creates the metadata directory, so its absence is what tells a
    // first cluster boot (root must come out identical on every replica, hence
    // explicit credentials) apart from a restart that recovers the root user it
    // already stored. `--fresh` has already wiped by this point, so a wiped
    // replica is correctly treated as a first boot.
    let fresh_cluster = config.cluster.enabled
        && !Path::new(&config.system.path)
            .join(metadata::impls::METADATA_DIR)
            .exists();

    validate_root_credentials(
        fresh_cluster,
        env::var(IGGY_ROOT_USERNAME_ENV).ok().as_deref(),
        env::var(IGGY_ROOT_PASSWORD_ENV).ok().as_deref(),
    )
}

fn validate_root_credentials(
    explicit_required: bool,
    username: Option<&str>,
    password: Option<&str>,
) -> Result<(), ServerError> {
    match (username, password) {
        (Some(username), Some(password)) => {
            validate_credential_length(
                IGGY_ROOT_USERNAME_ENV,
                username,
                MIN_USERNAME_LENGTH,
                MAX_USERNAME_LENGTH,
            )?;
            validate_credential_length(
                IGGY_ROOT_PASSWORD_ENV,
                password,
                MIN_PASSWORD_LENGTH,
                MAX_PASSWORD_LENGTH,
            )
        }
        (Some(_), None) => Err(ServerError::RootCredentialsIncomplete {
            provided_env: IGGY_ROOT_USERNAME_ENV,
            missing_env: IGGY_ROOT_PASSWORD_ENV,
        }),
        (None, Some(_)) => Err(ServerError::RootCredentialsIncomplete {
            provided_env: IGGY_ROOT_PASSWORD_ENV,
            missing_env: IGGY_ROOT_USERNAME_ENV,
        }),
        (None, None) if explicit_required => Err(ServerError::ClusterRootCredentialsRequired {
            username_env: IGGY_ROOT_USERNAME_ENV,
            password_env: IGGY_ROOT_PASSWORD_ENV,
        }),
        (None, None) => Ok(()),
    }
}

fn validate_credential_length(
    env_name: &'static str,
    value: &str,
    min: usize,
    max: usize,
) -> Result<(), ServerError> {
    if (min..=max).contains(&value.len()) {
        Ok(())
    } else {
        Err(ServerError::RootCredentialLength {
            env_name,
            length: value.len(),
            min,
            max,
        })
    }
}

/// Build the replica auth context from cluster config. Returns `None` when the
/// cluster or replica auth is disabled, keeping the handshake in legacy mode.
/// Only the derived MAC keys are carried onward in [`ReplicaAuth`]; the raw
/// secrets (masked in config logs via `config_env(secret)`) are read here only
/// to derive them. A non-empty `previous_shared_secret` opens the verify-only
/// rotation acceptance window (see the [`ReplicaAuth`] rustdoc for the rolling
/// rotation procedure). `ClusterConfig::validate` guarantees a non-empty
/// secret whenever both `cluster.enabled` and `cluster.auth.enabled` are set
/// (validate early-returns `Ok` while `cluster.enabled` is false).
pub(in crate::boot) fn load_replica_auth(config: &ServerConfig) -> Option<ReplicaAuth> {
    if !config.cluster.enabled || !config.cluster.auth.enabled {
        return None;
    }
    let auth = ReplicaAuth::new(config.cluster.auth.shared_secret.as_bytes());
    let previous_shared_secret = &config.cluster.auth.previous_shared_secret;
    if previous_shared_secret.is_empty() {
        return Some(auth);
    }
    Some(auth.with_previous_secret(previous_shared_secret.as_bytes()))
}

/// Build the replica TLS context from cluster config. Returns `None` when
/// the cluster or replica TLS is disabled. Every shard calls this once at
/// boot: CA mode re-reads the same PEM files per shard; self-signed mode
/// mints a per-shard throwaway certificate. Neither mode carries client
/// certificates, so TLS authenticates the acceptor only; peer
/// authentication comes from the PSK handshake (`ClusterConfig::validate`
/// enforces `cluster.auth.enabled` whenever `cluster.tls.enabled`).
///
/// Both rustls configs are TLS 1.3 only with the [`REPLICA_ALPN`]
/// protocol pinned. The dialer's SNI / certificate-verify name for each
/// peer is the roster entry's `ip` field (a hostname or IP literal, the
/// same string the connector dials).
pub(in crate::boot) fn load_replica_tls_ctx(
    config: &ServerConfig,
    topology: &TcpTopology,
) -> Result<Option<ReplicaTlsCtx>, ServerError> {
    let tls = &config.cluster.tls;
    if !config.cluster.enabled || !tls.enabled {
        return Ok(None);
    }
    let credential_error = |source: std::io::Error| ServerError::ListenerCredentials {
        transport: "cluster.tls",
        source,
    };

    let credentials = if tls.self_signed {
        warn_ignored_certificate_files("cluster.tls", &tls.cert_file, &tls.key_file);
        let san = config
            .cluster
            .nodes
            .iter()
            .find(|node| node.replica_id == topology.self_replica_id)
            .map(|node| node.ip.as_str())
            .ok_or_else(|| {
                credential_error(std::io::Error::other(format!(
                    "replica id {} not present in cluster.nodes",
                    topology.self_replica_id
                )))
            })?;
        let (cert_chain, key_der) = server_common::generate_self_signed_certificate(san)
            .map_err(|error| credential_error(std::io::Error::other(error.to_string())))?;
        TlsServerCredentials {
            cert_chain,
            key_der,
        }
    } else {
        load_pem(Path::new(&tls.cert_file), Path::new(&tls.key_file)).map_err(credential_error)?
    };

    let mut server =
        rustls::ServerConfig::builder_with_protocol_versions(&[&rustls::version::TLS13])
            .with_no_client_auth()
            .with_single_cert(credentials.cert_chain, credentials.key_der)
            .map_err(|error| {
                credential_error(std::io::Error::other(format!(
                    "replica TLS server config rejected credentials: {error}"
                )))
            })?;
    server.alpn_protocols = vec![REPLICA_ALPN.to_vec()];

    let client_builder =
        rustls::ClientConfig::builder_with_protocol_versions(&[&rustls::version::TLS13]);
    let mut client = if tls.self_signed {
        client_builder
            .dangerous()
            .with_custom_certificate_verifier(Arc::new(AcceptAnyServerCert))
            .with_no_client_auth()
    } else {
        let roots = load_ca_pem(Path::new(&tls.ca_file)).map_err(credential_error)?;
        client_builder
            .with_root_certificates(Arc::new(roots))
            .with_no_client_auth()
    };
    client.alpn_protocols = vec![REPLICA_ALPN.to_vec()];

    // Keyed by replica id, never by roster position: sparse ids (dynamic
    // replica join) would make a positional lookup verify against another
    // peer's SNI name.
    let peer_names = config
        .cluster
        .nodes
        .iter()
        .map(|node| {
            let name = ServerName::try_from(node.ip.clone()).map_err(|error| {
                credential_error(std::io::Error::new(
                    std::io::ErrorKind::InvalidInput,
                    format!(
                        "cluster node '{}' ip '{}' is not a valid TLS server name: {error}",
                        node.name, node.ip
                    ),
                ))
            })?;
            Ok((node.replica_id, name))
        })
        .collect::<Result<HashMap<_, _>, ServerError>>()?;

    Ok(Some(ReplicaTlsCtx {
        server: Arc::new(server),
        client: Arc::new(client),
        peer_names,
    }))
}

pub(in crate::boot) fn load_tcp_tls_server_credentials(
    config: &ServerConfig,
) -> Result<TlsServerCredentials, ServerError> {
    let tls = &config.tcp.tls;
    if ephemeral_certificate("tcp.tls", tls.self_signed, &tls.cert_file) {
        return Ok(self_signed_for_loopback());
    }

    load_pem(Path::new(&tls.cert_file), Path::new(&tls.key_file)).map_err(|source| {
        ServerError::ListenerCredentials {
            transport: "tcp.tls",
            source,
        }
    })
}

pub(in crate::boot) fn load_wss_server_credentials(
    config: &ServerConfig,
) -> Result<TlsServerCredentials, ServerError> {
    let tls = &config.websocket.tls;
    if ephemeral_certificate("websocket.tls", tls.self_signed, &tls.cert_file) {
        return Ok(self_signed_for_loopback());
    }

    load_pem(Path::new(&tls.cert_file), Path::new(&tls.key_file)).map_err(|source| {
        ServerError::ListenerCredentials {
            transport: "websocket.tls",
            source,
        }
    })
}

pub(in crate::boot) fn load_quic_server_credentials(
    config: &ServerConfig,
) -> Result<replica_io::QuicServerCredentials, ServerError> {
    let certificate = &config.quic.certificate;
    if certificate.self_signed {
        warn_ignored_certificate_files(
            "quic.certificate",
            &certificate.cert_file,
            &certificate.key_file,
        );
        let (cert_chain, key_der) = server_common::generate_self_signed_certificate("localhost")
            .map_err(|error| ServerError::ListenerCredentials {
                transport: "quic",
                source: std::io::Error::other(error.to_string()),
            })?;
        return Ok(replica_io::QuicServerCredentials {
            cert_chain,
            key_der,
        });
    }

    let credentials = load_pem(
        Path::new(&certificate.cert_file),
        Path::new(&certificate.key_file),
    )
    .map_err(|source| ServerError::ListenerCredentials {
        transport: "quic",
        source,
    })?;
    Ok(replica_io::QuicServerCredentials {
        cert_chain: credentials.cert_chain,
        key_der: credentials.key_der,
    })
}

/// Client-listener certificate precedence: `self_signed = true` mints an
/// ephemeral loopback certificate only while `cert_file` is absent from disk.
/// An existing PEM pair wins, so a deployment that lays certificates down
/// serves them without also having to unset the flag - the contract every
/// SDK test lane relies on when it points the server at `core/certs/`.
fn ephemeral_certificate(section: &str, self_signed: bool, cert_file: &str) -> bool {
    if !self_signed {
        return false;
    }
    if Path::new(cert_file).exists() {
        info!(
            "{section}.self_signed = true but cert_file = {cert_file} exists on disk; loading it - remove the file or clear the path to serve an ephemeral certificate"
        );
        return false;
    }
    true
}

/// `self_signed = true` never reads the PEM pair (cluster and QUIC keep the
/// flag authoritative: their generated certificates carry non-loopback SANs),
/// so a cert path resolving on disk looks active to an operator who never
/// asked for it.
fn warn_ignored_certificate_files(section: &str, cert_file: &str, key_file: &str) {
    let found: Vec<String> = [("cert_file", cert_file), ("key_file", key_file)]
        .into_iter()
        .filter(|(_, path)| Path::new(path).exists())
        .map(|(field, path)| format!("{field} = {path}"))
        .collect();
    if found.is_empty() {
        return;
    }

    warn!(
        "{section}.self_signed = true, ignoring certificate files found on disk ({}); set {section}.self_signed = false to load them",
        found.join(", ")
    );
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn fresh_cluster_bootstrap_requires_explicit_root_credentials() {
        assert!(matches!(
            validate_root_credentials(true, None, None),
            Err(ServerError::ClusterRootCredentialsRequired {
                username_env: IGGY_ROOT_USERNAME_ENV,
                password_env: IGGY_ROOT_PASSWORD_ENV,
            })
        ));
        validate_root_credentials(true, Some("root"), Some("secret"))
            .expect("both credentials supplied must satisfy the fresh-cluster guard");
    }

    #[test]
    fn single_node_bootstrap_generates_root_credentials_when_unset() {
        validate_root_credentials(false, None, None)
            .expect("a single node mints its own root password");
    }

    #[test]
    fn half_set_root_credentials_are_rejected_in_both_directions() {
        assert!(matches!(
            validate_root_credentials(false, Some("root"), None),
            Err(ServerError::RootCredentialsIncomplete {
                provided_env: IGGY_ROOT_USERNAME_ENV,
                missing_env: IGGY_ROOT_PASSWORD_ENV,
            })
        ));
        assert!(matches!(
            validate_root_credentials(false, None, Some("secret")),
            Err(ServerError::RootCredentialsIncomplete {
                provided_env: IGGY_ROOT_PASSWORD_ENV,
                missing_env: IGGY_ROOT_USERNAME_ENV,
            })
        ));
    }

    #[test]
    fn out_of_range_root_credentials_are_rejected() {
        assert!(matches!(
            validate_root_credentials(false, Some(""), Some("secret")),
            Err(ServerError::RootCredentialLength {
                env_name: IGGY_ROOT_USERNAME_ENV,
                length: 0,
                ..
            })
        ));
        let too_long = "x".repeat(MAX_PASSWORD_LENGTH + 1);
        assert!(matches!(
            validate_root_credentials(false, Some("root"), Some(&too_long)),
            Err(ServerError::RootCredentialLength {
                env_name: IGGY_ROOT_PASSWORD_ENV,
                ..
            })
        ));
    }
}
