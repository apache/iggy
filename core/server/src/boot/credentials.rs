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
use crate::shell::ShellShard;
use configs::server::ServerConfig;
use consensus::{MetadataHandle, RESERVED_CLIENT_ID};
use futures::{FutureExt, pin_mut, select};
use iggy_binary_protocol::PrepareHeader;
use iggy_binary_protocol::requests::users::CreateUserRequest;
use iggy_binary_protocol::{
    Operation, WireGlobalPermissions, WireName, WireOptions, WirePermissions,
};
use iggy_common::UserStatus;
use iggy_common::defaults::{
    DEFAULT_ROOT_PASSWORD, DEFAULT_ROOT_USERNAME, MAX_PASSWORD_LENGTH, MAX_USERNAME_LENGTH,
    MIN_PASSWORD_LENGTH, MIN_USERNAME_LENGTH,
};
use journal::prepare_journal::find_prepare_header;
use journal::superblock::SuperblockStore;
use journal::{Journal, JournalHandle};
use message_bus::MessageBus;
use message_bus::lifecycle::ShutdownToken;
use message_bus::replica::auth::ReplicaAuth;
use message_bus::replica::handshake::ReplicaTlsCtx;
use message_bus::replica::io as replica_io;
use message_bus::transports::tls::{
    AcceptAnyServerCert, REPLICA_ALPN, TlsServerCredentials, load_ca_pem, load_pem,
    self_signed_for_loopback,
};
use metadata::impls::metadata::{IggySnapshot, StreamsFrontend};
use metadata::impls::recovery::RecoveryError;
use metadata::impls::{METADATA_DIR, SNAPSHOT_FILE_NAME};
use rustls::pki_types::ServerName;
use server_common::crypto;
use std::collections::HashMap;
use std::env;
use std::path::Path;
use std::rc::Rc;
use std::sync::Arc;
use std::time::{Duration, Instant};
use tracing::{info, warn};

const IGGY_ROOT_USERNAME_ENV: &str = "IGGY_ROOT_USERNAME";

const IGGY_ROOT_PASSWORD_ENV: &str = "IGGY_ROOT_PASSWORD";

const ROOT_INITIALIZATION_LOG_INTERVAL: Duration = Duration::from_secs(10);

/// Initial root identity and password hash, resolved before shard startup.
pub(in crate::boot) type RootCredentials = CreateUserRequest;

/// Replicate root creation, waiting through promotions until it commits or shutdown starts.
/// The caller supplies the timer so the simulator can run the same initialization loop.
///
/// # Errors
/// Returns an error if a standalone primary needs credentials or root creation is rejected.
#[allow(clippy::future_not_send, clippy::too_many_lines)]
pub async fn initialize_root_user<B, J, S, SB, Sleep>(
    shard: &Rc<ShellShard<B, J, S, SB>>,
    root_credentials: Option<&CreateUserRequest>,
    shutdown: ShutdownToken,
    sleep: impl Fn(Duration) -> Sleep,
) -> Result<(), ServerError>
where
    B: MessageBus,
    J: JournalHandle,
    J::Target: Journal<Entry = server_common::Message<PrepareHeader>, Header = PrepareHeader>,
    SB: SuperblockStore,
    Sleep: Future<Output = ()>,
{
    let metadata = shard.plane.metadata();
    let mut last_log = None;
    let mut last_error_log = None;
    loop {
        if !metadata
            .mux_stm
            .users()
            .read(|users| users.items.is_empty())
        {
            return Ok(());
        }
        if shutdown.is_triggered() {
            return Ok(());
        }
        let root_in_progress = metadata.consensus.as_ref().is_some_and(|consensus| {
            consensus.pipeline_has_message_from_client(RESERVED_CLIENT_ID)
        });
        let log_due =
            last_log.is_none_or(|last: Instant| last.elapsed() >= ROOT_INITIALIZATION_LOG_INTERVAL);
        if metadata.is_caught_up_primary() && !root_in_progress {
            let Some(credentials) = root_credentials else {
                if metadata
                    .consensus
                    .as_ref()
                    .is_none_or(|consensus| consensus.replica_count() == 1)
                {
                    return Err(ServerError::RootCredentialsRequired {
                        username_env: IGGY_ROOT_USERNAME_ENV,
                        password_env: IGGY_ROOT_PASSWORD_ENV,
                    });
                }
                if log_due {
                    warn!(
                        "Waiting for root initialization without local credentials. \
                         Set both {IGGY_ROOT_USERNAME_ENV} and {IGGY_ROOT_PASSWORD_ENV} \
                         on every replica before the first cluster start."
                    );
                    last_log = Some(Instant::now());
                }
                sleep(Duration::from_millis(50)).await;
                continue;
            };
            let submission = metadata.submit_root_user_in_process(credentials).fuse();
            let shutdown_wait = shutdown.wait().fuse();
            pin_mut!(submission, shutdown_wait);
            let result = loop {
                let log_tick = sleep(ROOT_INITIALIZATION_LOG_INTERVAL).fuse();
                pin_mut!(log_tick);
                select! {
                    result = submission => break result,
                    () = shutdown_wait => return Ok(()),
                    () = log_tick => {
                        warn!("Waiting for root initialization to commit through metadata replication.");
                        last_log = Some(Instant::now());
                    }
                }
            };
            match result {
                Ok(0) => {
                    info!("Root user creation committed through metadata replication.");
                    return Ok(());
                }
                Ok(status) => {
                    return Err(ServerError::RootInitializationRejected { status });
                }
                Err(error) => {
                    if last_error_log.is_none_or(|last: Instant| {
                        last.elapsed() >= ROOT_INITIALIZATION_LOG_INTERVAL
                    }) {
                        warn!(
                            ?error,
                            "Root initialization could not be submitted, retrying."
                        );
                        last_error_log = Some(Instant::now());
                    }
                }
            }
        } else if log_due {
            info!(
                root_in_progress,
                "Waiting for the primary to initialize the root user."
            );
            last_log = Some(Instant::now());
        }
        sleep(Duration::from_millis(50)).await;
    }
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
         well-known development credentials, INSECURE outside development"
    );
}

/// Resolve and hash initial credentials before shard threads start.
pub(in crate::boot) fn resolve_root_credentials(
    config: &ServerConfig,
) -> Result<Option<RootCredentials>, ServerError> {
    let metadata_path = Path::new(&config.path).join(METADATA_DIR);
    let persisted_root = validate_root_journal(&metadata_path)?;
    let is_solo = !config.cluster.enabled || config.cluster.nodes.len() == 1;
    let seed_required = is_solo && !persisted_root;
    let username = env::var(IGGY_ROOT_USERNAME_ENV).ok();
    let password = env::var(IGGY_ROOT_PASSWORD_ENV).ok();

    let credentials =
        validate_root_credentials(seed_required, username.as_deref(), password.as_deref())?;
    credentials
        .map(|(username, password)| {
            let username =
                WireName::new(username).map_err(|_| ServerError::RootCredentialLength {
                    env_name: IGGY_ROOT_USERNAME_ENV,
                    length: username.len(),
                    min: MIN_USERNAME_LENGTH,
                    max: MAX_USERNAME_LENGTH,
                })?;
            Ok(CreateUserRequest {
                username,
                password: crypto::hash_password(password),
                status: UserStatus::Active.as_code(),
                permissions: Some(WirePermissions {
                    global: WireGlobalPermissions {
                        manage_servers: true,
                        read_servers: true,
                        manage_users: true,
                        read_users: true,
                        manage_streams: true,
                        read_streams: true,
                        manage_topics: true,
                        read_topics: true,
                        poll_messages: true,
                        send_messages: true,
                    },
                    streams: Vec::new(),
                }),
                options: WireOptions::empty(),
            })
        })
        .transpose()
}

fn validate_root_journal(metadata_path: &Path) -> Result<bool, ServerError> {
    let snapshot_path = metadata_path.join(SNAPSHOT_FILE_NAME);
    if snapshot_path.exists() {
        let (snapshot, _) = IggySnapshot::load(&snapshot_path)
            .map_err(|error| ServerError::MetadataRecovery(RecoveryError::Snapshot(error)))?;
        if snapshot.snapshot().sequence_number > 0 {
            return Ok(true);
        }
    }
    let path = metadata_path.join("journal.wal");
    let result = find_prepare_header(&path, 1)
        .map_err(|error| ServerError::MetadataRecovery(RecoveryError::Io(error)))?;
    let Some(header) = result.header else {
        if result.has_entries {
            return Err(ServerError::RootBootstrapMissing { path });
        }
        return Ok(false);
    };
    if header.op != 1
        || header.operation != Operation::CreateUser
        || header.client != RESERVED_CLIENT_ID
        || header.user_id != 0
    {
        return Err(ServerError::RootBootstrapNotPersisted {
            path,
            op: header.op,
            operation: header.operation,
            client: header.client,
            user_id: header.user_id,
        });
    }
    Ok(true)
}

fn validate_root_credentials<'a>(
    seed_required: bool,
    username: Option<&'a str>,
    password: Option<&'a str>,
) -> Result<Option<(&'a str, &'a str)>, ServerError> {
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
            )?;
            Ok(Some((username, password)))
        }
        (Some(_), None) => Err(ServerError::RootCredentialsIncomplete {
            provided_env: IGGY_ROOT_USERNAME_ENV,
            missing_env: IGGY_ROOT_PASSWORD_ENV,
        }),
        (None, Some(_)) => Err(ServerError::RootCredentialsIncomplete {
            provided_env: IGGY_ROOT_PASSWORD_ENV,
            missing_env: IGGY_ROOT_USERNAME_ENV,
        }),
        (None, None) if seed_required => Err(ServerError::RootCredentialsRequired {
            username_env: IGGY_ROOT_USERNAME_ENV,
            password_env: IGGY_ROOT_PASSWORD_ENV,
        }),
        (None, None) => Ok(None),
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
    use iggy_binary_protocol::{Command, HEADER_SIZE};

    #[test]
    fn given_reordered_prepares_when_validating_should_find_the_latest_root_op_one() {
        let metadata_path = tempfile::tempdir().unwrap();
        let journal_path = metadata_path.path().join("journal.wal");
        let root = root_prepare_header(1, Operation::CreateUser);
        let other = root_prepare_header(2, Operation::Register);
        for headers in [[root, other], [other, root]] {
            let bytes: Vec<_> = headers
                .iter()
                .flat_map(|header| bytemuck::bytes_of(header).iter().copied())
                .collect();
            std::fs::write(&journal_path, &bytes).unwrap();
            assert!(validate_root_journal(metadata_path.path()).unwrap());
            assert_eq!(std::fs::read(&journal_path).unwrap(), bytes);
        }
        let wrong_root = root_prepare_header(1, Operation::Register);
        for (headers, valid) in [([wrong_root, root], true), ([root, wrong_root], false)] {
            let bytes: Vec<_> = headers
                .iter()
                .flat_map(|header| bytemuck::bytes_of(header).iter().copied())
                .collect();
            std::fs::write(&journal_path, &bytes).unwrap();
            assert_eq!(validate_root_journal(metadata_path.path()).is_ok(), valid);
            assert_eq!(std::fs::read(&journal_path).unwrap(), bytes);
        }
    }

    #[test]
    fn given_complete_prepares_without_op_one_when_validating_should_refuse_before_replay() {
        let metadata_path = tempfile::tempdir().unwrap();
        let journal_path = metadata_path.path().join("journal.wal");
        let header = root_prepare_header(2, Operation::CreateUser);
        let bytes = bytemuck::bytes_of(&header);
        std::fs::write(&journal_path, bytes).unwrap();
        let error = validate_root_journal(metadata_path.path()).unwrap_err();
        assert!(
            matches!(&error, ServerError::RootBootstrapMissing { path } if path == &journal_path)
        );
        let message = error.to_string();
        assert!(message.contains("no validated op 1"));
        assert!(message.contains("at most one replica at a time"));
        assert!(message.contains("others hold quorum"));
        assert_eq!(std::fs::read(&journal_path).unwrap(), bytes);
    }

    #[test]
    fn given_a_journal_without_root_creation_when_bootstrapping_should_refuse_without_modifying_it()
    {
        for sealed in [false, true] {
            let metadata_path = tempfile::tempdir().expect("metadata directory");
            let journal_path = metadata_path.path().join("journal.wal");
            let mut header = PrepareHeader {
                command: Command::Prepare,
                operation: Operation::Register,
                client: 7,
                op: 1,
                size: u32::try_from(HEADER_SIZE).unwrap(),
                ..PrepareHeader::default()
            };
            if sealed {
                header.checksum = header.identity_checksum();
            }
            let bytes = bytemuck::bytes_of(&header);
            std::fs::write(&journal_path, bytes).expect("write an old root baseline journal");

            let error = validate_root_journal(metadata_path.path()).unwrap_err();
            assert!(matches!(
                &error,
                ServerError::RootBootstrapNotPersisted { path, op: 1, operation: Operation::Register, .. }
                    if path == &journal_path
            ));
            let message = error.to_string();
            assert!(message.contains(&journal_path.display().to_string()));
            assert!(message.contains("op 1, operation Register"));
            assert!(message.contains("metadata/snapshot.bin"));
            assert!(message.contains("--fresh"));
            assert!(message.contains("at most one replica at a time"));
            assert!(message.contains("others hold quorum"));
            assert_eq!(
                std::fs::read(journal_path).expect("unchanged journal"),
                bytes
            );
        }
    }

    #[test]
    fn given_an_incomplete_first_entry_when_validating_should_require_fresh_solo_credentials() {
        let metadata_path = tempfile::tempdir().unwrap();
        for bytes in [
            Vec::new(),
            vec![0u8; HEADER_SIZE - 1],
            vec![0u8; HEADER_SIZE],
        ] {
            std::fs::write(metadata_path.path().join("journal.wal"), &bytes).unwrap();
            let persisted_root = validate_root_journal(metadata_path.path()).unwrap();
            assert!(!persisted_root);
            assert!(matches!(
                validate_root_credentials(!persisted_root, None, None),
                Err(ServerError::RootCredentialsRequired { .. })
            ));
        }
    }

    #[compio::test]
    async fn given_a_persisted_root_or_snapshot_when_validating_should_allow_recovery_without_credentials()
     {
        let metadata_path = tempfile::tempdir().unwrap();
        let mut header = PrepareHeader {
            command: Command::Prepare,
            operation: Operation::CreateUser,
            op: 1,
            size: u32::try_from(HEADER_SIZE).unwrap(),
            ..PrepareHeader::default()
        };
        header.checksum = header.identity_checksum();
        std::fs::write(
            metadata_path.path().join("journal.wal"),
            bytemuck::bytes_of(&header),
        )
        .unwrap();
        assert!(validate_root_journal(metadata_path.path()).unwrap());
        IggySnapshot::new(1)
            .persist(&metadata_path.path().join(SNAPSHOT_FILE_NAME))
            .await
            .unwrap();
        std::fs::write(metadata_path.path().join("journal.wal"), [0u8; HEADER_SIZE]).unwrap();
        assert!(validate_root_journal(metadata_path.path()).unwrap());
    }

    #[compio::test]
    async fn given_a_zero_op_checkpoint_when_validating_should_still_require_root_in_the_wal() {
        let metadata_path = tempfile::tempdir().unwrap();
        IggySnapshot::new(0)
            .persist(&metadata_path.path().join(SNAPSHOT_FILE_NAME))
            .await
            .unwrap();
        assert!(!validate_root_journal(metadata_path.path()).unwrap());
        let path = metadata_path.path().join("journal.wal");
        let incomplete = root_prepare_header(2, Operation::Register);
        std::fs::write(&path, bytemuck::bytes_of(&incomplete)).unwrap();
        assert!(matches!(
            validate_root_journal(metadata_path.path()),
            Err(ServerError::RootBootstrapMissing { .. })
        ));
        let root = root_prepare_header(1, Operation::CreateUser);
        std::fs::write(&path, bytemuck::bytes_of(&root)).unwrap();
        assert!(validate_root_journal(metadata_path.path()).unwrap());
    }

    #[test]
    fn given_a_corrupt_checkpoint_when_validating_root_should_refuse_without_modifying_it() {
        let metadata_path = tempfile::tempdir().unwrap();
        let path = metadata_path.path().join(SNAPSHOT_FILE_NAME);
        let bytes = b"corrupt snapshot";
        std::fs::write(&path, bytes).unwrap();
        assert!(matches!(
            validate_root_journal(metadata_path.path()),
            Err(ServerError::MetadataRecovery(RecoveryError::Snapshot(_)))
        ));
        assert_eq!(std::fs::read(&path).unwrap(), bytes);
    }

    fn root_prepare_header(op: u64, operation: Operation) -> PrepareHeader {
        let mut header = PrepareHeader {
            command: Command::Prepare,
            operation,
            op,
            size: u32::try_from(HEADER_SIZE).unwrap(),
            ..PrepareHeader::default()
        };
        header.checksum = header.identity_checksum();
        header
    }

    #[test]
    fn given_fresh_solo_metadata_and_no_root_credentials_when_validating_should_refuse_to_boot() {
        assert!(matches!(
            validate_root_credentials(true, None, None),
            Err(ServerError::RootCredentialsRequired {
                username_env: IGGY_ROOT_USERNAME_ENV,
                password_env: IGGY_ROOT_PASSWORD_ENV,
            })
        ));
    }

    #[test]
    fn given_fresh_solo_metadata_and_both_root_credentials_when_validating_should_return_them() {
        let credentials = validate_root_credentials(true, Some("root"), Some("secret"))
            .expect("both credentials satisfy root initialization");

        assert_eq!(credentials, Some(("root", "secret")));
    }

    #[test]
    fn given_recoverable_metadata_and_no_root_credentials_when_validating_should_boot_without_them()
    {
        let credentials = validate_root_credentials(false, None, None)
            .expect("persisted metadata holds the root user");

        assert_eq!(credentials, None);
    }

    #[test]
    fn given_half_set_root_credentials_when_validating_should_reject_both_directions() {
        for seed_required in [true, false] {
            assert!(matches!(
                validate_root_credentials(seed_required, Some("root"), None),
                Err(ServerError::RootCredentialsIncomplete {
                    provided_env: IGGY_ROOT_USERNAME_ENV,
                    missing_env: IGGY_ROOT_PASSWORD_ENV,
                })
            ));
            assert!(matches!(
                validate_root_credentials(seed_required, None, Some("secret")),
                Err(ServerError::RootCredentialsIncomplete {
                    provided_env: IGGY_ROOT_PASSWORD_ENV,
                    missing_env: IGGY_ROOT_USERNAME_ENV,
                })
            ));
        }
    }

    #[test]
    fn given_out_of_range_root_credentials_when_validating_should_reject_them() {
        assert!(matches!(
            validate_root_credentials(true, Some(""), Some("secret")),
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
