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

//! The credentials of the `s3` tiered storage backend and of `s3` geo peers.
//!
//! A node picks the source of each store once, at boot, see
//! [`CredentialSource::select`]. Static keys from the configuration never
//! change. Any other source gets a refresher of [`crate::credential_refresh`].
//! Every shard's store of that bucket reads the same [`SharedCredentials`], so
//! one refresh reaches every shard.
//!
//! The boot waits a few seconds for the first credentials of the backend, and
//! a source that refuses them for good refuses the boot. A peer's refresher
//! never holds up the boot: its failures are logged and fetched again, and the
//! peer's check reports the outage.

use std::time::SystemTime;

use configs::geo::GeoPeerConfig;
use configs::tiered_storage::{TieredStorageBackend, TieredStorageConfig};
use iggy_object_store::s3::credentials::{
    CredentialError, CredentialSource, CredentialTransport, Credentials, SharedCredentials,
};

use tracing::info;

use crate::credential_refresh::{self, Refresh};
use crate::server_error::ServerError;

/// The credentials that every shard's `s3` store signs with, `None` for any
/// other backend.
///
/// # Errors
/// [`ServerError::TieredStorage`] when the node has no source, and
/// [`ServerError::TieredStorageCredentials`] when the source refuses the
/// first fetch for a reason that a retry cannot fix, such as a role that
/// does not trust the token.
pub fn credentials(
    config: &TieredStorageConfig,
    roles: &mut RoleCredentials,
) -> Result<Option<SharedCredentials>, ServerError> {
    credentials_from(config, |name| std::env::var(name).ok(), roles)
}

/// The roles whose credentials a refresher of this node already keeps fresh,
/// each with its cell. A store that takes one of them shares the cell, so a
/// node asks the credential service once per role and not once per store: a
/// node with many peers would otherwise make one exchange per peer for the
/// same answer.
///
/// Two stores share only when their sources are equal in every field, the
/// role and the address of the service included.
#[derive(Default)]
pub struct RoleCredentials(Vec<(CredentialSource, SharedCredentials)>);

impl RoleCredentials {
    fn of(&self, source: &CredentialSource) -> Option<SharedCredentials> {
        self.0
            .iter()
            .find(|(kept, _)| kept == source)
            .map(|(_, shared)| shared.clone())
    }

    fn keep(&mut self, source: CredentialSource, shared: SharedCredentials) {
        self.0.push((source, shared));
    }
}

/// The credentials that every shard's store of the `s3` geo peer `peer`
/// signs with: its static keys, or else the node's web identity or instance
/// role. The boot does not wait for the first credentials of a role. A peer
/// whose role is in `roles` shares its cell and starts no refresher.
///
/// The node, not the peer, calls STS, so web identity uses STS in
/// `node_region`, the region of `[tiered_storage]`, and in the peer's region
/// only when that is empty.
///
/// # Errors
/// [`ServerError::GeoPeer`] when the node has no source, or when the
/// refresher does not start.
pub fn peer_credentials(
    peer: &GeoPeerConfig,
    node_region: &str,
    roles: &mut RoleCredentials,
) -> Result<SharedCredentials, ServerError> {
    peer_credentials_from(
        peer,
        node_region,
        |name| std::env::var(name).ok(),
        roles,
        credential_refresh::start,
    )
}

fn credentials_from(
    config: &TieredStorageConfig,
    env: impl Fn(&str) -> Option<String>,
    roles: &mut RoleCredentials,
) -> Result<Option<SharedCredentials>, ServerError> {
    if config.backend != TieredStorageBackend::S3 {
        return Ok(None);
    }
    let static_keys = (!config.access_key_id.is_empty()).then(|| Credentials {
        access_key_id: config.access_key_id.clone(),
        secret_access_key: config.secret_access_key.clone(),
        session_token: None,
    });
    let source = CredentialSource::select(static_keys, &config.region, env).ok_or_else(|| {
        ServerError::TieredStorage {
            reason: "the s3 backend has no credentials. Set tiered_storage.access_key_id and \
                     tiered_storage.secret_access_key, run with web identity \
                     (AWS_WEB_IDENTITY_TOKEN_FILE and AWS_ROLE_ARN), or unset \
                     AWS_EC2_METADATA_DISABLED to use the role of the instance"
                .to_owned(),
        }
    })?;
    if let CredentialSource::Static(keys) = source {
        return Ok(Some(SharedCredentials::fixed(keys)));
    }
    let shared = SharedCredentials::refreshed();
    credential_refresh::start_and_wait(S3Refresh::backend(source.clone(), shared.clone()))?;
    roles.keep(source, shared.clone());
    Ok(Some(shared))
}

/// `start` starts the refresher of a role that no store of the node takes
/// yet, without waiting for its first fetch.
fn peer_credentials_from(
    peer: &GeoPeerConfig,
    node_region: &str,
    env: impl Fn(&str) -> Option<String>,
    roles: &mut RoleCredentials,
    start: impl FnOnce(S3Refresh) -> Result<(), ServerError>,
) -> Result<SharedCredentials, ServerError> {
    let refuse = |reason: String| ServerError::GeoPeer {
        origin: peer.origin.clone(),
        reason,
    };
    let source = peer_source(peer, node_region, env).ok_or_else(|| {
        refuse(
            "the s3 peer has no credentials. Set its access_key_id and secret_access_key, run \
             with web identity (AWS_WEB_IDENTITY_TOKEN_FILE and AWS_ROLE_ARN), or unset \
             AWS_EC2_METADATA_DISABLED to use the role of the instance"
                .to_owned(),
        )
    })?;
    if let CredentialSource::Static(keys) = source {
        return Ok(SharedCredentials::fixed(keys));
    }
    if let Some(shared) = roles.of(&source) {
        info!(
            source = source.name(),
            "geo peer {:?} signs with credentials that the node already keeps fresh", peer.origin
        );
        return Ok(shared);
    }
    let shared = SharedCredentials::refreshed();
    start(S3Refresh::peer(
        &peer.origin,
        source.clone(),
        shared.clone(),
    ))
    .map_err(|error| refuse(error.to_string()))?;
    roles.keep(source, shared.clone());
    Ok(shared)
}

/// The first source of `peer`, `None` when the node has none.
fn peer_source(
    peer: &GeoPeerConfig,
    node_region: &str,
    env: impl Fn(&str) -> Option<String>,
) -> Option<CredentialSource> {
    let static_keys = (!peer.access_key_id.is_empty()).then(|| Credentials {
        access_key_id: peer.access_key_id.clone(),
        secret_access_key: peer.secret_access_key.clone(),
        session_token: None,
    });
    let sts_region = match node_region.trim() {
        "" => peer.region.trim(),
        node_region => node_region,
    };
    CredentialSource::select(static_keys, sts_region, env)
}

/// The refresher of one bucket: its source, and the credentials that every
/// shard signs with.
struct S3Refresh {
    subject: String,
    source: CredentialSource,
    shared: SharedCredentials,
}

impl S3Refresh {
    fn backend(source: CredentialSource, shared: SharedCredentials) -> Self {
        Self {
            subject: "the s3 backend".to_owned(),
            source,
            shared,
        }
    }

    fn peer(origin: &str, source: CredentialSource, shared: SharedCredentials) -> Self {
        Self {
            subject: format!("geo peer {origin:?}"),
            source,
            shared,
        }
    }
}

impl Refresh for S3Refresh {
    fn subject(&self) -> &str {
        &self.subject
    }

    fn source_name(&self) -> &'static str {
        self.source.name()
    }

    fn endpoint(&self) -> &str {
        self.source.endpoint().unwrap_or_default()
    }

    async fn fetch(
        &self,
        transport: &impl CredentialTransport,
    ) -> Result<Option<SystemTime>, CredentialError> {
        let fetched = self.source.fetch(transport).await?;
        self.shared.replace(fetched.credentials);
        Ok(fetched.expires_at)
    }
}

#[cfg(test)]
mod tests {
    use std::cell::{Cell, RefCell};
    use std::collections::VecDeque;
    use std::future::{Future, ready};
    use std::path::PathBuf;

    use std::sync::mpsc;
    use std::time::Duration;

    use bytes::Bytes;
    use iggy_object_store::s3::credentials::{
        CredentialRequest, CredentialResponse, InstanceMetadata, WebIdentity,
    };

    use super::*;
    use crate::credential_refresh::{RetryDelays, keep_fresh, rfc3339};

    /// Answers each request with the next of its answers.
    struct ScriptedTransport {
        answers: RefCell<VecDeque<Result<(u16, String), CredentialError>>>,
    }

    impl ScriptedTransport {
        fn new(answers: impl IntoIterator<Item = Result<(u16, String), CredentialError>>) -> Self {
            Self {
                answers: RefCell::new(answers.into_iter().collect()),
            }
        }
    }

    impl CredentialTransport for ScriptedTransport {
        fn send(
            &self,
            _request: CredentialRequest,
        ) -> impl Future<Output = Result<CredentialResponse, CredentialError>> {
            let answer =
                self.answers.borrow_mut().pop_front().unwrap_or_else(|| {
                    Err(CredentialError::Unreachable("no more answers".to_owned()))
                });
            ready(answer.map(|(status, body)| CredentialResponse {
                status,
                body: Bytes::from(body),
            }))
        }
    }

    /// The three answers of the instance metadata service for one fetch,
    /// with credentials `key` that expire at `expires_at`.
    fn metadata_answers(
        key: &str,
        expires_at: SystemTime,
    ) -> [Result<(u16, String), CredentialError>; 3] {
        [
            Ok((200, "token".to_owned())),
            Ok((200, "role".to_owned())),
            Ok((
                200,
                format!(
                    r#"{{"Code":"Success","AccessKeyId":"{key}","SecretAccessKey":"s","Token":"t","Expiration":"{}"}}"#,
                    rfc3339(expires_at)
                ),
            )),
        ]
    }

    fn metadata() -> CredentialSource {
        CredentialSource::InstanceMetadata(InstanceMetadata {
            endpoint: "http://169.254.169.254".to_owned(),
        })
    }

    const FAST: RetryDelays = RetryDelays {
        first: Duration::from_millis(10),
        max: Duration::from_millis(40),
    };

    fn key_of(shared: &SharedCredentials) -> Option<String> {
        shared.current().map(|keys| keys.access_key_id.clone())
    }

    /// Credentials that expire within a second are fetched again a second
    /// later, before they expire, and every handle sees the new ones.
    #[test]
    fn given_credentials_that_expire_soon_when_keeping_fresh_should_replace_them_before_they_expire()
     {
        let soon = SystemTime::now() + Duration::from_secs(2);
        let later = SystemTime::now() + Duration::from_hours(1);
        let transport = ScriptedTransport::new(
            metadata_answers("first", soon)
                .into_iter()
                .chain(metadata_answers("second", later)),
        );
        let node = SharedCredentials::refreshed();
        let shard = node.clone();
        let source = metadata();
        let (first, first_fetch) = mpsc::sync_channel(1);

        compio::runtime::Runtime::new().unwrap().block_on(async {
            let refresh = S3Refresh::backend(source, node.clone());
            let refresher = keep_fresh(&refresh, &transport, Some(first), FAST);
            let _ = compio::time::timeout(Duration::from_millis(1500), refresher).await;
        });

        assert!(matches!(first_fetch.try_recv(), Ok(Ok(Some(_)))));
        assert_eq!(key_of(&shard).as_deref(), Some("second"));
        assert!(
            SystemTime::now() < soon,
            "the refresh came before the expiry"
        );
    }

    #[test]
    fn given_failing_fetches_when_keeping_fresh_should_retry_and_report_the_first_failure() {
        let later = SystemTime::now() + Duration::from_hours(1);
        let transport = ScriptedTransport::new(
            [
                Err(CredentialError::Unreachable("refused".to_owned())),
                Ok((503, String::new())),
            ]
            .into_iter()
            .chain(metadata_answers("recovered", later)),
        );
        let shared = SharedCredentials::refreshed();
        let source = metadata();
        let (first, first_fetch) = mpsc::sync_channel(1);

        compio::runtime::Runtime::new().unwrap().block_on(async {
            let refresh = S3Refresh::backend(source, shared.clone());
            let refresher = keep_fresh(&refresh, &transport, Some(first), FAST);
            let _ = compio::time::timeout(Duration::from_millis(300), refresher).await;
        });

        assert!(matches!(
            first_fetch.try_recv(),
            Ok(Err(CredentialError::Unreachable(_)))
        ));
        assert_eq!(key_of(&shared).as_deref(), Some("recovered"));
    }

    #[test]
    fn given_a_refused_first_fetch_when_keeping_fresh_should_report_it_and_stop() {
        let transport = ScriptedTransport::new([Ok((403, String::new()))]);
        let shared = SharedCredentials::refreshed();
        let (first, first_fetch) = mpsc::sync_channel(1);

        let refresh = S3Refresh::backend(metadata(), shared.clone());
        compio::runtime::Runtime::new()
            .unwrap()
            .block_on(keep_fresh(&refresh, &transport, Some(first), FAST));

        assert!(matches!(
            first_fetch.try_recv(),
            Ok(Err(CredentialError::Refused(_)))
        ));
        assert!(shared.current().is_none());
    }

    fn s3_section(access_key_id: &str) -> TieredStorageConfig {
        TieredStorageConfig {
            backend: TieredStorageBackend::S3,
            bucket: "segments".to_owned(),
            region: "eu-central-1".to_owned(),
            access_key_id: access_key_id.to_owned(),
            secret_access_key: if access_key_id.is_empty() {
                String::new()
            } else {
                "secret".to_owned()
            },
            ..TieredStorageConfig::default()
        }
    }

    #[test]
    fn given_static_keys_when_picking_credentials_should_fix_them_without_a_refresher() {
        let shared = credentials_from(&s3_section("AKIDEXAMPLE"), |_| None, &mut roles())
            .unwrap()
            .unwrap();

        assert!(!shared.refreshes());
        assert_eq!(key_of(&shared).as_deref(), Some("AKIDEXAMPLE"));
        assert!(
            credentials_from(&TieredStorageConfig::default(), |_| None, &mut roles())
                .unwrap()
                .is_none()
        );
    }

    #[test]
    fn given_no_source_when_picking_credentials_should_refuse_the_boot() {
        let result = credentials_from(
            &s3_section(""),
            |name| (name == "AWS_EC2_METADATA_DISABLED").then(|| "true".to_owned()),
            &mut roles(),
        );

        assert!(
            matches!(result, Err(ServerError::TieredStorage { .. })),
            "{result:?}"
        );
    }

    #[test]
    fn given_a_missing_token_file_when_picking_credentials_should_refuse_the_boot() {
        let directory = tempfile::tempdir().unwrap();
        let token_file: PathBuf = directory.path().join("token");
        let token_file_text = token_file.to_string_lossy().into_owned();

        let result = credentials_from(
            &s3_section(""),
            move |name| match name {
                "AWS_WEB_IDENTITY_TOKEN_FILE" => Some(token_file_text.clone()),
                "AWS_ROLE_ARN" => Some("arn:aws:iam::123456789012:role/iggy".to_owned()),
                "AWS_ENDPOINT_URL_STS" => Some("http://127.0.0.1:1".to_owned()),
                _ => None,
            },
            &mut roles(),
        );

        assert!(
            matches!(
                result,
                Err(ServerError::TieredStorageCredentials {
                    source: CredentialError::TokenFile { .. },
                    ..
                })
            ),
            "{result:?}"
        );
    }

    const ROLE_ARN: &str = "arn:aws:iam::123456789012:role/iggy";

    fn s3_peer(access_key_id: &str) -> GeoPeerConfig {
        GeoPeerConfig {
            origin: "dc-b".to_owned(),
            backend: TieredStorageBackend::S3,
            bucket: "iggy-dc-b".to_owned(),
            region: "us-east-1".to_owned(),
            access_key_id: access_key_id.to_owned(),
            secret_access_key: if access_key_id.is_empty() {
                String::new()
            } else {
                "peerSecretKeyExample".to_owned()
            },
            ..GeoPeerConfig::default()
        }
    }

    /// The environment of `pairs`.
    fn env_of(pairs: &[(&str, &str)]) -> impl Fn(&str) -> Option<String> + use<> {
        let pairs = pairs
            .iter()
            .map(|(name, value)| ((*name).to_owned(), (*value).to_owned()))
            .collect::<Vec<_>>();
        move |name| {
            pairs
                .iter()
                .find(|(key, _)| key == name)
                .map(|(_, value)| value.clone())
        }
    }

    fn web_identity_env(extra: &[(&str, &str)]) -> impl Fn(&str) -> Option<String> + use<> {
        let mut pairs = vec![
            ("AWS_WEB_IDENTITY_TOKEN_FILE", "/var/run/token"),
            ("AWS_ROLE_ARN", ROLE_ARN),
        ];
        pairs.extend_from_slice(extra);
        env_of(&pairs)
    }

    fn roles() -> RoleCredentials {
        RoleCredentials::default()
    }

    /// For a peer that must not get a refresher. It takes the refresher as
    /// the starter does.
    #[allow(clippy::needless_pass_by_value)]
    fn no_refresher(refresh: S3Refresh) -> Result<(), ServerError> {
        panic!("a refresher started for {}", refresh.subject());
    }

    #[test]
    fn given_static_keys_when_picking_a_peers_credentials_should_fix_them_without_a_refresher() {
        let shared = peer_credentials_from(
            &s3_peer("AKIDPEEREXAMPLE"),
            "eu-central-1",
            web_identity_env(&[]),
            &mut roles(),
            no_refresher,
        )
        .unwrap();

        assert!(!shared.refreshes());
        let keys = shared.current().unwrap();
        assert_eq!(keys.access_key_id, "AKIDPEEREXAMPLE");
        assert_eq!(keys.secret_access_key, "peerSecretKeyExample");
        assert_eq!(keys.session_token, None);
    }

    #[test]
    fn given_a_peer_without_keys_when_selecting_should_take_web_identity_then_instance_metadata() {
        let web_identity = peer_source(&s3_peer(""), "eu-central-1", web_identity_env(&[]));
        let instance = peer_source(&s3_peer(""), "eu-central-1", env_of(&[]));
        let half_web_identity = peer_source(
            &s3_peer(""),
            "eu-central-1",
            env_of(&[("AWS_WEB_IDENTITY_TOKEN_FILE", "/var/run/token")]),
        );

        assert_eq!(
            web_identity,
            Some(CredentialSource::WebIdentity(WebIdentity {
                token_file: PathBuf::from("/var/run/token"),
                role_arn: ROLE_ARN.to_owned(),
                session_name: "iggy-tiered-storage".to_owned(),
                endpoint: "https://sts.eu-central-1.amazonaws.com".to_owned(),
            }))
        );
        assert_eq!(instance, Some(metadata()));
        assert_eq!(half_web_identity, Some(metadata()));
    }

    #[test]
    fn given_no_source_when_picking_a_peers_credentials_should_refuse_naming_the_peer() {
        let result = peer_credentials_from(
            &s3_peer(""),
            "eu-central-1",
            env_of(&[("AWS_EC2_METADATA_DISABLED", "true")]),
            &mut roles(),
            no_refresher,
        );

        let Err(ServerError::GeoPeer { origin, reason }) = result else {
            panic!("{result:?}");
        };
        assert_eq!(origin, "dc-b");
        for named in [
            "access_key_id",
            "AWS_WEB_IDENTITY_TOKEN_FILE",
            "AWS_ROLE_ARN",
            "AWS_EC2_METADATA_DISABLED",
        ] {
            assert!(reason.contains(named), "{reason}");
        }
    }

    /// The node calls STS, so it calls STS of its own region, and of the
    /// peer's only when the node has no region. `AWS_ENDPOINT_URL_STS` wins
    /// over both.
    #[test]
    fn given_web_identity_when_selecting_for_a_peer_should_call_sts_of_the_node_region() {
        let sts = |node_region: &str, extra: &[(&str, &str)]| {
            peer_source(&s3_peer(""), node_region, web_identity_env(extra))
                .as_ref()
                .and_then(CredentialSource::endpoint)
                .map(str::to_owned)
        };

        assert_eq!(
            sts("eu-central-1", &[]).as_deref(),
            Some("https://sts.eu-central-1.amazonaws.com")
        );
        assert_eq!(
            sts(" ", &[]).as_deref(),
            Some("https://sts.us-east-1.amazonaws.com")
        );
        assert_eq!(
            sts(
                "eu-central-1",
                &[("AWS_ENDPOINT_URL_STS", "http://127.0.0.1:4566")]
            )
            .as_deref(),
            Some("http://127.0.0.1:4566")
        );
    }

    /// The boot does not wait for a peer's first credentials: the peer's role
    /// goes to a refresher, and the cell is empty until that fetches.
    #[test]
    fn given_a_peer_role_when_picking_its_credentials_should_start_a_refresher_and_not_wait() {
        let started = RefCell::new(Vec::new());

        let shared = peer_credentials_from(
            &s3_peer(""),
            "eu-central-1",
            web_identity_env(&[]),
            &mut roles(),
            |refresh| {
                started.borrow_mut().push(refresh.subject().to_owned());
                Ok(())
            },
        )
        .unwrap();

        assert!(shared.refreshes());
        assert!(shared.current().is_none());
        assert_eq!(*started.borrow(), [r#"geo peer "dc-b""#]);
    }

    #[test]
    fn given_a_refresher_that_does_not_start_when_picking_a_peers_credentials_should_refuse_naming_the_peer()
     {
        let mut roles = roles();

        let result = peer_credentials_from(
            &s3_peer(""),
            "eu-central-1",
            web_identity_env(&[]),
            &mut roles,
            |_| {
                Err(ServerError::TieredStorage {
                    reason: "no runtime".to_owned(),
                })
            },
        );

        let Err(ServerError::GeoPeer { origin, reason }) = result else {
            panic!("a peer without a refresher was accepted");
        };
        assert_eq!(origin, "dc-b");
        assert!(reason.contains("no runtime"), "{reason}");
        assert!(roles.0.is_empty(), "a role without a refresher was kept");
    }

    /// Stores that take the same role share one refresher and one cell, so
    /// a node with many peers asks the credential service once per role.
    #[test]
    fn given_peers_and_a_backend_of_one_role_when_picking_credentials_should_share_one_refresher() {
        let mut roles = roles();
        let started = Cell::new(0);
        let start = |_: S3Refresh| {
            started.set(started.get() + 1);
            Ok(())
        };
        let pick = |origin: &str, roles: &mut RoleCredentials| {
            let peer = GeoPeerConfig {
                origin: origin.to_owned(),
                ..s3_peer("")
            };
            peer_credentials_from(&peer, "eu-central-1", web_identity_env(&[]), roles, start)
                .unwrap()
        };

        let first = pick("dc-b", &mut roles);
        let second = pick("dc-c", &mut roles);
        first.replace(Credentials {
            access_key_id: "ASIASHARED".to_owned(),
            secret_access_key: "secret".to_owned(),
            session_token: Some("token".to_owned()),
        });

        assert_eq!(started.get(), 1);
        assert_eq!(second.current().unwrap().access_key_id, "ASIASHARED");

        // The backend's refresher keeps the same role fresh.
        let backend = SharedCredentials::refreshed();
        let mut roles = RoleCredentials::default();
        let source = peer_source(&s3_peer(""), "eu-central-1", web_identity_env(&[])).unwrap();
        roles.keep(source, backend.clone());
        let peer = pick("dc-b", &mut roles);
        backend.replace(Credentials {
            access_key_id: "ASIABACKEND".to_owned(),
            secret_access_key: "secret".to_owned(),
            session_token: None,
        });

        assert_eq!(started.get(), 1, "the peer started a refresher of its own");
        assert_eq!(peer.current().unwrap().access_key_id, "ASIABACKEND");
    }

    /// Another role, or the same role at another address of the service, is
    /// another source, with a refresher and a cell of its own.
    #[test]
    fn given_peers_of_different_sources_when_picking_credentials_should_keep_them_apart() {
        let mut roles = roles();
        let started = Cell::new(0);
        let start = |_: S3Refresh| {
            started.set(started.get() + 1);
            Ok(())
        };
        let other_role = web_identity_env(&[("AWS_ROLE_SESSION_NAME", "other")]);
        let other_service = web_identity_env(&[("AWS_ENDPOINT_URL_STS", "http://127.0.0.1:1")]);

        let first = peer_credentials_from(
            &s3_peer(""),
            "eu-central-1",
            web_identity_env(&[]),
            &mut roles,
            start,
        )
        .unwrap();
        let second =
            peer_credentials_from(&s3_peer(""), "eu-central-1", other_role, &mut roles, start)
                .unwrap();
        let third = peer_credentials_from(
            &s3_peer(""),
            "eu-central-1",
            other_service,
            &mut roles,
            start,
        )
        .unwrap();
        first.replace(Credentials {
            access_key_id: "ASIAFIRST".to_owned(),
            secret_access_key: "secret".to_owned(),
            session_token: None,
        });

        assert_eq!(started.get(), 3);
        assert!(second.current().is_none());
        assert!(third.current().is_none());
    }

    #[test]
    fn given_a_peer_refresher_when_its_fetch_succeeds_should_fill_the_shared_credentials() {
        let later = SystemTime::now() + Duration::from_hours(1);
        let transport = ScriptedTransport::new(metadata_answers("ASIAPEER", later));
        let node = SharedCredentials::refreshed();
        let shard = node.clone();
        let refresh = S3Refresh::peer("dc-b", metadata(), node);

        compio::runtime::Runtime::new().unwrap().block_on(async {
            let refresher = keep_fresh(&refresh, &transport, None, FAST);
            let _ = compio::time::timeout(Duration::from_millis(300), refresher).await;
        });

        assert_eq!(refresh.subject(), r#"geo peer "dc-b""#);
        let keys = shard.current().unwrap();
        assert_eq!(keys.access_key_id, "ASIAPEER");
        assert_eq!(keys.session_token.as_deref(), Some("t"));
    }

    /// Without a waiting boot, a refused fetch is fetched again, so a peer
    /// whose role is fixed later starts reading then.
    #[test]
    fn given_a_refused_fetch_of_a_peer_when_keeping_fresh_should_fetch_again() {
        let later = SystemTime::now() + Duration::from_hours(1);
        let transport = ScriptedTransport::new(
            std::iter::once(Ok((403, String::new()))).chain(metadata_answers("granted", later)),
        );
        let shared = SharedCredentials::refreshed();
        let refresh = S3Refresh::peer("dc-b", metadata(), shared.clone());

        compio::runtime::Runtime::new().unwrap().block_on(async {
            let refresher = keep_fresh(&refresh, &transport, None, FAST);
            let _ = compio::time::timeout(Duration::from_millis(300), refresher).await;
        });

        assert_eq!(key_of(&shared).as_deref(), Some("granted"));
    }
}
