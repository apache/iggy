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

//! The access tokens of the `gcs` tiered storage backend and of `gcs` geo
//! peers.
//!
//! A node picks the source of each `[tiered_storage.gcs]` or
//! `[geo.peers.gcs]` table once, at boot, see [`TokenSource::select`], and a
//! refresher of [`crate::credential_refresh`] keeps its token fresh. Every
//! shard's store of that bucket reads the same [`SharedToken`].
//!
//! The boot waits a few seconds for the first token of the backend, and a
//! source that refuses it for good refuses the boot. A peer's refresher
//! never holds up the boot: its failures are logged and fetched again, and
//! the peer's check reports the outage.

use std::path::Path;
use std::time::SystemTime;

use configs::tiered_storage::{GcsAuth as GcsAuthSetting, GcsConfig as GcsSection};
use iggy_object_store::gcs::credentials::{
    CredentialError, CredentialTransport, GcsAuth, KeyFileError, SharedToken, TokenSource,
};

use crate::credential_refresh::{self, Refresh};
use crate::server_error::ServerError;

/// The refresher of one bucket: its source, and the token that every shard
/// sends.
struct GcsRefresh {
    subject: String,
    source: TokenSource,
    shared: SharedToken,
}

/// How the stores of the `gcs` backend authorize their requests. The boot
/// waits for the first token.
///
/// # Errors
/// [`ServerError::TieredStorageGcsKeyFile`] for a key file that cannot sign,
/// and what [`credential_refresh::start_and_wait`] returns.
pub fn backend_auth(section: &GcsSection) -> Result<GcsAuth, ServerError> {
    let Some(source) = select(section, |name| std::env::var(name).ok(), read)
        .map_err(|source| ServerError::TieredStorageGcsKeyFile { source })?
    else {
        return Ok(GcsAuth::Anonymous);
    };
    let shared = SharedToken::new();
    credential_refresh::start_and_wait(GcsRefresh {
        subject: "the gcs backend".to_owned(),
        source,
        shared: shared.clone(),
    })?;
    Ok(GcsAuth::Token(shared))
}

/// How the stores of the `gcs` peer `origin` authorize their requests. The
/// boot does not wait for the first token.
///
/// # Errors
/// [`ServerError::GeoPeer`] for a key file that cannot sign or a refresher
/// that does not start.
pub fn peer_auth(origin: &str, section: &GcsSection) -> Result<GcsAuth, ServerError> {
    let refuse = |reason: String| ServerError::GeoPeer {
        origin: origin.to_owned(),
        reason,
    };
    let Some(source) = select(section, |name| std::env::var(name).ok(), read)
        .map_err(|error| refuse(error.to_string()))?
    else {
        return Ok(GcsAuth::Anonymous);
    };
    let shared = SharedToken::new();
    credential_refresh::start(GcsRefresh {
        subject: format!("geo peer {origin:?}"),
        source,
        shared: shared.clone(),
    })
    .map_err(|error| refuse(error.to_string()))?;
    Ok(GcsAuth::Token(shared))
}

/// The source of `section`, `None` when it sends no credentials. `env` reads
/// a variable of the process environment, and `read` reads a key file.
fn select(
    section: &GcsSection,
    env: impl Fn(&str) -> Option<String>,
    read: impl Fn(&Path) -> std::io::Result<String>,
) -> Result<Option<TokenSource>, KeyFileError> {
    match section.auth {
        GcsAuthSetting::None => Ok(None),
        GcsAuthSetting::Auto => {
            let key_file = section.service_account_key.trim();
            let key_file = (!key_file.is_empty()).then(|| Path::new(key_file));
            TokenSource::select(key_file, env, read).map(Some)
        }
    }
}

fn read(path: &Path) -> std::io::Result<String> {
    std::fs::read_to_string(path)
}

impl Refresh for GcsRefresh {
    fn subject(&self) -> &str {
        &self.subject
    }

    fn source_name(&self) -> &'static str {
        self.source.name()
    }

    fn endpoint(&self) -> &str {
        self.source.endpoint()
    }

    async fn fetch(
        &self,
        transport: &impl CredentialTransport,
    ) -> Result<Option<SystemTime>, CredentialError> {
        let fetched = self.source.fetch(transport).await?;
        self.shared.replace(fetched.token);
        Ok(Some(fetched.expires_at))
    }
}

#[cfg(test)]
mod tests {
    use std::cell::RefCell;
    use std::collections::VecDeque;
    use std::future::{Future, ready};
    use std::sync::mpsc;
    use std::time::Duration;

    use bytes::Bytes;
    use iggy_object_store::gcs::credentials::{
        CredentialRequest, CredentialResponse, METADATA_HOST_ENV, MetadataServer,
    };

    use super::*;
    use crate::credential_refresh::{RetryDelays, keep_fresh};

    const FAST: RetryDelays = RetryDelays {
        first: Duration::from_millis(10),
        max: Duration::from_millis(40),
    };

    /// Answers each request with the next of its answers.
    struct ScriptedTransport {
        answers: RefCell<VecDeque<(u16, String)>>,
    }

    impl CredentialTransport for ScriptedTransport {
        fn send(
            &self,
            _request: CredentialRequest,
        ) -> impl Future<Output = Result<CredentialResponse, CredentialError>> {
            let answer = self.answers.borrow_mut().pop_front().map_or_else(
                || Err(CredentialError::Unreachable("no more answers".to_owned())),
                |(status, body)| {
                    Ok(CredentialResponse {
                        status,
                        body: Bytes::from(body),
                    })
                },
            );
            ready(answer)
        }
    }

    fn token_answer(token: &str, expires_in: u64) -> (u16, String) {
        (
            200,
            format!(
                r#"{{"access_token":"{token}","expires_in":{expires_in},"token_type":"Bearer"}}"#
            ),
        )
    }

    fn section(auth: GcsAuthSetting, service_account_key: &str) -> GcsSection {
        GcsSection {
            bucket: "segments".to_owned(),
            auth,
            service_account_key: service_account_key.to_owned(),
            ..GcsSection::default()
        }
    }

    fn missing(path: &Path) -> std::io::Result<String> {
        Err(std::io::Error::new(
            std::io::ErrorKind::NotFound,
            path.display().to_string(),
        ))
    }

    #[test]
    fn given_auth_settings_when_selecting_should_send_none_or_follow_the_default_order() {
        let host = |name: &str| (name == METADATA_HOST_ENV).then(|| "127.0.0.1:9".to_owned());

        let anonymous = select(&section(GcsAuthSetting::None, "/key.json"), host, missing);
        let metadata = select(&section(GcsAuthSetting::Auto, " "), host, missing);
        let key_file = select(&section(GcsAuthSetting::Auto, "/key.json"), host, missing);

        assert!(matches!(anonymous, Ok(None)), "{anonymous:?}");
        assert_eq!(
            metadata.unwrap(),
            Some(TokenSource::MetadataServer(MetadataServer {
                endpoint: "http://127.0.0.1:9".to_owned(),
            }))
        );
        assert!(
            matches!(&key_file, Err(KeyFileError::Read { path, .. }) if path == Path::new("/key.json")),
            "{key_file:?}"
        );
    }

    #[test]
    fn given_a_key_file_that_cannot_be_read_when_starting_a_peer_should_name_the_peer() {
        let directory = tempfile::tempdir().unwrap();
        let path = directory.path().join("missing.json");

        let result = peer_auth(
            "dc-b",
            &section(GcsAuthSetting::Auto, &path.to_string_lossy()),
        );

        assert!(
            matches!(&result, Err(ServerError::GeoPeer { origin, .. }) if origin == "dc-b"),
            "{result:?}"
        );
    }

    /// A token that expires within seconds is fetched again before it
    /// expires, and every handle sees the new one.
    #[test]
    fn given_a_token_that_expires_soon_when_keeping_fresh_should_replace_it_before_it_expires() {
        let transport = ScriptedTransport {
            answers: RefCell::new(VecDeque::from([
                token_answer("first", 2),
                token_answer("second", 3600),
            ])),
        };
        let node = SharedToken::new();
        let shard = node.clone();
        let refresh = GcsRefresh {
            subject: "the gcs backend".to_owned(),
            source: TokenSource::MetadataServer(MetadataServer {
                endpoint: "http://127.0.0.1:9".to_owned(),
            }),
            shared: node,
        };
        let (first, first_fetch) = mpsc::sync_channel(1);
        let started = SystemTime::now();

        compio::runtime::Runtime::new().unwrap().block_on(async {
            let refresher = keep_fresh(&refresh, &transport, Some(first), FAST);
            let _ = compio::time::timeout(Duration::from_millis(1500), refresher).await;
        });

        assert!(matches!(first_fetch.try_recv(), Ok(Ok(Some(_)))));
        assert_eq!(
            shard
                .current()
                .map(|token| token.secret().to_owned())
                .as_deref(),
            Some("second")
        );
        assert!(
            SystemTime::now() < started + Duration::from_secs(2),
            "the refresh came before the expiry"
        );
    }

    /// Without a waiting boot, a refused fetch is fetched again, so a peer
    /// whose service account gets its role later starts reading then.
    #[test]
    fn given_a_refused_fetch_without_a_waiting_boot_when_keeping_fresh_should_fetch_again() {
        let transport = ScriptedTransport {
            answers: RefCell::new(VecDeque::from([
                (403, String::new()),
                token_answer("granted", 3600),
            ])),
        };
        let shared = SharedToken::new();
        let refresh = GcsRefresh {
            subject: "geo peer \"dc-b\"".to_owned(),
            source: TokenSource::MetadataServer(MetadataServer {
                endpoint: "http://127.0.0.1:9".to_owned(),
            }),
            shared: shared.clone(),
        };

        compio::runtime::Runtime::new().unwrap().block_on(async {
            let refresher = keep_fresh(&refresh, &transport, None, FAST);
            let _ = compio::time::timeout(Duration::from_millis(300), refresher).await;
        });

        assert_eq!(
            shared
                .current()
                .map(|token| token.secret().to_owned())
                .as_deref(),
            Some("granted")
        );
    }
}
