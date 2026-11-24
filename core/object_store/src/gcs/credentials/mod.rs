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

//! Where a GCS store takes its access tokens from, and how a node shares
//! them.
//!
//! [`TokenSource::select`] picks the first source that a node has, in the
//! order of Google's Application Default Credentials: a service account key
//! file that the configuration names, then the one that
//! `GOOGLE_APPLICATION_CREDENTIALS` names, then the metadata server of a
//! Compute Engine instance or a GKE node, which also serves GKE Workload
//! Identity.
//!
//! Each source builds its requests and reads the answers without I/O, over
//! the [`CredentialTransport`] that every credential source shares.
//!
//! [`SharedToken`] holds the access token of a node. Every store of the
//! node reads it before each request, so one refresh reaches every shard.

mod metadata_server;
mod service_account;

use std::fmt;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::{Duration, SystemTime};

use arc_swap::ArcSwapOption;
use serde::Deserialize;

pub use metadata_server::MetadataServer;
pub use service_account::{KeyFileError, ServiceAccountKey};

pub use crate::credentials::{
    CredentialError, CredentialRequest, CredentialResponse, CredentialTransport, HttpTransport,
    next_refresh,
};

/// The environment variable that names a service account key file.
pub const KEY_FILE_ENV: &str = "GOOGLE_APPLICATION_CREDENTIALS";
/// The environment variable that names the host of the metadata server,
/// as Google's client libraries read it.
pub const METADATA_HOST_ENV: &str = "GCE_METADATA_HOST";
const DEFAULT_METADATA_HOST: &str = "metadata.google.internal";

/// An `OAuth2` access token. `Debug` shows no value.
#[derive(Clone, PartialEq, Eq)]
pub struct AccessToken(String);

/// The access token that every GCS store of a node sends.
///
/// A clone is another handle to the same token, so a refresh reaches every
/// shard at once. A store reads it before each request. It is empty until
/// the first fetch of the node's refresher succeeds.
#[derive(Clone, Default)]
pub struct SharedToken {
    current: Arc<ArcSwapOption<AccessToken>>,
}

/// How a GCS store authorizes its requests.
#[derive(Debug, Clone)]
pub enum GcsAuth {
    /// A bearer token that a refresher of the node keeps fresh.
    Token(SharedToken),
    /// No `Authorization` header, for an emulator such as fake-gcs-server.
    Anonymous,
}

/// A token that a source handed out, and when it expires.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct FetchedToken {
    pub token: AccessToken,
    pub expires_at: SystemTime,
}

/// Where a node takes its GCS access tokens from.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum TokenSource {
    /// A service account key file, whose key signs a JWT that the token
    /// endpoint of the key exchanges for an access token.
    ServiceAccount(ServiceAccountKey),
    /// The service account of the instance, from its metadata server.
    MetadataServer(MetadataServer),
}

/// The answer of a token endpoint and of the metadata server.
#[derive(Deserialize)]
struct TokenAnswer {
    access_token: String,
    expires_in: u64,
    #[serde(default)]
    token_type: Option<String>,
}

impl AccessToken {
    #[must_use]
    pub const fn new(value: String) -> Self {
        Self(value)
    }

    /// The token as the `Authorization` header sends it.
    #[must_use]
    pub fn secret(&self) -> &str {
        &self.0
    }
}

impl fmt::Debug for AccessToken {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("AccessToken(..)")
    }
}

impl SharedToken {
    /// An empty token cell, which a refresher fills.
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    /// The token to send now, `None` before the first fetch.
    #[must_use]
    pub fn current(&self) -> Option<Arc<AccessToken>> {
        self.current.load_full()
    }

    /// Replaces the token for every handle.
    pub fn replace(&self, token: AccessToken) {
        self.current.store(Some(Arc::new(token)));
    }
}

impl fmt::Debug for SharedToken {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("SharedToken")
            .field("set", &self.current.load().is_some())
            .finish()
    }
}

impl TokenSource {
    /// The first source that a node has: the key file at `key_file`, then
    /// the one that `GOOGLE_APPLICATION_CREDENTIALS` names, then the
    /// metadata server, at `GCE_METADATA_HOST` when that is set. `env` reads
    /// a variable of the process environment, and an empty value counts as
    /// unset. `read` reads a key file.
    ///
    /// # Errors
    /// [`KeyFileError`] when the key file cannot be read, is not a service
    /// account key, or holds a key that cannot sign.
    pub fn select(
        key_file: Option<&Path>,
        env: impl Fn(&str) -> Option<String>,
        read: impl Fn(&Path) -> std::io::Result<String>,
    ) -> Result<Self, KeyFileError> {
        let env = |name: &str| env(name).filter(|value| !value.trim().is_empty());
        let key_file = key_file
            .map(Path::to_path_buf)
            .or_else(|| env(KEY_FILE_ENV).map(PathBuf::from));
        if let Some(path) = key_file {
            let text = read(&path).map_err(|source| KeyFileError::Read {
                path: path.clone(),
                source,
            })?;
            return ServiceAccountKey::parse(&path, &text).map(Self::ServiceAccount);
        }
        let host = env(METADATA_HOST_ENV).unwrap_or_else(|| DEFAULT_METADATA_HOST.to_owned());
        Ok(Self::MetadataServer(MetadataServer {
            endpoint: format!("http://{}", host.trim()),
        }))
    }

    /// What a log line calls this source.
    #[must_use]
    pub const fn name(&self) -> &'static str {
        match self {
            Self::ServiceAccount(_) => "service account key",
            Self::MetadataServer(_) => "metadata server",
        }
    }

    /// The URL of the service that hands out the tokens.
    #[must_use]
    pub fn endpoint(&self) -> &str {
        match self {
            Self::ServiceAccount(key) => &key.token_uri,
            Self::MetadataServer(server) => &server.endpoint,
        }
    }

    /// Fetches a token once.
    ///
    /// # Errors
    /// The [`CredentialError`] that tells whether a retry can succeed.
    pub async fn fetch(
        &self,
        transport: &impl CredentialTransport,
    ) -> Result<FetchedToken, CredentialError> {
        match self {
            Self::ServiceAccount(key) => key.fetch(transport, SystemTime::now()).await,
            Self::MetadataServer(server) => server.fetch(transport).await,
        }
    }
}

/// The token in the answer of a token service, which expires `expires_in`
/// seconds after `now`.
fn parse_token(
    body: &[u8],
    now: SystemTime,
    service: &str,
) -> Result<FetchedToken, CredentialError> {
    // The error of a parser can quote the document, which holds the token.
    let answer: TokenAnswer = serde_json::from_slice(body)
        .map_err(|_| CredentialError::Malformed(format!("the token answer of {service}")))?;
    if let Some(token_type) = &answer.token_type
        && !token_type.eq_ignore_ascii_case("bearer")
    {
        return Err(CredentialError::Malformed(format!(
            "{service} handed out a {token_type:?} token, not a bearer token"
        )));
    }
    if answer.access_token.is_empty() {
        return Err(CredentialError::Malformed(format!(
            "{service} handed out an empty token"
        )));
    }
    Ok(FetchedToken {
        token: AccessToken::new(answer.access_token),
        expires_at: now + Duration::from_secs(answer.expires_in),
    })
}

/// The error that a status other than a success means. `what` names the
/// service and what was asked of it.
fn refusal(status: u16, what: &str) -> CredentialError {
    match status {
        408 | 429 | 500..=599 => CredentialError::Unavailable(format!("{what} answered {status}")),
        _ => CredentialError::Refused(format!("{what} answered {status}")),
    }
}

#[cfg(test)]
mod tests {
    use std::io;

    use super::*;

    fn env(pairs: &'static [(&'static str, &'static str)]) -> impl Fn(&str) -> Option<String> {
        move |name| {
            pairs
                .iter()
                .find(|(key, _)| *key == name)
                .map(|(_, value)| (*value).to_owned())
        }
    }

    fn no_file(path: &Path) -> io::Result<String> {
        Err(io::Error::new(
            io::ErrorKind::NotFound,
            format!("{} is missing", path.display()),
        ))
    }

    /// Reads every path as a key file of the account named after it.
    #[allow(clippy::unnecessary_wraps)]
    fn key_file(path: &Path) -> io::Result<String> {
        Ok(service_account::tests::key_file_json(
            &path.display().to_string(),
        ))
    }

    #[test]
    fn given_sources_when_selecting_should_take_the_first_in_application_default_order() {
        let both = env(&[
            (KEY_FILE_ENV, "/var/run/env-key.json"),
            (METADATA_HOST_ENV, "127.0.0.1:8080"),
        ]);

        let configured =
            TokenSource::select(Some(Path::new("/etc/iggy/key.json")), &both, key_file).unwrap();
        let from_env = TokenSource::select(None, &both, key_file).unwrap();
        let metadata = TokenSource::select(None, env(&[(KEY_FILE_ENV, " ")]), no_file).unwrap();
        let overridden =
            TokenSource::select(None, env(&[(METADATA_HOST_ENV, "127.0.0.1:8080")]), no_file)
                .unwrap();

        assert!(
            matches!(&configured, TokenSource::ServiceAccount(key) if key.client_email == "/etc/iggy/key.json"),
            "{configured:?}"
        );
        assert!(
            matches!(&from_env, TokenSource::ServiceAccount(key) if key.client_email == "/var/run/env-key.json"),
            "{from_env:?}"
        );
        assert_eq!(
            metadata,
            TokenSource::MetadataServer(MetadataServer {
                endpoint: "http://metadata.google.internal".to_owned(),
            })
        );
        assert_eq!(overridden.endpoint(), "http://127.0.0.1:8080");
        assert_eq!(overridden.name(), "metadata server");
    }

    #[test]
    fn given_a_key_file_that_cannot_be_read_when_selecting_should_refuse() {
        let result = TokenSource::select(Some(Path::new("/missing.json")), env(&[]), no_file);

        assert!(
            matches!(&result, Err(KeyFileError::Read { path, .. }) if path == Path::new("/missing.json")),
            "{result:?}"
        );
    }

    #[test]
    fn given_token_answers_when_parsing_should_take_bearer_tokens_only() {
        let now = SystemTime::UNIX_EPOCH + Duration::from_hours(1000);

        let fetched = parse_token(
            br#"{"access_token":"ya29.token","expires_in":3599,"token_type":"Bearer"}"#,
            now,
            "the service",
        )
        .unwrap();

        assert_eq!(fetched.token.secret(), "ya29.token");
        assert_eq!(fetched.expires_at, now + Duration::from_secs(3599));
        for body in [
            &br#"{"access_token":"t","expires_in":1,"token_type":"MAC"}"#[..],
            br#"{"access_token":"","expires_in":1}"#,
            br#"{"expires_in":1}"#,
            b"not json",
        ] {
            assert!(
                matches!(
                    parse_token(body, now, "s"),
                    Err(CredentialError::Malformed(_))
                ),
                "{}",
                String::from_utf8_lossy(body)
            );
        }
    }

    #[test]
    fn given_statuses_when_refusing_should_tell_an_outage_from_a_refusal() {
        assert!(refusal(503, "s").is_transient());
        assert!(refusal(429, "s").is_transient());
        assert!(!refusal(400, "s").is_transient());
        assert!(!refusal(404, "s").is_transient());
    }

    #[test]
    fn given_tokens_when_debug_printing_should_show_no_value() {
        let shared = SharedToken::new();
        shared.replace(AccessToken::new("ya29.secret-token".to_owned()));

        let printed = format!(
            "{shared:?} {:?} {:?}",
            shared.current(),
            GcsAuth::Token(shared.clone())
        );

        assert!(!printed.contains("secret-token"), "{printed}");
    }

    #[test]
    fn given_shared_token_when_replaced_should_reach_every_handle_on_every_thread() {
        let node = SharedToken::new();
        let shard = node.clone();
        assert!(shard.current().is_none());

        node.replace(AccessToken::new("first".to_owned()));
        let seen =
            std::thread::spawn(move || shard.current().map(|token| token.secret().to_owned()))
                .join()
                .unwrap();

        assert_eq!(seen.as_deref(), Some("first"));
    }
}
