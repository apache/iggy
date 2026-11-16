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

//! Where an S3 store takes its credentials from, and how a node shares them.
//!
//! [`CredentialSource::select`] picks the first source that a node has, in
//! the order of the AWS SDKs: static keys from the configuration, then web
//! identity, then the instance metadata service. Web identity comes before
//! instance metadata because a pod on EKS can reach the metadata service of
//! its node, which hands out the role of the node instead of the pod's.
//!
//! Each source builds its requests and reads the answers without I/O, so
//! tests answer them from recorded responses. A [`CredentialTransport`]
//! sends the requests, and [`HttpTransport`] does that over HTTP.
//!
//! [`SharedCredentials`] holds the credentials of a node. Every store of the
//! node reads them before it signs a request, so one refresh reaches every
//! shard.

mod imds;
mod web_identity;

use std::fmt;
use std::future::Future;
use std::io;
use std::path::PathBuf;
use std::sync::Arc;
use std::time::{Duration, SystemTime};

use arc_swap::ArcSwapOption;
use bytes::Bytes;
use chrono::DateTime;
use http::Method;

pub use super::sigv4::Credentials;
pub use imds::InstanceMetadata;
pub use web_identity::WebIdentity;

use crate::error::ObjectStoreError;

/// How long before credentials expire a node fetches new ones.
pub const REFRESH_MARGIN: Duration = Duration::from_mins(5);
/// The shortest wait before the next fetch.
pub const MIN_REFRESH_WAIT: Duration = Duration::from_secs(1);

const DEFAULT_SESSION_NAME: &str = "iggy-tiered-storage";
const DEFAULT_METADATA_ENDPOINT: &str = "http://169.254.169.254";

/// The credentials that every S3 store of a node signs with.
///
/// A clone is another handle to the same credentials, so a refresh reaches
/// every shard at once. A store reads them before each request. `Debug`
/// shows no value.
#[derive(Clone)]
pub struct SharedCredentials {
    current: Arc<ArcSwapOption<Credentials>>,
    refreshed: bool,
}

/// Credentials that a source handed out, and when they expire.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct FetchedCredentials {
    pub credentials: Credentials,
    /// `None` for credentials that never expire.
    pub expires_at: Option<SystemTime>,
}

/// Where a node takes its S3 credentials from.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum CredentialSource {
    /// Keys from the configuration, which never expire.
    Static(Credentials),
    /// A token file that a platform such as EKS keeps fresh, exchanged at
    /// STS for temporary credentials.
    WebIdentity(WebIdentity),
    /// The role of an EC2 instance, from the instance metadata service.
    InstanceMetadata(InstanceMetadata),
}

/// Why a source handed out no credentials.
#[derive(Debug, thiserror::Error)]
pub enum CredentialError {
    /// The request got no answer: a refused connection, a timeout or a
    /// failed name lookup.
    #[error("the credential service did not answer: {0}")]
    Unreachable(String),
    /// The service failed in a way that a retry can get past, such as an
    /// HTTP 5xx.
    #[error("the credential service failed: {0}")]
    Unavailable(String),
    /// The service refused the request for a reason that only an operator
    /// can change, such as a role that does not trust the token.
    #[error("the credential service refused the request: {0}")]
    Refused(String),
    #[error("malformed answer of the credential service: {0}")]
    Malformed(String),
    #[error("the web identity token file {path:?}: {source}")]
    TokenFile {
        path: PathBuf,
        #[source]
        source: io::Error,
    },
}

/// One HTTP request of a credential source. `Debug` shows neither the
/// headers nor the body, which can hold tokens.
pub struct CredentialRequest {
    pub method: Method,
    pub url: String,
    pub headers: Vec<(&'static str, String)>,
    pub body: String,
}

/// The answer to a [`CredentialRequest`]. `Debug` shows only the status and
/// the length of the body, which can hold credentials.
pub struct CredentialResponse {
    pub status: u16,
    pub body: Bytes,
}

/// How a credential source reaches its service.
pub trait CredentialTransport {
    /// Sends `request` and returns the answer, whatever its status.
    ///
    /// # Errors
    /// [`CredentialError::Unreachable`] when no answer came.
    fn send(
        &self,
        request: CredentialRequest,
    ) -> impl Future<Output = Result<CredentialResponse, CredentialError>>;
}

/// Sends the requests of a credential source over HTTP, each with a
/// deadline.
pub struct HttpTransport {
    client: cyper::Client,
    timeout: Duration,
}

impl SharedCredentials {
    /// Keys that never change, such as static keys from the configuration.
    #[must_use]
    pub fn fixed(credentials: Credentials) -> Self {
        Self {
            current: Arc::new(ArcSwapOption::from_pointee(credentials)),
            refreshed: false,
        }
    }

    /// Credentials that a refresher keeps fresh. They are empty until its
    /// first fetch succeeds.
    #[must_use]
    pub fn refreshed() -> Self {
        Self {
            current: Arc::new(ArcSwapOption::empty()),
            refreshed: true,
        }
    }

    /// The credentials to sign with now, `None` before the first fetch.
    #[must_use]
    pub fn current(&self) -> Option<Arc<Credentials>> {
        self.current.load_full()
    }

    /// Replaces the credentials for every handle.
    pub fn replace(&self, credentials: Credentials) {
        self.current.store(Some(Arc::new(credentials)));
    }

    /// Whether a refresher keeps these credentials fresh, so that expired
    /// credentials are an outage that the next refresh ends.
    #[must_use]
    pub const fn refreshes(&self) -> bool {
        self.refreshed
    }
}

impl From<Credentials> for SharedCredentials {
    fn from(credentials: Credentials) -> Self {
        Self::fixed(credentials)
    }
}

impl fmt::Debug for SharedCredentials {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("SharedCredentials")
            .field("refreshed", &self.refreshed)
            .field("set", &self.current.load().is_some())
            .finish()
    }
}

impl CredentialSource {
    /// The first source that a node has: `static_keys`, then web identity,
    /// then the instance metadata service. `env` reads a variable of the
    /// process environment, and an empty value counts as unset.
    ///
    /// Web identity needs `AWS_WEB_IDENTITY_TOKEN_FILE` and `AWS_ROLE_ARN`.
    /// It takes `AWS_ROLE_SESSION_NAME` and `AWS_ENDPOINT_URL_STS` when they
    /// are set, and otherwise uses STS in `region`. The metadata service is
    /// at `AWS_EC2_METADATA_SERVICE_ENDPOINT` when that is set.
    ///
    /// Returns `None` when `AWS_EC2_METADATA_DISABLED` is `true` and no other
    /// source is there.
    pub fn select(
        static_keys: Option<Credentials>,
        region: &str,
        env: impl Fn(&str) -> Option<String>,
    ) -> Option<Self> {
        let env = |name: &str| env(name).filter(|value| !value.trim().is_empty());
        if let Some(keys) = static_keys {
            return Some(Self::Static(keys));
        }
        if let (Some(token_file), Some(role_arn)) =
            (env("AWS_WEB_IDENTITY_TOKEN_FILE"), env("AWS_ROLE_ARN"))
        {
            return Some(Self::WebIdentity(WebIdentity {
                token_file: PathBuf::from(token_file),
                role_arn,
                session_name: env("AWS_ROLE_SESSION_NAME")
                    .unwrap_or_else(|| DEFAULT_SESSION_NAME.to_owned()),
                endpoint: env("AWS_ENDPOINT_URL_STS")
                    .unwrap_or_else(|| format!("https://sts.{region}.amazonaws.com")),
            }));
        }
        if env("AWS_EC2_METADATA_DISABLED").is_some_and(|value| value.eq_ignore_ascii_case("true"))
        {
            return None;
        }
        Some(Self::InstanceMetadata(InstanceMetadata {
            endpoint: env("AWS_EC2_METADATA_SERVICE_ENDPOINT")
                .unwrap_or_else(|| DEFAULT_METADATA_ENDPOINT.to_owned()),
        }))
    }

    /// What a log line calls this source.
    #[must_use]
    pub const fn name(&self) -> &'static str {
        match self {
            Self::Static(_) => "static keys",
            Self::WebIdentity(_) => "web identity",
            Self::InstanceMetadata(_) => "instance metadata",
        }
    }

    /// The URL of the service that hands out the credentials, `None` for
    /// static keys.
    #[must_use]
    pub fn endpoint(&self) -> Option<&str> {
        match self {
            Self::Static(_) => None,
            Self::WebIdentity(source) => Some(&source.endpoint),
            Self::InstanceMetadata(source) => Some(&source.endpoint),
        }
    }

    /// Fetches credentials once.
    ///
    /// # Errors
    /// The [`CredentialError`] that tells whether a retry can succeed, see
    /// [`CredentialError::is_transient`].
    pub async fn fetch(
        &self,
        transport: &impl CredentialTransport,
    ) -> Result<FetchedCredentials, CredentialError> {
        match self {
            Self::Static(credentials) => Ok(FetchedCredentials {
                credentials: credentials.clone(),
                expires_at: None,
            }),
            Self::WebIdentity(source) => source.fetch(transport).await,
            Self::InstanceMetadata(source) => source.fetch(transport).await,
        }
    }
}

impl CredentialError {
    /// Whether the same fetch can succeed later without a change by an
    /// operator.
    #[must_use]
    pub const fn is_transient(&self) -> bool {
        matches!(self, Self::Unreachable(_) | Self::Unavailable(_))
    }
}

impl fmt::Debug for CredentialRequest {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("CredentialRequest")
            .field("method", &self.method)
            .field("url", &self.url)
            .finish_non_exhaustive()
    }
}

impl fmt::Debug for CredentialResponse {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("CredentialResponse")
            .field("status", &self.status)
            .field("body_len", &self.body.len())
            .finish()
    }
}

impl HttpTransport {
    /// A transport for the requests of a source at `endpoint`, each of which
    /// fails as unreachable after `timeout`.
    ///
    /// # Errors
    /// [`ObjectStoreError::InvalidRequest`] for an endpoint that is not an
    /// `http` or `https` URL, and [`ObjectStoreError::Transport`] when the
    /// client cannot be built.
    pub fn new(endpoint: &str, timeout: Duration) -> Result<Self, ObjectStoreError> {
        let scheme = url::Url::parse(endpoint)
            .map_err(|_| ObjectStoreError::InvalidRequest(format!("{endpoint:?} is not a URL")))?
            .scheme()
            .to_owned();
        Ok(Self {
            client: super::http_client(&scheme)?,
            timeout,
        })
    }
}

impl CredentialTransport for HttpTransport {
    async fn send(
        &self,
        request: CredentialRequest,
    ) -> Result<CredentialResponse, CredentialError> {
        let unreachable =
            |error: &dyn fmt::Display| CredentialError::Unreachable(error.to_string());
        let mut builder = self
            .client
            .request(request.method, request.url.as_str())
            .map_err(|error| unreachable(&error))?;
        for (name, value) in &request.headers {
            builder = builder
                .header(*name, value.as_str())
                .map_err(|error| CredentialError::Malformed(format!("header {name}: {error}")))?;
        }
        let exchange = async {
            let response = builder.body(request.body).send().await?;
            let status = response.status().as_u16();
            let body = response.bytes().await?;
            Ok::<_, cyper::Error>(CredentialResponse { status, body })
        };
        compio::time::timeout(self.timeout, exchange)
            .await
            .map_err(|_| {
                CredentialError::Unreachable(format!("no answer within {:?}", self.timeout))
            })?
            .map_err(|error| unreachable(&error))
    }
}

/// How long to wait before the next fetch, after one that handed out
/// credentials that expire at `expires_at`.
///
/// The next fetch comes [`REFRESH_MARGIN`] before the expiry. Credentials
/// that expire sooner than that are still used for half of the time that
/// they have left, so a service that hands them out close to their expiry
/// costs no busy loop.
#[must_use]
pub fn next_refresh(expires_at: SystemTime, now: SystemTime) -> Duration {
    let left = expires_at.duration_since(now).unwrap_or_default();
    left.saturating_sub(REFRESH_MARGIN)
        .max(left / 2)
        .max(MIN_REFRESH_WAIT)
}

/// The time of an `Expiration` field.
fn parse_expiration(text: &str) -> Result<SystemTime, CredentialError> {
    DateTime::parse_from_rfc3339(text.trim())
        .map(SystemTime::from)
        .map_err(|_| CredentialError::Malformed(format!("expiration {text:?}")))
}

#[cfg(test)]
mod tests {
    use std::cell::RefCell;
    use std::collections::VecDeque;
    use std::future::ready;

    use super::*;

    /// Answers each request with the next recorded response, and keeps the
    /// requests.
    #[derive(Default)]
    pub struct RecordedTransport {
        answers: RefCell<VecDeque<Result<CredentialResponse, CredentialError>>>,
        pub requests: RefCell<Vec<CredentialRequest>>,
    }

    impl RecordedTransport {
        pub fn answering(answers: impl IntoIterator<Item = (u16, &'static str)>) -> Self {
            Self {
                answers: RefCell::new(
                    answers
                        .into_iter()
                        .map(|(status, body)| {
                            Ok(CredentialResponse {
                                status,
                                body: Bytes::from_static(body.as_bytes()),
                            })
                        })
                        .collect(),
                ),
                requests: RefCell::default(),
            }
        }
    }

    impl CredentialTransport for RecordedTransport {
        fn send(
            &self,
            request: CredentialRequest,
        ) -> impl Future<Output = Result<CredentialResponse, CredentialError>> {
            self.requests.borrow_mut().push(request);
            ready(self.answers.borrow_mut().pop_front().unwrap_or_else(|| {
                Err(CredentialError::Unreachable(
                    "no more recorded answers".to_owned(),
                ))
            }))
        }
    }

    fn keys(id: &str) -> Credentials {
        Credentials {
            access_key_id: id.to_owned(),
            secret_access_key: "secret".to_owned(),
            session_token: None,
        }
    }

    fn env(pairs: &'static [(&'static str, &'static str)]) -> impl Fn(&str) -> Option<String> {
        move |name| {
            pairs
                .iter()
                .find(|(key, _)| *key == name)
                .map(|(_, value)| (*value).to_owned())
        }
    }

    #[test]
    fn given_sources_when_selecting_should_take_the_first_in_aws_sdk_order() {
        let web_identity = env(&[
            ("AWS_WEB_IDENTITY_TOKEN_FILE", "/var/run/token"),
            ("AWS_ROLE_ARN", "arn:aws:iam::123456789012:role/iggy"),
        ]);

        assert_eq!(
            CredentialSource::select(Some(keys("static")), "eu-central-1", &web_identity),
            Some(CredentialSource::Static(keys("static")))
        );
        assert_eq!(
            CredentialSource::select(None, "eu-central-1", &web_identity),
            Some(CredentialSource::WebIdentity(WebIdentity {
                token_file: PathBuf::from("/var/run/token"),
                role_arn: "arn:aws:iam::123456789012:role/iggy".to_owned(),
                session_name: DEFAULT_SESSION_NAME.to_owned(),
                endpoint: "https://sts.eu-central-1.amazonaws.com".to_owned(),
            }))
        );
        assert_eq!(
            CredentialSource::select(None, "eu-central-1", env(&[])),
            Some(CredentialSource::InstanceMetadata(InstanceMetadata {
                endpoint: DEFAULT_METADATA_ENDPOINT.to_owned(),
            }))
        );
    }

    #[test]
    fn given_environment_overrides_when_selecting_should_take_them() {
        let overridden = CredentialSource::select(
            None,
            "eu-central-1",
            env(&[
                ("AWS_WEB_IDENTITY_TOKEN_FILE", "/token"),
                ("AWS_ROLE_ARN", "arn"),
                ("AWS_ROLE_SESSION_NAME", "node-1"),
                ("AWS_ENDPOINT_URL_STS", "http://localhost:4566"),
            ]),
        );
        let half_web_identity = CredentialSource::select(
            None,
            "eu-central-1",
            env(&[
                ("AWS_WEB_IDENTITY_TOKEN_FILE", "/token"),
                ("AWS_ROLE_ARN", " "),
                ("AWS_EC2_METADATA_SERVICE_ENDPOINT", "http://127.0.0.1:1338"),
            ]),
        );

        assert!(matches!(
            overridden,
            Some(CredentialSource::WebIdentity(WebIdentity { ref session_name, ref endpoint, .. }))
                if session_name == "node-1" && endpoint == "http://localhost:4566"
        ));
        assert_eq!(
            half_web_identity
                .as_ref()
                .and_then(CredentialSource::endpoint),
            Some("http://127.0.0.1:1338")
        );
        assert_eq!(
            CredentialSource::select(
                None,
                "eu-central-1",
                env(&[("AWS_EC2_METADATA_DISABLED", "TRUE")])
            ),
            None
        );
    }

    #[test]
    fn given_shared_credentials_when_replaced_should_reach_every_handle_on_every_thread() {
        let node = SharedCredentials::refreshed();
        let shard = node.clone();
        assert!(shard.current().is_none());

        node.replace(keys("first"));
        let seen =
            std::thread::spawn(move || shard.current().map(|keys| keys.access_key_id.clone()))
                .join()
                .unwrap();

        assert_eq!(seen.as_deref(), Some("first"));
        assert!(node.refreshes());
        assert!(!SharedCredentials::fixed(keys("static")).refreshes());
    }

    #[test]
    fn given_credentials_when_debug_printing_should_show_no_value() {
        let shared = SharedCredentials::fixed(keys("AKIDEXAMPLE"));
        let request = CredentialRequest {
            method: Method::POST,
            url: "https://sts.example/".to_owned(),
            headers: vec![("x-aws-ec2-metadata-token", "token-value".to_owned())],
            body: "WebIdentityToken=token-value".to_owned(),
        };

        let printed = format!(
            "{shared:?} {request:?} {:?}",
            CredentialResponse {
                status: 200,
                body: Bytes::from_static(b"secret-value"),
            }
        );

        assert!(!printed.contains("AKIDEXAMPLE"), "{printed}");
        assert!(!printed.contains("token-value"), "{printed}");
        assert!(!printed.contains("secret"), "{printed}");
    }

    #[test]
    fn given_expiries_when_scheduling_should_refresh_before_each_and_never_spin() {
        let now = SystemTime::UNIX_EPOCH + Duration::from_hours(1000);

        assert_eq!(
            next_refresh(now + Duration::from_hours(1), now),
            Duration::from_mins(55)
        );
        assert_eq!(
            next_refresh(now + Duration::from_mins(4), now),
            Duration::from_mins(2)
        );
        assert_eq!(next_refresh(now, now), MIN_REFRESH_WAIT);
        assert_eq!(
            next_refresh(now - Duration::from_mins(1), now),
            MIN_REFRESH_WAIT
        );
    }

    #[test]
    fn given_static_keys_when_fetching_should_hand_them_out_without_expiry() {
        let fetched = crate::contract::shard_runtime().block_on(
            CredentialSource::Static(keys("static")).fetch(&RecordedTransport::default()),
        );

        assert_eq!(
            fetched.unwrap(),
            FetchedCredentials {
                credentials: keys("static"),
                expires_at: None,
            }
        );
    }
}
