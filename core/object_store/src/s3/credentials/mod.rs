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
use std::path::PathBuf;
use std::sync::Arc;
use std::time::SystemTime;

use arc_swap::ArcSwapOption;
use chrono::DateTime;

pub use super::sigv4::Credentials;
pub use crate::credentials::{
    CredentialError, CredentialRequest, CredentialResponse, CredentialTransport, HttpTransport,
    MIN_REFRESH_WAIT, REFRESH_MARGIN, next_refresh,
};
pub use imds::InstanceMetadata;
pub use web_identity::WebIdentity;

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

/// The time of an `Expiration` field.
fn parse_expiration(text: &str) -> Result<SystemTime, CredentialError> {
    DateTime::parse_from_rfc3339(text.trim())
        .map(SystemTime::from)
        .map_err(|_| CredentialError::Malformed(format!("expiration {text:?}")))
}

#[cfg(test)]
mod tests {
    use bytes::Bytes;
    use http::Method;

    use super::*;
    use crate::credentials::testing::RecordedTransport;

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
