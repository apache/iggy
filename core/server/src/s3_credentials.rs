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

//! The credentials of the `s3` tiered storage backend.
//!
//! A node picks its source once, at boot, see [`CredentialSource::select`].
//! Static keys from the configuration never change. Any other source gets a
//! refresher of [`crate::credential_refresh`]. Every shard's store reads the
//! same [`SharedCredentials`], so one refresh reaches every shard.

use std::time::SystemTime;

use configs::tiered_storage::{TieredStorageBackend, TieredStorageConfig};
use iggy_object_store::s3::credentials::{
    CredentialError, CredentialSource, CredentialTransport, Credentials, SharedCredentials,
};

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
pub fn credentials(config: &TieredStorageConfig) -> Result<Option<SharedCredentials>, ServerError> {
    credentials_from(config, |name| std::env::var(name).ok())
}

fn credentials_from(
    config: &TieredStorageConfig,
    env: impl Fn(&str) -> Option<String>,
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
    credential_refresh::start_and_wait(S3Refresh {
        source,
        shared: shared.clone(),
    })?;
    Ok(Some(shared))
}

/// The refresher of the `s3` backend: its source, and the credentials that
/// every shard signs with.
struct S3Refresh {
    source: CredentialSource,
    shared: SharedCredentials,
}

impl Refresh for S3Refresh {
    fn subject(&self) -> &'static str {
        "the s3 backend"
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
    use std::cell::RefCell;
    use std::collections::VecDeque;
    use std::future::{Future, ready};
    use std::path::PathBuf;

    use std::sync::mpsc;
    use std::time::Duration;

    use bytes::Bytes;
    use iggy_object_store::s3::credentials::{
        CredentialRequest, CredentialResponse, InstanceMetadata,
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
            let refresh = S3Refresh {
                source,
                shared: node.clone(),
            };
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
            let refresh = S3Refresh {
                source,
                shared: shared.clone(),
            };
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

        let refresh = S3Refresh {
            source: metadata(),
            shared: shared.clone(),
        };
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
        let shared = credentials_from(&s3_section("AKIDEXAMPLE"), |_| None)
            .unwrap()
            .unwrap();

        assert!(!shared.refreshes());
        assert_eq!(key_of(&shared).as_deref(), Some("AKIDEXAMPLE"));
        assert!(
            credentials_from(&TieredStorageConfig::default(), |_| None)
                .unwrap()
                .is_none()
        );
    }

    #[test]
    fn given_no_source_when_picking_credentials_should_refuse_the_boot() {
        let result = credentials_from(&s3_section(""), |name| {
            (name == "AWS_EC2_METADATA_DISABLED").then(|| "true".to_owned())
        });

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

        let result = credentials_from(&s3_section(""), move |name| match name {
            "AWS_WEB_IDENTITY_TOKEN_FILE" => Some(token_file_text.clone()),
            "AWS_ROLE_ARN" => Some("arn:aws:iam::123456789012:role/iggy".to_owned()),
            "AWS_ENDPOINT_URL_STS" => Some("http://127.0.0.1:1".to_owned()),
            _ => None,
        });

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
}
