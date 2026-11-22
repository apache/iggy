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

use crate::ConfigurationError;
use configs::{ConfigEnv, DisplayFromStrOrNumber};
use iggy_common::{IggyDuration, Validatable};
use serde::{Deserialize, Serialize};
use serde_with::serde_as;

/// External authentication callout configuration.
///
/// When enabled, login attempts whose credential is not valid locally (an
/// unknown username, or a personal access token that is unknown or expired) are
/// forwarded to an external HTTP service for each kind listed in
/// [`Self::credential_types`]. The service maps the caller to a
/// pre-provisioned Iggy username or denies the login. Local credentials never
/// hit the callout. The secret is forwarded so the external service can
/// verify the requester.
///
/// The callout can fire more than once for a single logical login because
/// the SDK replays after transient failures. The external service must be
/// idempotent; one-time passwords will not survive a replay.
#[serde_as]
#[derive(Debug, Deserialize, Serialize, Clone, ConfigEnv)]
pub struct ExternalAuthConfig {
    #[serde(default)]
    pub enabled: bool,
    #[serde(default, skip_serializing)]
    #[config_env(secret)]
    pub url: String,
    #[serde_as(as = "DisplayFromStrOrNumber")]
    #[serde(default = "default_external_auth_timeout")]
    #[config_env(leaf)]
    pub timeout: IggyDuration,
    /// Per-shard-thread cap on concurrent ext-auth callouts. TCP and WS
    /// logins round-robin across shards, so the external service can see up
    /// to `shards * max_concurrent_callouts` requests in flight.
    #[serde(default = "default_max_concurrent_callouts")]
    pub max_concurrent_callouts: u32,
    /// Credential kinds forwarded to the service. Personal access tokens are
    /// opt-in so enabling the callout never sends tokens to a service that
    /// predates them.
    #[serde(default = "default_credential_types")]
    #[config_env(leaf)]
    pub credential_types: Vec<ExternalAuthCredentialType>,
}

/// A credential kind the callout can forward. The serialized name matches
/// the `credentials.type` tag of the callout request.
#[derive(Debug, Deserialize, Serialize, Clone, Copy, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum ExternalAuthCredentialType {
    UsernamePassword,
    PersonalAccessToken,
}

impl ExternalAuthConfig {
    /// `true` when the callout is enabled and forwards `credential_type`.
    #[must_use]
    pub fn forwards(&self, credential_type: ExternalAuthCredentialType) -> bool {
        self.enabled && self.credential_types.contains(&credential_type)
    }
}

impl Default for ExternalAuthConfig {
    fn default() -> Self {
        Self {
            enabled: false,
            url: String::new(),
            timeout: default_external_auth_timeout(),
            max_concurrent_callouts: default_max_concurrent_callouts(),
            credential_types: default_credential_types(),
        }
    }
}

impl Validatable<ConfigurationError> for ExternalAuthConfig {
    fn validate(&self) -> Result<(), ConfigurationError> {
        if !self.enabled {
            return Ok(());
        }
        if self.url.is_empty() {
            eprintln!("external_auth.url must be set when external_auth.enabled = true");
            return Err(ConfigurationError::InvalidConfigurationValue);
        }
        let parsed = url::Url::parse(&self.url).map_err(|e| {
            eprintln!("external_auth.url is not a valid URL: {e}");
            ConfigurationError::InvalidConfigurationValue
        })?;
        match parsed.scheme() {
            "http" | "https" => {}
            _ => {
                eprintln!("external_auth.url must use http or https");
                return Err(ConfigurationError::InvalidConfigurationValue);
            }
        }
        if parsed.host_str().is_none_or(|h| h.is_empty()) {
            eprintln!("external_auth.url has no host");
            return Err(ConfigurationError::InvalidConfigurationValue);
        }
        if self.timeout.get_duration().is_zero() {
            eprintln!("external_auth.timeout must be greater than zero");
            return Err(ConfigurationError::InvalidConfigurationValue);
        }
        if self.max_concurrent_callouts == 0 {
            eprintln!("external_auth.max_concurrent_callouts must be greater than zero");
            return Err(ConfigurationError::InvalidConfigurationValue);
        }
        if self.credential_types.is_empty() {
            eprintln!("external_auth.credential_types must name at least one credential type");
            return Err(ConfigurationError::InvalidConfigurationValue);
        }
        Ok(())
    }
}

fn default_external_auth_timeout() -> IggyDuration {
    IggyDuration::new_from_secs(5)
}

const fn default_max_concurrent_callouts() -> u32 {
    64
}

fn default_credential_types() -> Vec<ExternalAuthCredentialType> {
    vec![ExternalAuthCredentialType::UsernamePassword]
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn given_disabled_config_when_validating_should_accept_empty_url() {
        let config = ExternalAuthConfig {
            enabled: false,
            ..ExternalAuthConfig::default()
        };
        assert!(config.validate().is_ok());
    }

    #[test]
    fn given_enabled_config_with_empty_url_when_validating_should_reject() {
        let config = ExternalAuthConfig {
            enabled: true,
            ..ExternalAuthConfig::default()
        };
        assert!(config.validate().is_err());
    }

    #[test]
    fn given_enabled_config_with_invalid_scheme_when_validating_should_reject() {
        let config = ExternalAuthConfig {
            enabled: true,
            url: "ftp://auth.example.com".to_owned(),
            ..ExternalAuthConfig::default()
        };
        assert!(config.validate().is_err());
    }

    #[test]
    fn given_enabled_config_with_bare_scheme_when_validating_should_reject() {
        let config = ExternalAuthConfig {
            enabled: true,
            url: "https://".to_owned(),
            ..ExternalAuthConfig::default()
        };
        assert!(config.validate().is_err());
    }

    #[test]
    fn given_enabled_config_with_scheme_slash_slash_slash_when_validating_should_reject() {
        // WHATWG parses "http:///" as empty-host, which we reject.
        let config = ExternalAuthConfig {
            enabled: true,
            url: "http:///".to_owned(),
            ..ExternalAuthConfig::default()
        };
        assert!(config.validate().is_err());
    }

    #[test]
    fn given_enabled_config_with_query_only_url_when_validating_should_reject() {
        let config = ExternalAuthConfig {
            enabled: true,
            url: "https://?x".to_owned(),
            ..ExternalAuthConfig::default()
        };
        assert!(config.validate().is_err());
    }

    #[test]
    fn given_enabled_config_with_zero_timeout_when_validating_should_reject() {
        let config = ExternalAuthConfig {
            enabled: true,
            url: "https://auth.example.com/verify".to_owned(),
            timeout: "0 s".parse().expect("valid duration"),
            ..ExternalAuthConfig::default()
        };
        assert!(config.validate().is_err());
    }

    #[test]
    fn given_enabled_config_with_zero_max_concurrent_callouts_when_validating_should_reject() {
        let config = ExternalAuthConfig {
            enabled: true,
            url: "https://auth.example.com/verify".to_owned(),
            max_concurrent_callouts: 0,
            ..ExternalAuthConfig::default()
        };
        assert!(config.validate().is_err());
    }

    #[test]
    fn given_enabled_config_with_valid_url_when_validating_should_accept() {
        let config = ExternalAuthConfig {
            enabled: true,
            url: "https://auth.example.com/verify".to_owned(),
            ..ExternalAuthConfig::default()
        };
        assert!(config.validate().is_ok());
    }

    #[test]
    fn given_enabled_config_with_empty_credential_types_when_validating_should_reject() {
        let config = ExternalAuthConfig {
            enabled: true,
            url: "https://auth.example.com/verify".to_owned(),
            credential_types: Vec::new(),
            ..ExternalAuthConfig::default()
        };
        assert!(config.validate().is_err());
    }

    #[test]
    fn given_default_config_when_checking_forwarding_should_forward_only_passwords() {
        let config = ExternalAuthConfig {
            enabled: true,
            ..ExternalAuthConfig::default()
        };
        assert!(config.forwards(ExternalAuthCredentialType::UsernamePassword));
        assert!(!config.forwards(ExternalAuthCredentialType::PersonalAccessToken));
    }

    #[test]
    fn given_disabled_config_when_checking_forwarding_should_forward_nothing() {
        let config = ExternalAuthConfig {
            credential_types: vec![
                ExternalAuthCredentialType::UsernamePassword,
                ExternalAuthCredentialType::PersonalAccessToken,
            ],
            ..ExternalAuthConfig::default()
        };
        assert!(!config.forwards(ExternalAuthCredentialType::UsernamePassword));
        assert!(!config.forwards(ExternalAuthCredentialType::PersonalAccessToken));
    }

    #[test]
    fn given_config_without_credential_types_when_parsing_should_default_to_passwords() {
        let config: ExternalAuthConfig =
            serde_json::from_str(r#"{"enabled": true, "url": "https://auth.example.com"}"#)
                .unwrap();
        assert_eq!(
            config.credential_types,
            vec![ExternalAuthCredentialType::UsernamePassword]
        );
    }

    #[test]
    fn given_config_with_personal_access_token_when_parsing_should_forward_tokens() {
        let config: ExternalAuthConfig = serde_json::from_str(
            r#"{"enabled": true, "url": "https://auth.example.com",
                "credential_types": ["personal_access_token"]}"#,
        )
        .unwrap();
        assert!(config.forwards(ExternalAuthCredentialType::PersonalAccessToken));
        assert!(!config.forwards(ExternalAuthCredentialType::UsernamePassword));
    }
}
