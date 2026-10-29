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
use configs::ConfigEnv;
use iggy_common::{IggyDuration, Validatable};
use serde::{Deserialize, Serialize};
use serde_with::DisplayFromStr;
use serde_with::serde_as;

/// External authentication callout configuration.
///
/// When enabled, login attempts for usernames not found in the local user
/// store are forwarded to an external HTTP service. The service maps the
/// caller to a pre-provisioned Iggy username or denies the login. Local
/// users never hit the callout. The credential (password) is forwarded
/// so the external service can verify the requester.
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
    #[serde_as(as = "DisplayFromStr")]
    #[serde(default = "default_external_auth_timeout")]
    #[config_env(leaf)]
    pub timeout: IggyDuration,
    #[serde(default = "default_max_concurrent_callouts")]
    pub max_concurrent_callouts: u32,
}

impl Default for ExternalAuthConfig {
    fn default() -> Self {
        Self {
            enabled: false,
            url: String::new(),
            timeout: default_external_auth_timeout(),
            max_concurrent_callouts: default_max_concurrent_callouts(),
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
        if !self.url.starts_with("http://") && !self.url.starts_with("https://") {
            eprintln!("external_auth.url must start with http:// or https://");
            return Err(ConfigurationError::InvalidConfigurationValue);
        }
        // Catch bare schemes ("http://") with no host.
        let after_scheme = self.url.split_once("://").map_or("", |(_, rest)| rest);
        if after_scheme.is_empty() || after_scheme.starts_with('/') {
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
        Ok(())
    }
}

fn default_external_auth_timeout() -> IggyDuration {
    IggyDuration::new_from_secs(5)
}

const fn default_max_concurrent_callouts() -> u32 {
    64
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
    fn given_enabled_config_with_scheme_and_path_only_when_validating_should_reject() {
        let config = ExternalAuthConfig {
            enabled: true,
            url: "http:///path".to_owned(),
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
}
