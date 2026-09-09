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

use std::path::Path;

use secrecy::SecretString;
use tracing::warn;

use crate::bridge::error::BridgeError;
use crate::bridge::topic_map::{TopicMapping, validate_identifier_name};

const DEFAULT_IGGY_ADDR: &str = "127.0.0.1:8090";
/// Matches `DEFAULT_ROOT_USERNAME` (`core/server/src/boot/credentials.rs`) - the root user's
/// username is always `iggy` regardless of how its password was provisioned, so defaulting this
/// one field is safe. The password is a different story - see why there is no
/// `DEFAULT_IGGY_PASSWORD` at [`IggyBridgeConfig::from_env`].
const DEFAULT_IGGY_USERNAME: &str = "iggy";

/// Connection + topic-mapping config for [`IggyBridge`](crate::bridge::iggy_bridge::IggyBridge).
///
/// `Debug` is safe to derive: `password` is `SecretString`, which redacts on `Debug` by design
/// (`secrecy` crate) - never add a plain `String` credential field here without the same
/// treatment (see `connector-pr-review` blocker B1 in the connectors subsystem for why).
#[derive(Debug, Clone)]
pub struct IggyBridgeConfig {
    pub address: String,
    pub username: String,
    pub password: SecretString,
    pub topic_mapping: TopicMapping,
}

impl IggyBridgeConfig {
    /// The complete set of `IGGY_KAFKA_*` vars this module reads. Mirrors `main.rs`'s
    /// `KNOWN_KAFKA_ENV_VARS` guard - add new vars to both, or a typo silently no-ops instead of
    /// surfacing (`IGGY_KAFKA_` is a `DELEGATED_ENV_VAR_PREFIXES` entry in `core/configs`, so the
    /// central provider's own typo-detection doesn't cover this namespace either).
    pub const KNOWN_ENV_VARS: &'static [&'static str] = &[
        "IGGY_KAFKA_IGGY_ADDR",
        "IGGY_KAFKA_IGGY_USERNAME",
        "IGGY_KAFKA_IGGY_PASSWORD",
        "IGGY_KAFKA_IGGY_STREAM",
        "IGGY_KAFKA_TOPIC_MAP_PATH",
    ];

    /// Builds config from `IGGY_KAFKA_*` env vars, defaulting to the Iggy server's own
    /// out-of-the-box address and root username.
    ///
    /// `IGGY_KAFKA_IGGY_PASSWORD` has no default and must be set explicitly. `iggy-server` only
    /// provisions the well-known `iggy`/`iggy` root credentials when started with
    /// `--with-default-root-credentials` (`args.rs`, itself documented "INSECURE - FOR DEVELOPMENT
    /// ONLY!"); without that flag it generates a random password (`boot/credentials.rs`) that no
    /// constant here could ever guess. Defaulting the password to `"iggy"` would make this bridge
    /// silently authenticate as root, with the credential in no config file and no log, on any
    /// server that does happen to run dev-flagged - the failure mode of getting it wrong is a
    /// loud, immediate auth rejection instead.
    ///
    /// `IGGY_KAFKA_IGGY_STREAM` and `IGGY_KAFKA_TOPIC_MAP_PATH` both influence the mapping's
    /// default stream; when both are set, the TOML file's own `default_stream` wins and
    /// `IGGY_KAFKA_IGGY_STREAM` is ignored entirely for the topics it covers (a TOML file is a
    /// complete mapping document, not an overlay) - a `warn!` fires so that isn't silently
    /// mysterious to whoever set the env var expecting it to matter.
    ///
    /// # Errors
    ///
    /// Returns [`BridgeError::InvalidConfig`] if `IGGY_KAFKA_IGGY_PASSWORD` is unset, or if
    /// `IGGY_KAFKA_TOPIC_MAP_PATH` is set but the file is missing or fails to parse.
    pub fn from_env() -> Result<Self, BridgeError> {
        let address =
            std::env::var("IGGY_KAFKA_IGGY_ADDR").unwrap_or_else(|_| DEFAULT_IGGY_ADDR.to_string());
        let username = std::env::var("IGGY_KAFKA_IGGY_USERNAME")
            .unwrap_or_else(|_| DEFAULT_IGGY_USERNAME.to_string());
        let password = std::env::var("IGGY_KAFKA_IGGY_PASSWORD").map_err(|_| {
            BridgeError::InvalidConfig(
                "IGGY_KAFKA_IGGY_PASSWORD must be set - iggy-server generates a random root \
                 password unless started with --with-default-root-credentials, so there is no \
                 safe default to fall back to"
                    .to_string(),
            )
        })?;
        let stream_env = std::env::var("IGGY_KAFKA_IGGY_STREAM").ok();
        // Caught here, not left to TopicMapping::from_toml_str's own validation: that only runs
        // when IGGY_KAFKA_TOPIC_MAP_PATH is set, since the no-file branch below builds a
        // TopicMapping directly rather than through the validating constructor. An invalid
        // (empty, whitespace-padded, or oversized) IGGY_KAFKA_IGGY_STREAM would otherwise pass
        // from_env cleanly and only fail much later, deep in the first ensure_stream_and_topic
        // call, as an opaque Identifier::named error - or, for whitespace, not fail at all and
        // silently create a stream named e.g. " kafka ".
        if let Some(ref stream) = stream_env {
            validate_identifier_name("IGGY_KAFKA_IGGY_STREAM", stream)?;
        }
        let topic_map_path = std::env::var("IGGY_KAFKA_TOPIC_MAP_PATH").ok();

        let topic_mapping = match topic_map_path {
            Some(path) => {
                if stream_env.is_some() {
                    warn!(
                        "IGGY_KAFKA_IGGY_STREAM is set but ignored: IGGY_KAFKA_TOPIC_MAP_PATH's \
                         own default_stream takes precedence"
                    );
                }
                TopicMapping::from_file(Path::new(&path))?
            }
            None => TopicMapping {
                default_stream: stream_env.unwrap_or_else(|| "kafka".to_string()),
                topics: std::collections::HashMap::new(),
            },
        };

        Ok(Self {
            address,
            username,
            password: SecretString::from(password),
            topic_mapping,
        })
    }
}

#[cfg(test)]
mod tests {
    use serial_test::serial;

    use super::*;

    fn test_config() -> IggyBridgeConfig {
        IggyBridgeConfig {
            address: "127.0.0.1:8090".to_string(),
            username: "iggy".to_string(),
            password: SecretString::from("iggy"),
            topic_mapping: TopicMapping {
                default_stream: "kafka".to_string(),
                topics: std::collections::HashMap::new(),
            },
        }
    }

    #[test]
    fn debug_output_does_not_expose_password() {
        // A distinctive value, not the shared fixture's "iggy" - that string also appears as the
        // username and inside the struct's own name, which would make a substring check here
        // pass trivially regardless of whether the password field itself is actually redacted.
        let mut config = test_config();
        config.password = SecretString::from("correct-horse-battery-staple");
        let debug_output = format!("{config:?}");
        assert!(
            !debug_output.contains("correct-horse-battery-staple"),
            "Debug output must not expose the plaintext password: {debug_output}"
        );
    }

    /// Regression test for coverage: every other `from_env` test sets `IGGY_KAFKA_IGGY_ADDR`/
    /// `_USERNAME`/`_STREAM` explicitly (or goes through the topic-map-file branch), so the four
    /// documented defaults - address, username, no-file branch, and `default_stream` - never
    /// actually ran.
    #[test]
    #[serial]
    fn from_env_uses_documented_defaults_when_only_password_is_set() {
        // Safety: single-threaded within this function; no other test in this crate touches
        // these four vars.
        unsafe {
            std::env::set_var("IGGY_KAFKA_IGGY_PASSWORD", "iggy");
            std::env::remove_var("IGGY_KAFKA_IGGY_ADDR");
            std::env::remove_var("IGGY_KAFKA_IGGY_USERNAME");
            std::env::remove_var("IGGY_KAFKA_IGGY_STREAM");
            std::env::remove_var("IGGY_KAFKA_TOPIC_MAP_PATH");
        }
        let result = IggyBridgeConfig::from_env();
        unsafe {
            std::env::remove_var("IGGY_KAFKA_IGGY_PASSWORD");
        }

        let config = result.expect("valid config from documented defaults alone");
        assert_eq!(config.address, "127.0.0.1:8090");
        assert_eq!(config.username, "iggy");
        assert_eq!(config.topic_mapping.default_stream, "kafka");
        assert!(config.topic_mapping.topics.is_empty());
    }

    #[test]
    #[serial]
    fn from_env_rejects_missing_password() {
        // Safety: single-threaded within this function; no other test in this crate touches
        // IGGY_KAFKA_IGGY_PASSWORD.
        unsafe {
            std::env::remove_var("IGGY_KAFKA_IGGY_PASSWORD");
        }
        let result = IggyBridgeConfig::from_env();
        assert!(
            matches!(result, Err(BridgeError::InvalidConfig(_))),
            "no default password must mean no default: {result:?}"
        );
    }

    #[test]
    #[serial]
    fn from_env_rejects_missing_topic_map_file() {
        // Safety: single-threaded within this function; no other test in this crate touches
        // these three vars.
        unsafe {
            std::env::set_var("IGGY_KAFKA_IGGY_PASSWORD", "iggy");
            std::env::set_var("IGGY_KAFKA_TOPIC_MAP_PATH", "/nonexistent/topic_map.toml");
        }
        let result = IggyBridgeConfig::from_env();
        unsafe {
            std::env::remove_var("IGGY_KAFKA_IGGY_PASSWORD");
            std::env::remove_var("IGGY_KAFKA_TOPIC_MAP_PATH");
        }
        assert!(matches!(result, Err(BridgeError::InvalidConfig(_))));
    }

    #[test]
    #[serial]
    fn from_env_prefers_topic_map_files_default_stream_over_env_var() {
        let file = tempfile::NamedTempFile::new().expect("create temp file");
        std::fs::write(file.path(), "default_stream = \"from-toml\"\n").expect("write temp file");

        // Safety: single-threaded within this function; no other test in this crate touches
        // these three vars.
        unsafe {
            std::env::set_var("IGGY_KAFKA_IGGY_PASSWORD", "iggy");
            std::env::set_var("IGGY_KAFKA_IGGY_STREAM", "from-env");
            std::env::set_var("IGGY_KAFKA_TOPIC_MAP_PATH", file.path());
        }
        let result = IggyBridgeConfig::from_env();
        unsafe {
            std::env::remove_var("IGGY_KAFKA_IGGY_PASSWORD");
            std::env::remove_var("IGGY_KAFKA_IGGY_STREAM");
            std::env::remove_var("IGGY_KAFKA_TOPIC_MAP_PATH");
        }

        assert_eq!(
            result.expect("valid config").topic_mapping.default_stream,
            "from-toml"
        );
    }

    #[test]
    #[serial]
    fn from_env_rejects_empty_stream_env_var() {
        // Safety: single-threaded within this function; no other test in this crate touches
        // these two vars.
        unsafe {
            std::env::set_var("IGGY_KAFKA_IGGY_PASSWORD", "iggy");
            std::env::set_var("IGGY_KAFKA_IGGY_STREAM", "");
        }
        let result = IggyBridgeConfig::from_env();
        unsafe {
            std::env::remove_var("IGGY_KAFKA_IGGY_PASSWORD");
            std::env::remove_var("IGGY_KAFKA_IGGY_STREAM");
        }
        assert!(matches!(result, Err(BridgeError::InvalidConfig(_))));
    }

    #[test]
    #[serial]
    fn from_env_rejects_stream_env_var_with_whitespace_instead_of_trimming_it() {
        // Safety: single-threaded within this function; no other test in this crate touches
        // these two vars.
        unsafe {
            std::env::set_var("IGGY_KAFKA_IGGY_PASSWORD", "iggy");
            std::env::set_var("IGGY_KAFKA_IGGY_STREAM", " kafka ");
        }
        let result = IggyBridgeConfig::from_env();
        unsafe {
            std::env::remove_var("IGGY_KAFKA_IGGY_PASSWORD");
            std::env::remove_var("IGGY_KAFKA_IGGY_STREAM");
        }
        assert!(
            matches!(result, Err(BridgeError::InvalidConfig(_))),
            "must reject, not silently trim, and store a stream named ' kafka '"
        );
    }
}
