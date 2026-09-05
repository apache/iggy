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
use crate::bridge::topic_map::TopicMapping;

const DEFAULT_IGGY_ADDR: &str = "127.0.0.1:8090";
/// Matches the Iggy server's own default root user - not a made-up example, the same default
/// every fresh `iggy-server` and every CLI quick-start in this repo uses.
const DEFAULT_IGGY_USERNAME: &str = "iggy";
const DEFAULT_IGGY_PASSWORD: &str = "iggy";

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
    /// out-of-the-box address and root credentials.
    ///
    /// `IGGY_KAFKA_IGGY_STREAM` and `IGGY_KAFKA_TOPIC_MAP_PATH` both influence the mapping's
    /// default stream; when both are set, the TOML file's own `default_stream` wins and
    /// `IGGY_KAFKA_IGGY_STREAM` is ignored entirely for the topics it covers (a TOML file is a
    /// complete mapping document, not an overlay) - a `warn!` fires so that isn't silently
    /// mysterious to whoever set the env var expecting it to matter.
    ///
    /// # Errors
    ///
    /// Returns [`BridgeError::InvalidConfig`] if `IGGY_KAFKA_TOPIC_MAP_PATH` is set but the file
    /// is missing or fails to parse.
    pub fn from_env() -> Result<Self, BridgeError> {
        let address =
            std::env::var("IGGY_KAFKA_IGGY_ADDR").unwrap_or_else(|_| DEFAULT_IGGY_ADDR.to_string());
        let username = std::env::var("IGGY_KAFKA_IGGY_USERNAME")
            .unwrap_or_else(|_| DEFAULT_IGGY_USERNAME.to_string());
        let password = std::env::var("IGGY_KAFKA_IGGY_PASSWORD")
            .unwrap_or_else(|_| DEFAULT_IGGY_PASSWORD.to_string());
        let stream_env = std::env::var("IGGY_KAFKA_IGGY_STREAM").ok();
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

    /// Every var `from_env` actually reads must be declared, or a future rename here silently
    /// desyncs from the allowlist (as opposed to `KNOWN_ENV_VARS` listing a var this module
    /// never reads, which the compiler can't catch either but is far less consequential).
    #[test]
    fn known_env_vars_covers_every_var_from_env_reads() {
        for var in [
            "IGGY_KAFKA_IGGY_ADDR",
            "IGGY_KAFKA_IGGY_USERNAME",
            "IGGY_KAFKA_IGGY_PASSWORD",
            "IGGY_KAFKA_IGGY_STREAM",
            "IGGY_KAFKA_TOPIC_MAP_PATH",
        ] {
            assert!(
                IggyBridgeConfig::KNOWN_ENV_VARS.contains(&var),
                "{var} read by from_env() but missing from KNOWN_ENV_VARS"
            );
        }
    }

    #[test]
    fn from_env_rejects_missing_topic_map_file() {
        // Safety: single-threaded within this function; no other test in this crate touches
        // IGGY_KAFKA_TOPIC_MAP_PATH.
        unsafe {
            std::env::set_var("IGGY_KAFKA_TOPIC_MAP_PATH", "/nonexistent/topic_map.toml");
        }
        let result = IggyBridgeConfig::from_env();
        unsafe {
            std::env::remove_var("IGGY_KAFKA_TOPIC_MAP_PATH");
        }
        assert!(matches!(result, Err(BridgeError::InvalidConfig(_))));
    }

    #[test]
    fn from_env_prefers_topic_map_files_default_stream_over_env_var() {
        let file = tempfile::NamedTempFile::new().expect("create temp file");
        std::fs::write(file.path(), "default_stream = \"from-toml\"\n").expect("write temp file");

        // Safety: single-threaded within this function; no other test in this crate touches
        // IGGY_KAFKA_IGGY_STREAM or IGGY_KAFKA_TOPIC_MAP_PATH.
        unsafe {
            std::env::set_var("IGGY_KAFKA_IGGY_STREAM", "from-env");
            std::env::set_var("IGGY_KAFKA_TOPIC_MAP_PATH", file.path());
        }
        let result = IggyBridgeConfig::from_env();
        unsafe {
            std::env::remove_var("IGGY_KAFKA_IGGY_STREAM");
            std::env::remove_var("IGGY_KAFKA_TOPIC_MAP_PATH");
        }

        assert_eq!(
            result.expect("valid config").topic_mapping.default_stream,
            "from-toml"
        );
    }
}
