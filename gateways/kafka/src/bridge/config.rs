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

use secrecy::{ExposeSecret, SecretString};

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
        let default_stream =
            std::env::var("IGGY_KAFKA_IGGY_STREAM").unwrap_or_else(|_| "kafka".to_string());

        let topic_mapping = match std::env::var("IGGY_KAFKA_TOPIC_MAP_PATH") {
            Ok(path) => TopicMapping::from_file(Path::new(&path))?,
            Err(_) => TopicMapping {
                default_stream,
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

    /// Builds the `iggy://` connection string the SDK's `IggyClientBuilder::from_connection_string`
    /// expects, embedding credentials. Never pass the result to a `tracing`/`format!` call that
    /// might reach a log line - it exposes `password` in full, unlike this struct's own `Debug`.
    ///
    /// Pins `reconnection_retries` to `RECONNECTION_RETRIES` rather than the SDK's own default
    /// (`TcpClientReconnectionConfig::default()` is `max_retries: None` - unlimited, one dial per
    /// second, forever). A Kafka client already retries at the wire-protocol level once a handler
    /// maps a bridge failure to a retriable error code; the bridge blocking a request task inside
    /// an unbounded internal reconnect loop would just add a second, invisible retry layer
    /// underneath that one instead of surfacing the failure so the mapped code can be sent.
    #[must_use]
    pub fn connection_string(&self) -> String {
        format!(
            "iggy://{}:{}@{}?reconnection_retries={RECONNECTION_RETRIES}",
            self.username,
            self.password.expose_secret(),
            self.address
        )
    }
}

/// Passes attempted, after the first, before `IggyBridge::connect` gives up and returns
/// `Err` - see [`IggyBridgeConfig::connection_string`]'s doc comment for why this is bounded
/// at all. At the default `reconnection_interval` (1s), a fully unreachable address fails in a
/// few seconds rather than hanging.
const RECONNECTION_RETRIES: u32 = 3;

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
    fn connection_string_embeds_credentials_and_address() {
        let config = test_config();
        assert_eq!(
            config.connection_string(),
            "iggy://iggy:iggy@127.0.0.1:8090?reconnection_retries=3"
        );
    }

    #[test]
    fn debug_output_does_not_expose_password() {
        let config = test_config();
        let debug_output = format!("{config:?}");
        assert!(
            !debug_output.contains("iggy://iggy:iggy"),
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
}
