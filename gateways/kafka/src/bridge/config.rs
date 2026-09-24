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

use iggy::prelude::IggyByteSize;
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
/// Iggy's default `message_bus.max_message_size`.
pub const DEFAULT_MAX_MESSAGE_SIZE: u64 = 64 * 1024 * 1024;

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
    /// Iggy's `message_bus.max_message_size`, in bytes. Larger partitions answer 10.
    pub max_message_size: u64,
}

impl IggyBridgeConfig {
    /// The complete set of `IGGY_KAFKA_*` vars this module reads. `main.rs`'s
    /// `reject_unknown_kafka_env_vars` checks this list *and* its own separate
    /// `KNOWN_KAFKA_ENV_VARS`, not one merged copy - a var added only here is already
    /// recognized there with no corresponding edit needed, and vice versa. A var this module
    /// reads still has to be listed *somewhere* the guard checks, or a typo silently no-ops
    /// instead of surfacing (`IGGY_KAFKA_` is a `SERVER_ALLOWED_ENV_PREFIXES` entry in
    /// `core/configs`, so the server's own unknown-variable check doesn't cover this namespace
    /// either).
    pub const KNOWN_ENV_VARS: &'static [&'static str] = &[
        "IGGY_KAFKA_IGGY_ADDR",
        "IGGY_KAFKA_IGGY_USERNAME",
        "IGGY_KAFKA_IGGY_PASSWORD",
        "IGGY_KAFKA_IGGY_STREAM",
        "IGGY_KAFKA_TOPIC_MAP_PATH",
        "IGGY_KAFKA_IGGY_MAX_MESSAGE_SIZE",
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
    /// Returns [`BridgeError::InvalidConfig`] if `IGGY_KAFKA_IGGY_PASSWORD` is unset or empty, if
    /// `IGGY_KAFKA_IGGY_USERNAME` is set but empty, or if `IGGY_KAFKA_TOPIC_MAP_PATH` is set but
    /// the file is missing, fails to parse, or fails validation (e.g. a malformed mapping key or
    /// an empty `default_stream`).
    pub fn from_env() -> Result<Self, BridgeError> {
        let address =
            std::env::var("IGGY_KAFKA_IGGY_ADDR").unwrap_or_else(|_| DEFAULT_IGGY_ADDR.to_string());
        let username = std::env::var("IGGY_KAFKA_IGGY_USERNAME")
            .unwrap_or_else(|_| DEFAULT_IGGY_USERNAME.to_string());
        // set-but-empty (`VAR=""`) is `Ok("")` from `env::var`, not `Err` - distinct from unset,
        // and unchecked here would otherwise pass both cleanly through to a login attempt neither
        // could ever satisfy.
        if username.is_empty() {
            return Err(BridgeError::InvalidConfig(
                "IGGY_KAFKA_IGGY_USERNAME must not be empty".to_string(),
            ));
        }
        let password = std::env::var("IGGY_KAFKA_IGGY_PASSWORD").map_err(|_| {
            BridgeError::InvalidConfig(
                "IGGY_KAFKA_IGGY_PASSWORD must be set - iggy-server generates a random root \
                 password unless started with --with-default-root-credentials, so there is no \
                 safe default to fall back to"
                    .to_string(),
            )
        })?;
        if password.is_empty() {
            return Err(BridgeError::InvalidConfig(
                "IGGY_KAFKA_IGGY_PASSWORD must not be empty".to_string(),
            ));
        }
        let stream_env = std::env::var("IGGY_KAFKA_IGGY_STREAM").ok();
        let topic_map_path = std::env::var("IGGY_KAFKA_TOPIC_MAP_PATH").ok();

        let topic_mapping = if let Some(path) = topic_map_path {
            if stream_env.is_some() {
                warn!(
                    "IGGY_KAFKA_IGGY_STREAM is set but ignored: IGGY_KAFKA_TOPIC_MAP_PATH's own \
                     default_stream takes precedence"
                );
            }
            TopicMapping::from_file(Path::new(&path))?
        } else {
            // Validated here, not unconditionally above: on the file branch above,
            // IGGY_KAFKA_IGGY_STREAM is ignored entirely (the warn! there already says so), so a
            // padded or empty value must not hard-fail startup when a topic-map file is also set.
            // TopicMapping::new below validates default_stream on this branch too, but its own
            // message says "topic mapping's default_stream", not IGGY_KAFKA_IGGY_STREAM, leaving
            // whoever reads the error to work out which env var that phrase actually refers to.
            // Checked here first so the message names the var an operator can actually go fix.
            if let Some(ref stream) = stream_env {
                validate_identifier_name("IGGY_KAFKA_IGGY_STREAM", stream)?;
            }
            TopicMapping::new(
                stream_env.unwrap_or_else(|| "kafka".to_string()),
                std::collections::HashMap::new(),
            )?
        };

        let max_message_size = match std::env::var("IGGY_KAFKA_IGGY_MAX_MESSAGE_SIZE") {
            Err(_) => DEFAULT_MAX_MESSAGE_SIZE,
            Ok(value) => value
                .parse::<IggyByteSize>()
                .ok()
                .map(|size| size.as_bytes_u64())
                .filter(|bytes| *bytes > 0)
                .ok_or_else(|| {
                    BridgeError::InvalidConfig(format!(
                        "IGGY_KAFKA_IGGY_MAX_MESSAGE_SIZE '{value}' is not a size, for example 64MiB"
                    ))
                })?,
        };

        Ok(Self {
            address,
            username,
            password: SecretString::from(password),
            topic_mapping,
            max_message_size,
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
            topic_mapping: TopicMapping::new("kafka".to_string(), std::collections::HashMap::new())
                .expect("valid mapping for this test's fixture data"),
            max_message_size: DEFAULT_MAX_MESSAGE_SIZE,
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
        // Safety: process-wide, not per-variable - env::set_var/remove_var race any concurrent
        // env read or write on another thread, not just ones touching these same vars.
        // #[serial] (this test's own attribute) is what prevents that.
        unsafe {
            std::env::set_var("IGGY_KAFKA_IGGY_PASSWORD", "iggy");
            std::env::remove_var("IGGY_KAFKA_IGGY_ADDR");
            std::env::remove_var("IGGY_KAFKA_IGGY_USERNAME");
            std::env::remove_var("IGGY_KAFKA_IGGY_STREAM");
            std::env::remove_var("IGGY_KAFKA_TOPIC_MAP_PATH");
            std::env::remove_var("IGGY_KAFKA_IGGY_MAX_MESSAGE_SIZE");
        }
        let result = IggyBridgeConfig::from_env();
        unsafe {
            std::env::remove_var("IGGY_KAFKA_IGGY_PASSWORD");
        }

        let config = result.expect("valid config from documented defaults alone");
        assert_eq!(config.max_message_size, DEFAULT_MAX_MESSAGE_SIZE);
        assert_eq!(config.address, "127.0.0.1:8090");
        assert_eq!(config.username, "iggy");
        assert_eq!(config.topic_mapping.default_stream(), "kafka");
        // No overrides in the no-file default: an arbitrary topic must resolve via the plain
        // default-stream rule, not fall through to some file-loaded override.
        assert_eq!(
            config.topic_mapping.resolve("anything"),
            ("kafka", "anything")
        );
    }

    #[test]
    #[serial]
    fn from_env_reads_the_iggy_message_size_cap() {
        // Safety: process-wide, not per-variable - see the note on
        // from_env_uses_documented_defaults_when_only_password_is_set above.
        unsafe {
            std::env::set_var("IGGY_KAFKA_IGGY_PASSWORD", "iggy");
            std::env::set_var("IGGY_KAFKA_IGGY_MAX_MESSAGE_SIZE", "1MiB");
        }
        let one_mebibyte = IggyBridgeConfig::from_env();
        unsafe {
            std::env::set_var("IGGY_KAFKA_IGGY_MAX_MESSAGE_SIZE", "lots");
        }
        let garbage = IggyBridgeConfig::from_env();
        unsafe {
            std::env::remove_var("IGGY_KAFKA_IGGY_PASSWORD");
            std::env::remove_var("IGGY_KAFKA_IGGY_MAX_MESSAGE_SIZE");
        }

        assert_eq!(one_mebibyte.unwrap().max_message_size, 1024 * 1024);
        assert!(matches!(garbage, Err(BridgeError::InvalidConfig(_))));
    }

    #[test]
    #[serial]
    fn from_env_rejects_missing_password() {
        // Safety: process-wide, not per-variable - see the note on
        // from_env_uses_documented_defaults_when_only_password_is_set above.
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
    fn from_env_rejects_empty_password() {
        // Safety: process-wide, not per-variable - see the note on
        // from_env_uses_documented_defaults_when_only_password_is_set above.
        unsafe {
            std::env::set_var("IGGY_KAFKA_IGGY_PASSWORD", "");
        }
        let result = IggyBridgeConfig::from_env();
        unsafe {
            std::env::remove_var("IGGY_KAFKA_IGGY_PASSWORD");
        }
        assert!(
            matches!(result, Err(BridgeError::InvalidConfig(_))),
            "set-but-empty (VAR=\"\") must not be treated as a usable password: {result:?}"
        );
    }

    #[test]
    #[serial]
    fn from_env_rejects_empty_username() {
        // Safety: process-wide, not per-variable - see the note on
        // from_env_uses_documented_defaults_when_only_password_is_set above.
        unsafe {
            std::env::set_var("IGGY_KAFKA_IGGY_PASSWORD", "iggy");
            std::env::set_var("IGGY_KAFKA_IGGY_USERNAME", "");
        }
        let result = IggyBridgeConfig::from_env();
        unsafe {
            std::env::remove_var("IGGY_KAFKA_IGGY_PASSWORD");
            std::env::remove_var("IGGY_KAFKA_IGGY_USERNAME");
        }
        assert!(
            matches!(result, Err(BridgeError::InvalidConfig(_))),
            "set-but-empty (VAR=\"\") must not skip the DEFAULT_IGGY_USERNAME fallback silently: \
             {result:?}"
        );
    }

    #[test]
    #[serial]
    fn from_env_rejects_missing_topic_map_file() {
        // Safety: process-wide, not per-variable - see the note on
        // from_env_uses_documented_defaults_when_only_password_is_set above.
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

        // Safety: process-wide, not per-variable - see the note on
        // from_env_uses_documented_defaults_when_only_password_is_set above.
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
            result.expect("valid config").topic_mapping.default_stream(),
            "from-toml"
        );
    }

    #[test]
    #[serial]
    fn from_env_ignores_invalid_stream_env_var_when_topic_map_file_is_set() {
        // Regression test: IGGY_KAFKA_IGGY_STREAM is documented (README.md) and warn!()-logged as
        // fully ignored once IGGY_KAFKA_TOPIC_MAP_PATH is set - a padded/empty value there must
        // not hard-fail startup on a var the map-file branch never reads.
        let file = tempfile::NamedTempFile::new().expect("create temp file");
        std::fs::write(file.path(), "default_stream = \"from-toml\"\n").expect("write temp file");

        // Safety: process-wide, not per-variable - see the note on
        // from_env_uses_documented_defaults_when_only_password_is_set above.
        unsafe {
            std::env::set_var("IGGY_KAFKA_IGGY_PASSWORD", "iggy");
            std::env::set_var("IGGY_KAFKA_IGGY_STREAM", " padded and invalid ");
            std::env::set_var("IGGY_KAFKA_TOPIC_MAP_PATH", file.path());
        }
        let result = IggyBridgeConfig::from_env();
        unsafe {
            std::env::remove_var("IGGY_KAFKA_IGGY_PASSWORD");
            std::env::remove_var("IGGY_KAFKA_IGGY_STREAM");
            std::env::remove_var("IGGY_KAFKA_TOPIC_MAP_PATH");
        }

        assert_eq!(
            result
                .expect("an ignored var's own invalidity must not fail startup")
                .topic_mapping
                .default_stream(),
            "from-toml"
        );
    }

    #[test]
    #[serial]
    fn from_env_rejects_empty_stream_env_var() {
        // Safety: process-wide, not per-variable - see the note on
        // from_env_uses_documented_defaults_when_only_password_is_set above.
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
        // Safety: process-wide, not per-variable - see the note on
        // from_env_uses_documented_defaults_when_only_password_is_set above.
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
