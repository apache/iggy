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

use crate::{
    ConnectionStringOptions, IggyDuration, IggyError, NonZeroIggyDuration, PendingOperationsConfig,
    WebSocketClientReconnectionConfig,
};
use std::str::FromStr;

#[derive(Debug, Clone)]
pub struct WebSocketConnectionStringOptions {
    pending_operations: PendingOperationsConfig,
    heartbeat_interval: NonZeroIggyDuration,
    reconnection: WebSocketClientReconnectionConfig,

    read_buffer_size: Option<usize>,
    write_buffer_size: Option<usize>,
    max_write_buffer_size: Option<usize>,
    max_message_size: Option<usize>,
    max_frame_size: Option<usize>,

    accept_unmasked_frames: Option<bool>,

    tls_enabled: bool,
    tls_domain: String,
    tls_ca_file: Option<String>,
    tls_validate_certificate: bool,
}

impl WebSocketConnectionStringOptions {
    pub fn pending_operations(&self) -> PendingOperationsConfig {
        self.pending_operations
    }

    pub fn heartbeat_interval(&self) -> NonZeroIggyDuration {
        self.heartbeat_interval
    }

    pub fn reconnection(&self) -> &WebSocketClientReconnectionConfig {
        &self.reconnection
    }

    pub fn read_buffer_size(&self) -> Option<usize> {
        self.read_buffer_size
    }

    pub fn write_buffer_size(&self) -> Option<usize> {
        self.write_buffer_size
    }

    pub fn max_write_buffer_size(&self) -> Option<usize> {
        self.max_write_buffer_size
    }

    pub fn max_message_size(&self) -> Option<usize> {
        self.max_message_size
    }

    pub fn max_frame_size(&self) -> Option<usize> {
        self.max_frame_size
    }

    pub fn accept_unmasked_frames(&self) -> Option<bool> {
        self.accept_unmasked_frames
    }

    pub fn tls_enabled(&self) -> bool {
        self.tls_enabled
    }

    pub fn tls_domain(&self) -> &str {
        &self.tls_domain
    }

    pub fn tls_ca_file(&self) -> Option<&str> {
        self.tls_ca_file.as_deref()
    }

    pub fn tls_validate_certificate(&self) -> bool {
        self.tls_validate_certificate
    }
}

impl ConnectionStringOptions for WebSocketConnectionStringOptions {
    fn retries(&self) -> Option<u32> {
        self.reconnection.max_retries
    }

    fn heartbeat_interval(&self) -> NonZeroIggyDuration {
        self.heartbeat_interval
    }

    fn parse_options(options_str: &str) -> Result<Self, IggyError> {
        let mut parsed_options = WebSocketConnectionStringOptions::default();

        if options_str.is_empty() {
            return Ok(parsed_options);
        }

        for option in options_str.split('&') {
            let parts: Vec<&str> = option.split('=').collect();
            if parts.len() != 2 {
                return Err(IggyError::InvalidConnectionString);
            }

            match parts[0] {
                "max_pending_operations" => {
                    parsed_options.pending_operations.max_operations = parts[1]
                        .parse()
                        .map_err(|_| IggyError::InvalidConnectionString)?;
                }
                "max_pending_operation_bytes" => {
                    parsed_options.pending_operations.max_bytes = parts[1]
                        .parse()
                        .map_err(|_| IggyError::InvalidConnectionString)?;
                }
                "heartbeat_interval" => {
                    parsed_options.heartbeat_interval = NonZeroIggyDuration::from_str(parts[1])
                        .map_err(|_| IggyError::InvalidConnectionString)?;
                }
                "reconnection_max_retries" | "reconnection_retries" => {
                    if parts[0] == "reconnection_retries" {
                        tracing::warn!(
                            "WebSocket connection string option `reconnection_retries` is deprecated; use `reconnection_max_retries` instead"
                        );
                    }
                    let retries = match parts[1] {
                        "unlimited" => None,
                        val => Some(
                            val.parse::<u32>()
                                .map_err(|_| IggyError::InvalidConnectionString)?,
                        ),
                    };
                    parsed_options.reconnection.max_retries = retries;
                }
                "reconnection_interval" => {
                    parsed_options.reconnection.interval = NonZeroIggyDuration::from_str(parts[1])
                        .map_err(|_| IggyError::InvalidConnectionString)?;
                }
                "reestablish_after" => {
                    parsed_options.reconnection.reestablish_after =
                        IggyDuration::from_str(parts[1])
                            .map_err(|_| IggyError::InvalidConnectionString)?;
                }

                "read_buffer_size" => {
                    let size = parts[1]
                        .parse::<usize>()
                        .map_err(|_| IggyError::InvalidConnectionString)?;
                    parsed_options.read_buffer_size = Some(size);
                }
                "write_buffer_size" => {
                    let size = parts[1]
                        .parse::<usize>()
                        .map_err(|_| IggyError::InvalidConnectionString)?;
                    parsed_options.write_buffer_size = Some(size);
                }
                "max_write_buffer_size" => {
                    let size = parts[1]
                        .parse::<usize>()
                        .map_err(|_| IggyError::InvalidConnectionString)?;
                    parsed_options.max_write_buffer_size = Some(size);
                }
                "max_message_size" => {
                    let size = parts[1]
                        .parse::<usize>()
                        .map_err(|_| IggyError::InvalidConnectionString)?;
                    parsed_options.max_message_size = Some(size);
                }
                "max_frame_size" => {
                    let size = parts[1]
                        .parse::<usize>()
                        .map_err(|_| IggyError::InvalidConnectionString)?;
                    parsed_options.max_frame_size = Some(size);
                }

                "accept_unmasked_frames" => {
                    parsed_options.accept_unmasked_frames = Some(parts[1] == "true");
                }

                "tls" => {
                    parsed_options.tls_enabled = parts[1] == "true";
                }
                "tls_domain" => {
                    parsed_options.tls_domain = parts[1].to_string();
                }
                "tls_ca_file" => {
                    parsed_options.tls_ca_file = Some(parts[1].to_string());
                }
                "tls_validate_certificate" => {
                    parsed_options.tls_validate_certificate = parts[1]
                        .parse::<bool>()
                        .map_err(|_| IggyError::InvalidConnectionString)?;
                }

                _ => return Err(IggyError::InvalidConnectionString),
            }
        }
        Ok(parsed_options)
    }
}

impl Default for WebSocketConnectionStringOptions {
    fn default() -> Self {
        WebSocketConnectionStringOptions {
            pending_operations: PendingOperationsConfig::default(),
            heartbeat_interval: NonZeroIggyDuration::from_str("5s").unwrap(),
            reconnection: WebSocketClientReconnectionConfig::default(),
            read_buffer_size: None,
            write_buffer_size: None,
            max_write_buffer_size: None,
            max_message_size: None,
            max_frame_size: None,
            accept_unmasked_frames: None,
            tls_enabled: false,
            tls_domain: "".to_string(),
            tls_ca_file: None,
            tls_validate_certificate: false,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::fmt;
    use std::sync::{Arc, Mutex};
    use std::time::Duration;
    use tracing::field::{Field, Visit};
    use tracing::span::{Attributes, Id, Record};
    use tracing::{Event, Level, Metadata, Subscriber};

    #[test]
    fn given_valid_retry_options_when_parsing_should_use_the_last_value() {
        for (query, expected) in [
            ("", None),
            ("reconnection_max_retries=5", Some(5)),
            ("reconnection_retries=5", Some(5)),
            ("reconnection_retries=3&reconnection_max_retries=8", Some(8)),
            ("reconnection_max_retries=8&reconnection_retries=3", Some(3)),
            (
                "reconnection_max_retries=1&reconnection_max_retries=2",
                Some(2),
            ),
            ("reconnection_retries=1&reconnection_retries=2", Some(2)),
            (
                "reconnection_retries=3&reconnection_max_retries=8&reconnection_retries=unlimited",
                None,
            ),
            ("reconnection_max_retries=0", Some(0)),
            ("reconnection_max_retries=4294967295", Some(u32::MAX)),
        ] {
            let options = WebSocketConnectionStringOptions::parse_options(query).unwrap();
            assert_eq!(options.retries(), expected, "{query}");
        }
    }

    #[test]
    fn given_an_invalid_retry_occurrence_when_parsing_should_reject_the_query() {
        for query in [
            "reconnection_max_retries=invalid",
            "reconnection_retries=-1",
            "reconnection_max_retries=4294967296",
            "reconnection_max_retries=",
            "reconnection_max_retries= 5",
            "reconnection_retries=invalid&reconnection_max_retries=5",
            "reconnection_max_retries=5&reconnection_retries=invalid",
        ] {
            assert_eq!(
                WebSocketConnectionStringOptions::parse_options(query).unwrap_err(),
                IggyError::InvalidConnectionString,
                "{query}"
            );
        }
    }

    #[test]
    fn given_cooldown_options_when_parsing_should_preserve_duration_rules() {
        for (query, expected) in [
            ("", Duration::from_secs(5)),
            ("reestablish_after=500ms", Duration::from_millis(500)),
            ("reestablish_after=0", Duration::ZERO),
        ] {
            let options = WebSocketConnectionStringOptions::parse_options(query).unwrap();
            assert_eq!(
                options.reconnection().reestablish_after.get_duration(),
                expected
            );
        }
        for query in [
            "reestablish_after=500",
            "reestablish_after=invalid",
            "reestablish_after=",
        ] {
            assert_eq!(
                WebSocketConnectionStringOptions::parse_options(query).unwrap_err(),
                IggyError::InvalidConnectionString,
                "{query}"
            );
        }
    }

    #[test]
    fn given_tls_validation_options_when_parsing_should_accept_exact_booleans() {
        for (query, expected) in [
            ("", false),
            ("tls_validate_certificate=true", true),
            ("tls_validate_certificate=false", false),
        ] {
            let options = WebSocketConnectionStringOptions::parse_options(query).unwrap();
            assert_eq!(options.tls_validate_certificate(), expected);
        }
        for value in ["ture", "yes", "1", "TRUE", "", "invalid", "true ", "%74rue"] {
            let query = format!("tls_validate_certificate={value}");
            assert_eq!(
                WebSocketConnectionStringOptions::parse_options(&query).unwrap_err(),
                IggyError::InvalidConnectionString,
                "{query}"
            );
        }
        assert_eq!(
            WebSocketConnectionStringOptions::parse_options(
                "tls_validate_certificate=ture&tls_validate_certificate=true"
            )
            .unwrap_err(),
            IggyError::InvalidConnectionString,
        );
    }

    #[derive(Clone, Default)]
    struct Warnings(Arc<Mutex<Vec<String>>>);

    impl Subscriber for Warnings {
        fn enabled(&self, metadata: &Metadata<'_>) -> bool {
            *metadata.level() == Level::WARN
        }

        fn new_span(&self, _: &Attributes<'_>) -> Id {
            Id::from_u64(1)
        }

        fn record(&self, _: &Id, _: &Record<'_>) {}
        fn record_follows_from(&self, _: &Id, _: &Id) {}
        fn enter(&self, _: &Id) {}
        fn exit(&self, _: &Id) {}

        fn event(&self, event: &Event<'_>) {
            if self.enabled(event.metadata()) {
                event.record(&mut self.clone());
            }
        }
    }

    impl Visit for Warnings {
        fn record_debug(&mut self, field: &Field, value: &dyn fmt::Debug) {
            if field.name() == "message" {
                self.0.lock().unwrap().push(format!("{value:?}"));
            }
        }
    }

    #[test]
    fn given_deprecated_retry_options_when_parsing_should_warn_even_if_overridden() {
        let warnings = Warnings::default();
        tracing::subscriber::with_default(warnings.clone(), || {
            WebSocketConnectionStringOptions::parse_options("reconnection_max_retries=5").unwrap();
            assert!(warnings.0.lock().unwrap().is_empty());

            WebSocketConnectionStringOptions::parse_options(
                "reconnection_retries=1&reconnection_retries=2&reconnection_max_retries=3",
            )
            .unwrap();
            assert!(
                WebSocketConnectionStringOptions::parse_options("reconnection_retries=invalid")
                    .is_err()
            );
        });
        let messages = warnings.0.lock().unwrap();
        assert_eq!(messages.len(), 3);
        for message in messages.iter() {
            assert!(
                message.contains("`reconnection_retries` is deprecated"),
                "{message}"
            );
            assert!(
                message.contains("use `reconnection_max_retries` instead"),
                "{message}"
            );
        }
    }
}
