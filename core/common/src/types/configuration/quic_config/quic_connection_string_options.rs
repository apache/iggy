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
    QuicClientReconnectionConfig,
};
use iggy_binary_protocol::MAX_RECORD_REPLY_SIZE;
use std::str::FromStr;
use tracing::warn;

#[derive(Debug)]
pub struct QuicConnectionStringOptions {
    pending_operations: PendingOperationsConfig,
    reconnection: QuicClientReconnectionConfig,
    response_buffer_size: u64,
    max_concurrent_bidi_streams: u64,
    datagram_send_buffer_size: u64,
    initial_mtu: u16,
    send_window: u64,
    receive_window: u64,
    keep_alive_interval: u64,
    max_idle_timeout: u64,
    validate_certificate: bool,
    heartbeat_interval: NonZeroIggyDuration,
}

impl QuicConnectionStringOptions {
    pub fn pending_operations(&self) -> PendingOperationsConfig {
        self.pending_operations
    }

    pub fn reconnection(&self) -> &QuicClientReconnectionConfig {
        &self.reconnection
    }

    pub fn response_buffer_size(&self) -> u64 {
        self.response_buffer_size
    }

    pub fn max_concurrent_bidi_streams(&self) -> u64 {
        self.max_concurrent_bidi_streams
    }

    pub fn datagram_send_buffer_size(&self) -> u64 {
        self.datagram_send_buffer_size
    }

    pub fn initial_mtu(&self) -> u16 {
        self.initial_mtu
    }

    pub fn send_window(&self) -> u64 {
        self.send_window
    }

    pub fn receive_window(&self) -> u64 {
        self.receive_window
    }

    pub fn keep_alive_interval(&self) -> u64 {
        self.keep_alive_interval
    }

    pub fn max_idle_timeout(&self) -> u64 {
        self.max_idle_timeout
    }

    pub fn validate_certificate(&self) -> bool {
        self.validate_certificate
    }
}

impl ConnectionStringOptions for QuicConnectionStringOptions {
    fn retries(&self) -> Option<u32> {
        self.reconnection.max_retries
    }

    fn heartbeat_interval(&self) -> NonZeroIggyDuration {
        self.heartbeat_interval
    }

    fn parse_options(options: &str) -> Result<QuicConnectionStringOptions, IggyError> {
        let options = options.split('&').collect::<Vec<&str>>();
        let mut pending_operations = PendingOperationsConfig::default();
        let mut response_buffer_size = MAX_RECORD_REPLY_SIZE as u64;
        let mut max_concurrent_bidi_streams = 10000;
        let mut datagram_send_buffer_size = 100_000;
        let mut initial_mtu = 1200;
        let mut send_window = 100_000;
        let mut receive_window = 100_000;
        let mut keep_alive_interval = 5000;
        let mut max_idle_timeout = 10000;
        let mut validate_certificate = false;
        let mut heartbeat_interval = "5s".to_owned();

        // For reconnection config
        let mut reconnection_max_retries = "unlimited".to_owned();
        let mut reconnection_interval = "1s".to_owned();
        let mut reestablish_after = IggyDuration::from_str("5s").unwrap();

        for option in options {
            let option_parts = option.split('=').collect::<Vec<&str>>();
            if option_parts.len() != 2 {
                return Err(IggyError::InvalidConnectionString);
            }
            match option_parts[0] {
                "max_pending_operations" => {
                    pending_operations.max_operations = option_parts[1]
                        .parse()
                        .map_err(|_| IggyError::InvalidConnectionString)?;
                }
                "max_pending_operation_bytes" => {
                    pending_operations.max_bytes = option_parts[1]
                        .parse()
                        .map_err(|_| IggyError::InvalidConnectionString)?;
                }
                "response_buffer_size" => match option_parts[1].parse::<u64>() {
                    Ok(value) => {
                        response_buffer_size = value;
                    }
                    Err(_) => {
                        return Err(IggyError::InvalidConnectionString);
                    }
                },
                "max_concurrent_bidi_streams" => match option_parts[1].parse::<u64>() {
                    Ok(value) => {
                        max_concurrent_bidi_streams = value;
                    }
                    Err(_) => {
                        return Err(IggyError::InvalidConnectionString);
                    }
                },
                "datagram_send_buffer_size" => match option_parts[1].parse::<u64>() {
                    Ok(value) => {
                        datagram_send_buffer_size = value;
                    }
                    Err(_) => {
                        return Err(IggyError::InvalidConnectionString);
                    }
                },
                "initial_mtu" => match option_parts[1].parse::<u16>() {
                    Ok(value) => {
                        initial_mtu = value;
                    }
                    Err(_) => {
                        return Err(IggyError::InvalidConnectionString);
                    }
                },
                "send_window" => match option_parts[1].parse::<u64>() {
                    Ok(value) => {
                        send_window = value;
                    }
                    Err(_) => {
                        return Err(IggyError::InvalidConnectionString);
                    }
                },
                "receive_window" => match option_parts[1].parse::<u64>() {
                    Ok(value) => {
                        receive_window = value;
                    }
                    Err(_) => {
                        return Err(IggyError::InvalidConnectionString);
                    }
                },
                "keep_alive_interval" => match option_parts[1].parse::<u64>() {
                    Ok(value) => {
                        keep_alive_interval = value;
                    }
                    Err(_) => {
                        return Err(IggyError::InvalidConnectionString);
                    }
                },
                "max_idle_timeout" => match option_parts[1].parse::<u64>() {
                    Ok(value) => {
                        max_idle_timeout = value;
                    }
                    Err(_) => {
                        return Err(IggyError::InvalidConnectionString);
                    }
                },
                "tls_validate_certificate" => {
                    validate_certificate = bool::from_str(option_parts[1])
                        .map_err(|_| IggyError::InvalidConnectionString)?;
                }
                "validate_certificate" => {
                    warn!(
                        "QUIC connection string option `validate_certificate` is deprecated; use `tls_validate_certificate` instead"
                    );
                    validate_certificate = bool::from_str(option_parts[1])
                        .map_err(|_| IggyError::InvalidConnectionString)?;
                }
                "heartbeat_interval" => {
                    heartbeat_interval = option_parts[1].to_string();
                }
                "reconnection_max_retries" => {
                    reconnection_max_retries = option_parts[1].to_string();
                }
                "reconnection_interval" => {
                    reconnection_interval = option_parts[1].to_string();
                }
                "reestablish_after" => {
                    reestablish_after = IggyDuration::from_str(option_parts[1])
                        .map_err(|_| IggyError::InvalidConnectionString)?;
                }
                "reconnection_reestablish_after" => {
                    warn!(
                        "QUIC connection string option `reconnection_reestablish_after` is deprecated; use `reestablish_after` instead"
                    );
                    reestablish_after = IggyDuration::from_str(option_parts[1])
                        .map_err(|_| IggyError::InvalidConnectionString)?;
                }
                _ => {
                    return Err(IggyError::InvalidConnectionString);
                }
            }
        }

        let reconnection = QuicClientReconnectionConfig {
            enabled: true,
            max_retries: match reconnection_max_retries.as_str() {
                "unlimited" => None,
                _ => Some(
                    reconnection_max_retries
                        .parse()
                        .map_err(|_| IggyError::InvalidNumberValue)?,
                ),
            },
            interval: NonZeroIggyDuration::from_str(reconnection_interval.as_str())
                .map_err(|_| IggyError::InvalidConnectionString)?,
            reestablish_after,
        };

        let heartbeat_interval = NonZeroIggyDuration::from_str(heartbeat_interval.as_str())
            .map_err(|_| IggyError::InvalidConnectionString)?;

        let mut connection_string_options = QuicConnectionStringOptions::new(
            reconnection,
            response_buffer_size,
            max_concurrent_bidi_streams,
            datagram_send_buffer_size,
            initial_mtu,
            send_window,
            receive_window,
            keep_alive_interval,
            max_idle_timeout,
            validate_certificate,
            heartbeat_interval,
        );

        connection_string_options.pending_operations = pending_operations;
        Ok(connection_string_options)
    }
}

impl QuicConnectionStringOptions {
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        reconnection: QuicClientReconnectionConfig,
        response_buffer_size: u64,
        max_concurrent_bidi_streams: u64,
        datagram_send_buffer_size: u64,
        initial_mtu: u16,
        send_window: u64,
        receive_window: u64,
        keep_alive_interval: u64,
        max_idle_timeout: u64,
        validate_certificate: bool,
        heartbeat_interval: NonZeroIggyDuration,
    ) -> Self {
        Self {
            pending_operations: PendingOperationsConfig::default(),
            reconnection,
            response_buffer_size,
            max_concurrent_bidi_streams,
            datagram_send_buffer_size,
            initial_mtu,
            send_window,
            receive_window,
            keep_alive_interval,
            max_idle_timeout,
            validate_certificate,
            heartbeat_interval,
        }
    }
}

impl Default for QuicConnectionStringOptions {
    fn default() -> Self {
        QuicConnectionStringOptions {
            pending_operations: PendingOperationsConfig::default(),
            reconnection: Default::default(),
            response_buffer_size: MAX_RECORD_REPLY_SIZE as u64,
            max_concurrent_bidi_streams: 10000,
            datagram_send_buffer_size: 100_000,
            initial_mtu: 1200,
            send_window: 100_000,
            receive_window: 100_000,
            keep_alive_interval: 5000,
            max_idle_timeout: 10000,
            validate_certificate: false,
            heartbeat_interval: NonZeroIggyDuration::from_str("5s").unwrap(),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{ConnectionString, QuicClientConfig, QuicClientConfigBuilder};
    use std::io::{self, Write};
    use std::sync::{Arc, Mutex};

    const QUIC_URL: &str = "iggy+quic://user:secret@127.0.0.1:8080";

    fn parse(options: &str) -> Result<QuicConnectionStringOptions, IggyError> {
        QuicConnectionStringOptions::parse_options(options)
    }

    fn duration(value: &str) -> IggyDuration {
        IggyDuration::from_str(value).unwrap()
    }

    fn client_config(options: &str) -> QuicClientConfig {
        let connection_string =
            ConnectionString::<QuicConnectionStringOptions>::new(&format!("{QUIC_URL}?{options}"))
                .unwrap();
        QuicClientConfig::from(connection_string)
    }

    #[test]
    fn cooldown_canonical_and_deprecated_keys_have_identical_values() {
        let canonical = parse("reestablish_after=500ms").unwrap();
        let deprecated = parse("reconnection_reestablish_after=500ms").unwrap();

        assert_eq!(canonical.reconnection().reestablish_after, duration("500ms"));
        assert_eq!(
            canonical.reconnection().reestablish_after,
            deprecated.reconnection().reestablish_after
        );
        assert_eq!(
            parse("reestablish_after=0")
                .unwrap()
                .reconnection()
                .reestablish_after,
            duration("0")
        );
    }

    #[test]
    fn cooldown_last_occurrence_wins_in_original_order() {
        for (options, expected) in [
            (
                "reconnection_reestablish_after=100ms&reestablish_after=500ms",
                "500ms",
            ),
            (
                "reestablish_after=500ms&reconnection_reestablish_after=100ms",
                "100ms",
            ),
            ("reestablish_after=100ms&reestablish_after=500ms", "500ms"),
            (
                "reconnection_reestablish_after=100ms&reconnection_reestablish_after=500ms",
                "500ms",
            ),
        ] {
            assert_eq!(
                parse(options).unwrap().reconnection().reestablish_after,
                duration(expected),
                "{options}"
            );
        }
    }

    #[test]
    fn cooldown_rejects_invalid_values_even_when_later_overridden() {
        for options in [
            "reestablish_after=invalid",
            "reconnection_reestablish_after=invalid",
            "reestablish_after=500",
            "reestablish_after=invalid&reconnection_reestablish_after=1s",
            "reconnection_reestablish_after=invalid&reestablish_after=1s",
            "reestablish_after=1s&reestablish_after=invalid",
            "reconnection_reestablish_after=1s&reconnection_reestablish_after=invalid",
            "reestablish_after=",
        ] {
            assert!(
                matches!(parse(options), Err(IggyError::InvalidConnectionString)),
                "{options}"
            );
        }
    }

    #[test]
    fn cooldown_defaults_and_retry_option_remain_unchanged() {
        assert_eq!(
            QuicConnectionStringOptions::default()
                .reconnection()
                .reestablish_after,
            duration("5s")
        );
        assert_eq!(
            parse("reconnection_max_retries=unlimited")
                .unwrap()
                .reconnection()
                .reestablish_after,
            duration("5s")
        );
        assert_eq!(parse("reconnection_max_retries=3").unwrap().retries(), Some(3));
        assert_eq!(
            parse("reconnection_max_retries=unlimited")
                .unwrap()
                .retries(),
            None
        );
        assert!(matches!(
            parse("reconnection_max_retries=invalid"),
            Err(IggyError::InvalidNumberValue)
        ));
        assert!(matches!(
            parse("reconnection_retries=3"),
            Err(IggyError::InvalidConnectionString)
        ));
    }

    #[test]
    fn tls_canonical_and_deprecated_keys_accept_exact_booleans() {
        for key in ["tls_validate_certificate", "validate_certificate"] {
            for (value, expected) in [("true", true), ("false", false)] {
                let options = parse(&format!("{key}={value}")).unwrap();
                assert_eq!(options.validate_certificate(), expected, "{key}={value}");
            }
        }
        assert!(!QuicConnectionStringOptions::default().validate_certificate());
        assert!(!parse("reconnection_max_retries=1").unwrap().validate_certificate());
    }

    #[test]
    fn tls_last_occurrence_wins_in_original_order() {
        for (options, expected) in [
            ("tls_validate_certificate=true&validate_certificate=false", false),
            ("validate_certificate=false&tls_validate_certificate=true", true),
            ("tls_validate_certificate=false&tls_validate_certificate=true", true),
            ("validate_certificate=true&validate_certificate=false", false),
            ("validate_certificate=true&tls_validate_certificate=false", false),
            ("tls_validate_certificate=false&validate_certificate=true", true),
        ] {
            assert_eq!(parse(options).unwrap().validate_certificate(), expected, "{options}");
        }
    }

    #[test]
    fn tls_rejects_invalid_values_even_when_later_overridden() {
        for key in ["tls_validate_certificate", "validate_certificate"] {
            for value in ["ture", "invalid", "yes", "1", "", "TRUE", "False", "0"] {
                let options = format!("{key}={value}");
                assert!(
                    matches!(parse(&options), Err(IggyError::InvalidConnectionString)),
                    "{options}"
                );
            }
            let options = format!("{key}=invalid&tls_validate_certificate=true");
            assert!(
                matches!(parse(&options), Err(IggyError::InvalidConnectionString)),
                "{options}"
            );
            let options = format!("tls_validate_certificate=true&{key}=invalid");
            assert!(
                matches!(parse(&options), Err(IggyError::InvalidConnectionString)),
                "{options}"
            );
        }
    }

    #[test]
    fn connection_string_options_propagate_to_quic_client_config() {
        let canonical = client_config(
            "reestablish_after=750ms&tls_validate_certificate=true&reconnection_max_retries=2",
        );
        let deprecated = client_config(
            "reconnection_reestablish_after=750ms&validate_certificate=true&reconnection_max_retries=2",
        );
        assert_eq!(canonical.reconnection.reestablish_after, duration("750ms"));
        assert_eq!(
            canonical.reconnection.reestablish_after,
            deprecated.reconnection.reestablish_after
        );
        assert!(canonical.validate_certificate);
        assert!(deprecated.validate_certificate);
        assert_eq!(canonical.reconnection.max_retries, Some(2));
        assert_eq!(deprecated.reconnection.max_retries, Some(2));

        let disabled = client_config("tls_validate_certificate=false");
        assert!(!disabled.validate_certificate);

        let omitted = ConnectionString::<QuicConnectionStringOptions>::new(QUIC_URL).unwrap();
        let omitted = QuicClientConfig::from(omitted);
        assert!(!omitted.validate_certificate);
        assert_eq!(omitted.reconnection.reestablish_after, duration("5s"));
    }

    #[test]
    fn existing_public_quic_builder_and_config_api_are_compatible() {
        let config = QuicClientConfigBuilder::new()
            .with_validate_certificate(true)
            .with_reestablish_after(duration("750ms"))
            .with_reconnection_max_retries(Some(2))
            .build()
            .unwrap();
        assert!(config.validate_certificate);
        assert_eq!(config.reconnection.reestablish_after, duration("750ms"));
        assert_eq!(config.reconnection.max_retries, Some(2));

        let mut default_config = QuicClientConfig::default();
        assert!(!default_config.validate_certificate);
        default_config.validate_certificate = true;
        assert!(default_config.validate_certificate);
    }

    struct WarningWriter(Arc<Mutex<Vec<u8>>>);

    impl Write for WarningWriter {
        fn write(&mut self, bytes: &[u8]) -> io::Result<usize> {
            self.0.lock().unwrap().extend_from_slice(bytes);
            Ok(bytes.len())
        }

        fn flush(&mut self) -> io::Result<()> {
            Ok(())
        }
    }

    fn capture_warnings(f: impl FnOnce()) -> String {
        let output = Arc::new(Mutex::new(Vec::new()));
        let writer = Arc::clone(&output);
        let subscriber = tracing_subscriber::fmt()
            .with_max_level(tracing::Level::WARN)
            .with_ansi(false)
            .without_time()
            .with_writer(move || WarningWriter(Arc::clone(&writer)))
            .finish();

        // A thread-local subscriber avoids changing logging for concurrently running tests.
        tracing::subscriber::with_default(subscriber, f);
        let bytes = output.lock().unwrap().clone();
        String::from_utf8(bytes).unwrap()
    }

    #[test]
    fn deprecated_aliases_warn_on_every_occurrence_and_identify_replacements() {
        let warnings = capture_warnings(|| {
            parse(
                "reconnection_reestablish_after=100ms&reestablish_after=500ms&reconnection_reestablish_after=1s",
            )
            .unwrap();
            parse(
                "validate_certificate=false&tls_validate_certificate=true&validate_certificate=true",
            )
            .unwrap();
        });
        assert_eq!(
            warnings
                .matches("option `reconnection_reestablish_after` is deprecated")
                .count(),
            2
        );
        assert_eq!(
            warnings
                .matches("option `validate_certificate` is deprecated")
                .count(),
            2
        );
        assert!(warnings.contains("use `reestablish_after` instead"));
        assert!(warnings.contains("use `tls_validate_certificate` instead"));

        let warnings = capture_warnings(|| {
            parse("reestablish_after=500ms&tls_validate_certificate=true").unwrap();
        });
        assert!(warnings.is_empty(), "canonical options must not warn: {warnings}");

        let warnings = capture_warnings(|| {
            assert!(matches!(
                parse("validate_certificate=invalid"),
                Err(IggyError::InvalidConnectionString)
            ));
        });
        assert!(warnings.contains("use `tls_validate_certificate` instead"));
    }
}
