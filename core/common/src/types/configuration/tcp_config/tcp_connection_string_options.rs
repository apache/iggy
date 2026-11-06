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
    TcpClientReconnectionConfig,
};
use std::str::FromStr;

#[derive(Debug)]
pub struct TcpConnectionStringOptions {
    pending_operations: PendingOperationsConfig,
    tls_enabled: bool,
    tls_domain: String,
    tls_ca_file: Option<String>,
    tls_validate_certificate: bool,
    reconnection: TcpClientReconnectionConfig,
    heartbeat_interval: NonZeroIggyDuration,
    nodelay: bool,
}

impl TcpConnectionStringOptions {
    pub fn pending_operations(&self) -> PendingOperationsConfig {
        self.pending_operations
    }

    pub fn tls_enabled(&self) -> bool {
        self.tls_enabled
    }

    pub fn tls_domain(&self) -> &str {
        &self.tls_domain
    }

    pub fn tls_ca_file(&self) -> &Option<String> {
        &self.tls_ca_file
    }

    pub fn tls_validate_certificate(&self) -> bool {
        self.tls_validate_certificate
    }

    pub fn reconnection(&self) -> &TcpClientReconnectionConfig {
        &self.reconnection
    }

    pub fn nodelay(&self) -> bool {
        self.nodelay
    }
}

impl ConnectionStringOptions for TcpConnectionStringOptions {
    fn retries(&self) -> Option<u32> {
        self.reconnection.max_retries
    }

    fn heartbeat_interval(&self) -> NonZeroIggyDuration {
        self.heartbeat_interval
    }

    fn parse_options(options: &str) -> Result<TcpConnectionStringOptions, IggyError> {
        let options = options.split('&').collect::<Vec<&str>>();
        let mut pending_operations = PendingOperationsConfig::default();
        let mut tls_enabled = false;
        let mut tls_domain = "".to_string();
        let mut tls_ca_file = None;
        let mut tls_validate_certificate = true;
        let mut reconnection_max_retries = "unlimited".to_owned();
        let mut reconnection_interval = "1s".to_owned();
        let mut reestablish_after = "5s".to_owned();
        let mut heartbeat_interval = "5s".to_owned();
        let mut nodelay = false;

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
                "tls" => {
                    tls_enabled = option_parts[1] == "true";
                }
                "tls_domain" => {
                    tls_domain = option_parts[1].to_string();
                }
                "tls_ca_file" => {
                    tls_ca_file = Some(option_parts[1].to_string());
                }
                "tls_validate_certificate" => {
                    tls_validate_certificate = option_parts[1]
                        .parse::<bool>()
                        .map_err(|_| IggyError::InvalidConnectionString)?;
                }
                "reconnection_max_retries" => {
                    reconnection_max_retries = option_parts[1].to_string();
                }
                "reconnection_retries" => {
                    tracing::warn!(
                        "Connection-string option 'reconnection_retries' is deprecated; use 'reconnection_max_retries' instead"
                    );
                    reconnection_max_retries = option_parts[1].to_string();
                }
                "reconnection_interval" => {
                    reconnection_interval = option_parts[1].to_string();
                }
                "reestablish_after" => {
                    reestablish_after = option_parts[1].to_string();
                }
                "heartbeat_interval" => {
                    heartbeat_interval = option_parts[1].to_string();
                }
                "nodelay" => {
                    nodelay = option_parts[1] == "true";
                }
                _ => {
                    return Err(IggyError::InvalidConnectionString);
                }
            }
        }

        let reconnection = TcpClientReconnectionConfig {
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
            reestablish_after: IggyDuration::from_str(reestablish_after.as_str())
                .map_err(|_| IggyError::InvalidConnectionString)?,
        };

        let heartbeat_interval = NonZeroIggyDuration::from_str(heartbeat_interval.as_str())
            .map_err(|_| IggyError::InvalidConnectionString)?;

        let mut connection_string_options = TcpConnectionStringOptions::new(
            tls_enabled,
            tls_domain,
            tls_ca_file,
            reconnection,
            heartbeat_interval,
            nodelay,
        );

        connection_string_options.pending_operations = pending_operations;
        connection_string_options.tls_validate_certificate = tls_validate_certificate;
        Ok(connection_string_options)
    }
}

impl TcpConnectionStringOptions {
    pub fn new(
        tls_enabled: bool,
        tls_domain: String,
        tls_ca_file: Option<String>,
        reconnection: TcpClientReconnectionConfig,
        heartbeat_interval: NonZeroIggyDuration,
        nodelay: bool,
    ) -> Self {
        Self {
            pending_operations: PendingOperationsConfig::default(),
            tls_enabled,
            tls_domain,
            tls_ca_file,
            tls_validate_certificate: true,
            reconnection,
            heartbeat_interval,
            nodelay,
        }
    }
}

impl Default for TcpConnectionStringOptions {
    fn default() -> Self {
        TcpConnectionStringOptions {
            pending_operations: PendingOperationsConfig::default(),
            tls_enabled: false,
            tls_domain: "".to_string(),
            tls_ca_file: None,
            tls_validate_certificate: true,
            reconnection: Default::default(),
            heartbeat_interval: NonZeroIggyDuration::from_str("5s").unwrap(),
            nodelay: false,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::TcpClientConfig;
    use crate::types::configuration::auth_config::connection_string::ConnectionString;
    use std::sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    };

    fn client_config(options: &str) -> Result<TcpClientConfig, IggyError> {
        let suffix = if options.is_empty() {
            String::new()
        } else {
            format!("?{options}")
        };
        let value = format!("iggy+tcp://user:secret@localhost:8090{suffix}");
        ConnectionString::<TcpConnectionStringOptions>::new(&value).map(TcpClientConfig::from)
    }

    #[test]
    fn should_accept_canonical_and_deprecated_retry_options_in_order() {
        for (options, expected) in [
            ("reconnection_max_retries=5", Some(5)),
            ("reconnection_retries=5", Some(5)),
            (
                "reconnection_retries=3&reconnection_max_retries=7",
                Some(7),
            ),
            (
                "reconnection_max_retries=7&reconnection_retries=3",
                Some(3),
            ),
            (
                "reconnection_max_retries=1&reconnection_max_retries=9",
                Some(9),
            ),
            ("reconnection_retries=1&reconnection_retries=2", Some(2)),
            ("reconnection_max_retries=unlimited", None),
            ("reconnection_retries=unlimited", None),
            ("reconnection_max_retries=0", Some(0)),
        ] {
            assert_eq!(
                client_config(options).unwrap().reconnection.max_retries,
                expected,
                "{options}"
            );
        }
        assert_eq!(client_config("").unwrap().reconnection.max_retries, None);
    }

    #[test]
    fn should_preserve_retry_number_validation() {
        for key in ["reconnection_max_retries", "reconnection_retries"] {
            for value in ["abc", "-1", "4294967296", ""] {
                let options = format!("{key}={value}");
                assert!(
                    matches!(
                        client_config(&options),
                        Err(IggyError::InvalidNumberValue)
                    ),
                    "{options}"
                );
            }
        }
    }

    #[test]
    fn should_propagate_tls_certificate_validation_to_tcp_client_config() {
        assert!(TcpConnectionStringOptions::default().tls_validate_certificate());
        assert!(TcpClientConfig::default().tls_validate_certificate);
        assert!(client_config("").unwrap().tls_validate_certificate);
        assert!(client_config("tls=true").unwrap().tls_validate_certificate);
        assert!(
            client_config("tls=true&tls_validate_certificate=true")
                .unwrap()
                .tls_validate_certificate
        );
        assert!(
            !client_config("tls=true&tls_validate_certificate=false")
                .unwrap()
                .tls_validate_certificate
        );
        assert!(
            client_config("tls_validate_certificate=false&tls_validate_certificate=true")
                .unwrap()
                .tls_validate_certificate
        );
        assert!(
            !client_config("tls_validate_certificate=true&tls_validate_certificate=false")
                .unwrap()
                .tls_validate_certificate
        );
    }

    #[test]
    fn should_reject_invalid_tls_certificate_validation_values() {
        for value in ["ture", "yes", "1", "", "TRUE", "False", "0", " true"] {
            let options = format!("tls_validate_certificate={value}");
            assert!(
                matches!(
                    client_config(&options),
                    Err(IggyError::InvalidConnectionString)
                ),
                "{options}"
            );
        }
    }

    struct WarningCounter(Arc<AtomicUsize>);

    impl tracing::Subscriber for WarningCounter {
        fn enabled(&self, _metadata: &tracing::Metadata<'_>) -> bool {
            true
        }

        fn new_span(&self, _attributes: &tracing::span::Attributes<'_>) -> tracing::span::Id {
            tracing::span::Id::from_u64(1)
        }

        fn record(&self, _span: &tracing::span::Id, _values: &tracing::span::Record<'_>) {}

        fn record_follows_from(&self, _span: &tracing::span::Id, _follows: &tracing::span::Id) {}

        fn event(&self, event: &tracing::Event<'_>) {
            if *event.metadata().level() == tracing::Level::WARN
                && event.metadata().target() == module_path!()
            {
                self.0.fetch_add(1, Ordering::Relaxed);
            }
        }

        fn enter(&self, _span: &tracing::span::Id) {}

        fn exit(&self, _span: &tracing::span::Id) {}
    }

    #[test]
    fn should_warn_for_each_deprecated_retry_option_even_if_overridden() {
        let warnings = Arc::new(AtomicUsize::new(0));
        tracing::subscriber::with_default(WarningCounter(Arc::clone(&warnings)), || {
            client_config("reconnection_max_retries=5").unwrap();
            assert_eq!(warnings.load(Ordering::Relaxed), 0);

            client_config("reconnection_retries=3").unwrap();
            client_config("reconnection_retries=3&reconnection_max_retries=7").unwrap();
            client_config("reconnection_max_retries=7&reconnection_retries=3").unwrap();
            client_config("reconnection_retries=1&reconnection_retries=2").unwrap();
        });
        assert_eq!(warnings.load(Ordering::Relaxed), 5);
    }
}
