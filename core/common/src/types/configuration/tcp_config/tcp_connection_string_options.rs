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
        let mut reconnection_max_retries = None;
        let mut reconnection_interval = NonZeroIggyDuration::from_str("1s").unwrap();
        let mut reestablish_after = IggyDuration::from_str("5s").unwrap();
        let mut heartbeat_interval = NonZeroIggyDuration::from_str("5s").unwrap();
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
                    tls_enabled = option_parts[1]
                        .parse::<bool>()
                        .map_err(|_| IggyError::InvalidConnectionString)?;
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
                    reconnection_max_retries = match option_parts[1] {
                        "unlimited" => None,
                        value => Some(
                            value
                                .parse()
                                .map_err(|_| IggyError::InvalidConnectionString)?,
                        ),
                    };
                }
                "reconnection_retries" => {
                    tracing::warn!(
                        "TCP connection string option `reconnection_retries` is deprecated; use `reconnection_max_retries` instead"
                    );
                    reconnection_max_retries = match option_parts[1] {
                        "unlimited" => None,
                        value => Some(
                            value
                                .parse()
                                .map_err(|_| IggyError::InvalidConnectionString)?,
                        ),
                    };
                }
                "reconnection_interval" => {
                    reconnection_interval = NonZeroIggyDuration::from_str(option_parts[1])
                        .map_err(|_| IggyError::InvalidConnectionString)?;
                }
                "reestablish_after" => {
                    reestablish_after = IggyDuration::from_str(option_parts[1])
                        .map_err(|_| IggyError::InvalidConnectionString)?;
                }
                "heartbeat_interval" => {
                    heartbeat_interval = NonZeroIggyDuration::from_str(option_parts[1])
                        .map_err(|_| IggyError::InvalidConnectionString)?;
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
            max_retries: reconnection_max_retries,
            interval: reconnection_interval,
            reestablish_after,
        };

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
            ("reconnection_retries=3&reconnection_max_retries=7", Some(7)),
            ("reconnection_max_retries=7&reconnection_retries=3", Some(3)),
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
                        Err(IggyError::InvalidConnectionString)
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
        for value in [
            concat!("tu", "re"),
            "yes",
            "1",
            "",
            "TRUE",
            "False",
            "0",
            " true",
        ] {
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

    #[test]
    fn should_reject_invalid_retry_occurrences_before_overrides() {
        for first in ["reconnection_max_retries", "reconnection_retries"] {
            for last in ["reconnection_max_retries", "reconnection_retries"] {
                for value in ["abc", "-1", "4294967296", ""] {
                    let options = format!("{first}={value}&{last}=3");
                    assert!(
                        matches!(
                            client_config(&options),
                            Err(IggyError::InvalidConnectionString)
                        ),
                        "{options}"
                    );
                }
            }
        }
    }

    #[test]
    fn should_reject_invalid_tls_enabling_values_before_overrides() {
        for key in ["tls", "tls_validate_certificate"] {
            for value in [
                concat!("tu", "re"),
                "invalid",
                "yes",
                "1",
                "",
                "TRUE",
                "False",
                "0",
                "%74rue",
                "%ZZ",
            ] {
                for suffix in [
                    String::new(),
                    format!("&{key}=true"),
                    format!("&{key}=false"),
                ] {
                    let options = format!("{key}={value}{suffix}");
                    assert!(
                        matches!(
                            client_config(&options),
                            Err(IggyError::InvalidConnectionString)
                        ),
                        "{options}"
                    );
                }
            }
            assert!(matches!(
                client_config(&format!("{key}=true&&{key}=false")),
                Err(IggyError::InvalidConnectionString)
            ));
        }
        for (options, expected) in [
            ("tls=true", true),
            ("tls=false", false),
            ("tls=false&tls=true", true),
            ("tls=true&tls=false", false),
        ] {
            assert_eq!(client_config(options).unwrap().tls_enabled, expected);
        }
        assert!(client_config("tls_enabled=true").is_err());
    }

    #[test]
    fn should_validate_each_duration_occurrence_and_preserve_units() {
        for key in [
            "reestablish_after",
            "reconnection_interval",
            "heartbeat_interval",
        ] {
            for value in ["invalid", "500", "", "18446744073709551616s"] {
                let options = format!("{key}={value}&{key}=1s");
                assert!(
                    matches!(
                        client_config(&options),
                        Err(IggyError::InvalidConnectionString)
                    ),
                    "{options}"
                );
            }
        }
        for value in ["0", "unlimited", "disabled", "none", "500ms", "2s", "1m"] {
            assert_eq!(
                client_config(&format!("reestablish_after={value}"))
                    .unwrap()
                    .reconnection
                    .reestablish_after,
                value.parse::<IggyDuration>().unwrap()
            );
        }
        let config = client_config("reestablish_after=1s&reestablish_after=750ms&reconnection_interval=2s&heartbeat_interval=3s").unwrap();
        assert_eq!(
            config.reconnection.reestablish_after,
            "750ms".parse().unwrap()
        );
        assert_eq!(config.reconnection.interval, "2s".parse().unwrap());
        assert_eq!(config.heartbeat_interval, "3s".parse().unwrap());
    }

    #[test]
    fn should_preserve_existing_constructor_and_builder_defaults() {
        let options = TcpConnectionStringOptions::new(
            false,
            String::new(),
            None,
            Default::default(),
            "5s".parse().unwrap(),
            false,
        );
        assert!(options.tls_validate_certificate());
        assert!(!options.tls_enabled());
        assert_eq!(options.tls_domain(), "");
        assert!(options.tls_ca_file().is_none());
        let config = crate::TcpClientConfigBuilder::new()
            .with_tls_enabled(true)
            .with_tls_validate_certificate(false)
            .with_tls_domain("localhost".into())
            .with_tls_ca_file("ca.pem".into())
            .with_reconnection_max_retries(Some(2))
            .build()
            .unwrap();
        assert!(config.tls_enabled);
        assert!(!config.tls_validate_certificate);
        assert_eq!(config.tls_domain, "localhost");
        assert_eq!(config.tls_ca_file.as_deref(), Some("ca.pem"));
        assert_eq!(config.reconnection.max_retries, Some(2));
    }
}
