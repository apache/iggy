/* Licensed to the Apache Software Foundation (ASF) under one
* or more contributor license agreements.  See the NOTICE file
* distributed with this work for additional information
* regarding copyright ownership.  The ASF licenses this file
* to you under the Apache License, Version 2.0 (the
* "License"); you may not use this file except in compliance
* with the License.  You may obtain a copy of the License at
*
*   http://www.apache.org/licenses/LICENSE-2.0
*
* Unless required by applicable law or agreed to in writing,
* software distributed under the License is distributed on an
* "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
* KIND, either express or implied.  See the License for the
* specific language governing permissions and limitations
* under the License.
*/

use crate::{ConnectionStringOptions, IggyDuration, IggyError, TcpClientReconnectionConfig};
use std::str::FromStr;

#[derive(Debug)]
pub struct TcpConnectionStringOptions {
    tls_enabled: bool,
    tls_domain: String,
    tls_ca_file: Option<String>,
    reconnection: TcpClientReconnectionConfig,
    heartbeat_interval: IggyDuration,
    nodelay: bool,
}

impl TcpConnectionStringOptions {
    pub fn tls_enabled(&self) -> bool {
        self.tls_enabled
    }

    pub fn tls_domain(&self) -> &str {
        &self.tls_domain
    }

    pub fn tls_ca_file(&self) -> &Option<String> {
        &self.tls_ca_file
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

    fn heartbeat_interval(&self) -> IggyDuration {
        self.heartbeat_interval
    }

    fn parse_options(options: &str) -> Result<TcpConnectionStringOptions, IggyError> {
        let options = options.split('&').collect::<Vec<&str>>();
        let mut tls_enabled = false;
        let mut tls_domain = "".to_string();
        let mut tls_ca_file = None;
        let mut reconnection_retries = "unlimited".to_owned();
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
                "tls" => {
                    tls_enabled = option_parts[1] == "true";
                }
                "tls_domain" => {
                    tls_domain = option_parts[1].to_string();
                }
                "tls_ca_file" => {
                    tls_ca_file = Some(option_parts[1].to_string());
                }
                "reconnection_retries" => {
                    reconnection_retries = option_parts[1].to_string();
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
            max_retries: match reconnection_retries.as_str() {
                "unlimited" => None,
                _ => Some(
                    reconnection_retries
                        .parse()
                        .map_err(|_| IggyError::InvalidNumberValue)?,
                ),
            },
            interval: IggyDuration::from_str(reconnection_interval.as_str())
                .map_err(|_| IggyError::InvalidConnectionString)?,
            reestablish_after: IggyDuration::from_str(reestablish_after.as_str())
                .map_err(|_| IggyError::InvalidConnectionString)?,
        };

        let heartbeat_interval = IggyDuration::from_str(heartbeat_interval.as_str())
            .map_err(|_| IggyError::InvalidConnectionString)?;

        let connection_string_options = TcpConnectionStringOptions::new(
            tls_enabled,
            tls_domain,
            tls_ca_file,
            reconnection,
            heartbeat_interval,
            nodelay,
        );

        Ok(connection_string_options)
    }
}

impl TcpConnectionStringOptions {
    pub fn new(
        tls_enabled: bool,
        tls_domain: String,
        tls_ca_file: Option<String>,
        reconnection: TcpClientReconnectionConfig,
        heartbeat_interval: IggyDuration,
        nodelay: bool,
    ) -> Self {
        Self {
            tls_enabled,
            tls_domain,
            tls_ca_file,
            reconnection,
            heartbeat_interval,
            nodelay,
        }
    }
}

impl Default for TcpConnectionStringOptions {
    fn default() -> Self {
        TcpConnectionStringOptions {
            tls_enabled: false,
            tls_domain: "".to_string(),
            tls_ca_file: None,
            reconnection: Default::default(),
            heartbeat_interval: IggyDuration::from_str("5s").unwrap(),
            nodelay: false,
        }
    }
}
