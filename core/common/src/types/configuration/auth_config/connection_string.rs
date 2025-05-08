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

use crate::{
    AutoLogin, ConnectionStringOptions, Credentials, IggyDuration, IggyError,
    TcpClientReconnectionConfig,
};
use std::str::FromStr;

const CONNECTION_STRING_PREFIX: &str = "iggy://";

#[derive(Debug)]
pub struct ConnectionString {
    server_address: String,
    auto_login: AutoLogin,
    options: ConnectionStringOptions,
}

impl ConnectionString {
    pub fn server_address(&self) -> &str {
        &self.server_address
    }

    pub fn auto_login(&self) -> &AutoLogin {
        &self.auto_login
    }

    pub fn options(&self) -> &ConnectionStringOptions {
        &self.options
    }
}

impl ConnectionString {
    pub fn new(connection_string: &str) -> Result<Self, IggyError> {
        if connection_string.is_empty() {
            return Err(IggyError::InvalidConnectionString);
        }

        if !connection_string.starts_with(CONNECTION_STRING_PREFIX) {
            return Err(IggyError::InvalidConnectionString);
        }

        let connection_string = connection_string.replace(CONNECTION_STRING_PREFIX, "");
        let parts = connection_string.split('@').collect::<Vec<&str>>();

        if parts.len() != 2 {
            return Err(IggyError::InvalidConnectionString);
        }

        let credentials = parts[0].split(':').collect::<Vec<&str>>();
        if credentials.len() != 2 {
            return Err(IggyError::InvalidConnectionString);
        }

        let username = credentials[0];
        let password = credentials[1];
        if username.is_empty() || password.is_empty() {
            return Err(IggyError::InvalidConnectionString);
        }

        let server_and_options = parts[1].split('?').collect::<Vec<&str>>();
        if server_and_options.len() > 2 {
            return Err(IggyError::InvalidConnectionString);
        }

        let server_address = server_and_options[0];
        if server_address.is_empty() {
            return Err(IggyError::InvalidConnectionString);
        }

        if !server_address.contains(':') {
            return Err(IggyError::InvalidConnectionString);
        }

        let port = server_address.split(':').collect::<Vec<&str>>()[1];
        if port.is_empty() {
            return Err(IggyError::InvalidConnectionString);
        }

        if port.parse::<u16>().is_err() {
            return Err(IggyError::InvalidConnectionString);
        }

        let connection_string_options;
        if let Some(options) = server_and_options.get(1) {
            connection_string_options = ConnectionString::parse_options(options)?;
        } else {
            connection_string_options = ConnectionStringOptions::default();
        }

        Ok(ConnectionString {
            server_address: server_address.to_owned(),
            auto_login: AutoLogin::Enabled(Credentials::UsernamePassword(
                username.to_owned(),
                password.to_owned(),
            )),
            options: connection_string_options,
        })
    }

    fn parse_options(options: &str) -> Result<ConnectionStringOptions, IggyError> {
        let options = options.split('&').collect::<Vec<&str>>();
        let mut tls_enabled = false;
        let mut tls_domain = "localhost".to_string();
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

        let connection_string_options = ConnectionStringOptions::new(
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

impl FromStr for ConnectionString {
    type Err = IggyError;
    fn from_str(s: &str) -> Result<Self, Self::Err> {
        ConnectionString::new(s)
    }
}
