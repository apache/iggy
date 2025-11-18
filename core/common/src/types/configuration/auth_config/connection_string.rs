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

use crate::{AutoLogin, ConnectionStringOptions, Credentials, IggyError, TransportProtocol};
use std::str::FromStr;

const DEFAULT_CONNECTION_STRING_PREFIX: &str = "iggy://";
const CONNECTION_STRING_PREFIX: &str = "iggy+";

#[derive(Debug)]
pub struct ConnectionString<T: ConnectionStringOptions + Default> {
    server_address: String,
    auto_login: AutoLogin,
    options: T,
}

impl<T: ConnectionStringOptions + Default> ConnectionString<T> {
    pub fn server_address(&self) -> &str {
        &self.server_address
    }

    pub fn auto_login(&self) -> &AutoLogin {
        &self.auto_login
    }

    pub fn options(&self) -> &T {
        &self.options
    }

    pub fn new(connection_string: &str) -> Result<Self, IggyError> {
        let protocol_parts = connection_string.split("://").collect::<Vec<&str>>();
        if protocol_parts.len() != 2 {
            return Err(IggyError::InvalidConnectionString);
        }
        let connection_string = protocol_parts[1];
        let parts = connection_string.split('@').collect::<Vec<&str>>();
        let mut username = "";
        let mut password = "";
        let mut pat_token = "";

        if parts.len() != 2 {
            return Err(IggyError::InvalidConnectionString);
        }

        let credentials = parts[0].split(':').collect::<Vec<&str>>();
        if credentials.len() == 1 {
            pat_token = credentials[0];
        } else if credentials.len() == 2 {
            username = credentials[0];
            password = credentials[1];

            if username.is_empty() || password.is_empty() {
                return Err(IggyError::InvalidConnectionString);
            }
        } else {
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

        if !server_address.contains(':') || server_address.starts_with(':') {
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
            connection_string_options = T::parse_options(options)?;
        } else {
            connection_string_options = T::default();
        }

        if credentials.len() == 1 {
            return Ok(ConnectionString {
                server_address: server_address.to_owned(),
                auto_login: AutoLogin::Enabled(Credentials::PersonalAccessToken(
                    pat_token.to_owned(),
                )),
                options: connection_string_options,
            });
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
}

impl<T: ConnectionStringOptions + Default> FromStr for ConnectionString<T> {
    type Err = IggyError;
    fn from_str(s: &str) -> Result<Self, Self::Err> {
        ConnectionString::<T>::new(s)
    }
}

/// ConnectionStringUtils is a utility struct for connection strings.
pub struct ConnectionStringUtils;

impl ConnectionStringUtils {
    pub fn parse_protocol(connection_string: &str) -> Result<TransportProtocol, IggyError> {
        if connection_string.is_empty() {
            return Err(IggyError::InvalidConnectionString);
        }

        if connection_string.starts_with(DEFAULT_CONNECTION_STRING_PREFIX) {
            return Ok(TransportProtocol::Tcp);
        }

        if !connection_string.starts_with(CONNECTION_STRING_PREFIX) {
            return Err(IggyError::InvalidConnectionString);
        }

        let connection_string = connection_string.replace(CONNECTION_STRING_PREFIX, "");
        TransportProtocol::from_str(connection_string.split("://").collect::<Vec<&str>>()[0])
            .map_err(|_| IggyError::InvalidConnectionString)
    }
}

/// Unit tests for ConnectionString and ConnectionStringUtils,
/// common behavior which isn't type-specific will use TcpConnectionStringOptions
/// as default.
#[cfg(test)]
mod tests {
    use super::*;
    use crate::IggyDuration;
    use crate::TcpConnectionStringOptions;

    #[test]
    fn should_fail_without_username() {
        let server_address = "127.0.0.1";
        let port = "1234";
        let username = "";
        let password = "secret";
        let value = format!(
            "{DEFAULT_CONNECTION_STRING_PREFIX}{username}:{password}@{server_address}:{port}"
        );
        let connection_string = ConnectionString::<TcpConnectionStringOptions>::new(&value);
        assert!(connection_string.is_err());
    }

    #[test]
    fn should_fail_without_password() {
        let server_address = "127.0.0.1";
        let port = "1234";
        let username = "user";
        let password = "";
        let value = format!(
            "{DEFAULT_CONNECTION_STRING_PREFIX}{username}:{password}@{server_address}:{port}"
        );
        let connection_string = ConnectionString::<TcpConnectionStringOptions>::new(&value);
        assert!(connection_string.is_err());
    }

    #[test]
    fn should_fail_without_server_address() {
        let server_address = "";
        let port = "1234";
        let username = "user";
        let password = "secret";
        let value = format!(
            "{DEFAULT_CONNECTION_STRING_PREFIX}{username}:{password}@{server_address}:{port}"
        );
        let connection_string = ConnectionString::<TcpConnectionStringOptions>::new(&value);
        assert!(connection_string.is_err());
    }

    #[test]
    fn should_fail_without_port() {
        let server_address = "127.0.0.1";
        let port = "";
        let username = "user";
        let password = "secret";
        let value = format!(
            "{DEFAULT_CONNECTION_STRING_PREFIX}{username}:{password}@{server_address}:{port}"
        );
        let connection_string = ConnectionString::<TcpConnectionStringOptions>::new(&value);
        assert!(connection_string.is_err());
    }

    #[test]
    fn should_fail_with_invalid_options() {
        let server_address = "127.0.0.1";
        let port = "1234";
        let username = "user";
        let password = "secret";
        let value = format!(
            "{DEFAULT_CONNECTION_STRING_PREFIX}{username}:{password}@{server_address}:{port}?invalid_option=invalid"
        );
        let connection_string = ConnectionString::<TcpConnectionStringOptions>::new(&value);
        assert!(connection_string.is_err());
    }

    #[test]
    fn should_succeed_without_options() {
        let server_address = "127.0.0.1";
        let port = "1234";
        let username = "user";
        let password = "secret";
        let value = format!(
            "{DEFAULT_CONNECTION_STRING_PREFIX}{username}:{password}@{server_address}:{port}"
        );
        let connection_string = ConnectionString::<TcpConnectionStringOptions>::new(&value);
        assert!(connection_string.is_ok());

        let connection_string = connection_string.unwrap();
        assert_eq!(
            connection_string.server_address,
            format!("{server_address}:{port}")
        );
        assert_eq!(
            connection_string.auto_login,
            AutoLogin::Enabled(Credentials::UsernamePassword(
                username.to_string(),
                password.to_string()
            ))
        );

        assert!(connection_string.options.retries().is_none());
        assert_eq!(
            connection_string.options.heartbeat_interval(),
            IggyDuration::from_str("5s").unwrap()
        );
    }

    #[test]
    fn should_succeed_with_options() {
        let server_address = "127.0.0.1";
        let port = "1234";
        let username = "user";
        let password = "secret";
        let retries = "3";
        let heartbeat_interval = "10s";
        let value = format!(
            "{DEFAULT_CONNECTION_STRING_PREFIX}{username}:{password}@{server_address}:{port}?reconnection_retries={retries}&heartbeat_interval={heartbeat_interval}"
        );
        let connection_string = ConnectionString::<TcpConnectionStringOptions>::new(&value);
        assert!(connection_string.is_ok());

        let connection_string = connection_string.unwrap();
        assert_eq!(
            connection_string.server_address,
            format!("{server_address}:{port}")
        );
        assert_eq!(
            connection_string.auto_login,
            AutoLogin::Enabled(Credentials::UsernamePassword(
                username.to_string(),
                password.to_string()
            ))
        );

        assert_eq!(connection_string.options.retries().unwrap(), 3);
        assert_eq!(
            connection_string.options.heartbeat_interval(),
            IggyDuration::from_str("10s").unwrap()
        );
    }

    #[test]
    fn should_succeed_with_pat() {
        let server_address = "127.0.0.1";
        let port = "1234";
        let pat = "iggypat-1234567890abcdef";
        let value = format!("{DEFAULT_CONNECTION_STRING_PREFIX}{pat}@{server_address}:{port}");
        let connection_string = ConnectionString::<TcpConnectionStringOptions>::new(&value);
        assert!(connection_string.is_ok());

        let connection_string = connection_string.unwrap();
        assert_eq!(
            connection_string.server_address,
            format!("{server_address}:{port}")
        );
        assert_eq!(
            connection_string.auto_login,
            AutoLogin::Enabled(Credentials::PersonalAccessToken(pat.to_string()))
        );

        assert!(connection_string.options.retries().is_none());
        assert_eq!(
            connection_string.options.heartbeat_interval(),
            IggyDuration::from_str("5s").unwrap()
        );
    }
}
