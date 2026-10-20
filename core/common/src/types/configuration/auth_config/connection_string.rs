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

use std::str::FromStr;

use secrecy::SecretString;

use crate::{
    AutoLogin, ConnectionStringOptions, Credentials, IggyError, TransportProtocol,
    validate_server_address,
};

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
        if server_address.starts_with('[') {
            validate_server_address(server_address)
                .map_err(|_| IggyError::InvalidConnectionString)?;
        } else {
            let (host, port) = server_address
                .split_once(':')
                .ok_or(IggyError::InvalidConnectionString)?;
            if host.is_empty() || !port.parse::<u16>().is_ok_and(|port| port != 0) {
                return Err(IggyError::InvalidConnectionString);
            }
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
                    SecretString::from(pat_token),
                )),
                options: connection_string_options,
            });
        }

        Ok(ConnectionString {
            server_address: server_address.to_owned(),
            auto_login: AutoLogin::Enabled(Credentials::UsernamePassword(
                username.to_owned(),
                SecretString::from(password),
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
    use crate::{
        HttpClientConfig, HttpConnectionStringOptions, NonZeroIggyDuration, QuicClientConfig,
        QuicConnectionStringOptions, TcpClientConfig, TcpConnectionStringOptions,
        WebSocketClientConfig, WebSocketConnectionStringOptions,
    };
    use secrecy::ExposeSecret;

    #[test]
    fn should_succeed_with_ipv4_address_or_hostname() {
        for server_address in [
            "127.0.0.1:8090",
            "localhost:8090",
            "iggy.default.svc.cluster.local:8090",
            "my_iggy_server:8090",
        ] {
            let value = format!("iggy://user:secret@{server_address}");
            let connection_string =
                ConnectionString::<TcpConnectionStringOptions>::new(&value).unwrap();
            assert_eq!(connection_string.server_address(), server_address);
        }
    }

    #[test]
    fn should_succeed_with_ipv6_address() {
        for server_address in [
            "[2001:db8::1]:8090",
            "[2001:db8:0:0:0:0:0:1]:65535",
            "[::ffff:192.0.2.1]:8090",
        ] {
            let value = format!(
                "iggy://user:secret@{server_address}?reconnection_retries=3&heartbeat_interval=10s"
            );
            let connection_string = ConnectionString::<TcpConnectionStringOptions>::new(&value)
                .unwrap_or_else(|error| panic!("Failed to parse {server_address}: {error}"));

            assert_eq!(connection_string.server_address(), server_address);
            match connection_string.auto_login() {
                AutoLogin::Enabled(Credentials::UsernamePassword(username, password)) => {
                    assert_eq!(username, "user");
                    assert_eq!(password.expose_secret(), "secret");
                }
                _ => panic!("Expected UsernamePassword credentials"),
            }
            assert_eq!(connection_string.options().retries(), Some(3));
            assert_eq!(
                connection_string.options().heartbeat_interval(),
                NonZeroIggyDuration::from_str("10s").unwrap()
            );
        }
    }

    #[test]
    fn should_succeed_with_bracketed_ipv6_address_and_port() {
        let connection_string =
            ConnectionString::<TcpConnectionStringOptions>::new("iggy://user:secret@[::1]:8090")
                .unwrap();

        assert_eq!(connection_string.server_address(), "[::1]:8090");
    }

    #[test]
    fn should_succeed_with_ipv6_address_and_pat() {
        let value = "iggy://iggypat-1234567890abcdef@[::1]:8090";
        let connection_string = ConnectionString::<TcpConnectionStringOptions>::new(value).unwrap();

        assert_eq!(connection_string.server_address(), "[::1]:8090");
        match connection_string.auto_login() {
            AutoLogin::Enabled(Credentials::PersonalAccessToken(token)) => {
                assert_eq!(token.expose_secret(), "iggypat-1234567890abcdef");
            }
            _ => panic!("Expected PersonalAccessToken credentials"),
        }
    }

    #[test]
    fn should_preserve_ipv6_address_in_client_configs() {
        let tcp = ConnectionString::<TcpConnectionStringOptions>::new(
            "iggy+tcp://user:secret@[::1]:8090",
        )
        .unwrap();
        assert_eq!(TcpClientConfig::from(tcp).server_address, "[::1]:8090");

        let quic = ConnectionString::<QuicConnectionStringOptions>::new(
            "iggy+quic://user:secret@[::1]:8080",
        )
        .unwrap();
        assert_eq!(QuicClientConfig::from(quic).server_address, "[::1]:8080");

        let http = ConnectionString::<HttpConnectionStringOptions>::new(
            "iggy+http://user:secret@[::1]:3000",
        )
        .unwrap();
        assert_eq!(HttpClientConfig::from(http).api_url, "http://[::1]:3000");

        let websocket = ConnectionString::<WebSocketConnectionStringOptions>::new(
            "iggy+ws://user:secret@[::1]:8092",
        )
        .unwrap();
        assert_eq!(
            WebSocketClientConfig::from(websocket).server_address,
            "[::1]:8092"
        );
    }

    #[test]
    fn should_fail_with_invalid_ipv6_address_or_port() {
        for server_address in [
            "[::1]",
            "[::1]:",
            "[::1]:invalid",
            "[::1]:65536",
            "[::1]:0",
            "[::1]:-1",
            "[::1]:8090:1234",
            "[::1:8090",
            "::1:8090",
            "2001:db8::1:8090",
            "[]:8090",
            "[localhost]:8090",
            "[gggg::1]:8090",
            "localhost",
            "localhost:0",
            "127.0.0.1:0",
        ] {
            let value = format!("iggy://user:secret@{server_address}");
            assert!(
                matches!(
                    ConnectionString::<TcpConnectionStringOptions>::new(&value),
                    Err(IggyError::InvalidConnectionString)
                ),
                "Expected InvalidConnectionString for {server_address}"
            );
        }
    }

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
        match &connection_string.auto_login {
            AutoLogin::Enabled(Credentials::UsernamePassword(u, p)) => {
                assert_eq!(u, username);
                assert_eq!(p.expose_secret(), password);
            }
            _ => panic!("Expected UsernamePassword credentials"),
        }

        assert!(connection_string.options.retries().is_none());
        assert_eq!(
            connection_string.options.heartbeat_interval(),
            NonZeroIggyDuration::from_str("5s").unwrap()
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
        match &connection_string.auto_login {
            AutoLogin::Enabled(Credentials::UsernamePassword(u, p)) => {
                assert_eq!(u, username);
                assert_eq!(p.expose_secret(), password);
            }
            _ => panic!("Expected UsernamePassword credentials"),
        }

        assert_eq!(connection_string.options.retries().unwrap(), 3);
        assert_eq!(
            connection_string.options.heartbeat_interval(),
            NonZeroIggyDuration::from_str("10s").unwrap()
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
        match &connection_string.auto_login {
            AutoLogin::Enabled(Credentials::PersonalAccessToken(token)) => {
                assert_eq!(token.expose_secret(), pat);
            }
            _ => panic!("Expected PersonalAccessToken credentials"),
        }

        assert!(connection_string.options.retries().is_none());
        assert_eq!(
            connection_string.options.heartbeat_interval(),
            NonZeroIggyDuration::from_str("5s").unwrap()
        );
    }
}
