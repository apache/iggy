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

use crate::{AutoLogin, IggyDuration, IggyError, TcpClientConfig};
use std::net::SocketAddr;

/// Builder for the TCP client configuration.
/// Allows configuring the TCP client with custom settings or using defaults:
/// - `server_address`: Default is "127.0.0.1:8090"
/// - `auto_login`: Default is AutoLogin::Disabled.
/// - `reconnection`: Default is enabled unlimited retries and 1 second interval.
/// - `tls_enabled`: Default is false.
/// - `tls_domain`: Default is "localhost".
/// - `tls_ca_file`: Default is None.
#[derive(Debug, Default)]
pub struct TcpClientConfigBuilder {
    config: TcpClientConfig,
}

impl TcpClientConfigBuilder {
    pub fn new() -> Self {
        TcpClientConfigBuilder::default()
    }

    /// Sets the server address for the TCP client.
    pub fn with_server_address(mut self, server_address: String) -> Self {
        self.config.server_address = server_address;
        self
    }

    /// Sets the auto sign in during connection.
    pub fn with_auto_sign_in(mut self, auto_sign_in: AutoLogin) -> Self {
        self.config.auto_login = auto_sign_in;
        self
    }

    pub fn with_enabled_reconnection(mut self) -> Self {
        self.config.reconnection.enabled = true;
        self
    }

    /// Sets the number of retries when connecting to the server.
    pub fn with_reconnection_max_retries(mut self, max_retries: Option<u32>) -> Self {
        self.config.reconnection.max_retries = max_retries;
        self
    }

    /// Sets the interval between retries when connecting to the server.
    pub fn with_reconnection_interval(mut self, interval: IggyDuration) -> Self {
        self.config.reconnection.interval = interval;
        self
    }

    /// Sets whether to use TLS when connecting to the server.
    pub fn with_tls_enabled(mut self, tls_enabled: bool) -> Self {
        self.config.tls_enabled = tls_enabled;
        self
    }

    /// Sets the domain to use for TLS when connecting to the server.
    pub fn with_tls_domain(mut self, tls_domain: String) -> Self {
        self.config.tls_domain = tls_domain;
        self
    }

    /// Sets the path to the CA file for TLS.
    pub fn with_tls_ca_file(mut self, tls_ca_file: String) -> Self {
        self.config.tls_ca_file = Some(tls_ca_file);
        self
    }

    /// Sets whether to validate the TLS certificate.
    pub fn with_tls_validate_certificate(mut self, tls_validate_certificate: bool) -> Self {
        self.config.tls_validate_certificate = tls_validate_certificate;
        self
    }

    /// Sets the nodelay option for the TCP socket.
    pub fn with_no_delay(mut self) -> Self {
        self.config.nodelay = true;
        self
    }

    /// Builds the TCP client configuration.
    pub fn build(self) -> Result<TcpClientConfig, IggyError> {
        let addr = self.config.server_address.trim();

        if addr.parse::<SocketAddr>().is_err() {
            let (ip, port) = addr.rsplit_once(':').unwrap_or((addr, ""));
            return Err(IggyError::InvalidIpAddress(ip.to_owned(), port.to_owned()));
        }

        Ok(self.config)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::IggyError;

    fn builder_with_address(addr: &str) -> TcpClientConfigBuilder {
        let mut builder = TcpClientConfigBuilder::default();
        builder.config.server_address = addr.to_string();
        builder
    }

    #[test]
    fn valid_ipv4_should_succeed() {
        let builder = builder_with_address("127.0.0.1:8080");
        assert!(builder.build().is_ok());
    }

    #[test]
    fn valid_ipv6_with_brackets_should_succeed() {
        let builder = builder_with_address("[::1]:8080");
        assert!(builder.build().is_ok());
    }

    #[test]
    fn valid_ipv6_without_brackets_should_fail() {
        let builder = builder_with_address("::1:8080");
        assert!(matches!(
            builder.build(),
            Err(IggyError::InvalidIpAddress(_, _))
        ));
    }

    #[test]
    fn invalid_ip_should_fail() {
        let builder = builder_with_address("invalid.ip:8080");
        assert!(matches!(
            builder.build(),
            Err(IggyError::InvalidIpAddress(_, _))
        ));
    }

    #[test]
    fn invalid_port_should_fail() {
        let builder = builder_with_address("127.0.0.1:invalid");
        assert!(matches!(
            builder.build(),
            Err(IggyError::InvalidIpAddress(_, _))
        ));
    }

    #[test]
    fn ipv6_missing_closing_bracket_should_fail() {
        let builder = builder_with_address("[::1:8080");
        assert!(matches!(
            builder.build(),
            Err(IggyError::InvalidIpAddress(_, _))
        ));
    }

    #[test]
    fn missing_port_should_fail() {
        let builder = builder_with_address("127.0.0.1");
        assert!(matches!(
            builder.build(),
            Err(IggyError::InvalidIpAddress(_, _))
        ));
    }
}
