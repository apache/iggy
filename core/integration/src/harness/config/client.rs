/*
 * Licensed to the Apache Software Foundation (ASF) under one
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

use bon::Builder;
use iggy::prelude::{DEFAULT_ROOT_PASSWORD, DEFAULT_ROOT_USERNAME};
use iggy_common::TransportProtocol;
use std::path::PathBuf;

#[derive(Debug, Clone)]
pub struct AutoLoginConfig {
    pub username: String,
    pub password: String,
}

impl AutoLoginConfig {
    pub fn root() -> Self {
        Self {
            username: DEFAULT_ROOT_USERNAME.to_string(),
            password: DEFAULT_ROOT_PASSWORD.to_string(),
        }
    }

    pub fn new(username: impl Into<String>, password: impl Into<String>) -> Self {
        Self {
            username: username.into(),
            password: password.into(),
        }
    }
}

#[derive(Debug, Clone, Builder)]
pub struct ClientConfig {
    #[builder(default)]
    pub transport: TransportProtocol,
    #[builder(default)]
    pub tcp_nodelay: bool,
    #[builder(default)]
    pub tls_enabled: bool,
    #[builder(into)]
    pub tls_domain: Option<String>,
    #[builder(into)]
    pub tls_ca_file: Option<PathBuf>,
    #[builder(default)]
    pub tls_validate_certificate: bool,
    pub auto_login: Option<AutoLoginConfig>,
}

impl Default for ClientConfig {
    fn default() -> Self {
        Self::builder().build()
    }
}

impl ClientConfig {
    pub fn tcp() -> Self {
        Self::default()
    }

    /// Creates TCP client that auto-logins as root after connect.
    pub fn root_tcp() -> Self {
        Self::builder().auto_login(AutoLoginConfig::root()).build()
    }

    pub fn quic() -> Self {
        Self::builder().transport(TransportProtocol::Quic).build()
    }

    /// Creates QUIC client that auto-logins as root after connect.
    pub fn root_quic() -> Self {
        Self::builder()
            .transport(TransportProtocol::Quic)
            .auto_login(AutoLoginConfig::root())
            .build()
    }

    pub fn http() -> Self {
        Self::builder().transport(TransportProtocol::Http).build()
    }

    /// Creates HTTP client that auto-logins as root after connect.
    pub fn root_http() -> Self {
        Self::builder()
            .transport(TransportProtocol::Http)
            .auto_login(AutoLoginConfig::root())
            .build()
    }

    pub fn websocket() -> Self {
        Self::builder()
            .transport(TransportProtocol::WebSocket)
            .build()
    }

    /// Creates WebSocket client that auto-logins as root after connect.
    pub fn root_websocket() -> Self {
        Self::builder()
            .transport(TransportProtocol::WebSocket)
            .auto_login(AutoLoginConfig::root())
            .build()
    }

    pub fn with_nodelay(mut self) -> Self {
        self.tcp_nodelay = true;
        self
    }

    pub fn with_tls(
        mut self,
        domain: impl Into<String>,
        ca_file: Option<PathBuf>,
        validate: bool,
    ) -> Self {
        self.tls_enabled = true;
        self.tls_domain = Some(domain.into());
        self.tls_ca_file = ca_file;
        self.tls_validate_certificate = validate;
        self
    }

    /// Add auto-login to any client config.
    pub fn with_auto_login(
        mut self,
        username: impl Into<String>,
        password: impl Into<String>,
    ) -> Self {
        self.auto_login = Some(AutoLoginConfig::new(username, password));
        self
    }

    /// Add root auto-login to any client config.
    pub fn with_root_login(mut self) -> Self {
        self.auto_login = Some(AutoLoginConfig::root());
        self
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_client_config() {
        let tcp = ClientConfig::tcp().with_nodelay();
        assert_eq!(tcp.transport, TransportProtocol::Tcp);
        assert!(tcp.tcp_nodelay);

        let quic = ClientConfig::quic();
        assert_eq!(quic.transport, TransportProtocol::Quic);

        let tls = ClientConfig::tcp().with_tls("localhost", None, false);
        assert!(tls.tls_enabled);
        assert_eq!(tls.tls_domain, Some("localhost".to_string()));
    }

    #[test]
    fn test_root_tcp_config() {
        let root_tcp = ClientConfig::root_tcp();
        assert_eq!(root_tcp.transport, TransportProtocol::Tcp);
        assert!(root_tcp.auto_login.is_some());
        let login = root_tcp.auto_login.unwrap();
        assert_eq!(login.username, DEFAULT_ROOT_USERNAME);
        assert_eq!(login.password, DEFAULT_ROOT_PASSWORD);
    }

    #[test]
    fn test_with_auto_login() {
        let config = ClientConfig::tcp().with_auto_login("user", "pass");
        assert!(config.auto_login.is_some());
        let login = config.auto_login.unwrap();
        assert_eq!(login.username, "user");
        assert_eq!(login.password, "pass");
    }

    #[test]
    fn test_with_root_login() {
        let config = ClientConfig::quic().with_root_login();
        assert_eq!(config.transport, TransportProtocol::Quic);
        assert!(config.auto_login.is_some());
        let login = config.auto_login.unwrap();
        assert_eq!(login.username, DEFAULT_ROOT_USERNAME);
    }
}
