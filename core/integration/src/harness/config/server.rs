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

use super::common::{EncryptionConfig, IpAddrKind, TlsConfig};
use bon::Builder;
use std::collections::HashMap;

#[derive(Debug, Clone, Builder)]
pub struct TestServerConfig {
    #[builder(default = true)]
    pub quic_enabled: bool,
    #[builder(default = true)]
    pub websocket_enabled: bool,
    #[builder(default = true)]
    pub http_enabled: bool,
    pub encryption: Option<EncryptionConfig>,
    pub tls: Option<TlsConfig>,
    pub websocket_tls: Option<TlsConfig>,
    #[builder(default = true)]
    pub cleanup: bool,
    #[builder(default)]
    pub ip_kind: IpAddrKind,
    #[builder(default)]
    pub extra_envs: HashMap<String, String>,
    #[builder(into)]
    pub executable_path: Option<String>,
}

impl Default for TestServerConfig {
    fn default() -> Self {
        Self::builder().build()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_server_config_builder() {
        let config = TestServerConfig::builder()
            .quic_enabled(false)
            .extra_envs(HashMap::from([("FOO".to_string(), "BAR".to_string())]))
            .build();

        assert!(!config.quic_enabled);
        assert_eq!(config.extra_envs.get("FOO"), Some(&"BAR".to_string()));
    }

    #[test]
    fn test_server_config_defaults() {
        let config = TestServerConfig::default();
        assert!(config.quic_enabled);
        assert!(config.websocket_enabled);
        assert!(config.http_enabled);
        assert!(config.cleanup);
    }
}
