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

use crate::test_server::{ClientFactory, Transport};
use async_trait::async_trait;
use iggy::prelude::{ClientWrapper, TcpClientConfig, TcpClientSyncTcp, TcpClientSyncTls};
use std::sync::Arc;

#[derive(Debug, Clone, Default)]
pub struct TcpClientSyncFactory {
    pub server_addr: String,
    pub nodelay: bool,
    pub tls_enabled: bool,
    pub tls_domain: String,
    pub tls_ca_file: Option<String>,
    pub tls_validate_certificate: bool,
}

#[async_trait]
impl ClientFactory for TcpClientSyncFactory {
    async fn create_client(&self) -> ClientWrapper {
        let config = TcpClientConfig {
            server_address: self.server_addr.clone(),
            nodelay: self.nodelay,
            tls_enabled: self.tls_enabled,
            tls_domain: self.tls_domain.clone(),
            tls_ca_file: self.tls_ca_file.clone(),
            tls_validate_certificate: self.tls_validate_certificate,
            ..TcpClientConfig::default()
        };

        let client: Box<dyn iggy_binary_protocol::Client> = if self.tls_enabled {
            Box::new(TcpClientSyncTls::create_tcp_tls(Arc::new(config)).unwrap_or_else(|e| {
                panic!(
                    "Failed to create TcpClientSyncTls, iggy-server has address {}, error: {:?}",
                    self.server_addr, e
                )
            }))
        } else {
            Box::new(TcpClientSyncTcp::create_tcp(Arc::new(config)).unwrap_or_else(|e| {
                panic!(
                    "Failed to create TcpClientSyncTcp, iggy-server has address {}, error: {:?}",
                    self.server_addr, e
                )
            }))
        };

        // Connect the client
        client.connect().await.unwrap_or_else(|e| {
            if self.tls_enabled {
                panic!(
                    "Failed to connect to iggy-server at {} with TLS enabled, error: {:?}\\n\\\n                    Hint: Make sure the server is started with TLS enabled and self-signed certificate:\\n\\\n                    IGGY_TCP_TLS_ENABLED=true IGGY_TCP_TLS_SELF_SIGNED=true\\n\n                    or start iggy-bench with relevant tcp tls arguments: --tls --tls-domain <domain> --tls-ca-file <ca_file>\\n",
                    self.server_addr, e
                )
            } else {
                panic!(
                    "Failed to connect to iggy-server at {}, error: {:?}",
                    self.server_addr, e
                )
            }
        });

        ClientWrapper::TcpSync(client)
    }

    fn transport(&self) -> Transport {
        Transport::Tcp
    }

    fn server_addr(&self) -> String {
        self.server_addr.clone()
    }
}