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

use crate::test_server::ClientFactory;
use async_trait::async_trait;
use iggy::client::Client;
use iggy::tcp::client::TcpClient;
use iggy::tcp::config::TcpClientConfig;
use std::sync::Arc;

#[derive(Debug, Clone, Default)]
pub struct TcpClientFactory {
    pub server_addr: String,
    pub nodelay: bool,
}

#[async_trait]
impl ClientFactory for TcpClientFactory {
    async fn create_client(&self) -> Box<dyn Client> {
        let config = TcpClientConfig {
            server_address: self.server_addr.clone(),
            nodelay: self.nodelay,
            ..TcpClientConfig::default()
        };
        let client = TcpClient::create(Arc::new(config)).unwrap_or_else(|e| {
            panic!(
                "Failed to create TcpClient, iggy-server has address {}, error: {:?}",
                self.server_addr, e
            )
        });
        iggy::client::Client::connect(&client)
            .await
            .unwrap_or_else(|e| {
                panic!(
                    "Failed to connect to iggy-server at {}, error: {:?}",
                    self.server_addr, e
                )
            });
        Box::new(client)
    }
}

unsafe impl Send for TcpClientFactory {}
unsafe impl Sync for TcpClientFactory {}
