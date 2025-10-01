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
use iggy::prelude::{Client, ClientWrapper, WebSocketClientConfig};
use iggy::websocket::websocket_client::WebSocketClient;
use iggy_common::TransportProtocol;
use std::sync::Arc;

#[derive(Debug, Clone)]
pub struct WebSocketClientFactory {
    pub server_addr: String,
}

#[async_trait]
impl ClientFactory for WebSocketClientFactory {
    async fn create_client(&self) -> ClientWrapper {
        let config = WebSocketClientConfig {
            server_address: self.server_addr.clone(),
            ..WebSocketClientConfig::default()
        };
        let client = WebSocketClient::create(Arc::new(config)).unwrap();
        Client::connect(&client).await.unwrap();
        ClientWrapper::WebSocket(client)
    }

    fn transport(&self) -> TransportProtocol {
        TransportProtocol::WebSocket
    }

    fn server_addr(&self) -> String {
        self.server_addr.clone()
    }
}

unsafe impl Send for WebSocketClientFactory {}
unsafe impl Sync for WebSocketClientFactory {}
