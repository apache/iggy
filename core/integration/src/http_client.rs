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
use iggy::http::http_client::HttpClient;
use iggy::prelude::{ClientWrapper, HttpClientConfig};
use std::sync::Arc;

#[derive(Debug, Clone)]
pub struct HttpClientFactory {
    pub server_addr: String,
}

#[async_trait]
impl ClientFactory for HttpClientFactory {
    async fn create_client(&self) -> ClientWrapper {
        let config = HttpClientConfig {
            api_url: format!("http://{}", self.server_addr.clone()),
            ..HttpClientConfig::default()
        };
        let client = HttpClient::create(Arc::new(config)).unwrap();
        ClientWrapper::Http(client)
    }

    fn transport(&self) -> Transport {
        Transport::Http
    }

    fn server_addr(&self) -> String {
        self.server_addr.clone()
    }
}

unsafe impl Send for HttpClientFactory {}
unsafe impl Sync for HttpClientFactory {}
