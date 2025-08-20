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

use crate::{ConnectionString, ConnectionStringOptions, HttpConnectionStringOptions};

/// Configuration for the HTTP client.
#[derive(Debug, Clone)]
pub struct HttpClientConfig {
    /// The URL of the Iggy API.
    pub api_url: String,
    /// The number of retries to perform on transient errors.
    pub retries: u32,
}

impl Default for HttpClientConfig {
    fn default() -> HttpClientConfig {
        HttpClientConfig {
            api_url: "http://127.0.0.1:3000".to_string(),
            retries: 3,
        }
    }
}

impl From<ConnectionString<HttpConnectionStringOptions>> for HttpClientConfig {
    fn from(connection_string: ConnectionString<HttpConnectionStringOptions>) -> Self {
        HttpClientConfig {
            api_url: format!("http://{}", connection_string.server_address()),
            retries: connection_string.options().retries().unwrap(),
        }
    }
}
