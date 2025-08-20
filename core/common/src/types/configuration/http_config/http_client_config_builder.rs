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

use crate::HttpClientConfig;

/// The builder for the `HttpClientConfig` configuration.
/// Allows configuring the HTTP client with custom settings or using defaults:
/// - `api_url`: Default is "http://127.0.0.1:3000"
/// - `retries`: Default is 3.
#[derive(Debug, Default)]
pub struct HttpClientConfigBuilder {
    config: HttpClientConfig,
}

impl HttpClientConfigBuilder {
    /// Create a new `HttpClientConfigBuilder` with default settings.
    pub fn new() -> Self {
        HttpClientConfigBuilder::default()
    }

    /// Sets the API URL for the HTTP client.
    pub fn with_api_url(mut self, url: String) -> Self {
        self.config.api_url = url;
        self
    }

    /// Sets the number of retries for the HTTP client.
    pub fn with_retries(mut self, retries: u32) -> Self {
        self.config.retries = retries;
        self
    }

    /// Builds the `HttpClientConfig` instance.
    pub fn build(self) -> HttpClientConfig {
        self.config
    }
}
