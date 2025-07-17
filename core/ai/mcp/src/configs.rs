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

use serde::{Deserialize, Serialize};
use strum::Display;

#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct McpServerConfig {
    pub http_api: Option<HttpApiConfig>,
    pub iggy: IggyConfig,
    pub permissions: PermissionsConfig,
    pub transport: McpTransport,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct IggyConfig {
    pub address: String,
    pub username: Option<String>,
    pub password: Option<String>,
    pub token: Option<String>,
    pub consumer_name: Option<String>,
}

#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct HttpApiConfig {
    pub address: String,
    pub path: String,
    pub api_key: Option<String>,
    pub tls: Option<HttpTlsConfig>,
}

#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct PermissionsConfig {
    pub create: bool,
    pub read: bool,
    pub update: bool,
    pub delete: bool,
}

#[derive(Debug, Deserialize, Serialize, Clone)]
pub struct HttpTlsConfig {
    pub enabled: bool,
    pub cert_file: String,
    pub key_file: String,
}

#[derive(Clone, Copy, Debug, Default, Display, PartialEq, Eq, Serialize, Deserialize)]
#[strum(serialize_all = "snake_case")]
#[serde(rename_all = "snake_case")]
pub enum McpTransport {
    #[default]
    #[strum(to_string = "http")]
    Http,
    #[strum(to_string = "stdio")]
    Stdio,
}
