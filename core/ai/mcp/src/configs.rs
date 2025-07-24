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

use axum::http::Method;
use iggy::prelude::{DEFAULT_ROOT_PASSWORD, DEFAULT_ROOT_USERNAME};
use serde::{Deserialize, Serialize};
use strum::Display;
use tower_http::cors::{AllowOrigin, CorsLayer};

#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct McpServerConfig {
    pub http: Option<HttpApiConfig>,
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
    pub consumer: Option<String>,
}

#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct HttpApiConfig {
    pub address: String,
    pub path: String,
    pub cors: Option<HttpCorsConfig>,
    pub tls: Option<HttpTlsConfig>,
}

#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct PermissionsConfig {
    pub create: bool,
    pub read: bool,
    pub update: bool,
    pub delete: bool,
}

#[derive(Debug, Default, Deserialize, Serialize, Clone)]
pub struct HttpTlsConfig {
    pub enabled: bool,
    pub cert: String,
    pub key: String,
}

#[derive(Debug, Default, Deserialize, Serialize, Clone)]
pub struct HttpCorsConfig {
    pub enabled: bool,
    pub allowed_methods: Vec<String>,
    pub allowed_origins: Vec<String>,
    pub allowed_headers: Vec<String>,
    pub exposed_headers: Vec<String>,
    pub allow_credentials: bool,
    pub allow_private_network: bool,
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

impl Default for McpServerConfig {
    fn default() -> Self {
        Self {
            http: Some(HttpApiConfig::default()),
            iggy: IggyConfig::default(),
            permissions: PermissionsConfig::default(),
            transport: McpTransport::Http,
        }
    }
}

impl Default for IggyConfig {
    fn default() -> Self {
        Self {
            address: "localhost:8090".to_owned(),
            username: Some(DEFAULT_ROOT_USERNAME.to_owned()),
            password: Some(DEFAULT_ROOT_PASSWORD.to_owned()),
            token: None,
            consumer: None,
        }
    }
}

impl Default for HttpApiConfig {
    fn default() -> Self {
        Self {
            address: "localhost:8082".to_owned(),
            path: "/mcp".to_owned(),
            tls: None,
            cors: None,
        }
    }
}

impl Default for PermissionsConfig {
    fn default() -> Self {
        Self {
            create: true,
            read: true,
            update: true,
            delete: true,
        }
    }
}

pub fn configure_cors(config: &HttpCorsConfig) -> CorsLayer {
    let allowed_origins = match &config.allowed_origins {
        origins if origins.is_empty() => AllowOrigin::default(),
        origins if origins.first().unwrap() == "*" => AllowOrigin::any(),
        origins => AllowOrigin::list(origins.iter().map(|s| s.parse().unwrap())),
    };

    let allowed_headers = config
        .allowed_headers
        .iter()
        .filter(|s| !s.is_empty())
        .map(|s| s.parse().unwrap())
        .collect::<Vec<_>>();

    let exposed_headers = config
        .exposed_headers
        .iter()
        .filter(|s| !s.is_empty())
        .map(|s| s.parse().unwrap())
        .collect::<Vec<_>>();

    let allowed_methods = config
        .allowed_methods
        .iter()
        .filter(|s| !s.is_empty())
        .map(|s| match s.to_uppercase().as_str() {
            "GET" => Method::GET,
            "POST" => Method::POST,
            "PUT" => Method::PUT,
            "DELETE" => Method::DELETE,
            "HEAD" => Method::HEAD,
            "OPTIONS" => Method::OPTIONS,
            "CONNECT" => Method::CONNECT,
            "PATCH" => Method::PATCH,
            "TRACE" => Method::TRACE,
            _ => panic!("Invalid HTTP method: {s}"),
        })
        .collect::<Vec<_>>();

    CorsLayer::new()
        .allow_methods(allowed_methods)
        .allow_origin(allowed_origins)
        .allow_headers(allowed_headers)
        .expose_headers(exposed_headers)
        .allow_credentials(config.allow_credentials)
        .allow_private_network(config.allow_private_network)
}
