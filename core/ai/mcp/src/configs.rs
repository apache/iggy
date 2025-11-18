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

use axum::http::Method;
use figment::{
    Metadata, Profile, Provider,
    providers::{Format, Toml},
    value::Dict,
};
use iggy::prelude::{DEFAULT_ROOT_PASSWORD, DEFAULT_ROOT_USERNAME};
use iggy_common::{CustomEnvProvider, FileConfigProvider};
use serde::{Deserialize, Serialize};
use std::fmt::Formatter;
use strum::Display;
use tower_http::cors::{AllowOrigin, CorsLayer};

#[derive(Debug, Clone, Deserialize, Serialize)]
#[serde(default)]
pub struct McpServerConfig {
    pub http: HttpConfig,
    pub iggy: IggyConfig,
    pub permissions: PermissionsConfig,
    pub transport: McpTransport,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct IggyConfig {
    pub address: String,
    pub username: String,
    pub password: String,
    pub token: String,
    pub consumer: String,
    pub tls: IggyTlsConfig,
}

#[derive(Debug, Default, Clone, Serialize, Deserialize)]
pub struct IggyTlsConfig {
    pub enabled: bool,
    pub ca_file: String,
    pub domain: Option<String>,
}

#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct PermissionsConfig {
    pub create: bool,
    pub read: bool,
    pub update: bool,
    pub delete: bool,
}

#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct HttpConfig {
    pub address: String,
    pub path: String,
    pub cors: HttpCorsConfig,
    pub tls: HttpTlsConfig,
}

#[derive(Debug, Default, Deserialize, Serialize, Clone)]
pub struct HttpTlsConfig {
    pub enabled: bool,
    pub cert_file: String,
    pub key_file: String,
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
            http: HttpConfig::default(),
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
            username: DEFAULT_ROOT_USERNAME.to_owned(),
            password: DEFAULT_ROOT_PASSWORD.to_owned(),
            token: "".to_owned(),
            consumer: "iggy-mcp".to_owned(),
            tls: IggyTlsConfig::default(),
        }
    }
}

impl Default for HttpConfig {
    fn default() -> Self {
        Self {
            address: "localhost:8082".to_owned(),
            path: "/mcp".to_owned(),
            cors: HttpCorsConfig::default(),
            tls: HttpTlsConfig::default(),
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

impl std::fmt::Display for IggyConfig {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "{{ address: {}, username: {}, password: {}, token: {}, consumer: {}, tls: {} }}",
            self.address,
            self.username,
            if !self.password.is_empty() {
                "****"
            } else {
                ""
            },
            if !self.token.is_empty() { "****" } else { "" },
            self.consumer,
            self.tls
        )
    }
}

impl std::fmt::Display for IggyTlsConfig {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "{{ enabled: {}, ca_file: {:?}, domain: {:?} }}",
            self.enabled, self.ca_file, self.domain
        )
    }
}

impl std::fmt::Display for PermissionsConfig {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "{{ create: {}, read: {}, update: {}, delete: {} }}",
            self.create, self.read, self.update, self.delete
        )
    }
}

impl std::fmt::Display for McpServerConfig {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "{{ http: {}, iggy: {}, permissions: {:?}, transport: {} }}",
            self.http, self.iggy, self.permissions, self.transport
        )
    }
}

impl std::fmt::Display for HttpConfig {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "{{ address: {}, path: {}, cors: {}, tls: {} }}",
            self.address, self.path, self.cors, self.tls
        )
    }
}

impl std::fmt::Display for HttpTlsConfig {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "{{ enabled: {}, cert_file: {}, key_file: {} }}",
            self.enabled, self.cert_file, self.key_file
        )
    }
}

impl std::fmt::Display for HttpCorsConfig {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "{{ enabled: {}, allowed_methods: {:?}, allowed_origins: {:?}, allowed_headers: {:?}, exposed_headers: {:?}, allow_credentials: {}, allow_private_network: {} }}",
            self.enabled,
            self.allowed_methods,
            self.allowed_origins,
            self.allowed_headers,
            self.exposed_headers,
            self.allow_credentials,
            self.allow_private_network
        )
    }
}

impl McpServerConfig {
    pub fn config_provider(path: String) -> FileConfigProvider<McpServerEnvProvider> {
        let default_config = Toml::string(include_str!("../../../ai/mcp/config.toml"));
        FileConfigProvider::new(
            path,
            McpServerEnvProvider::default(),
            true,
            Some(default_config),
        )
    }
}

#[derive(Debug, Clone)]
pub struct McpServerEnvProvider {
    provider: CustomEnvProvider<McpServerConfig>,
}

impl Default for McpServerEnvProvider {
    fn default() -> Self {
        Self {
            provider: CustomEnvProvider::new("IGGY_MCP_", &[]),
        }
    }
}

impl Provider for McpServerEnvProvider {
    fn metadata(&self) -> Metadata {
        Metadata::named("iggy-mcp-server-config")
    }

    fn data(&self) -> Result<figment::value::Map<Profile, Dict>, figment::Error> {
        self.provider.deserialize().map_err(|_| {
            figment::Error::from("Cannot deserialize environment variables for MCP config")
        })
    }
}
