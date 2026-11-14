// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

use crate::error::McpRuntimeError;
use ::configs::{FileConfigProvider, TypedEnvProvider};
use axum::http::{HeaderName, HeaderValue, Method};
use configs_derive::ConfigEnv;
use figment::{
    Metadata, Profile, Provider,
    providers::{Format, Toml},
    value::Dict,
};
use iggy::prelude::{DEFAULT_ROOT_PASSWORD, DEFAULT_ROOT_USERNAME};
use serde::{Deserialize, Serialize};
use std::fmt::{Display as FmtDisplay, Formatter};
use std::str::FromStr;
use strum::Display;
use tower_http::cors::{AllowOrigin, CorsLayer};

const ANY_ORIGIN: &str = "*";

#[derive(Debug, Clone, Deserialize, Serialize, ConfigEnv)]
#[config_env(prefix = "IGGY_MCP_", name = "iggy-mcp-server-config")]
#[serde(default)]
pub struct McpServerConfig {
    pub http: HttpConfig,
    pub iggy: IggyConfig,
    pub permissions: PermissionsConfig,
    #[config_env(leaf)]
    pub transport: McpTransport,
    pub telemetry: TelemetryConfig,
}

#[derive(Debug, Clone, Serialize, Deserialize, ConfigEnv)]
pub struct IggyConfig {
    pub address: String,
    pub username: String,
    #[config_env(secret)]
    pub password: String,
    #[config_env(secret)]
    pub token: String,
    pub consumer: String,
    pub tls: IggyTlsConfig,
}

#[derive(Debug, Default, Clone, Serialize, Deserialize, ConfigEnv)]
pub struct IggyTlsConfig {
    pub enabled: bool,
    pub ca_file: String,
    pub domain: Option<String>,
}

#[derive(Debug, Clone, Deserialize, Serialize, ConfigEnv)]
pub struct PermissionsConfig {
    pub create: bool,
    pub read: bool,
    pub update: bool,
    pub delete: bool,
    #[serde(default)]
    pub admin: bool,
}

#[derive(Debug, Clone, Deserialize, Serialize, ConfigEnv)]
#[serde(default)]
pub struct HttpConfig {
    pub address: String,
    pub path: String,
    #[config_env(secret)]
    pub auth_token: String,
    pub allowed_hosts: Vec<String>,
    pub cors: HttpCorsConfig,
    pub tls: HttpTlsConfig,
}

#[derive(Debug, Default, Deserialize, Serialize, Clone, ConfigEnv)]
pub struct HttpTlsConfig {
    pub enabled: bool,
    pub cert_file: String,
    pub key_file: String,
}

#[derive(Debug, Default, Deserialize, Serialize, Clone, ConfigEnv)]
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

#[derive(Debug, Deserialize, Serialize, Clone, ConfigEnv)]
pub struct TelemetryConfig {
    pub enabled: bool,
    pub service_name: String,
    pub logs: TelemetryLogsConfig,
    pub traces: TelemetryTracesConfig,
}

impl Default for TelemetryConfig {
    fn default() -> Self {
        Self {
            enabled: false,
            service_name: "iggy-mcp".to_owned(),
            logs: TelemetryLogsConfig::default(),
            traces: TelemetryTracesConfig::default(),
        }
    }
}

impl FmtDisplay for TelemetryConfig {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "{{ enabled: {}, service_name: {}, logs: {}, traces: {} }}",
            self.enabled, self.service_name, self.logs, self.traces
        )
    }
}

#[derive(Debug, Deserialize, Serialize, Clone, ConfigEnv)]
pub struct TelemetryLogsConfig {
    #[config_env(leaf)]
    pub transport: TelemetryTransport,
    pub endpoint: String,
}

impl Default for TelemetryLogsConfig {
    fn default() -> Self {
        Self {
            transport: TelemetryTransport::Grpc,
            endpoint: "http://localhost:4317".to_owned(),
        }
    }
}

impl FmtDisplay for TelemetryLogsConfig {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "{{ transport: {}, endpoint: {} }}",
            self.transport, self.endpoint
        )
    }
}

#[derive(Debug, Deserialize, Serialize, Clone, ConfigEnv)]
pub struct TelemetryTracesConfig {
    #[config_env(leaf)]
    pub transport: TelemetryTransport,
    pub endpoint: String,
}

impl Default for TelemetryTracesConfig {
    fn default() -> Self {
        Self {
            transport: TelemetryTransport::Grpc,
            endpoint: "http://localhost:4317".to_owned(),
        }
    }
}

impl FmtDisplay for TelemetryTracesConfig {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "{{ transport: {}, endpoint: {} }}",
            self.transport, self.endpoint
        )
    }
}

#[derive(Debug, Serialize, Deserialize, PartialEq, Display, Copy, Clone)]
#[serde(rename_all = "lowercase")]
pub enum TelemetryTransport {
    #[strum(to_string = "grpc")]
    Grpc,
    #[strum(to_string = "http")]
    Http,
}

impl FromStr for TelemetryTransport {
    type Err = String;
    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s {
            "grpc" => Ok(TelemetryTransport::Grpc),
            "http" => Ok(TelemetryTransport::Http),
            _ => Err(format!("Invalid telemetry transport: {s}")),
        }
    }
}

impl Default for McpServerConfig {
    fn default() -> Self {
        Self {
            http: HttpConfig::default(),
            iggy: IggyConfig::default(),
            permissions: PermissionsConfig::default(),
            transport: McpTransport::Http,
            telemetry: TelemetryConfig::default(),
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
            address: "127.0.0.1:8082".to_owned(),
            path: "/mcp".to_owned(),
            auth_token: String::new(),
            allowed_hosts: vec![
                "localhost".to_owned(),
                "127.0.0.1".to_owned(),
                "::1".to_owned(),
            ],
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
            admin: false,
        }
    }
}

impl HttpCorsConfig {
    pub fn allows_any_origin(&self) -> bool {
        self.allowed_origins
            .iter()
            .any(|origin| origin == ANY_ORIGIN)
    }
}

pub fn configure_cors(config: &HttpCorsConfig) -> Result<CorsLayer, McpRuntimeError> {
    let allowed_origins = if config.allows_any_origin() {
        if config.allowed_origins.len() > 1 {
            return Err(McpRuntimeError::InvalidCorsConfiguration(format!(
                "the {ANY_ORIGIN} origin cannot be listed with other origins"
            )));
        }
        if config.allow_credentials {
            return Err(McpRuntimeError::InvalidCorsConfiguration(format!(
                "allow_credentials cannot be combined with the {ANY_ORIGIN} origin"
            )));
        }
        AllowOrigin::any()
    } else if config.allowed_origins.is_empty() {
        AllowOrigin::default()
    } else {
        AllowOrigin::list(parsed_values::<HeaderValue>(
            &config.allowed_origins,
            "origin",
        )?)
    };
    let allowed_headers = parsed_values::<HeaderName>(&config.allowed_headers, "header")?;
    let exposed_headers = parsed_values::<HeaderName>(&config.exposed_headers, "header")?;
    let allowed_methods = config
        .allowed_methods
        .iter()
        .filter(|method| !method.is_empty())
        .map(|method| match method.to_uppercase().as_str() {
            "GET" => Ok(Method::GET),
            "POST" => Ok(Method::POST),
            "PUT" => Ok(Method::PUT),
            "DELETE" => Ok(Method::DELETE),
            "HEAD" => Ok(Method::HEAD),
            "OPTIONS" => Ok(Method::OPTIONS),
            "CONNECT" => Ok(Method::CONNECT),
            "PATCH" => Ok(Method::PATCH),
            "TRACE" => Ok(Method::TRACE),
            _ => Err(McpRuntimeError::InvalidCorsConfiguration(format!(
                "invalid HTTP method {method}"
            ))),
        })
        .collect::<Result<Vec<_>, _>>()?;

    Ok(CorsLayer::new()
        .allow_methods(allowed_methods)
        .allow_origin(allowed_origins)
        .allow_headers(allowed_headers)
        .expose_headers(exposed_headers)
        .allow_credentials(config.allow_credentials)
        .allow_private_network(config.allow_private_network))
}

fn parsed_values<T: FromStr>(values: &[String], kind: &str) -> Result<Vec<T>, McpRuntimeError> {
    values
        .iter()
        .filter(|value| !value.is_empty())
        .map(|value| {
            value.parse().map_err(|_| {
                McpRuntimeError::InvalidCorsConfiguration(format!("invalid {kind} {value:?}"))
            })
        })
        .collect()
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
            "{{ create: {}, read: {}, update: {}, delete: {}, admin: {} }}",
            self.create, self.read, self.update, self.delete, self.admin
        )
    }
}

impl std::fmt::Display for McpServerConfig {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "{{ http: {}, iggy: {}, permissions: {:?}, transport: {}, telemetry: {} }}",
            self.http, self.iggy, self.permissions, self.transport, self.telemetry
        )
    }
}

impl std::fmt::Display for HttpConfig {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "{{ address: {}, path: {}, auth_token: {}, allowed_hosts: {:?}, cors: {}, tls: {} }}",
            self.address,
            self.path,
            if self.auth_token.is_empty() {
                ""
            } else {
                "****"
            },
            self.allowed_hosts,
            self.cors,
            self.tls
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
    provider: TypedEnvProvider<McpServerConfig>,
}

impl Default for McpServerEnvProvider {
    fn default() -> Self {
        Self {
            provider: TypedEnvProvider::from_config(McpServerConfig::ENV_PREFIX),
        }
    }
}

impl Provider for McpServerEnvProvider {
    fn metadata(&self) -> Metadata {
        Metadata::named(McpServerConfig::ENV_PROVIDER_NAME)
    }

    fn data(&self) -> Result<figment::value::Map<Profile, Dict>, figment::Error> {
        self.provider.data()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::panic::catch_unwind;

    #[test]
    fn given_invalid_cors_values_when_configuring_should_refuse_them() {
        let configs = [
            HttpCorsConfig {
                allowed_methods: vec!["FETCH".to_owned()],
                ..HttpCorsConfig::default()
            },
            HttpCorsConfig {
                allowed_origins: vec!["http://console.example\n".to_owned()],
                ..HttpCorsConfig::default()
            },
            HttpCorsConfig {
                allowed_headers: vec!["not a header".to_owned()],
                ..HttpCorsConfig::default()
            },
            HttpCorsConfig {
                allowed_origins: vec!["http://console.example".to_owned(), "*".to_owned()],
                ..HttpCorsConfig::default()
            },
        ];

        for config in configs {
            let result = catch_unwind(|| configure_cors(&config));
            assert!(
                matches!(result, Ok(Err(_))),
                "{config} must be refused with an error, not accepted or a panic"
            );
        }
    }
}
