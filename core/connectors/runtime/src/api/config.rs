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

use crate::{configs::ConfigFormat, error::RuntimeError};
use axum::http::{HeaderValue, Method};
use serde::{Deserialize, Serialize};
use tower_http::cors::{AllowOrigin, CorsLayer};
use tracing::error;

pub const JSON_HEADER: HeaderValue = HeaderValue::from_static("application/json");
pub const YAML_HEADER: HeaderValue = HeaderValue::from_static("application/yaml");
pub const TOML_HEADER: HeaderValue = HeaderValue::from_static("application/toml");
pub const TEXT_HEADER: HeaderValue = HeaderValue::from_static("text/plain");

#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct HttpApiConfig {
    pub enabled: bool,
    pub address: String,
    pub api_key: Option<String>,
    pub cors: Option<HttpCorsConfig>,
    pub tls: Option<HttpTlsConfig>,
}

#[derive(Debug, Deserialize, Serialize, Clone)]
pub struct HttpCorsConfig {
    pub enabled: bool,
    pub allowed_methods: Vec<String>,
    pub allowed_origins: Vec<String>,
    pub allowed_headers: Vec<String>,
    pub exposed_headers: Vec<String>,
    pub allow_credentials: bool,
    pub allow_private_network: bool,
}

#[derive(Debug, Deserialize, Serialize, Clone)]
pub struct HttpTlsConfig {
    pub enabled: bool,
    pub cert_file: String,
    pub key_file: String,
}

pub fn map_connector_config(
    config: &serde_json::Value,
    format: ConfigFormat,
) -> Result<(HeaderValue, String), RuntimeError> {
    match format {
        ConfigFormat::Json => Ok((JSON_HEADER, config.to_string())),
        ConfigFormat::Yaml => {
            let config = serde_yml::to_value(config).map_err(|error| {
                error!("Failed to convert configuration to YAML. {error}");
                RuntimeError::CannotConvertConfiguration
            })?;
            let config = serde_yml::to_string(&config).map_err(|error| {
                error!("Failed to serialize YAML configuration. {error}");
                RuntimeError::CannotConvertConfiguration
            })?;
            Ok((YAML_HEADER, config))
        }
        ConfigFormat::Toml => {
            let config = toml::to_string(config).map_err(|error| {
                error!("Failed to convert configuration to TOML. {error}");
                RuntimeError::CannotConvertConfiguration
            })?;
            Ok((TOML_HEADER, config))
        }
        ConfigFormat::Text => Ok((TEXT_HEADER, config.to_string())),
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
            _ => panic!("Invalid HTTP method: {}", s),
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
