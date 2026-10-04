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

use crate::configs::connectors::ConfigFormat;
use crate::error::RuntimeError;
use axum::http::{HeaderValue, Method};
use configs_derive::ConfigEnv;
use iggy_common::serde_secret::serialize_secret;
use secrecy::SecretString;
use serde::{Deserialize, Serialize};
use std::fmt::Formatter;
use tower_http::cors::{AllowOrigin, CorsLayer};
use tracing::error;

pub const JSON_HEADER: HeaderValue = HeaderValue::from_static("application/json");
pub const YAML_HEADER: HeaderValue = HeaderValue::from_static("application/yaml");
pub const TOML_HEADER: HeaderValue = HeaderValue::from_static("application/toml");
pub const TEXT_HEADER: HeaderValue = HeaderValue::from_static("text/plain");

#[derive(Clone, Deserialize, Serialize, ConfigEnv)]
pub struct HttpConfig {
    pub enabled: bool,
    pub address: String,
    #[config_env(secret, leaf)]
    #[serde(serialize_with = "serialize_secret")]
    pub api_key: SecretString,
    pub cors: HttpCorsConfig,
    pub tls: HttpTlsConfig,
    pub metrics: HttpMetricsConfig,
}

impl std::fmt::Debug for HttpConfig {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("HttpConfig")
            .field("enabled", &self.enabled)
            .field("address", &self.address)
            .field("api_key", &"[REDACTED]")
            .field("cors", &self.cors)
            .field("tls", &self.tls)
            .field("metrics", &self.metrics)
            .finish()
    }
}

#[derive(Debug, Clone, Deserialize, Serialize, ConfigEnv)]
pub struct HttpMetricsConfig {
    pub enabled: bool,
    pub endpoint: String,
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

impl HttpCorsConfig {
    /// Whether `configure_cors` will turn this into `AllowOrigin::any()`.
    ///
    /// Only the first entry decides, because that is what the mapping below
    /// reads. A `"*"` in any later position never reaches a served request:
    /// `AllowOrigin::list` panics on a wildcard, so such a config takes the
    /// process down at startup rather than allowing anything.
    ///
    /// Compared untrimmed, because `configure_cors` compares untrimmed: `" *"`
    /// becomes a list entry no `Origin` matches, so it allows nothing and there
    /// is nothing to warn about. `core/server/src/http.rs` trims before the same
    /// comparison; porting that here means trimming in both places at once,
    /// since trimming only this one would warn about a closed config and
    /// trimming only the mapping would open one silently.
    pub fn allows_any_origin(&self) -> bool {
        self.allowed_origins
            .first()
            .is_some_and(|origin| origin == "*")
    }

    /// Whether the resulting policy admits an origin nobody owns, assuming one
    /// gets built: a wildcard past the first position panics `configure_cors`,
    /// so that shape warns and then dies.
    ///
    /// `*` is one. `null` is the other: a browser sends it from a sandboxed
    /// iframe, a `data:` URL and a `file://` page, so listing it hands the
    /// cross-origin read to whoever gets the operator to open a page.
    /// `AllowOrigin::list` echoes any listed value back on a match, so a
    /// `null` anywhere counts, not only first. Untrimmed for the reason above.
    ///
    /// Separate from `allows_any_origin` because `configure_cors` has to keep
    /// mapping `["null"]` to a one-entry list rather than widening it, and
    /// defined in terms of it so the two cannot disagree about `*`.
    pub fn allows_unowned_origin(&self) -> bool {
        self.allows_any_origin() || self.allowed_origins.iter().any(|origin| origin == "null")
    }
}

#[derive(Debug, Default, Deserialize, Serialize, Clone, ConfigEnv)]
pub struct HttpTlsConfig {
    pub enabled: bool,
    pub cert_file: String,
    pub key_file: String,
}

pub fn map_connector_config(
    plugin_config: &serde_json::Value,
    format: ConfigFormat,
) -> Result<(HeaderValue, String), RuntimeError> {
    match format {
        ConfigFormat::Json => Ok((JSON_HEADER, plugin_config.to_string())),
        ConfigFormat::Yaml => {
            let plugin_config = serde_yaml_ng::to_value(plugin_config).map_err(|error| {
                error!("Failed to convert configuration to YAML. {error}");
                RuntimeError::CannotConvertConfiguration
            })?;
            let plugin_config = serde_yaml_ng::to_string(&plugin_config).map_err(|error| {
                error!("Failed to serialize YAML configuration. {error}");
                RuntimeError::CannotConvertConfiguration
            })?;
            Ok((YAML_HEADER, plugin_config))
        }
        ConfigFormat::Toml => {
            let plugin_config = toml::to_string(plugin_config).map_err(|error| {
                error!("Failed to convert configuration to TOML. {error}");
                RuntimeError::CannotConvertConfiguration
            })?;
            Ok((TOML_HEADER, plugin_config))
        }
        ConfigFormat::Text => Ok((TEXT_HEADER, plugin_config.to_string())),
    }
}

pub fn configure_cors(config: &HttpCorsConfig) -> CorsLayer {
    let allowed_origins = match &config.allowed_origins {
        origins if origins.is_empty() => AllowOrigin::default(),
        _ if config.allows_any_origin() => AllowOrigin::any(),
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

impl Default for HttpMetricsConfig {
    fn default() -> Self {
        Self {
            enabled: false,
            endpoint: "/metrics".to_owned(),
        }
    }
}

impl std::fmt::Display for HttpMetricsConfig {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "{{ enabled: {}, endpoint: {} }}",
            self.enabled, self.endpoint
        )
    }
}

impl Default for HttpConfig {
    fn default() -> Self {
        Self {
            enabled: true,
            address: "localhost:8081".to_owned(),
            api_key: SecretString::from(""),
            cors: HttpCorsConfig::default(),
            tls: HttpTlsConfig::default(),
            metrics: HttpMetricsConfig::default(),
        }
    }
}

impl std::fmt::Display for HttpConfig {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "{{ address: {}, api_key: ******, cors: {}, tls: {}, metrics: {} }}",
            self.address, self.cors, self.tls, self.metrics
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

#[cfg(test)]
mod tests {
    use super::*;

    fn cors(allowed_origins: &[&str]) -> HttpCorsConfig {
        HttpCorsConfig {
            allowed_origins: allowed_origins.iter().map(|s| (*s).to_owned()).collect(),
            ..HttpCorsConfig::default()
        }
    }

    #[test]
    fn given_a_leading_wildcard_when_classified_should_allow_any_origin() {
        assert!(cors(&["*"]).allows_any_origin());
        assert!(cors(&["*"]).allows_unowned_origin());
    }

    #[test]
    fn given_pinned_or_absent_origins_when_classified_should_allow_none() {
        assert!(
            !cors(&[]).allows_any_origin(),
            "an empty list emits no header"
        );
        assert!(!cors(&[]).allows_unowned_origin());
        assert!(!cors(&["https://console.example"]).allows_any_origin());
        assert!(!cors(&["https://console.example"]).allows_unowned_origin());
        assert!(
            !cors(&["https://console.example", "*"]).allows_any_origin(),
            "only the first entry reaches `AllowOrigin::any`; a later `*` makes \
             `AllowOrigin::list` panic, so that config never serves a request"
        );
    }

    #[test]
    fn given_the_classified_origin_shapes_when_built_should_produce_a_layer() {
        // The predicates above describe what `configure_cors` does with each
        // shape, so a shape that cannot build would make the warning the last
        // line an operator sees before the process dies.
        for origins in [
            &["*"][..],
            &[][..],
            &["https://console.example"][..],
            &["null"][..],
        ] {
            let _layer = configure_cors(&cors(origins));
        }
    }

    #[test]
    #[should_panic(expected = "Wildcard origin")]
    fn given_a_trailing_wildcard_when_built_should_panic_rather_than_allow_nothing() {
        let _layer = configure_cors(&cors(&["https://console.example", "*"]));
    }

    #[test]
    fn given_a_null_origin_when_classified_should_report_an_unowned_one() {
        assert!(
            cors(&["null"]).allows_unowned_origin(),
            "a sandboxed iframe, a data: URL and a file:// page all send Origin: null"
        );
        assert!(
            cors(&["https://console.example", "null"]).allows_unowned_origin(),
            "`AllowOrigin::list` echoes any listed value, so position does not matter"
        );
        assert!(
            !cors(&["null"]).allows_any_origin(),
            "`configure_cors` still has to build a one-entry list, not widen to any()"
        );
    }
}
