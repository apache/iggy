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

use crate::api::config::HttpConfig;
use ::configs::{FileConfigProvider, TypedEnvProvider};
use configs_derive::ConfigEnv;
use derive_more::Display;
use figment::providers::{Format, Toml};
use figment::value::Dict;
use figment::{Metadata, Profile, Provider};
use iggy_common::IggyDuration;
use iggy_common::defaults::{DEFAULT_ROOT_PASSWORD, DEFAULT_ROOT_USERNAME};
use secrecy::{ExposeSecret, SecretString};
use serde::{Deserialize, Serialize};
use serde_with::{DisplayFromStr, serde_as};
use std::collections::HashMap;
use std::fmt::{Display, Formatter};
use std::str::FromStr;
use std::time::Duration;

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
            service_name: "iggy-connectors".to_owned(),
            logs: TelemetryLogsConfig::default(),
            traces: TelemetryTracesConfig::default(),
        }
    }
}

impl Display for TelemetryConfig {
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

impl Display for TelemetryLogsConfig {
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

impl Display for TelemetryTracesConfig {
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
    #[display("grpc")]
    Grpc,
    #[display("http")]
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

#[derive(Debug, Default, Clone, Deserialize, Serialize, ConfigEnv)]
#[config_env(prefix = "IGGY_CONNECTORS_", name = "iggy-connectors-config")]
#[serde(default)]
pub struct ConnectorsRuntimeConfig {
    pub http: HttpConfig,
    pub iggy: IggyConfig,
    pub connectors: ConnectorsConfig,
    pub state: StateConfig,
    pub telemetry: TelemetryConfig,
    pub logging: LoggingConfig,
}

#[derive(Debug, Default, Clone, Deserialize, Serialize, ConfigEnv)]
pub struct LoggingConfig {
    #[serde(default)]
    #[config_env(leaf)]
    pub format: LogFormat,
}

#[derive(
    Debug,
    Default,
    Clone,
    Copy,
    PartialEq,
    Eq,
    Deserialize,
    Serialize,
    strum::Display,
    strum::EnumString,
)]
#[serde(rename_all = "lowercase")]
#[strum(serialize_all = "lowercase", ascii_case_insensitive)]
pub enum LogFormat {
    #[default]
    Text,
    Json,
}

#[cfg(test)]
mod log_format_tests {
    use super::*;

    #[test]
    fn given_no_explicit_value_when_defaulted_should_be_text() {
        assert_eq!(LogFormat::default(), LogFormat::Text);
    }

    #[test]
    fn given_text_string_when_parsed_should_return_text_variant() {
        assert_eq!(LogFormat::from_str("text").unwrap(), LogFormat::Text);
        assert_eq!(LogFormat::from_str("TEXT").unwrap(), LogFormat::Text);
    }

    #[test]
    fn given_json_string_when_parsed_should_return_json_variant() {
        assert_eq!(LogFormat::from_str("json").unwrap(), LogFormat::Json);
        assert_eq!(LogFormat::from_str("Json").unwrap(), LogFormat::Json);
    }

    #[test]
    fn given_invalid_string_when_parsed_should_return_err() {
        assert!(LogFormat::from_str("yaml").is_err());
        assert!(LogFormat::from_str("").is_err());
    }

    #[test]
    fn given_log_format_when_displayed_should_match_lowercase_variant_name() {
        assert_eq!(LogFormat::Text.to_string(), "text");
        assert_eq!(LogFormat::Json.to_string(), "json");
    }

    #[test]
    fn given_toml_with_logging_section_when_deserialized_should_use_format() {
        let toml = r#"format = "json""#;
        let parsed: LoggingConfig = toml::from_str(toml).expect("parse logging");
        assert_eq!(parsed.format, LogFormat::Json);
    }

    #[test]
    fn given_toml_without_format_field_when_deserialized_should_default_to_text() {
        let parsed: LoggingConfig = toml::from_str("").expect("parse empty logging");
        assert_eq!(parsed.format, LogFormat::Text);
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, ConfigEnv)]
pub struct IggyConfig {
    pub address: String,
    pub username: String,
    #[config_env(secret)]
    pub password: String,
    #[config_env(secret)]
    pub token: String,
    pub tls: IggyTlsConfig,
}

#[derive(Debug, Default, Clone, Serialize, Deserialize, ConfigEnv)]
pub struct IggyTlsConfig {
    pub enabled: bool,
    pub ca_file: String,
    pub domain: Option<String>,
}

#[serde_as]
#[derive(Debug, Clone, Serialize, Deserialize, ConfigEnv)]
pub struct RetryConfig {
    pub enabled: bool,
    pub max_attempts: u32,
    #[config_env(leaf)]
    #[serde_as(as = "DisplayFromStr")]
    pub initial_backoff: IggyDuration,
    #[config_env(leaf)]
    #[serde_as(as = "DisplayFromStr")]
    pub max_backoff: IggyDuration,
    pub backoff_multiplier: u32,
}

impl Default for RetryConfig {
    fn default() -> Self {
        Self {
            enabled: true,
            max_attempts: 3,
            initial_backoff: IggyDuration::new_from_secs(1),
            max_backoff: IggyDuration::new_from_secs(30),
            backoff_multiplier: 2,
        }
    }
}

impl Display for RetryConfig {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "{{ enabled: {}, max_attempts: {}, initial_backoff: {}, max_backoff: {}, backoff_multiplier: {} }}",
            self.enabled,
            self.max_attempts,
            self.initial_backoff,
            self.max_backoff,
            self.backoff_multiplier
        )
    }
}

#[derive(Debug, Default, Clone, Deserialize, Serialize, ConfigEnv)]
#[serde(default)]
pub struct LocalConnectorsConfig {
    pub config_dir: String,
}

#[serde_as]
#[derive(Debug, Default, Clone, Deserialize, Serialize, ConfigEnv)]
pub struct HttpConnectorsConfig {
    pub base_url: String,
    #[config_env(leaf)]
    #[serde_as(as = "DisplayFromStr")]
    #[serde(default = "default_from_secs")]
    pub timeout: IggyDuration,
    #[config_env(skip)]
    #[serde(default)]
    pub request_headers: HashMap<String, String>,
    #[config_env(skip)]
    #[serde(default)]
    pub url_templates: HashMap<String, String>,
    #[serde(default)]
    pub response: ResponseConfig,
    #[serde(default)]
    pub retry: RetryConfig,
}

impl Display for HttpConnectorsConfig {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "{{ type: \"http\", base_url: {:?}, request_headers: {:?}, timeout: {}, url_templates: {:?}, response: {:?}, retry: {} }}",
            self.base_url,
            self.request_headers.keys(),
            self.timeout,
            self.url_templates,
            self.response,
            self.retry
        )
    }
}

fn default_from_secs() -> IggyDuration {
    IggyDuration::new_from_secs(10)
}

#[derive(Debug, Default, Clone, Deserialize, Serialize, ConfigEnv)]
#[serde(default)]
pub struct ResponseConfig {
    pub data_path: Option<String>,
    pub error_path: Option<String>,
}

#[allow(clippy::large_enum_variant)]
#[derive(Debug, Clone, Deserialize, Serialize, ConfigEnv)]
#[config_env(tag = "config_type")]
#[serde(tag = "config_type", rename_all = "lowercase")]
pub enum ConnectorsConfig {
    Local(LocalConnectorsConfig),
    Http(HttpConnectorsConfig),
}

impl Default for ConnectorsConfig {
    fn default() -> Self {
        Self::Local(LocalConnectorsConfig::default())
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, ConfigEnv)]
pub struct StateConfig {
    pub path: String,
    #[serde(default)]
    #[config_env(leaf)]
    pub storage: StateStorageKind,
    #[serde(default)]
    pub http: HttpStateConfig,
}

impl Display for StateConfig {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "{{ path: {}, storage: {}, http: {} }}",
            self.path, self.storage, self.http
        )
    }
}

#[derive(
    Debug,
    Default,
    Clone,
    Copy,
    PartialEq,
    Eq,
    Deserialize,
    Serialize,
    strum::Display,
    strum::EnumString,
)]
#[serde(rename_all = "lowercase")]
#[strum(serialize_all = "lowercase", ascii_case_insensitive)]
pub enum StateStorageKind {
    #[default]
    File,
    Http,
}

#[serde_as]
#[derive(Debug, Clone, Serialize, Deserialize, ConfigEnv)]
#[serde(default)]
pub struct HttpStateConfig {
    pub url: String,
    #[config_env(leaf)]
    #[serde_as(as = "DisplayFromStr")]
    pub timeout: IggyDuration,
    #[config_env(skip)]
    #[serde(serialize_with = "serialize_secret_map")]
    pub request_headers: HashMap<String, SecretString>,
    pub retry: RetryConfig,
}

impl Default for HttpStateConfig {
    fn default() -> Self {
        Self {
            url: String::new(),
            timeout: IggyDuration::new_from_secs(5),
            request_headers: HashMap::new(),
            retry: default_state_retry(),
        }
    }
}

impl Display for HttpStateConfig {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "{{ url: {:?}, timeout: {}, request_headers: {:?}, retry: {} }}",
            self.url,
            self.timeout,
            self.request_headers.keys(),
            self.retry
        )
    }
}

fn default_state_retry() -> RetryConfig {
    RetryConfig {
        enabled: true,
        max_attempts: 4,
        initial_backoff: IggyDuration::new(Duration::from_millis(200)),
        max_backoff: IggyDuration::new_from_secs(2),
        backoff_multiplier: 2,
    }
}

/// Exposes the header values on serialization, mirroring the
/// `serialize_secret` treatment of `http.api_key`: the plaintext is the point
/// when the config is rendered back out, while `Debug`/`Display` stay
/// redacted through `SecretString`.
fn serialize_secret_map<S: serde::Serializer>(
    headers: &HashMap<String, SecretString>,
    serializer: S,
) -> Result<S::Ok, S::Error> {
    serializer.collect_map(
        headers
            .iter()
            .map(|(name, value)| (name, value.expose_secret())),
    )
}

#[cfg(test)]
mod state_config_tests {
    use super::*;

    #[test]
    fn given_legacy_path_only_state_section_when_parsed_should_default_to_file_storage() {
        let parsed: StateConfig = toml::from_str(r#"path = "local_state""#).expect("parse state");
        assert_eq!(parsed.path, "local_state");
        assert_eq!(parsed.storage, StateStorageKind::File);
        assert!(parsed.http.url.is_empty());
        assert_eq!(parsed.http.timeout.get_duration(), Duration::from_secs(5));
    }

    #[test]
    fn given_http_state_section_when_parsed_should_populate_backend_config() {
        let toml = r#"
            path = "local_state"
            storage = "http"

            [http]
            url = "http://127.0.0.1:8080/connectors/state"
            timeout = "10s"

            [http.request_headers]
            authorization = "Bearer token"

            [http.retry]
            enabled = true
            max_attempts = 7
            initial_backoff = "100ms"
            max_backoff = "1s"
            backoff_multiplier = 3
        "#;
        let parsed: StateConfig = toml::from_str(toml).expect("parse state");
        assert_eq!(parsed.storage, StateStorageKind::Http);
        assert_eq!(parsed.http.url, "http://127.0.0.1:8080/connectors/state");
        assert_eq!(parsed.http.timeout.get_duration(), Duration::from_secs(10));
        assert!(parsed.http.request_headers.contains_key("authorization"));
        assert_eq!(parsed.http.retry.max_attempts, 7);
        assert_eq!(parsed.http.retry.backoff_multiplier, 3);
    }

    #[test]
    fn given_unknown_storage_kind_when_parsed_should_fail() {
        let result = toml::from_str::<StateConfig>(
            r#"
            path = "local_state"
            storage = "s3"
        "#,
        );
        assert!(result.is_err(), "unknown storage kinds must fail boot");
    }

    #[test]
    fn given_state_config_when_displayed_should_not_render_header_values() {
        let mut config = StateConfig::default();
        config
            .http
            .request_headers
            .insert("authorization".to_string(), "Bearer top-secret".into());
        let display_output = config.to_string();
        let debug_output = format!("{config:?}");
        assert!(!display_output.contains("top-secret"), "{display_output}");
        assert!(!debug_output.contains("top-secret"), "{debug_output}");
    }
}

impl Display for ConnectorsRuntimeConfig {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "{{ http: {}, iggy: {}, connectors: {}, state: {}, telemetry: {}, logging: {{ format: {} }} }}",
            self.http, self.iggy, self.connectors, self.state, self.telemetry, self.logging.format
        )
    }
}

impl Display for IggyConfig {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "{{ address: {}, username: {}, password: {}, token: {}, tls: {} }}",
            self.address,
            self.username,
            if !self.password.is_empty() {
                "****"
            } else {
                ""
            },
            if !self.token.is_empty() { "****" } else { "" },
            self.tls
        )
    }
}

impl Display for IggyTlsConfig {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "{{ enabled: {}, ca_file: {:?}, domain: {:?} }}",
            self.enabled, self.ca_file, self.domain
        )
    }
}

impl Display for ConnectorsConfig {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            ConnectorsConfig::Local(config) => write!(
                f,
                "{{ type: \"file\", config_dir: {:?} }}",
                config.config_dir
            ),
            ConnectorsConfig::Http(config) => write!(f, "{config}",),
        }
    }
}

impl Default for StateConfig {
    fn default() -> Self {
        Self {
            path: "local_state".to_owned(),
            storage: StateStorageKind::default(),
            http: HttpStateConfig::default(),
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
            tls: IggyTlsConfig::default(),
        }
    }
}

impl ConnectorsRuntimeConfig {
    pub fn config_provider(path: String) -> FileConfigProvider<ConnectorsEnvProvider> {
        let default_config =
            Toml::string(include_str!("../../../../connectors/runtime/config.toml"));
        FileConfigProvider::new(
            path,
            ConnectorsEnvProvider::default(),
            true,
            Some(default_config),
        )
    }
}

#[derive(Debug, Clone)]
pub struct ConnectorsEnvProvider {
    provider: TypedEnvProvider<ConnectorsRuntimeConfig>,
}

impl Default for ConnectorsEnvProvider {
    fn default() -> Self {
        Self {
            provider: TypedEnvProvider::from_config(ConnectorsRuntimeConfig::ENV_PREFIX),
        }
    }
}

impl Provider for ConnectorsEnvProvider {
    fn metadata(&self) -> Metadata {
        Metadata::named(ConnectorsRuntimeConfig::ENV_PROVIDER_NAME)
    }

    fn data(&self) -> Result<figment::value::Map<Profile, Dict>, figment::Error> {
        self.provider.data()
    }
}
