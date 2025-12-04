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

use crate::api::config::HttpConfig;
use figment::providers::{Format, Toml};
use figment::value::Dict;
use figment::{Metadata, Profile, Provider};
use iggy_common::defaults::{DEFAULT_ROOT_PASSWORD, DEFAULT_ROOT_USERNAME};
use iggy_common::{CustomEnvProvider, FileConfigProvider, IggyDuration};
use serde::{Deserialize, Serialize};
use serde_with::{DisplayFromStr, serde_as};
use std::collections::HashMap;
use std::fmt::{Display, Formatter};

#[derive(Debug, Default, Clone, Deserialize, Serialize)]
#[serde(default)]
pub struct ConnectorsRuntimeConfig {
    pub http: HttpConfig,
    pub iggy: IggyConfig,
    pub connectors: ConnectorsConfig,
    pub state: StateConfig,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct IggyConfig {
    pub address: String,
    pub username: String,
    pub password: String,
    pub token: String,
    pub tls: IggyTlsConfig,
}

#[derive(Debug, Default, Clone, Serialize, Deserialize)]
pub struct IggyTlsConfig {
    pub enabled: bool,
    pub ca_file: String,
    pub domain: Option<String>,
}

#[serde_as]
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RetryConfig {
    pub enabled: bool,
    pub max_attempts: u32,
    #[serde_as(as = "DisplayFromStr")]
    pub initial_backoff: IggyDuration,
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

#[derive(Debug, Default, Clone, Deserialize, Serialize)]
#[serde(default)]
pub struct LocalConnectorsConfig {
    pub config_dir: String,
}

#[serde_as]
#[derive(Debug, Default, Clone, Deserialize, Serialize)]
pub struct HttpConnectorsConfig {
    pub base_url: String,
    #[serde_as(as = "DisplayFromStr")]
    #[serde(default = "default_from_secs")]
    pub timeout: IggyDuration,
    #[serde(default)]
    pub request_headers: HashMap<String, String>,
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

#[derive(Debug, Default, Clone, Deserialize, Serialize)]
#[serde(default)]
pub struct ResponseConfig {
    pub data_path: Option<String>,
    pub error_path: Option<String>,
}

#[allow(clippy::large_enum_variant)]
#[derive(Debug, Clone, Deserialize, Serialize)]
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

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct StateConfig {
    pub path: String,
}

impl Display for StateConfig {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "{{ path: {} }}", self.path)
    }
}

impl Display for ConnectorsRuntimeConfig {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "{{ http: {}, iggy: {}, connectors: {}, state: {:} }}",
            self.http, self.iggy, self.connectors, self.state
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
    provider: CustomEnvProvider<ConnectorsRuntimeConfig>,
}

impl Default for ConnectorsEnvProvider {
    fn default() -> Self {
        Self {
            provider: CustomEnvProvider::new("IGGY_CONNECTORS_", &[]),
        }
    }
}

impl Provider for ConnectorsEnvProvider {
    fn metadata(&self) -> Metadata {
        Metadata::named("iggy-connectors-config")
    }

    fn data(&self) -> Result<figment::value::Map<Profile, Dict>, figment::Error> {
        self.provider.deserialize().map_err(|_| {
            figment::Error::from("Cannot deserialize environment variables for connectors config")
        })
    }
}
