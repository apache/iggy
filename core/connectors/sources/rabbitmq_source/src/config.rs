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

use iggy_connector_sdk::Error;
use secrecy::{ExposeSecret, SecretString};
use serde::Deserialize;
use std::time::{Duration, Instant};
use url::Url;

#[derive(Debug, Deserialize)]
pub struct RabbitMqSourceConfig {
    pub amqp_url: SecretString,
    pub queue: String,
    #[serde(default = "default_batch_size")]
    pub batch_size: u16,
    #[serde(default = "default_batch_timeout")]
    pub batch_timeout: String,
    #[serde(default)]
    pub include_metadata: bool,
    #[serde(default = "default_timeout_secs")]
    pub timeout_secs: u64,
    #[serde(default)]
    pub verbose_logging: bool,
    #[serde(default)]
    pub tls: Option<RabbitMqTlsConfig>,
}

#[derive(Debug, Deserialize)]
pub struct RabbitMqTlsConfig {
    pub ca_file: Option<String>,
    pub client_cert_file: Option<String>,
    pub client_key_file: Option<String>,
}

impl RabbitMqSourceConfig {
    pub fn validate(&self) -> Result<(), Error> {
        let url = Url::parse(self.amqp_url.expose_secret())
            .map_err(|_| Error::InvalidConfigValue("invalid AMQP URL".into()))?;
        if !matches!(url.scheme(), "amqp" | "amqps") || url.host_str().is_none() {
            return Err(Error::InvalidConfigValue("invalid AMQP URL".into()));
        }
        if self.queue.trim().is_empty() || self.queue.len() > 255 {
            return Err(Error::InvalidConfigValue(
                "queue must contain 1 to 255 bytes".into(),
            ));
        }
        if self.batch_size == 0 {
            return Err(Error::InvalidConfigValue(
                "batch_size must be positive".into(),
            ));
        }
        let timeout = humantime::parse_duration(&self.batch_timeout)
            .map_err(|_| Error::InvalidConfigValue("invalid batch_timeout".into()))?;
        if timeout.is_zero() || self.timeout_secs == 0 {
            return Err(Error::InvalidConfigValue(
                "timeouts must be positive".into(),
            ));
        }
        if Instant::now().checked_add(timeout).is_none()
            || Instant::now()
                .checked_add(Duration::from_secs(self.timeout_secs))
                .is_none()
        {
            return Err(Error::InvalidConfigValue("timeouts are too large".into()));
        }
        if let Some(tls) = &self.tls {
            if url.scheme() != "amqps" {
                return Err(Error::InvalidConfigValue("TLS requires amqps".into()));
            }
            if tls.client_cert_file.is_some() != tls.client_key_file.is_some() {
                return Err(Error::InvalidConfigValue(
                    "client certificate and key must be supplied together".into(),
                ));
            }
        }
        Ok(())
    }
}

fn default_batch_size() -> u16 {
    100
}

fn default_batch_timeout() -> String {
    "10ms".to_owned()
}

fn default_timeout_secs() -> u64 {
    30
}
