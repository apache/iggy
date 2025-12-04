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

mod response_extractor;
mod url_builder;

use crate::configs::connectors::http_provider::response_extractor::ResponseExtractor;
use crate::configs::connectors::http_provider::url_builder::{TemplateKeys, UrlBuilder};
use crate::configs::connectors::{
    ConnectorConfigVersions, ConnectorsConfig, ConnectorsConfigProvider, CreateSinkConfig,
    CreateSourceConfig, SinkConfig, SourceConfig,
};
use crate::configs::runtime::{ResponseConfig, RetryConfig};
use crate::error::RuntimeError;
use async_trait::async_trait;
use reqwest;
use reqwest_middleware::{ClientBuilder, ClientWithMiddleware};
use reqwest_retry::{Jitter, RetryTransientMiddleware, policies::ExponentialBackoff};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::str::FromStr;
use std::time::Duration;

#[derive(Debug, Serialize, Deserialize)]
struct SetActiveVersionRequest {
    version: u64,
}

pub struct HttpConnectorsConfigProvider {
    url_builder: UrlBuilder,
    response_extractor: ResponseExtractor,
    client: ClientWithMiddleware,
}

impl HttpConnectorsConfigProvider {
    pub fn new(
        base_url: &str,
        timeout: Duration,
        request_headers: &HashMap<String, String>,
        url_templates: &HashMap<String, String>,
        response_config: &ResponseConfig,
        retry_config: &RetryConfig,
    ) -> Result<Self, RuntimeError> {
        let mut headers = reqwest::header::HeaderMap::new();
        for (key, value) in request_headers {
            let header_name = reqwest::header::HeaderName::from_str(key).map_err(|err| {
                RuntimeError::InvalidConfiguration(format!("Invalid header name '{key}': {err}"))
            })?;
            let header_value = reqwest::header::HeaderValue::from_str(value).map_err(|err| {
                RuntimeError::InvalidConfiguration(format!(
                    "Invalid header value for '{key}': {err}"
                ))
            })?;
            headers.insert(header_name, header_value);
        }

        let client = reqwest::Client::builder()
            .default_headers(headers)
            .timeout(timeout)
            .build()
            .map_err(|err| {
                RuntimeError::InvalidConfiguration(format!("Failed to build HTTP client: {err}"))
            })?;

        let mut client_with_middleware = ClientBuilder::new(client);

        if retry_config.enabled {
            tracing::trace!("Apply retry config: {:?}", retry_config);

            let retry_policy = ExponentialBackoff::builder()
                .retry_bounds(
                    retry_config.initial_backoff.get_duration(),
                    retry_config.max_backoff.get_duration(),
                )
                .base(retry_config.backoff_multiplier)
                .jitter(Jitter::Bounded)
                .build_with_max_retries(retry_config.max_attempts);

            let retry_transient_middleware =
                RetryTransientMiddleware::new_with_policy(retry_policy);

            client_with_middleware = client_with_middleware.with(retry_transient_middleware);
        }

        let final_client = client_with_middleware.build();

        let url_builder = UrlBuilder::new(base_url, url_templates);
        let response_extractor = ResponseExtractor::new(response_config);

        Ok(Self {
            url_builder,
            response_extractor,
            client: final_client,
        })
    }

    async fn check_response_status(
        response: reqwest::Response,
    ) -> Result<reqwest::Response, RuntimeError> {
        if !response.status().is_success() {
            let status = response.status();
            let error_text = response
                .text()
                .await
                .unwrap_or_else(|_| "Unknown error".to_string());
            return Err(RuntimeError::HttpRequestFailed(format!(
                "HTTP {status} - {error_text}"
            )));
        }
        Ok(response)
    }
}

#[async_trait]
impl ConnectorsConfigProvider for HttpConnectorsConfigProvider {
    async fn create_sink_config(
        &self,
        key: &str,
        config: CreateSinkConfig,
    ) -> Result<SinkConfig, RuntimeError> {
        let mut vars = HashMap::new();
        vars.insert("key", key);
        let url = self.url_builder.build(TemplateKeys::CREATE_SINK, &vars);

        let response = self
            .client
            .post(&url)
            .json(&config)
            .send()
            .await
            .map_err(|err| RuntimeError::HttpRequestFailed(err.to_string()))?;

        let response = Self::check_response_status(response).await?;

        let response_text = response.text().await.map_err(|err| {
            RuntimeError::HttpRequestFailed(format!("Failed to read response: {err}"))
        })?;

        self.response_extractor.extract(&response_text)
    }

    async fn create_source_config(
        &self,
        key: &str,
        config: CreateSourceConfig,
    ) -> Result<SourceConfig, RuntimeError> {
        let mut vars = HashMap::new();
        vars.insert("key", key);
        let url = self.url_builder.build(TemplateKeys::CREATE_SOURCE, &vars);

        let response = self
            .client
            .post(&url)
            .json(&config)
            .send()
            .await
            .map_err(|err| RuntimeError::HttpRequestFailed(err.to_string()))?;

        let response = Self::check_response_status(response).await?;

        let response_text = response.text().await.map_err(|err| {
            RuntimeError::HttpRequestFailed(format!("Failed to read response: {err}"))
        })?;

        self.response_extractor.extract(&response_text)
    }

    async fn get_active_configs(&self) -> Result<ConnectorsConfig, RuntimeError> {
        let vars = HashMap::new();
        let url = self
            .url_builder
            .build(TemplateKeys::GET_ACTIVE_CONFIGS, &vars);

        let response = self
            .client
            .get(&url)
            .send()
            .await
            .map_err(|err| RuntimeError::HttpRequestFailed(err.to_string()))?;

        let response = Self::check_response_status(response).await?;

        let response_text = response.text().await.map_err(|err| {
            RuntimeError::HttpRequestFailed(format!("Failed to read response: {err}"))
        })?;

        self.response_extractor.extract(&response_text)
    }

    async fn get_active_configs_versions(&self) -> Result<ConnectorConfigVersions, RuntimeError> {
        let vars = HashMap::new();
        let url = self
            .url_builder
            .build(TemplateKeys::GET_ACTIVE_VERSIONS, &vars);

        let response = self
            .client
            .get(&url)
            .send()
            .await
            .map_err(|err| RuntimeError::HttpRequestFailed(err.to_string()))?;

        let response = Self::check_response_status(response).await?;

        let response_text = response.text().await.map_err(|err| {
            RuntimeError::HttpRequestFailed(format!("Failed to read response: {err}"))
        })?;

        self.response_extractor.extract(&response_text)
    }

    async fn set_active_sink_version(&self, key: &str, version: u64) -> Result<(), RuntimeError> {
        let mut vars = HashMap::new();
        vars.insert("key", key);
        let url = self.url_builder.build(TemplateKeys::SET_ACTIVE_SINK, &vars);

        let request_body = SetActiveVersionRequest { version };
        let response = self
            .client
            .put(&url)
            .json(&request_body)
            .send()
            .await
            .map_err(|err| RuntimeError::HttpRequestFailed(err.to_string()))?;

        Self::check_response_status(response).await?;

        Ok(())
    }

    async fn set_active_source_version(&self, key: &str, version: u64) -> Result<(), RuntimeError> {
        let mut vars = HashMap::new();
        vars.insert("key", key);
        let url = self
            .url_builder
            .build(TemplateKeys::SET_ACTIVE_SOURCE, &vars);

        let request_body = SetActiveVersionRequest { version };
        let response = self
            .client
            .put(&url)
            .json(&request_body)
            .send()
            .await
            .map_err(|err| RuntimeError::HttpRequestFailed(err.to_string()))?;

        Self::check_response_status(response).await?;

        Ok(())
    }

    async fn get_sink_configs(&self, key: &str) -> Result<Vec<SinkConfig>, RuntimeError> {
        let mut vars = HashMap::new();
        vars.insert("key", key);
        let url = self
            .url_builder
            .build(TemplateKeys::GET_SINK_CONFIGS, &vars);

        let response = self
            .client
            .get(&url)
            .send()
            .await
            .map_err(|err| RuntimeError::HttpRequestFailed(err.to_string()))?;

        let response = Self::check_response_status(response).await?;

        let response_text = response.text().await.map_err(|err| {
            RuntimeError::HttpRequestFailed(format!("Failed to read response: {err}"))
        })?;

        self.response_extractor.extract(&response_text)
    }

    async fn get_sink_config(
        &self,
        key: &str,
        version: Option<u64>,
    ) -> Result<Option<SinkConfig>, RuntimeError> {
        let mut vars = HashMap::new();
        vars.insert("key", key);

        let url = match version {
            Some(v) => {
                let version_str = v.to_string();
                vars.insert("version", &version_str);
                self.url_builder.build(TemplateKeys::GET_SINK_CONFIG, &vars)
            }
            None => self
                .url_builder
                .build(TemplateKeys::GET_ACTIVE_SINK_CONFIG, &vars),
        };

        let response = self
            .client
            .get(&url)
            .send()
            .await
            .map_err(|err| RuntimeError::HttpRequestFailed(err.to_string()))?;

        if response.status() == reqwest::StatusCode::NOT_FOUND {
            return Ok(None);
        }

        let response = Self::check_response_status(response).await?;

        let response_text = response.text().await.map_err(|err| {
            RuntimeError::HttpRequestFailed(format!("Failed to read response: {err}"))
        })?;

        let config: SinkConfig = self.response_extractor.extract(&response_text)?;
        Ok(Some(config))
    }

    async fn get_source_configs(&self, key: &str) -> Result<Vec<SourceConfig>, RuntimeError> {
        let mut vars = HashMap::new();
        vars.insert("key", key);
        let url = self
            .url_builder
            .build(TemplateKeys::GET_SOURCE_CONFIGS, &vars);

        let response = self
            .client
            .get(&url)
            .send()
            .await
            .map_err(|err| RuntimeError::HttpRequestFailed(err.to_string()))?;

        let response = Self::check_response_status(response).await?;

        let response_text = response.text().await.map_err(|err| {
            RuntimeError::HttpRequestFailed(format!("Failed to read response: {err}"))
        })?;

        self.response_extractor.extract(&response_text)
    }

    async fn get_source_config(
        &self,
        key: &str,
        version: Option<u64>,
    ) -> Result<Option<SourceConfig>, RuntimeError> {
        let mut vars = HashMap::new();
        vars.insert("key", key);

        let url = match version {
            Some(v) => {
                let version_str = v.to_string();
                vars.insert("version", &version_str);
                self.url_builder
                    .build(TemplateKeys::GET_SOURCE_CONFIG, &vars)
            }
            None => self
                .url_builder
                .build(TemplateKeys::GET_ACTIVE_SOURCE_CONFIG, &vars),
        };

        let response = self
            .client
            .get(&url)
            .send()
            .await
            .map_err(|err| RuntimeError::HttpRequestFailed(err.to_string()))?;

        if response.status() == reqwest::StatusCode::NOT_FOUND {
            return Ok(None);
        }

        let response = Self::check_response_status(response).await?;

        let response_text = response.text().await.map_err(|err| {
            RuntimeError::HttpRequestFailed(format!("Failed to read response: {err}"))
        })?;

        let config: SourceConfig = self.response_extractor.extract(&response_text)?;
        Ok(Some(config))
    }

    async fn delete_sink_config(
        &self,
        key: &str,
        version: Option<u64>,
    ) -> Result<(), RuntimeError> {
        let mut vars = HashMap::new();
        vars.insert("key", key);

        let mut query_params = HashMap::new();
        let version = version.map(|v| v.to_string()).unwrap_or_default();
        if !version.is_empty() {
            query_params.insert("version", version.as_str());
        }

        let url = self.url_builder.build_with_query(
            TemplateKeys::DELETE_SINK_CONFIG,
            &vars,
            &query_params,
        );

        let response = self
            .client
            .delete(&url)
            .send()
            .await
            .map_err(|err| RuntimeError::HttpRequestFailed(err.to_string()))?;

        Self::check_response_status(response).await?;

        Ok(())
    }

    async fn delete_source_config(
        &self,
        key: &str,
        version: Option<u64>,
    ) -> Result<(), RuntimeError> {
        let mut vars = HashMap::new();
        vars.insert("key", key);

        let mut query_params = HashMap::new();
        let version = version.map(|v| v.to_string()).unwrap_or_default();
        if !version.is_empty() {
            query_params.insert("version", version.as_str());
        }

        let url = self.url_builder.build_with_query(
            TemplateKeys::DELETE_SOURCE_CONFIG,
            &vars,
            &query_params,
        );

        let response = self
            .client
            .delete(&url)
            .send()
            .await
            .map_err(|err| RuntimeError::HttpRequestFailed(err.to_string()))?;

        Self::check_response_status(response).await?;

        Ok(())
    }
}
