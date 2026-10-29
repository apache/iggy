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

use anyhow::{Context, Result, bail};
use passterm::{Stream, isatty, prompt_password_tty};
use reqwest::{Method, StatusCode};
use secrecy::{ExposeSecret, SecretString};
use serde_json::Value;
use std::path::PathBuf;

static ENV_CONNECTORS_API_KEY: &str = "IGGY_CONNECTORS_API_KEY";
static DEFAULT_API_URL: &str = "http://localhost:8081";

/// Sink or source: separate routes, and both sides may use the same key.
#[derive(Debug, Clone, Copy)]
pub enum ConnectorKind {
    Sinks,
    Sources,
}

impl ConnectorKind {
    pub fn path(self) -> &'static str {
        match self {
            ConnectorKind::Sinks => "sinks",
            ConnectorKind::Sources => "sources",
        }
    }

    pub fn singular(self) -> &'static str {
        match self {
            ConnectorKind::Sinks => "sink",
            ConnectorKind::Sources => "source",
        }
    }
}

/// Where a connector command reaches the runtime: flag, context or default.
pub struct ConnectorEndpoint {
    pub api_url: Option<String>,
    pub api_key: Option<String>,
    pub tls_ca_file: Option<PathBuf>,
}

impl ConnectorEndpoint {
    pub fn connect(&self) -> Result<ConnectorsApiClient> {
        ConnectorsApiClient::new(
            self.api_url.clone(),
            self.api_key.clone(),
            self.tls_ca_file.clone(),
        )
    }
}

/// HTTP client for the connectors runtime control API.
///
/// Responses are read as JSON values: the response types are private to the
/// runtime binary, and pulling in the SDK `api` feature would add Avro,
/// Protobuf and FlatBuffers to the CLI dependency tree.
pub struct ConnectorsApiClient {
    inner: reqwest::Client,
    base_url: String,
    api_key: Option<SecretString>,
}

impl ConnectorsApiClient {
    pub fn new(
        api_url: Option<String>,
        api_key: Option<String>,
        tls_ca_file: Option<PathBuf>,
    ) -> Result<Self> {
        let mut builder = reqwest::Client::builder();
        if let Some(ca_file) = tls_ca_file {
            let pem = std::fs::read(&ca_file)
                .with_context(|| format!("problem reading CA file {}", ca_file.display()))?;
            let certificate = reqwest::Certificate::from_pem(&pem)
                .with_context(|| format!("problem parsing CA file {} as PEM", ca_file.display()))?;
            builder = builder.add_root_certificate(certificate);
        }
        let inner = builder
            .build()
            .with_context(|| "problem building connectors API client")?;
        let base_url = api_url
            .unwrap_or_else(|| DEFAULT_API_URL.to_string())
            .trim_end_matches('/')
            .to_string();
        let api_key = api_key
            .or_else(|| std::env::var(ENV_CONNECTORS_API_KEY).ok())
            .map(SecretString::from);
        Ok(Self {
            inner,
            base_url,
            api_key,
        })
    }

    pub async fn get_value(&mut self, path: &str) -> Result<Value> {
        let response = self.execute(Method::GET, path, &[], None).await?;
        let response = Self::ensure_success(response, path).await?;
        response
            .json()
            .await
            .with_context(|| format!("problem decoding connectors API response {path} as JSON"))
    }

    pub async fn get_text(&mut self, path: &str, query: &[(&str, String)]) -> Result<String> {
        let response = self.execute(Method::GET, path, query, None).await?;
        let response = Self::ensure_success(response, path).await?;
        response
            .text()
            .await
            .with_context(|| format!("problem reading connectors API response {path} as text"))
    }

    pub async fn post_json(&mut self, path: &str, body: &Value) -> Result<Value> {
        let response = self.execute(Method::POST, path, &[], Some(body)).await?;
        let response = Self::ensure_success(response, path).await?;
        response
            .json()
            .await
            .with_context(|| format!("problem decoding connectors API response {path} as JSON"))
    }

    pub async fn post_empty(&mut self, path: &str) -> Result<()> {
        let response = self.execute(Method::POST, path, &[], None).await?;
        Self::ensure_success(response, path).await?;
        Ok(())
    }

    pub async fn put_json(&mut self, path: &str, body: &Value) -> Result<()> {
        let response = self.execute(Method::PUT, path, &[], Some(body)).await?;
        Self::ensure_success(response, path).await?;
        Ok(())
    }

    pub async fn delete(&mut self, path: &str, query: &[(&str, String)]) -> Result<()> {
        let response = self.execute(Method::DELETE, path, query, None).await?;
        Self::ensure_success(response, path).await?;
        Ok(())
    }

    /// Sends one request, retrying once after prompting for the key.
    ///
    /// The prompt only happens when no key is configured at all and stdin is
    /// a terminal: a runtime without a key never triggers it, and a wrong
    /// configured key fails with the server's reason instead of asking again.
    async fn execute(
        &mut self,
        method: Method,
        path: &str,
        query: &[(&str, String)],
        body: Option<&Value>,
    ) -> Result<reqwest::Response> {
        for attempt in 0..2 {
            let mut request = self
                .inner
                .request(method.clone(), format!("{}{path}", self.base_url));
            if !query.is_empty() {
                request = request.query(query);
            }
            if let Some(body) = body {
                request = request.json(body);
            }
            if let Some(key) = &self.api_key {
                request = request.header("api-key", key.expose_secret());
            }
            let response = request
                .send()
                .await
                .with_context(|| format!("problem calling connectors API {method} {path}"))?;
            if response.status() == StatusCode::UNAUTHORIZED
                && self.api_key.is_none()
                && attempt == 0
                && isatty(Stream::Stdin)
            {
                let key = prompt_password_tty(Some("Connectors API key: "))?;
                self.api_key = Some(SecretString::from(key));
                continue;
            }
            return Ok(response);
        }
        bail!("connectors API request {method} {path} was not sent")
    }

    async fn ensure_success(response: reqwest::Response, path: &str) -> Result<reqwest::Response> {
        if response.status().is_success() {
            return Ok(response);
        }
        let status = response.status();
        let body = response.text().await.unwrap_or_default();
        if let Ok(error) = serde_json::from_str::<Value>(&body)
            && let (Some(code), Some(reason)) = (
                error.get("code").and_then(Value::as_str),
                error.get("reason").and_then(Value::as_str),
            )
        {
            bail!("connectors API {path} failed with {code}: {reason}");
        }
        bail!("connectors API {path} failed with status {status}: {body}");
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn should_map_kinds_to_routes() {
        assert_eq!(ConnectorKind::Sinks.path(), "sinks");
        assert_eq!(ConnectorKind::Sources.path(), "sources");
        assert_eq!(ConnectorKind::Sinks.singular(), "sink");
        assert_eq!(ConnectorKind::Sources.singular(), "source");
    }

    #[test]
    fn should_default_to_local_runtime_url() {
        let client = ConnectorsApiClient::new(None, None, None).unwrap();
        assert_eq!(client.base_url, "http://localhost:8081");
        assert!(client.api_key.is_none());
    }

    #[test]
    fn should_strip_trailing_slash_from_url() {
        let client =
            ConnectorsApiClient::new(Some("http://host:8081/".to_string()), None, None).unwrap();
        assert_eq!(client.base_url, "http://host:8081");
    }
}
