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

//! BigQuery access: credentials, the `tables.get` REST call, and
//! `AppendRows` on the table's `_default` write stream.
//!
//! The schema is fetched over REST with a plain `reqwest` client. Writes go
//! through `gcloud-bigquery`, which owns the gRPC channel pool and token
//! refresh for the Storage Write API.

use std::fmt;
use std::sync::Arc;
use std::time::Duration;

use gcloud_bigquery::client::google_cloud_auth::credentials::CredentialsFile;
use gcloud_bigquery::client::google_cloud_auth::token::DefaultTokenSourceProvider;
use gcloud_bigquery::client::{
    ChannelConfig, Client, ClientConfig, HttpClientConfig, StreamingWriteConfig,
};
use gcloud_bigquery::storage_write::AppendRowsRequestBuilder;
use gcloud_bigquery::storage_write::stream::default::DefaultStream;
use gcloud_googleapis::cloud::bigquery::storage::v1::append_rows_response::Response;
use iggy_connector_sdk::Error;
use reqwest::header::AUTHORIZATION;
use secrecy::{ExposeSecret, SecretString};
use token_source::{TokenSource, TokenSourceProvider};
use tokio::time::timeout;
use tracing::info;

use crate::encode::Chunk;
use crate::error::{AppendError, TableError};
use crate::{BigQuerySinkConfig, Settings};

const BIGQUERY_REST_ENDPOINT: &str = "https://bigquery.googleapis.com";
const GRPC_KEEP_ALIVE_INTERVAL: Duration = Duration::from_secs(30);
const GRPC_KEEP_ALIVE_TIMEOUT: Duration = Duration::from_secs(10);

/// Where credentials come from. Resolved from the config without I/O so the
/// selection can be unit tested.
#[derive(Debug, Clone)]
pub(crate) enum CredentialSource {
    /// Local fake or emulator: no credentials, plain-text endpoints.
    Emulator {
        rest: String,
        grpc: String,
    },
    KeyFile(String),
    InlineKey(SecretString),
    ApplicationDefault,
}

/// Result of one `AppendRows` call that reached BigQuery.
#[derive(Debug, PartialEq, Eq)]
pub(crate) enum AppendOutcome {
    Appended,
    /// Nothing was appended. Each entry is a row index in the request and
    /// BigQuery's reason.
    RowErrors(Vec<(usize, String)>),
}

pub(crate) struct BigQueryClient {
    http: reqwest::Client,
    table_url: String,
    token: Option<Arc<dyn TokenSource>>,
    stream: Option<DefaultStream>,
    write_client: Client,
    table_path: String,
    write_stream_name: String,
    missing_value: i32,
    timeout: Duration,
}

impl CredentialSource {
    pub(crate) fn from_config(config: &BigQuerySinkConfig) -> Self {
        if let (Some(rest), Some(grpc)) =
            (&config.emulator_endpoint, &config.emulator_grpc_endpoint)
        {
            return CredentialSource::Emulator {
                rest: rest.trim_end_matches('/').to_owned(),
                grpc: grpc.clone(),
            };
        }
        if let Some(path) = &config.credentials_path {
            return CredentialSource::KeyFile(path.clone());
        }
        if let Some(json) = &config.credentials_json {
            return CredentialSource::InlineKey(json.clone());
        }
        CredentialSource::ApplicationDefault
    }

    pub(crate) fn label(&self) -> &'static str {
        match self {
            CredentialSource::Emulator { .. } => "none (emulator endpoint override)",
            CredentialSource::KeyFile(_) => "service account key file",
            CredentialSource::InlineKey(_) => "inline service account key",
            CredentialSource::ApplicationDefault => "application default credentials",
        }
    }
}

impl BigQueryClient {
    /// Build credentials and the gRPC channel pool. No BigQuery API call is
    /// made yet.
    pub(crate) async fn connect(
        id: u32,
        config: &BigQuerySinkConfig,
        settings: &Settings,
    ) -> Result<Self, Error> {
        install_crypto_provider();
        let source = CredentialSource::from_config(config);
        info!("BigQuery sink ID: {id}: using {}", source.label());

        let channel_config = ChannelConfig::default()
            .with_connect_timeout(settings.timeout)
            .with_timeout(settings.timeout)
            .with_http2_keep_alive_interval(GRPC_KEEP_ALIVE_INTERVAL)
            .with_keep_alive_timeout(GRPC_KEEP_ALIVE_TIMEOUT)
            .with_keep_alive_while_idle(true);
        let write_config = StreamingWriteConfig::default().with_channel_config(channel_config);

        let (client_config, rest_base, token) = match &source {
            CredentialSource::Emulator { rest, grpc } => (
                ClientConfig::new_with_emulator(grpc, rest.clone()),
                rest.clone(),
                None,
            ),
            CredentialSource::KeyFile(path) => {
                let credentials =
                    CredentialsFile::new_from_file(path.clone())
                        .await
                        .map_err(|e| {
                            Error::InitError(format!("cannot load credentials_path '{path}': {e}"))
                        })?;
                with_credentials(credentials).await?
            }
            CredentialSource::InlineKey(json) => {
                let credentials = CredentialsFile::new_from_str(json.expose_secret())
                    .await
                    .map_err(|e| Error::InitError(format!("cannot parse credentials_json: {e}")))?;
                with_credentials(credentials).await?
            }
            CredentialSource::ApplicationDefault => {
                let (client_config, _) = ClientConfig::new_with_auth().await.map_err(|e| {
                    Error::InitError(format!("application default credentials: {e}"))
                })?;
                let provider = HttpClientConfig::default_token_provider()
                    .await
                    .map_err(|e| {
                        Error::InitError(format!("application default credentials: {e}"))
                    })?;
                (
                    client_config,
                    BIGQUERY_REST_ENDPOINT.to_owned(),
                    Some(provider.token_source()),
                )
            }
        };

        let write_client = Client::new(client_config.with_streaming_write_config(write_config))
            .await
            .map_err(|e| {
                Error::InitError(format!("cannot connect to the Storage Write API: {e}"))
            })?;
        let http = reqwest::Client::builder()
            .timeout(settings.timeout)
            .build()
            .map_err(|e| Error::InitError(format!("cannot build HTTP client: {e}")))?;

        let table_path = format!(
            "projects/{}/datasets/{}/tables/{}",
            config.project_id, config.dataset, config.table
        );
        Ok(BigQueryClient {
            http,
            table_url: format!(
                "{rest_base}/bigquery/v2/projects/{}/datasets/{}/tables/{}?fields=schema",
                config.project_id, config.dataset, config.table
            ),
            token,
            stream: None,
            write_client,
            write_stream_name: format!("{table_path}/streams/_default"),
            table_path,
            missing_value: settings.missing_value.as_proto(),
            timeout: settings.timeout,
        })
    }

    /// `tables.get`, returning the raw response body.
    pub(crate) async fn fetch_table(&self) -> Result<Vec<u8>, TableError> {
        let mut request = self.http.get(&self.table_url);
        if let Some(token) = &self.token {
            let value = token
                .token()
                .await
                .map_err(|e| TableError::Token(e.to_string()))?;
            request = request.header(AUTHORIZATION, value);
        }
        let response = request
            .send()
            .await
            .map_err(|e| TableError::Transport(e.to_string()))?;
        let status = response.status();
        let body = response
            .bytes()
            .await
            .map_err(|e| TableError::Transport(e.to_string()))?;
        if !status.is_success() {
            return Err(TableError::Http {
                status: status.as_u16(),
                body: error_message(&body),
            });
        }
        Ok(body.to_vec())
    }

    /// Resolve the table's `_default` write stream. Also proves the
    /// credentials can reach the Storage Write API.
    pub(crate) async fn resolve_write_stream(&self) -> Result<DefaultStream, AppendError> {
        timeout(
            self.timeout,
            self.write_client
                .default_storage_writer()
                .create_write_stream(&self.table_path),
        )
        .await
        .map_err(|_| deadline_exceeded("write stream setup", self.timeout))?
        .map_err(AppendError::from)
    }

    pub(crate) fn set_write_stream(&mut self, stream: DefaultStream) {
        self.stream = Some(stream);
    }

    pub(crate) fn write_stream_name(&self) -> &str {
        &self.write_stream_name
    }

    /// One `AppendRows` request for one chunk.
    pub(crate) async fn append(&self, chunk: &Chunk) -> Result<AppendOutcome, AppendError> {
        timeout(self.timeout, self.append_inner(chunk))
            .await
            .map_err(|_| deadline_exceeded("AppendRows", self.timeout))?
    }

    async fn append_inner(&self, chunk: &Chunk) -> Result<AppendOutcome, AppendError> {
        let stream = self.stream.as_ref().ok_or(AppendError::MissingStream)?;
        let request = AppendRowsRequestBuilder::new_arrow(
            chunk.schema_bytes.clone(),
            chunk.batch_bytes.clone(),
        )
        .with_default_missing_value_interpretation(self.missing_value);
        let mut responses = stream.append_rows(vec![request]).await?;
        let response = responses
            .message()
            .await?
            .ok_or(AppendError::ResponseStreamClosed)?;

        if !response.row_errors.is_empty() {
            let rows = response
                .row_errors
                .into_iter()
                .map(|row| {
                    (
                        usize::try_from(row.index).unwrap_or(usize::MAX),
                        row.message,
                    )
                })
                .collect();
            return Ok(AppendOutcome::RowErrors(rows));
        }
        match response.response {
            Some(Response::Error(status)) => Err(AppendError::Rpc {
                code: status.code.into(),
                message: status.message,
            }),
            Some(Response::AppendResult(_)) | None => Ok(AppendOutcome::Appended),
        }
    }
}

fn deadline_exceeded(operation: &str, duration: Duration) -> AppendError {
    AppendError::Rpc {
        code: gcloud_gax::grpc::Code::DeadlineExceeded,
        message: format!("{operation} timed out after {duration:?}"),
    }
}

impl fmt::Debug for BigQueryClient {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("BigQueryClient")
            .field("table_path", &self.table_path)
            .field("stream_open", &self.stream.is_some())
            .finish_non_exhaustive()
    }
}

async fn with_credentials(
    credentials: CredentialsFile,
) -> Result<(ClientConfig, String, Option<Arc<dyn TokenSource>>), Error> {
    let (client_config, _) = ClientConfig::new_with_credentials(credentials.clone())
        .await
        .map_err(|e| Error::InitError(format!("service account credentials: {e}")))?;
    let provider: DefaultTokenSourceProvider =
        HttpClientConfig::default_token_provider_with(credentials)
            .await
            .map_err(|e| Error::InitError(format!("service account credentials: {e}")))?;
    Ok((
        client_config,
        BIGQUERY_REST_ENDPOINT.to_owned(),
        Some(provider.token_source()),
    ))
}

/// Google APIs answer errors with a pretty-printed JSON document. Keep the
/// log on one line by pulling out `error.message`, falling back to the body
/// with line breaks collapsed.
fn error_message(body: &[u8]) -> String {
    serde_json::from_slice::<serde_json::Value>(body)
        .ok()
        .and_then(|value| {
            value
                .pointer("/error/message")
                .and_then(serde_json::Value::as_str)
                .map(str::to_owned)
        })
        .unwrap_or_else(|| {
            String::from_utf8_lossy(body)
                .split_whitespace()
                .collect::<Vec<_>>()
                .join(" ")
        })
}

/// tonic builds its TLS config from rustls' process-wide default provider.
/// With more than one provider compiled in there is no default and the
/// first TLS connection panics, so pick one explicitly. The plugin is its
/// own shared library, so this only affects its own copy of rustls.
fn install_crypto_provider() {
    let _ = rustls::crypto::ring::default_provider().install_default();
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::test_support::config;

    #[test]
    fn given_no_credentials_should_use_application_default() {
        assert!(matches!(
            CredentialSource::from_config(&config()),
            CredentialSource::ApplicationDefault
        ));
    }

    #[test]
    fn given_key_file_should_use_it() {
        let mut config = config();
        config.credentials_path = Some("/secrets/key.json".into());
        assert!(matches!(
            CredentialSource::from_config(&config),
            CredentialSource::KeyFile(path) if path == "/secrets/key.json"
        ));
    }

    #[test]
    fn given_inline_key_should_use_it() {
        let mut config = config();
        config.credentials_json = Some(SecretString::from("{\"type\":\"service_account\"}"));
        let CredentialSource::InlineKey(secret) = CredentialSource::from_config(&config) else {
            panic!("expected inline key");
        };
        assert_eq!(secret.expose_secret(), "{\"type\":\"service_account\"}");
    }

    #[test]
    fn given_emulator_endpoint_overrides_should_skip_credentials() {
        let mut config = config();
        config.emulator_endpoint = Some("http://127.0.0.1:9050/".into());
        config.emulator_grpc_endpoint = Some("127.0.0.1:9060".into());
        let CredentialSource::Emulator { rest, grpc } = CredentialSource::from_config(&config)
        else {
            panic!("expected emulator endpoint override");
        };
        assert_eq!(rest, "http://127.0.0.1:9050");
        assert_eq!(grpc, "127.0.0.1:9060");
    }

    #[test]
    fn given_google_error_body_should_extract_single_line_message() {
        let body = br#"{
  "error": {
    "code": 404,
    "message": "Not found: Table proj:iggy.missing",
    "status": "NOT_FOUND"
  }
}"#;
        assert_eq!(error_message(body), "Not found: Table proj:iggy.missing");
        assert_eq!(error_message(b"bad\ngateway"), "bad gateway");
    }

    #[tokio::test]
    async fn given_missing_key_file_should_fail_to_connect() {
        let mut config = config();
        config.credentials_path = Some("/definitely/not/here.json".into());
        let settings = Settings::from_config(1, &config);
        let error = BigQueryClient::connect(1, &config, &settings)
            .await
            .unwrap_err();
        assert!(
            matches!(&error, Error::InitError(reason) if reason.contains("credentials_path")),
            "{error:?}"
        );
    }

    #[tokio::test]
    async fn given_invalid_inline_key_should_fail_to_connect() {
        let mut config = config();
        config.credentials_json = Some(SecretString::from("not json"));
        let settings = Settings::from_config(1, &config);
        let error = BigQueryClient::connect(1, &config, &settings)
            .await
            .unwrap_err();
        let Error::InitError(reason) = &error else {
            panic!("expected InitError, got {error:?}");
        };
        assert!(reason.contains("credentials_json"));
        assert!(!reason.contains("not json"), "secret leaked: {reason}");
    }
}
