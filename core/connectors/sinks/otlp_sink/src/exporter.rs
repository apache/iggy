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

//! OTLP/HTTP transport. A request is encoded once and the same bytes are reused on every retry.

use crate::signal::ExportResponse;
use bytes::Bytes;
use flate2::Compression;
use flate2::write::GzEncoder;
use iggy_connector_sdk::Error;
use reqwest::header::{CONTENT_ENCODING, CONTENT_TYPE, HeaderMap, HeaderName, HeaderValue};
use reqwest::{StatusCode, Url};
use std::error::Error as StdError;
use std::fmt;
use std::io::Write as _;
use std::time::Duration;

const PROTOBUF_CONTENT_TYPE: &str = "application/x-protobuf";
const MAX_ERROR_BODY_BYTES: usize = 512;

/// Why one export attempt failed, and whether trying again could help.
#[derive(Debug)]
pub(crate) enum ExportError {
    Transient(String),
    Permanent(String),
}

impl ExportError {
    /// Whether another attempt can succeed.
    pub fn is_retryable(&self) -> bool {
        matches!(self, Self::Transient(_))
    }

    /// Classify an HTTP status the way the OTLP/HTTP specification does: only 429, 502, 503
    /// and 504 are retryable, every other 4xx or 5xx must not be retried.
    fn from_http_status(status: StatusCode, body: &[u8]) -> Self {
        let excerpt = String::from_utf8_lossy(&body[..body.len().min(MAX_ERROR_BODY_BYTES)]);
        let message = format!("HTTP {status}: {excerpt}");
        match status {
            StatusCode::TOO_MANY_REQUESTS
            | StatusCode::BAD_GATEWAY
            | StatusCode::SERVICE_UNAVAILABLE
            | StatusCode::GATEWAY_TIMEOUT => Self::Transient(message),
            _ => Self::Permanent(message),
        }
    }

    fn from_reqwest(error: reqwest::Error) -> Self {
        // The URL can carry a query with credentials in it.
        let error = error.without_url();
        let message = with_causes(format!("HTTP request: {error}"), error.source());
        if error.is_builder() {
            Self::Permanent(message)
        } else {
            Self::Transient(message)
        }
    }
}

impl fmt::Display for ExportError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let (Self::Transient(message) | Self::Permanent(message)) = self;
        f.write_str(message)
    }
}

impl From<ExportError> for Error {
    fn from(error: ExportError) -> Self {
        match error {
            ExportError::Permanent(message) => Error::PermanentHttpError(message),
            ExportError::Transient(message) => Error::CannotStoreData(message),
        }
    }
}

/// Settings resolved once at `open()`. No `Debug`: the header values are exposed secrets.
pub(crate) struct ExporterSettings<'a> {
    pub endpoint: &'a Url,
    /// The OTLP/HTTP path of the signal, appended to the endpoint's own path.
    pub path: &'static str,
    pub gzip: bool,
    pub timeout: Duration,
    pub headers: Vec<(String, String)>,
}

#[derive(Debug)]
pub(crate) struct HttpExporter {
    client: reqwest::Client,
    url: Url,
    gzip: bool,
}

impl HttpExporter {
    pub fn new(settings: &ExporterSettings<'_>) -> Result<Self, Error> {
        let mut headers = HeaderMap::with_capacity(settings.headers.len() + 2);
        for (key, value) in &settings.headers {
            let name = HeaderName::from_bytes(key.as_bytes())
                .map_err(|_| Error::InvalidConfigValue(format!("header name '{key}'")))?;
            let mut value = HeaderValue::from_str(value)
                .map_err(|_| Error::InvalidConfigValue(format!("value of header '{key}'")))?;
            value.set_sensitive(true);
            headers.insert(name, value);
        }
        headers.insert(
            CONTENT_TYPE,
            HeaderValue::from_static(PROTOBUF_CONTENT_TYPE),
        );
        if settings.gzip {
            headers.insert(CONTENT_ENCODING, HeaderValue::from_static("gzip"));
        }

        let client = reqwest::Client::builder()
            .default_headers(headers)
            .timeout(settings.timeout)
            .connect_timeout(settings.timeout)
            // A followed 301 or 302 turns the POST into a GET without a body, and a redirect
            // to another host would carry the configured headers, API keys among them.
            .redirect(reqwest::redirect::Policy::none())
            .build()
            .map_err(|error| Error::InitError(format!("OTLP HTTP client: {error}")))?;

        Ok(Self {
            client,
            url: signal_url(settings.endpoint, settings.path),
            gzip: settings.gzip,
        })
    }

    /// Prepare the bytes sent on every attempt of one request, compressing them here once.
    pub fn prepare_body(&self, encoded: Vec<u8>) -> Result<Bytes, Error> {
        if self.gzip {
            gzip(&encoded).map(Bytes::from)
        } else {
            Ok(Bytes::from(encoded))
        }
    }

    pub async fn export<U: ExportResponse>(&self, body: Bytes) -> Result<U, ExportError> {
        let response = self
            .client
            .post(self.url.clone())
            .body(body)
            .send()
            .await
            .map_err(ExportError::from_reqwest)?;

        let status = response.status();
        if !status.is_success() {
            return Err(ExportError::from_http_status(
                status,
                &error_excerpt(response).await,
            ));
        }
        let protobuf = response.headers().get(CONTENT_TYPE).is_some_and(|value| {
            value
                .as_bytes()
                .starts_with(PROTOBUF_CONTENT_TYPE.as_bytes())
        });
        // The receiver accepted the request, so a body that fails to arrive loses only the
        // `partial_success` report. Retrying would send accepted items again.
        let body = response.bytes().await.unwrap_or_default();
        Ok(decode_http_response(protobuf, &body))
    }
}

/// The endpoint with the signal path appended to its own path. A query string is kept.
fn signal_url(endpoint: &Url, signal_path: &str) -> Url {
    let mut url = endpoint.clone();
    let path = format!("{}{signal_path}", url.path().trim_end_matches('/'));
    url.set_path(&path);
    url
}

/// The start of an error response, which is all `from_http_status` keeps of it.
async fn error_excerpt(mut response: reqwest::Response) -> Vec<u8> {
    let mut excerpt = Vec::new();
    while excerpt.len() < MAX_ERROR_BODY_BYTES {
        match response.chunk().await {
            Ok(Some(chunk)) => excerpt.extend_from_slice(&chunk),
            Ok(None) | Err(_) => break,
        }
    }
    excerpt
}

/// A success response carries `partial_success`, encoded as protobuf because the request was.
/// A backend that answers with an empty or unparsable body accepted everything.
fn decode_http_response<U: ExportResponse>(protobuf: bool, body: &[u8]) -> U {
    if !protobuf || body.is_empty() {
        return U::default();
    }
    U::decode(body).unwrap_or_default()
}

/// `head`, then every cause in the chain that starts at `source`. A cause that the text
/// already ends with is skipped, because some errors copy the message of their source.
fn with_causes(head: String, mut source: Option<&(dyn StdError + 'static)>) -> String {
    let mut text = head;
    while let Some(cause) = source {
        let cause_text = cause.to_string();
        if !text.ends_with(&cause_text) {
            text.push_str(": ");
            text.push_str(&cause_text);
        }
        source = cause.source();
    }
    text
}

fn gzip(bytes: &[u8]) -> Result<Vec<u8>, Error> {
    let mut encoder = GzEncoder::new(Vec::with_capacity(bytes.len() / 2), Compression::default());
    encoder
        .write_all(bytes)
        .and_then(|()| encoder.finish())
        .map_err(|error| Error::Serialization(format!("gzip: {error}")))
}

#[cfg(test)]
mod tests {
    use super::*;
    use opentelemetry_proto::tonic::collector::logs::v1::{
        ExportLogsPartialSuccess, ExportLogsServiceResponse,
    };
    use prost::Message;
    use std::io::Read as _;

    #[test]
    fn given_retryable_http_statuses_should_be_transient() {
        for status in [429, 502, 503, 504] {
            let status = StatusCode::from_u16(status).expect("valid status");
            assert!(ExportError::from_http_status(status, b"").is_retryable());
        }
    }

    #[test]
    fn given_other_http_errors_should_be_permanent() {
        for status in [400, 401, 403, 404, 413, 500, 501] {
            let status = StatusCode::from_u16(status).expect("valid status");
            let error = ExportError::from_http_status(status, b"bad request");
            assert!(!error.is_retryable(), "{status} should not be retried");
            assert!(matches!(Error::from(error), Error::PermanentHttpError(_)));
        }
    }

    #[test]
    fn given_transient_error_should_map_to_retryable_sdk_error() {
        let error = ExportError::from_http_status(StatusCode::SERVICE_UNAVAILABLE, b"");
        assert!(matches!(Error::from(error), Error::CannotStoreData(_)));
    }

    #[test]
    fn given_long_error_body_should_be_truncated_in_message() {
        let body = vec![b'x'; MAX_ERROR_BODY_BYTES * 4];
        let error = ExportError::from_http_status(StatusCode::BAD_REQUEST, &body);
        assert!(error.to_string().len() < MAX_ERROR_BODY_BYTES + 64);
    }

    #[test]
    fn given_protobuf_success_body_should_read_partial_success() {
        let response = ExportLogsServiceResponse {
            partial_success: Some(ExportLogsPartialSuccess {
                rejected_log_records: 3,
                error_message: "dropped".to_owned(),
            }),
        };

        let decoded: ExportLogsServiceResponse =
            decode_http_response(true, &response.encode_to_vec());

        assert_eq!(decoded.rejected(), Some((3, "dropped")));
    }

    #[test]
    fn given_non_protobuf_success_body_should_mean_nothing_rejected() {
        let empty: ExportLogsServiceResponse = decode_http_response(true, b"");
        let garbage: ExportLogsServiceResponse = decode_http_response(false, b"OK");
        let json: ExportLogsServiceResponse =
            decode_http_response(false, br#"{"partialSuccess":{"rejectedLogRecords":"2"}}"#);

        assert!(empty.rejected().is_none());
        assert!(garbage.rejected().is_none());
        assert!(json.rejected().is_none());
    }

    #[test]
    fn given_gzip_should_round_trip() {
        let compressed = gzip(b"otlp payload").expect("gzip succeeds");

        let mut decoded = String::new();
        flate2::read::GzDecoder::new(compressed.as_slice())
            .read_to_string(&mut decoded)
            .expect("valid gzip");
        assert_eq!(decoded, "otlp payload");
    }

    #[test]
    fn given_invalid_http_header_name_should_fail_construction() {
        let endpoint = Url::parse("http://localhost:4318").expect("valid endpoint");
        let settings = ExporterSettings {
            endpoint: &endpoint,
            path: "/v1/logs",
            gzip: false,
            timeout: Duration::from_secs(1),
            headers: vec![("bad header".to_owned(), "v".to_owned())],
        };

        assert!(matches!(
            HttpExporter::new(&settings),
            Err(Error::InvalidConfigValue(_))
        ));
    }

    #[test]
    fn given_endpoint_with_trailing_slash_should_build_signal_url() {
        let endpoint = Url::parse("http://collector:4318/").expect("valid endpoint");
        let settings = ExporterSettings {
            endpoint: &endpoint,
            path: "/v1/logs",
            gzip: true,
            timeout: Duration::from_secs(1),
            headers: vec![],
        };

        let exporter = HttpExporter::new(&settings).expect("valid settings");

        assert_eq!(exporter.url.as_str(), "http://collector:4318/v1/logs");
        assert!(exporter.gzip);
    }

    #[test]
    fn given_endpoint_with_path_and_query_should_append_signal_path_before_query() {
        let endpoint =
            Url::parse("https://otlp.example.com/otlp/?key=abc").expect("valid endpoint");

        let url = signal_url(&endpoint, "/v1/logs");

        assert_eq!(
            url.as_str(),
            "https://otlp.example.com/otlp/v1/logs?key=abc"
        );
    }
}
