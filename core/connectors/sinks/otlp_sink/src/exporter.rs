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

use bytes::Bytes;
use flate2::Compression;
use flate2::write::GzEncoder;
use iggy_connector_sdk::Error;
use opentelemetry_proto::tonic::collector::logs::v1::ExportLogsServiceResponse;
use prost::Message;
use reqwest::header::{CONTENT_ENCODING, CONTENT_TYPE, HeaderMap, HeaderName, HeaderValue};
use reqwest::{StatusCode, Url};
use std::error::Error as StdError;
use std::fmt;
use std::io::Write as _;
use std::time::Duration;

const PROTOBUF_CONTENT_TYPE: &str = "application/x-protobuf";
const MAX_ERROR_BODY_BYTES: usize = 512;
/// A success response holds at most a `partial_success` report, so this is plenty.
const MAX_RESPONSE_BYTES: usize = 64 * 1024;

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
        let permanent = error.is_builder();
        let message = format!("HTTP request: {}", describe(error));
        if permanent {
            Self::Permanent(message)
        } else {
            Self::Transient(message)
        }
    }

    /// The receiver answered with a success status, so it has the request, but its
    /// `partial_success` report is lost. Trying again would export the request twice.
    fn unreadable_response(problem: fmt::Arguments<'_>) -> Self {
        Self::Permanent(format!(
            "the receiver accepted the request, but its response {problem}"
        ))
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

    pub async fn export(&self, body: Bytes) -> Result<ExportLogsServiceResponse, ExportError> {
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
        let protobuf = response
            .headers()
            .get(CONTENT_TYPE)
            .is_some_and(is_protobuf);
        let body = success_body(response).await?;
        decode_http_response(protobuf, &body)
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

/// The body of a success response, refused once it passes `MAX_RESPONSE_BYTES`.
async fn success_body(mut response: reqwest::Response) -> Result<Vec<u8>, ExportError> {
    let mut body = Vec::new();
    loop {
        let chunk = match response.chunk().await {
            Ok(Some(chunk)) => chunk,
            Ok(None) => return Ok(body),
            Err(error) => {
                return Err(ExportError::unreadable_response(format_args!(
                    "could not be read: {}",
                    describe(error)
                )));
            }
        };
        if body.len() + chunk.len() > MAX_RESPONSE_BYTES {
            return Err(ExportError::unreadable_response(format_args!(
                "is larger than {MAX_RESPONSE_BYTES} bytes"
            )));
        }
        body.extend_from_slice(&chunk);
    }
}

/// Whether a `Content-Type` is protobuf. A media type is case-insensitive and can carry
/// parameters, such as `; charset=binary`.
fn is_protobuf(content_type: &HeaderValue) -> bool {
    content_type.to_str().is_ok_and(|value| {
        let media_type = value
            .split_once(';')
            .map_or(value, |(media_type, _)| media_type);
        media_type
            .trim()
            .eq_ignore_ascii_case(PROTOBUF_CONTENT_TYPE)
    })
}

/// A success response carries `partial_success`, encoded as protobuf because the request was.
/// An empty body is a response without it. A backend that answers with another content type
/// reports nothing, so it accepted everything.
fn decode_http_response(
    protobuf: bool,
    body: &[u8],
) -> Result<ExportLogsServiceResponse, ExportError> {
    if !protobuf {
        return Ok(ExportLogsServiceResponse::default());
    }
    ExportLogsServiceResponse::decode(body).map_err(|error| {
        ExportError::unreadable_response(format_args!(
            "is not a protobuf ExportLogsServiceResponse: {error}"
        ))
    })
}

/// The error and its causes, without the URL, which can carry a query with credentials in it.
fn describe(error: reqwest::Error) -> String {
    let error = error.without_url();
    with_causes(error.to_string(), error.source())
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
    use crate::logs::rejected_log_records;
    use opentelemetry_proto::tonic::collector::logs::v1::ExportLogsPartialSuccess;
    use std::io::Read as _;

    fn read_body(body: Vec<u8>) -> Result<Vec<u8>, ExportError> {
        let response = reqwest::Response::from(http::Response::new(body));
        tokio::runtime::Runtime::new()
            .expect("runtime")
            .block_on(success_body(response))
    }

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

        let decoded =
            decode_http_response(true, &response.encode_to_vec()).expect("valid response");

        assert_eq!(rejected_log_records(&decoded), Some((3, "dropped")));
    }

    #[test]
    fn given_empty_protobuf_success_body_should_mean_nothing_rejected() {
        let empty = decode_http_response(true, b"").expect("an empty body is a valid response");

        assert!(rejected_log_records(&empty).is_none());
    }

    #[test]
    fn given_undecodable_protobuf_success_body_should_fail_without_retry() {
        let error = decode_http_response(true, &[0xff, 0xff, 0xff])
            .expect_err("the partial-success report is lost");

        assert!(
            !error.is_retryable(),
            "a retry would export the request twice"
        );
    }

    #[test]
    fn given_non_protobuf_success_body_should_mean_nothing_rejected() {
        let text = decode_http_response(false, b"OK").expect("not a protobuf response");
        let json = decode_http_response(false, br#"{"partialSuccess":{"rejectedLogRecords":"2"}}"#)
            .expect("not a protobuf response");

        assert!(rejected_log_records(&text).is_none());
        assert!(rejected_log_records(&json).is_none());
    }

    #[test]
    fn given_protobuf_content_types_should_match_case_insensitively_with_parameters() {
        for value in [
            "application/x-protobuf",
            "Application/X-Protobuf",
            "application/x-protobuf; charset=binary",
            " application/x-protobuf ;",
        ] {
            assert!(is_protobuf(&HeaderValue::from_static(value)), "{value}");
        }
        for value in ["application/json", "application/x-protobufs", "text/plain"] {
            assert!(!is_protobuf(&HeaderValue::from_static(value)), "{value}");
        }
    }

    #[test]
    fn given_success_body_within_limit_should_read_it() {
        let body = vec![1u8; MAX_RESPONSE_BYTES];

        assert_eq!(read_body(body.clone()).expect("within the limit"), body);
    }

    #[test]
    fn given_success_body_above_limit_should_fail_without_retry() {
        let error = read_body(vec![1u8; MAX_RESPONSE_BYTES + 1]).expect_err("above the limit");

        assert!(
            !error.is_retryable(),
            "a retry would export the request twice"
        );
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
