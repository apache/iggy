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
use iggy_connector_sdk::retry::is_transient_status;
use opensearch::http::{StatusCode, response::Response};
use std::fmt;

pub(crate) const DEFAULT_MAX_RETRIES: u32 = 3;
pub(crate) const DEFAULT_RETRY_DELAY: &str = "1s";
pub(crate) const DEFAULT_RETRY_MAX_DELAY: &str = "30s";
pub(crate) const DEFAULT_MAX_OPEN_RETRIES: u32 = 5;
pub(crate) const DEFAULT_OPEN_RETRY_MAX_DELAY: &str = "30s";
pub(crate) const DEFAULT_CB_THRESHOLD: u32 = 5;
pub(crate) const DEFAULT_CB_COOL_DOWN: &str = "60s";
const UNREADABLE_BODY: &str = "unknown error";

/// Failure of one OpenSearch HTTP request, kept structured so the retry predicate and the
/// caller's error mapping both work from the same facts.
#[derive(Debug)]
pub(crate) enum RequestFailure {
    Transport(String),
    Status { status: StatusCode, body: String },
}

impl RequestFailure {
    pub(crate) fn transport(error: opensearch::Error) -> Self {
        Self::Transport(error.to_string())
    }

    pub(crate) async fn check(response: Response) -> Result<Response, Self> {
        let status = response.status_code();
        if status.is_success() {
            return Ok(response);
        }
        let body = response
            .text()
            .await
            .unwrap_or_else(|_| UNREADABLE_BODY.to_string());
        Err(Self::Status { status, body })
    }

    /// Like `check`, but also reads the body so a transport failure while receiving it is
    /// classified as transient and retried together with the request.
    pub(crate) async fn read_text(response: Response) -> Result<String, Self> {
        Self::check(response)
            .await?
            .text()
            .await
            .map_err(Self::transport)
    }

    /// Network errors, 429 and 5xx are worth another attempt; other statuses are permanent.
    pub(crate) fn is_transient(&self) -> bool {
        match self {
            Self::Transport(_) => true,
            Self::Status { status, .. } => is_transient_status(*status),
        }
    }
}

impl RequestFailure {
    /// Maps a search failure to the SDK variant that tells the runtime whether another poll
    /// can succeed: connectivity and exhausted-retry statuses are transient, other statuses are not.
    pub(crate) fn into_search_error(self) -> Error {
        match self {
            Self::Transport(message) => {
                Error::Connection(format!("Failed to execute search: {message}"))
            }
            failure if failure.is_transient() => {
                Error::HttpRequestFailed(format!("Search request failed: {failure}"))
            }
            failure => Error::PermanentHttpError(format!("Search request failed: {failure}")),
        }
    }
}

impl fmt::Display for RequestFailure {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Transport(message) => f.write_str(message),
            Self::Status { status, body } => write!(f, "HTTP {}: {body}", status.as_u16()),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn status_failure(code: u16) -> RequestFailure {
        RequestFailure::Status {
            status: StatusCode::from_u16(code).expect("valid status code"),
            body: String::new(),
        }
    }

    #[test]
    fn given_429_or_5xx_when_check_transient_should_return_true() {
        assert!(status_failure(429).is_transient());
        assert!(status_failure(503).is_transient());
    }

    #[test]
    fn given_other_4xx_when_check_transient_should_return_false() {
        assert!(!status_failure(404).is_transient());
        assert!(!status_failure(400).is_transient());
    }

    #[test]
    fn given_transport_failure_when_check_transient_should_return_true() {
        assert!(RequestFailure::Transport("connection reset".to_string()).is_transient());
    }

    #[test]
    fn given_transport_failure_when_mapped_should_return_connection_error() {
        let error = RequestFailure::Transport("reset".to_string()).into_search_error();
        assert!(matches!(error, Error::Connection(_)));
    }

    #[test]
    fn given_transient_status_when_mapped_should_return_http_request_failed() {
        assert!(matches!(
            status_failure(503).into_search_error(),
            Error::HttpRequestFailed(_)
        ));
        assert!(matches!(
            status_failure(429).into_search_error(),
            Error::HttpRequestFailed(_)
        ));
    }

    #[test]
    fn given_permanent_status_when_mapped_should_return_permanent_http_error() {
        assert!(matches!(
            status_failure(400).into_search_error(),
            Error::PermanentHttpError(_)
        ));
        assert!(matches!(
            status_failure(401).into_search_error(),
            Error::PermanentHttpError(_)
        ));
    }

    #[test]
    fn given_status_failure_when_displayed_should_use_numeric_code() {
        let failure = RequestFailure::Status {
            status: StatusCode::SERVICE_UNAVAILABLE,
            body: "temporary".to_string(),
        };
        assert_eq!(failure.to_string(), "HTTP 503: temporary");
    }
}
