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

//! What every credential source has in common: the requests that it sends,
//! how they reach its service, why a fetch fails, and when the next fetch is
//! due.
//!
//! Each source builds its requests and reads the answers without I/O, so
//! tests answer them from recorded responses. A [`CredentialTransport`]
//! sends the requests, and [`HttpTransport`] does that over HTTP.

use std::fmt;
use std::future::Future;
use std::io;
use std::path::PathBuf;
use std::time::{Duration, SystemTime};

use bytes::Bytes;
use http::Method;

use crate::error::ObjectStoreError;

/// How long before credentials expire a node fetches new ones.
pub const REFRESH_MARGIN: Duration = Duration::from_mins(5);
/// The shortest wait before the next fetch.
pub const MIN_REFRESH_WAIT: Duration = Duration::from_secs(1);

/// Why a source handed out no credentials.
#[derive(Debug, thiserror::Error)]
pub enum CredentialError {
    /// The request got no answer: a refused connection, a timeout or a
    /// failed name lookup.
    #[error("the credential service did not answer: {0}")]
    Unreachable(String),
    /// The service failed in a way that a retry can get past, such as an
    /// HTTP 5xx.
    #[error("the credential service failed: {0}")]
    Unavailable(String),
    /// The service refused the request for a reason that only an operator
    /// can change, such as a role that does not trust the token.
    #[error("the credential service refused the request: {0}")]
    Refused(String),
    #[error("malformed answer of the credential service: {0}")]
    Malformed(String),
    #[error("the web identity token file {path:?}: {source}")]
    TokenFile {
        path: PathBuf,
        #[source]
        source: io::Error,
    },
}

/// One HTTP request of a credential source. `Debug` shows neither the
/// headers nor the body, which can hold tokens.
pub struct CredentialRequest {
    pub method: Method,
    pub url: String,
    pub headers: Vec<(&'static str, String)>,
    pub body: String,
}

/// The answer to a [`CredentialRequest`]. `Debug` shows only the status and
/// the length of the body, which can hold credentials.
pub struct CredentialResponse {
    pub status: u16,
    pub body: Bytes,
}

/// How a credential source reaches its service.
pub trait CredentialTransport {
    /// Sends `request` and returns the answer, whatever its status.
    ///
    /// # Errors
    /// [`CredentialError::Unreachable`] when no answer came.
    fn send(
        &self,
        request: CredentialRequest,
    ) -> impl Future<Output = Result<CredentialResponse, CredentialError>>;
}

/// Sends the requests of a credential source over HTTP, each with a
/// deadline.
pub struct HttpTransport {
    client: cyper::Client,
    timeout: Duration,
}

impl CredentialError {
    /// Whether the same fetch can succeed later without a change by an
    /// operator.
    #[must_use]
    pub const fn is_transient(&self) -> bool {
        matches!(self, Self::Unreachable(_) | Self::Unavailable(_))
    }
}

impl fmt::Debug for CredentialRequest {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("CredentialRequest")
            .field("method", &self.method)
            .field("url", &self.url)
            .finish_non_exhaustive()
    }
}

impl fmt::Debug for CredentialResponse {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("CredentialResponse")
            .field("status", &self.status)
            .field("body_len", &self.body.len())
            .finish()
    }
}

impl HttpTransport {
    /// A transport for the requests of a source at `endpoint`, each of which
    /// fails as unreachable after `timeout`.
    ///
    /// # Errors
    /// [`ObjectStoreError::InvalidRequest`] for an endpoint that is not an
    /// `http` or `https` URL, and [`ObjectStoreError::Transport`] when the
    /// client cannot be built.
    pub fn new(endpoint: &str, timeout: Duration) -> Result<Self, ObjectStoreError> {
        let scheme = url::Url::parse(endpoint)
            .map_err(|_| ObjectStoreError::InvalidRequest(format!("{endpoint:?} is not a URL")))?
            .scheme()
            .to_owned();
        Ok(Self {
            client: crate::http::client(&scheme)?,
            timeout,
        })
    }
}

impl CredentialTransport for HttpTransport {
    async fn send(
        &self,
        request: CredentialRequest,
    ) -> Result<CredentialResponse, CredentialError> {
        let unreachable =
            |error: &dyn fmt::Display| CredentialError::Unreachable(error.to_string());
        let mut builder = self
            .client
            .request(request.method, request.url.as_str())
            .map_err(|error| unreachable(&error))?;
        for (name, value) in &request.headers {
            builder = builder
                .header(*name, value.as_str())
                .map_err(|error| CredentialError::Malformed(format!("header {name}: {error}")))?;
        }
        let exchange = async {
            let response = builder.body(request.body).send().await?;
            let status = response.status().as_u16();
            let body = response.bytes().await?;
            Ok::<_, cyper::Error>(CredentialResponse { status, body })
        };
        compio::time::timeout(self.timeout, exchange)
            .await
            .map_err(|_| {
                CredentialError::Unreachable(format!("no answer within {:?}", self.timeout))
            })?
            .map_err(|error| unreachable(&error))
    }
}

/// How long to wait before the next fetch, after one that handed out
/// credentials that expire at `expires_at`.
///
/// The next fetch comes [`REFRESH_MARGIN`] before the expiry. Credentials
/// that expire sooner than that are still used for half of the time that
/// they have left, so a service that hands them out close to their expiry
/// costs no busy loop.
#[must_use]
pub fn next_refresh(expires_at: SystemTime, now: SystemTime) -> Duration {
    let left = expires_at.duration_since(now).unwrap_or_default();
    left.saturating_sub(REFRESH_MARGIN)
        .max(left / 2)
        .max(MIN_REFRESH_WAIT)
}

/// A transport for the tests of a source.
#[cfg(test)]
pub mod testing {
    use std::cell::RefCell;
    use std::collections::VecDeque;
    use std::future::{Future, ready};

    use bytes::Bytes;

    use super::{CredentialError, CredentialRequest, CredentialResponse, CredentialTransport};

    /// Answers each request with the next recorded response, and keeps the
    /// requests.
    #[derive(Default)]
    pub struct RecordedTransport {
        answers: RefCell<VecDeque<Result<CredentialResponse, CredentialError>>>,
        pub requests: RefCell<Vec<CredentialRequest>>,
    }

    impl RecordedTransport {
        pub fn answering(answers: impl IntoIterator<Item = (u16, &'static str)>) -> Self {
            Self {
                answers: RefCell::new(
                    answers
                        .into_iter()
                        .map(|(status, body)| {
                            Ok(CredentialResponse {
                                status,
                                body: Bytes::from_static(body.as_bytes()),
                            })
                        })
                        .collect(),
                ),
                requests: RefCell::default(),
            }
        }
    }

    impl CredentialTransport for RecordedTransport {
        fn send(
            &self,
            request: CredentialRequest,
        ) -> impl Future<Output = Result<CredentialResponse, CredentialError>> {
            self.requests.borrow_mut().push(request);
            ready(self.answers.borrow_mut().pop_front().unwrap_or_else(|| {
                Err(CredentialError::Unreachable(
                    "no more recorded answers".to_owned(),
                ))
            }))
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn given_expiries_when_scheduling_should_refresh_before_each_and_never_spin() {
        let now = SystemTime::UNIX_EPOCH + Duration::from_hours(1000);

        assert_eq!(
            next_refresh(now + Duration::from_hours(1), now),
            Duration::from_mins(55)
        );
        assert_eq!(
            next_refresh(now + Duration::from_mins(4), now),
            Duration::from_mins(2)
        );
        assert_eq!(next_refresh(now, now), MIN_REFRESH_WAIT);
        assert_eq!(
            next_refresh(now - Duration::from_mins(1), now),
            MIN_REFRESH_WAIT
        );
    }

    #[test]
    fn given_requests_and_answers_when_debug_printing_should_show_no_secret() {
        let request = CredentialRequest {
            method: Method::POST,
            url: "https://oauth2.example/token".to_owned(),
            headers: vec![("authorization", "token-value".to_owned())],
            body: "assertion=token-value".to_owned(),
        };
        let response = CredentialResponse {
            status: 200,
            body: Bytes::from_static(b"secret-value"),
        };

        let printed = format!("{request:?} {response:?}");

        assert!(!printed.contains("token-value"), "{printed}");
        assert!(!printed.contains("secret"), "{printed}");
    }
}
