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

//! The S3 provider, for AWS S3 and the stores with its API, such as Ceph RGW
//! and R2.

pub mod credentials;
mod sigv4;
mod xml;

use std::cell::{Cell, RefCell};
use std::io;
use std::ops::Range;
use std::rc::Rc;
use std::sync::Arc;
use std::time::SystemTime;

use base64::Engine;
use base64::engine::general_purpose::STANDARD as BASE64;
use chrono::{DateTime, TimeDelta, Utc};
use futures::StreamExt;
use http::header::{CONTENT_LENGTH, CONTENT_RANGE, DATE, ETAG, LAST_MODIFIED};
use http::{HeaderMap, Method, StatusCode};
use send_wrapper::SendWrapper;
use url::Url;

pub use credentials::SharedCredentials;
pub use sigv4::Credentials;

use crate::error::ObjectStoreError;
use crate::resolver::ShardResolver;
use crate::store::{
    DeleteFailure, DeleteOutcome, ListPage, ListRequest, ObjectMeta, ObjectRange, ObjectStore,
    PutBody, PutMode, PutOptions, UserMetadata, validate_delete, validate_key, validate_metadata,
    validate_prefix, validate_range,
};
use sigv4::{EMPTY_PAYLOAD_SHA256, SigningRequest, UNSIGNED_PAYLOAD};

const METADATA_HEADER_PREFIX: &str = "x-amz-meta-";
const CHECKSUM_HEADER: &str = "x-amz-checksum-crc32c";
/// The largest offset that a `Range` header carries: S3 reads offsets as
/// signed 64-bit numbers. No object is that large.
const MAX_RANGE_OFFSET: u64 = i64::MAX.unsigned_abs();
/// A clock correction smaller than this is the rounding of the store's
/// `Date` header, not a skew that a new signature fixes.
const MIN_CLOCK_CORRECTION: TimeDelta = TimeDelta::seconds(2);

/// Where the bucket goes in a request URL.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum UrlStyle {
    /// `{endpoint}/{bucket}/{key}`, which most stores with the S3 API take.
    Path,
    /// `{bucket}.{endpoint host}/{key}`, which AWS expects.
    VirtualHosted,
}

/// Where an [`S3Store`] sends its requests, and how it signs them.
#[derive(Debug, Clone)]
pub struct S3Config {
    /// The base URL of the store, without a path, such as
    /// `https://s3.eu-central-1.amazonaws.com` or `http://localhost:4566`.
    pub endpoint: String,
    pub region: String,
    pub bucket: String,
    pub url_style: UrlStyle,
    /// What the store signs with: fixed keys, or the credentials that a
    /// refresher of the node keeps fresh.
    pub credentials: SharedCredentials,
}

/// An [`ObjectStore`] in one S3 bucket, over one HTTP client.
///
/// - A put streams its body with an explicit `content-length`, signed as
///   `UNSIGNED-PAYLOAD`. `If-None-Match: *` makes it a conditional create,
///   and `x-amz-checksum-crc32c` makes the store verify the bytes and keep
///   the CRC32C, which `head` reports.
/// - Keys go into the URL path percent-encoded, and a listing asks for
///   URL-encoded keys, so a key that XML cannot hold still lists.
/// - The client follows no redirect, because a redirect would resend a body
///   that is already sent, under a signature for another host.
/// - Lookups of the store's host never block the shard, see
///   [`ShardResolver`].
/// - When the store refuses a request for the skew of the clocks, the store
///   learns the offset from the answer's `Date` header and fails the call as
///   transient, so a retry signs with the corrected clock. A second refusal
///   after a correction is permanent.
pub struct S3Store {
    client: cyper::Client,
    /// `scheme://host[:port]` of every request.
    origin: String,
    /// The `host` header, which the signature covers.
    host: String,
    /// The encoded path of the bucket, empty when the host names the bucket.
    bucket_path: String,
    bucket: String,
    region: String,
    credentials: SharedCredentials,
    /// How far the store's clock runs ahead of this node's.
    clock_offset: Cell<TimeDelta>,
}

/// One request before its signature.
struct S3Request<'a> {
    method: Method,
    /// The encoded path.
    path: String,
    query: Vec<(&'a str, &'a str)>,
    /// The headers that the signature covers, with lowercase names.
    headers: Vec<(String, String)>,
    payload_sha256: &'a str,
}

/// What a failed call was about, for the error that it returns.
enum Subject<'a> {
    Object(&'a str),
    Range { key: &'a str, range: Range<u64> },
    Bucket,
}

impl S3Store {
    /// A store over a client of its own. The endpoint's host is looked up on
    /// the first request.
    ///
    /// # Errors
    /// [`ObjectStoreError::InvalidRequest`] for an endpoint that is not an
    /// `http` or `https` URL without a path, and
    /// [`ObjectStoreError::Transport`] when the client cannot be built.
    pub fn new(config: S3Config) -> Result<Self, ObjectStoreError> {
        let invalid = |reason: &str| {
            ObjectStoreError::InvalidRequest(format!("endpoint {:?} {reason}", config.endpoint))
        };
        let endpoint = Url::parse(&config.endpoint).map_err(|_| invalid("is not a URL"))?;
        let scheme = endpoint.scheme();
        if !matches!(scheme, "http" | "https") {
            return Err(invalid("is neither an http nor an https URL"));
        }
        if endpoint.path() != "/"
            || endpoint.query().is_some()
            || !endpoint.username().is_empty()
            || endpoint.password().is_some()
        {
            return Err(invalid("has a path, a query or user information"));
        }
        let host = endpoint.host_str().ok_or_else(|| invalid("has no host"))?;
        let authority = endpoint
            .port()
            .map_or_else(|| host.to_owned(), |port| format!("{host}:{port}"));
        let (host, bucket_path) = match config.url_style {
            UrlStyle::Path => (
                authority,
                format!("/{}", sigv4::encode_path(&config.bucket)),
            ),
            UrlStyle::VirtualHosted => (format!("{}.{authority}", config.bucket), String::new()),
        };
        Ok(Self {
            client: http_client(scheme)?,
            origin: format!("{scheme}://{host}"),
            host,
            bucket_path,
            bucket: config.bucket,
            region: config.region,
            credentials: config.credentials,
            clock_offset: Cell::new(TimeDelta::zero()),
        })
    }

    /// Creates the bucket, and succeeds when this account owns it already.
    /// For tests and local setups: an operator creates a production bucket,
    /// with its own policies. Outside `us-east-1`, AWS refuses a create
    /// without a location constraint, which this call does not send.
    ///
    /// # Errors
    /// Any error of the store.
    pub async fn create_bucket(&self) -> Result<(), ObjectStoreError> {
        let request = S3Request {
            method: Method::PUT,
            path: self.bucket_root(),
            query: Vec::new(),
            headers: Vec::new(),
            payload_sha256: EMPTY_PAYLOAD_SHA256,
        };
        let response = self.send(request, &[], cyper::Body::empty()).await?;
        if response.status().is_success() {
            return Ok(());
        }
        let (status, date, error) = read_failure(response).await;
        if error.code == "BucketAlreadyOwnedByYou" {
            return Ok(());
        }
        Err(self.failure(status, date, &error, &Subject::Bucket))
    }

    fn object_path(&self, key: &str) -> String {
        format!("{}/{}", self.bucket_path, sigv4::encode_path(key))
    }

    fn bucket_root(&self) -> String {
        if self.bucket_path.is_empty() {
            "/".to_owned()
        } else {
            self.bucket_path.clone()
        }
    }

    /// Signs and sends `request`. `unsigned` headers go out without a place
    /// in the signature.
    async fn send(
        &self,
        mut request: S3Request<'_>,
        unsigned: &[(&str, String)],
        body: cyper::Body,
    ) -> Result<cyper::Response, ObjectStoreError> {
        let credentials = self.credentials.current().ok_or_else(|| {
            ObjectStoreError::Unavailable(
                "the node has no credentials for the store yet, and fetches them".to_owned(),
            )
        })?;
        let now = Utc::now() + self.clock_offset.get();
        request.headers.extend([
            ("host".to_owned(), self.host.clone()),
            ("x-amz-date".to_owned(), sigv4::amz_date(now)),
            (
                "x-amz-content-sha256".to_owned(),
                request.payload_sha256.to_owned(),
            ),
        ]);
        if let Some(token) = &credentials.session_token {
            request
                .headers
                .push(("x-amz-security-token".to_owned(), token.clone()));
        }
        let query = sigv4::canonical_query(&request.query);
        let authorization = sigv4::authorization(
            &SigningRequest {
                method: request.method.as_str(),
                path: &request.path,
                query: &query,
                headers: &request.headers,
                payload_sha256: request.payload_sha256,
            },
            &credentials,
            &self.region,
            now,
        );
        let mut url = format!("{}{}", self.origin, request.path);
        if !query.is_empty() {
            url.push('?');
            url.push_str(&query);
        }
        let mut builder = self
            .client
            .request(request.method, url.as_str())
            .map_err(transport)?;
        let headers = request
            .headers
            .iter()
            .map(|(name, value)| (name.as_str(), value.as_str()))
            .chain(unsigned.iter().map(|(name, value)| (*name, value.as_str())))
            .chain([("authorization", authorization.as_str())]);
        for (name, value) in headers {
            builder = builder.header(name, value).map_err(|error| {
                ObjectStoreError::InvalidRequest(format!("header {name}: {error}"))
            })?;
        }
        builder.body(body).send().await.map_err(transport)
    }

    /// The error of an answer that is not a success. A refusal for the skew
    /// of the clocks corrects the clock once. Expired credentials that a
    /// refresher keeps fresh are an outage that the next refresh ends.
    fn failure(
        &self,
        status: StatusCode,
        date: Option<DateTime<Utc>>,
        error: &xml::ErrorBody,
        subject: &Subject<'_>,
    ) -> ObjectStoreError {
        if self.credentials.refreshes()
            && matches!(error.code.as_str(), "ExpiredToken" | "TokenRefreshRequired")
        {
            return ObjectStoreError::Unavailable(format!(
                "{}; the node refreshes its credentials",
                detail(status, error)
            ));
        }
        if error.code == "RequestTimeTooSkewed"
            && let Some(store_time) = date
        {
            let offset = store_time - Utc::now();
            if (offset - self.clock_offset.get()).abs() >= MIN_CLOCK_CORRECTION {
                self.clock_offset.set(offset);
                return ObjectStoreError::Unavailable(format!(
                    "{}; the clocks differ by {}s, which the next request corrects",
                    detail(status, error),
                    offset.num_seconds()
                ));
            }
        }
        classify(status, error, subject, &self.bucket)
    }
}

impl ObjectStore for S3Store {
    async fn put(
        &self,
        key: &str,
        body: PutBody,
        options: &PutOptions,
    ) -> Result<(), ObjectStoreError> {
        validate_key(key)?;
        validate_metadata(&options.metadata)?;
        let mut headers: Vec<(String, String)> = options
            .metadata
            .iter()
            .map(|(name, value)| (format!("{METADATA_HEADER_PREFIX}{name}"), value.clone()))
            .collect();
        match options.mode {
            PutMode::Overwrite => {}
            PutMode::IfAbsent => headers.push(("if-none-match".to_owned(), "*".to_owned())),
        }
        if let Some(crc32c) = options.crc32c {
            headers.push((
                CHECKSUM_HEADER.to_owned(),
                BASE64.encode(crc32c.to_be_bytes()),
            ));
        }
        // An HTTP client sends a body of unknown length chunked, which S3
        // refuses, so the length goes out as a header.
        let unsigned = [("content-length", body.len().to_string())];
        let body_failure = Rc::new(RefCell::new(None));
        let body = if body.is_empty() {
            // An empty body has no last byte to hold back, so it must end
            // without an error before the request goes out.
            let mut pieces = body.into_pieces();
            while let Some(piece) = pieces.next().await {
                piece.map_err(ObjectStoreError::Body)?;
            }
            cyper::Body::empty()
        } else {
            streamed(body, Rc::clone(&body_failure))
        };
        let request = S3Request {
            method: Method::PUT,
            path: self.object_path(key),
            query: Vec::new(),
            headers,
            payload_sha256: UNSIGNED_PAYLOAD,
        };
        let sent = self.send(request, &unsigned, body).await;
        if let Some(error) = body_failure.borrow_mut().take() {
            return Err(ObjectStoreError::Body(error));
        }
        let response = sent?;
        if response.status().is_success() {
            return Ok(());
        }
        let (status, date, error) = read_failure(response).await;
        Err(self.failure(status, date, &error, &Subject::Object(key)))
    }

    async fn get_range(
        &self,
        key: &str,
        range: Range<u64>,
    ) -> Result<ObjectRange, ObjectStoreError> {
        validate_key(key)?;
        validate_range(&range)?;
        let invalid_range = || ObjectStoreError::InvalidRange {
            key: key.to_owned(),
            start: range.start,
            end: range.end,
        };
        if range.start > MAX_RANGE_OFFSET {
            // Past the end of any object, but a missing key is still
            // `NotFound`.
            self.head(key).await?;
            return Err(invalid_range());
        }
        let last = (range.end - 1).min(MAX_RANGE_OFFSET);
        let request = S3Request {
            method: Method::GET,
            path: self.object_path(key),
            query: Vec::new(),
            headers: vec![("range".to_owned(), format!("bytes={}-{last}", range.start))],
            payload_sha256: EMPTY_PAYLOAD_SHA256,
        };
        let response = self.send(request, &[], cyper::Body::empty()).await?;
        let status = response.status();
        if !status.is_success() {
            let (status, date, error) = read_failure(response).await;
            let subject = Subject::Range {
                key,
                range: range.clone(),
            };
            return Err(self.failure(status, date, &error, &subject));
        }
        let etag = etag(response.headers())?;
        let size = match status {
            StatusCode::PARTIAL_CONTENT => content_range_size(response.headers())?,
            // A store that ignores the range sends the whole object.
            StatusCode::OK => header_number(response.headers(), &CONTENT_LENGTH)?,
            status => {
                return Err(ObjectStoreError::Malformed(format!(
                    "a ranged GET answered {status}"
                )));
            }
        };
        if range.start >= size {
            return Err(invalid_range());
        }
        let expected = range.end.min(size) - range.start;
        let bytes = response.bytes().await.map_err(transport)?;
        let bytes = if status == StatusCode::OK {
            let start = usize::try_from(range.start).unwrap_or(usize::MAX);
            let end = usize::try_from(range.end.min(size)).unwrap_or(usize::MAX);
            bytes.slice(start.min(bytes.len())..end.min(bytes.len()))
        } else {
            bytes
        };
        if bytes.len() as u64 != expected {
            return Err(ObjectStoreError::Malformed(format!(
                "a ranged GET of {}..{} answered {} bytes of a {size}-byte object",
                range.start,
                range.end,
                bytes.len()
            )));
        }
        Ok(ObjectRange { bytes, etag, size })
    }

    async fn head(&self, key: &str) -> Result<ObjectMeta, ObjectStoreError> {
        validate_key(key)?;
        let request = S3Request {
            method: Method::HEAD,
            path: self.object_path(key),
            query: Vec::new(),
            headers: vec![("x-amz-checksum-mode".to_owned(), "ENABLED".to_owned())],
            payload_sha256: EMPTY_PAYLOAD_SHA256,
        };
        let response = self.send(request, &[], cyper::Body::empty()).await?;
        if !response.status().is_success() {
            let (status, date, error) = read_failure(response).await;
            return Err(self.failure(status, date, &error, &Subject::Object(key)));
        }
        let headers = response.headers();
        Ok(ObjectMeta {
            size: header_number(headers, &CONTENT_LENGTH)?,
            etag: etag(headers)?,
            last_modified: header_time(headers)?,
            metadata: user_metadata(headers),
            crc32c: header_crc32c(headers)?,
        })
    }

    async fn delete(&self, keys: &[String]) -> Result<DeleteOutcome, ObjectStoreError> {
        validate_delete(keys)?;
        let mut outcome = DeleteOutcome::default();
        let mut valid = Vec::with_capacity(keys.len());
        for key in keys {
            match validate_key(key) {
                Ok(()) => valid.push(key.as_str()),
                Err(error) => outcome.failed.push(DeleteFailure {
                    key: key.clone(),
                    error,
                }),
            }
        }
        if valid.is_empty() {
            return Ok(outcome);
        }
        let body = xml::delete_request(valid);
        let payload_sha256 = sigv4::sha256_hex(body.as_bytes());
        // A batch delete needs a digest of its body, and Content-MD5 is the
        // one that every store with the S3 API takes.
        let request = S3Request {
            method: Method::POST,
            path: self.bucket_root(),
            query: vec![("delete", "")],
            headers: vec![
                (
                    "content-md5".to_owned(),
                    BASE64.encode(md5::compute(body.as_bytes()).0),
                ),
                ("content-type".to_owned(), "application/xml".to_owned()),
            ],
            payload_sha256: &payload_sha256,
        };
        let response = self.send(request, &[], cyper::Body::from(body)).await?;
        if !response.status().is_success() {
            let (status, date, error) = read_failure(response).await;
            return Err(self.failure(status, date, &error, &Subject::Bucket));
        }
        let answer = response.bytes().await.map_err(transport)?;
        for refused in xml::parse_delete(&answer)? {
            // A missing key counts as deleted.
            if refused.code == "NoSuchKey" {
                continue;
            }
            outcome.failed.push(DeleteFailure {
                error: key_failure(&refused),
                key: refused.key,
            });
        }
        Ok(outcome)
    }

    async fn list(&self, request: &ListRequest<'_>) -> Result<ListPage, ObjectStoreError> {
        validate_prefix(request.prefix)?;
        let max_keys = request.page_len().to_string();
        let mut query = vec![
            ("list-type", "2"),
            ("max-keys", max_keys.as_str()),
            ("encoding-type", "url"),
        ];
        if !request.prefix.is_empty() {
            query.push(("prefix", request.prefix));
        }
        if let Some(token) = request.continuation {
            query.push(("continuation-token", token));
        }
        let listing = S3Request {
            method: Method::GET,
            path: self.bucket_root(),
            query,
            headers: Vec::new(),
            payload_sha256: EMPTY_PAYLOAD_SHA256,
        };
        let response = self.send(listing, &[], cyper::Body::empty()).await?;
        if !response.status().is_success() {
            let (status, date, error) = read_failure(response).await;
            return Err(self.failure(status, date, &error, &Subject::Bucket));
        }
        xml::parse_list(&response.bytes().await.map_err(transport)?)
    }
}

impl Subject<'_> {
    const fn key(&self) -> Option<&str> {
        match self {
            Self::Object(key) | Self::Range { key, .. } => Some(key),
            Self::Bucket => None,
        }
    }
}

/// The pieces of `body` as a request body. A failure of the body is left in
/// `failure`, so the put returns the body's own error, not the one that the
/// client makes of it.
fn streamed(body: PutBody, failure: Rc<RefCell<Option<io::Error>>>) -> cyper::Body {
    let pieces = body.into_pieces().map(move |piece| {
        piece.map_err(|error| {
            let kind = error.kind();
            *failure.borrow_mut() = Some(error);
            cyper::Error::System(io::Error::from(kind))
        })
    });
    // The client polls the body on the shard thread that sends it.
    cyper::Body::stream(SendWrapper::new(pieces))
}

/// The status, the `Date` header and the error body of a failed answer.
async fn read_failure(
    response: cyper::Response,
) -> (StatusCode, Option<DateTime<Utc>>, xml::ErrorBody) {
    let status = response.status();
    let date = response
        .headers()
        .get(DATE)
        .and_then(|value| value.to_str().ok())
        .and_then(|value| DateTime::parse_from_rfc2822(value).ok())
        .map(|date| date.with_timezone(&Utc));
    let body = response.bytes().await.unwrap_or_default();
    (status, date, xml::parse_error(&body))
}

/// The error that `status` and the error body mean for a call about
/// `subject`.
fn classify(
    status: StatusCode,
    error: &xml::ErrorBody,
    subject: &Subject<'_>,
    bucket: &str,
) -> ObjectStoreError {
    let detail = || detail(status, error);
    let key = || subject.key().unwrap_or_default().to_owned();
    match (status.as_u16(), error.code.as_str()) {
        (404, "NoSuchBucket") => ObjectStoreError::BucketNotFound(bucket.to_owned()),
        (404, _) if subject.key().is_some() => ObjectStoreError::NotFound { key: key() },
        (404, _) => ObjectStoreError::BucketNotFound(bucket.to_owned()),
        (412, _) if subject.key().is_some() => ObjectStoreError::PreconditionFailed { key: key() },
        (416, _) => match subject {
            Subject::Range { key, range } => ObjectStoreError::InvalidRange {
                key: (*key).to_owned(),
                start: range.start,
                end: range.end,
            },
            Subject::Object(_) | Subject::Bucket => ObjectStoreError::Malformed(detail()),
        },
        (403, "InvalidObjectState") if subject.key().is_some() => {
            ObjectStoreError::Archived { key: key() }
        }
        (400, "BadDigest" | "XAmzContentChecksumMismatch" | "XAmzContentSHA256Mismatch")
            if subject.key().is_some() =>
        {
            ObjectStoreError::ChecksumMismatch { key: key() }
        }
        // A body that changed on the way.
        (400, "BadDigest" | "RequestTimeout") => ObjectStoreError::Transport(detail()),
        // AWS asks for a retry of a conditional write that raced another.
        (409, "ConditionalRequestConflict" | "OperationAborted")
        | (429 | 503, _)
        | (_, "SlowDown" | "Throttling" | "RequestLimitExceeded") => {
            ObjectStoreError::Throttled(detail())
        }
        (
            400,
            "AuthorizationHeaderMalformed" | "ExpiredToken" | "InvalidAccessKeyId" | "InvalidToken",
        )
        | (401 | 403, _) => ObjectStoreError::Auth(detail()),
        (500..=599, _) => ObjectStoreError::Unavailable(detail()),
        (300..=399, _) => ObjectStoreError::InvalidRequest(format!(
            "{}: the bucket needs another region, endpoint or URL style",
            detail()
        )),
        (400..=499, _) => ObjectStoreError::InvalidRequest(detail()),
        _ => ObjectStoreError::Malformed(detail()),
    }
}

/// The error of one key that a batch delete refused.
fn key_failure(refused: &xml::DeleteError) -> ObjectStoreError {
    let detail = format!("{}: {}", refused.code, refused.message);
    match refused.code.as_str() {
        "AccessDenied" | "InvalidAccessKeyId" | "SignatureDoesNotMatch" => {
            ObjectStoreError::Auth(detail)
        }
        "SlowDown" | "Throttling" => ObjectStoreError::Throttled(detail),
        "InvalidObjectState" => ObjectStoreError::Archived {
            key: refused.key.clone(),
        },
        // An error that names nothing the caller can change, such as
        // `InternalError`, is worth another batch.
        _ => ObjectStoreError::Unavailable(detail),
    }
}

fn detail(status: StatusCode, error: &xml::ErrorBody) -> String {
    match (error.code.is_empty(), error.message.is_empty()) {
        (true, _) => status.to_string(),
        (false, true) => format!("{status} {}", error.code),
        (false, false) => format!("{status} {}: {}", error.code, error.message),
    }
}

fn etag(headers: &HeaderMap) -> Result<String, ObjectStoreError> {
    header_text(headers, &ETAG).map(|etag| xml::unquote_etag(etag).to_owned())
}

fn header_text<'a>(
    headers: &'a HeaderMap,
    name: &http::HeaderName,
) -> Result<&'a str, ObjectStoreError> {
    headers
        .get(name)
        .ok_or_else(|| ObjectStoreError::Malformed(format!("an answer without {name}")))?
        .to_str()
        .map_err(|_| ObjectStoreError::Malformed(format!("{name} is not ASCII")))
}

fn header_number(headers: &HeaderMap, name: &http::HeaderName) -> Result<u64, ObjectStoreError> {
    let text = header_text(headers, name)?;
    text.parse()
        .map_err(|_| ObjectStoreError::Malformed(format!("{name} {text:?} is not a number")))
}

/// The size of the whole object from `Content-Range: bytes a-b/size`.
fn content_range_size(headers: &HeaderMap) -> Result<u64, ObjectStoreError> {
    let text = header_text(headers, &CONTENT_RANGE)?;
    text.rsplit_once('/')
        .and_then(|(_, size)| size.parse().ok())
        .ok_or_else(|| ObjectStoreError::Malformed(format!("Content-Range {text:?}")))
}

fn header_time(headers: &HeaderMap) -> Result<SystemTime, ObjectStoreError> {
    let text = header_text(headers, &LAST_MODIFIED)?;
    DateTime::parse_from_rfc2822(text)
        .map(SystemTime::from)
        .map_err(|_| ObjectStoreError::Malformed(format!("Last-Modified {text:?}")))
}

fn user_metadata(headers: &HeaderMap) -> UserMetadata {
    headers
        .iter()
        .filter_map(|(name, value)| {
            let name = name.as_str().strip_prefix(METADATA_HEADER_PREFIX)?;
            Some((name.to_owned(), value.to_str().ok()?.to_owned()))
        })
        .collect()
}

/// The CRC32C that the store keeps for the object, `None` when it keeps
/// none.
fn header_crc32c(headers: &HeaderMap) -> Result<Option<u32>, ObjectStoreError> {
    let Some(value) = headers.get(CHECKSUM_HEADER) else {
        return Ok(None);
    };
    let malformed = || ObjectStoreError::Malformed(format!("{CHECKSUM_HEADER} {value:?}"));
    let bytes = BASE64.decode(value.as_bytes()).map_err(|_| malformed())?;
    let bytes: [u8; 4] = bytes.try_into().map_err(|_| malformed())?;
    Ok(Some(u32::from_be_bytes(bytes)))
}

/// A client for a shard: host lookups off the shard thread, no redirects,
/// and the system's certificate authorities only for `https`.
fn http_client(scheme: &str) -> Result<cyper::Client, ObjectStoreError> {
    let builder = cyper::Client::builder()
        .custom_resolver(ShardResolver::default())
        .redirect(cyper::redirect::Policy::none());
    let builder = if scheme == "https" {
        builder.use_rustls_default()
    } else {
        // Without a config, the client loads the system's certificate
        // authorities when it is built, and fails on a host without them,
        // although it never dials https.
        builder.use_rustls(Arc::new(plain_http_tls_config()?))
    };
    builder.build().map_err(transport)
}

/// A TLS config that trusts no authority, for a client that dials only plain
/// http.
fn plain_http_tls_config() -> Result<rustls::ClientConfig, ObjectStoreError> {
    let provider = Arc::new(rustls::crypto::ring::default_provider());
    Ok(rustls::ClientConfig::builder_with_provider(provider)
        .with_safe_default_protocol_versions()
        .map_err(|error| ObjectStoreError::Transport(error.to_string()))?
        .with_root_certificates(rustls::RootCertStore::empty())
        .with_no_client_auth())
}

#[allow(clippy::needless_pass_by_value)]
fn transport(error: cyper::Error) -> ObjectStoreError {
    ObjectStoreError::Transport(error.to_string())
}

#[cfg(test)]
mod tests {
    use http::HeaderValue;

    use super::*;

    /// Whether an error is the one that a case expects.
    type Expected = fn(&ObjectStoreError) -> bool;

    fn error_body(code: &str) -> xml::ErrorBody {
        xml::ErrorBody {
            code: code.to_owned(),
            message: String::new(),
        }
    }

    fn classified(status: u16, code: &str, subject: &Subject<'_>) -> ObjectStoreError {
        classify(
            StatusCode::from_u16(status).unwrap(),
            &error_body(code),
            subject,
            "segments",
        )
    }

    #[test]
    fn given_store_answers_when_classifying_should_map_each_to_its_error() {
        let object = Subject::Object("a/b");
        let range = Subject::Range {
            key: "a/b",
            range: 5..9,
        };
        let bucket = Subject::Bucket;
        let cases: [(u16, &str, &Subject<'_>, Expected); 19] = [
            (
                404,
                "NoSuchKey",
                &object,
                |error| matches!(error, ObjectStoreError::NotFound { key } if key == "a/b"),
            ),
            (404, "", &object, |error| {
                matches!(error, ObjectStoreError::NotFound { .. })
            }),
            (
                404,
                "NoSuchBucket",
                &object,
                |error| matches!(error, ObjectStoreError::BucketNotFound(bucket) if bucket == "segments"),
            ),
            (404, "", &bucket, |error| {
                matches!(error, ObjectStoreError::BucketNotFound(_))
            }),
            (
                412,
                "PreconditionFailed",
                &object,
                |error| matches!(error, ObjectStoreError::PreconditionFailed { key } if key == "a/b"),
            ),
            (416, "InvalidRange", &range, |error| {
                matches!(
                    error,
                    ObjectStoreError::InvalidRange { key, start: 5, end: 9 } if key == "a/b"
                )
            }),
            (403, "InvalidObjectState", &object, |error| {
                matches!(error, ObjectStoreError::Archived { .. })
            }),
            (
                400,
                "BadDigest",
                &object,
                |error| matches!(error, ObjectStoreError::ChecksumMismatch { key } if key == "a/b"),
            ),
            (400, "XAmzContentChecksumMismatch", &object, |error| {
                matches!(error, ObjectStoreError::ChecksumMismatch { .. })
            }),
            (400, "BadDigest", &bucket, |error| {
                matches!(error, ObjectStoreError::Transport(_))
            }),
            (409, "ConditionalRequestConflict", &object, |error| {
                matches!(error, ObjectStoreError::Throttled(_))
            }),
            (503, "SlowDown", &object, |error| {
                matches!(error, ObjectStoreError::Throttled(_))
            }),
            (500, "InternalError", &bucket, |error| {
                matches!(error, ObjectStoreError::Unavailable(_))
            }),
            (403, "SignatureDoesNotMatch", &object, |error| {
                matches!(error, ObjectStoreError::Auth(_))
            }),
            (400, "ExpiredToken", &bucket, |error| {
                matches!(error, ObjectStoreError::Auth(_))
            }),
            (400, "RequestTimeout", &object, |error| {
                matches!(error, ObjectStoreError::Transport(_))
            }),
            (301, "PermanentRedirect", &bucket, |error| {
                matches!(error, ObjectStoreError::InvalidRequest(_))
            }),
            (400, "InvalidArgument", &object, |error| {
                matches!(error, ObjectStoreError::InvalidRequest(_))
            }),
            (200, "", &object, |error| {
                matches!(error, ObjectStoreError::Malformed(_))
            }),
        ];
        for (status, code, subject, expected) in cases {
            let error = classified(status, code, subject);
            assert!(expected(&error), "{status} {code}: {error:?}");
        }
    }

    #[test]
    fn given_refused_keys_when_mapping_should_keep_the_retry_class() {
        let refused = |code: &str| xml::DeleteError {
            key: "a".to_owned(),
            code: code.to_owned(),
            message: String::new(),
        };

        assert!(matches!(
            key_failure(&refused("AccessDenied")),
            ObjectStoreError::Auth(_)
        ));
        assert!(matches!(
            key_failure(&refused("InternalError")),
            ObjectStoreError::Unavailable(_)
        ));
        assert!(matches!(
            key_failure(&refused("SlowDown")),
            ObjectStoreError::Throttled(_)
        ));
    }

    #[test]
    fn given_answer_headers_when_reading_should_parse_sizes_times_and_checksums() {
        let mut headers = HeaderMap::new();
        headers.insert(CONTENT_RANGE, HeaderValue::from_static("bytes 250-255/256"));
        headers.insert(ETAG, HeaderValue::from_static("\"abc\""));
        headers.insert(
            LAST_MODIFIED,
            HeaderValue::from_static("Wed, 28 Oct 2026 00:00:00 GMT"),
        );
        headers.insert(
            CHECKSUM_HEADER,
            HeaderValue::from_str(&BASE64.encode(0xdead_beef_u32.to_be_bytes())).unwrap(),
        );
        headers.insert("x-amz-meta-start-offset", HeaderValue::from_static("0"));
        headers.insert("x-amz-request-id", HeaderValue::from_static("id"));

        assert_eq!(content_range_size(&headers).unwrap(), 256);
        assert_eq!(etag(&headers).unwrap(), "abc");
        assert_eq!(
            header_time(&headers).unwrap(),
            SystemTime::UNIX_EPOCH + std::time::Duration::from_hours(498_096)
        );
        assert_eq!(header_crc32c(&headers).unwrap(), Some(0xdead_beef));
        assert_eq!(
            user_metadata(&headers),
            UserMetadata::from([("start-offset".to_owned(), "0".to_owned())])
        );
        assert_eq!(header_crc32c(&HeaderMap::new()).unwrap(), None);
        assert!(matches!(
            content_range_size(&HeaderMap::new()),
            Err(ObjectStoreError::Malformed(_))
        ));
    }

    /// A store whose refresher has fetched nothing yet fails each call as
    /// transient, before any request, so the caller retries.
    #[test]
    fn given_no_credentials_yet_when_calling_should_fail_as_transient_before_any_request() {
        let store = S3Store::new(S3Config {
            endpoint: "http://127.0.0.1:1".to_owned(),
            region: "us-east-1".to_owned(),
            bucket: "segments".to_owned(),
            url_style: UrlStyle::Path,
            credentials: SharedCredentials::refreshed(),
        })
        .unwrap();

        let result = crate::contract::shard_runtime().block_on(store.head("a/b"));

        assert!(
            matches!(&result, Err(ObjectStoreError::Unavailable(reason)) if reason.contains("no credentials")),
            "{result:?}"
        );
    }

    #[test]
    fn given_expired_credentials_when_classifying_should_wait_for_a_refresh_only_when_refreshed() {
        let store = |credentials: SharedCredentials| {
            S3Store::new(S3Config {
                endpoint: "http://127.0.0.1:1".to_owned(),
                region: "us-east-1".to_owned(),
                bucket: "segments".to_owned(),
                url_style: UrlStyle::Path,
                credentials,
            })
            .unwrap()
        };
        let expired = error_body("ExpiredToken");
        let status = StatusCode::BAD_REQUEST;
        let subject = Subject::Object("a/b");

        let refreshed =
            store(SharedCredentials::refreshed()).failure(status, None, &expired, &subject);
        let fixed = store(
            Credentials {
                access_key_id: "id".to_owned(),
                secret_access_key: "secret".to_owned(),
                session_token: Some("token".to_owned()),
            }
            .into(),
        )
        .failure(status, None, &expired, &subject);

        assert!(
            matches!(refreshed, ObjectStoreError::Unavailable(_)),
            "{refreshed:?}"
        );
        assert!(matches!(fixed, ObjectStoreError::Auth(_)), "{fixed:?}");
    }

    #[test]
    fn given_endpoints_when_building_should_refuse_what_cannot_address_a_bucket() {
        let config = |endpoint: &str| S3Config {
            endpoint: endpoint.to_owned(),
            region: "us-east-1".to_owned(),
            bucket: "segments".to_owned(),
            url_style: UrlStyle::Path,
            credentials: Credentials {
                access_key_id: "id".to_owned(),
                secret_access_key: "secret".to_owned(),
                session_token: None,
            }
            .into(),
        };

        for endpoint in [
            "not a url",
            "ftp://store",
            "http://store/prefix",
            "http://store/?a=b",
            "http://user:pass@store",
        ] {
            assert!(
                matches!(
                    S3Store::new(config(endpoint)),
                    Err(ObjectStoreError::InvalidRequest(_))
                ),
                "{endpoint}"
            );
        }
        let path_style = S3Store::new(config("http://localhost:4566")).unwrap();
        assert_eq!(path_style.origin, "http://localhost:4566");
        assert_eq!(path_style.object_path("a b/c"), "/segments/a%20b/c");
        let virtual_hosted = S3Store::new(S3Config {
            url_style: UrlStyle::VirtualHosted,
            ..config("http://s3.example:9000")
        })
        .unwrap();
        assert_eq!(virtual_hosted.host, "segments.s3.example:9000");
        assert_eq!(virtual_hosted.object_path("a"), "/a");
        assert_eq!(virtual_hosted.bucket_root(), "/");
    }
}
