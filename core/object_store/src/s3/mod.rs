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
use std::ops::Range;
use std::rc::Rc;
use std::time::{Duration, SystemTime};

use base64::Engine;
use base64::engine::general_purpose::STANDARD as BASE64;
use chrono::{DateTime, TimeDelta, Utc};
use futures::StreamExt;
use http::header::{CONTENT_LENGTH, CONTENT_RANGE, DATE, ETAG, LAST_MODIFIED};
use http::{HeaderMap, Method, StatusCode};
use url::Url;

pub use credentials::SharedCredentials;
pub use sigv4::Credentials;

use crate::error::ObjectStoreError;
use crate::http::{streamed, transport, within};
use crate::layout::KeyLayout;
use crate::store::{
    DeleteFailure, DeleteOutcome, ListPage, ListRequest, ObjectMeta, ObjectRange, ObjectStore,
    PartsLimits, PartsUpload, PutBody, PutMode, PutOptions, StoredPart, SweptUploads, UserMetadata,
    validate_delete, validate_key, validate_metadata, validate_prefix, validate_range,
};
use sigv4::{EMPTY_PAYLOAD_SHA256, SigningRequest, UNSIGNED_PAYLOAD};

const METADATA_HEADER_PREFIX: &str = "x-amz-meta-";
const CHECKSUM_HEADER: &str = "x-amz-checksum-crc32c";
const CHECKSUM_TYPE_HEADER: &str = "x-amz-checksum-type";
/// The checksum type that makes the CRC32C of an object of a multipart upload
/// the CRC32C of its bytes, which the store checks at completion and reports
/// on a head, as for an object of one put.
const FULL_OBJECT: &str = "FULL_OBJECT";
/// The code of a conditional write that raced another one.
const CONDITIONAL_CONFLICT: &str = "ConditionalRequestConflict";
/// The fewest bytes of a part other than the last: 5 MiB.
const MIN_PART_LEN: u64 = 5 * 1024 * 1024;
/// The most parts of one multipart upload.
const MAX_PARTS: u32 = 10_000;
/// The most uploads that one page of a sweep lists and aborts, so that a page
/// takes about as long as any other call.
const SWEEP_PAGE_LEN: usize = 100;
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
///   [`crate::ShardResolver`].
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
            client: crate::http::client(scheme)?,
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

    /// `AbortMultipartUpload` of `upload_id` under `key`.
    async fn abort_upload(&self, key: &str, upload_id: &str) -> Result<(), ObjectStoreError> {
        let request = S3Request {
            method: Method::DELETE,
            path: self.object_path(key),
            query: vec![("uploadId", upload_id)],
            headers: Vec::new(),
            payload_sha256: EMPTY_PAYLOAD_SHA256,
        };
        let response = self.send(request, &[], cyper::Body::empty()).await?;
        if response.status().is_success() {
            return Ok(());
        }
        let (status, date, error) = read_failure(response).await;
        Err(self.failure(status, date, &error, &Subject::Object(key)))
    }

    /// Signs and sends `request` with `body` as an unsigned payload. A
    /// failure of the body is returned as [`ObjectStoreError::Body`], not as
    /// the error that the client makes of it.
    async fn send_body(
        &self,
        request: S3Request<'_>,
        body: PutBody,
    ) -> Result<cyper::Response, ObjectStoreError> {
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
            streamed(body.into_pieces(), Rc::clone(&body_failure))
        };
        let sent = self.send(request, &unsigned, body).await;
        if let Some(error) = body_failure.borrow_mut().take() {
            return Err(ObjectStoreError::Body(error));
        }
        sent
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
            headers.push((CHECKSUM_HEADER.to_owned(), encode_crc32c(crc32c)));
        }
        let request = S3Request {
            method: Method::PUT,
            path: self.object_path(key),
            query: Vec::new(),
            headers,
            payload_sha256: UNSIGNED_PAYLOAD,
        };
        let response = self.send_body(request, body).await?;
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

    fn parts_limits(&self) -> Option<PartsLimits> {
        Some(PartsLimits {
            min_part_len: MIN_PART_LEN,
            max_parts: MAX_PARTS,
        })
    }

    /// `CreateMultipartUpload` with the user metadata and a full object
    /// CRC32C. A store that refuses either, or multipart uploads, as not
    /// implemented or as an invalid argument is
    /// [`ObjectStoreError::Unsupported`].
    async fn begin_parts(
        &self,
        key: &str,
        options: &PutOptions,
        _layout: &KeyLayout,
    ) -> Result<PartsUpload, ObjectStoreError> {
        validate_key(key)?;
        validate_metadata(&options.metadata)?;
        let mut headers: Vec<(String, String)> = options
            .metadata
            .iter()
            .map(|(name, value)| (format!("{METADATA_HEADER_PREFIX}{name}"), value.clone()))
            .collect();
        headers.extend([
            ("x-amz-checksum-algorithm".to_owned(), "CRC32C".to_owned()),
            (CHECKSUM_TYPE_HEADER.to_owned(), FULL_OBJECT.to_owned()),
        ]);
        let request = S3Request {
            method: Method::POST,
            path: self.object_path(key),
            query: vec![("uploads", "")],
            headers,
            payload_sha256: EMPTY_PAYLOAD_SHA256,
        };
        let response = self.send(request, &[], cyper::Body::empty()).await?;
        if !response.status().is_success() {
            let (status, date, error) = read_failure(response).await;
            if refuses_multipart(status, &error) {
                return Err(ObjectStoreError::Unsupported(format!(
                    "multipart uploads with a full object CRC32C: {}",
                    detail(status, &error)
                )));
            }
            return Err(self.failure(status, date, &error, &Subject::Object(key)));
        }
        let upload_id = xml::parse_initiate(&response.bytes().await.map_err(transport)?)?;
        Ok(PartsUpload {
            key: key.to_owned(),
            id: upload_id,
        })
    }

    /// `UploadPart` with the part's CRC32C. The answer's etag goes into the
    /// completion.
    async fn put_part(
        &self,
        upload: &PartsUpload,
        number: u32,
        body: PutBody,
        crc32c: u32,
    ) -> Result<StoredPart, ObjectStoreError> {
        let part_number = number.to_string();
        let request = S3Request {
            method: Method::PUT,
            path: self.object_path(&upload.key),
            query: vec![
                ("partNumber", part_number.as_str()),
                ("uploadId", upload.id.as_str()),
            ],
            headers: vec![(CHECKSUM_HEADER.to_owned(), encode_crc32c(crc32c))],
            payload_sha256: UNSIGNED_PAYLOAD,
        };
        let response = self.send_body(request, body).await?;
        if !response.status().is_success() {
            let (status, date, error) = read_failure(response).await;
            return Err(self.failure(status, date, &error, &Subject::Object(&upload.key)));
        }
        Ok(StoredPart {
            number,
            tag: etag(response.headers())?,
            crc32c,
        })
    }

    /// `CompleteMultipartUpload` with `If-None-Match: *` for a create-only
    /// put, the full object CRC32C and the object's size, all of which the
    /// store checks. A 200 succeeds only with a `CompleteMultipartUploadResult`
    /// that carries an etag. An error in its body is classified by its code,
    /// and a body that holds neither is [`ObjectStoreError::Unavailable`].
    /// Both can follow a completion that created the object. A
    /// `ConditionalRequestConflict` ends the upload for S3, which asks for a
    /// new one, so it is [`ObjectStoreError::UploadConflict`].
    async fn complete_parts(
        &self,
        upload: &PartsUpload,
        parts: &[StoredPart],
        options: &PutOptions,
        len: u64,
    ) -> Result<(), ObjectStoreError> {
        let key = upload.key.as_str();
        let body = xml::complete_request(parts, encode_crc32c);
        let payload_sha256 = sigv4::sha256_hex(body.as_bytes());
        let mut headers = vec![
            ("content-type".to_owned(), "application/xml".to_owned()),
            (CHECKSUM_TYPE_HEADER.to_owned(), FULL_OBJECT.to_owned()),
            ("x-amz-mp-object-size".to_owned(), len.to_string()),
        ];
        if let Some(crc32c) = options.crc32c {
            headers.push((CHECKSUM_HEADER.to_owned(), encode_crc32c(crc32c)));
        }
        match options.mode {
            PutMode::Overwrite => {}
            PutMode::IfAbsent => headers.push(("if-none-match".to_owned(), "*".to_owned())),
        }
        let request = S3Request {
            method: Method::POST,
            path: self.object_path(key),
            query: vec![("uploadId", upload.id.as_str())],
            headers,
            payload_sha256: &payload_sha256,
        };
        let response = self.send(request, &[], cyper::Body::from(body)).await?;
        if !response.status().is_success() {
            let (status, date, error) = read_failure(response).await;
            if error.code == CONDITIONAL_CONFLICT {
                return Err(ObjectStoreError::UploadConflict {
                    key: key.to_owned(),
                });
            }
            return Err(self.failure(status, date, &error, &Subject::Object(key)));
        }
        let answer = response.bytes().await.map_err(transport)?;
        match xml::parse_complete(&answer) {
            Ok(xml::Completion::Created) => Ok(()),
            Ok(xml::Completion::Failed(error)) if error.code == CONDITIONAL_CONFLICT => {
                Err(ObjectStoreError::UploadConflict {
                    key: key.to_owned(),
                })
            }
            Ok(xml::Completion::Failed(error)) => Err(embedded_failure(
                &error,
                &Subject::Object(key),
                &self.bucket,
            )),
            Err(unread) => Err(ObjectStoreError::Unavailable(format!(
                "a 200 answer to the completion of {key:?} holds neither its result nor an \
                 error: {unread}"
            ))),
        }
    }

    /// `AbortMultipartUpload`. An upload that is gone, aborted or completed,
    /// counts as aborted.
    async fn abort_parts(&self, upload: &PartsUpload, _parts: u32) -> Result<(), ObjectStoreError> {
        match self.abort_upload(&upload.key, &upload.id).await {
            Err(ObjectStoreError::NotFound { .. }) => Ok(()),
            aborted => aborted,
        }
    }

    /// Lists the multipart uploads under the root, and aborts those that the
    /// layout builds the keys of and that began before the cutoff, by the
    /// store's clock as the answers' `Date` headers corrected it. An abort
    /// that gets no answer in time counts as failed, and the page goes on.
    async fn sweep_uploads(
        &self,
        layout: &KeyLayout,
        older_than: Duration,
        after: Option<&str>,
        request_timeout: Duration,
    ) -> Result<SweptUploads, ObjectStoreError> {
        let prefix = format!("{}/", layout.root());
        let max_uploads = SWEEP_PAGE_LEN.to_string();
        let mut query = vec![
            ("uploads", ""),
            ("prefix", prefix.as_str()),
            ("max-uploads", max_uploads.as_str()),
            ("encoding-type", "url"),
        ];
        let markers = after.and_then(|after| after.split_once(' '));
        if let Some((upload_id, key)) = markers {
            query.extend([("key-marker", key), ("upload-id-marker", upload_id)]);
        }
        let listing = S3Request {
            method: Method::GET,
            path: self.bucket_root(),
            query,
            headers: Vec::new(),
            payload_sha256: EMPTY_PAYLOAD_SHA256,
        };
        let page = within(request_timeout, "the listing of multipart uploads", async {
            let response = self.send(listing, &[], cyper::Body::empty()).await?;
            if !response.status().is_success() {
                let (status, date, error) = read_failure(response).await;
                return Err(self.failure(status, date, &error, &Subject::Bucket));
            }
            xml::parse_uploads(&response.bytes().await.map_err(transport)?)
        })
        .await?;
        let store_now = SystemTime::from(Utc::now() + self.clock_offset.get());
        let cutoff = store_now
            .checked_sub(older_than)
            .unwrap_or(SystemTime::UNIX_EPOCH);
        let mut swept = SweptUploads {
            next: page
                .next
                .map(|(key, upload_id)| format!("{upload_id} {key}")),
            ..SweptUploads::default()
        };
        for upload in page.uploads {
            if !layout.owns_key(&upload.key) {
                continue;
            }
            if upload.initiated >= cutoff {
                swept.kept += 1;
                continue;
            }
            let abort = self.abort_upload(&upload.key, &upload.upload_id);
            match within(request_timeout, "an abort of a multipart upload", abort).await {
                Ok(()) | Err(ObjectStoreError::NotFound { .. }) => swept.removed += 1,
                Err(_) => swept.failed += 1,
            }
        }
        Ok(swept)
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

/// Whether a refusal of `CreateMultipartUpload` says that the store does not
/// take multipart uploads, or not with a full object CRC32C, rather than that
/// something about this call is wrong.
fn refuses_multipart(status: StatusCode, error: &xml::ErrorBody) -> bool {
    matches!(
        (status.as_u16(), error.code.as_str()),
        (501, _) | (_, "NotImplemented") | (400, "InvalidArgument" | "InvalidRequest")
    )
}

/// The error that the body of a 200 answer to `CompleteMultipartUpload`
/// holds. The status was sent before the failure, so the code alone decides,
/// as a server-side failure where it names nothing else.
fn embedded_failure(
    error: &xml::ErrorBody,
    subject: &Subject<'_>,
    bucket: &str,
) -> ObjectStoreError {
    let status = match error.code.as_str() {
        "PreconditionFailed" => StatusCode::PRECONDITION_FAILED,
        "NoSuchUpload" | "NoSuchKey" | "NoSuchBucket" => StatusCode::NOT_FOUND,
        "ConditionalRequestConflict" | "OperationAborted" => StatusCode::CONFLICT,
        "BadDigest" | "InvalidPart" | "InvalidPartOrder" | "EntityTooSmall" | "InvalidRequest" => {
            StatusCode::BAD_REQUEST
        }
        _ => StatusCode::INTERNAL_SERVER_ERROR,
    };
    classify(status, error, subject, bucket)
}

/// The CRC32C of the S3 headers and bodies: the big-endian value in base64.
fn encode_crc32c(crc32c: u32) -> String {
    BASE64.encode(crc32c.to_be_bytes())
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
    // A multipart upload without a full object checksum reports a checksum
    // of the parts' checksums, with a `-` and the number of parts after it,
    // which no CRC32C of the bytes equals.
    let composite = headers
        .get(CHECKSUM_TYPE_HEADER)
        .is_some_and(|kind| kind.as_bytes().eq_ignore_ascii_case(b"COMPOSITE"));
    if composite || value.as_bytes().contains(&b'-') {
        return Ok(None);
    }
    let malformed = || ObjectStoreError::Malformed(format!("{CHECKSUM_HEADER} {value:?}"));
    let bytes = BASE64.decode(value.as_bytes()).map_err(|_| malformed())?;
    let bytes: [u8; 4] = bytes.try_into().map_err(|_| malformed())?;
    Ok(Some(u32::from_be_bytes(bytes)))
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use bytes::Bytes;
    use http::HeaderValue;

    use super::*;
    use crate::error::ErrorClass;
    use crate::http::test_endpoints::{Answer, Recorded, Recorder};
    use crate::store::PUT_ID_METADATA;

    /// Whether an error is the one that a case expects.
    type Expected = fn(&ObjectStoreError) -> bool;

    /// What a fake store answers to the request of a case.
    type Answers = fn(&Recorded) -> Answer;

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

    fn static_store(endpoint: &str) -> S3Store {
        S3Store::new(S3Config {
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
        })
        .unwrap()
    }

    fn put_to(store: &S3Store) -> Result<(), ObjectStoreError> {
        let options = PutOptions {
            mode: PutMode::IfAbsent,
            ..PutOptions::default()
        };
        crate::contract::shard_runtime().block_on(store.put(
            "a/b",
            PutBody::from_bytes("segment bytes"),
            &options,
        ))
    }

    /// A refused connection proves that the store never saw the put, so a
    /// retry needs no check for an object of its own.
    #[test]
    fn given_store_that_refuses_the_connection_when_putting_should_fail_as_never_sent() {
        let store = static_store(&crate::http::test_endpoints::refusing());

        let result = put_to(&store);

        assert!(
            matches!(&result, Err(ObjectStoreError::NotSent(_))),
            "{result:?}"
        );
        assert!(!result.unwrap_err().put_may_have_stored());
    }

    #[test]
    fn given_store_that_hangs_up_after_the_request_when_putting_should_fail_as_unknown() {
        let store = static_store(&crate::http::test_endpoints::hanging_up());

        let result = put_to(&store);

        assert!(
            matches!(&result, Err(ObjectStoreError::Transport(_))),
            "{result:?}"
        );
        assert!(result.unwrap_err().put_may_have_stored());
    }

    /// Drops a put of `body` to `silent` once it ran for a moment, and says
    /// whether the store then saw the connection close.
    fn closes_when_dropped(silent: &crate::http::test_endpoints::Silent, body: PutBody) -> bool {
        let store = static_store(&silent.endpoint);
        let options = PutOptions::default();
        crate::contract::shard_runtime().block_on(async {
            let put = Box::pin(store.put("a/b", body, &options));
            let wait = std::pin::pin!(compio::time::sleep(Duration::from_millis(200)));
            let raced = futures::future::select(put, wait).await;
            let futures::future::Either::Right(((), put)) = raced else {
                panic!("the store answered a put it never answers");
            };
            drop(put);
            // The connection task runs on this runtime, so it must close the
            // connection before the runtime goes.
            compio::time::sleep(Duration::from_millis(200)).await;
            silent.client_closed(Duration::from_secs(2)).await
        })
    }

    /// The first attempt of a put that a second one passed is dropped, and the
    /// store must not keep waiting for it on an open connection.
    #[test]
    fn given_a_put_waiting_for_its_answer_when_dropped_should_close_the_connection() {
        let silent = crate::http::test_endpoints::Silent::start(true);

        assert!(closes_when_dropped(
            &silent,
            PutBody::from_bytes("segment bytes")
        ));
    }

    #[test]
    fn given_a_put_whose_store_stopped_reading_the_body_when_dropped_should_close_the_connection() {
        let silent = crate::http::test_endpoints::Silent::start(false);
        // More than the buffers of both sockets hold, so the client waits to
        // write when it is dropped.
        let body = PutBody::from_bytes_in_pieces(vec![7_u8; 64 << 20], 1 << 20);

        assert!(closes_when_dropped(&silent, body));
    }

    const UPLOAD: &str = "VXBsb2FkIElE";
    const PUT_ID: &str = "0000000000000000000000000000002a";

    fn upload(key: &str) -> PartsUpload {
        PartsUpload {
            key: key.to_owned(),
            id: UPLOAD.to_owned(),
        }
    }

    fn run<T>(call: impl std::future::Future<Output = T>) -> T {
        crate::contract::shard_runtime().block_on(call)
    }

    fn error_answer(status: u16, code: &str) -> Answer {
        let body = format!("<Error><Code>{code}</Code><Message>m</Message></Error>");
        (status, Vec::new(), body.into_bytes())
    }

    #[test]
    fn given_an_upload_in_parts_when_beginning_should_ask_for_a_full_object_crc32c_with_the_metadata()
     {
        let fake = Recorder::start(|_| {
            let body = format!(
                "<InitiateMultipartUploadResult><Bucket>segments</Bucket><Key>a/b</Key>\
                 <UploadId>{UPLOAD}</UploadId></InitiateMultipartUploadResult>"
            );
            (200, Vec::new(), body.into_bytes())
        });
        let store = static_store(&fake.endpoint);
        let options = PutOptions {
            mode: PutMode::IfAbsent,
            metadata: UserMetadata::from([(PUT_ID_METADATA.to_owned(), PUT_ID.to_owned())]),
            crc32c: Some(7),
            ..PutOptions::default()
        };

        let begun = run(store.begin_parts("a/b", &options, &KeyLayout::new("iggy").unwrap()));

        assert_eq!(begun.unwrap(), upload("a/b"));
        let [request] = fake.requests().try_into().unwrap();
        assert_eq!(request.method, "POST");
        assert_eq!(request.target, "/segments/a/b?uploads=");
        assert_eq!(request.headers["x-amz-checksum-algorithm"], "CRC32C");
        assert_eq!(request.headers["x-amz-checksum-type"], "FULL_OBJECT");
        assert_eq!(request.headers["x-amz-meta-put-id"], PUT_ID);
        assert!(!request.headers.contains_key("if-none-match"));
    }

    #[test]
    fn given_refusals_of_a_begin_when_classifying_should_fall_back_only_on_what_says_unsupported() {
        let cases: [(Answers, Expected); 4] = [
            (
                |_| error_answer(501, "NotImplemented"),
                |error| matches!(error, ObjectStoreError::Unsupported(_)),
            ),
            (
                |_| error_answer(400, "InvalidArgument"),
                |error| matches!(error, ObjectStoreError::Unsupported(_)),
            ),
            (
                |_| error_answer(403, "AccessDenied"),
                |error| matches!(error, ObjectStoreError::Auth(_)),
            ),
            (
                |_| error_answer(503, "SlowDown"),
                |error| matches!(error, ObjectStoreError::Throttled(_)),
            ),
        ];
        for (answer, expected) in cases {
            let fake = Recorder::start(answer);
            let store = static_store(&fake.endpoint);

            let begun = run(store.begin_parts(
                "a/b",
                &PutOptions::default(),
                &KeyLayout::new("iggy").unwrap(),
            ));

            let error = begun.unwrap_err();
            assert!(expected(&error), "{error:?}");
        }
    }

    #[test]
    fn given_a_part_when_putting_should_send_its_number_upload_and_crc32c_and_keep_its_etag() {
        let fake =
            Recorder::start(|_| (200, vec![("etag", "\"part-etag\"".to_owned())], Vec::new()));
        let store = static_store(&fake.endpoint);
        let bytes = Bytes::from_static(b"part bytes");
        let crc32c = crc32c::crc32c(&bytes);

        let stored = run(store.put_part(
            &upload("a/b"),
            2,
            PutBody::from_bytes(bytes.clone()),
            crc32c,
        ));

        assert_eq!(
            stored.unwrap(),
            StoredPart {
                number: 2,
                tag: "part-etag".to_owned(),
                crc32c,
            }
        );
        let [request] = fake.requests().try_into().unwrap();
        assert_eq!(request.method, "PUT");
        assert_eq!(
            request.target,
            format!("/segments/a/b?partNumber=2&uploadId={UPLOAD}")
        );
        assert_eq!(
            request.headers["x-amz-checksum-crc32c"],
            encode_crc32c(crc32c)
        );
        assert_eq!(request.body, bytes);
    }

    fn parts() -> Vec<StoredPart> {
        vec![
            StoredPart {
                number: 1,
                tag: "e1".to_owned(),
                crc32c: 1,
            },
            StoredPart {
                number: 2,
                tag: "e2".to_owned(),
                crc32c: 2,
            },
        ]
    }

    #[test]
    fn given_parts_when_completing_a_create_only_put_should_send_the_condition_crc32c_size_and_parts()
     {
        let fake = Recorder::start(|_| {
            let body = "<CompleteMultipartUploadResult><Key>a/b</Key><ETag>\"x-2\"</ETag>\
                        </CompleteMultipartUploadResult>";
            (200, Vec::new(), body.as_bytes().to_vec())
        });
        let store = static_store(&fake.endpoint);
        let options = PutOptions {
            mode: PutMode::IfAbsent,
            crc32c: Some(0xdead_beef),
            ..PutOptions::default()
        };

        run(store.complete_parts(&upload("a/b"), &parts(), &options, 12)).unwrap();

        let [request] = fake.requests().try_into().unwrap();
        assert_eq!(request.method, "POST");
        assert_eq!(request.target, format!("/segments/a/b?uploadId={UPLOAD}"));
        assert_eq!(request.headers["if-none-match"], "*");
        assert_eq!(
            request.headers["x-amz-checksum-crc32c"],
            encode_crc32c(0xdead_beef)
        );
        assert_eq!(request.headers["x-amz-checksum-type"], "FULL_OBJECT");
        assert_eq!(request.headers["x-amz-mp-object-size"], "12");
        assert_eq!(
            String::from_utf8(request.body).unwrap(),
            format!(
                r#"<?xml version="1.0" encoding="UTF-8"?><CompleteMultipartUpload xmlns="http://s3.amazonaws.com/doc/2006-03-01/"><Part><PartNumber>1</PartNumber><ETag>"e1"</ETag><ChecksumCRC32C>{}</ChecksumCRC32C></Part><Part><PartNumber>2</PartNumber><ETag>"e2"</ETag><ChecksumCRC32C>{}</ChecksumCRC32C></Part></CompleteMultipartUpload>"#,
                encode_crc32c(1),
                encode_crc32c(2)
            )
        );
    }

    #[test]
    fn given_an_overwrite_when_completing_should_send_no_condition() {
        let fake = Recorder::start(|_| {
            (
                200,
                Vec::new(),
                b"<CompleteMultipartUploadResult><ETag>\"x-2\"</ETag></CompleteMultipartUploadResult>"
                    .to_vec(),
            )
        });
        let store = static_store(&fake.endpoint);

        run(store.complete_parts(&upload("a/b"), &parts(), &PutOptions::default(), 12)).unwrap();

        let [request] = fake.requests().try_into().unwrap();
        assert!(!request.headers.contains_key("if-none-match"));
        assert!(!request.headers.contains_key("x-amz-checksum-crc32c"));
    }

    #[test]
    fn given_answers_to_a_completion_when_classifying_should_map_each_and_read_errors_in_a_200() {
        let cases: [(Answers, Expected); 8] = [
            (
                |_| error_answer(412, "PreconditionFailed"),
                |error| matches!(error, ObjectStoreError::PreconditionFailed { key } if key == "a/b"),
            ),
            (
                |_| error_answer(404, "NoSuchUpload"),
                |error| matches!(error, ObjectStoreError::NotFound { .. }),
            ),
            (
                |_| error_answer(409, "ConditionalRequestConflict"),
                |error| matches!(error, ObjectStoreError::UploadConflict { key } if key == "a/b"),
            ),
            (
                |_| {
                    let body = "<Error><Code>ConditionalRequestConflict</Code></Error>";
                    (200, Vec::new(), body.as_bytes().to_vec())
                },
                |error| matches!(error, ObjectStoreError::UploadConflict { .. }),
            ),
            (
                |_| error_answer(409, "OperationAborted"),
                |error| matches!(error, ObjectStoreError::Throttled(_)),
            ),
            (
                |_| error_answer(400, "BadDigest"),
                |error| matches!(error, ObjectStoreError::ChecksumMismatch { .. }),
            ),
            (
                |_| error_answer(200, "InternalError"),
                |error| matches!(error, ObjectStoreError::Unavailable(_)),
            ),
            (
                |_| {
                    let body = "<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n\n<Error>\
                                <Code>PreconditionFailed</Code></Error>";
                    (200, Vec::new(), body.as_bytes().to_vec())
                },
                |error| matches!(error, ObjectStoreError::PreconditionFailed { .. }),
            ),
        ];
        for (answer, expected) in cases {
            let fake = Recorder::start(answer);
            let store = static_store(&fake.endpoint);

            let completed =
                run(store.complete_parts(&upload("a/b"), &parts(), &PutOptions::default(), 12));

            let error = completed.unwrap_err();
            assert!(expected(&error), "{error:?}");
        }
    }

    #[test]
    fn given_an_error_root_with_attributes_in_a_200_when_completing_should_classify_its_code() {
        let fake = Recorder::start(|_| {
            let body = "<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n<Error \
                        xmlns=\"http://s3.amazonaws.com/doc/2006-03-01/\">\
                        <Code>PreconditionFailed</Code></Error>";
            (200, Vec::new(), body.as_bytes().to_vec())
        });
        let store = static_store(&fake.endpoint);

        let completed =
            run(store.complete_parts(&upload("a/b"), &parts(), &PutOptions::default(), 12));

        let error = completed.unwrap_err();
        assert!(
            matches!(&error, ObjectStoreError::PreconditionFailed { key } if key == "a/b"),
            "{error:?}"
        );
    }

    /// The store sent its status before it finished, so a 200 that names no
    /// outcome can follow a completion that created the object, and the
    /// caller's check of the object under the key settles it.
    #[test]
    fn given_a_200_without_a_result_when_completing_should_fail_as_one_that_may_have_created_the_object()
     {
        let cases: [Answers; 4] = [
            |_| (200, Vec::new(), Vec::new()),
            |_| {
                (
                    200,
                    Vec::new(),
                    b"<CompleteMultipartUploadResult/>".to_vec(),
                )
            },
            |_| (200, Vec::new(), b"<html>bad gateway</html>".to_vec()),
            |_| {
                let body = "<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n\n   ";
                (200, Vec::new(), body.as_bytes().to_vec())
            },
        ];
        for answer in cases {
            let fake = Recorder::start(answer);
            let store = static_store(&fake.endpoint);

            let completed =
                run(store.complete_parts(&upload("a/b"), &parts(), &PutOptions::default(), 12));

            let error = completed.unwrap_err();
            assert!(
                matches!(error, ObjectStoreError::Unavailable(_)),
                "{error:?}"
            );
            assert_eq!(error.class(), ErrorClass::Transient);
            assert!(error.put_may_have_stored());
        }
    }

    #[test]
    fn given_an_upload_when_aborting_should_delete_it_and_take_one_that_is_gone() {
        let fake = Recorder::start(|_| (204, Vec::new(), Vec::new()));
        let store = static_store(&fake.endpoint);

        run(store.abort_parts(&upload("a/b"), 3)).unwrap();

        let [request] = fake.requests().try_into().unwrap();
        assert_eq!(request.method, "DELETE");
        assert_eq!(request.target, format!("/segments/a/b?uploadId={UPLOAD}"));
        let gone = Recorder::start(|_| error_answer(404, "NoSuchUpload"));
        assert!(run(static_store(&gone.endpoint).abort_parts(&upload("a/b"), 3)).is_ok());
    }

    /// Answers a listing of uploads with an old and a young upload of the
    /// root, and an old one of a longer root, and every abort with 204.
    fn uploads_listing(request: &Recorded) -> Answer {
        if request.method == "DELETE" {
            return (204, Vec::new(), Vec::new());
        }
        let at = |age: TimeDelta| {
            (Utc::now() - age).to_rfc3339_opts(chrono::SecondsFormat::Millis, true)
        };
        let ours = "iggy/1/2/3/4/00000000000000000000-00000000000000000099.log";
        let longer = "iggy/b/1/2/3/4/00000000000000000000-00000000000000000099.log";
        let body = format!(
            "<ListMultipartUploadsResult><IsTruncated>false</IsTruncated>\
             <Upload><Key>{ours}</Key><UploadId>old</UploadId><Initiated>{}</Initiated></Upload>\
             <Upload><Key>{ours}</Key><UploadId>young</UploadId><Initiated>{}</Initiated></Upload>\
             <Upload><Key>{longer}</Key><UploadId>other-root</UploadId><Initiated>{}</Initiated></Upload>\
             </ListMultipartUploadsResult>",
            at(TimeDelta::hours(2)),
            at(TimeDelta::minutes(5)),
            at(TimeDelta::hours(2)),
        );
        (200, Vec::new(), body.into_bytes())
    }

    #[test]
    fn given_uploads_under_the_root_when_sweeping_should_abort_only_the_old_ones_of_this_root() {
        let fake = Recorder::start(uploads_listing);
        let store = static_store(&fake.endpoint);
        let layout = KeyLayout::new("iggy").unwrap();

        let swept = run(store.sweep_uploads(
            &layout,
            Duration::from_secs(3600),
            None,
            Duration::from_secs(30),
        ))
        .unwrap();

        assert_eq!(
            swept,
            SweptUploads {
                removed: 1,
                kept: 1,
                failed: 0,
                next: None,
            }
        );
        let requests = fake.requests();
        assert!(
            requests[0].target.starts_with("/segments?")
                && requests[0].target.contains("uploads=")
                && requests[0].target.contains("prefix=iggy%2F"),
            "{}",
            requests[0].target
        );
        let aborted: Vec<&str> = requests
            .iter()
            .filter(|request| request.method == "DELETE")
            .map(|request| request.target.as_str())
            .collect();
        assert_eq!(
            aborted,
            ["/segments/iggy/1/2/3/4/00000000000000000000-00000000000000000099.log?uploadId=old"]
        );
    }

    /// Lists two old uploads, and answers the abort of the first only after
    /// 1.5 s. The endpoint takes one connection at a time, so the abort of
    /// the second waits for it and is answered about 0.5 s after it left.
    fn uploads_with_a_stalled_abort(request: &Recorded) -> Answer {
        if request.method == "DELETE" {
            if request.target.ends_with("uploadId=stalled") {
                std::thread::sleep(Duration::from_millis(1500));
            }
            return (204, Vec::new(), Vec::new());
        }
        let old =
            (Utc::now() - TimeDelta::hours(2)).to_rfc3339_opts(chrono::SecondsFormat::Millis, true);
        let key = "iggy/1/2/3/4/00000000000000000000-00000000000000000099.log";
        let body = format!(
            "<ListMultipartUploadsResult><IsTruncated>false</IsTruncated>\
             <Upload><Key>{key}</Key><UploadId>stalled</UploadId><Initiated>{old}</Initiated></Upload>\
             <Upload><Key>{key}</Key><UploadId>old</UploadId><Initiated>{old}</Initiated></Upload>\
             </ListMultipartUploadsResult>"
        );
        (200, Vec::new(), body.into_bytes())
    }

    #[test]
    fn given_an_abort_that_stalls_when_sweeping_should_count_it_failed_and_abort_the_rest() {
        let fake = Recorder::start(uploads_with_a_stalled_abort);
        let store = static_store(&fake.endpoint);
        let layout = KeyLayout::new("iggy").unwrap();

        let swept = run(store.sweep_uploads(
            &layout,
            Duration::from_secs(3600),
            None,
            Duration::from_secs(1),
        ))
        .unwrap();

        assert_eq!((swept.removed, swept.kept, swept.failed), (1, 0, 1));
        let aborted: Vec<String> = fake
            .requests()
            .iter()
            .filter(|request| request.method == "DELETE")
            .map(|request| request.target.clone())
            .collect();
        assert!(
            aborted
                .last()
                .is_some_and(|target| target.ends_with("uploadId=old")),
            "{aborted:?}"
        );
    }

    #[test]
    fn given_checksums_of_multipart_objects_when_reading_a_head_should_take_only_a_full_object_one()
    {
        let headers = |pairs: &[(&'static str, &str)]| {
            let mut headers = HeaderMap::new();
            for (name, value) in pairs {
                headers.insert(*name, HeaderValue::from_str(value).unwrap());
            }
            headers
        };
        let full = encode_crc32c(0xdead_beef);

        assert_eq!(
            header_crc32c(&headers(&[
                (CHECKSUM_HEADER, &full),
                (CHECKSUM_TYPE_HEADER, "FULL_OBJECT")
            ]))
            .unwrap(),
            Some(0xdead_beef)
        );
        assert_eq!(
            header_crc32c(&headers(&[(CHECKSUM_HEADER, &full)])).unwrap(),
            Some(0xdead_beef)
        );
        assert_eq!(
            header_crc32c(&headers(&[
                (CHECKSUM_HEADER, "3q2+7w==-3"),
                (CHECKSUM_TYPE_HEADER, "COMPOSITE")
            ]))
            .unwrap(),
            None
        );
        assert_eq!(
            header_crc32c(&headers(&[(CHECKSUM_HEADER, "3q2+7w==-3")])).unwrap(),
            None
        );
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
