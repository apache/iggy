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

//! The Google Cloud Storage provider, over the JSON API.
//!
//! The JSON API takes a conditional create as `ifGenerationMatch=0` on a
//! single-request upload, and the provider uses nothing else of Google: no
//! client library, no S3 interoperability mode. The XML API would take the
//! same condition as a header, but only the JSON API answers a missing
//! bucket with a body on every call and lists from a start key.

pub mod credentials;
mod json;

use std::cell::{Cell, RefCell};
use std::fmt::Write as _;
use std::io;
use std::ops::Range;
use std::rc::Rc;
use std::time::{Duration, SystemTime};

use bytes::Bytes;
use chrono::DateTime;
use futures::stream::{self, StreamExt};
use http::header::{CONTENT_LENGTH, CONTENT_RANGE, DATE};
use http::{HeaderMap, Method, StatusCode};
use percent_encoding::{AsciiSet, NON_ALPHANUMERIC, utf8_percent_encode};
use url::Url;

pub use credentials::{AccessToken, GcsAuth, SharedToken};

use crate::error::ObjectStoreError;
use crate::http::{streamed, transport, within};
use crate::layout::KeyLayout;
use crate::store::{
    DeleteFailure, DeleteOutcome, ListPage, ListRequest, MAX_KEYS_PER_REQUEST, ObjectMeta,
    ObjectRange, ObjectStore, PUT_ID_METADATA, PartsLimits, PartsUpload, PutBody, PutMode,
    PutOptions, StoredPart, SweptUploads, UserMetadata, validate_delete, validate_key,
    validate_metadata, validate_prefix, validate_range,
};
use json::{ErrorBody, ObjectResource, UploadMetadata};

/// The JSON API of Google Cloud Storage.
pub const DEFAULT_ENDPOINT: &str = "https://storage.googleapis.com";

/// How many deletes of one batch are in flight at once. The API has no
/// batch delete but a multipart batch endpoint, so each key is one request.
pub const DELETE_CONCURRENCY: usize = 16;

/// A path segment or a query value keeps letters, digits and `-._~`, and
/// an object name keeps no `/`: the API takes the name as one segment.
const COMPONENT: &AsciiSet = &NON_ALPHANUMERIC
    .remove(b'-')
    .remove(b'.')
    .remove(b'_')
    .remove(b'~');

/// The largest offset that a `Range` header carries, as for S3. No object is
/// that large.
const MAX_RANGE_OFFSET: u64 = i64::MAX.unsigned_abs();
const GENERATION_HEADER: &str = "x-goog-generation";
/// How many pages a listing follows by their token, within one call, while
/// they come back empty. The API can answer an empty page with a token.
const MAX_EMPTY_PAGES: usize = 16;
/// The most source objects of one compose, so the most parts of an upload.
pub const MAX_COMPOSE_SOURCES: u32 = 32;
/// The most versions of temporary objects that one page of a sweep, an
/// abort or a release lists and deletes.
const SWEEP_PAGE_LEN: usize = 100;
/// The most pages that an abort or a release of one upload lists. An upload
/// holds at most [`MAX_COMPOSE_SOURCES`] temporary objects and its composite.
const MAX_UPLOAD_PAGES: usize = 8;
/// The host of Google Cloud Storage and its regional and private endpoints.
const GOOGLE_HOST_SUFFIX: &str = ".googleapis.com";

/// Where a [`GcsStore`] sends its requests, and how it authorizes them.
#[derive(Debug, Clone)]
pub struct GcsConfig {
    /// The base URL of the JSON API, without a path: [`DEFAULT_ENDPOINT`],
    /// or the URL of an emulator, such as `http://localhost:4443`.
    pub endpoint: String,
    pub bucket: String,
    pub auth: GcsAuth,
}

/// An [`ObjectStore`] in one GCS bucket, over one HTTP client.
///
/// - A put is one `uploadType=multipart` request: a JSON part with the name,
///   the user metadata and the CRC32C, then the body, streamed with an
///   explicit `content-length`. `ifGenerationMatch=0` makes it a
///   conditional create, and the store checks the CRC32C and keeps it,
///   which `head` reports. The provider also runs the CRC32C over the body
///   as it goes and withholds the last byte on a mismatch.
/// - The etag is the generation of the object, which changes with every
///   put. A ranged read takes it from `x-goog-generation`.
/// - A listing asks for the objects from a start key, and resumes after the
///   last key of the page, so it never depends on a page token.
/// - A delete sends one request per key, [`DELETE_CONCURRENCY`] at once.
/// - An upload in parts keeps each part as a temporary object, composes them
///   into a temporary composite whose CRC32C it checks, and then composes
///   that one source into the object with the put's condition. A compose
///   checks no CRC32C of its own, so no unchecked byte ever sits under the
///   key. Only an `https` endpoint of Google takes uploads in parts by
///   default: an emulator such as fake-gcs-server composes over an object
///   whatever `ifGenerationMatch` says, so a create-only compose there could
///   replace one, see [`GcsStore::with_uploads_in_parts`].
/// - The client follows no redirect, and lookups of the store's host never
///   block the shard, see [`crate::ShardResolver`].
pub struct GcsStore {
    client: cyper::Client,
    /// `scheme://host[:port]` of every request.
    origin: String,
    /// The bucket, encoded as a path segment.
    bucket_segment: String,
    bucket: String,
    auth: GcsAuth,
    /// Whether [`ObjectStore::parts_limits`] offers uploads in parts.
    uploads_in_parts: bool,
}

/// One page of the versions of the objects under a prefix.
struct Versions {
    items: Vec<ObjectResource>,
    /// The name that the next page starts at, `None` after the last page.
    next: Option<String>,
    /// The store's clock when it answered, from the `Date` header.
    store_now: Option<SystemTime>,
}

/// What a failed call was about, for the error that it returns.
enum Subject<'a> {
    Object(&'a str),
    Upload(&'a str),
    Range { key: &'a str, range: Range<u64> },
    Bucket,
}

/// The request of one put, but for its body: the parts around the body, and
/// where they go.
struct Upload {
    url: String,
    content_type: String,
    /// Everything before the bytes of the body.
    head: Bytes,
    /// Everything after them.
    tail: Bytes,
}

impl GcsStore {
    /// A store over a client of its own. The endpoint's host is looked up on
    /// the first request.
    ///
    /// # Errors
    /// [`ObjectStoreError::InvalidRequest`] for an endpoint that is not an
    /// `http` or `https` URL without a path, or an empty bucket, and
    /// [`ObjectStoreError::Transport`] when the client cannot be built.
    pub fn new(config: GcsConfig) -> Result<Self, ObjectStoreError> {
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
        if config.bucket.trim().is_empty() || config.bucket.contains('/') {
            return Err(ObjectStoreError::InvalidRequest(format!(
                "bucket {:?} is not a bucket name",
                config.bucket
            )));
        }
        let google = scheme == "https"
            && (host == "storage.googleapis.com" || host.ends_with(GOOGLE_HOST_SUFFIX));
        Ok(Self {
            client: crate::http::client(scheme)?,
            origin: format!("{scheme}://{authority}"),
            bucket_segment: encode(&config.bucket),
            bucket: config.bucket,
            auth: config.auth,
            uploads_in_parts: google,
        })
    }

    /// The store with uploads in parts on or off. They are on only for an
    /// `https` endpoint of Google by default, because only GCS itself
    /// refuses a compose with `ifGenerationMatch=0` over an object. Turn
    /// them on for another endpoint only when it decides that condition
    /// atomically, as the tests of the provider do on an emulator for the
    /// cases that never meet an object under the key.
    #[must_use]
    pub fn with_uploads_in_parts(mut self, on: bool) -> Self {
        self.uploads_in_parts = on;
        self
    }

    /// Creates the bucket, and succeeds when it exists already. For tests
    /// against an emulator: an operator creates a production bucket, in its
    /// project and with its own policies.
    ///
    /// # Errors
    /// Any error of the store.
    pub async fn create_bucket(&self) -> Result<(), ObjectStoreError> {
        let body = serde_json::json!({ "name": self.bucket }).to_string();
        let headers = [("content-type", "application/json".to_owned())];
        let url = format!("{}/storage/v1/b", self.origin);
        let response = self
            .send(Method::POST, &url, &headers, cyper::Body::from(body))
            .await?;
        if response.status().is_success() || response.status() == StatusCode::CONFLICT {
            return Ok(());
        }
        let (status, error) = read_failure(response).await;
        Err(self.failure(status, &error, &Subject::Bucket))
    }

    fn object_url(&self, key: &str) -> String {
        format!(
            "{}/storage/v1/b/{}/o/{}",
            self.origin,
            self.bucket_segment,
            encode(key)
        )
    }

    fn media_url(&self, key: &str) -> String {
        format!("{}?alt=media", self.object_url(key))
    }

    /// The request of a put under `key`, but for the body.
    fn upload(&self, key: &str, options: &PutOptions) -> Upload {
        let mut url = format!(
            "{}/upload/storage/v1/b/{}/o?uploadType=multipart&name={}",
            self.origin,
            self.bucket_segment,
            encode(key)
        );
        match options.mode {
            PutMode::Overwrite => {}
            PutMode::IfAbsent => url.push_str("&ifGenerationMatch=0"),
        }
        let metadata = UploadMetadata {
            name: key,
            metadata: &options.metadata,
            crc32c: options.crc32c.map(json::encode_crc32c),
        };
        // Serializing strings and a map of strings cannot fail.
        let metadata = serde_json::to_string(&metadata).unwrap_or_default();
        // 128 random bits make a boundary that no body holds by chance.
        let boundary = format!("iggy-{:032x}", rand::random::<u128>());
        let head = format!(
            "--{boundary}\r\ncontent-type: application/json; charset=UTF-8\r\n\r\n{metadata}\r\n\
             --{boundary}\r\ncontent-type: application/octet-stream\r\n\r\n"
        );
        let tail = format!("\r\n--{boundary}--\r\n");
        Upload {
            url,
            content_type: format!("multipart/related; boundary={boundary}"),
            head: Bytes::from(head),
            tail: Bytes::from(tail),
        }
    }

    /// The URL of one page of the objects under `prefix` from `start`, the
    /// key that the page starts at or after.
    fn list_url(
        &self,
        prefix: &str,
        start: Option<&str>,
        max_results: usize,
        page_token: Option<&str>,
    ) -> String {
        let mut url = format!(
            "{}/storage/v1/b/{}/o?maxResults={max_results}",
            self.origin, self.bucket_segment
        );
        let parameters = [
            ("prefix", Some(prefix).filter(|prefix| !prefix.is_empty())),
            ("startOffset", start),
            ("pageToken", page_token),
        ];
        for (name, value) in parameters {
            if let Some(value) = value {
                // Writing to a `String` cannot fail.
                let _ = write!(url, "&{name}={}", encode(value));
            }
        }
        url
    }

    /// Sends a request with the token of the node.
    async fn send(
        &self,
        method: Method,
        url: &str,
        headers: &[(&str, String)],
        body: cyper::Body,
    ) -> Result<cyper::Response, ObjectStoreError> {
        let authorization = self.authorization()?;
        let mut builder = self.client.request(method, url).map_err(transport)?;
        let headers = headers
            .iter()
            .map(|(name, value)| (*name, value.as_str()))
            .chain(
                authorization
                    .as_deref()
                    .map(|value| ("authorization", value)),
            );
        for (name, value) in headers {
            builder = builder.header(name, value).map_err(|error| {
                ObjectStoreError::InvalidRequest(format!("header {name}: {error}"))
            })?;
        }
        builder.body(body).send().await.map_err(transport)
    }

    /// The `Authorization` header, `None` for an anonymous store.
    fn authorization(&self) -> Result<Option<String>, ObjectStoreError> {
        match &self.auth {
            GcsAuth::Anonymous => Ok(None),
            GcsAuth::Token(shared) => {
                let token = shared.current().ok_or_else(|| {
                    ObjectStoreError::Unavailable(
                        "the node has no access token for the store yet, and fetches one"
                            .to_owned(),
                    )
                })?;
                Ok(Some(format!("Bearer {}", token.secret())))
            }
        }
    }

    /// The error of an answer that is not a success. A token that the store
    /// refuses is one that expired or was revoked, and the next refresh ends
    /// that.
    fn failure(
        &self,
        status: StatusCode,
        error: &ErrorBody,
        subject: &Subject<'_>,
    ) -> ObjectStoreError {
        if status == StatusCode::UNAUTHORIZED && matches!(self.auth, GcsAuth::Token(_)) {
            return ObjectStoreError::Unavailable(format!(
                "{}; the node refreshes its access token",
                detail(status, error)
            ));
        }
        classify(status, error, subject, &self.bucket)
    }

    /// One upload of `body` under `key`, and the store's answer when it
    /// committed the object.
    async fn upload_object(
        &self,
        key: &str,
        body: PutBody,
        options: &PutOptions,
    ) -> Result<cyper::Response, ObjectStoreError> {
        validate_key(key)?;
        validate_metadata(&options.metadata)?;
        let len = body.len();
        let upload = self.upload(key, options);
        let content_length = upload.head.len() as u64 + len + upload.tail.len() as u64;
        let headers = [
            ("content-type", upload.content_type),
            ("content-length", content_length.to_string()),
        ];
        let mismatch = Rc::new(Cell::new(false));
        let body_failure = Rc::new(RefCell::new(None));
        let pieces = if body.is_empty() {
            // An empty body has no last byte to hold back, so it must end
            // without an error before the request goes out.
            let mut pieces = body.into_pieces();
            while let Some(piece) = pieces.next().await {
                piece.map_err(ObjectStoreError::Body)?;
            }
            if options.crc32c.is_some_and(|expected| expected != 0) {
                return Err(ObjectStoreError::ChecksumMismatch {
                    key: key.to_owned(),
                });
            }
            stream::iter(Vec::new()).left_stream()
        } else {
            checked(body, options.crc32c, Rc::clone(&mismatch)).right_stream()
        };
        let parts = stream::iter([Ok(upload.head)])
            .chain(pieces)
            .chain(stream::iter([Ok(upload.tail)]));
        let sent = self
            .send(
                Method::POST,
                &upload.url,
                &headers,
                streamed(parts, Rc::clone(&body_failure)),
            )
            .await;
        if mismatch.get() {
            return Err(ObjectStoreError::ChecksumMismatch {
                key: key.to_owned(),
            });
        }
        if let Some(error) = body_failure.borrow_mut().take() {
            return Err(ObjectStoreError::Body(error));
        }
        let response = sent?;
        if response.status().is_success() {
            return Ok(response);
        }
        let (status, error) = read_failure(response).await;
        Err(self.failure(status, &error, &Subject::Upload(key)))
    }

    /// Deletes version `generation` of the object under `key` for good, also
    /// on a bucket that keeps noncurrent versions. A missing version counts
    /// as deleted.
    async fn delete_version(&self, key: &str, generation: u64) -> Result<(), ObjectStoreError> {
        let url = format!("{}?generation={generation}", self.object_url(key));
        let response = self
            .send(Method::DELETE, &url, &[], cyper::Body::empty())
            .await?;
        if response.status().is_success() {
            return Ok(());
        }
        let (status, error) = read_failure(response).await;
        match self.failure(status, &error, &Subject::Object(key)) {
            ObjectStoreError::NotFound { .. } => Ok(()),
            error => Err(error),
        }
    }

    /// One page of every version of the objects under `prefix`, from the
    /// name `start` on. The start is inclusive, so a page that ended in the
    /// middle of the versions of a name lists the rest of them again on the
    /// next page, with those that a caller deleted gone.
    async fn list_versions(
        &self,
        prefix: &str,
        start: Option<&str>,
    ) -> Result<Versions, ObjectStoreError> {
        let url = format!(
            "{}&versions=true",
            self.list_url(prefix, start, SWEEP_PAGE_LEN, None)
        );
        let response = self
            .send(Method::GET, &url, &[], cyper::Body::empty())
            .await?;
        if !response.status().is_success() {
            let (status, error) = read_failure(response).await;
            return Err(self.failure(status, &error, &Subject::Bucket));
        }
        let store_now = response
            .headers()
            .get(DATE)
            .and_then(|value| value.to_str().ok())
            .and_then(|value| DateTime::parse_from_rfc2822(value).ok())
            .map(SystemTime::from);
        let listing = json::parse_list(&response.bytes().await.map_err(transport)?)?;
        let more = listing.next_page_token.is_some() || listing.items.len() >= SWEEP_PAGE_LEN;
        // A page that holds nothing past its start would list itself again.
        let next = listing
            .items
            .last()
            .map(|item| item.name.clone())
            .filter(|last| more && Some(last.as_str()) != start);
        Ok(Versions {
            items: listing.items,
            next,
            store_now,
        })
    }

    /// Deletes every version of every temporary object of `upload`, and
    /// returns how many the store kept.
    async fn remove_upload(&self, upload: &PartsUpload) -> Result<usize, ObjectStoreError> {
        let mut failed = 0;
        let mut start: Option<String> = None;
        for _ in 0..MAX_UPLOAD_PAGES {
            let page = self.list_versions(&upload.id, start.as_deref()).await?;
            failed += stream::iter(&page.items)
                .map(|item| self.delete_version(&item.name, item.generation))
                .buffer_unordered(DELETE_CONCURRENCY)
                .filter(|deleted| std::future::ready(deleted.is_err()))
                .count()
                .await;
            start = page.next;
            if start.is_none() {
                break;
            }
        }
        Ok(failed)
    }

    /// Composes `sources`, each pinned to its generation, into the object
    /// under `destination`, with `mode` as its condition. An error names
    /// `subject`, the key of the upload.
    async fn compose(
        &self,
        destination: &str,
        sources: &[(String, u64)],
        mode: &PutMode,
        metadata: &UserMetadata,
        crc32c: u32,
        subject: &str,
    ) -> Result<ObjectResource, ObjectStoreError> {
        let mut url = format!("{}/compose", self.object_url(destination));
        match mode {
            PutMode::Overwrite => {}
            PutMode::IfAbsent => url.push_str("?ifGenerationMatch=0"),
        }
        let body = compose_request(sources, metadata, crc32c);
        let headers = [("content-type", "application/json".to_owned())];
        let response = self
            .send(Method::POST, &url, &headers, cyper::Body::from(body))
            .await?;
        if !response.status().is_success() {
            let (status, error) = read_failure(response).await;
            return Err(self.failure(status, &error, &Subject::Object(subject)));
        }
        json::parse_object(&response.bytes().await.map_err(transport)?)
    }

    /// Deletes the object under `key`. A missing object counts as deleted.
    async fn delete_one(&self, key: &str) -> Result<(), ObjectStoreError> {
        let response = self
            .send(
                Method::DELETE,
                &self.object_url(key),
                &[],
                cyper::Body::empty(),
            )
            .await?;
        if response.status().is_success() {
            return Ok(());
        }
        let (status, error) = read_failure(response).await;
        match self.failure(status, &error, &Subject::Object(key)) {
            ObjectStoreError::NotFound { .. } => Ok(()),
            error => Err(error),
        }
    }
}

impl ObjectStore for GcsStore {
    async fn put(
        &self,
        key: &str,
        body: PutBody,
        options: &PutOptions,
    ) -> Result<(), ObjectStoreError> {
        let response = self.upload_object(key, body, options).await?;
        // The store committed the object. Reading the answer lets the
        // connection serve the next request.
        let _ = response.bytes().await;
        Ok(())
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
        let headers = [("range", format!("bytes={}-{last}", range.start))];
        let response = self
            .send(
                Method::GET,
                &self.media_url(key),
                &headers,
                cyper::Body::empty(),
            )
            .await?;
        let status = response.status();
        if !status.is_success() {
            let (status, error) = read_failure(response).await;
            let subject = Subject::Range {
                key,
                range: range.clone(),
            };
            return Err(self.failure(status, &error, &subject));
        }
        let etag = header_text(response.headers(), GENERATION_HEADER)?.to_owned();
        let size = match status {
            StatusCode::PARTIAL_CONTENT => content_range_size(response.headers())?,
            // A store that ignores the range sends the whole object.
            StatusCode::OK => header_number(response.headers(), CONTENT_LENGTH.as_str())?,
            status => {
                return Err(ObjectStoreError::Malformed(format!(
                    "a ranged read answered {status}"
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
                "a ranged read of {}..{} answered {} bytes of a {size}-byte object",
                range.start,
                range.end,
                bytes.len()
            )));
        }
        Ok(ObjectRange { bytes, etag, size })
    }

    async fn head(&self, key: &str) -> Result<ObjectMeta, ObjectStoreError> {
        validate_key(key)?;
        let response = self
            .send(
                Method::GET,
                &self.object_url(key),
                &[],
                cyper::Body::empty(),
            )
            .await?;
        if !response.status().is_success() {
            let (status, error) = read_failure(response).await;
            return Err(self.failure(status, &error, &Subject::Object(key)));
        }
        json::parse_object(&response.bytes().await.map_err(transport)?)?.meta()
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
        // Without a token, no request of the batch can go out.
        self.authorization()?;
        let mut deletes = stream::iter(valid)
            .map(|key| async move { (key, self.delete_one(key).await) })
            .buffer_unordered(DELETE_CONCURRENCY);
        while let Some((key, result)) = deletes.next().await {
            if let Err(error) = result {
                outcome.failed.push(DeleteFailure {
                    key: key.to_owned(),
                    error,
                });
            }
        }
        Ok(outcome)
    }

    async fn list(&self, request: &ListRequest<'_>) -> Result<ListPage, ObjectStoreError> {
        validate_prefix(request.prefix)?;
        let page_len = request.page_len();
        // One more than the page holds tells whether more follow, on a store
        // that sends no page token, such as an emulator.
        let max_results = (page_len + 1).min(MAX_KEYS_PER_REQUEST);
        let after = request.continuation;
        let mut page_token: Option<String> = None;
        let mut empty_pages = 0;
        loop {
            let url = self.list_url(request.prefix, after, max_results, page_token.as_deref());
            let response = self
                .send(Method::GET, &url, &[], cyper::Body::empty())
                .await?;
            if !response.status().is_success() {
                let (status, error) = read_failure(response).await;
                return Err(self.failure(status, &error, &Subject::Bucket));
            }
            let listing = json::parse_list(&response.bytes().await.map_err(transport)?)?;
            let may_follow = empty_pages < MAX_EMPTY_PAGES;
            match page(listing, after, page_len, max_results, may_follow)? {
                PageStep::Done(page) => return Ok(page),
                PageStep::Follow(token) => {
                    empty_pages += 1;
                    page_token = Some(token);
                }
            }
        }
    }

    /// Up to [`MAX_COMPOSE_SOURCES`] parts, each a temporary object of its
    /// own, which one compose joins.
    fn parts_limits(&self) -> Option<PartsLimits> {
        self.uploads_in_parts.then_some(PartsLimits {
            min_part_len: 1,
            max_parts: MAX_COMPOSE_SOURCES,
        })
    }

    /// Sends nothing: the upload is the prefix of its temporary objects,
    /// named after the put id of `options`, or after a random id without
    /// one.
    async fn begin_parts(
        &self,
        key: &str,
        options: &PutOptions,
        layout: &KeyLayout,
    ) -> Result<PartsUpload, ObjectStoreError> {
        if !self.uploads_in_parts {
            return Err(ObjectStoreError::Unsupported(format!(
                "uploads in parts at {}, which may compose over an object whatever the \
                 condition says",
                self.origin
            )));
        }
        validate_key(key)?;
        validate_metadata(&options.metadata)?;
        let upload_id = options
            .metadata
            .get(PUT_ID_METADATA)
            .and_then(|id| u128::from_str_radix(id, 16).ok())
            .unwrap_or_else(rand::random);
        Ok(PartsUpload {
            key: key.to_owned(),
            id: layout.upload_prefix(upload_id),
        })
    }

    /// Creates the part as a temporary object, whose CRC32C the store checks,
    /// only where none is: an attempt that lands after a retry finds the part
    /// and replaces nothing. A part that is there already, from an earlier
    /// attempt or an earlier upload of the same put, counts as stored when it
    /// has the length and the CRC32C of this one. Its generation pins it as
    /// a source of the compose.
    async fn put_part(
        &self,
        upload: &PartsUpload,
        number: u32,
        body: PutBody,
        crc32c: u32,
    ) -> Result<StoredPart, ObjectStoreError> {
        let key = temporary_key(upload, number);
        let len = body.len();
        let options = PutOptions {
            mode: PutMode::IfAbsent,
            crc32c: Some(crc32c),
            ..PutOptions::default()
        };
        let generation = match self.upload_object(&key, body, &options).await {
            Ok(response) => {
                json::parse_object(&response.bytes().await.map_err(transport)?)?.generation
            }
            Err(ObjectStoreError::PreconditionFailed { .. }) => {
                let held = self.head(&key).await?;
                if held.size != len || held.crc32c != Some(crc32c) {
                    return Err(ObjectStoreError::Unavailable(format!(
                        "temporary object {key:?} holds other bytes than part {number} of {:?}",
                        upload.key
                    )));
                }
                held.etag.parse().map_err(|_| {
                    ObjectStoreError::Malformed(format!(
                        "temporary object {key:?} has no generation but {:?}",
                        held.etag
                    ))
                })?
            }
            Err(error) => return Err(error),
        };
        Ok(StoredPart {
            number,
            tag: generation.to_string(),
            crc32c,
        })
    }

    /// Composes the temporary objects into a temporary composite, compares
    /// its CRC32C with the expected one, and only then composes the
    /// composite into the object, with `ifGenerationMatch=0` for a
    /// create-only put and the user metadata. A compose takes no CRC32C to
    /// check, so the object never holds bytes that the provider did not
    /// check. The temporary objects stay for [`ObjectStore::release_parts`].
    async fn complete_parts(
        &self,
        upload: &PartsUpload,
        parts: &[StoredPart],
        options: &PutOptions,
        _len: u64,
    ) -> Result<(), ObjectStoreError> {
        let key = upload.key.as_str();
        validate_key(key)?;
        validate_metadata(&options.metadata)?;
        if parts.is_empty() || parts.len() > MAX_COMPOSE_SOURCES as usize {
            return Err(ObjectStoreError::InvalidRequest(format!(
                "a compose takes 1 to {MAX_COMPOSE_SOURCES} parts, not {}",
                parts.len()
            )));
        }
        let Some(expected) = options.crc32c else {
            return Err(ObjectStoreError::InvalidRequest(format!(
                "an upload in parts of {key:?} needs the CRC32C of the whole object"
            )));
        };
        let sources = parts
            .iter()
            .map(|part| {
                let generation = part.tag.parse().map_err(|_| {
                    ObjectStoreError::InvalidRequest(format!(
                        "part {} of {key:?} has no generation but {:?}",
                        part.number, part.tag
                    ))
                })?;
                Ok((temporary_key(upload, part.number), generation))
            })
            .collect::<Result<Vec<_>, ObjectStoreError>>()?;
        let composite_key = temporary_key(upload, COMPOSITE_NUMBER);
        let composite = self
            .compose(
                &composite_key,
                &sources,
                &PutMode::Overwrite,
                &UserMetadata::new(),
                expected,
                key,
            )
            .await?;
        if composed_crc32c(&composite)? != Some(expected) {
            // The composite stays for the abort of the upload.
            return Err(ObjectStoreError::ChecksumMismatch {
                key: key.to_owned(),
            });
        }
        let published = self
            .compose(
                key,
                &[(composite_key, composite.generation)],
                &options.mode,
                &options.metadata,
                expected,
                key,
            )
            .await?;
        // One checked source composes into the same bytes, so a mismatch here
        // is a fault of the store, and the object goes.
        if composed_crc32c(&published)? != Some(expected) {
            self.delete_version(key, published.generation).await?;
            return Err(ObjectStoreError::ChecksumMismatch {
                key: key.to_owned(),
            });
        }
        Ok(())
    }

    /// Deletes every version of the temporary objects of the upload.
    async fn release_parts(&self, upload: &PartsUpload) -> Result<(), ObjectStoreError> {
        match self.remove_upload(upload).await? {
            0 => Ok(()),
            failed => Err(ObjectStoreError::Unavailable(format!(
                "the store kept {failed} temporary objects of {:?}",
                upload.key
            ))),
        }
    }

    /// Deletes every version of the temporary objects of the upload, also
    /// those of attempts that landed after the upload gave them up.
    async fn abort_parts(&self, upload: &PartsUpload, _parts: u32) -> Result<(), ObjectStoreError> {
        self.release_parts(upload).await
    }

    /// Lists every version of the temporary objects of uploads in parts,
    /// and deletes for good those that the store last changed before the
    /// cutoff, by the store's clock as the listing's `Date` header gives it.
    /// A listing without that header removes nothing.
    async fn sweep_uploads(
        &self,
        layout: &KeyLayout,
        older_than: Duration,
        after: Option<&str>,
        request_timeout: Duration,
    ) -> Result<SweptUploads, ObjectStoreError> {
        let prefix = layout.uploads_prefix();
        let page = within(
            request_timeout,
            "the listing of uploads in parts",
            self.list_versions(&prefix, after),
        )
        .await?;
        let Some(store_now) = page.store_now else {
            return Err(ObjectStoreError::Malformed(
                "a listing of uploads in parts came without the store's Date, so the age of \
                 no upload is known"
                    .to_owned(),
            ));
        };
        let cutoff = store_now
            .checked_sub(older_than)
            .unwrap_or(SystemTime::UNIX_EPOCH);
        let mut swept = SweptUploads {
            next: page.next,
            ..SweptUploads::default()
        };
        let mut abandoned = Vec::new();
        for item in page.items {
            if layout.parse_upload_part_key(&item.name).is_none() {
                continue;
            }
            let generation = item.generation;
            let listed = item.listed()?;
            if listed.last_modified >= cutoff {
                swept.kept += 1;
            } else {
                abandoned.push((listed.key, generation));
            }
        }
        let failed = stream::iter(&abandoned)
            .map(|(key, generation)| {
                within(
                    request_timeout,
                    "the delete of a temporary object",
                    self.delete_version(key, *generation),
                )
            })
            .buffer_unordered(DELETE_CONCURRENCY)
            .filter(|deleted| std::future::ready(deleted.is_err()))
            .count()
            .await;
        swept.failed = failed as u64;
        swept.removed = (abandoned.len() - failed) as u64;
        Ok(swept)
    }
}

/// The number of the temporary composite of an upload, which the parts,
/// numbered from 1, never take.
const COMPOSITE_NUMBER: u32 = 0;

/// The key of temporary object `number` of `upload`.
fn temporary_key(upload: &PartsUpload, number: u32) -> String {
    format!("{}{number:05}", upload.id)
}

/// The body of a compose of `sources`, each pinned to its generation, into
/// an object with `metadata`. The CRC32C goes along, although the API
/// documents that a compose ignores it.
fn compose_request(sources: &[(String, u64)], metadata: &UserMetadata, crc32c: u32) -> String {
    let sources: Vec<serde_json::Value> = sources
        .iter()
        .map(|(name, generation)| {
            serde_json::json!({
                "name": name,
                "generation": generation.to_string(),
            })
        })
        .collect();
    let mut destination = serde_json::json!({
        "contentType": "application/octet-stream",
        "crc32c": json::encode_crc32c(crc32c),
    });
    if !metadata.is_empty() {
        destination["metadata"] = serde_json::json!(metadata);
    }
    serde_json::json!({
        "kind": "storage#composeRequest",
        "sourceObjects": sources,
        "destination": destination,
    })
    .to_string()
}

/// The CRC32C that the answer to a compose reports for the object.
fn composed_crc32c(composed: &ObjectResource) -> Result<Option<u32>, ObjectStoreError> {
    composed
        .crc32c
        .as_deref()
        .map(json::parse_crc32c)
        .transpose()
}

/// What one answer to a listing makes of the page.
#[derive(Debug, PartialEq, Eq)]
enum PageStep {
    Done(ListPage),
    /// The answer held no object after the start but a token: the listing
    /// asks again with it.
    Follow(String),
}

/// The page that `listing` makes, for a request that asked for
/// `max_results` objects from `after`, and whose page holds `page_len`.
/// The objects at or before `after` are dropped, as the start key of the
/// API is inclusive. More follow when the API sent a token, or as many
/// objects as asked for.
///
/// # Errors
/// [`ObjectStoreError::Unavailable`] for an empty answer with a token when
/// `may_follow` allows no more, and a malformed object.
fn page(
    listing: json::ObjectList,
    after: Option<&str>,
    page_len: usize,
    max_results: usize,
    may_follow: bool,
) -> Result<PageStep, ObjectStoreError> {
    let returned = listing.items.len();
    let mut objects = Vec::with_capacity(returned.min(page_len));
    for item in listing.items {
        if after.is_some_and(|after| item.name.as_str() <= after) {
            continue;
        }
        objects.push(item.listed()?);
    }
    if objects.is_empty()
        && let Some(token) = listing.next_page_token
    {
        if may_follow {
            return Ok(PageStep::Follow(token));
        }
        // Ending the page here would end the listing short.
        return Err(ObjectStoreError::Unavailable(format!(
            "the store answered {MAX_EMPTY_PAGES} empty pages of a listing in a row"
        )));
    }
    let more = listing.next_page_token.is_some() || returned >= max_results;
    let truncated = objects.len() > page_len;
    objects.truncate(page_len);
    let next = if truncated || more {
        objects.last().map(|object| object.key.clone())
    } else {
        None
    };
    Ok(PageStep::Done(ListPage { objects, next }))
}

impl Subject<'_> {
    const fn key(&self) -> Option<&str> {
        match self {
            Self::Object(key) | Self::Upload(key) | Self::Range { key, .. } => Some(key),
            Self::Bucket => None,
        }
    }
}

/// The pieces of `body`, checked against `expected` as they go: the piece
/// that completes the body is withheld when the bytes hash to another
/// CRC32C, and `mismatch` is set.
fn checked(
    body: PutBody,
    expected: Option<u32>,
    mismatch: Rc<Cell<bool>>,
) -> impl futures::Stream<Item = io::Result<Bytes>> + 'static {
    let len = body.len();
    let mut seen = 0_u64;
    let mut crc32c = 0_u32;
    body.into_pieces().map(move |piece| {
        let piece = piece?;
        let Some(expected) = expected else {
            return Ok(piece);
        };
        crc32c = crc32c::crc32c_append(crc32c, &piece);
        seen += piece.len() as u64;
        if seen == len && crc32c != expected {
            mismatch.set(true);
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "the body hashes to another CRC32C than the put declares",
            ));
        }
        Ok(piece)
    })
}

/// The status and the error body of a failed answer.
async fn read_failure(response: cyper::Response) -> (StatusCode, ErrorBody) {
    let status = response.status();
    let body = response.bytes().await.unwrap_or_default();
    (status, json::parse_error(&body))
}

/// The error that `status` and the error body mean for a call about
/// `subject`.
fn classify(
    status: StatusCode,
    error: &ErrorBody,
    subject: &Subject<'_>,
    bucket: &str,
) -> ObjectStoreError {
    let detail = || detail(status, error);
    let key = || subject.key().unwrap_or_default().to_owned();
    let bucket_missing = error
        .message
        .to_ascii_lowercase()
        .contains("bucket does not exist");
    match status.as_u16() {
        // An upload names no object that could be missing.
        404 if bucket_missing || matches!(subject, Subject::Bucket | Subject::Upload(_)) => {
            ObjectStoreError::BucketNotFound(bucket.to_owned())
        }
        404 => ObjectStoreError::NotFound { key: key() },
        412 if subject.key().is_some() => ObjectStoreError::PreconditionFailed { key: key() },
        416 => match subject {
            Subject::Range { key, range } => ObjectStoreError::InvalidRange {
                key: (*key).to_owned(),
                start: range.start,
                end: range.end,
            },
            Subject::Object(_) | Subject::Upload(_) | Subject::Bucket => {
                ObjectStoreError::Malformed(detail())
            }
        },
        400 if matches!(subject, Subject::Upload(_)) && names_a_digest_mismatch(&error.message) => {
            ObjectStoreError::ChecksumMismatch { key: key() }
        }
        // The upload broke off, so a body that changed on the way.
        408 => ObjectStoreError::Transport(detail()),
        409 | 429 | 503 => ObjectStoreError::Throttled(detail()),
        403 if error
            .reason
            .to_ascii_lowercase()
            .contains("ratelimitexceeded") =>
        {
            ObjectStoreError::Throttled(detail())
        }
        401 | 403 => ObjectStoreError::Auth(detail()),
        500..=599 => ObjectStoreError::Unavailable(detail()),
        300..=399 => ObjectStoreError::InvalidRequest(format!(
            "{}: the endpoint redirects, which the store does not follow",
            detail()
        )),
        400..=499 => ObjectStoreError::InvalidRequest(detail()),
        _ => ObjectStoreError::Malformed(detail()),
    }
}

/// Whether an upload's 400 says that the bytes do not match the CRC32C or
/// the MD5 of its metadata.
fn names_a_digest_mismatch(message: &str) -> bool {
    let message = message.to_ascii_lowercase();
    let digest = ["crc32c", "md5", "hash", "checksum"]
        .iter()
        .any(|word| message.contains(word));
    let differs = ["doesn't match", "does not match", "mismatch"]
        .iter()
        .any(|word| message.contains(word));
    digest && differs
}

fn detail(status: StatusCode, error: &ErrorBody) -> String {
    match (error.reason.is_empty(), error.message.is_empty()) {
        (true, true) => status.to_string(),
        (true, false) => format!("{status}: {}", error.message),
        (false, true) => format!("{status} {}", error.reason),
        (false, false) => format!("{status} {}: {}", error.reason, error.message),
    }
}

fn encode(component: &str) -> String {
    utf8_percent_encode(component, COMPONENT).to_string()
}

fn header_text<'a>(headers: &'a HeaderMap, name: &str) -> Result<&'a str, ObjectStoreError> {
    headers
        .get(name)
        .ok_or_else(|| ObjectStoreError::Malformed(format!("an answer without {name}")))?
        .to_str()
        .map_err(|_| ObjectStoreError::Malformed(format!("{name} is not ASCII")))
}

fn header_number(headers: &HeaderMap, name: &str) -> Result<u64, ObjectStoreError> {
    let text = header_text(headers, name)?;
    text.parse()
        .map_err(|_| ObjectStoreError::Malformed(format!("{name} {text:?} is not a number")))
}

/// The size of the whole object from `Content-Range: bytes a-b/size`.
fn content_range_size(headers: &HeaderMap) -> Result<u64, ObjectStoreError> {
    let text = header_text(headers, CONTENT_RANGE.as_str())?;
    text.rsplit_once('/')
        .and_then(|(_, size)| size.parse().ok())
        .ok_or_else(|| ObjectStoreError::Malformed(format!("Content-Range {text:?}")))
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;
    use std::io::{Read, Write};
    use std::net::{TcpListener, TcpStream};
    use std::sync::atomic::{AtomicI64, AtomicUsize, Ordering};
    use std::sync::{Arc, Mutex};
    use std::time::Duration;

    use super::*;

    /// Whether an error is the one that a case expects.
    type Expected = fn(&ObjectStoreError) -> bool;

    /// What the fake store answers to the request of a case.
    type Answers = fn(&Captured) -> Answer;

    /// One request that [`FakeGcs`] took.
    #[derive(Debug, Clone)]
    struct Captured {
        method: String,
        target: String,
        headers: BTreeMap<String, String>,
        body: Vec<u8>,
        /// Whether the connection closed before the declared body arrived.
        short: bool,
    }

    /// What [`FakeGcs`] answers: the status and the body.
    type Answer = (u16, Vec<u8>);

    /// An HTTP server on a thread of its own, each request on a thread of
    /// its own, that keeps every request and answers it with `answer`.
    struct FakeGcs {
        endpoint: String,
        requests: Arc<Mutex<Vec<Captured>>>,
        in_flight: Arc<AtomicUsize>,
        most_in_flight: Arc<AtomicUsize>,
        /// Seconds that the `Date` of each answer runs ahead of this host.
        store_clock_offset: Arc<AtomicI64>,
    }

    impl FakeGcs {
        fn start(answer: fn(&Captured) -> Answer) -> Self {
            let listener = TcpListener::bind("127.0.0.1:0").unwrap();
            let endpoint = format!("http://{}", listener.local_addr().unwrap());
            let fake = Self {
                endpoint,
                requests: Arc::default(),
                in_flight: Arc::default(),
                most_in_flight: Arc::default(),
                store_clock_offset: Arc::default(),
            };
            let requests = Arc::clone(&fake.requests);
            let store_clock_offset = Arc::clone(&fake.store_clock_offset);
            let in_flight = Arc::clone(&fake.in_flight);
            let most_in_flight = Arc::clone(&fake.most_in_flight);
            std::thread::spawn(move || {
                for connection in listener.incoming() {
                    let Ok(connection) = connection else { return };
                    let requests = Arc::clone(&requests);
                    let in_flight = Arc::clone(&in_flight);
                    let most_in_flight = Arc::clone(&most_in_flight);
                    let offset = store_clock_offset.load(Ordering::SeqCst);
                    std::thread::spawn(move || {
                        let now = in_flight.fetch_add(1, Ordering::SeqCst) + 1;
                        most_in_flight.fetch_max(now, Ordering::SeqCst);
                        serve(connection, answer, &requests, offset);
                        in_flight.fetch_sub(1, Ordering::SeqCst);
                    });
                }
            });
            fake
        }

        fn store(&self, auth: GcsAuth) -> GcsStore {
            GcsStore::new(GcsConfig {
                endpoint: self.endpoint.clone(),
                bucket: "segments".to_owned(),
                auth,
            })
            .unwrap()
            .with_uploads_in_parts(true)
        }

        fn requests(&self) -> Vec<Captured> {
            self.requests.lock().unwrap().clone()
        }
    }

    /// Reads one request from `connection`, keeps it, and answers it.
    fn serve(
        mut connection: TcpStream,
        answer: fn(&Captured) -> Answer,
        requests: &Mutex<Vec<Captured>>,
        store_clock_offset: i64,
    ) {
        let mut received = Vec::new();
        let mut buffer = [0_u8; 8192];
        let head_end = loop {
            if let Some(end) = received.windows(4).position(|window| window == b"\r\n\r\n") {
                break end;
            }
            match connection.read(&mut buffer) {
                Ok(0) | Err(_) => return,
                Ok(read) => received.extend_from_slice(&buffer[..read]),
            }
        };
        let head = String::from_utf8_lossy(&received[..head_end]).into_owned();
        let mut lines = head.split("\r\n");
        let mut request_line = lines.next().unwrap_or_default().split(' ');
        let method = request_line.next().unwrap_or_default().to_owned();
        let target = request_line.next().unwrap_or_default().to_owned();
        let headers: BTreeMap<String, String> = lines
            .filter_map(|line| line.split_once(':'))
            .map(|(name, value)| (name.trim().to_ascii_lowercase(), value.trim().to_owned()))
            .collect();
        let declared: usize = headers
            .get("content-length")
            .and_then(|length| length.parse().ok())
            .unwrap_or(0);
        let mut body = received[head_end + 4..].to_vec();
        let mut short = false;
        while body.len() < declared {
            match connection.read(&mut buffer) {
                Ok(0) | Err(_) => {
                    short = true;
                    break;
                }
                Ok(read) => body.extend_from_slice(&buffer[..read]),
            }
        }
        let captured = Captured {
            method,
            target,
            headers,
            body,
            short,
        };
        requests.lock().unwrap().push(captured.clone());
        if short {
            return;
        }
        let (status, answer_body) = answer(&captured);
        let reply = format!(
            "HTTP/1.1 {status} Fake\r\ncontent-length: {}\r\ncontent-type: application/json\r\n\
             date: {}\r\nconnection: close\r\n\r\n",
            answer_body.len(),
            (chrono::Utc::now() + chrono::TimeDelta::seconds(store_clock_offset))
                .format("%a, %d %b %Y %H:%M:%S GMT")
        );
        let _ = connection.write_all(reply.as_bytes());
        let _ = connection.write_all(&answer_body);
    }

    fn token(value: &str) -> GcsAuth {
        let shared = SharedToken::new();
        shared.replace(AccessToken::new(value.to_owned()));
        GcsAuth::Token(shared)
    }

    /// The configuration of a store at `endpoint`. An `https` client takes
    /// the process's TLS provider, which the server installs at boot.
    fn config(endpoint: &str) -> GcsConfig {
        let _ = rustls::crypto::ring::default_provider().install_default();
        GcsConfig {
            endpoint: endpoint.to_owned(),
            bucket: "segments".to_owned(),
            auth: GcsAuth::Anonymous,
        }
    }

    fn error_body(reason: &str, message: &str) -> ErrorBody {
        ErrorBody {
            reason: reason.to_owned(),
            message: message.to_owned(),
        }
    }

    #[test]
    fn given_endpoints_when_building_should_refuse_what_cannot_address_a_bucket() {
        for endpoint in [
            "not a url",
            "ftp://store",
            "http://store/prefix",
            "http://store/?a=b",
            "http://user:pass@store",
        ] {
            assert!(
                matches!(
                    GcsStore::new(config(endpoint)),
                    Err(ObjectStoreError::InvalidRequest(_))
                ),
                "{endpoint}"
            );
        }
        for bucket in ["", " ", "a/b"] {
            assert!(
                GcsStore::new(GcsConfig {
                    bucket: bucket.to_owned(),
                    ..config(DEFAULT_ENDPOINT)
                })
                .is_err(),
                "{bucket:?}"
            );
        }
        let store = GcsStore::new(config(DEFAULT_ENDPOINT)).unwrap();
        assert_eq!(store.origin, "https://storage.googleapis.com");
        let emulator = GcsStore::new(config("http://localhost:4443")).unwrap();
        assert_eq!(emulator.origin, "http://localhost:4443");
    }

    #[test]
    fn given_an_endpoint_other_than_google_when_building_should_put_every_object_whole() {
        for endpoint in [
            DEFAULT_ENDPOINT,
            "https://storage.us-central1.rep.googleapis.com",
        ] {
            let store = GcsStore::new(config(endpoint)).unwrap();
            assert!(store.parts_limits().is_some(), "{endpoint}");
        }
        let options = PutOptions {
            mode: PutMode::IfAbsent,
            ..PutOptions::default()
        };
        for endpoint in [
            "http://localhost:4443",
            "https://fake-gcs.internal",
            "http://storage.googleapis.com",
        ] {
            let store = GcsStore::new(config(endpoint)).unwrap();

            let begun = crate::contract::shard_runtime().block_on(store.begin_parts(
                "a/b",
                &options,
                &parts_layout(),
            ));

            assert_eq!(store.parts_limits(), None, "{endpoint}");
            assert!(
                matches!(begun, Err(ObjectStoreError::Unsupported(_))),
                "{endpoint}: {begun:?}"
            );
            let enabled = store.with_uploads_in_parts(true);
            assert!(enabled.parts_limits().is_some(), "{endpoint}");
        }
    }

    #[test]
    fn given_keys_when_building_urls_should_send_each_as_one_encoded_segment() {
        let store = GcsStore::new(config(DEFAULT_ENDPOINT)).unwrap();

        assert_eq!(
            store.object_url("iggy/1/2 b/caf\u{e9}+~.log"),
            "https://storage.googleapis.com/storage/v1/b/segments/o/iggy%2F1%2F2%20b%2Fcaf%C3%A9%2B~.log"
        );
        assert_eq!(
            store.media_url("a/b"),
            "https://storage.googleapis.com/storage/v1/b/segments/o/a%2Fb?alt=media"
        );
        assert_eq!(
            store.list_url("iggy/", Some("iggy/a b"), 11, Some("tok/en=")),
            "https://storage.googleapis.com/storage/v1/b/segments/o?maxResults=11\
             &prefix=iggy%2F&startOffset=iggy%2Fa%20b&pageToken=tok%2Fen%3D"
        );
        assert_eq!(
            store.list_url("", None, 1000, None),
            "https://storage.googleapis.com/storage/v1/b/segments/o?maxResults=1000"
        );
    }

    #[test]
    fn given_put_options_when_building_the_upload_should_ask_for_a_create_with_the_crc32c() {
        let store = GcsStore::new(config(DEFAULT_ENDPOINT)).unwrap();
        let options = PutOptions {
            mode: PutMode::IfAbsent,
            metadata: UserMetadata::from([("end-offset".to_owned(), "99".to_owned())]),
            crc32c: Some(crc32c::crc32c(b"hello world")),
            ..PutOptions::default()
        };

        let create = store.upload("iggy/a b", &options);
        let overwrite = store.upload("iggy/a b", &PutOptions::default());

        assert_eq!(
            create.url,
            "https://storage.googleapis.com/upload/storage/v1/b/segments/o\
             ?uploadType=multipart&name=iggy%2Fa%20b&ifGenerationMatch=0"
        );
        assert_eq!(
            overwrite.url,
            "https://storage.googleapis.com/upload/storage/v1/b/segments/o\
             ?uploadType=multipart&name=iggy%2Fa%20b"
        );
        let boundary = create
            .content_type
            .strip_prefix("multipart/related; boundary=")
            .unwrap();
        assert_eq!(
            create.head,
            format!(
                "--{boundary}\r\ncontent-type: application/json; charset=UTF-8\r\n\r\n\
                 {{\"name\":\"iggy/a b\",\"metadata\":{{\"end-offset\":\"99\"}},\
                 \"crc32c\":\"yZRlqg==\"}}\r\n\
                 --{boundary}\r\ncontent-type: application/octet-stream\r\n\r\n"
            )
        );
        assert_eq!(create.tail, format!("\r\n--{boundary}--\r\n"));
        assert!(
            String::from_utf8_lossy(&overwrite.head).contains("{\"name\":\"iggy/a b\"}\r\n"),
            "no metadata and no crc32c: {:?}",
            overwrite.head
        );
        assert_ne!(create.content_type, overwrite.content_type);
    }

    /// Runs each case through [`classify`]: a status, a reason, a message, what
    /// the call was about, and the error that the answer must make.
    fn assert_classified(cases: &[(u16, &str, &str, &Subject<'_>, Expected)]) {
        for (status, reason, message, subject, expected) in cases {
            let error = classify(
                StatusCode::from_u16(*status).unwrap(),
                &error_body(reason, message),
                subject,
                "segments",
            );
            assert!(expected(&error), "{status} {reason} {message}: {error:?}");
        }
    }

    #[test]
    fn given_answers_about_an_object_when_classifying_should_map_each_to_its_error() {
        let object = Subject::Object("a/b");
        let upload = Subject::Upload("a/b");
        let range = Subject::Range {
            key: "a/b",
            range: 5..9,
        };
        let bucket = Subject::Bucket;
        let crc_mismatch =
            "Provided CRC32C \"AAAAAA==\" doesn't match calculated CRC32C \"yZRlqg==\".";
        assert_classified(&[
            (
                404,
                "notFound",
                "No such object: segments/a/b",
                &object,
                |error| matches!(error, ObjectStoreError::NotFound { key } if key == "a/b"),
            ),
            (404, "", "Not Found", &range, |error| {
                matches!(error, ObjectStoreError::NotFound { .. })
            }),
            (
                404,
                "notFound",
                "The specified bucket does not exist.",
                &range,
                |error| matches!(error, ObjectStoreError::BucketNotFound(bucket) if bucket == "segments"),
            ),
            (404, "notFound", "Not Found", &bucket, |error| {
                matches!(error, ObjectStoreError::BucketNotFound(_))
            }),
            (404, "notFound", "Not Found", &upload, |error| {
                matches!(error, ObjectStoreError::BucketNotFound(_))
            }),
            (
                412,
                "conditionNotMet",
                "",
                &upload,
                |error| matches!(error, ObjectStoreError::PreconditionFailed { key } if key == "a/b"),
            ),
            (416, "requestedRangeNotSatisfiable", "", &range, |error| {
                matches!(
                    error,
                    ObjectStoreError::InvalidRange { key, start: 5, end: 9 } if key == "a/b"
                )
            }),
            (
                400,
                "invalid",
                crc_mismatch,
                &upload,
                |error| matches!(error, ObjectStoreError::ChecksumMismatch { key } if key == "a/b"),
            ),
            (
                400,
                "invalid",
                "Provided MD5 hash doesn't match calculated MD5 hash.",
                &upload,
                |error| matches!(error, ObjectStoreError::ChecksumMismatch { .. }),
            ),
            (400, "invalid", crc_mismatch, &object, |error| {
                matches!(error, ObjectStoreError::InvalidRequest(_))
            }),
            (
                400,
                "invalidArgument",
                "Invalid argument.",
                &upload,
                |error| matches!(error, ObjectStoreError::InvalidRequest(_)),
            ),
        ]);
    }

    #[test]
    fn given_outages_and_refusals_when_classifying_should_keep_their_retry_class() {
        let object = Subject::Object("a/b");
        let upload = Subject::Upload("a/b");
        let bucket = Subject::Bucket;
        assert_classified(&[
            (408, "uploadBrokenConnection", "", &upload, |error| {
                matches!(error, ObjectStoreError::Transport(_))
            }),
            (409, "conflict", "", &upload, |error| {
                matches!(error, ObjectStoreError::Throttled(_))
            }),
            (429, "rateLimitExceeded", "", &upload, |error| {
                matches!(error, ObjectStoreError::Throttled(_))
            }),
            (503, "backendError", "", &bucket, |error| {
                matches!(error, ObjectStoreError::Throttled(_))
            }),
            (403, "userRateLimitExceeded", "", &object, |error| {
                matches!(error, ObjectStoreError::Throttled(_))
            }),
            (
                403,
                "forbidden",
                "no storage.objects.get access",
                &object,
                |error| matches!(error, ObjectStoreError::Auth(_)),
            ),
            (401, "authError", "Invalid Credentials", &object, |error| {
                matches!(error, ObjectStoreError::Auth(_))
            }),
            (500, "internalError", "", &object, |error| {
                matches!(error, ObjectStoreError::Unavailable(_))
            }),
            (502, "", "", &object, |error| {
                matches!(error, ObjectStoreError::Unavailable(_))
            }),
            (302, "found", "", &object, |error| {
                matches!(error, ObjectStoreError::InvalidRequest(_))
            }),
            (200, "", "", &object, |error| {
                matches!(error, ObjectStoreError::Malformed(_))
            }),
        ]);
    }

    #[test]
    fn given_a_refused_token_when_classifying_should_wait_for_a_refresh_only_with_a_token() {
        let anonymous = GcsStore::new(config(DEFAULT_ENDPOINT)).unwrap();
        let with_token = GcsStore::new(GcsConfig {
            auth: token("ya29.expired"),
            ..config(DEFAULT_ENDPOINT)
        })
        .unwrap();
        let refused = error_body("authError", "Invalid Credentials");
        let subject = Subject::Object("a/b");

        let refreshed = with_token.failure(StatusCode::UNAUTHORIZED, &refused, &subject);
        let fixed = anonymous.failure(StatusCode::UNAUTHORIZED, &refused, &subject);

        assert!(
            matches!(refreshed, ObjectStoreError::Unavailable(_)),
            "{refreshed:?}"
        );
        assert!(matches!(fixed, ObjectStoreError::Auth(_)), "{fixed:?}");
    }

    /// A store whose refresher has fetched nothing yet fails each call as
    /// transient, before any request, so the caller retries.
    #[test]
    fn given_no_token_yet_when_calling_should_fail_as_transient_before_any_request() {
        let fake = FakeGcs::start(|_| (200, Vec::new()));
        let store = fake.store(GcsAuth::Token(SharedToken::new()));

        let (head, delete) = crate::contract::shard_runtime().block_on(async {
            (
                store.head("a/b").await,
                store.delete(&["a/b".to_owned()]).await,
            )
        });

        assert!(
            matches!(&head, Err(ObjectStoreError::Unavailable(reason)) if reason.contains("no access token")),
            "{head:?}"
        );
        assert!(
            matches!(&delete, Err(ObjectStoreError::Unavailable(_))),
            "{delete:?}"
        );
        assert!(fake.requests().is_empty());
    }

    fn listing(names: &[&str], token: Option<&str>) -> json::ObjectList {
        json::parse_list(
            serde_json::json!({
                "items": names
                    .iter()
                    .map(|name| serde_json::json!({
                        "name": name,
                        "size": "1",
                        "generation": "7",
                        "timeCreated": "2026-10-28T00:00:00Z",
                    }))
                    .collect::<Vec<_>>(),
                "nextPageToken": token,
            })
            .to_string()
            .as_bytes(),
        )
        .unwrap()
    }

    fn keys_and_next(step: PageStep) -> (Vec<String>, Option<String>) {
        match step {
            PageStep::Done(page) => (
                page.objects.into_iter().map(|object| object.key).collect(),
                page.next,
            ),
            PageStep::Follow(token) => panic!("follows {token}"),
        }
    }

    #[test]
    fn given_listing_answers_when_paging_should_resume_after_the_last_key() {
        let page_of = |names: &[&str], token, after, page_len, max_results| {
            keys_and_next(page(listing(names, token), after, page_len, max_results, true).unwrap())
        };
        let strings = |names: &[&str]| {
            names
                .iter()
                .map(|name| (*name).to_owned())
                .collect::<Vec<_>>()
        };

        assert_eq!(
            page_of(&["a", "b", "c"], None, None, 2, 3),
            (strings(&["a", "b"]), Some("b".to_owned())),
            "one more than the page holds: more follow"
        );
        assert_eq!(
            page_of(&["b", "c", "d"], None, Some("b"), 2, 3),
            (strings(&["c", "d"]), Some("d".to_owned())),
            "the inclusive start key is dropped, and a full answer means more"
        );
        assert_eq!(
            page_of(&["b", "c"], None, Some("b"), 2, 3),
            (strings(&["c"]), None),
            "a short answer without a token is the end"
        );
        assert_eq!(
            page_of(&["c"], Some("token"), Some("b"), 2, 3),
            (strings(&["c"]), Some("c".to_owned())),
            "a short answer with a token is not the end"
        );
        assert_eq!(page_of(&[], None, None, 5, 6), (Vec::new(), None));
        assert_eq!(
            page(listing(&["b"], Some("token")), Some("b"), 2, 3, true).unwrap(),
            PageStep::Follow("token".to_owned()),
            "an empty page with a token is followed"
        );
        assert!(matches!(
            page(listing(&[], Some("token")), None, 2, 3, false),
            Err(ObjectStoreError::Unavailable(_))
        ));
    }

    #[test]
    fn given_a_put_when_sending_should_send_the_body_between_its_parts_with_the_token() {
        let fake = FakeGcs::start(|_| (200, br#"{"name":"a/b"}"#.to_vec()));
        let store = fake.store(token("ya29.node"));
        let body = Bytes::from_static(b"segment bytes");
        let options = PutOptions {
            mode: PutMode::IfAbsent,
            metadata: UserMetadata::new(),
            crc32c: Some(crc32c::crc32c(&body)),
            ..PutOptions::default()
        };

        crate::contract::shard_runtime()
            .block_on(store.put(
                "iggy/a",
                PutBody::from_stream(
                    body.len() as u64,
                    stream::iter([Ok(body.slice(..7)), Ok(body.slice(7..))]),
                ),
                &options,
            ))
            .unwrap();

        let [request] = fake.requests().try_into().unwrap();
        assert_eq!(request.method, "POST");
        assert_eq!(
            request.target,
            "/upload/storage/v1/b/segments/o?uploadType=multipart&name=iggy%2Fa&ifGenerationMatch=0"
        );
        assert_eq!(request.headers["authorization"], "Bearer ya29.node");
        assert_eq!(
            request.headers["content-length"],
            request.body.len().to_string()
        );
        let boundary = request.headers["content-type"]
            .strip_prefix("multipart/related; boundary=")
            .unwrap()
            .to_owned();
        let text = String::from_utf8(request.body).unwrap();
        assert_eq!(
            text,
            format!(
                "--{boundary}\r\ncontent-type: application/json; charset=UTF-8\r\n\r\n\
                 {{\"name\":\"iggy/a\",\"crc32c\":\"{}\"}}\r\n\
                 --{boundary}\r\ncontent-type: application/octet-stream\r\n\r\n\
                 segment bytes\r\n--{boundary}--\r\n",
                json::encode_crc32c(crc32c::crc32c(&body))
            )
        );
    }

    /// The provider checks the CRC32C itself as well, so the store never gets
    /// the last byte of a body that hashes to another value.
    #[test]
    fn given_a_wrong_crc32c_when_putting_should_withhold_the_last_byte() {
        let fake = FakeGcs::start(|_| (200, Vec::new()));
        let store = fake.store(GcsAuth::Anonymous);
        let body = Bytes::from(vec![7_u8; 100_000]);
        let options = PutOptions {
            crc32c: Some(crc32c::crc32c(&body) ^ 1),
            ..PutOptions::default()
        };

        let result = crate::contract::shard_runtime().block_on(store.put(
            "a/b",
            PutBody::from_bytes(body),
            &options,
        ));

        assert!(
            matches!(&result, Err(ObjectStoreError::ChecksumMismatch { key }) if key == "a/b"),
            "{result:?}"
        );
        std::thread::sleep(Duration::from_millis(50));
        assert!(
            fake.requests().iter().all(|request| request.short),
            "{:?}",
            fake.requests()
                .iter()
                .map(|request| request.short)
                .collect::<Vec<_>>()
        );
    }

    fn put_to(endpoint: &str) -> Result<(), ObjectStoreError> {
        let store = GcsStore::new(config(endpoint)).unwrap();
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
    fn given_a_store_that_refuses_the_connection_when_putting_should_fail_as_never_sent() {
        let result = put_to(&crate::http::test_endpoints::refusing());

        assert!(
            matches!(&result, Err(ObjectStoreError::NotSent(_))),
            "{result:?}"
        );
        assert!(!result.unwrap_err().put_may_have_stored());
    }

    #[test]
    fn given_a_store_that_hangs_up_after_the_request_when_putting_should_fail_as_unknown() {
        let result = put_to(&crate::http::test_endpoints::hanging_up());

        assert!(
            matches!(&result, Err(ObjectStoreError::Transport(_))),
            "{result:?}"
        );
        assert!(result.unwrap_err().put_may_have_stored());
    }

    const PART_BYTES: &[u8] = b"part one, part two";

    fn parts_layout() -> KeyLayout {
        KeyLayout::new("iggy").unwrap()
    }

    fn temporaries() -> PartsUpload {
        PartsUpload {
            key: "a/b".to_owned(),
            id: parts_layout().upload_prefix(0x2a),
        }
    }

    fn object_json(name: &str, generation: u64, crc32c: u32, updated: &str) -> Vec<u8> {
        serde_json::json!({
            "name": name,
            "size": "18",
            "generation": generation.to_string(),
            "timeCreated": updated,
            "crc32c": json::encode_crc32c(crc32c),
        })
        .to_string()
        .into_bytes()
    }

    #[test]
    fn given_an_upload_in_parts_when_beginning_should_name_its_temporaries_after_the_put_id() {
        let store = GcsStore::new(config(DEFAULT_ENDPOINT)).unwrap();
        let options = PutOptions {
            metadata: UserMetadata::from([(PUT_ID_METADATA.to_owned(), format!("{:032x}", 0x2a))]),
            ..PutOptions::default()
        };

        let begun = crate::contract::shard_runtime()
            .block_on(store.begin_parts("a/b", &options, &parts_layout()))
            .unwrap();

        assert_eq!(begun, temporaries());
    }

    #[test]
    fn given_a_part_when_putting_should_store_a_checked_temporary_and_keep_its_generation() {
        let fake = FakeGcs::start(|request| {
            (
                200,
                object_json(
                    "t",
                    77,
                    crc32c::crc32c(&request.body),
                    "2026-01-01T00:00:00Z",
                ),
            )
        });
        let store = fake.store(GcsAuth::Anonymous);
        let bytes = Bytes::from_static(PART_BYTES);
        let crc32c = crc32c::crc32c(&bytes);

        let stored = crate::contract::shard_runtime()
            .block_on(store.put_part(&temporaries(), 2, PutBody::from_bytes(bytes), crc32c))
            .unwrap();

        assert_eq!(
            stored,
            StoredPart {
                number: 2,
                tag: "77".to_owned(),
                crc32c,
            }
        );
        let [request] = fake.requests().try_into().unwrap();
        assert_eq!(
            request.target,
            "/upload/storage/v1/b/segments/o?uploadType=multipart&name=iggy%2Fmeta%2Fuploads%2F\
             0000000000000000000000000000002a%2F00002&ifGenerationMatch=0"
        );
        let text = String::from_utf8_lossy(&request.body);
        assert!(
            text.contains(&json::encode_crc32c(crc32c)),
            "the part carries its CRC32C: {text}"
        );
    }

    /// Refuses the create of the part, and reports a part of `size` bytes and
    /// the CRC32C of [`PART_BYTES`] under generation 55.
    fn part_stored_before(request: &Captured, size: &str) -> Answer {
        if request.method == "POST" {
            return (412, Vec::new());
        }
        let body = serde_json::json!({
            "name": "t",
            "size": size,
            "generation": "55",
            "timeCreated": "2026-01-01T00:00:00Z",
            "crc32c": json::encode_crc32c(crc32c::crc32c(PART_BYTES)),
        });
        (200, body.to_string().into_bytes())
    }

    #[test]
    fn given_a_part_stored_before_when_putting_it_again_should_take_its_generation_only_when_it_matches()
     {
        let crc32c = crc32c::crc32c(PART_BYTES);
        let put_again = |fake: &FakeGcs| {
            crate::contract::shard_runtime().block_on(fake.store(GcsAuth::Anonymous).put_part(
                &temporaries(),
                2,
                PutBody::from_bytes(Bytes::from_static(PART_BYTES)),
                crc32c,
            ))
        };
        let same = FakeGcs::start(|request| part_stored_before(request, "18"));
        let other = FakeGcs::start(|request| part_stored_before(request, "17"));

        let found = put_again(&same).unwrap();
        let refused = put_again(&other);

        assert_eq!(
            found,
            StoredPart {
                number: 2,
                tag: "55".to_owned(),
                crc32c,
            }
        );
        let methods: Vec<String> = same
            .requests()
            .iter()
            .map(|request| request.method.clone())
            .collect();
        assert_eq!(methods, ["POST", "GET"]);
        assert!(
            matches!(refused, Err(ObjectStoreError::Unavailable(_))),
            "{refused:?}"
        );
    }

    fn stored_parts() -> Vec<StoredPart> {
        vec![
            StoredPart {
                number: 1,
                tag: "11".to_owned(),
                crc32c: 1,
            },
            StoredPart {
                number: 2,
                tag: "12".to_owned(),
                crc32c: 2,
            },
        ]
    }

    fn create_options() -> PutOptions {
        PutOptions {
            mode: PutMode::IfAbsent,
            metadata: UserMetadata::from([(PUT_ID_METADATA.to_owned(), "2a".to_owned())]),
            crc32c: Some(crc32c::crc32c(PART_BYTES)),
            ..PutOptions::default()
        }
    }

    /// Answers each compose with an object of `crc32c` and generation 99,
    /// for the composite, then 100, for the object.
    fn composed(request: &Captured, crc32c: u32) -> Answer {
        let generation = if request.target.contains("%2F00000/compose") {
            99
        } else {
            100
        };
        (
            200,
            object_json("a/b", generation, crc32c, "2026-01-01T00:00:00Z"),
        )
    }

    #[test]
    fn given_parts_when_completing_a_create_only_put_should_check_a_composite_before_composing_the_object()
     {
        let fake = FakeGcs::start(|request| composed(request, crc32c::crc32c(PART_BYTES)));
        let store = fake.store(GcsAuth::Anonymous);

        crate::contract::shard_runtime()
            .block_on(store.complete_parts(&temporaries(), &stored_parts(), &create_options(), 18))
            .unwrap();

        let prefix = parts_layout().upload_prefix(0x2a);
        let composite = format!("{prefix}00000");
        let [into_composite, into_object] = fake.requests().try_into().unwrap();
        assert_eq!(into_composite.method, "POST");
        assert_eq!(
            into_composite.target,
            format!("/storage/v1/b/segments/o/{}/compose", encode(&composite))
        );
        let body: serde_json::Value = serde_json::from_slice(&into_composite.body).unwrap();
        assert_eq!(
            body["sourceObjects"],
            serde_json::json!([
                { "name": format!("{prefix}00001"), "generation": "11" },
                { "name": format!("{prefix}00002"), "generation": "12" },
            ])
        );
        assert_eq!(body["destination"]["metadata"], serde_json::Value::Null);
        assert_eq!(
            into_object.target,
            "/storage/v1/b/segments/o/a%2Fb/compose?ifGenerationMatch=0"
        );
        let body: serde_json::Value = serde_json::from_slice(&into_object.body).unwrap();
        assert_eq!(
            body["sourceObjects"],
            serde_json::json!([{ "name": composite, "generation": "99" }])
        );
        assert_eq!(body["destination"]["metadata"][PUT_ID_METADATA], "2a");
        assert_eq!(
            body["destination"]["crc32c"],
            json::encode_crc32c(crc32c::crc32c(PART_BYTES))
        );
    }

    #[test]
    fn given_an_overwrite_when_completing_should_compose_the_object_without_a_condition() {
        let fake = FakeGcs::start(|request| composed(request, crc32c::crc32c(PART_BYTES)));
        let store = fake.store(GcsAuth::Anonymous);
        let options = PutOptions {
            mode: PutMode::Overwrite,
            ..create_options()
        };

        crate::contract::shard_runtime()
            .block_on(store.complete_parts(&temporaries(), &stored_parts(), &options, 18))
            .unwrap();

        assert_eq!(
            fake.requests()[1].target,
            "/storage/v1/b/segments/o/a%2Fb/compose"
        );
    }

    /// A compose takes no CRC32C to check, so the provider compares the one
    /// that the composite reports, and a mismatch never reaches the key.
    #[test]
    fn given_a_composite_of_another_crc32c_when_completing_should_fail_before_composing_the_object()
    {
        let fake = FakeGcs::start(|request| composed(request, crc32c::crc32c(PART_BYTES) ^ 1));
        let store = fake.store(GcsAuth::Anonymous);

        let completed = crate::contract::shard_runtime().block_on(store.complete_parts(
            &temporaries(),
            &stored_parts(),
            &create_options(),
            18,
        ));

        assert!(
            matches!(&completed, Err(ObjectStoreError::ChecksumMismatch { key }) if key == "a/b"),
            "{completed:?}"
        );
        let [request] = fake.requests().try_into().unwrap();
        assert!(
            request.target.contains("%2F00000/compose"),
            "only the composite: {}",
            request.target
        );
    }

    #[test]
    fn given_more_parts_than_a_compose_takes_when_completing_should_refuse_before_any_request() {
        let fake = FakeGcs::start(|_| (200, Vec::new()));
        let store = fake.store(GcsAuth::Anonymous);
        let parts: Vec<StoredPart> = (1..=MAX_COMPOSE_SOURCES + 1)
            .map(|number| StoredPart {
                number,
                tag: "1".to_owned(),
                crc32c: 0,
            })
            .collect();

        let completed = crate::contract::shard_runtime().block_on(store.complete_parts(
            &temporaries(),
            &parts,
            &create_options(),
            18,
        ));

        assert!(
            matches!(completed, Err(ObjectStoreError::InvalidRequest(_))),
            "{completed:?}"
        );
        assert!(fake.requests().is_empty());
        assert_eq!(store.parts_limits().unwrap().max_parts, MAX_COMPOSE_SOURCES);
    }

    #[test]
    fn given_answers_to_a_compose_when_classifying_should_tell_a_conflict_from_a_lost_part() {
        let cases: [(Answers, Expected); 2] = [
            (
                |_| (412, Vec::new()),
                |error| matches!(error, ObjectStoreError::PreconditionFailed { key } if key == "a/b"),
            ),
            (
                |_| (404, Vec::new()),
                |error| matches!(error, ObjectStoreError::NotFound { .. }),
            ),
        ];
        for (answer, expected) in cases {
            let fake = FakeGcs::start(answer);
            let store = fake.store(GcsAuth::Anonymous);

            let completed = crate::contract::shard_runtime().block_on(store.complete_parts(
                &temporaries(),
                &stored_parts(),
                &create_options(),
                18,
            ));

            let error = completed.unwrap_err();
            assert!(expected(&error), "{error:?}");
        }
    }

    /// Lists two versions of an old temporary, a young one, and an old
    /// object under the prefix that is no temporary; deletes are answered
    /// 204.
    fn temporaries_listing(request: &Captured) -> Answer {
        if request.method == "DELETE" {
            return (204, Vec::new());
        }
        let at = |age: chrono::TimeDelta| (chrono::Utc::now() - age).to_rfc3339();
        let layout = KeyLayout::new("iggy").unwrap();
        let item = |name: String, generation: &str, updated: String| {
            serde_json::json!({
                "name": name,
                "size": "1",
                "generation": generation,
                "timeCreated": updated,
            })
        };
        let old = at(chrono::TimeDelta::hours(2));
        let body = serde_json::json!({
            "items": [
                item(layout.upload_part_key(1, 1), "7", old.clone()),
                item(layout.upload_part_key(1, 1), "8", old.clone()),
                item(layout.upload_part_key(2, 1), "9", at(chrono::TimeDelta::minutes(5))),
                item(format!("{}stray", layout.uploads_prefix()), "1", old),
            ]
        });
        (200, body.to_string().into_bytes())
    }

    #[test]
    fn given_versions_of_temporaries_when_sweeping_should_delete_each_old_version_for_good() {
        let fake = FakeGcs::start(temporaries_listing);
        let store = fake.store(GcsAuth::Anonymous);
        let layout = parts_layout();

        let swept = crate::contract::shard_runtime()
            .block_on(store.sweep_uploads(
                &layout,
                Duration::from_secs(3600),
                None,
                Duration::from_secs(30),
            ))
            .unwrap();

        assert_eq!((swept.removed, swept.kept, swept.failed), (2, 1, 0));
        let requests = fake.requests();
        assert!(
            requests[0]
                .target
                .contains("prefix=iggy%2Fmeta%2Fuploads%2F")
                && requests[0].target.contains("versions=true"),
            "{}",
            requests[0].target
        );
        let mut deleted: Vec<&str> = requests
            .iter()
            .filter(|request| request.method == "DELETE")
            .map(|request| request.target.as_str())
            .collect();
        deleted.sort_unstable();
        let version = |generation: u64| {
            format!(
                "/storage/v1/b/segments/o/{}?generation={generation}",
                encode(&layout.upload_part_key(1, 1))
            )
        };
        assert_eq!(deleted, [version(7), version(8)]);
    }

    /// The temporary is two hours old by this node's clock, and half an hour
    /// old by the store's, which runs 90 minutes behind.
    #[test]
    fn given_a_store_clock_behind_the_nodes_when_sweeping_should_take_the_cutoff_from_the_store() {
        let fake = FakeGcs::start(|request| {
            if request.method == "DELETE" {
                return (204, Vec::new());
            }
            let two_hours_ago = (chrono::Utc::now() - chrono::TimeDelta::hours(2)).to_rfc3339();
            let body = serde_json::json!({
                "items": [{
                    "name": parts_layout().upload_part_key(1, 1),
                    "size": "1",
                    "generation": "1",
                    "timeCreated": two_hours_ago,
                }]
            });
            (200, body.to_string().into_bytes())
        });
        fake.store_clock_offset.store(-90 * 60, Ordering::SeqCst);
        let store = fake.store(GcsAuth::Anonymous);

        let swept = crate::contract::shard_runtime()
            .block_on(store.sweep_uploads(
                &parts_layout(),
                Duration::from_hours(1),
                None,
                Duration::from_secs(30),
            ))
            .unwrap();

        assert_eq!((swept.removed, swept.kept), (0, 1));
        assert!(
            fake.requests()
                .iter()
                .all(|request| request.method != "DELETE")
        );
    }

    #[test]
    fn given_an_upload_when_aborting_should_delete_every_version_of_its_temporaries() {
        let fake = FakeGcs::start(|request| {
            if request.method == "DELETE" {
                return (204, Vec::new());
            }
            let prefix = parts_layout().upload_prefix(0x2a);
            let item = |number: u32, generation: &str| {
                serde_json::json!({
                    "name": format!("{prefix}{number:05}"),
                    "size": "1",
                    "generation": generation,
                    "timeCreated": "2026-01-01T00:00:00Z",
                })
            };
            let body = serde_json::json!({
                "items": [item(0, "3"), item(1, "4"), item(1, "5"), item(2, "6")]
            });
            (200, body.to_string().into_bytes())
        });
        let store = fake.store(GcsAuth::Anonymous);

        crate::contract::shard_runtime()
            .block_on(store.abort_parts(&temporaries(), 2))
            .unwrap();

        let requests = fake.requests();
        assert!(
            requests[0].target.contains(&format!(
                "prefix={}",
                encode(&parts_layout().upload_prefix(0x2a))
            )) && requests[0].target.contains("versions=true"),
            "{}",
            requests[0].target
        );
        let mut deleted: Vec<String> = requests
            .iter()
            .filter(|request| request.method == "DELETE")
            .map(|request| {
                let generation = request.target.rsplit_once("generation=").unwrap().1;
                generation.to_owned()
            })
            .collect();
        deleted.sort_unstable();
        assert_eq!(deleted, ["3", "4", "5", "6"]);
    }

    #[test]
    fn given_a_batch_when_deleting_should_send_one_request_per_key_a_few_at_once() {
        let fake = FakeGcs::start(|request| {
            std::thread::sleep(Duration::from_millis(20));
            if request.target.ends_with("%2Fmissing") {
                (
                    404,
                    br#"{"error":{"code":404,"message":"No such object: segments/a/missing"}}"#
                        .to_vec(),
                )
            } else if request.target.ends_with("%2Ffailing") {
                (500, br#"{"error":{"code":500,"message":"Backend Error","errors":[{"reason":"backendError"}]}}"#.to_vec())
            } else if request.target.ends_with("%2Fdenied") {
                (403, br#"{"error":{"code":403,"message":"Forbidden","errors":[{"reason":"forbidden"}]}}"#.to_vec())
            } else {
                (204, Vec::new())
            }
        });
        let store = fake.store(GcsAuth::Anonymous);
        let mut keys: Vec<String> = (0..40).map(|index| format!("a/{index}")).collect();
        keys.extend(["a/missing", "a/failing", "a/denied", "../invalid"].map(str::to_owned));

        let outcome = crate::contract::shard_runtime()
            .block_on(store.delete(&keys))
            .unwrap();

        let mut failed: Vec<(String, ErrorClassName)> = outcome
            .failed
            .iter()
            .map(|failure| (failure.key.clone(), ErrorClassName::of(&failure.error)))
            .collect();
        failed.sort();
        assert_eq!(
            failed,
            [
                ("../invalid".to_owned(), ErrorClassName::InvalidKey),
                ("a/denied".to_owned(), ErrorClassName::Auth),
                ("a/failing".to_owned(), ErrorClassName::Unavailable),
            ]
        );
        let requests = fake.requests();
        assert_eq!(requests.len(), 43);
        assert!(requests.iter().all(|request| request.method == "DELETE"
            && request.target.starts_with("/storage/v1/b/segments/o/a%2F")));
        let most = fake.most_in_flight.load(Ordering::SeqCst);
        assert!(
            (2..=DELETE_CONCURRENCY).contains(&most),
            "{most} deletes in flight at once"
        );
    }

    /// The variant of an error, which a test compares without its text.
    #[derive(Debug, PartialEq, Eq, PartialOrd, Ord)]
    enum ErrorClassName {
        InvalidKey,
        Auth,
        Unavailable,
        Other,
    }

    impl ErrorClassName {
        fn of(error: &ObjectStoreError) -> Self {
            match error {
                ObjectStoreError::InvalidKey { .. } => Self::InvalidKey,
                ObjectStoreError::Auth(_) => Self::Auth,
                ObjectStoreError::Unavailable(_) => Self::Unavailable,
                _ => Self::Other,
            }
        }
    }

    #[test]
    fn given_a_ranged_read_when_answered_should_take_the_generation_as_the_etag() {
        let fake = FakeGcs::start(|request| {
            assert_eq!(request.headers["range"], "bytes=2-4");
            (200, b"hello".to_vec())
        });
        let store = fake.store(GcsAuth::Anonymous);

        let result = crate::contract::shard_runtime().block_on(store.get_range("a/b", 2..5));

        assert!(
            matches!(&result, Err(ObjectStoreError::Malformed(reason)) if reason.contains(GENERATION_HEADER)),
            "an answer without the generation: {result:?}"
        );
        let [request] = fake.requests().try_into().unwrap();
        assert_eq!(request.target, "/storage/v1/b/segments/o/a%2Fb?alt=media");
    }
}
