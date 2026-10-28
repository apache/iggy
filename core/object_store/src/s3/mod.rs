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

//! S3 and S3-compatible stores (MinIO, Ceph RGW, R2) over cyper, signed with [`sigv4`].

mod sigv4;
mod xml;

pub use sigv4::Credentials;

use crate::body::read_piece;
use crate::resolver::OffloadResolver;
use crate::{
    ListPage, MAX_KEYS_PER_REQUEST, ObjectMeta, ObjectStore, ObjectStoreError, PutBody, PutOptions,
    UploadPacer, validate_key, validate_metadata, validate_prefix,
};
use base64::Engine;
use base64::engine::general_purpose::STANDARD as BASE64;
use bytes::Bytes;
use chrono::Utc;
use compio::fs::File;
use futures::stream;
use http::{Method, StatusCode};
use md5::{Digest, Md5};
use send_wrapper::SendWrapper;
use sigv4::{EMPTY_PAYLOAD_SHA256, SigningRequest, UNSIGNED_PAYLOAD};
use std::cell::RefCell;
use std::io;
use std::ops::Range;
use std::rc::Rc;
use url::Url;

const METADATA_HEADER_PREFIX: &str = "x-amz-meta-";

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct S3Config {
    /// Base URL of the service, such as `https://s3.eu-west-1.amazonaws.com` or
    /// `http://127.0.0.1:9000`.
    pub endpoint: String,
    pub region: String,
    pub bucket: String,
    /// Address the bucket in the path (`endpoint/bucket/key`), as MinIO and most compatible
    /// stores expect, instead of in the host name (`bucket.endpoint/key`).
    pub path_style: bool,
    pub credentials: Credentials,
}

pub struct S3Store {
    client: cyper::Client,
    scheme: String,
    host: String,
    bucket_path: String,
    region: String,
    credentials: Credentials,
}

/// One request before signing: everything the signature covers except the clock.
struct S3Request<'a> {
    method: Method,
    path: String,
    query: Vec<(&'a str, &'a str)>,
    headers: Vec<(String, String)>,
    payload_sha256: &'a str,
}

impl S3Store {
    pub fn new(config: S3Config) -> Result<Self, ObjectStoreError> {
        let endpoint = Url::parse(&config.endpoint).map_err(|error| {
            ObjectStoreError::InvalidRequest(format!("endpoint {:?}: {error}", config.endpoint))
        })?;
        let host = endpoint.host_str().ok_or_else(|| {
            ObjectStoreError::InvalidRequest(format!("endpoint {:?} has no host", config.endpoint))
        })?;
        if !matches!(endpoint.scheme(), "http" | "https") || endpoint.path() != "/" {
            return Err(ObjectStoreError::InvalidRequest(format!(
                "endpoint {:?} must be an http or https URL without a path",
                config.endpoint
            )));
        }
        let authority = match endpoint.port() {
            Some(port) => format!("{host}:{port}"),
            None => host.to_owned(),
        };
        let (host, bucket_path) = if config.path_style {
            (
                authority,
                format!("/{}", sigv4::encode_path(&config.bucket)),
            )
        } else {
            (format!("{}.{authority}", config.bucket), String::new())
        };
        // A redirect would resend a streamed body that is already consumed, under a signature
        // that names the original host.
        let client = cyper::Client::builder()
            .custom_resolver(OffloadResolver::default())
            .redirect(cyper::redirect::Policy::none())
            .build()
            .map_err(transport)?;
        Ok(Self {
            client,
            scheme: endpoint.scheme().to_owned(),
            host,
            bucket_path,
            region: config.region,
            credentials: config.credentials,
        })
    }

    /// Creates the bucket, succeeding when this account already owns it. For tests and local
    /// setups: in production the operator creates the bucket and its lifecycle rules.
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
        match error_from(response).await {
            ObjectStoreError::Service { code, .. } if code == "BucketAlreadyOwnedByYou" => Ok(()),
            error => Err(error),
        }
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

    /// Signs and sends `request`. `unsigned` headers are sent but left out of the signature.
    async fn send(
        &self,
        mut request: S3Request<'_>,
        unsigned: &[(&str, String)],
        body: cyper::Body,
    ) -> Result<cyper::Response, ObjectStoreError> {
        let now = Utc::now();
        request.headers.extend([
            ("host".to_owned(), self.host.clone()),
            ("x-amz-date".to_owned(), sigv4::amz_date(now)),
            (
                "x-amz-content-sha256".to_owned(),
                request.payload_sha256.to_owned(),
            ),
        ]);
        if let Some(token) = &self.credentials.session_token {
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
            &self.credentials,
            &self.region,
            now,
        );
        let mut url = format!("{}://{}{}", self.scheme, self.host, request.path);
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
        if options.if_absent {
            headers.push(("if-none-match".to_owned(), "*".to_owned()));
        }
        if let Some(checksum) = options.crc32c {
            headers.push((
                "x-amz-checksum-crc32c".to_owned(),
                BASE64.encode(checksum.to_be_bytes()),
            ));
        }
        // S3 refuses a chunked PUT, and a file stream does not know its length, so the header
        // is set here rather than left to the HTTP client.
        let unsigned = [("content-length", body.len().to_string())];
        let source_failure = Rc::new(RefCell::new(None));
        let body = match body {
            PutBody::Bytes(bytes) => cyper::Body::from(bytes),
            PutBody::File {
                file,
                offset,
                len,
                pacer,
            } => file_body(file, offset, len, pacer, Rc::clone(&source_failure)),
        };
        let request = S3Request {
            method: Method::PUT,
            path: self.object_path(key),
            query: Vec::new(),
            headers,
            payload_sha256: UNSIGNED_PAYLOAD,
        };
        let sent = self.send(request, &unsigned, body).await;
        if let Some(error) = source_failure.borrow_mut().take() {
            return Err(ObjectStoreError::Source(error));
        }
        let response = sent?;
        if response.status().is_success() {
            Ok(())
        } else {
            Err(error_from(response).await)
        }
    }

    async fn get_range(&self, key: &str, range: Range<u64>) -> Result<Bytes, ObjectStoreError> {
        validate_key(key)?;
        if range.is_empty() {
            return Err(ObjectStoreError::InvalidRequest(format!(
                "empty range {range:?}"
            )));
        }
        let request = S3Request {
            method: Method::GET,
            path: self.object_path(key),
            query: Vec::new(),
            headers: vec![(
                "range".to_owned(),
                format!("bytes={}-{}", range.start, range.end - 1),
            )],
            payload_sha256: EMPTY_PAYLOAD_SHA256,
        };
        let response = self.send(request, &[], cyper::Body::empty()).await?;
        let status = response.status();
        // A 200 means the store ignored the range, which is only harmless from offset zero.
        if status != StatusCode::PARTIAL_CONTENT && !(status == StatusCode::OK && range.start == 0)
        {
            return Err(if status.is_success() {
                ObjectStoreError::Malformed(format!("ranged GET answered {status}"))
            } else {
                error_from(response).await
            });
        }
        let bytes = response.bytes().await.map_err(transport)?;
        if bytes.len() as u64 > range.end - range.start {
            return Err(ObjectStoreError::Malformed(format!(
                "ranged GET returned {} bytes for {range:?}",
                bytes.len()
            )));
        }
        Ok(bytes)
    }

    async fn head(&self, key: &str) -> Result<ObjectMeta, ObjectStoreError> {
        validate_key(key)?;
        let request = S3Request {
            method: Method::HEAD,
            path: self.object_path(key),
            query: Vec::new(),
            headers: Vec::new(),
            payload_sha256: EMPTY_PAYLOAD_SHA256,
        };
        let response = self.send(request, &[], cyper::Body::empty()).await?;
        if !response.status().is_success() {
            return Err(error_from(response).await);
        }
        let headers = response.headers();
        let size = headers
            .get(http::header::CONTENT_LENGTH)
            .and_then(|value| value.to_str().ok())
            .and_then(|value| value.parse().ok())
            .ok_or_else(|| ObjectStoreError::Malformed("HEAD without Content-Length".to_owned()))?;
        let etag = headers
            .get(http::header::ETAG)
            .and_then(|value| value.to_str().ok())
            .map(|value| value.trim_matches('"').to_owned())
            .unwrap_or_default();
        let mut metadata: Vec<(String, String)> = headers
            .iter()
            .filter_map(|(name, value)| {
                let name = name.as_str().strip_prefix(METADATA_HEADER_PREFIX)?;
                Some((name.to_owned(), value.to_str().ok()?.to_owned()))
            })
            .collect();
        metadata.sort();
        Ok(ObjectMeta {
            size,
            etag,
            metadata,
        })
    }

    async fn delete(&self, keys: &[String]) -> Result<(), ObjectStoreError> {
        if keys.is_empty() {
            return Ok(());
        }
        if keys.len() > MAX_KEYS_PER_REQUEST {
            return Err(ObjectStoreError::InvalidRequest(format!(
                "{} keys in one delete, at most {MAX_KEYS_PER_REQUEST}",
                keys.len()
            )));
        }
        for key in keys {
            validate_key(key)?;
        }
        let body = xml::delete_request(keys);
        let payload_sha256 = sigv4::sha256_hex(body.as_bytes());
        // Multi-object delete requires a body digest. Content-MD5 is the one every
        // S3-compatible store accepts.
        let request = S3Request {
            method: Method::POST,
            path: self.bucket_root(),
            query: vec![("delete", "")],
            headers: vec![
                (
                    "content-md5".to_owned(),
                    BASE64.encode(Md5::digest(body.as_bytes())),
                ),
                ("content-type".to_owned(), "application/xml".to_owned()),
            ],
            payload_sha256: &payload_sha256,
        };
        let response = self.send(request, &[], cyper::Body::from(body)).await?;
        if !response.status().is_success() {
            return Err(error_from(response).await);
        }
        xml::parse_delete(&response.bytes().await.map_err(transport)?)
    }

    async fn list(
        &self,
        prefix: &str,
        continuation: Option<&str>,
    ) -> Result<ListPage, ObjectStoreError> {
        validate_prefix(prefix)?;
        let max_keys = MAX_KEYS_PER_REQUEST.to_string();
        let mut query = vec![("list-type", "2"), ("max-keys", max_keys.as_str())];
        if !prefix.is_empty() {
            query.push(("prefix", prefix));
        }
        if let Some(token) = continuation {
            query.push(("continuation-token", token));
        }
        let request = S3Request {
            method: Method::GET,
            path: self.bucket_root(),
            query,
            headers: Vec::new(),
            payload_sha256: EMPTY_PAYLOAD_SHA256,
        };
        let response = self.send(request, &[], cyper::Body::empty()).await?;
        if !response.status().is_success() {
            return Err(error_from(response).await);
        }
        xml::parse_list(&response.bytes().await.map_err(transport)?)
    }
}

/// Streams `len` bytes of `file` one piece at a time. A read failure aborts the request and is
/// left in `failure`, so the caller reports a broken source rather than a broken network.
fn file_body(
    file: File,
    offset: u64,
    len: u64,
    pacer: Option<Rc<UploadPacer>>,
    failure: Rc<RefCell<Option<io::Error>>>,
) -> cyper::Body {
    let pieces = stream::try_unfold((file, 0), move |(file, sent)| {
        let failure = Rc::clone(&failure);
        let pacer = pacer.clone();
        async move {
            if sent == len {
                return Ok(None);
            }
            match read_piece(&file, offset, len, sent, pacer.as_deref()).await {
                Ok(piece) => {
                    let next = sent + piece.len() as u64;
                    Ok(Some((piece, (file, next))))
                }
                Err(error) => {
                    let kind = error.kind();
                    *failure.borrow_mut() = Some(error);
                    Err(cyper::Error::System(io::Error::from(kind)))
                }
            }
        }
    });
    // The client polls the body on the shard thread that built it.
    cyper::Body::stream(SendWrapper::new(pieces))
}

async fn error_from(response: cyper::Response) -> ObjectStoreError {
    let status = response.status().as_u16();
    let body = response.bytes().await.unwrap_or_default();
    let error = xml::parse_error(&body);
    classify(status, error.code, error.message)
}

fn classify(status: u16, code: String, message: String) -> ObjectStoreError {
    let detail = || format!("{status} {code}: {message}");
    match (status, code.as_str()) {
        (412, _) => ObjectStoreError::PreconditionFailed,
        (404, "NoSuchBucket") => ObjectStoreError::Service {
            status,
            code,
            message,
        },
        (404, _) => ObjectStoreError::NotFound,
        (416, _) => ObjectStoreError::InvalidRange,
        (403, "InvalidObjectState") => ObjectStoreError::Archived,
        (
            400,
            "BadDigest"
            | "InvalidDigest"
            | "XAmzContentChecksumMismatch"
            | "XAmzContentSHA256Mismatch",
        ) => ObjectStoreError::ChecksumMismatch,
        (401 | 403, _)
        | (
            400,
            "AuthorizationHeaderMalformed" | "ExpiredToken" | "InvalidAccessKeyId" | "InvalidToken",
        ) => ObjectStoreError::Auth(detail()),
        // A conflicting concurrent conditional write: AWS asks for a retry.
        (409, "ConditionalRequestConflict") | (429 | 503, _) | (_, "SlowDown") => {
            ObjectStoreError::Throttled(detail())
        }
        (400, "RequestTimeout") => ObjectStoreError::Transport(detail()),
        _ => ObjectStoreError::Service {
            status,
            code,
            message,
        },
    }
}

fn transport(error: cyper::Error) -> ObjectStoreError {
    ObjectStoreError::Transport(error.to_string())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn classified(status: u16, code: &str) -> ObjectStoreError {
        classify(status, code.to_owned(), String::new())
    }

    #[test]
    fn given_store_errors_when_classifying_should_map_to_typed_errors() {
        assert!(matches!(
            classified(404, "NoSuchKey"),
            ObjectStoreError::NotFound
        ));
        assert!(matches!(classified(404, ""), ObjectStoreError::NotFound));
        assert!(matches!(
            classified(404, "NoSuchBucket"),
            ObjectStoreError::Service { status: 404, .. }
        ));
        assert!(matches!(
            classified(412, ""),
            ObjectStoreError::PreconditionFailed
        ));
        assert!(matches!(
            classified(416, "InvalidRange"),
            ObjectStoreError::InvalidRange
        ));
        assert!(matches!(
            classified(403, "InvalidObjectState"),
            ObjectStoreError::Archived
        ));
        assert!(matches!(
            classified(403, "SignatureDoesNotMatch"),
            ObjectStoreError::Auth(_)
        ));
        assert!(matches!(
            classified(400, "AuthorizationHeaderMalformed"),
            ObjectStoreError::Auth(_)
        ));
        assert!(matches!(
            classified(503, "SlowDown"),
            ObjectStoreError::Throttled(_)
        ));
        assert!(matches!(
            classified(409, "ConditionalRequestConflict"),
            ObjectStoreError::Throttled(_)
        ));
        assert!(matches!(
            classified(400, "XAmzContentChecksumMismatch"),
            ObjectStoreError::ChecksumMismatch
        ));
        assert!(classified(500, "InternalError").is_retryable());
        assert!(!classified(400, "InvalidArgument").is_retryable());
    }
}
