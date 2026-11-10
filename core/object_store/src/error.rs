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

use std::io;

/// What a caller can do about a failed call. Retries and timeouts live above
/// [`crate::ObjectStore`], and they branch on the class.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum ErrorClass {
    /// No object exists under the key.
    NotFound,
    /// A conditional create found an object under the key.
    PreconditionFailed,
    /// The same call can succeed after a backoff. A put that failed this way
    /// can still have stored its object, see [`crate::ObjectStore::put`].
    Transient,
    /// The same call cannot succeed until an operator or the caller changes
    /// something, such as the credentials or the request.
    Permanent,
}

#[derive(Debug, thiserror::Error)]
pub enum ObjectStoreError {
    #[error("no object under {key:?}")]
    NotFound { key: String },
    #[error("an object already exists under {key:?}")]
    PreconditionFailed { key: String },
    #[error("the store throttled the request: {0}")]
    Throttled(String),
    /// The store answered with a server-side failure, such as an HTTP 5xx.
    #[error("the store is unavailable: {0}")]
    Unavailable(String),
    /// The request got no answer: a refused or reset connection, a failed
    /// name lookup or a failed TLS handshake.
    #[error("transport failed: {0}")]
    Transport(String),
    /// The store received bytes whose CRC32C differs from the one sent with
    /// them, so they changed on the way.
    #[error("the store received a body for {key:?} whose CRC32C does not match")]
    ChecksumMismatch { key: String },
    #[error("the store refused the credentials: {0}")]
    Auth(String),
    #[error("the bucket does not exist: {0}")]
    BucketNotFound(String),
    /// The object moved to an archive tier and needs a restore before a read.
    #[error("object {key:?} is archived")]
    Archived { key: String },
    #[error("malformed store response: {0}")]
    Malformed(String),
    #[error("range {start}..{end} starts at or past the end of {key:?}")]
    InvalidRange { key: String, start: u64, end: u64 },
    #[error("invalid key {key:?}: it {reason}")]
    InvalidKey { key: String, reason: &'static str },
    #[error("invalid request: {0}")]
    InvalidRequest(String),
    /// The caller's [`crate::PutBody`] failed or broke its declared length.
    /// It holds the body's own error, unchanged.
    #[error("the put body failed: {0}")]
    Body(#[source] io::Error),
}

impl ObjectStoreError {
    /// The class a retry policy acts on.
    ///
    /// A failed body is permanent for the call that sent it: the caller owns
    /// the body and decides whether a new one can succeed.
    #[must_use]
    pub const fn class(&self) -> ErrorClass {
        match self {
            Self::NotFound { .. } => ErrorClass::NotFound,
            Self::PreconditionFailed { .. } => ErrorClass::PreconditionFailed,
            Self::Throttled(_)
            | Self::Unavailable(_)
            | Self::Transport(_)
            | Self::ChecksumMismatch { .. } => ErrorClass::Transient,
            Self::Auth(_)
            | Self::BucketNotFound(_)
            | Self::Archived { .. }
            | Self::Malformed(_)
            | Self::InvalidRange { .. }
            | Self::InvalidKey { .. }
            | Self::InvalidRequest(_)
            | Self::Body(_) => ErrorClass::Permanent,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn given_each_error_when_classifying_should_match_its_retry_class() {
        let key = || "a/b".to_owned();
        let cases = [
            (
                ObjectStoreError::NotFound { key: key() },
                ErrorClass::NotFound,
            ),
            (
                ObjectStoreError::PreconditionFailed { key: key() },
                ErrorClass::PreconditionFailed,
            ),
            (
                ObjectStoreError::Throttled("slow down".to_owned()),
                ErrorClass::Transient,
            ),
            (
                ObjectStoreError::Unavailable("503".to_owned()),
                ErrorClass::Transient,
            ),
            (
                ObjectStoreError::Transport("reset".to_owned()),
                ErrorClass::Transient,
            ),
            (
                ObjectStoreError::ChecksumMismatch { key: key() },
                ErrorClass::Transient,
            ),
            (
                ObjectStoreError::Auth("denied".to_owned()),
                ErrorClass::Permanent,
            ),
            (
                ObjectStoreError::BucketNotFound("iggy".to_owned()),
                ErrorClass::Permanent,
            ),
            (
                ObjectStoreError::Archived { key: key() },
                ErrorClass::Permanent,
            ),
            (
                ObjectStoreError::Malformed("xml".to_owned()),
                ErrorClass::Permanent,
            ),
            (
                ObjectStoreError::InvalidRange {
                    key: key(),
                    start: 5,
                    end: 6,
                },
                ErrorClass::Permanent,
            ),
            (
                ObjectStoreError::InvalidKey {
                    key: key(),
                    reason: "is empty",
                },
                ErrorClass::Permanent,
            ),
            (
                ObjectStoreError::InvalidRequest("empty range".to_owned()),
                ErrorClass::Permanent,
            ),
            (
                ObjectStoreError::Body(io::Error::other("read failed")),
                ErrorClass::Permanent,
            ),
        ];

        for (error, class) in cases {
            assert_eq!(error.class(), class, "{error}");
        }
    }
}
