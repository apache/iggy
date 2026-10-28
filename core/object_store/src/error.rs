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

#[derive(Debug, thiserror::Error)]
pub enum ObjectStoreError {
    #[error("object not found")]
    NotFound,
    #[error("an object already exists under the key")]
    PreconditionFailed,
    #[error("requested range starts at or past the end of the object")]
    InvalidRange,
    #[error("store throttled the request: {0}")]
    Throttled(String),
    #[error("store refused the credentials or the signature: {0}")]
    Auth(String),
    #[error("object is archived and must be restored before it can be read")]
    Archived,
    #[error("store received a body whose checksum does not match")]
    ChecksumMismatch,
    #[error("transport failed: {0}")]
    Transport(String),
    #[error("malformed store response: {0}")]
    Malformed(String),
    #[error("reading the upload source failed: {0}")]
    Source(#[source] io::Error),
    #[error("store file operation failed: {0}")]
    Io(#[source] io::Error),
    #[error("{failed} of the keys were not deleted, first {key:?}: {code}")]
    DeleteFailed {
        failed: usize,
        key: String,
        code: String,
    },
    #[error("invalid key {0:?}")]
    InvalidKey(String),
    #[error("invalid request: {0}")]
    InvalidRequest(String),
    #[error("store answered {status} {code}: {message}")]
    Service {
        status: u16,
        code: String,
        message: String,
    },
}

impl ObjectStoreError {
    /// Whether the same request may succeed when sent again after a backoff.
    pub fn is_retryable(&self) -> bool {
        match self {
            Self::Throttled(_)
            | Self::Transport(_)
            | Self::ChecksumMismatch
            | Self::Io(_)
            | Self::DeleteFailed { .. } => true,
            Self::Service { status, .. } => *status >= 500,
            _ => false,
        }
    }
}
