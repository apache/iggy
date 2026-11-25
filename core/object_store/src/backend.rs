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

//! The provider that a configuration names.

use std::ops::Range;
use std::time::Duration;

use crate::error::ObjectStoreError;
use crate::fs::FsStore;
use crate::gcs::GcsStore;
use crate::layout::KeyLayout;
use crate::mock_s3::MockS3Store;
use crate::s3::S3Store;
use crate::store::{
    DeleteOutcome, ListPage, ListRequest, ObjectMeta, ObjectRange, ObjectStore, PartsLimits,
    PartsUpload, PutBody, PutOptions, StoredPart, SweptUploads,
};

/// One of the providers, chosen at boot.
///
/// The methods of [`ObjectStore`] return `impl Future`, so a provider cannot
/// sit behind `dyn`. The set of providers is closed, so an enum dispatches
/// each call instead.
pub enum Backend {
    Fs(FsStore),
    S3(S3Store),
    Gcs(GcsStore),
    MockS3(MockS3Store),
}

impl ObjectStore for Backend {
    async fn put(
        &self,
        key: &str,
        body: PutBody,
        options: &PutOptions,
    ) -> Result<(), ObjectStoreError> {
        match self {
            Self::Fs(store) => store.put(key, body, options).await,
            Self::S3(store) => store.put(key, body, options).await,
            Self::Gcs(store) => store.put(key, body, options).await,
            Self::MockS3(store) => store.put(key, body, options).await,
        }
    }

    async fn get_range(
        &self,
        key: &str,
        range: Range<u64>,
    ) -> Result<ObjectRange, ObjectStoreError> {
        match self {
            Self::Fs(store) => store.get_range(key, range).await,
            Self::S3(store) => store.get_range(key, range).await,
            Self::Gcs(store) => store.get_range(key, range).await,
            Self::MockS3(store) => store.get_range(key, range).await,
        }
    }

    async fn head(&self, key: &str) -> Result<ObjectMeta, ObjectStoreError> {
        match self {
            Self::Fs(store) => store.head(key).await,
            Self::S3(store) => store.head(key).await,
            Self::Gcs(store) => store.head(key).await,
            Self::MockS3(store) => store.head(key).await,
        }
    }

    async fn delete(&self, keys: &[String]) -> Result<DeleteOutcome, ObjectStoreError> {
        match self {
            Self::Fs(store) => store.delete(keys).await,
            Self::S3(store) => store.delete(keys).await,
            Self::Gcs(store) => store.delete(keys).await,
            Self::MockS3(store) => store.delete(keys).await,
        }
    }

    async fn list(&self, request: &ListRequest<'_>) -> Result<ListPage, ObjectStoreError> {
        match self {
            Self::Fs(store) => store.list(request).await,
            Self::S3(store) => store.list(request).await,
            Self::Gcs(store) => store.list(request).await,
            Self::MockS3(store) => store.list(request).await,
        }
    }

    fn parts_limits(&self) -> Option<PartsLimits> {
        match self {
            Self::Fs(store) => store.parts_limits(),
            Self::S3(store) => store.parts_limits(),
            Self::Gcs(store) => store.parts_limits(),
            Self::MockS3(store) => store.parts_limits(),
        }
    }

    async fn begin_parts(
        &self,
        key: &str,
        options: &PutOptions,
        layout: &KeyLayout,
    ) -> Result<PartsUpload, ObjectStoreError> {
        match self {
            Self::Fs(store) => store.begin_parts(key, options, layout).await,
            Self::S3(store) => store.begin_parts(key, options, layout).await,
            Self::Gcs(store) => store.begin_parts(key, options, layout).await,
            Self::MockS3(store) => store.begin_parts(key, options, layout).await,
        }
    }

    async fn put_part(
        &self,
        upload: &PartsUpload,
        number: u32,
        body: PutBody,
        crc32c: u32,
    ) -> Result<StoredPart, ObjectStoreError> {
        match self {
            Self::Fs(store) => store.put_part(upload, number, body, crc32c).await,
            Self::S3(store) => store.put_part(upload, number, body, crc32c).await,
            Self::Gcs(store) => store.put_part(upload, number, body, crc32c).await,
            Self::MockS3(store) => store.put_part(upload, number, body, crc32c).await,
        }
    }

    async fn complete_parts(
        &self,
        upload: &PartsUpload,
        parts: &[StoredPart],
        options: &PutOptions,
        len: u64,
    ) -> Result<(), ObjectStoreError> {
        match self {
            Self::Fs(store) => store.complete_parts(upload, parts, options, len).await,
            Self::S3(store) => store.complete_parts(upload, parts, options, len).await,
            Self::Gcs(store) => store.complete_parts(upload, parts, options, len).await,
            Self::MockS3(store) => store.complete_parts(upload, parts, options, len).await,
        }
    }

    async fn abort_parts(&self, upload: &PartsUpload, parts: u32) -> Result<(), ObjectStoreError> {
        match self {
            Self::Fs(store) => store.abort_parts(upload, parts).await,
            Self::S3(store) => store.abort_parts(upload, parts).await,
            Self::Gcs(store) => store.abort_parts(upload, parts).await,
            Self::MockS3(store) => store.abort_parts(upload, parts).await,
        }
    }

    async fn sweep_uploads(
        &self,
        layout: &KeyLayout,
        older_than: Duration,
        after: Option<&str>,
    ) -> Result<SweptUploads, ObjectStoreError> {
        match self {
            Self::Fs(store) => store.sweep_uploads(layout, older_than, after).await,
            Self::S3(store) => store.sweep_uploads(layout, older_than, after).await,
            Self::Gcs(store) => store.sweep_uploads(layout, older_than, after).await,
            Self::MockS3(store) => store.sweep_uploads(layout, older_than, after).await,
        }
    }
}
