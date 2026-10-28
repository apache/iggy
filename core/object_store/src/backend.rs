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

use crate::fs::FsStore;
use crate::s3::S3Store;
use crate::{ListPage, ObjectMeta, ObjectStore, ObjectStoreError, PutBody, PutOptions};
use bytes::Bytes;
use std::ops::Range;

/// The configured provider, dispatched statically: the trait's async methods are not object safe,
/// and the set of providers is closed.
pub enum Backend {
    Fs(FsStore),
    S3(S3Store),
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
        }
    }

    async fn get_range(&self, key: &str, range: Range<u64>) -> Result<Bytes, ObjectStoreError> {
        match self {
            Self::Fs(store) => store.get_range(key, range).await,
            Self::S3(store) => store.get_range(key, range).await,
        }
    }

    async fn head(&self, key: &str) -> Result<ObjectMeta, ObjectStoreError> {
        match self {
            Self::Fs(store) => store.head(key).await,
            Self::S3(store) => store.head(key).await,
        }
    }

    async fn delete(&self, keys: &[String]) -> Result<(), ObjectStoreError> {
        match self {
            Self::Fs(store) => store.delete(keys).await,
            Self::S3(store) => store.delete(keys).await,
        }
    }

    async fn list(
        &self,
        prefix: &str,
        continuation: Option<&str>,
    ) -> Result<ListPage, ObjectStoreError> {
        match self {
            Self::Fs(store) => store.list(prefix, continuation).await,
            Self::S3(store) => store.list(prefix, continuation).await,
        }
    }
}
