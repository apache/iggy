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

//! The provider contract, run against every provider in this crate, and
//! against a provider that refuses `IfAbsent`.

// Every future here runs on one thread, as on a shard.
#![allow(clippy::future_not_send)]

use std::ops::Range;

use iggy_object_store::{
    DeleteOutcome, ListPage, ListRequest, MemoryStore, ObjectMeta, ObjectRange, ObjectStore,
    ObjectStoreError, PutBody, PutMode, PutOptions,
};

/// A provider whose store cannot decide a conditional create atomically, so
/// it refuses `IfAbsent` and runs only the cases that every provider runs.
#[derive(Debug, Default)]
struct WithoutConditionalCreate(MemoryStore);

impl ObjectStore for WithoutConditionalCreate {
    async fn put(
        &self,
        key: &str,
        body: PutBody,
        options: &PutOptions,
    ) -> Result<(), ObjectStoreError> {
        match options.mode {
            PutMode::Overwrite => self.0.put(key, body, options).await,
            _ => Err(ObjectStoreError::Unsupported(format!(
                "put mode {:?}",
                options.mode
            ))),
        }
    }

    async fn get_range(
        &self,
        key: &str,
        range: Range<u64>,
    ) -> Result<ObjectRange, ObjectStoreError> {
        self.0.get_range(key, range).await
    }

    async fn head(&self, key: &str) -> Result<ObjectMeta, ObjectStoreError> {
        self.0.head(key).await
    }

    async fn delete(&self, keys: &[String]) -> Result<DeleteOutcome, ObjectStoreError> {
        self.0.delete(keys).await
    }

    async fn list(&self, request: &ListRequest<'_>) -> Result<ListPage, ObjectStoreError> {
        self.0.list(request).await
    }
}

macro_rules! fs_store {
    ($($case:ident),* $(,)?) => {$(
        #[test]
        fn $case() {
            let directory = tempfile::tempdir().expect("temporary directory");
            let store = iggy_object_store::FsStore::new(directory.path());
            iggy_object_store::contract::shard_runtime()
                .block_on(iggy_object_store::contract::$case(&store, stringify!($case)));
        }
    )*};
}

macro_rules! memory_store {
    ($($case:ident),* $(,)?) => {$(
        #[test]
        fn $case() {
            let store = iggy_object_store::MemoryStore::new();
            futures::executor::block_on(
                iggy_object_store::contract::$case(&store, stringify!($case)),
            );
        }
    )*};
}

macro_rules! without_conditional_create {
    ($($case:ident),* $(,)?) => {$(
        #[test]
        fn $case() {
            let store = crate::WithoutConditionalCreate::default();
            futures::executor::block_on(
                iggy_object_store::contract::$case(&store, stringify!($case)),
            );
        }
    )*};
}

mod fs_store {
    iggy_object_store::contract_cases!(fs_store);
    iggy_object_store::if_absent_contract_cases!(fs_store);
}

mod memory_store {
    iggy_object_store::contract_cases!(memory_store);
    iggy_object_store::if_absent_contract_cases!(memory_store);
}

mod without_conditional_create {
    iggy_object_store::contract_cases!(without_conditional_create);
    without_conditional_create! {
        given_store_without_atomic_create_when_putting_if_absent_should_refuse_as_unsupported,
    }
}
