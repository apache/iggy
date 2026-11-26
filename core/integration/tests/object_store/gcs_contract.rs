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

//! The provider contract of `iggy_object_store`, run against the GCS
//! provider and a bucket of its own in fake-gcs-server for each test.
//! Docker must run.
//!
//! The emulator decides the conditional creates, so the `IfAbsent` cases
//! run too. It checks no CRC32C, so the checksum cases exercise the check
//! that the provider runs on its own, not the one of GCS.

use iggy_object_store::gcs::{GcsConfig, GcsStore};
use iggy_object_store::{ListRequest, ObjectStore, ObjectStoreError};
use integration::harness::fake_gcs::{self, FakeGcsBucket};

macro_rules! gcs_store {
    ($($case:ident),* $(,)?) => {$(
        #[test]
        fn $case() {
            let bucket = bucket();
            iggy_object_store::contract::shard_runtime().block_on(async {
                let store = GcsStore::new(bucket.gcs_config()).expect("the GCS store");
                iggy_object_store::contract::$case(&store, stringify!($case)).await;
            });
        }
    )*};
}

/// The cases of uploads in parts, which the store takes only on Google's
/// endpoints unless they are turned on.
macro_rules! gcs_parts_store {
    ($($case:ident),* $(,)?) => {$(
        #[test]
        fn $case() {
            let bucket = bucket();
            iggy_object_store::contract::shard_runtime().block_on(async {
                let store = GcsStore::new(bucket.gcs_config())
                    .expect("the GCS store")
                    .with_uploads_in_parts(true);
                iggy_object_store::contract::$case(&store, stringify!($case)).await;
            });
        }
    )*};
}

iggy_object_store::contract_cases!(gcs_store);
iggy_object_store::if_absent_contract_cases!(gcs_store);
// fake-gcs-server composes over an object whatever `ifGenerationMatch` says,
// and answers a compose of a missing source with 500, not 404, so the cases
// of a refused or a lost upload in parts run against S3 and a real bucket
// only, and the server puts every object whole on the emulator.
gcs_parts_store!(
    given_parts_when_completing_should_create_the_whole_object_once,
    given_an_abandoned_upload_when_sweeping_should_remove_it_only_past_the_grace,
);

#[test]
fn given_missing_bucket_when_listing_should_fail_as_bucket_not_found() {
    let bucket = bucket();
    let missing = GcsConfig {
        bucket: format!("{}-missing", bucket.bucket),
        ..bucket.gcs_config()
    };

    let result = iggy_object_store::contract::shard_runtime().block_on(async {
        let store = GcsStore::new(missing)?;
        store.list(&ListRequest::new("contract/")).await.map(drop)
    });

    assert!(
        matches!(&result, Err(ObjectStoreError::BucketNotFound(_))),
        "{result:?}"
    );
}

fn bucket() -> FakeGcsBucket {
    tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .expect("a runtime for the fake-gcs-server fixture")
        .block_on(fake_gcs::new_bucket())
        .expect("a fake-gcs-server bucket: Docker must run")
}
