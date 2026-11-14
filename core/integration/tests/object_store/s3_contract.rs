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

//! The provider contract of `iggy_object_store`, run against the S3 provider
//! and a bucket of its own in Floci for each test. Docker must run.

use iggy_object_store::s3::{Credentials, S3Config, S3Store};
use iggy_object_store::{ListRequest, ObjectStore, ObjectStoreError};
use integration::harness::floci::{self, ACCESS_KEY_ID, FlociBucket};

macro_rules! s3_store {
    ($($case:ident),* $(,)?) => {$(
        #[test]
        fn $case() {
            let bucket = bucket();
            iggy_object_store::contract::shard_runtime().block_on(async {
                let store = S3Store::new(bucket.s3_config()).expect("the S3 store");
                iggy_object_store::contract::$case(&store, stringify!($case)).await;
            });
        }
    )*};
}

iggy_object_store::contract_cases!(s3_store);
iggy_object_store::if_absent_contract_cases!(s3_store);

/// A wrong secret is a refusal that no retry gets past.
#[test]
fn given_wrong_secret_when_listing_should_fail_as_auth() {
    let bucket = bucket();
    let wrong = S3Config {
        credentials: Credentials {
            access_key_id: ACCESS_KEY_ID.to_owned(),
            secret_access_key: "wrong".to_owned(),
            session_token: None,
        }
        .into(),
        ..bucket.s3_config()
    };

    let result = list(wrong);

    assert!(
        matches!(&result, Err(ObjectStoreError::Auth(_))),
        "{result:?}"
    );
}

#[test]
fn given_missing_bucket_when_listing_should_fail_as_bucket_not_found() {
    let bucket = bucket();
    let missing = S3Config {
        bucket: format!("{}-missing", bucket.bucket),
        ..bucket.s3_config()
    };

    let result = list(missing);

    assert!(
        matches!(&result, Err(ObjectStoreError::BucketNotFound(_))),
        "{result:?}"
    );
}

fn bucket() -> FlociBucket {
    tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .expect("a runtime for the Floci fixture")
        .block_on(floci::new_bucket())
        .expect("a Floci bucket: Docker must run")
}

fn list(config: S3Config) -> Result<(), ObjectStoreError> {
    iggy_object_store::contract::shard_runtime().block_on(async {
        let store = S3Store::new(config)?;
        store.list(&ListRequest::new("contract/")).await.map(drop)
    })
}
