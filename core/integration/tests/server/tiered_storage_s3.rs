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

//! A server with the `s3` tiered storage backend, against the Floci bucket
//! that the harness makes for it. Docker must run.

use std::time::Duration;

use iggy_object_store::s3::S3Store;
use iggy_object_store::{IdentityRead, KeyLayout, StoreIdentity};
use integration::harness::floci::FlociBucket;
use integration::iggy_harness;
use tokio::time::{Instant, sleep};

/// How long the store identity check of a new server takes at most to claim
/// its bucket.
const CLAIM_DEADLINE: Duration = Duration::from_secs(30);
const POLL_INTERVAL: Duration = Duration::from_millis(200);

#[iggy_harness(server(tiered_storage.backend = "s3"))]
async fn given_s3_tiered_storage_when_the_server_boots_should_claim_its_bucket(
    harness: &TestHarness,
) {
    let bucket = harness
        .tiered_storage_bucket()
        .expect("the harness makes a bucket for the s3 backend")
        .clone();
    let deadline = Instant::now() + CLAIM_DEADLINE;

    loop {
        let read = read_identity(bucket.clone()).await;
        if matches!(read, IdentityRead::Found(_)) {
            return;
        }
        assert!(
            Instant::now() < deadline,
            "the server did not claim {bucket:?}: {read:?}"
        );
        sleep(POLL_INTERVAL).await;
    }
}

/// The S3 provider runs on compio, not on the harness's runtime.
async fn read_identity(bucket: FlociBucket) -> IdentityRead {
    tokio::task::spawn_blocking(move || {
        compio::runtime::Runtime::new()
            .expect("a compio runtime")
            .block_on(async {
                let store = S3Store::new(bucket.s3_config()).expect("the S3 store");
                let layout = KeyLayout::new("iggy").expect("the default prefix");
                StoreIdentity::read(&store, &layout)
                    .await
                    .expect("the read of the identity object")
            })
    })
    .await
    .expect("the identity task")
}
