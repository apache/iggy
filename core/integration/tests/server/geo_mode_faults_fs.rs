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

//! A node with the `fs` object store, whose store goes down while a geo
//! topic and a normal topic take sends.

use std::collections::HashMap;
use std::path::Path;

use integration::harness::{TestHarness, TestServerConfig};

use crate::server::scenarios::geo_mode_faults_scenario::{
    Bucket, FsStoreOutage, RawSender, Topics, assert_settled, produce_through_store_fault,
};

#[tokio::test]
async fn given_a_geo_topic_when_its_store_stops_answering_should_refuse_geo_sends_and_keep_the_normal_topic_serving()
 {
    let store = tempfile::tempdir().expect("a directory for the object store");
    let mut harness = server_with_fs_store(store.path());
    harness.start().await.expect("the server boots");
    let client = harness.tcp_root_client().await.expect("tcp root client");
    let topics = Topics::create(&client, "geo-store-outage").await;
    let node = harness
        .server()
        .tcp_addr()
        .expect("the server's tcp address");
    let mut raw = RawSender::connect(node, &client, &topics).await;
    let mut outage = FsStoreOutage::new(store.path());

    let ledger = produce_through_store_fault(&client, &mut raw, &topics, &mut outage).await;

    assert_settled(
        &client,
        &topics,
        &ledger,
        &Bucket::Fs(store.path().to_owned()),
    )
    .await;
}

fn server_with_fs_store(store: &Path) -> TestHarness {
    let extra_envs = HashMap::from([
        // A geo topic takes messages once the metadata primary stored its
        // object, and a node without an origin stores none.
        ("IGGY_GEO_ORIGIN".to_owned(), "dc-a".to_owned()),
        ("IGGY_TIERED_STORAGE_BACKEND".to_owned(), "fs".to_owned()),
        (
            "IGGY_TIERED_STORAGE_PATH".to_owned(),
            store.to_string_lossy().into_owned(),
        ),
        (
            "IGGY_DATA_MAINTENANCE_MESSAGES_CLEANER_ENABLED".to_owned(),
            "true".to_owned(),
        ),
        (
            "IGGY_DATA_MAINTENANCE_MESSAGES_INTERVAL".to_owned(),
            "100ms".to_owned(),
        ),
    ]);
    TestHarness::builder()
        .cluster_nodes(1)
        .server(TestServerConfig::builder().extra_envs(extra_envs).build())
        .build()
        .expect("the harness")
}
