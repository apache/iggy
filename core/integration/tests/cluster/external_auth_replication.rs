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

//! Cluster-mode external auth: the reserved `external_auth_user_id` rides
//! the snapshot so every replica's in-apply authorization gate produces
//! identical decisions regardless of node-local config.

use iggy::prelude::*;
use integration::harness::TestHarnessBuilder;
use integration::harness::config::TestServerConfig;
use integration::harness::ext_auth_server::{EXT_AUTH_PASSWORD, EXT_AUTH_USERNAME, ExtAuthServer};
use std::collections::HashMap;
use std::time::Duration;
use tokio::time::{Instant, sleep};

const LOGIN_BUDGET: Duration = Duration::from_secs(60);
const LOGIN_RETRY: Duration = Duration::from_millis(250);

fn ext_auth_cluster_config(url: String) -> TestServerConfig {
    TestServerConfig::builder()
        .extra_envs(HashMap::from([
            ("IGGY_EXTERNAL_AUTH_ENABLED".to_string(), "true".to_string()),
            ("IGGY_EXTERNAL_AUTH_URL".to_string(), url),
            (
                "IGGY_EXTERNAL_AUTH_FORWARD_CREDENTIALS".to_string(),
                "true".to_string(),
            ),
            ("IGGY_EXTERNAL_AUTH_TIMEOUT".to_string(), "5 s".to_string()),
        ]))
        .build()
}

/// Wait until the ext-auth client can see the named stream, retrying through
/// replication lag.
async fn wait_for_stream(ext: &IggyClient, stream_name: &str) {
    let deadline = Instant::now() + LOGIN_BUDGET;
    loop {
        match ext.get_streams().await {
            Ok(streams) if streams.iter().any(|s| s.name == stream_name) => return,
            Ok(_) => sleep(LOGIN_RETRY).await,
            Err(_) if Instant::now() < deadline => sleep(LOGIN_RETRY).await,
            Err(e) => panic!("ext-auth user cannot list streams: {e}"),
        }
    }
}

#[tokio::test]
async fn given_cluster_when_ext_auth_login_should_replicate_and_authorize() {
    let auth_server = ExtAuthServer::start().await;

    let mut harness = TestHarnessBuilder::default()
        .test_name("cluster__ext_auth_replication__given_cluster_when_ext_auth_login_should_replicate_and_authorize")
        .server(ext_auth_cluster_config(auth_server.url()))
        .cluster_nodes(3)
        .build()
        .expect("build test harness");

    harness.start().await.expect("start cluster");

    let root = harness.root_client_for_node(0).await.unwrap();
    root.create_stream("ext-auth-cluster").await.unwrap();

    // Ext-auth login on node 0 (the primary in a fresh cluster).
    let ext = harness
        .node(0)
        .tcp_client()
        .unwrap()
        .with_login(EXT_AUTH_USERNAME, EXT_AUTH_PASSWORD)
        .connect()
        .await
        .unwrap();

    wait_for_stream(&ext, "ext-auth-cluster").await;

    let streams = ext.get_streams().await.unwrap();
    assert!(
        streams.iter().any(|s| s.name == "ext-auth-cluster"),
        "ext-auth user should see the stream"
    );

    let denied = ext.create_stream("should-fail").await;
    assert!(denied.is_err(), "ext-auth user must not create streams");
}

#[tokio::test]
async fn given_cluster_when_ext_auth_login_on_follower_should_forward_and_authorize() {
    let auth_server = ExtAuthServer::start().await;

    let mut harness = TestHarnessBuilder::default()
        .test_name("cluster__ext_auth_replication__given_cluster_when_ext_auth_login_on_follower_should_forward_and_authorize")
        .server(ext_auth_cluster_config(auth_server.url()))
        .cluster_nodes(3)
        .build()
        .expect("build test harness");

    harness.start().await.expect("start cluster");

    let root = harness.root_client_for_node(0).await.unwrap();
    root.create_stream("ext-auth-follower").await.unwrap();

    // Ext-auth login on node 2 (a follower); the Register op is forwarded
    // through consensus to the primary.
    let ext = harness
        .node(2)
        .tcp_client()
        .unwrap()
        .with_login(EXT_AUTH_USERNAME, EXT_AUTH_PASSWORD)
        .connect()
        .await
        .unwrap();

    wait_for_stream(&ext, "ext-auth-follower").await;

    let streams = ext.get_streams().await.unwrap();
    assert!(
        streams.iter().any(|s| s.name == "ext-auth-follower"),
        "ext-auth user on follower should see the stream"
    );

    let denied = ext.create_stream("should-fail-follower").await;
    assert!(
        denied.is_err(),
        "ext-auth user on follower must not create streams"
    );
}
