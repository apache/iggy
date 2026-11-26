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

//! Three nodes that share one `fs` store and a tiered topic on it.

use std::collections::{BTreeMap, HashMap};
use std::path::Path;
use std::time::Duration;

use iggy::prelude::*;
use integration::harness::{TestHarness, TestServerConfig, resolve_config_paths};
use tokio::time::{Instant, sleep};

use crate::server::http_client::{HttpClient, node_url};
use crate::server::tiered_storage_fs::{
    POLL_INTERVAL, STREAM, TIERING_DEADLINE, TOPIC, assert_manifest_lists_stored_segments,
    assert_reads_every_message, create_tiered_topic, fs_store_envs, read_every_message,
    segment_logs, send_every_message, tiered_topic_options,
};

const NODES: usize = 3;
/// How long the restarted cluster may take to serve the topic again.
const RESTART_DEADLINE: Duration = Duration::from_secs(60);
/// How long the nodes may take to settle their store identity checks.
const IDENTITY_DEADLINE: Duration = Duration::from_secs(60);
/// Several metadata probe rounds: a primary that admitted a tiered topic
/// without the report of every node would do so within one.
const REFUSAL_WINDOW: Duration = Duration::from_secs(10);
const STORE_PATH_ENV: &str = "IGGY_TIERED_STORAGE_PATH";

/// Every node evicts before the restart, so whichever node leads the
/// partition after it has to read the evicted segments from the store.
#[tokio::test]
async fn given_three_nodes_sharing_an_fs_store_when_each_evicts_tiered_segments_should_read_them_back_after_a_restart()
 {
    let store = tempfile::tempdir().expect("a directory for the object store");
    let mut harness = cluster_with_fs_store(store.path());
    harness.start().await.expect("the cluster boots");
    let client = harness.tcp_root_client().await.expect("tcp root client");
    let stream = Identifier::named(STREAM).expect("stream identifier");
    let topic = Identifier::named(TOPIC).expect("topic identifier");
    client
        .create_stream(STREAM, &StreamCreateOptions::default())
        .await
        .expect("create stream");
    create_tiered_topic(&client, &stream).await;
    send_every_message(&client, &stream, &topic).await;

    let evicted = wait_for_eviction_on_every_node(&harness, store.path()).await;
    assert_reads_every_message(&client, &stream, &topic).await;
    assert_manifest_lists_stored_segments(store.path()).await;
    let uploaded = segment_logs(store.path());

    drop(client);
    harness
        .restart_cluster()
        .await
        .expect("the cluster restarts");
    read_back_after_restart(&harness, &stream, &topic).await;

    for (node, segments) in &evicted {
        let local = segment_logs(&harness.node(*node).data_path());
        assert!(
            segments.iter().all(|segment| !local.contains(segment)),
            "node {node} holds an evicted segment on its disk again: evicted {segments:?}, on \
             disk {local:?}"
        );
    }
    let stored = segment_logs(store.path());
    assert!(
        uploaded.iter().all(|segment| stored.contains(segment)),
        "the store lost segments over the restart: it held {uploaded:?}, it holds {stored:?}"
    );
}

/// The last node reaches a store of its own, where replica 0 never claims
/// the root, so its identity check does not pass while the other two do.
#[tokio::test]
async fn given_a_node_that_fails_its_store_identity_check_when_a_tiered_topic_is_created_should_refuse_it_until_the_node_passes()
 {
    let store = tempfile::tempdir().expect("a directory for the object store");
    let elsewhere = tempfile::tempdir().expect("a directory for another store");
    let mut harness = cluster_with_fs_store(store.path());
    harness
        .node_mut(NODES - 1)
        .add_env(STORE_PATH_ENV, elsewhere.path().to_string_lossy());
    harness.start().await.expect("the cluster boots");
    wait_for_identity_checks(&harness, [true, true, false]).await;
    let client = harness.tcp_root_client().await.expect("tcp root client");
    let stream = Identifier::named(STREAM).expect("stream identifier");
    client
        .create_stream(STREAM, &StreamCreateOptions::default())
        .await
        .expect("create stream");

    let refused_until = Instant::now() + REFUSAL_WINDOW;
    while Instant::now() < refused_until {
        let created = client
            .create_topic(&stream, TOPIC, &tiered_topic_options())
            .await;
        assert!(
            matches!(created, Err(IggyError::FeatureUnavailable)),
            "a tiered topic was created while a node fails its store identity check: \
             {created:?}"
        );
        sleep(POLL_INTERVAL).await;
    }

    harness
        .node_mut(NODES - 1)
        .add_env(STORE_PATH_ENV, store.path().to_string_lossy());
    harness
        .restart_node(NODES - 1)
        .expect("the node restarts on the shared store");
    create_tiered_topic(&client, &stream).await;
}

fn cluster_with_fs_store(store: &Path) -> TestHarness {
    let mut envs = fs_store_envs(store);
    envs.extend(
        resolve_config_paths(&HashMap::from([(
            "sharding.cpu_allocation".to_owned(),
            "2".to_owned(),
        )]))
        .expect("valid server config paths"),
    );
    TestHarness::builder()
        .server(TestServerConfig::builder().extra_envs(envs).build())
        .cluster_nodes(NODES)
        .build()
        .expect("the harness")
}

/// Waits until every node lacks a segment that the store holds, and returns
/// the segments each node evicted.
async fn wait_for_eviction_on_every_node(
    harness: &TestHarness,
    store: &Path,
) -> BTreeMap<usize, Vec<String>> {
    let deadline = Instant::now() + TIERING_DEADLINE;
    loop {
        let uploaded = segment_logs(store);
        let evicted: BTreeMap<usize, Vec<String>> = (0..NODES)
            .map(|node| {
                let local = segment_logs(&harness.node(node).data_path());
                let gone = uploaded
                    .iter()
                    .filter(|segment| !local.contains(segment))
                    .cloned()
                    .collect();
                (node, gone)
            })
            .collect();
        if evicted.values().all(|segments| !segments.is_empty()) {
            return evicted;
        }
        assert!(
            Instant::now() < deadline,
            "within {TIERING_DEADLINE:?} not every node evicted an uploaded segment: the store \
             holds {uploaded:?}, the nodes evicted {evicted:?}"
        );
        sleep(POLL_INTERVAL).await;
    }
}

/// Waits until `object_store_identity_verified` of each node is what
/// `verified` gives for it.
async fn wait_for_identity_checks(harness: &TestHarness, verified: [bool; NODES]) {
    let mut sessions = Vec::with_capacity(NODES);
    for node in 0..NODES {
        sessions.push(HttpClient::login_root_no_redirect(node_url(harness, node)).await);
    }
    let deadline = Instant::now() + IDENTITY_DEADLINE;
    loop {
        let mut observed = [false; NODES];
        for (node, session) in sessions.iter().enumerate() {
            observed[node] = identity_verified(session).await;
        }
        if observed == verified {
            return;
        }
        assert!(
            Instant::now() < deadline,
            "within {IDENTITY_DEADLINE:?} the nodes did not settle their store identity checks: \
             expected {verified:?}, observed {observed:?}"
        );
        sleep(POLL_INTERVAL).await;
    }
}

/// Whether a shard of the node behind `session` reports that the node passes
/// its store identity check. Every shard of a node reports the same gauge.
async fn identity_verified(session: &HttpClient) -> bool {
    const GAUGE: &str = "object_store_identity_verified";
    let text = session
        .get("/metrics")
        .await
        .text()
        .await
        .unwrap_or_default();
    text.lines()
        .filter(|line| {
            line.strip_prefix(GAUGE)
                .is_some_and(|rest| rest.starts_with('{') || rest.starts_with(' '))
        })
        .filter_map(|line| line.rsplit(' ').next()?.parse::<f64>().ok())
        .any(|value| value >= 1.0)
}

/// Reads every message once the restarted cluster serves the topic again.
async fn read_back_after_restart(harness: &TestHarness, stream: &Identifier, topic: &Identifier) {
    let deadline = Instant::now() + RESTART_DEADLINE;
    loop {
        let read = match harness.tcp_root_client().await {
            Ok(client) => read_every_message(&client, stream, topic).await,
            Err(error) => Err(format!("connect: {error}")),
        };
        match read {
            Ok(()) => return,
            Err(error) => assert!(
                Instant::now() < deadline,
                "within {RESTART_DEADLINE:?} of the restart the cluster did not serve every \
                 message: {error}"
            ),
        }
        sleep(POLL_INTERVAL).await;
    }
}
