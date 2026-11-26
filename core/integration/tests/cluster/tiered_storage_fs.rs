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
use std::str::FromStr;
use std::time::Duration;

use iggy::prelude::*;
use integration::harness::disk::{is_segment_log, walk};
use integration::harness::{
    TestHarness, TestServerConfig, partition_primary, resolve_config_paths,
};
use tokio::time::{Instant, sleep};

use super::partition_state_transfer::{FULLY_SERVED_MARKER, INSTALL_MARKER, await_marker};
use crate::server::http_client::{HttpClient, node_url};
use crate::server::tiered_storage_fs::{
    MESSAGES, PARTITION_ID, POLL_INTERVAL, STREAM, TIERING_DEADLINE, TOPIC,
    assert_manifest_lists_stored_segments, assert_reads_every_message, create_tiered_topic,
    create_tiered_topic_with, fs_store_envs, read_every_message, read_manifest, segment_logs,
    send_every_message, tiered_topic_options,
};

const NODES: usize = 3;
/// How long the restarted cluster may take to serve the topic again.
const RESTART_DEADLINE: Duration = Duration::from_secs(60);
/// How long the survivors may take to elect the rebuilt member and let it
/// serve the partition.
const ELECTION_DEADLINE: Duration = Duration::from_secs(60);
const AFTER_TRANSFER_PAYLOAD: &str = "sent-after-the-transfer";
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

/// The primary still keeps the segments that the store holds on its disk,
/// so only the bytes of the rest may cross to a member rebuilt by state
/// transfer. Once that member leads the partition, it has to read the stored
/// segments from the store.
#[tokio::test]
async fn given_stored_segments_kept_on_the_primary_when_a_member_rebuilds_its_partition_by_state_transfer_should_read_them_from_the_store()
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
    let options = TopicCreateOptions {
        // A day: every stored segment stays on the disks for the whole test.
        local_retention_age: None,
        ..tiered_topic_options()
    };
    create_tiered_topic_with(&client, &stream, &options).await;
    send_every_message(&client, &stream, &topic).await;

    let primary = usize::from(
        partition_primary(&client, &stream, &topic, PARTITION_ID)
            .await
            .expect("route the partition"),
    );
    // Views go round the nodes in order, so the survivors elect this member
    // once the primary stops.
    let rebuilt = (primary + 1) % NODES;
    let (stored, active) = wait_for_stored_segments_kept_on(&harness, store.path(), primary).await;

    harness.stop_node(rebuilt).expect("stop the member");
    let segment = walk(&harness.node(rebuilt).data_path(), &mut |path| {
        is_segment_log(path)
    })
    .expect("the member holds a segment of the partition");
    // The member keeps its record of the partition, so it returns as one that
    // lost its history and awaits a transfer.
    std::fs::remove_dir_all(segment.parent().expect("partition directory"))
        .expect("remove the partition directory of the member");
    harness.start_node(rebuilt).expect("restart the member");
    await_marker(&harness, rebuilt, INSTALL_MARKER).await;
    await_marker(&harness, primary, FULLY_SERVED_MARKER).await;

    // A `.log` never comes back once it left the disk, so the primary held
    // the stored segments for the whole transfer.
    let on_primary = partition_segment_logs(&harness.node(primary).data_path());
    assert!(
        stored.iter().all(|segment| on_primary.contains(segment)),
        "node {primary} served the transfer without the stored segments {stored:?} on its \
         disk, it holds {on_primary:?}"
    );
    assert_lacks_stored_segments(&harness, rebuilt, &stored, "after the transfer");
    let on_rebuilt = partition_segment_logs(&harness.node(rebuilt).data_path());
    assert!(
        on_rebuilt.contains(&active),
        "node {rebuilt} lacks the active segment {active} after the transfer, it holds \
         {on_rebuilt:?}"
    );

    drop(client);
    harness.stop_node(primary).expect("stop the primary");
    let client = wait_for_partition_primary(&harness, &stream, &topic, rebuilt).await;
    let deadline = Instant::now() + ELECTION_DEADLINE;
    while let Err(error) = read_every_message(&client, &stream, &topic).await {
        assert!(
            Instant::now() < deadline,
            "within {ELECTION_DEADLINE:?} node {rebuilt} led the partition but did not serve \
             every message: {error}"
        );
        sleep(POLL_INTERVAL).await;
    }

    let mut messages = vec![IggyMessage::from_str(AFTER_TRANSFER_PAYLOAD).expect("message")];
    client
        .send_messages(
            &stream,
            &topic,
            &Partitioning::partition_id(PARTITION_ID),
            &mut messages,
        )
        .await
        .expect("send a message after the transfer");
    let polled = client
        .poll_messages(
            &stream,
            &topic,
            PARTITION_ID,
            &Consumer::default(),
            &PollingStrategy::offset(MESSAGES),
            PollOptions {
                count: 10,
                ..PollOptions::default()
            },
        )
        .await
        .expect("poll the message sent after the transfer");
    let appended: Vec<(u64, &[u8])> = polled
        .messages
        .iter()
        .map(|message| (message.header.offset, message.payload.as_ref()))
        .collect();
    assert_eq!(
        appended,
        [(MESSAGES, AFTER_TRANSFER_PAYLOAD.as_bytes())],
        "the message sent after the transfer must follow the others once"
    );
    assert_eq!(
        partition_primary(&client, &stream, &topic, PARTITION_ID)
            .await
            .map(usize::from),
        Ok(rebuilt),
        "the partition moved off node {rebuilt} while it served the reads"
    );
    assert_lacks_stored_segments(&harness, rebuilt, &stored, "after it served them");
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

/// Waits until the store holds every sealed segment of the partition primary
/// `node`, and the manifest lists each of them, while `node` still keeps
/// their `.log` files. Returns their names and the name of the active segment.
///
/// The primary lists a segment in the manifest only once a committed floor
/// advance matched its hash. A transfer leaves the bytes of such a segment
/// out when the store also holds it as the offer is built. An object in the
/// store alone does not prove that.
async fn wait_for_stored_segments_kept_on(
    harness: &TestHarness,
    store: &Path,
    node: usize,
) -> (Vec<String>, String) {
    let deadline = Instant::now() + TIERING_DEADLINE;
    loop {
        let local = partition_segment_logs(&harness.node(node).data_path());
        let uploaded = segment_logs(store);
        let manifest = read_manifest(store.to_owned()).await;
        let listed: Vec<String> = manifest.map_or_default(|manifest| {
            manifest
                .segments
                .iter()
                .map(|segment| format!("{:020}.log", segment.start_offset))
                .collect()
        });
        if let Some((active, sealed)) = local.split_last()
            && !sealed.is_empty()
            && sealed
                .iter()
                .all(|segment| listed.contains(segment) && uploaded.contains(segment))
        {
            return (sealed.to_vec(), active.clone());
        }
        assert!(
            Instant::now() < deadline,
            "within {TIERING_DEADLINE:?} the store did not hold every sealed segment of node \
             {node}: it keeps {local:?}, the store holds {uploaded:?}, the manifest lists \
             {listed:?}"
        );
        sleep(POLL_INTERVAL).await;
    }
}

/// The names of the segment logs in the partition directories under
/// `data_path`, in offset order. Unlike [`segment_logs`], it leaves out the
/// server's own log and the copies that the journal or a quarantine keeps.
fn partition_segment_logs(data_path: &Path) -> Vec<String> {
    let mut logs = Vec::new();
    let _ = walk(data_path, &mut |path| {
        if is_segment_log(path)
            && let Some(name) = path.file_name()
        {
            logs.push(name.to_string_lossy().into_owned());
        }
        false
    });
    logs.sort();
    logs
}

fn assert_lacks_stored_segments(harness: &TestHarness, node: usize, stored: &[String], when: &str) {
    let local = partition_segment_logs(&harness.node(node).data_path());
    let copied: Vec<&String> = stored
        .iter()
        .filter(|segment| local.contains(segment))
        .collect();
    assert!(
        copied.is_empty(),
        "node {node} holds the stored segments {copied:?} on its disk {when}, where only the \
         store may hold them: it holds {local:?}"
    );
}

/// A client of `node` once the partition route names `node` as the primary.
async fn wait_for_partition_primary(
    harness: &TestHarness,
    stream: &Identifier,
    topic: &Identifier,
    node: usize,
) -> IggyClient {
    let deadline = Instant::now() + ELECTION_DEADLINE;
    loop {
        let route = match harness.root_client_for_node(node).await {
            Ok(client) => match partition_primary(&client, stream, topic, PARTITION_ID).await {
                Ok(primary) if usize::from(primary) == node => return client,
                route => format!("{route:?}"),
            },
            Err(error) => format!("connect: {error}"),
        };
        assert!(
            Instant::now() < deadline,
            "within {ELECTION_DEADLINE:?} node {node} did not become the partition primary: \
             the route says {route}"
        );
        sleep(POLL_INTERVAL).await;
    }
}
