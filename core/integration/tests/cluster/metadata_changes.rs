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

use std::collections::BTreeMap;
use std::time::{Duration, Instant};

use futures::future::join_all;
use iggy::prelude::*;
use iggy_common::MAX_METADATA_CHANGES_LIMIT;
use integration::harness::{TestHarness, disk};
use integration::iggy_harness;
use tokio::time::sleep;

const STREAM_PREFIX: &str = "cluster-changes";
const WRITERS: usize = 3;
const STREAMS_PER_WRITER: usize = 5;
const STREAMS: [MetadataChangeGroup; 1] = [MetadataChangeGroup::Streams];
const SESSIONS: [MetadataChangeGroup; 1] = [MetadataChangeGroup::Sessions];
// With `journal_slots = 256` and the checkpoint margin, every node
// checkpoints and drains its journal once 192 operations committed.
const STREAMS_PAST_CHECKPOINT: usize = 220;
const CHECKPOINT_MARKER: &str = "forced checkpoint completed";
const INSTALL_MARKER: &str = "metadata state transfer installed";
const CONVERGE_TIMEOUT: Duration = Duration::from_secs(60);
const POLL_INTERVAL: Duration = Duration::from_millis(100);

#[iggy_harness(cluster_nodes = 3)]
async fn given_followers_on_every_node_when_changes_commit_concurrently_should_read_one_history(
    harness: &TestHarness,
) {
    let readers = node_readers(harness).await;
    let (baseline, anchor) = current_streams(&readers[0]).await;
    let writers = harness.tcp_root_clients(WRITERS).await.expect("writers");
    join_all(
        writers
            .iter()
            .enumerate()
            .map(|(index, writer)| write_streams(writer, index)),
    )
    .await;
    let expected = listed_streams(&writers[0]).await;

    let mut histories = Vec::with_capacity(readers.len());
    for reader in &readers {
        let history = follow(reader, &anchor, &STREAMS, |records| {
            mirror(&baseline, records) == expected
        })
        .await;
        let (fresh, _) = current_streams(reader).await;
        assert_eq!(fresh, expected);
        histories.push(history);
    }
    assert!(histories.windows(2).all(|pair| pair[0] == pair[1]));
}

#[iggy_harness(cluster_nodes = 3)]
async fn given_cursor_and_page_token_when_continued_on_other_nodes_should_resume_without_gaps(
    harness: &TestHarness,
) {
    let writer = harness.tcp_root_client().await.expect("writer");
    create_streams(&writer, "seed", 9).await;
    let readers = node_readers(harness).await;
    let (expected, anchor) = current_streams(&readers[0]).await;

    let mut query = MetadataChangesQuery {
        start: MetadataChangeStart::Current(None),
        groups: STREAMS.to_vec(),
        limit: 2,
        ..MetadataChangesQuery::default()
    };
    let mut items = BTreeMap::new();
    for reader in readers.iter().cycle() {
        let page = until_ok(reader, &query).await;
        assert_eq!(page.next, Some(anchor));
        items.extend(stream_names(&page.items));
        match page.current_page_token {
            Some(token) => query.start = MetadataChangeStart::Current(Some(token)),
            None => break,
        }
    }
    assert_eq!(items, expected);

    create_streams(&writer, "continued", 6).await;
    let reference = follow(&readers[0], &anchor, &STREAMS, |records| records.len() == 6).await;
    let mut cursor = anchor;
    let mut records = Vec::new();
    let deadline = Instant::now() + CONVERGE_TIMEOUT;
    for reader in readers.iter().cycle() {
        let page = until_ok(
            reader,
            &MetadataChangesQuery {
                limit: 1,
                ..after(cursor, &STREAMS)
            },
        )
        .await;
        cursor = page.next.expect("continuation");
        records.extend(page.records);
        if records.len() == reference.len() {
            break;
        }
        assert!(Instant::now() < deadline, "{records:?}");
    }
    assert_eq!(records, reference);
}

#[iggy_harness(cluster_nodes = 3)]
async fn given_primary_failover_and_restart_when_following_should_keep_one_history(
    harness: &mut TestHarness,
) {
    let writer = harness.tcp_root_client().await.expect("writer");
    create_streams(&writer, "before", 3).await;
    drop(writer);
    let readers = node_readers(harness).await;
    let start = latest(&readers[0], &STREAMS).await;

    let leader = disk::leader_node_index(harness).await;
    harness
        .kill_node(leader)
        .expect("kill the metadata primary");
    let survivors: Vec<usize> = (0..harness.cluster_size())
        .filter(|node| *node != leader)
        .collect();
    for index in 0..4 {
        create_after_failover(
            harness,
            survivors[0],
            &format!("{STREAM_PREFIX}-after-{index}"),
        )
        .await;
    }
    let done = |records: &[MetadataChangeRecord]| {
        records
            .iter()
            .filter(|record| matches!(record.change, MetadataChange::StreamCreated(_)))
            .count()
            == 4
    };
    let mut histories = Vec::with_capacity(harness.cluster_size());
    for node in &survivors {
        histories.push(follow(&readers[*node], &start, &STREAMS, done).await);
    }

    harness.start_node(leader).expect("restart the old primary");
    let restarted = node_reader(harness, leader).await;
    histories.push(follow(&restarted, &start, &STREAMS, done).await);
    assert!(histories.windows(2).all(|pair| pair[0] == pair[1]));
    let page = until_ok(&restarted, &after(start, &STREAMS)).await;
    assert_eq!(page.log_id, Some(start.log_id));
}

#[iggy_harness(cluster_nodes = 3, server(metadata.journal_slots = "256"))]
async fn given_checkpoints_and_state_transfer_when_following_should_serve_records_below_the_drained_journal(
    harness: &mut TestHarness,
) {
    let reader = node_reader(harness, 0).await;
    let start = latest(&reader, &[]).await;
    let writer = harness.tcp_root_client().await.expect("writer");
    create_streams(&writer, "checkpoint", STREAMS_PAST_CHECKPOINT).await;
    drop(writer);
    for node in 0..harness.cluster_size() {
        await_marker(harness, node, CHECKPOINT_MARKER).await;
    }
    let reference = follow(&reader, &start, &[], |records| {
        records
            .iter()
            .filter(|record| matches!(record.change, MetadataChange::StreamCreated(_)))
            .count()
            == STREAMS_PAST_CHECKPOINT
    })
    .await;

    harness
        .restart_node(1)
        .expect("restart over the drained journal");
    let restarted = node_reader(harness, 1).await;
    let replayed = follow(&restarted, &start, &[], |records| {
        records.len() >= reference.len()
    })
    .await;
    assert_eq!(replayed[..reference.len()], reference[..]);

    harness
        .restart_node_from_clean_slate(2)
        .expect("restart from a clean slate");
    await_marker(harness, 2, INSTALL_MARKER).await;
    let transferred = node_reader(harness, 2).await;
    let installed = follow(&transferred, &start, &[], |records| {
        records.len() >= reference.len()
    })
    .await;
    assert_eq!(installed[..reference.len()], reference[..]);
}

#[iggy_harness(
    cluster_nodes = 3,
    server(
        metadata.change_log_resource_capacity = "8",
        metadata.change_log_activity_capacity = "4"
    )
)]
async fn given_small_capacities_when_each_class_overflows_should_expire_only_its_readers(
    harness: &TestHarness,
) {
    let readers = node_readers(harness).await;
    let writer = harness.tcp_root_client().await.expect("writer");
    let resource_start = latest(&readers[0], &STREAMS).await;
    let activity_start = latest(&readers[0], &SESSIONS).await;

    create_streams(&writer, "overflow", 10).await;
    for reader in &readers {
        assert_expires(reader, &resource_start, &STREAMS).await;
        let activity = until_ok(reader, &after(activity_start, &SESSIONS)).await;
        assert!(activity.records.is_empty(), "{activity:?}");
    }

    for _ in 0..3 {
        let client = harness.tcp_root_client().await.expect("short session");
        client.logout_user().await.expect("logout");
    }
    for reader in &readers {
        assert_expires(reader, &activity_start, &SESSIONS).await;
        let (rebuilt, _) = current_streams(reader).await;
        assert_eq!(rebuilt, listed_streams(&writer).await);
    }
}

async fn node_readers(harness: &TestHarness) -> Vec<IggyClient> {
    let mut readers = Vec::with_capacity(harness.cluster_size());
    for node in 0..harness.cluster_size() {
        readers.push(node_reader(harness, node).await);
    }
    readers
}

/// An HTTP client of one node. HTTP reads answer from the node they reach,
/// so each reader sees that node's own log.
async fn node_reader(harness: &TestHarness, node: usize) -> IggyClient {
    let deadline = Instant::now() + CONVERGE_TIMEOUT;
    loop {
        let builder = harness.node(node).http_client().expect("HTTP endpoint");
        if let Ok(client) = builder.with_root_login().connect().await {
            return client;
        }
        assert!(
            Instant::now() < deadline,
            "node {node} never accepted an HTTP login"
        );
        sleep(POLL_INTERVAL).await;
    }
}

async fn write_streams(writer: &IggyClient, index: usize) {
    let names: Vec<String> = (0..STREAMS_PER_WRITER)
        .map(|stream| format!("{STREAM_PREFIX}-{index}-{stream}"))
        .collect();
    for name in &names {
        writer
            .create_stream(name, &StreamCreateOptions::default())
            .await
            .expect("create stream");
    }
    writer
        .update_stream(
            &Identifier::named(&names[0]).expect("stream id"),
            &StreamUpdateOptions {
                name: Some(format!("{}-renamed", names[0])),
                ..StreamUpdateOptions::default()
            },
        )
        .await
        .expect("rename stream");
    writer
        .delete_stream(&Identifier::named(&names[1]).expect("stream id"))
        .await
        .expect("delete stream");
}

async fn create_streams(writer: &IggyClient, label: &str, count: usize) {
    for index in 0..count {
        writer
            .create_stream(
                &format!("{STREAM_PREFIX}-{label}-{index}"),
                &StreamCreateOptions::default(),
            )
            .await
            .expect("create stream");
    }
}

/// Create a stream through `node` while the cluster elects a new primary. An
/// attempt whose reply was lost may have committed, so an existing name
/// counts as done.
async fn create_after_failover(harness: &TestHarness, node: usize, name: &str) {
    let deadline = Instant::now() + CONVERGE_TIMEOUT;
    loop {
        if let Ok(client) = harness.root_client_for_node(node).await {
            match client
                .create_stream(name, &StreamCreateOptions::default())
                .await
            {
                Ok(_) | Err(IggyError::StreamNameAlreadyExists(_)) => return,
                Err(_) => {}
            }
        }
        assert!(
            Instant::now() < deadline,
            "no node accepted {name} after the failover"
        );
        sleep(POLL_INTERVAL).await;
    }
}

async fn latest(client: &IggyClient, groups: &[MetadataChangeGroup]) -> MetadataChangeCursor {
    let page = until_ok(
        client,
        &MetadataChangesQuery {
            start: MetadataChangeStart::Latest,
            groups: groups.to_vec(),
            ..MetadataChangesQuery::default()
        },
    )
    .await;
    page.next.expect("latest cursor")
}

fn after(cursor: MetadataChangeCursor, groups: &[MetadataChangeGroup]) -> MetadataChangesQuery {
    MetadataChangesQuery {
        start: MetadataChangeStart::After(cursor),
        groups: groups.to_vec(),
        limit: MAX_METADATA_CHANGES_LIMIT,
        ..MetadataChangesQuery::default()
    }
}

/// One page, asked again while the node answers `behind`. Any other status
/// fails the test: a node catching up must never skip or reset.
async fn until_ok(client: &IggyClient, query: &MetadataChangesQuery) -> MetadataChanges {
    let deadline = Instant::now() + CONVERGE_TIMEOUT;
    loop {
        match client.get_metadata_changes(query).await {
            Ok(page) if page.status == MetadataChangeStatus::Ok => return page,
            Ok(page) => assert_eq!(page.status, MetadataChangeStatus::Behind, "{page:?}"),
            Err(error) => assert_eq!(
                error.as_code(),
                IggyError::TransientNotAccepted.as_code(),
                "{error}"
            ),
        }
        assert!(Instant::now() < deadline, "the node stayed behind");
        sleep(POLL_INTERVAL).await;
    }
}

async fn follow(
    client: &IggyClient,
    start: &MetadataChangeCursor,
    groups: &[MetadataChangeGroup],
    done: impl Fn(&[MetadataChangeRecord]) -> bool,
) -> Vec<MetadataChangeRecord> {
    let deadline = Instant::now() + CONVERGE_TIMEOUT;
    let mut cursor = *start;
    let mut records = Vec::new();
    loop {
        let page = until_ok(client, &after(cursor, groups)).await;
        cursor = page.next.expect("continuation");
        records.extend(page.records);
        if page.has_more {
            continue;
        }
        if done(&records) {
            return records;
        }
        assert!(
            Instant::now() < deadline,
            "the followed records never reached the expected state: {records:?}"
        );
        sleep(POLL_INTERVAL).await;
    }
}

async fn assert_expires(
    client: &IggyClient,
    start: &MetadataChangeCursor,
    groups: &[MetadataChangeGroup],
) {
    let deadline = Instant::now() + CONVERGE_TIMEOUT;
    loop {
        let page = client
            .get_metadata_changes(&after(*start, groups))
            .await
            .expect("change page");
        if page.status == MetadataChangeStatus::Expired {
            assert!(page.records.is_empty());
            assert_eq!(page.next, None);
            return;
        }
        assert!(
            matches!(
                page.status,
                MetadataChangeStatus::Ok | MetadataChangeStatus::Behind
            ),
            "{page:?}"
        );
        assert!(Instant::now() < deadline, "the cursor never expired");
        sleep(POLL_INTERVAL).await;
    }
}

/// Every stream of a fresh current scan, and the scan's anchor.
async fn current_streams(client: &IggyClient) -> (BTreeMap<u32, String>, MetadataChangeCursor) {
    let mut query = MetadataChangesQuery {
        start: MetadataChangeStart::Current(None),
        groups: STREAMS.to_vec(),
        limit: MAX_METADATA_CHANGES_LIMIT,
        ..MetadataChangesQuery::default()
    };
    let mut streams = BTreeMap::new();
    loop {
        let page = until_ok(client, &query).await;
        streams.extend(stream_names(&page.items));
        match page.current_page_token {
            Some(token) => query.start = MetadataChangeStart::Current(Some(token)),
            None => return (streams, page.next.expect("anchor")),
        }
    }
}

fn stream_names(items: &[MetadataCurrentItem]) -> BTreeMap<u32, String> {
    items
        .iter()
        .filter_map(|item| match item {
            MetadataCurrentItem::Stream(stream) => Some((stream.stream_id, stream.name.clone())),
            _ => None,
        })
        .collect()
}

async fn listed_streams(client: &IggyClient) -> BTreeMap<u32, String> {
    client
        .get_streams()
        .await
        .expect("streams")
        .into_iter()
        .map(|stream| (stream.id, stream.name))
        .collect()
}

/// `baseline` with every stream record applied in order.
fn mirror(
    baseline: &BTreeMap<u32, String>,
    records: &[MetadataChangeRecord],
) -> BTreeMap<u32, String> {
    let mut streams = baseline.clone();
    for record in records {
        match &record.change {
            MetadataChange::StreamCreated(stream) | MetadataChange::StreamUpdated(stream) => {
                streams.insert(stream.stream_id, stream.name.clone());
            }
            MetadataChange::StreamDeleted(stream) => {
                streams.remove(&stream.stream_id);
            }
            _ => {}
        }
    }
    streams
}

async fn await_marker(harness: &TestHarness, node: usize, marker: &str) {
    let deadline = Instant::now() + CONVERGE_TIMEOUT;
    while harness.node(node).stdout_occurrences(marker) == 0 {
        assert!(
            Instant::now() < deadline,
            "node {node} never logged {marker:?}"
        );
        sleep(POLL_INTERVAL).await;
    }
}
