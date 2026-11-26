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

//! A server with the `fs` tiered storage backend and a tiered topic on it.

use std::collections::{BTreeMap, HashMap};
use std::path::{Path, PathBuf};
use std::time::Duration;

use iggy::prelude::*;
use iggy_object_store::{FsStore, ObjectStore, PartitionManifest};
use integration::harness::{TestBinary, TestHarness, TestServerConfig};
use tokio::time::{Instant, sleep};

use super::http_client::HttpClient;

pub(crate) const STREAM: &str = "tiered-stream";
pub(crate) const TOPIC: &str = "tiered-topic";
pub(crate) const PARTITION_ID: u32 = 0;
const SEGMENT_BYTES: u64 = 1024 * 1024;
const MESSAGE_BYTES: usize = 100 * 1024;
pub(crate) const MESSAGES: u64 = 50;
/// How long the cluster may take to accept a tiered topic: the node claims
/// the store first, and the metadata primary then has to see it ready.
const READY_DEADLINE: Duration = Duration::from_secs(60);
/// How long the uploads and the local eviction of the test's segments may
/// take.
pub(crate) const TIERING_DEADLINE: Duration = Duration::from_secs(60);
pub(crate) const POLL_INTERVAL: Duration = Duration::from_millis(200);
/// How long the shards may take to report the reads of a pass on `/metrics`.
pub(crate) const METRICS_DEADLINE: Duration = Duration::from_secs(30);
const CHAIN_REBUILDS: &str = "partition_chain_rebuilds_total";
const CHAIN_REBUILD_REFUSALS: &str = "partition_chain_rebuild_refusals_total";

#[tokio::test]
async fn given_a_tiered_topic_when_its_segments_leave_the_disk_should_read_them_back_from_the_store()
 {
    let store = tempfile::tempdir().expect("a directory for the object store");
    let mut harness = server_with_fs_store(store.path());
    harness.start().await.expect("the server boots");
    let client = harness.tcp_root_client().await.expect("tcp root client");
    let stream = Identifier::named(STREAM).expect("stream identifier");
    let topic = Identifier::named(TOPIC).expect("topic identifier");
    client
        .create_stream(STREAM, &StreamCreateOptions::default())
        .await
        .expect("create stream");
    create_tiered_topic(&client, &stream).await;

    send_every_message(&client, &stream, &topic).await;
    let data_path = harness.server().data_path();
    let uploaded = wait_for_eviction(store.path(), &data_path).await;

    assert_reads_every_message(&client, &stream, &topic).await;
    assert_manifest_lists_stored_segments(store.path()).await;

    drop(client);
    harness.restart_server().await.expect("the server restarts");
    let client = harness.tcp_root_client().await.expect("tcp root client");

    assert_reads_every_message(&client, &stream, &topic).await;
    assert!(
        segment_logs(store.path()).len() >= uploaded,
        "the store lost segments over the restart"
    );
}

/// The test's messages fit the read cache of a shard at its defaults.
#[tokio::test]
async fn given_segments_read_once_from_the_store_when_reading_them_again_should_serve_them_from_the_read_cache()
 {
    let store = tempfile::tempdir().expect("a directory for the object store");
    let mut harness = server_with_fs_store(store.path());
    harness.start().await.expect("the server boots");
    let client = harness.tcp_root_client().await.expect("tcp root client");
    let http = HttpClient::login_root(&harness).await;
    let stream = Identifier::named(STREAM).expect("stream identifier");
    let topic = Identifier::named(TOPIC).expect("topic identifier");
    client
        .create_stream(STREAM, &StreamCreateOptions::default())
        .await
        .expect("create stream");
    create_tiered_topic(&client, &stream).await;
    send_every_message(&client, &stream, &topic).await;
    wait_until_only_the_open_segment_is_local(store.path(), &harness.server().data_path()).await;

    // One message a poll, so that the polls of a segment read it in order.
    if let Err(error) = read_every_message_in_polls_of(&client, &stream, &topic, 1).await {
        panic!("first pass: {error}");
    }
    let first =
        settled_read_cache_counts(&http, METRICS_DEADLINE, |counts| counts.demand_bytes > 0).await;
    assert!(
        first.blocks_fetched_ahead > 0,
        "readahead fetched nothing in front of the first pass: {first:?}"
    );
    if let Err(error) = read_every_message_in_polls_of(&client, &stream, &topic, 1).await {
        panic!("second pass: {error}");
    }
    let second =
        settled_read_cache_counts(&http, METRICS_DEADLINE, |counts| *counts != first).await;

    assert!(
        second.block_hits > first.block_hits,
        "the second pass took no block from the read cache: {first:?}, then {second:?}"
    );
    assert_eq!(
        second.demand_bytes, first.demand_bytes,
        "the second pass asked the store again: {first:?}, then {second:?}"
    );
}

/// A topic that keeps nothing it reads from the store asks the store again
/// on every pass. An update that gives it a cache takes effect at its next
/// read, with no restart, and its second pass after it reads from memory.
#[tokio::test]
async fn given_a_tiered_topic_that_caches_nothing_when_an_update_gives_it_a_cache_should_serve_its_next_second_pass_from_the_cache()
 {
    let store = tempfile::tempdir().expect("a directory for the object store");
    let mut harness = server_with_fs_store(store.path());
    harness.start().await.expect("the server boots");
    let client = harness.tcp_root_client().await.expect("tcp root client");
    let http = HttpClient::login_root(&harness).await;
    let stream = Identifier::named(STREAM).expect("stream identifier");
    let topic = Identifier::named(TOPIC).expect("topic identifier");
    client
        .create_stream(STREAM, &StreamCreateOptions::default())
        .await
        .expect("create stream");
    let read_cache = |size: &str| TopicReadCache {
        size: Some(size.parse().expect("a byte size")),
        read_ahead: None,
    };
    create_tiered_topic_with(
        &client,
        &stream,
        &TopicCreateOptions {
            read_cache: read_cache("0"),
            ..tiered_topic_options()
        },
    )
    .await;
    send_every_message(&client, &stream, &topic).await;
    wait_until_only_the_open_segment_is_local(store.path(), &harness.server().data_path()).await;

    for pass in ["first", "second"] {
        if let Err(error) = read_every_message_in_polls_of(&client, &stream, &topic, 1).await {
            panic!("{pass} pass without a cache: {error}");
        }
    }
    // The sealed segments are only in the store, and no read went through
    // the cache, so each pass read the store.
    let uncached = settled_read_cache_counts(&http, METRICS_DEADLINE, |_| true).await;
    assert_eq!(
        (
            uncached.block_hits,
            uncached.block_misses,
            uncached.demand_bytes,
            uncached.blocks_fetched_ahead
        ),
        (0, 0, 0, 0),
        "a topic that caches nothing read through the cache: {uncached:?}"
    );

    client
        .update_topic(
            &stream,
            &topic,
            &TopicUpdateOptions {
                read_cache: read_cache("32 MiB"),
                ..TopicUpdateOptions::default()
            },
        )
        .await
        .expect("the topic takes a read cache");
    if let Err(error) = read_every_message_in_polls_of(&client, &stream, &topic, 1).await {
        panic!("first pass with a cache: {error}");
    }
    let first =
        settled_read_cache_counts(&http, METRICS_DEADLINE, |counts| counts.demand_bytes > 0).await;
    if let Err(error) = read_every_message_in_polls_of(&client, &stream, &topic, 1).await {
        panic!("second pass with a cache: {error}");
    }
    let second =
        settled_read_cache_counts(&http, METRICS_DEADLINE, |counts| *counts != first).await;

    assert!(
        second.block_hits > first.block_hits,
        "the second pass took no block from the read cache: {first:?}, then {second:?}"
    );
    assert_eq!(
        second.demand_bytes, first.demand_bytes,
        "the second pass asked the store again: {first:?}, then {second:?}"
    );
}

/// A topic whose read cache is smaller than what one pass reads keeps no
/// more than it: its older blocks leave the cache for its newer ones.
#[tokio::test]
async fn given_a_tiered_topic_with_a_small_read_cache_when_a_pass_reads_more_than_it_should_evict_its_own_blocks()
 {
    let store = tempfile::tempdir().expect("a directory for the object store");
    let mut harness = server_with_fs_store(store.path());
    harness.start().await.expect("the server boots");
    let client = harness.tcp_root_client().await.expect("tcp root client");
    let http = HttpClient::login_root(&harness).await;
    let stream = Identifier::named(STREAM).expect("stream identifier");
    let topic = Identifier::named(TOPIC).expect("topic identifier");
    client
        .create_stream(STREAM, &StreamCreateOptions::default())
        .await
        .expect("create stream");
    // Eight blocks of the default block size, the least a partition keeps.
    let limit: IggyByteSize = "2 MiB".parse().expect("a byte size");
    create_tiered_topic_with(
        &client,
        &stream,
        &TopicCreateOptions {
            read_cache: TopicReadCache {
                size: Some(limit),
                read_ahead: None,
            },
            ..tiered_topic_options()
        },
    )
    .await;
    send_every_message(&client, &stream, &topic).await;
    wait_until_only_the_open_segment_is_local(store.path(), &harness.server().data_path()).await;

    if let Err(error) = read_every_message_in_polls_of(&client, &stream, &topic, 1).await {
        panic!("pass: {error}");
    }
    let counts =
        settled_read_cache_counts(&http, METRICS_DEADLINE, |counts| counts.demand_bytes > 0).await;

    assert!(
        counts.demand_bytes > limit.as_bytes_u64(),
        "the pass read no more than the limit, so nothing had to leave: {counts:?}"
    );
    assert!(
        counts.limit_evictions > 0,
        "no block left the cache for the topic's limit: {counts:?}"
    );
}

/// A node that lost the chain file of a partition whose front left the disk
/// reads that front back from the store, instead of serving the partition
/// from its first local segment.
#[tokio::test]
async fn given_an_evicted_front_when_the_chain_file_is_lost_should_rebuild_the_chain_and_read_every_message()
 {
    let store = tempfile::tempdir().expect("a directory for the object store");
    let mut harness = single_node_with_fs_store(store.path());
    harness.start().await.expect("the server boots");
    let client = harness.tcp_root_client().await.expect("tcp root client");
    let stream = Identifier::named(STREAM).expect("stream identifier");
    let topic = Identifier::named(TOPIC).expect("topic identifier");
    client
        .create_stream(STREAM, &StreamCreateOptions::default())
        .await
        .expect("create stream");
    create_tiered_topic(&client, &stream).await;
    send_every_message(&client, &stream, &topic).await;
    let data_path = harness.server().data_path();
    wait_until_only_the_open_segment_is_local(store.path(), &data_path).await;
    drop(client);

    harness.server_mut().stop().expect("the server stops");
    let chain = the_chain_file(&data_path);
    std::fs::remove_file(&chain).expect("delete the chain file");
    harness
        .server_mut()
        .start()
        .expect("the server starts again");
    let client = harness.tcp_root_client().await.expect("tcp root client");

    if let Err(error) = read_every_message_within(&client, &stream, &topic, TIERING_DEADLINE).await
    {
        panic!("after the chain file was lost: {error}");
    }
    assert!(chain.exists(), "the chain file was not written again");
    let http = HttpClient::login_root(&harness).await;
    assert_eq!(
        settled_metric(&http, CHAIN_REBUILDS, |rebuilds| rebuilds > 0).await,
        1
    );
}

/// A store that lost the first segment of the evicted front cannot account
/// for it, so the node refuses the partition and leaves its files as they
/// were, rather than serving it from the next segment on.
#[tokio::test]
async fn given_an_evicted_front_whose_first_object_is_gone_when_the_chain_file_is_lost_should_refuse_the_partition()
 {
    let store = tempfile::tempdir().expect("a directory for the object store");
    let mut harness = single_node_with_fs_store(store.path());
    harness.start().await.expect("the server boots");
    let client = harness.tcp_root_client().await.expect("tcp root client");
    let stream = Identifier::named(STREAM).expect("stream identifier");
    let topic = Identifier::named(TOPIC).expect("topic identifier");
    client
        .create_stream(STREAM, &StreamCreateOptions::default())
        .await
        .expect("create stream");
    create_tiered_topic(&client, &stream).await;
    send_every_message(&client, &stream, &topic).await;
    let data_path = harness.server().data_path();
    wait_until_only_the_open_segment_is_local(store.path(), &data_path).await;
    drop(client);

    harness.server_mut().stop().expect("the server stops");
    let chain = the_chain_file(&data_path);
    std::fs::remove_file(&chain).expect("delete the chain file");
    let stored = segment_logs(store.path());
    std::fs::remove_file(files_named(store.path(), &stored[0]).remove(0))
        .expect("delete the first segment's object");
    let partition_dir = chain.parent().expect("the partition directory").to_owned();
    let local_logs = file_contents(&partition_dir, &["log"]);
    let local_indexes = file_contents(&partition_dir, &["index"]);
    harness
        .server_mut()
        .start()
        .expect("the server starts again");
    let client = harness.tcp_root_client().await.expect("tcp root client");
    let http = HttpClient::login_root(&harness).await;

    assert_eq!(
        settled_metric(&http, CHAIN_REBUILD_REFUSALS, |refusals| refusals > 0).await,
        1
    );
    assert_eq!(metric(&http, CHAIN_REBUILDS).await, 0);
    let second_start = stored[1]
        .strip_suffix(".log")
        .and_then(|stem| stem.parse::<u64>().ok())
        .expect("a segment object named by its start");
    let refusal = format!(
        "the offsets 0 to {} below the local log are not accounted for",
        second_start - 1
    );
    assert!(
        harness.server().stdout_contains(&refusal),
        "the log does not name the unaccounted range: {refusal}"
    );
    // A poll that succeeded with no message, or with other ones, would pass
    // a check that only expects the read of every message to fail.
    let local_start = local_logs
        .keys()
        .next()
        .and_then(|name| name.strip_suffix(".log"))
        .and_then(|stem| stem.parse::<u64>().ok())
        .expect("a segment log named by its start");
    let polled = client
        .poll_messages(
            &stream,
            &topic,
            PARTITION_ID,
            &Consumer::default(),
            &PollingStrategy::offset(local_start),
            PollOptions {
                count: 1,
                ..PollOptions::default()
            },
        )
        .await;
    assert!(
        polled.is_err(),
        "the refused partition served a poll of its local segment at {local_start}: {:?}",
        polled.map(|polled| polled.messages.len())
    );
    assert!(!chain.exists(), "the refusal created a chain file");
    let logs = file_contents(&partition_dir, &["log"]);
    assert_eq!(
        logs.keys().collect::<Vec<_>>(),
        local_logs.keys().collect::<Vec<_>>(),
        "the refusal changed which segment logs are on disk"
    );
    assert!(logs == local_logs, "the refusal changed a segment log");
    // Boot rewrites the index of the segment the WAL checkpoint names, so
    // only the names are compared: the indexes of the evicted segments stay.
    assert_eq!(
        file_contents(&partition_dir, &["index"])
            .keys()
            .collect::<Vec<_>>(),
        local_indexes.keys().collect::<Vec<_>>()
    );
}

/// With every segment still on disk nothing below the local log can be
/// missing, so the node serves the partition without reading the store.
#[tokio::test]
async fn given_every_segment_on_disk_when_the_chain_file_is_lost_should_serve_without_the_store() {
    let store = tempfile::tempdir().expect("a directory for the object store");
    let mut harness = single_node_with_fs_store(store.path());
    harness.start().await.expect("the server boots");
    let client = harness.tcp_root_client().await.expect("tcp root client");
    let stream = Identifier::named(STREAM).expect("stream identifier");
    let topic = Identifier::named(TOPIC).expect("topic identifier");
    client
        .create_stream(STREAM, &StreamCreateOptions::default())
        .await
        .expect("create stream");
    create_tiered_topic_with(
        &client,
        &stream,
        &TopicCreateOptions {
            local_retention_age: None,
            ..tiered_topic_options()
        },
    )
    .await;
    send_every_message(&client, &stream, &topic).await;
    let data_path = harness.server().data_path();
    wait_for_upload(store.path()).await;
    drop(client);

    harness.server_mut().stop().expect("the server stops");
    let stored = segment_logs(store.path());
    let local = segment_logs(&data_path);
    assert!(
        stored.iter().all(|object| local.contains(object)),
        "a segment left the disk: the store holds {stored:?}, the data directory {local:?}"
    );
    let chain = the_chain_file(&data_path);
    std::fs::remove_file(&chain).expect("delete the chain file");
    harness
        .server_mut()
        .start()
        .expect("the server starts again");
    let client = harness.tcp_root_client().await.expect("tcp root client");

    assert_reads_every_message(&client, &stream, &topic).await;
    let http = HttpClient::login_root(&harness).await;
    assert_eq!(metric(&http, CHAIN_REBUILDS).await, 0);
    assert_eq!(metric(&http, CHAIN_REBUILD_REFUSALS).await, 0);
    assert!(
        !harness
            .server()
            .stdout_contains("waits for the object store to account"),
        "the partition waited for the store"
    );
}

fn server_with_fs_store(store: &Path) -> TestHarness {
    TestHarness::builder()
        .server(
            TestServerConfig::builder()
                .extra_envs(fs_store_envs(store))
                .build(),
        )
        .build()
        .expect("the harness")
}

/// [`server_with_fs_store`] on one node, so each partition has one replica
/// and no peer to restore it from.
fn single_node_with_fs_store(store: &Path) -> TestHarness {
    TestHarness::builder()
        .cluster_nodes(1)
        .server(
            TestServerConfig::builder()
                .extra_envs(fs_store_envs(store))
                .build(),
        )
        .build()
        .expect("the harness")
}

/// The settings of a server that tiers to the `fs` store at `store` and runs
/// its message cleaner often enough for a test.
pub(crate) fn fs_store_envs(store: &Path) -> HashMap<String, String> {
    HashMap::from([
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
    ])
}

pub(crate) fn tiered_topic_options() -> TopicCreateOptions {
    TopicCreateOptions {
        partitions_count: Some(1),
        segment_size: Some(IggyByteSize::from(SEGMENT_BYTES)),
        tiered_storage: Some(true),
        // Every uploaded segment may leave the disk at once.
        local_retention_age: Some(IggyExpiry::ExpireDuration(IggyDuration::from(
            Duration::from_secs(1),
        ))),
        ..TopicCreateOptions::default()
    }
}

/// Creates the topic once the cluster accepts `tiered_storage`.
pub(crate) async fn create_tiered_topic(client: &IggyClient, stream: &Identifier) {
    create_tiered_topic_with(client, stream, &tiered_topic_options()).await;
}

/// [`create_tiered_topic`] with `options` in place of [`tiered_topic_options`].
pub(crate) async fn create_tiered_topic_with(
    client: &IggyClient,
    stream: &Identifier,
    options: &TopicCreateOptions,
) {
    let deadline = Instant::now() + READY_DEADLINE;
    loop {
        match client.create_topic(stream, TOPIC, options).await {
            Ok(_) => return,
            Err(IggyError::FeatureUnavailable) if Instant::now() < deadline => {
                sleep(POLL_INTERVAL).await;
            }
            Err(error) => panic!("create the tiered topic: {error}"),
        }
    }
}

/// Waits until the store holds segments that the data directory no longer
/// does, and returns how many the store holds.
pub(crate) async fn wait_for_eviction(store: &Path, data_path: &Path) -> usize {
    let deadline = Instant::now() + TIERING_DEADLINE;
    loop {
        let uploaded = segment_logs(store);
        let local = segment_logs(data_path);
        let evicted = uploaded.iter().filter(|name| !local.contains(name)).count();
        if evicted > 0 {
            return uploaded.len();
        }
        assert!(
            Instant::now() < deadline,
            "within {TIERING_DEADLINE:?} no uploaded segment left the disk: the store holds \
             {uploaded:?}, the data directory {local:?}"
        );
        sleep(POLL_INTERVAL).await;
    }
}

/// Waits until the store holds a segment.
async fn wait_for_upload(store: &Path) {
    let deadline = Instant::now() + TIERING_DEADLINE;
    while segment_logs(store).is_empty() {
        assert!(
            Instant::now() < deadline,
            "within {TIERING_DEADLINE:?} no segment reached the store"
        );
        sleep(POLL_INTERVAL).await;
    }
}

/// Waits until the data directory holds the open segment alone, and the store
/// every other one.
async fn wait_until_only_the_open_segment_is_local(store: &Path, data_path: &Path) {
    let deadline = Instant::now() + TIERING_DEADLINE;
    loop {
        let uploaded = segment_logs(store);
        // The data directory holds other `.log` files, such as the server's
        // own log.
        let local: Vec<String> = segment_logs(data_path)
            .into_iter()
            .filter(|name| name.strip_suffix(".log").is_some_and(is_start_offset))
            .collect();
        if local.len() == 1 && !uploaded.is_empty() && !uploaded.contains(&local[0]) {
            return;
        }
        assert!(
            Instant::now() < deadline,
            "within {TIERING_DEADLINE:?} the sealed segments did not all leave the disk: the \
             store holds {uploaded:?}, the data directory {local:?}"
        );
        sleep(POLL_INTERVAL).await;
    }
}

/// Whether `name` is a start offset as segment files spell it.
fn is_start_offset(name: &str) -> bool {
    name.len() == 20 && name.bytes().all(|byte| byte.is_ascii_digit())
}

/// What the read caches of the server's shards did, summed over the shards.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct ReadCacheCounts {
    pub(crate) block_hits: u64,
    pub(crate) block_misses: u64,
    pub(crate) demand_bytes: u64,
    pub(crate) blocks_fetched_ahead: u64,
    pub(crate) held_bytes: u64,
    pub(crate) fresh_releases: u64,
    pub(crate) carried_bytes: u64,
    pub(crate) limit_evictions: u64,
}

/// The counts once two scrapes a poll interval apart agree and `ready`
/// accepts them, within `within`. A shard copies its counts out once a
/// tick, so one scrape can miss the last reads.
pub(crate) async fn settled_read_cache_counts(
    http: &HttpClient,
    within: Duration,
    ready: impl Fn(&ReadCacheCounts) -> bool,
) -> ReadCacheCounts {
    let deadline = Instant::now() + within;
    let mut last = read_cache_counts(http).await;
    loop {
        sleep(POLL_INTERVAL).await;
        let counts = read_cache_counts(http).await;
        if counts == last && ready(&counts) {
            return counts;
        }
        assert!(
            Instant::now() < deadline,
            "within {within:?} the read cache did not report the pass: {counts:?}"
        );
        last = counts;
    }
}

async fn read_cache_counts(http: &HttpClient) -> ReadCacheCounts {
    let exposition = http
        .get("/metrics")
        .await
        .text()
        .await
        .expect("metrics text");
    ReadCacheCounts {
        block_hits: summed(&exposition, "remote_read_block_hits_total"),
        block_misses: summed(&exposition, "remote_read_block_misses_total"),
        demand_bytes: summed(&exposition, "remote_read_demand_bytes_total"),
        blocks_fetched_ahead: summed(&exposition, "remote_read_blocks_fetched_ahead_total"),
        held_bytes: summed(&exposition, "remote_read_held_bytes"),
        fresh_releases: summed(&exposition, "remote_read_fresh_releases_total"),
        carried_bytes: summed(&exposition, "remote_read_carried_bytes_total"),
        limit_evictions: summed(&exposition, "remote_read_limit_evictions_total"),
    }
}

/// The sum of `series` over the shards once two scrapes a poll interval apart
/// agree and `ready` accepts it, within [`METRICS_DEADLINE`].
async fn settled_metric(http: &HttpClient, series: &str, ready: impl Fn(u64) -> bool) -> u64 {
    let deadline = Instant::now() + METRICS_DEADLINE;
    let mut last = metric(http, series).await;
    loop {
        sleep(POLL_INTERVAL).await;
        let value = metric(http, series).await;
        if value == last && ready(value) {
            return value;
        }
        assert!(
            Instant::now() < deadline,
            "within {METRICS_DEADLINE:?} {series} did not settle: {value}"
        );
        last = value;
    }
}

async fn metric(http: &HttpClient, series: &str) -> u64 {
    let exposition = http
        .get("/metrics")
        .await
        .text()
        .await
        .expect("metrics text");
    summed(&exposition, series)
}

/// The samples of `series` of every shard, summed.
fn summed(exposition: &str, series: &str) -> u64 {
    exposition
        .lines()
        .filter(|line| {
            line.strip_prefix(series)
                .is_some_and(|rest| rest.starts_with('{') || rest.starts_with(' '))
        })
        .map(|line| {
            line.rsplit(' ')
                .next()
                .and_then(|value| value.parse::<u64>().ok())
                .unwrap_or_else(|| panic!("unreadable sample: {line}"))
        })
        .sum()
}

pub(crate) async fn send_every_message(
    client: &IggyClient,
    stream: &Identifier,
    topic: &Identifier,
) {
    for index in 0..MESSAGES {
        let mut messages = vec![
            IggyMessage::builder()
                .payload(payload(index).into())
                .build()
                .expect("build message"),
        ];
        client
            .send_messages(
                stream,
                topic,
                &Partitioning::partition_id(PARTITION_ID),
                &mut messages,
            )
            .await
            .expect("send messages");
    }
}

pub(crate) async fn assert_reads_every_message(
    client: &IggyClient,
    stream: &Identifier,
    topic: &Identifier,
) {
    if let Err(error) = read_every_message(client, stream, topic).await {
        panic!("{error}");
    }
}

/// Reads every message the test sent and compares it with what was sent.
pub(crate) async fn read_every_message(
    client: &IggyClient,
    stream: &Identifier,
    topic: &Identifier,
) -> Result<(), String> {
    read_every_message_in_polls_of(client, stream, topic, 10).await
}

/// [`read_every_message`] until it succeeds, within `within`. Returns the
/// last failure once the time is up.
async fn read_every_message_within(
    client: &IggyClient,
    stream: &Identifier,
    topic: &Identifier,
    within: Duration,
) -> Result<(), String> {
    let deadline = Instant::now() + within;
    loop {
        match read_every_message(client, stream, topic).await {
            Ok(()) => return Ok(()),
            Err(error) if Instant::now() >= deadline => {
                return Err(format!("within {within:?}: {error}"));
            }
            Err(_) => sleep(POLL_INTERVAL).await,
        }
    }
}

/// [`read_every_message`] with `count` messages a poll.
async fn read_every_message_in_polls_of(
    client: &IggyClient,
    stream: &Identifier,
    topic: &Identifier,
    count: u32,
) -> Result<(), String> {
    let mut offset = 0;
    while offset < MESSAGES {
        let polled = client
            .poll_messages(
                stream,
                topic,
                PARTITION_ID,
                &Consumer::default(),
                &PollingStrategy::offset(offset),
                PollOptions {
                    count,
                    ..PollOptions::default()
                },
            )
            .await
            .map_err(|error| format!("poll from offset {offset}: {error}"))?;
        if polled.messages.is_empty() {
            return Err(format!("the poll from offset {offset} returned no message"));
        }
        for message in &polled.messages {
            if message.header.offset != offset {
                return Err(format!(
                    "the poll returned offset {} where {offset} was due",
                    message.header.offset
                ));
            }
            if message.payload.as_ref() != payload(offset).as_slice() {
                return Err(format!("the payload at offset {offset} differs"));
            }
            offset += 1;
        }
    }
    Ok(())
}

/// Waits for a manifest that lists a segment, then checks it against the
/// objects in the store: a reader with the bucket alone finds every segment
/// the manifest names, and they cover the offsets from 0 to its floor.
pub(crate) async fn assert_manifest_lists_stored_segments(store: &Path) {
    let deadline = Instant::now() + TIERING_DEADLINE;
    let manifest = loop {
        match read_manifest(store.to_owned()).await {
            Some(manifest) if !manifest.segments.is_empty() => break manifest,
            read => assert!(
                Instant::now() < deadline,
                "within {TIERING_DEADLINE:?} the store got no manifest that lists a segment: \
                 {read:?}"
            ),
        }
        sleep(POLL_INTERVAL).await;
    };

    let stored = segment_logs(store);
    let mut next_offset = 0;
    for segment in &manifest.segments {
        assert_eq!(
            segment.start_offset, next_offset,
            "the manifest leaves a gap: {manifest:?}"
        );
        let object = format!("{:020}.log", segment.start_offset);
        assert!(
            stored.contains(&object),
            "the manifest lists {object}, and the store holds {stored:?}"
        );
        next_offset = segment.end_offset + 1;
    }
    assert_eq!(manifest.floor, Some(next_offset - 1));
    assert_eq!(manifest.trim_point, None);
}

/// The manifest of the one partition, read through the provider on a compio
/// runtime, as the provider does not run on the harness's.
async fn read_manifest(store: PathBuf) -> Option<PartitionManifest> {
    let path = files_named(&store, "manifest.bin").pop()?;
    let key = path
        .strip_prefix(&store)
        .ok()?
        .to_string_lossy()
        .into_owned();
    tokio::task::spawn_blocking(move || {
        compio::runtime::Runtime::new()
            .expect("a compio runtime")
            .block_on(async {
                let store = FsStore::new(&store);
                let size = store.head(&key).await.ok()?.size;
                let object = store.get_range(&key, 0..size).await.ok()?;
                Some(PartitionManifest::decode(&object.bytes).expect("a manifest that decodes"))
            })
    })
    .await
    .expect("the read does not panic")
}

fn payload(index: u64) -> Vec<u8> {
    let mut payload = vec![(index % 251) as u8; MESSAGE_BYTES];
    payload[..8].copy_from_slice(&index.to_le_bytes());
    payload
}

/// The file names of the segment logs under `root`, at any depth.
pub(crate) fn segment_logs(root: &Path) -> Vec<String> {
    let mut logs: Vec<String> = files(root)
        .into_iter()
        .filter(|path| path.extension().is_some_and(|extension| extension == "log"))
        .filter_map(|path| Some(path.file_name()?.to_string_lossy().into_owned()))
        .collect();
    logs.sort();
    logs
}

/// The bytes of every file directly in `directory` with one of
/// `extensions`, by name.
fn file_contents(directory: &Path, extensions: &[&str]) -> BTreeMap<String, Vec<u8>> {
    std::fs::read_dir(directory)
        .expect("list the directory")
        .flatten()
        .map(|entry| entry.path())
        .filter(|path| {
            path.extension()
                .and_then(|extension| extension.to_str())
                .is_some_and(|extension| extensions.contains(&extension))
        })
        .map(|path| {
            let name = path
                .file_name()
                .map(|name| name.to_string_lossy().into_owned())
                .unwrap_or_default();
            (name, std::fs::read(&path).expect("read the file"))
        })
        .collect()
}

/// The chain file of the test's one partition under `data_path`.
fn the_chain_file(data_path: &Path) -> PathBuf {
    let mut chains = files_named(data_path, "remote.chain");
    assert_eq!(chains.len(), 1, "one chain file: {chains:?}");
    chains.remove(0)
}

fn files_named(root: &Path, name: &str) -> Vec<PathBuf> {
    files(root)
        .into_iter()
        .filter(|path| path.file_name().is_some_and(|file| file == name))
        .collect()
}

/// Every file under `root`, at any depth.
fn files(root: &Path) -> Vec<PathBuf> {
    let mut files = Vec::new();
    let mut pending = vec![root.to_owned()];
    while let Some(directory) = pending.pop() {
        let Ok(entries) = std::fs::read_dir(&directory) else {
            continue;
        };
        for entry in entries.flatten() {
            let path = entry.path();
            if path.is_dir() {
                pending.push(path);
            } else {
                files.push(path);
            }
        }
    }
    files
}
