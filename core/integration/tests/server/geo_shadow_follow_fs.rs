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

//! A shadow topic in `dc-b` of a geo topic of `dc-a`, which serves what the
//! manifests of `dc-a` list and nothing else: a message reaches a consumer in
//! `dc-b` once a long-term segment holds it and `dc-b` read a manifest that
//! lists that segment. Nothing in `dc-b` reads the fresh objects of `dc-a`.

use std::collections::BTreeMap;
use std::fs;
use std::path::Path;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex, PoisonError};
use std::time::Duration;

use iggy::prelude::*;
use iggy_object_store::{KeyLayout, LongTermKey, RecoveryRecord, SegmentKey, SegmentObject};
use integration::harness::{
    Bucket, BucketEvent, BucketMirror, BucketWatch, GeoCluster, GeoClusterPair,
    GeoClusterPairBuilder, GeoSide, ServerHandle,
};
use tokio::time::{Instant, sleep};

use super::geo_peers::gauge;
use super::http_client::HttpClient;

const STREAM: &str = "orders";
const TOPIC: &str = "payments";
/// How long the shadow may take to serve its first message.
const WARM_UP_DEADLINE: Duration = Duration::from_secs(120);
/// How long a test may take to read what it sent.
const READ_DEADLINE: Duration = Duration::from_secs(120);
/// The pause between two sends of the steady load.
const SEND_EVERY: Duration = Duration::from_millis(50);
/// How long the steady load runs after the shadow served its first message.
const MEASURE_FOR: Duration = Duration::from_secs(60);
/// The longest wait between two rewrites of a partition, the slowest step
/// from a commit to a stored manifest.
const REWRITE_INTERVAL_MAX: Duration = Duration::from_secs(10);
/// What a message may take beyond one rewrite interval and one manifest
/// poll: the upload and commit of the segment, the manifest, its read and
/// its commit in `dc-b`, and the reader's next poll.
const LAG_SLACK: Duration = Duration::from_secs(10);
/// How long the request test watches the bucket of `dc-a` through `dc-b`.
const REQUEST_WINDOW: Duration = Duration::from_secs(30);
/// `[geo] manifest_poll_interval` at its default.
const DEFAULT_MANIFEST_POLL: Duration = Duration::from_secs(5);
/// How long a new watch takes to report the directories it opened itself
/// when it started, which the request test leaves out.
const WATCH_SETTLE: Duration = Duration::from_secs(1);
const POLL_INTERVAL: Duration = Duration::from_millis(20);
/// The pause between two checks of a log, a gauge or the bucket. A check of
/// the bucket starts a thread and a runtime of its own.
const RETRY_INTERVAL: Duration = Duration::from_millis(250);
/// How long a restarted cluster may take to boot and answer a client.
const BOOT_DEADLINE: Duration = Duration::from_secs(60);
/// How long the first identity check of a restarted node may take.
const CHECK_DEADLINE: Duration = Duration::from_secs(30);
/// How long the test keeps polling a shadow whose root another cluster took.
const REFUSED_POLLS_FOR: Duration = Duration::from_secs(5);
/// The messages that each cluster sends in its one batch.
const BATCH_MESSAGES: u64 = 20;
/// What the node logs once it finds a claim that its shadow streams did not
/// come from.
const MISMATCH_LOG: &str = "the root of the geo peer is claimed by a cluster that its shadow \
                            streams did not come from";
/// What the node logs once the recovery records under the root lead from the
/// cluster that its shadow streams came from to the claim it found.
const SUCCESSOR_LOG: &str = "which the recovery records under the root name the successor of a \
                             cluster that the shadow streams came from";
/// What a node that read the peer logs once the claim names a cluster it does
/// not take, which only a restart ends.
const CHANGED_LOG: &str = "the store identity object of the geo peer no longer names the cluster";
/// How long the node rebuilt on an empty disk may take to recover `dc-a` and
/// claim its root.
const RECOVERY_DEADLINE: Duration = Duration::from_secs(120);
/// How long a running node may take from the claim to accept a recovered
/// origin: at most one regular identity check of the peer, and slack.
const SUCCESSOR_DEADLINE: Duration = Duration::from_secs(90);
/// How long a new mirror may take to copy the whole bucket of `dc-a`.
const MIRROR_SYNC_DEADLINE: Duration = Duration::from_secs(30);

#[tokio::test]
async fn given_steady_sends_to_a_geo_topic_when_a_consumer_follows_its_shadow_should_read_each_message_only_once_a_manifest_lists_it()
 {
    let pair = started_pair(GeoClusterPair::builder()).await;
    let lags = measure_lag(&pair).await;
    lags.report("defaults");
    lags.assert_within(REWRITE_INTERVAL_MAX + DEFAULT_MANIFEST_POLL + LAG_SLACK);
}

#[tokio::test]
async fn given_a_one_second_manifest_poll_when_a_consumer_follows_a_shadow_under_steady_sends_should_read_each_message_only_once_a_manifest_lists_it()
 {
    let pair =
        started_pair(GeoClusterPair::builder().extra_env("IGGY_GEO_MANIFEST_POLL_INTERVAL", "1s"))
            .await;
    let lags = measure_lag(&pair).await;
    lags.report("manifest_poll_interval = 1 s");
    lags.assert_within(REWRITE_INTERVAL_MAX + Duration::from_secs(1) + LAG_SLACK);
}

#[tokio::test]
async fn given_a_consumer_of_a_shadow_topic_when_manifests_move_the_floor_should_read_each_message_once_in_order()
 {
    let pair =
        started_pair(GeoClusterPair::builder().extra_env("IGGY_GEO_MANIFEST_POLL_INTERVAL", "1s"))
            .await;
    let (client_a, shadow) = create_and_follow(&pair).await;
    let client_b = pair.b().root_client().await.expect("a client of dc-b");
    let consumer = Consumer::new(Identifier::named("follower").expect("a consumer name"));

    send(&client_a, 0..50).await;
    let mut read = read_next(&pair, &client_b, shadow, &consumer, 3, 50).await;
    send(&client_a, 50..150).await;
    read.extend(read_next(&pair, &client_b, shadow, &consumer, 3, 100).await);

    let expected: Vec<String> = (0..150).map(payload).collect();
    assert_eq!(read, expected, "each message once, in the origin's order");
    let from_start = read_from_offset_zero(&client_b, shadow, 7, 150).await;
    assert_eq!(
        from_start, expected,
        "a reader from the start goes through every segment the manifests listed"
    );
}

#[tokio::test]
async fn given_a_shadow_without_consumers_when_its_origin_produces_should_send_the_peer_store_only_manifest_listings_and_reads()
 {
    let pair = started_pair(GeoClusterPair::builder().peer_reads_through_mirror(GeoSide::B)).await;
    let mirror = pair
        .b()
        .peer_mirror()
        .expect("dc-b reads dc-a through a mirror");
    let (client_a, _shadow) = create_and_follow(&pair).await;
    let producing = Arc::new(AtomicBool::new(true));
    let producer = tokio::spawn(produce_until_stopped(client_a, Arc::clone(&producing)));
    let http_b = HttpClient::login_root(pair.b().harness()).await;
    let deadline = Instant::now() + WARM_UP_DEADLINE;
    while manifest_requests(&scrape(&http_b).await).1 == 0 {
        assert!(
            Instant::now() < deadline && !producer.is_finished(),
            "dc-b read no manifest of dc-a"
        );
        sleep(Duration::from_millis(250)).await;
    }

    let watch = mirror.watch().expect("a watch on the mirror");
    sleep(WATCH_SETTLE).await;
    let started = std::time::Instant::now();
    let (lists_before, gets_before) = manifest_requests(&scrape(&http_b).await);
    sleep(REQUEST_WINDOW).await;
    let (lists_after, gets_after) = manifest_requests(&scrape(&http_b).await);
    let reads = watch.reads();
    let lost = events_lost(&watch);
    drop(watch);
    producing.store(false, Ordering::Release);
    let sent = producer.await.expect("the producer finishes");

    let layout = pair.a().bucket().layout();
    let key_of = |path: &Path| {
        path.strip_prefix(mirror.path())
            .ok()
            .and_then(Path::to_str)
            .map(str::to_owned)
    };
    let mut opens = Opens::default();
    for event in &reads {
        if let BucketEvent::Read { at, paths } = event
            && *at >= started
        {
            for path in paths {
                opens.count(layout, key_of(path).as_deref());
            }
        }
    }
    let (lists, gets) = (lists_after - lists_before, gets_after - gets_before);
    let manifests_stored = mirror
        .copies()
        .iter()
        .filter(|copy| copy.at >= started)
        .filter_map(|copy| key_of(&copy.path))
        .filter(|key| layout.parse_manifest_key(key).is_some())
        .count() as u64;
    eprintln!(
        "{sent} sends in about {REQUEST_WINDOW:?}: dc-a stored {manifests_stored} manifests, \
         dc-b listed {lists} times and read {gets} manifests; opens of its bucket: {opens:?}"
    );
    assert_eq!(lost, 0, "the watch lost no event");
    assert_eq!(
        (opens.fresh, opens.segments, opens.recovery),
        (0, 0, 0),
        "no fresh object, segment or recovery record of dc-a is read: {opens:?}"
    );
    let polls = REQUEST_WINDOW.as_secs() / DEFAULT_MANIFEST_POLL.as_secs();
    assert!(
        lists + 1 >= polls && lists <= polls + 1,
        "one listing per manifest poll: {lists} in {REQUEST_WINDOW:?}"
    );
    assert!(
        gets >= 1 && gets <= lists + 1 && gets <= manifests_stored + 1,
        "a manifest is read only when it changed: {gets} reads, {lists} listings, \
         {manifests_stored} stored"
    );
    assert!(
        opens.manifest_files > 0 && opens.manifest_files <= lists + gets + 2,
        "each listing and each read opens the manifest once: {opens:?}"
    );
}

/// `dc-a` loses every disk, and a new cluster in its place takes the same
/// root under the same prefix, recovers nothing, creates the same stream and
/// topic, and sends other messages in the same batch. Its objects take the
/// keys of the old cluster's. `dc-b` restarts, so it forgets the cluster it
/// read: it judges the claim it finds against the cluster that its shadow
/// stream came from, serves nothing of the new cluster through the old
/// shadow, reports the peer unverified, and reads nothing of the root but
/// its identity object.
#[tokio::test]
async fn given_a_followed_root_that_a_new_cluster_took_when_the_follower_restarts_should_serve_nothing_of_it_and_read_only_its_identity()
 {
    let mut pair =
        started_pair(GeoClusterPair::builder().peer_reads_through_mirror(GeoSide::B)).await;
    let (client_a, shadow) = create_and_follow(&pair).await;
    send_batch(&client_a, "old").await;
    let client_b = pair.b().root_client().await.expect("a client of dc-b");
    assert_eq!(
        read_from_offset_zero(&client_b, shadow, 100, BATCH_MESSAGES as usize).await,
        batch("old")
    );
    let old_cluster = pair.a().bucket().cluster_uid().await.expect("the claim");
    let old_segments = long_term_segments(pair.a().bucket()).await;
    drop((client_a, client_b));
    pair.cluster_mut(GeoSide::B)
        .stop()
        .await
        .expect("dc-b stops");

    let cluster_a = pair.cluster_mut(GeoSide::A);
    cluster_a
        .harness_mut()
        .kill_cluster()
        .expect("dc-a loses its nodes");
    let bucket = cluster_a.bucket().path().to_owned();
    std::fs::remove_dir_all(&bucket).expect("empty the bucket of dc-a");
    std::fs::create_dir_all(&bucket).expect("the bucket of dc-a again");
    cluster_a
        .harness_mut()
        .restart_node_from_clean_slate(0)
        .expect("dc-a starts on an empty disk");
    let client_a = booted_client(pair.a()).await;
    let (recreated, _) = recreate(&pair, &client_a).await;
    assert_eq!(recreated, shadow.topic_id, "the same topic id");
    send_batch(&client_a, "new").await;
    let deadline = Instant::now() + READ_DEADLINE;
    while long_term_segments(pair.a().bucket()).await.is_empty() {
        assert!(Instant::now() < deadline, "the new dc-a stored no segment");
        sleep(RETRY_INTERVAL).await;
    }
    assert_ne!(
        pair.a().bucket().cluster_uid().await.expect("the claim"),
        old_cluster,
        "another cluster claims the root"
    );
    assert_eq!(
        long_term_segments(pair.a().bucket()).await,
        old_segments,
        "the new cluster's segment takes the key of the old one's"
    );

    let mirror = pair
        .b()
        .peer_mirror()
        .expect("dc-b reads dc-a through a mirror");
    let watch = mirror.watch().expect("a watch on the mirror");
    let mirror = mirror.path().to_owned();
    sleep(WATCH_SETTLE).await;
    let started = std::time::Instant::now();
    pair.cluster_mut(GeoSide::B)
        .start()
        .await
        .expect("dc-b starts again");
    let client_b = booted_client(pair.b()).await;
    let node_b = pair.b().harness().node(0);
    let check_deadline = Instant::now() + CHECK_DEADLINE;
    let mut polls_end = None;
    while polls_end.is_none_or(|end| Instant::now() < end) {
        assert_serves_no_new_message(&client_b, shadow).await;
        if polls_end.is_none() && node_b.stdout_contains(MISMATCH_LOG) {
            polls_end = Some(Instant::now() + REFUSED_POLLS_FOR);
        }
        assert!(
            polls_end.is_some() || Instant::now() < check_deadline,
            "dc-b never reported the claim of the new cluster"
        );
        sleep(Duration::from_millis(250)).await;
    }
    let http_b = HttpClient::login_root(pair.b().harness()).await;
    assert_eq!(
        gauge(&scrape(&http_b).await, "geo_peer_identity_verified", "dc-a"),
        Some(0)
    );

    let layout = pair.a().bucket().layout();
    let identity = layout.identity_key();
    let read: Vec<String> = watch
        .reads()
        .into_iter()
        .filter_map(|event| match event {
            BucketEvent::Read { at, paths } if at >= started => Some(paths),
            _ => None,
        })
        .flatten()
        .filter_map(|path| {
            path.strip_prefix(&mirror)
                .ok()
                .and_then(Path::to_str)
                .map(str::to_owned)
        })
        .filter(|key| {
            !key.rsplit('/')
                .next()
                .is_some_and(|name| name.starts_with('.'))
        })
        .collect();
    let recovery = format!("{}/meta/recovery/", layout.root());
    assert!(
        read.iter()
            .all(|key| *key == identity || key.starts_with(&recovery)),
        "dc-b read more of the new cluster's root than its identity: {read:?}"
    );
    assert!(read.contains(&identity), "dc-b checked the claim: {read:?}");
}

/// `dc-a` loses every disk, and a cluster on an empty disk recovers it from
/// its bucket: the new cluster keeps every key, names itself the successor in
/// a recovery record, and sends more messages to the same topic. `dc-b` takes
/// it as the successor while it runs and again after a restart, serves the
/// messages of both clusters once and in order through the same shadow, and
/// reads the root as a healthy follower does.
///
/// The running `dc-b` reads the bucket itself: a mirror copies files in no
/// fixed order, so it could show the claim of the successor before the
/// recovery record that names it. The restarted `dc-b` reads a mirror that
/// copied the whole bucket before it started.
#[tokio::test]
async fn given_a_followed_origin_that_a_cluster_on_an_empty_disk_recovered_when_the_follower_restarts_should_serve_both_clusters_once_in_order_through_the_same_shadow()
 {
    // The test counts markers in the log files, which inherited settings
    // could turn off or filter.
    let mut pair = started_pair(
        GeoClusterPair::builder()
            .extra_env("IGGY_LOGGING_LEVEL", "info")
            .extra_env("IGGY_LOGGING_FILE_ENABLED", "true")
            .extra_env("IGGY_LOGGING_PATH", "logs"),
    )
    .await;
    let (client_a, shadow) = create_and_follow(&pair).await;
    let client_b = pair.b().root_client().await.expect("a client of dc-b");
    let consumer = Consumer::new(Identifier::named("follower").expect("a consumer name"));
    send_batch(&client_a, "retired").await;
    assert_eq!(
        read_next(
            &pair,
            &client_b,
            shadow,
            &consumer,
            3,
            BATCH_MESSAGES as usize
        )
        .await,
        batch("retired")
    );
    let retired = pair
        .a()
        .bucket()
        .cluster_uid()
        .await
        .expect("the claim")
        .expect("dc-a claimed its bucket");
    let retired_segments = long_term_segments(pair.a().bucket()).await;
    drop(client_a);

    let harness_a = pair.cluster_mut(GeoSide::A).harness_mut();
    harness_a.kill_cluster().expect("dc-a loses its nodes");
    harness_a
        .node_mut(0)
        .add_env("IGGY_GEO_RECOVER_CLUSTER_UID", format!("{retired:032x}"));
    harness_a
        .restart_node_from_clean_slate(0)
        .expect("dc-a starts on an empty disk");
    let successor = claim_after(pair.a().bucket(), retired).await;
    let claimed_at = Instant::now();
    let layout = pair.a().bucket().layout().clone();
    let record = pair
        .a()
        .bucket()
        .object(&layout.recovery_key(retired))
        .await
        .expect("read the recovery record")
        .expect("the recovery stored its record");
    assert_eq!(
        RecoveryRecord::decode(&record)
            .expect("the record decodes")
            .successor_cluster_uid,
        successor
    );
    assert_eq!(
        long_term_segments(pair.a().bucket()).await,
        retired_segments,
        "the recovery keeps the manifest and the keys of the retired cluster"
    );
    let client_a = booted_client(pair.a()).await;
    assert_eq!(
        read_origin(&client_a, BATCH_MESSAGES as usize).await,
        batch("retired"),
        "the recovered dc-a serves what the bucket held"
    );
    // A topic that dc-a creates after its recovery gets no shadow in dc-b:
    // the adoption compares new topic objects with the retired cluster that
    // the shadow stream recorded. Only a topic shadowed before the recovery
    // is covered.
    send_batch(&client_a, "successor").await;

    let node_b = pair.b().harness().node(0);
    await_log(
        node_b,
        SUCCESSOR_LOG,
        0,
        SUCCESSOR_DEADLINE.saturating_sub(claimed_at.elapsed()),
    )
    .await;
    let accepted_within = claimed_at.elapsed();
    assert_eq!(
        read_next(
            &pair,
            &client_b,
            shadow,
            &consumer,
            3,
            BATCH_MESSAGES as usize
        )
        .await,
        batch("successor"),
        "the running dc-b goes on from where the consumer stopped"
    );
    assert!(
        !node_b.stdout_contains(CHANGED_LOG) && !node_b.stdout_contains(MISMATCH_LOG),
        "the running dc-b refused the successor"
    );
    let http_b = HttpClient::login_root(pair.b().harness()).await;
    let exposition = scrape(&http_b).await;
    assert_eq!(
        gauge(&exposition, "geo_peer_identity_verified", "dc-a"),
        Some(1)
    );
    assert_eq!(gauge(&exposition, "geo_peer_unreachable", "dc-a"), Some(0));
    drop((client_a, client_b, http_b));

    pair.cluster_mut(GeoSide::B)
        .stop()
        .await
        .expect("dc-b stops");
    let accepted_before_restart = logged(pair.b().harness().node(0), SUCCESSOR_LOG);
    let mirror = BucketMirror::start(
        pair.a().bucket().path(),
        &pair.b().harness().test_dir().join("mirror"),
    )
    .expect("a mirror of the bucket of dc-a");
    mirror
        .wait_for_pass(MIRROR_SYNC_DEADLINE)
        .await
        .expect("the mirror copies the bucket of dc-a");
    pair.cluster_mut(GeoSide::B)
        .harness_mut()
        .node_mut(0)
        .add_env(
            "IGGY_GEO_PEERS_0_PATH",
            mirror.path().to_string_lossy().into_owned(),
        );
    let watch = mirror.watch().expect("a watch on the mirror");
    sleep(WATCH_SETTLE).await;
    let started = std::time::Instant::now();
    pair.cluster_mut(GeoSide::B)
        .start()
        .await
        .expect("dc-b starts again");
    let client_b = booted_client(pair.b()).await;
    let node_b = pair.b().harness().node(0);
    let restarted_within = await_log(
        node_b,
        SUCCESSOR_LOG,
        accepted_before_restart,
        CHECK_DEADLINE,
    )
    .await;
    eprintln!(
        "dc-a {retired:032x} recovered as {successor:032x}; the running dc-b accepted it within \
         {accepted_within:?} of the claim, the restarted one within {restarted_within:?} of taking \
         a client"
    );
    let expected = [batch("retired"), batch("successor")].concat();
    assert_eq!(
        read_from_offset_zero(&client_b, shadow, 7, expected.len()).await,
        expected,
        "each message of both clusters once, in the origin's order"
    );
    assert!(
        !node_b.stdout_contains(CHANGED_LOG) && !node_b.stdout_contains(MISMATCH_LOG),
        "the restarted dc-b refused the successor"
    );
    await_peer_gauges(
        &HttpClient::login_root(pair.b().harness()).await,
        &[
            ("geo_peer_identity_verified", 1),
            ("geo_peer_unreachable", 0),
            ("geo_shadow_topics_stale", 0),
        ],
    )
    .await;

    let mut opens = Opens::default();
    for event in watch.reads() {
        if let BucketEvent::Read { at, paths } = event
            && at >= started
        {
            for path in &paths {
                let key = path.strip_prefix(mirror.path()).ok().and_then(Path::to_str);
                opens.count(&layout, key);
            }
        }
    }
    eprintln!("opens of the bucket of dc-a by the restarted dc-b: {opens:?}");
    let lost = events_lost(&watch);
    assert_eq!(
        lost, 0,
        "the watch on the mirror lost events, so its reads cannot show what the restarted dc-b \
         opened"
    );
    let identity_reads = opens.other.get("meta/identity").copied().unwrap_or(0);
    assert!(
        identity_reads > 0 && opens.recovery > 0 && opens.manifest_files > 0 && opens.segments > 0,
        "dc-b checks the claim, follows the recovery record, and reads the manifests and \
         segments: {opens:?}"
    );
    assert_eq!(opens.fresh, 0, "dc-b reads no fresh object: {opens:?}");
}

/// The ids of the shadow topic in `dc-b` of the geo topic of `dc-a`.
#[derive(Debug, Clone, Copy)]
struct Shadow {
    stream_id: u32,
    topic_id: u32,
}

/// What `dc-b` opened under its mirror of the bucket of `dc-a`, by what it
/// holds.
#[derive(Debug, Default)]
struct Opens {
    /// The manifests of the stream: its directories, listed.
    manifest_directories: u64,
    /// The manifest files, opened by a listing for their header or by a
    /// read.
    manifest_files: u64,
    fresh: u64,
    segments: u64,
    recovery: u64,
    /// The rest, which no partition owns: the identity, topic and positions
    /// objects and the directories above them, by the first two parts of
    /// their keys under the root.
    other: BTreeMap<String, u64>,
}

impl Opens {
    /// Count an open of `key`, the path under the mirror, `None` for one
    /// outside it.
    fn count(&mut self, layout: &KeyLayout, key: Option<&str>) {
        let Some(key) = key else {
            *self.other.entry(String::new()).or_default() += 1;
            return;
        };
        if key
            .rsplit('/')
            .next()
            .is_some_and(|name| name.starts_with('.'))
        {
            // A scratch file of the mirror.
            return;
        }
        if layout.parse_manifest_key(key).is_some() {
            self.manifest_files += 1;
            return;
        }
        let Some(rest) = key
            .strip_prefix(layout.root())
            .and_then(|rest| rest.strip_prefix('/'))
        else {
            *self.other.entry(key.to_owned()).or_default() += 1;
            return;
        };
        let parts: Vec<&str> = rest.split('/').collect();
        let is_id = |part: &&str| part.parse::<u64>().is_ok();
        match parts.as_slice() {
            ["meta", "fresh", ..] => self.fresh += 1,
            ["meta", "recovery", ..] => self.recovery += 1,
            ["meta", id, ..] if is_id(id) => self.manifest_directories += 1,
            [id, ..] if is_id(id) => self.segments += 1,
            _ => {
                let place = parts.iter().take(2).copied().collect::<Vec<_>>().join("/");
                *self.other.entry(place).or_default() += 1;
            }
        }
    }
}

/// How long each message of the steady load took from its acknowledgement
/// in `dc-a` to its read in `dc-b`.
struct Lags(Vec<Duration>);

impl Lags {
    fn percentile(&self, share: usize) -> Duration {
        self.0[(self.0.len() - 1) * share / 100]
    }

    fn report(&self, setting: &str) {
        eprintln!(
            "shadow lag at {setting} over {} sends: p50 {:?}, p95 {:?}, max {:?}",
            self.0.len(),
            self.percentile(50),
            self.percentile(95),
            self.percentile(100)
        );
    }

    fn assert_within(&self, bound: Duration) {
        assert!(
            self.percentile(100) < bound,
            "a message took {:?} to reach dc-b, more than {bound:?}, p50 {:?}",
            self.percentile(100),
            self.percentile(50)
        );
    }
}

async fn started_pair(builder: GeoClusterPairBuilder) -> GeoClusterPair {
    let mut pair = builder.build().expect("the pair");
    pair.start().await.expect("both clusters boot");
    pair
}

/// Send to the geo topic of `dc-a` every [`SEND_EVERY`] while a consumer in
/// `dc-b` reads its shadow from offset 0, and return how long each message
/// sent after the first read took.
async fn measure_lag(pair: &GeoClusterPair) -> Lags {
    let (client_a, shadow) = create_and_follow(pair).await;
    let client_b = pair.b().root_client().await.expect("a client of dc-b");
    let acked = Arc::new(Mutex::new(Vec::new()));
    let sent = Arc::new(Mutex::new(None));
    let reader = tokio::spawn(read_and_check(
        client_b,
        pair.a().bucket().clone(),
        shadow,
        Arc::clone(&acked),
        Arc::clone(&sent),
    ));
    let warm_up = Instant::now() + WARM_UP_DEADLINE;
    let mut index = 0;
    while first_read(&acked).is_none() {
        send_one(&client_a, index, &acked).await;
        index += 1;
        assert!(
            Instant::now() < warm_up && !reader.is_finished(),
            "dc-b served none of {index} messages"
        );
        sleep(SEND_EVERY).await;
    }
    let measured_from = index;
    let measure_until = Instant::now() + MEASURE_FOR;
    while Instant::now() < measure_until {
        send_one(&client_a, index, &acked).await;
        index += 1;
        sleep(SEND_EVERY).await;
    }
    *sent.lock().unwrap_or_else(PoisonError::into_inner) = Some(index);
    let read_at = reader.await.expect("the reader finishes");
    let acked = acked.lock().unwrap_or_else(PoisonError::into_inner);
    let mut lags: Vec<Duration> = (measured_from..index)
        .map(|index| {
            let index = usize::try_from(index).expect("an index");
            read_at[index].saturating_duration_since(acked[index].0)
        })
        .collect();
    lags.sort_unstable();
    Lags(lags)
}

/// The acknowledgement of each message, and whether it was read.
type Acked = Arc<Mutex<Vec<(Instant, bool)>>>;

/// The index of the first message read, `None` before one was.
fn first_read(acked: &Acked) -> Option<usize> {
    acked
        .lock()
        .unwrap_or_else(PoisonError::into_inner)
        .iter()
        .position(|(_, read)| *read)
}

/// Send the message `index` and record when `dc-a` acknowledged it.
async fn send_one(client: &IggyClient, index: u64, acked: &Acked) {
    send(client, index..index + 1).await;
    acked
        .lock()
        .unwrap_or_else(PoisonError::into_inner)
        .push((Instant::now(), false));
}

/// Read the shadow from offset 0 until every message that was sent came,
/// each once and in order, and return when each one was read. Each poll
/// serves no offset past the floor of the newest manifest in the bucket of
/// `dc-a`: the shadow serves only what a manifest it read lists.
async fn read_and_check(
    client: IggyClient,
    bucket_a: Bucket,
    shadow: Shadow,
    acked: Acked,
    sent: Arc<Mutex<Option<u64>>>,
) -> Vec<Instant> {
    let mut read_at: Vec<Instant> = Vec::new();
    let deadline = Instant::now() + WARM_UP_DEADLINE + MEASURE_FOR + READ_DEADLINE;
    let done = |read: usize| {
        sent.lock()
            .unwrap_or_else(PoisonError::into_inner)
            .is_some_and(|sent| read as u64 >= sent)
    };
    while !done(read_at.len()) {
        let polled = client
            .poll_messages(
                &numeric(shadow.stream_id),
                &numeric(shadow.topic_id),
                0,
                &Consumer::default(),
                &PollingStrategy::offset(read_at.len() as u64),
                PollOptions {
                    count: 100,
                    ..PollOptions::default()
                },
            )
            .await;
        let now = Instant::now();
        if let Ok(polled) = polled
            && let Some(last) = polled.messages.last()
        {
            let floor = origin_floor(&bucket_a).await;
            assert!(
                floor.is_some_and(|floor| last.header.offset <= floor),
                "dc-b served offset {} above the floor {floor:?} of every manifest of dc-a",
                last.header.offset
            );
            let mut acked = acked.lock().unwrap_or_else(PoisonError::into_inner);
            for message in &polled.messages {
                let index = read_at.len();
                assert_eq!(
                    (message.header.offset, payload_of(message)),
                    (index as u64, payload(index as u64)),
                    "each message once, at its offset"
                );
                if let Some(entry) = acked.get_mut(index) {
                    entry.1 = true;
                }
                read_at.push(now);
            }
        }
        assert!(
            Instant::now() < deadline,
            "dc-b served {} messages",
            read_at.len()
        );
        sleep(POLL_INTERVAL).await;
    }
    read_at
}

/// The highest floor of the manifests in `bucket`.
async fn origin_floor(bucket: &Bucket) -> Option<u64> {
    bucket
        .manifests()
        .await
        .expect("read the manifests of dc-a")
        .iter()
        .filter_map(|manifest| manifest.floor)
        .max()
}

/// Create the geo topic of one partition in `dc-a`, wait until `dc-b`
/// adopts it, and return a client of `dc-a` and the shadow.
async fn create_and_follow(pair: &GeoClusterPair) -> (IggyClient, Shadow) {
    let client_a = pair.a().root_client().await.expect("a client of dc-a");
    client_a
        .create_stream(STREAM, &StreamCreateOptions::default())
        .await
        .expect("create the stream");
    let topic = pair
        .a()
        .create_geo_topic(&client_a, &named(STREAM), TOPIC, 1)
        .await
        .expect("dc-a takes the geo topic");
    let client_b = pair.b().root_client().await.expect("a client of dc-b");
    let stream = pair
        .cluster(GeoSide::B)
        .wait_for_shadow_topic(&client_b, STREAM, topic.id)
        .await
        .expect("dc-b adopts the geo topic");
    (
        client_a,
        Shadow {
            stream_id: stream.id,
            topic_id: topic.id,
        },
    )
}

/// One poll of the shadow from offset 0, which serves none of the messages
/// of the new cluster: an error, nothing, or the old cluster's.
async fn assert_serves_no_new_message(client: &IggyClient, shadow: Shadow) {
    let Ok(polled) = client
        .poll_messages(
            &numeric(shadow.stream_id),
            &numeric(shadow.topic_id),
            0,
            &Consumer::default(),
            &PollingStrategy::offset(0),
            PollOptions {
                count: 100,
                ..PollOptions::default()
            },
        )
        .await
    else {
        return;
    };
    let served: Vec<String> = polled.messages.iter().map(payload_of).collect();
    assert!(
        served.iter().all(|payload| !payload.starts_with("new")),
        "dc-b served the new cluster's messages through the old shadow: {served:?}"
    );
}

/// Wait until a client of `cluster` logs in, after a restart.
async fn booted_client(cluster: &GeoCluster) -> IggyClient {
    let deadline = Instant::now() + BOOT_DEADLINE;
    loop {
        match cluster.root_client().await {
            Ok(client) => return client,
            Err(error) => {
                assert!(
                    Instant::now() < deadline,
                    "{} never took a client: {error}",
                    cluster.origin()
                );
                sleep(Duration::from_millis(250)).await;
            }
        }
    }
}

/// Wait until the identity object of `bucket` names another cluster than
/// `retired`, and return that cluster.
async fn claim_after(bucket: &Bucket, retired: u128) -> u128 {
    let deadline = Instant::now() + RECOVERY_DEADLINE;
    loop {
        if let Ok(Some(claim)) = bucket.cluster_uid().await
            && claim != retired
        {
            return claim;
        }
        assert!(
            Instant::now() < deadline,
            "no cluster took the root of {retired:032x}"
        );
        sleep(RETRY_INTERVAL).await;
    }
}

/// Wait until `node` logged `marker` more than `before` times, and return how
/// long that took. A wait whose `within` ran out fails even if the marker is
/// there.
async fn await_log(node: &ServerHandle, marker: &str, before: usize, within: Duration) -> Duration {
    let started = Instant::now();
    loop {
        let waited = started.elapsed();
        assert!(
            waited < within,
            "the node never logged {marker:?} within {within:?}"
        );
        if logged(node, marker) > before {
            return waited;
        }
        sleep(RETRY_INTERVAL).await;
    }
}

/// How many times `node` logged `marker` in its own log files. They keep every
/// run of the node in any test mode, so a count taken before a restart tells
/// the runs apart.
fn logged(node: &ServerHandle, marker: &str) -> usize {
    let Ok(entries) = fs::read_dir(node.data_path().join("logs")) else {
        return 0;
    };
    entries
        .flatten()
        .filter_map(|entry| fs::read_to_string(entry.path()).ok())
        .map(|log| log.matches(marker).count())
        .sum()
}

/// How many times `watch` lost events, after which its reads are incomplete.
fn events_lost(watch: &BucketWatch) -> usize {
    watch
        .changes()
        .iter()
        .filter(
            |change| matches!(change, BucketEvent::Change { kind, .. } if kind == "events lost"),
        )
        .count()
}

/// Scrape `/metrics` through `http` until each gauge of the peer `dc-a`
/// reads its value in `expected`. A node that just started settles them only
/// after its first passes over the peer.
async fn await_peer_gauges(http: &HttpClient, expected: &[(&str, u64)]) {
    let deadline = Instant::now() + CHECK_DEADLINE;
    loop {
        let exposition = scrape(http).await;
        let read: Vec<Option<u64>> = expected
            .iter()
            .map(|(name, _)| gauge(&exposition, name, "dc-a"))
            .collect();
        if read
            .iter()
            .zip(expected)
            .all(|(read, (_, value))| *read == Some(*value))
        {
            return;
        }
        assert!(
            Instant::now() < deadline,
            "the gauges of dc-a read {read:?}, not {expected:?}"
        );
        sleep(RETRY_INTERVAL).await;
    }
}

/// Every message of the geo topic of `dc-a` from offset 0, once it serves
/// `wanted`.
async fn read_origin(client: &IggyClient, wanted: usize) -> Vec<String> {
    let deadline = Instant::now() + READ_DEADLINE;
    loop {
        if let Ok(polled) = client
            .poll_messages(
                &named(STREAM),
                &named(TOPIC),
                0,
                &Consumer::default(),
                &PollingStrategy::offset(0),
                PollOptions {
                    count: 100,
                    ..PollOptions::default()
                },
            )
            .await
            && polled.messages.len() >= wanted
        {
            return polled.messages.iter().map(payload_of).collect();
        }
        assert!(
            Instant::now() < deadline,
            "dc-a never served {wanted} messages"
        );
        sleep(POLL_INTERVAL).await;
    }
}

/// Create the stream and the geo topic of [`create_and_follow`] in `dc-a`
/// again, and return the topic's id and its stream's.
async fn recreate(pair: &GeoClusterPair, client: &IggyClient) -> (u32, u32) {
    let stream = client
        .create_stream(STREAM, &StreamCreateOptions::default())
        .await
        .expect("create the stream again");
    let topic = pair
        .a()
        .create_geo_topic(client, &named(STREAM), TOPIC, 1)
        .await
        .expect("dc-a takes the geo topic again");
    (topic.id, stream.id)
}

/// [`BATCH_MESSAGES`] messages of the cluster `cluster`, all of one length.
fn batch(cluster: &str) -> Vec<String> {
    (0..BATCH_MESSAGES)
        .map(|index| format!("{cluster}-cluster-{index:05}"))
        .collect()
}

/// Send [`batch`] of `cluster` to the geo topic of `dc-a` in one send.
async fn send_batch(client: &IggyClient, cluster: &str) {
    let mut messages: Vec<IggyMessage> = batch(cluster)
        .into_iter()
        .map(|payload| {
            IggyMessage::builder()
                .payload(payload.into_bytes().into())
                .build()
                .expect("a message")
        })
        .collect();
    client
        .send_messages(
            &named(STREAM),
            &named(TOPIC),
            &Partitioning::partition_id(0),
            &mut messages,
        )
        .await
        .expect("dc-a takes the send");
}

/// The keys of the objects of every long-term segment that a manifest in
/// `bucket` lists, sorted.
async fn long_term_segments(bucket: &Bucket) -> Vec<String> {
    let layout = bucket.layout();
    let mut keys = Vec::new();
    for manifest in bucket.manifests().await.expect("read the manifests") {
        for segment in &manifest.segments {
            for object in SegmentObject::ALL {
                keys.push(if manifest.segments_keyed_by_both_offsets {
                    layout.long_term_key(&LongTermKey {
                        prefix: manifest.prefix,
                        start_offset: segment.start_offset,
                        end_offset: segment.end_offset,
                        object,
                    })
                } else {
                    layout.segment_key(&SegmentKey {
                        prefix: manifest.prefix,
                        start_offset: segment.start_offset,
                        object,
                    })
                });
            }
        }
    }
    keys.sort();
    keys
}

/// Send to the geo topic of `dc-a`, one message at a time, until `producing`
/// is cleared, and return how many it sent.
async fn produce_until_stopped(client: IggyClient, producing: Arc<AtomicBool>) -> u64 {
    let mut index = 0;
    while producing.load(Ordering::Acquire) {
        send(&client, index..index + 1).await;
        index += 1;
    }
    index
}

fn named(name: &str) -> Identifier {
    Identifier::named(name).expect("a valid identifier")
}

fn numeric(id: u32) -> Identifier {
    Identifier::numeric(id).expect("a valid identifier")
}

fn payload(index: u64) -> String {
    format!("message-{index}")
}

fn payload_of(message: &IggyMessage) -> String {
    String::from_utf8_lossy(&message.payload).into_owned()
}

/// Send the messages `indexes` to the geo topic of `dc-a`, one a send.
async fn send(client: &IggyClient, indexes: std::ops::Range<u64>) {
    for index in indexes {
        let mut messages = [IggyMessage::builder()
            .payload(payload(index).into_bytes().into())
            .build()
            .expect("a message")];
        client
            .send_messages(
                &named(STREAM),
                &named(TOPIC),
                &Partitioning::partition_id(0),
                &mut messages,
            )
            .await
            .expect("dc-a takes the send");
    }
}

/// Poll the next messages of `consumer`, `count` at a time and committing
/// what it read, until `wanted` arrived. No poll serves an offset past the
/// floor of the newest manifest of `dc-a`.
async fn read_next(
    pair: &GeoClusterPair,
    client: &IggyClient,
    shadow: Shadow,
    consumer: &Consumer,
    count: u32,
    wanted: usize,
) -> Vec<String> {
    let mut read = Vec::new();
    let deadline = Instant::now() + READ_DEADLINE;
    while read.len() < wanted {
        let polled = client
            .poll_messages(
                &numeric(shadow.stream_id),
                &numeric(shadow.topic_id),
                0,
                consumer,
                &PollingStrategy::next(),
                PollOptions {
                    count,
                    commit: CommitPolicy::OnRead,
                    ..PollOptions::default()
                },
            )
            .await;
        if let Ok(polled) = polled
            && let Some(last) = polled.messages.last()
        {
            let floor = origin_floor(pair.a().bucket()).await;
            assert!(
                floor.is_some_and(|floor| last.header.offset <= floor),
                "dc-b served offset {} above the floor {floor:?} of every manifest of dc-a",
                last.header.offset
            );
            read.extend(polled.messages.iter().map(payload_of));
        }
        assert!(
            Instant::now() < deadline,
            "dc-b served {} of {wanted} messages",
            read.len()
        );
        sleep(POLL_INTERVAL).await;
    }
    read
}

/// Every message of the shadow from offset 0, `count` a poll, until
/// `wanted` came.
async fn read_from_offset_zero(
    client: &IggyClient,
    shadow: Shadow,
    count: u32,
    wanted: usize,
) -> Vec<String> {
    let mut read = Vec::new();
    let deadline = Instant::now() + READ_DEADLINE;
    while read.len() < wanted {
        let polled = client
            .poll_messages(
                &numeric(shadow.stream_id),
                &numeric(shadow.topic_id),
                0,
                &Consumer::default(),
                &PollingStrategy::offset(read.len() as u64),
                PollOptions {
                    count,
                    ..PollOptions::default()
                },
            )
            .await;
        if let Ok(polled) = polled {
            for message in &polled.messages {
                assert_eq!(message.header.offset, read.len() as u64);
                read.push(payload_of(message));
            }
        }
        assert!(
            Instant::now() < deadline,
            "dc-b served {} of {wanted} messages from the start",
            read.len()
        );
        sleep(POLL_INTERVAL).await;
    }
    read
}

async fn scrape(http: &HttpClient) -> String {
    let response = http.get("/metrics").await;
    assert!(response.status().is_success(), "/metrics scrape failed");
    response.text().await.expect("the metrics text")
}

/// The listings of the manifests of `dc-a` that `dc-b` sent, and the reads.
fn manifest_requests(exposition: &str) -> (u64, u64) {
    let sum = |request: &str| {
        exposition
            .lines()
            .filter(|line| {
                line.starts_with("geo_peer_manifest_requests_total")
                    && line.contains(&format!("request=\"{request}\""))
            })
            .filter_map(|line| line.rsplit(' ').next()?.parse::<f64>().ok())
            .map(|value| value as u64)
            .sum()
    };
    (sum("list"), sum("get"))
}
