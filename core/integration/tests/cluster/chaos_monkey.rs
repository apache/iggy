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

//! Runs producers and consumers against five nodes while killing and restarting
//! one random node at a time. This is an opt-in test that takes several minutes.
//!
//! # What it checks
//!
//! Producers put their identity and sequence number in each message id. The
//! test checks acknowledged messages against those ids, their payloads and
//! their order. Consumer group members join and leave during the run, while
//! standalone consumers also read and commit offsets.
//!
//! The main topic uses small segments and expires old messages. A second topic
//! keeps its full history so the final checks can cover sealed segments too.
//! Both use the default `replicated` durability. After the kills stop, readers
//! drain their partitions. The test compares each node's messages, stored
//! offsets and group membership. It then stops every node and, if the online
//! checks passed, compares the persisted segment files byte for byte.
//!
//! Missing, corrupt, reordered, phantom or unexplained duplicate messages fail
//! the test. So do partial batches, incorrect offset confirmations, missed
//! deliveries, conflicting reads or group assignments, unexpected node exits
//! or task panics, and replicas that disagree. Expiry must actually delete
//! segments during the run. A short run that never exercises expiry fails.
//!
//! # Running it
//!
//! Build the server first. The test starts the binary already in `target/`.
//!
//! ```text
//! cargo build --bin iggy-server
//! IGGY_TEST_CHAOS_DURATION_SECS=120 IGGY_TEST_CHAOS_KILL_INTERVAL_SECS=15 \
//! IGGY_TEST_CHAOS_DOWNTIME_SECS=5 IGGY_TEST_CHAOS_SEED=7 \
//!     cargo nextest run -p integration --run-ignored only --no-capture -E 'test(chaos_monkey)'
//! ```
//!
//! Duration, kill interval and downtime default to 120, 15 and 5 seconds.
//! Without `IGGY_TEST_CHAOS_SEED`, each run chooses and prints a random seed.
//! Downtime must be shorter than the kill interval, and the duration must
//! allow at least one kill and restart. These are chaos-phase durations;
//! startup, draining and verification take additional time.
//!
//! The seed repeats random choices, not thread scheduling, disk latency or
//! recovery progress. A failure is still a failed invariant, not an expected
//! consequence of choosing a random workload. To investigate a run-order
//! effect, repeat the same configurations alone and in both orders, using the
//! same server binary. Compare actual fault times and retained message ranges.
//!
//! # Reports and logs
//!
//! The printed artifact directory under `test_logs/` is retained on success
//! and failure. Remove it when the investigation is done; it includes server
//! data as well as logs. `chaos.jsonl` contains one JSON object per line:
//! metadata, phase changes, fault events, node views, retained message ranges,
//! counters, check results and the final verdict. Metadata includes the server
//! binary's SHA-256, host details and workload settings. Each node record points
//! to its saved effective configuration.
//!
//! Events are written as they happen. A report without a `finished` event is
//! incomplete, for example after a timeout or forced termination. A `panicked`
//! verdict records an interrupted check, not a successful run. The final disk
//! comparison is explicitly skipped when the online checks fail.
//!
//! Current captures are `server_<id>_stdout.log` and `server_<id>_stderr.log`.
//! Earlier captures are saved in `server_<id>_incarnation_<n>/` before each
//! restart, including startup retries. This test captures logs even when
//! `IGGY_TEST_VERBOSE` is set, and prints only short failure excerpts.
//! Client logs and the test's panic text are saved in `test_stdout.log`.
//!
//! # Observed failures and check limitations
//!
//! These are investigation notes, not reasons to accept a failing run:
//!
//! - Earlier runs found a rejoining node with committed messages still only
//!   in memory and different segment files after shutdown. The shutdown path
//!   has since changed, so that diagnosis needs rechecking. The stress case
//!   used seed 99, 180 seconds, a 14-second kill interval and 10-second downtime.
//!   That configuration does not fail on every run.
//! - Under `replicated` durability, a restarted node repairs partition logs
//!   from op 1. Repairing replicas can stop contributing to the write quorum.
//!   Heavy disk load has stalled partitions for over a minute.
//! - A `next` poll can observe a node before an acknowledged offset store has
//!   been applied there. The test counts these reads as redeliveries and checks
//!   stored offsets at the end; it cannot distinguish them from a lost store
//!   during the workload.
//! - HTTP group-offset reads currently resolve a standalone consumer instead.
//!   The test reads group offsets over TCP.
//! - A committed send or retried duplicate can be acknowledged without offset
//!   confirmation. The test checks the batch's messages but cannot verify its
//!   acknowledged placement. It fails if a producer gets no confirmations.
//! - Metadata log divergence has been observed separately in the simulator
//!   with `workload-fuzz --seed 62 --plane metadata --faults swarm --crash-prob
//!   0.01 --restart-prob 0.05 --crash-primary`. One traced path adopts a
//!   `StartView` probe without canonical headers and commits a discarded-view
//!   operation. This real-process test has not established that failure path.
//!
//! The `IGGY_TEST_` variables are test controls. They are forwarded to child
//! processes, whose environment validation permits that prefix.

use std::collections::{BTreeMap, HashMap, HashSet, btree_map};
use std::fmt;
use std::fs::File;
use std::future::Future;
use std::io::Read;
use std::ops::RangeInclusive;
use std::panic::{AssertUnwindSafe, resume_unwind};
use std::path::PathBuf;
use std::process::Command;
use std::str::FromStr;
use std::sync::{Arc, Mutex};
use std::time::{Duration, SystemTime};

use assert_cmd::cargo::CommandCargoExt;
use bytes::Bytes;
use futures::{FutureExt, StreamExt};
use iggy::prelude::*;
use integration::harness::{
    TestBinary, TestBinaryError, TestHarness, TestServerConfig, disk, resolve_config_paths,
};
use rand::rngs::StdRng;
use rand::{RngExt, SeedableRng};
use ring::digest::{Context, SHA256};
use serde::Serialize;
use serde_json::json;
use serial_test::parallel;
use sysinfo::System;
use tokio::sync::watch;
use tokio::task::JoinHandle;
use tokio::time::{Instant, sleep, timeout, timeout_at};

use self::report::Report;

mod report;

const CLUSTER_NODES: usize = 5;
const STREAM_NAME: &str = "chaos-stream";
const TOPIC_NAME: &str = "chaos-topic";
const HISTORY_TOPIC_NAME: &str = "chaos-history";
const GROUP_NAME: &str = "chaos-group";
const PARTITIONS: u32 = 4;
/// Two producers per partition, so their batches interleave.
const PRODUCERS: u32 = 8;
/// One per partition of the history topic. Their ids follow the main topic's
/// producers, so every message id stays unique.
const HISTORY_PRODUCERS: u32 = PARTITIONS;
/// Low-level members that churn. The `IggyConsumer` comes on top.
const GROUP_MEMBERS: u32 = 3;
/// The smallest segment the server accepts, so segments rotate under the kills.
const SEGMENT_SIZE_BYTES: u64 = 1024 * 1024;
/// A partition fills a segment in about half a minute, so sealed segments
/// expire and get deleted during the run, while every reader stays much
/// closer to the end than this.
const MESSAGE_EXPIRY: Duration = Duration::from_secs(30);
/// Standalone consumers get this id plus their partition. Away from
/// `VERIFIER_CONSUMER_ID`.
const FIRST_CONSUMER_ID: u32 = 10;
/// The server hashes this name into the standalone `IggyConsumer`'s id.
const STANDALONE_ICONSUMER_NAME: &str = "chaos-consumer";
const STANDALONE_ICONSUMER_PARTITION: u32 = 0;

const DURATION_ENV: &str = "IGGY_TEST_CHAOS_DURATION_SECS";
const KILL_INTERVAL_ENV: &str = "IGGY_TEST_CHAOS_KILL_INTERVAL_SECS";
const DOWNTIME_ENV: &str = "IGGY_TEST_CHAOS_DOWNTIME_SECS";
const SEED_ENV: &str = "IGGY_TEST_CHAOS_SEED";
const DEFAULT_DURATION: Duration = Duration::from_secs(120);
const DEFAULT_KILL_INTERVAL: Duration = Duration::from_secs(15);
const DEFAULT_DOWNTIME: Duration = Duration::from_secs(5);

/// Every reconnect gives a client a new id, so a member whose node dies stays
/// behind as a stale member until its lease expires. These values (taken from
/// client_table_restart.rs) expire it within one kill interval instead of the
/// default 30 s.
const GROUP_HEARTBEAT_INTERVAL: &str = "500ms";
const GROUP_SESSION_TIMEOUT: &str = "8s";
/// Each node runs its own segment cleaner on a timer that starts at boot.
/// With the default one-minute interval, a node killed within a minute of its
/// boot never cleans, so expiry would barely run during the chaos phase.
const CLEANER_INTERVAL: &str = "1s";

const PRODUCER_BATCH: u32 = 10;
const PRODUCER_PAUSE: Duration = Duration::from_millis(50);
const POLL_BATCH: u32 = 100;
const POLL_PAUSE: Duration = Duration::from_millis(50);
const RETRY_PAUSE: Duration = Duration::from_millis(100);
const MEMBER_SESSION_MS: RangeInclusive<u64> = 1_000..=4_000;
const MEMBER_PAUSE_MS: RangeInclusive<u64> = 0..=1_000;
const ICONSUMER_COMMIT_INTERVAL: Duration = Duration::from_secs(1);

/// Above the SDK's own 30 s response timeout, so only a call that outlives
/// its own deadline trips it.
const CALL_TIMEOUT: Duration = Duration::from_secs(45);
const CALL_TIMED_OUT: &str = "call_timed_out";
const LEADER_LOOKUP_TIMEOUT: Duration = Duration::from_secs(2);
const HEALTH_CHECK_INTERVAL: Duration = Duration::from_secs(1);
const GROUP_SAMPLE_INTERVAL: Duration = Duration::from_secs(1);
const RECOVERY_TIMEOUT: Duration = Duration::from_secs(60);
const DRAIN_TIMEOUT: Duration = Duration::from_secs(90);
const DRAIN_CHECK_INTERVAL: Duration = Duration::from_millis(500);
const CONVERGENCE_TIMEOUT: Duration = Duration::from_secs(60);
const CONVERGENCE_RETRY: Duration = Duration::from_secs(1);

const VERIFIER_CONSUMER_ID: u32 = 1;
const VERIFY_BATCH: u32 = 1_000;
const STDERR_TAIL_LINES: usize = 20;
const REPORTED_EXAMPLES: usize = 5;
const FAILURE_SUMMARY_CHARS: usize = 240;
const DIGEST_BUFFER_SIZE: usize = 64 * 1024;

type SharedLog = Arc<Mutex<ObservedLog>>;

#[tokio::test(flavor = "multi_thread")]
#[parallel]
#[ignore = "runs for minutes against a five-node cluster; run with --run-ignored only"]
async fn given_five_nodes_when_one_is_killed_every_interval_should_keep_acked_messages() {
    crate::use_saved_logs_on_failure();
    let config = ChaosConfig::from_env();
    let mut harness = build_cluster();
    let mut report =
        Report::create(harness.test_dir(), Instant::now()).expect("create chaos report");
    println!("{config}\nartifacts: {}", harness.test_dir().display());
    report
        .record("started", json!({"command_env": config.to_string()}))
        .expect("write run start");

    let result = AssertUnwindSafe(run_chaos(&mut harness, &config, &mut report))
        .catch_unwind()
        .await;
    let violations = match result {
        Ok(violations) => violations,
        Err(payload) => {
            let message = payload
                .downcast_ref::<String>()
                .map(String::as_str)
                .or_else(|| payload.downcast_ref::<&str>().copied())
                .unwrap_or("non-string panic payload");
            if let Err(error) =
                report.record("finished", json!({"outcome": "panicked", "error": message}))
            {
                eprintln!(
                    "cannot record panic in {}: {error}",
                    report.path().display()
                );
            }
            resume_unwind(payload);
        }
    };
    report
        .record(
            "finished",
            json!({
                "outcome": if violations.is_empty() { "passed" } else { "failed" },
                "violations": violations,
            }),
        )
        .expect("write final verdict");
    let examples = violations
        .iter()
        .take(REPORTED_EXAMPLES)
        .map(|violation| {
            violation
                .lines()
                .next()
                .unwrap_or_default()
                .chars()
                .take(FAILURE_SUMMARY_CHARS)
                .collect::<String>()
        })
        .collect::<Vec<_>>()
        .join("\n");
    assert!(
        violations.is_empty(),
        "{} violations:\n{examples}\nreport: {}",
        violations.len(),
        report.path().display()
    );
    println!("all checks passed; report: {}", report.path().display());
}

async fn run_chaos(
    harness: &mut TestHarness,
    config: &ChaosConfig,
    report: &mut Report,
) -> Vec<String> {
    report
        .record("metadata", run_metadata(config))
        .expect("write run metadata");
    harness.start().await.expect("start the cluster");
    report_nodes(harness, report, "cluster_started");
    create_topic_and_group(harness).await;

    let started = Instant::now();
    report
        .record("phase", json!({"name": "chaos"}))
        .expect("write chaos phase");
    let workload = Workload::spawn(harness, config.seed, started).await;
    let mut violations = Vec::new();
    // Kill and restart block the calling thread for seconds. The multi-thread
    // runtime keeps the workload running on its worker threads meanwhile.
    let kills = run_monkey(harness, config, started, &mut violations, report).await;
    report
        .record("phase", json!({"name": "drain"}))
        .expect("write drain phase");
    let mut outcomes = workload.finish().await;
    outcomes.check(&mut violations);
    report
        .record(
            "check",
            json!({
                "name": "workload",
                "outcome": if violations.is_empty() { "passed" } else { "failed" },
                "violations": violations,
            }),
        )
        .expect("write workload check");

    report
        .record("phase", json!({"name": "verify"}))
        .expect("write verification phase");
    let verifiers = connect_verifiers(harness, &mut violations).await;
    let view = wait_for_convergence(
        &verifiers,
        &outcomes.readers,
        started,
        &mut violations,
        report,
    )
    .await;
    let stats = verify_logs(
        &verifiers,
        view.as_ref(),
        &mut outcomes,
        &mut violations,
        report,
    )
    .await;
    for node in 0..CLUSTER_NODES {
        if let Some(stderr) = panic_report(harness, node) {
            violations.push(format!("node {node} panicked:\n{stderr}"));
        }
    }

    record_summary(report, &kills, &outcomes, view.as_ref(), &stats);
    report
        .record(
            "check",
            json!({
                "name": "online",
                "outcome": if violations.is_empty() { "passed" } else { "failed" },
                "violations": violations,
            }),
        )
        .expect("write online checks");

    // A graceful stop flushes what each node still holds in memory, so the
    // segment files can be compared byte for byte at rest.
    let data_paths: Vec<PathBuf> = harness
        .all_servers()
        .iter()
        .map(|server| server.data_path())
        .collect();
    report
        .record("phase", json!({"name": "stop"}))
        .expect("write stop phase");
    harness
        .stop()
        .await
        .expect("stop the cluster for the at-rest comparison");
    report_nodes(harness, report, "cluster_stopped");
    for node in 0..CLUSTER_NODES {
        if let Some(stderr) = panic_report(harness, node) {
            let violation = format!("node {node} panicked:\n{stderr}");
            if !violations.contains(&violation) {
                violations.push(violation);
            }
        }
    }
    if !violations.is_empty() {
        report
            .record(
                "check",
                json!({
                    "name": "replica_files",
                    "outcome": "skipped",
                    "reason": "online or shutdown checks failed",
                }),
            )
            .expect("write skipped disk check");
        return violations;
    }
    report
        .record("phase", json!({"name": "replica_files"}))
        .expect("write disk comparison phase");
    let comparison = disk::compare_replica_data(&data_paths, false);
    report
        .record(
            "check",
            json!({
                "name": "replica_files",
                "outcome": if comparison.problems.is_empty() { "passed" } else { "failed" },
                "files": comparison.files,
                "violations": comparison.problems,
            }),
        )
        .expect("write disk comparison result");
    violations.extend(comparison.problems);
    violations
}

#[derive(Debug, Clone, Copy)]
struct ChaosConfig {
    duration: Duration,
    kill_interval: Duration,
    downtime: Duration,
    seed: u64,
}

impl ChaosConfig {
    fn from_env() -> Self {
        let config = Self {
            duration: env_value(DURATION_ENV).map_or(DEFAULT_DURATION, Duration::from_secs),
            kill_interval: env_value(KILL_INTERVAL_ENV)
                .map_or(DEFAULT_KILL_INTERVAL, Duration::from_secs),
            downtime: env_value(DOWNTIME_ENV).map_or(DEFAULT_DOWNTIME, Duration::from_secs),
            seed: env_value(SEED_ENV).unwrap_or_else(rand::random),
        };
        assert!(
            config.downtime < config.kill_interval,
            "{DOWNTIME_ENV} must be below {KILL_INTERVAL_ENV}, so at most one node is down: \
             {config}"
        );
        assert!(
            config.kill_interval + config.downtime <= config.duration,
            "{DURATION_ENV} must leave room for at least one kill and restart: {config}"
        );
        config
    }
}

impl fmt::Display for ChaosConfig {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "{DURATION_ENV}={} {KILL_INTERVAL_ENV}={} {DOWNTIME_ENV}={} {SEED_ENV}={}",
            self.duration.as_secs(),
            self.kill_interval.as_secs(),
            self.downtime.as_secs(),
            self.seed
        )
    }
}

fn env_value<T: FromStr>(name: &str) -> Option<T>
where
    T::Err: fmt::Display,
{
    let value = std::env::var(name).ok()?;
    Some(
        value
            .parse()
            .unwrap_or_else(|error| panic!("{name}={value} is not valid: {error}")),
    )
}

fn build_cluster() -> TestHarness {
    let overrides = HashMap::from([
        (
            "consumer_group.heartbeat_interval".to_owned(),
            GROUP_HEARTBEAT_INTERVAL.to_owned(),
        ),
        (
            "consumer_group.session_timeout".to_owned(),
            GROUP_SESSION_TIMEOUT.to_owned(),
        ),
        (
            "data_maintenance.messages.interval".to_owned(),
            CLEANER_INTERVAL.to_owned(),
        ),
    ]);
    let extra_envs = resolve_config_paths(&overrides).expect("valid server config paths");
    TestHarness::builder()
        .server(
            TestServerConfig::builder()
                .extra_envs(extra_envs)
                .preserve_logs_on_restart(true)
                .dump_logs_on_failure(false)
                .build(),
        )
        .cluster_nodes(CLUSTER_NODES)
        .cleanup(false)
        .build()
        .expect("build the harness")
}

async fn create_topic_and_group(harness: &TestHarness) {
    let target = Target::new();
    let client = harness.tcp_root_client().await.expect("root client");
    client
        .create_stream(STREAM_NAME)
        .await
        .expect("create the stream");
    let options = TopicCreateOptions {
        partitions_count: Some(PARTITIONS),
        message_expiry: Some(IggyExpiry::ExpireDuration(IggyDuration::from(
            MESSAGE_EXPIRY,
        ))),
        segment_size: Some(IggyByteSize::from(SEGMENT_SIZE_BYTES)),
        ..TopicCreateOptions::default()
    };
    client
        .create_topic(&target.stream, TOPIC_NAME, &options)
        .await
        .expect("create the topic");
    let history = TopicCreateOptions {
        partitions_count: Some(PARTITIONS),
        segment_size: Some(IggyByteSize::from(SEGMENT_SIZE_BYTES)),
        ..TopicCreateOptions::default()
    };
    client
        .create_topic(&target.stream, HISTORY_TOPIC_NAME, &history)
        .await
        .expect("create the history topic");
    client
        .create_consumer_group(&target.stream, &target.topic, GROUP_NAME)
        .await
        .expect("create the consumer group");
}

/// Spreads first dials over the nodes. The SDK moves every client to the
/// leader after sign-in anyway.
struct Dialer<'a> {
    harness: &'a TestHarness,
    dialed: usize,
}

impl Dialer<'_> {
    async fn connect(&mut self) -> IggyClient {
        let node = self.dialed % CLUSTER_NODES;
        self.dialed += 1;
        self.harness
            .node(node)
            .tcp_client()
            .expect("every node exposes TCP")
            .with_reconnecting_root_login()
            .connect()
            .await
            .expect("connect a TCP client")
    }
}

#[derive(Clone)]
struct Target {
    /// The topic's name, for reports.
    name: &'static str,
    stream: Identifier,
    topic: Identifier,
    group: Identifier,
}

impl Target {
    fn new() -> Self {
        Self {
            name: TOPIC_NAME,
            stream: Identifier::named(STREAM_NAME).expect("valid stream name"),
            topic: Identifier::named(TOPIC_NAME).expect("valid topic name"),
            group: Identifier::named(GROUP_NAME).expect("valid group name"),
        }
    }

    /// The history topic has no group, so `group` does not apply to it.
    fn history() -> Self {
        Self {
            name: HISTORY_TOPIC_NAME,
            topic: Identifier::named(HISTORY_TOPIC_NAME).expect("valid topic name"),
            ..Self::new()
        }
    }
}

#[derive(Clone)]
struct TaskContext {
    target: Target,
    log: SharedLog,
    started: Instant,
}

/// A consumer identity with stored offsets of its own: the group, or a
/// standalone consumer. It must receive every acknowledged message of its
/// partitions, and no node deletes a segment past its stored offset.
struct Reader {
    name: String,
    consumer: Consumer,
    /// `None` for the group, which reads every partition.
    partition: Option<u32>,
    received: Mutex<Received>,
}

#[derive(Default)]
struct Received {
    ids: HashSet<u128>,
    deliveries: usize,
}

impl Reader {
    fn new(name: String, consumer: Consumer, partition: Option<u32>) -> Arc<Self> {
        Arc::new(Self {
            name,
            consumer,
            partition,
            received: Mutex::default(),
        })
    }

    fn reads(&self, partition: u32) -> bool {
        self.partition.is_none_or(|own| own == partition)
    }

    /// `acked` holds the acknowledged ids of each partition.
    fn received_all(&self, acked: &[HashSet<u128>]) -> bool {
        let received = self.received.lock().expect("received ids lock");
        (0u32..)
            .zip(acked)
            .all(|(partition, ids)| !self.reads(partition) || ids.is_subset(&received.ids))
    }
}

/// Every offset a reader or a node returned, with the message first read
/// there. Any later read of the offset must return the same message.
struct ObservedLog {
    partitions: Vec<BTreeMap<u64, LogEntry>>,
    conflicts: Vec<String>,
}

impl Default for ObservedLog {
    fn default() -> Self {
        Self {
            partitions: (0..PARTITIONS).map(|_| BTreeMap::new()).collect(),
            conflicts: Vec::new(),
        }
    }
}

impl ObservedLog {
    fn observe(&mut self, source: &str, partition: u32, entry: LogEntry) {
        let Some(entries) = self.partitions.get_mut(partition as usize) else {
            self.conflicts.push(format!(
                "{source} read partition {partition} of a {PARTITIONS}-partition topic"
            ));
            return;
        };
        match entries.entry(entry.offset) {
            btree_map::Entry::Vacant(slot) => {
                slot.insert(entry);
            }
            btree_map::Entry::Occupied(slot) if *slot.get() != entry => {
                self.conflicts.push(format!(
                    "partition {partition}: {source} read {entry:?}, an earlier read got {:?}",
                    slot.get()
                ));
            }
            btree_map::Entry::Occupied(_) => {}
        }
    }

    /// The highest offset read in each partition.
    fn ends(&self) -> Vec<Option<u64>> {
        self.partitions
            .iter()
            .map(|entries| entries.keys().next_back().copied())
            .collect()
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum ProducerPhase {
    Send,
    /// Get the batch in flight acknowledged, then stop.
    Finish,
    /// Stop now, even with a batch in flight.
    Abandon,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum ConsumerPhase {
    /// Members join, poll for a random while, leave, pause and repeat.
    /// Standalone consumers poll.
    Consume,
    /// Keep polling until every reader received and committed everything.
    Drain,
    /// Leave the group and stop.
    Stop,
}

struct Workload {
    /// The main topic's producers, then the history topic's.
    producers: Vec<JoinHandle<ProducerOutcome>>,
    consumers: Vec<JoinHandle<ConsumerOutcome>>,
    /// The group's `IggyConsumer`. It stops before the other readers.
    group_iconsumer: JoinHandle<ConsumerOutcome>,
    sampler: JoinHandle<SamplerOutcome>,
    producer_phase: watch::Sender<ProducerPhase>,
    consumer_phase: watch::Sender<ConsumerPhase>,
    group_iconsumer_phase: watch::Sender<ConsumerPhase>,
    readers: Vec<Arc<Reader>>,
    log: SharedLog,
    /// Reads the stored offsets that end the drain.
    observer: IggyClient,
    started: Instant,
}

impl Workload {
    async fn spawn(harness: &TestHarness, seed: u64, started: Instant) -> Self {
        let context = TaskContext {
            target: Target::new(),
            log: SharedLog::default(),
            started,
        };
        let history = TaskContext {
            target: Target::history(),
            ..context.clone()
        };
        let (producer_phase, producer_rx) = watch::channel(ProducerPhase::Send);
        let (consumer_phase, consumer_rx) = watch::channel(ConsumerPhase::Consume);
        let (group_iconsumer_phase, group_iconsumer_rx) = watch::channel(ConsumerPhase::Consume);
        let mut dialer = Dialer { harness, dialed: 0 };

        let mut producers = Vec::with_capacity((PRODUCERS + HISTORY_PRODUCERS) as usize);
        for producer in 0..PRODUCERS + HISTORY_PRODUCERS {
            let producer_context = if producer < PRODUCERS {
                &context
            } else {
                &history
            };
            producers.push(tokio::spawn(run_producer(
                dialer.connect().await,
                producer,
                producer_rx.clone(),
                producer_context.clone(),
            )));
        }

        let mut consumers = Vec::new();
        let group = Reader::new(
            "group".to_owned(),
            Consumer::group(context.target.group.clone()),
            None,
        );
        for member in 0..GROUP_MEMBERS {
            // Offset from the monkey's own stream of numbers.
            let rng = StdRng::seed_from_u64(seed.wrapping_add(u64::from(member) + 1));
            consumers.push(tokio::spawn(run_member(
                dialer.connect().await,
                member,
                rng,
                Arc::clone(&group),
                consumer_rx.clone(),
                context.clone(),
            )));
        }
        let client = dialer.connect().await;
        let builder = client
            .consumer_group(GROUP_NAME, STREAM_NAME, TOPIC_NAME)
            .expect("group IggyConsumer builder");
        let group_iconsumer = tokio::spawn(run_iggy_consumer(
            client,
            start_iggy_consumer(builder).await,
            "group iggy-consumer".to_owned(),
            Arc::clone(&group),
            group_iconsumer_rx,
            context.clone(),
        ));
        let mut readers = vec![group];

        for partition in 0..PARTITIONS {
            let id = FIRST_CONSUMER_ID + partition;
            let reader = Reader::new(
                format!("consumer {id}"),
                Consumer::new(Identifier::numeric(id).expect("valid consumer id")),
                Some(partition),
            );
            consumers.push(tokio::spawn(run_consumer(
                dialer.connect().await,
                Arc::clone(&reader),
                consumer_rx.clone(),
                context.clone(),
            )));
            readers.push(reader);
        }
        let client = dialer.connect().await;
        let builder = client
            .consumer(
                STANDALONE_ICONSUMER_NAME,
                STREAM_NAME,
                TOPIC_NAME,
                STANDALONE_ICONSUMER_PARTITION,
            )
            .expect("standalone IggyConsumer builder");
        let reader = Reader::new(
            format!("consumer {STANDALONE_ICONSUMER_NAME}"),
            Consumer::new(Identifier::named(STANDALONE_ICONSUMER_NAME).expect("valid name")),
            Some(STANDALONE_ICONSUMER_PARTITION),
        );
        consumers.push(tokio::spawn(run_iggy_consumer(
            client,
            start_iggy_consumer(builder).await,
            reader.name.clone(),
            Arc::clone(&reader),
            consumer_rx.clone(),
            context.clone(),
        )));
        readers.push(reader);

        let sampler = tokio::spawn(sample_group(
            dialer.connect().await,
            consumer_rx,
            context.clone(),
        ));

        Self {
            producers,
            consumers,
            group_iconsumer,
            sampler,
            producer_phase,
            consumer_phase,
            group_iconsumer_phase,
            readers,
            log: context.log,
            observer: dialer.connect().await,
            started,
        }
    }

    /// Stops the producers, waits until every reader received and committed
    /// everything, then stops the readers.
    async fn finish(self) -> Outcomes {
        let Self {
            producers,
            consumers,
            group_iconsumer,
            sampler,
            producer_phase,
            consumer_phase,
            group_iconsumer_phase,
            readers,
            log,
            observer,
            started,
        } = self;
        println!(
            "{} chaos phase over, stopping the producers",
            stamp(started)
        );
        producer_phase.send_replace(ProducerPhase::Finish);
        consumer_phase.send_replace(ConsumerPhase::Drain);

        let deadline = Instant::now() + RECOVERY_TIMEOUT;
        let mut producer_outcomes = Vec::with_capacity(producers.len());
        for mut handle in producers {
            let joined = match timeout_at(deadline, &mut handle).await {
                Ok(joined) => joined,
                Err(_) => {
                    producer_phase.send_replace(ProducerPhase::Abandon);
                    handle.await
                }
            };
            producer_outcomes.push(joined.expect("producer task panicked"));
        }
        // `spawn` starts the history topic's producers after the main topic's.
        let history_producers = producer_outcomes.split_off(PRODUCERS as usize);

        let mut acked = vec![HashSet::new(); PARTITIONS as usize];
        for producer in &producer_outcomes {
            acked[producer.partition as usize].extend(producer.acked_keys().map(u128::from));
        }
        println!("{} producers stopped, draining the readers", stamp(started));
        let drain_deadline = Instant::now() + DRAIN_TIMEOUT;
        let undrained = loop {
            match drain_progress(&observer, &readers, &log, &acked).await {
                Ok(()) => break None,
                Err(problem) if Instant::now() >= drain_deadline => break Some(problem),
                Err(_) => sleep(DRAIN_CHECK_INTERVAL).await,
            }
        };

        println!("{} drain over, readers stop", stamp(started));
        // The group's `IggyConsumer` stops before the members leave. Its
        // shutdown flush stores the last offset it consumed in every partition
        // it ever held, unless it stored that offset already. A leave can move
        // such a partition back to it, and the fence then admits the old
        // offset, so the group offset moves back.
        group_iconsumer_phase.send_replace(ConsumerPhase::Stop);
        let mut consumer_outcomes = Vec::with_capacity(consumers.len() + 1);
        consumer_outcomes.push(group_iconsumer.await.expect("consumer task panicked"));
        consumer_phase.send_replace(ConsumerPhase::Stop);
        for handle in consumers {
            consumer_outcomes.push(handle.await.expect("consumer task panicked"));
        }
        Outcomes {
            producers: producer_outcomes,
            history_producers,
            consumers: consumer_outcomes,
            sampler: sampler.await.expect("sampler task panicked"),
            readers,
            log: std::mem::take(&mut *log.lock().expect("observed log lock")),
            history_log: ObservedLog::default(),
            undrained,
        }
    }
}

/// Ok once every reader received every acknowledged message of its
/// partitions and stored the highest offset read there, otherwise what is
/// still missing. That offset comes from the readers, not from a node, so a
/// node that lags behind cannot end the drain early. This is also the only
/// check of the group's stored offsets, see `http_readers`.
async fn drain_progress(
    observer: &IggyClient,
    readers: &[Arc<Reader>],
    log: &SharedLog,
    acked: &[HashSet<u128>],
) -> Result<(), String> {
    if let Some(reader) = readers.iter().find(|reader| !reader.received_all(acked)) {
        return Err(format!("{} lacks acknowledged messages", reader.name));
    }
    let ends = log.lock().expect("observed log lock").ends();
    let target = Target::new();
    for reader in readers {
        for (partition, end) in (0u32..).zip(ends.iter().copied()) {
            let Some(end) = end.filter(|_| reader.reads(partition)) else {
                continue;
            };
            let stored = call(observer.get_consumer_offset(
                &reader.consumer,
                &target.stream,
                &target.topic,
                Some(partition),
            ))
            .await;
            match stored {
                Ok(Some(offset)) if offset.stored_offset == end => {}
                Ok(stored) => {
                    return Err(format!(
                        "partition {partition}: {} stored offset {:?}, the highest offset read \
                         there is {end}",
                        reader.name,
                        stored.map(|offset| offset.stored_offset)
                    ));
                }
                Err(_) => {
                    return Err(format!(
                        "partition {partition}: the stored offset of {} cannot be read",
                        reader.name
                    ));
                }
            }
        }
    }
    Ok(())
}

struct Outcomes {
    producers: Vec<ProducerOutcome>,
    history_producers: Vec<ProducerOutcome>,
    consumers: Vec<ConsumerOutcome>,
    sampler: SamplerOutcome,
    readers: Vec<Arc<Reader>>,
    log: ObservedLog,
    /// Only the final read fills it, because nothing reads the history topic
    /// before the end.
    history_log: ObservedLog,
    /// Why the drain timed out, if it did.
    undrained: Option<String>,
}

impl Outcomes {
    /// What the workload saw on its own, before any node is read.
    fn check(&self, violations: &mut Vec<String>) {
        for producer in self.producers.iter().chain(&self.history_producers) {
            if producer.sent_batches() > producer.acked_batches {
                violations.push(format!(
                    "producer {} got no ack for batch {} within {RECOVERY_TIMEOUT:?} after the \
                     chaos phase",
                    producer.producer, producer.acked_batches
                ));
            }
            // The server acks a batch without a confirmation when it cannot
            // resolve the batch's offsets. A producer that never got one
            // leaves the offset check with nothing to check.
            if producer.acked_batches > 0 && producer.confirmations.is_empty() {
                violations.push(format!(
                    "producer {} got no offset confirmation for any of its {} acked batches",
                    producer.producer, producer.acked_batches
                ));
            }
            producer
                .errors
                .check(&format!("producer {}", producer.producer), violations);
        }
        for consumer in &self.consumers {
            consumer.errors.check(&consumer.name, violations);
        }
        self.sampler.errors.check("the group sampler", violations);
        violations.extend(self.sampler.violations.iter().cloned());
        if let Some(problem) = &self.undrained {
            violations.push(format!(
                "the readers did not drain within {DRAIN_TIMEOUT:?}: {problem}"
            ));
        }
    }
}

/// A message id carries its producer and sequence number, so the checks can
/// account for every message without trusting the payload.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
struct MessageKey {
    producer: u32,
    seq: u64,
}

impl MessageKey {
    fn batch(self) -> u64 {
        self.seq / u64::from(PRODUCER_BATCH)
    }

    fn payload(self) -> Bytes {
        Bytes::from(format!("{self}"))
    }
}

impl fmt::Display for MessageKey {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "producer {} seq {}", self.producer, self.seq)
    }
}

/// The high half holds the producer plus one, because the SDK replaces a zero
/// id with a random one before it sends.
impl From<MessageKey> for u128 {
    fn from(key: MessageKey) -> Self {
        ((u128::from(key.producer) + 1) << 64) | u128::from(key.seq)
    }
}

impl TryFrom<u128> for MessageKey {
    type Error = u128;

    fn try_from(id: u128) -> Result<Self, Self::Error> {
        let producer = (id >> 64).checked_sub(1).ok_or(id)?;
        Ok(Self {
            producer: u32::try_from(producer).map_err(|_| id)?,
            // The low 64 bits are the sequence number.
            seq: id as u64,
        })
    }
}

fn batch_keys(producer: u32, batch: u64) -> impl Iterator<Item = MessageKey> {
    let first = batch * u64::from(PRODUCER_BATCH);
    (first..first + u64::from(PRODUCER_BATCH)).map(move |seq| MessageKey { producer, seq })
}

/// Why a client call returned no value.
enum Failure {
    Iggy(IggyError),
    TimedOut,
}

impl fmt::Display for Failure {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Iggy(error) => write!(f, "{error}"),
            Self::TimedOut => write!(f, "no answer within {CALL_TIMEOUT:?}"),
        }
    }
}

async fn call<T>(request: impl Future<Output = Result<T, IggyError>>) -> Result<T, Failure> {
    match timeout(CALL_TIMEOUT, request).await {
        Ok(result) => result.map_err(Failure::Iggy),
        Err(_) => Err(Failure::TimedOut),
    }
}

/// Client errors by kind. Failover produces many kinds of errors. The
/// forbidden ones mean the cluster lost committed state or rejected valid
/// credentials.
struct ErrorTally {
    started: Instant,
    counts: BTreeMap<&'static str, u64>,
    forbidden: Vec<String>,
}

impl ErrorTally {
    fn new(started: Instant) -> Self {
        Self {
            started,
            counts: BTreeMap::new(),
            forbidden: Vec::new(),
        }
    }

    fn record(&mut self, failure: &Failure) {
        let kind = match failure {
            Failure::Iggy(error) => error.as_string(),
            Failure::TimedOut => CALL_TIMED_OUT,
        };
        *self.counts.entry(kind).or_default() += 1;
        if let Failure::Iggy(error) = failure
            && is_forbidden(error)
        {
            self.forbidden
                .push(format!("{} {error}", stamp(self.started)));
        }
    }

    fn check(&self, name: &str, violations: &mut Vec<String>) {
        report(
            violations,
            &format!("errors at {name} that mean lost state or rejected credentials"),
            &self.forbidden,
        );
    }
}

fn is_forbidden(error: &IggyError) -> bool {
    matches!(
        error,
        IggyError::StreamIdNotFound(_)
            | IggyError::StreamNameNotFound(_)
            | IggyError::TopicIdNotFound(..)
            | IggyError::TopicNameNotFound(..)
            | IggyError::ConsumerGroupIdNotFound(..)
            | IggyError::ConsumerGroupNameNotFound(..)
            | IggyError::InvalidCredentials
            | IggyError::Unauthorized
    )
}

struct ProducerOutcome {
    producer: u32,
    partition: u32,
    /// Batches `0..acked_batches` were acknowledged, in order.
    acked_batches: u64,
    /// The send attempts of every batch that was sent, by batch. One entry
    /// more than `acked_batches` only when the producer gave up on its last
    /// batch. Every attempt can land, so a batch can appear in the log once
    /// per attempt.
    attempts: Vec<usize>,
    confirmations: Vec<Confirmation>,
    longest_wait: Duration,
    errors: ErrorTally,
}

impl ProducerOutcome {
    fn sent_batches(&self) -> u64 {
        self.attempts.len() as u64
    }

    fn acked_keys(&self) -> impl Iterator<Item = MessageKey> {
        (0..self.acked_batches).flat_map(|batch| batch_keys(self.producer, batch))
    }
}

struct Confirmation {
    batch: u64,
    partition: u32,
    base_offset: u64,
}

/// Sends fixed-size batches to one partition. A failed batch is sent again
/// with the same message ids until it is acknowledged, so a duplicate can
/// only come from a batch with a failed attempt.
async fn run_producer(
    client: IggyClient,
    producer: u32,
    phase: watch::Receiver<ProducerPhase>,
    context: TaskContext,
) -> ProducerOutcome {
    let target = context.target;
    let partition = producer % PARTITIONS;
    let partitioning = Partitioning::partition_id(partition);
    let mut outcome = ProducerOutcome {
        producer,
        partition,
        acked_batches: 0,
        attempts: Vec::new(),
        confirmations: Vec::new(),
        longest_wait: Duration::ZERO,
        errors: ErrorTally::new(context.started),
    };
    while *phase.borrow() == ProducerPhase::Send {
        let batch = outcome.sent_batches();
        let mut messages: Vec<IggyMessage> = batch_keys(producer, batch)
            .map(|key| {
                IggyMessage::builder()
                    .id(u128::from(key))
                    .payload(key.payload())
                    .build()
                    .expect("valid message")
            })
            .collect();
        let first_attempt = Instant::now();
        let mut attempts = 0;
        let acked =
            loop {
                attempts += 1;
                let sent = call(client.send_messages(
                    &target.stream,
                    &target.topic,
                    &partitioning,
                    &mut messages,
                ))
                .await;
                match sent {
                    Ok(response) => {
                        outcome
                            .confirmations
                            .extend(response.confirmations.iter().map(|confirmation| {
                                Confirmation {
                                    batch,
                                    partition: confirmation.partition_id,
                                    base_offset: confirmation.base_offset,
                                }
                            }));
                        break true;
                    }
                    Err(failure) => {
                        outcome.errors.record(&failure);
                        if *phase.borrow() == ProducerPhase::Abandon {
                            break false;
                        }
                        sleep(RETRY_PAUSE).await;
                    }
                }
            };
        outcome.attempts.push(attempts);
        if !acked {
            return outcome;
        }
        outcome.acked_batches += 1;
        outcome.longest_wait = outcome.longest_wait.max(first_attempt.elapsed());
        sleep(PRODUCER_PAUSE).await;
    }
    outcome
}

struct ConsumerOutcome {
    name: String,
    deliveries: usize,
    /// `None` for a task that never joins or leaves by itself: a standalone
    /// consumer, or the group's `IggyConsumer`, whose SDK does it.
    churn: Option<Churn>,
    errors: ErrorTally,
}

struct Churn {
    joins: u64,
    leaves: u64,
}

impl ConsumerOutcome {
    fn new(name: String, started: Instant) -> Self {
        Self {
            name,
            deliveries: 0,
            churn: None,
            errors: ErrorTally::new(started),
        }
    }

    /// Low-level consumers record a message before they commit it, and the
    /// `IggyConsumer` commits only what it handed over, so a lost commit can
    /// only cause a redelivery, never a gap.
    fn record<'a>(
        &mut self,
        reader: &Reader,
        partition: u32,
        messages: impl IntoIterator<Item = &'a IggyMessage>,
        log: &SharedLog,
    ) {
        let mut received = reader.received.lock().expect("received ids lock");
        let mut log = log.lock().expect("observed log lock");
        for message in messages {
            self.deliveries += 1;
            received.deliveries += 1;
            received.ids.insert(message.header.id);
            log.observe(&self.name, partition, LogEntry::from(message));
        }
    }
}

/// Polls the reader's next batch and stores its last offset.
async fn poll_and_commit(
    client: &IggyClient,
    reader: &Reader,
    outcome: &mut ConsumerOutcome,
    context: &TaskContext,
) -> Result<(), Failure> {
    let target = &context.target;
    let polled = call(client.poll_messages(
        &target.stream,
        &target.topic,
        reader.partition,
        &reader.consumer,
        &PollingStrategy::next(),
        POLL_BATCH,
        false,
    ))
    .await?;
    let Some(last) = polled.messages.last().map(|message| message.header.offset) else {
        sleep(POLL_PAUSE).await;
        return Ok(());
    };
    outcome.record(reader, polled.partition_id, &polled.messages, &context.log);
    let stored = call(client.store_consumer_offset(
        &reader.consumer,
        &target.stream,
        &target.topic,
        Some(polled.partition_id),
        last,
    ))
    .await;
    if let Err(failure) = stored {
        outcome.errors.record(&failure);
    }
    Ok(())
}

/// Joins the group, polls and commits for a random while, leaves, pauses and
/// joins again. In the drain phase it stays joined until told to stop.
async fn run_member(
    client: IggyClient,
    member: u32,
    mut rng: StdRng,
    group: Arc<Reader>,
    phase: watch::Receiver<ConsumerPhase>,
    context: TaskContext,
) -> ConsumerOutcome {
    let target = &context.target;
    let mut outcome = ConsumerOutcome::new(format!("member {member}"), context.started);
    let mut churn = Churn {
        joins: 0,
        leaves: 0,
    };
    while *phase.borrow() != ConsumerPhase::Stop {
        let joined =
            call(client.join_consumer_group(&target.stream, &target.topic, &target.group)).await;
        if let Err(failure) = joined {
            outcome.errors.record(&failure);
            sleep(RETRY_PAUSE).await;
            continue;
        }
        churn.joins += 1;
        let session_end =
            Instant::now() + Duration::from_millis(rng.random_range(MEMBER_SESSION_MS));
        loop {
            let current = *phase.borrow();
            if current == ConsumerPhase::Stop
                || (current == ConsumerPhase::Consume && Instant::now() >= session_end)
            {
                break;
            }
            if let Err(failure) = poll_and_commit(&client, &group, &mut outcome, &context).await {
                let lost_membership = matches!(
                    failure,
                    Failure::Iggy(IggyError::ConsumerGroupMemberNotFound(..))
                );
                outcome.errors.record(&failure);
                if lost_membership {
                    break;
                }
                sleep(RETRY_PAUSE).await;
            }
        }
        match call(client.leave_consumer_group(&target.stream, &target.topic, &target.group)).await
        {
            Ok(()) => churn.leaves += 1,
            Err(failure) => outcome.errors.record(&failure),
        }
        if *phase.borrow() == ConsumerPhase::Consume {
            sleep(Duration::from_millis(rng.random_range(MEMBER_PAUSE_MS))).await;
        }
    }
    outcome.churn = Some(churn);
    outcome
}

/// Reads one partition from the consumer's own stored offset until told to
/// stop.
async fn run_consumer(
    client: IggyClient,
    reader: Arc<Reader>,
    phase: watch::Receiver<ConsumerPhase>,
    context: TaskContext,
) -> ConsumerOutcome {
    let mut outcome = ConsumerOutcome::new(reader.name.clone(), context.started);
    while *phase.borrow() != ConsumerPhase::Stop {
        if let Err(failure) = poll_and_commit(&client, &reader, &mut outcome, &context).await {
            outcome.errors.record(&failure);
            sleep(RETRY_PAUSE).await;
        }
    }
    outcome
}

async fn start_iggy_consumer(builder: IggyConsumerBuilder) -> IggyConsumer {
    let commit_interval =
        NonZeroIggyDuration::new(ICONSUMER_COMMIT_INTERVAL).expect("non-zero commit interval");
    let mut consumer = builder
        // Commits only what was handed over. The default commits with the
        // poll and can skip a batch whose reply was lost, which would break
        // the no-loss check.
        .auto_commit(AutoCommit::IntervalOrWhen(
            commit_interval,
            AutoCommitWhen::ConsumingAllMessages,
        ))
        .batch_length(POLL_BATCH)
        .poll_interval(IggyDuration::from(POLL_PAUSE))
        .build();
    consumer.init().await.expect("init the IggyConsumer");
    consumer
}

/// The group's `IggyConsumer` joins once and relies on the SDK to rejoin
/// after every reconnect. The standalone one reads its partition.
async fn run_iggy_consumer(
    // Dropping the client aborts its heartbeat, and the session then expires.
    _client: IggyClient,
    mut consumer: IggyConsumer,
    name: String,
    reader: Arc<Reader>,
    mut phase: watch::Receiver<ConsumerPhase>,
    context: TaskContext,
) -> ConsumerOutcome {
    let mut outcome = ConsumerOutcome::new(name, context.started);
    loop {
        tokio::select! {
            changed = phase.changed() => {
                if changed.is_err() || *phase.borrow() == ConsumerPhase::Stop {
                    break;
                }
            }
            next = consumer.next() => match next {
                Some(Ok(received)) => {
                    outcome.record(
                        &reader,
                        received.partition_id,
                        [&received.message],
                        &context.log,
                    );
                }
                Some(Err(error)) => outcome.errors.record(&Failure::Iggy(error)),
                None => break,
            },
        }
    }
    if let Err(error) = consumer.shutdown().await {
        outcome.errors.record(&Failure::Iggy(error));
    }
    outcome
}

struct SamplerOutcome {
    samples: u64,
    errors: ErrorTally,
    violations: Vec<String>,
}

/// Reads the group once a second. A committed assignment never gives one
/// partition to two members, even mid-handoff: a partition moves from its
/// source to its target in one step.
async fn sample_group(
    client: IggyClient,
    phase: watch::Receiver<ConsumerPhase>,
    context: TaskContext,
) -> SamplerOutcome {
    let TaskContext {
        target, started, ..
    } = context;
    let mut outcome = SamplerOutcome {
        samples: 0,
        errors: ErrorTally::new(started),
        violations: Vec::new(),
    };
    while *phase.borrow() != ConsumerPhase::Stop {
        match call(client.get_consumer_group(&target.stream, &target.topic, &target.group)).await {
            Ok(Some(group)) => {
                outcome.samples += 1;
                if let Err(problem) = check_assignment(&group) {
                    outcome
                        .violations
                        .push(format!("{} {problem}", stamp(started)));
                }
            }
            Ok(None) => outcome.violations.push(format!(
                "{} consumer group {GROUP_NAME} is gone",
                stamp(started)
            )),
            Err(failure) => outcome.errors.record(&failure),
        }
        sleep(GROUP_SAMPLE_INTERVAL).await;
    }
    outcome
}

fn check_assignment(group: &ConsumerGroupDetails) -> Result<(), String> {
    let mut owners = HashMap::new();
    for member in &group.members {
        for &partition in &member.partitions {
            if partition >= PARTITIONS {
                return Err(format!(
                    "member {} holds partition {partition} of a {PARTITIONS}-partition topic",
                    member.id
                ));
            }
            if let Some(other) = owners.insert(partition, member.id) {
                return Err(format!(
                    "partition {partition} is held by members {other} and {}",
                    member.id
                ));
            }
        }
    }
    Ok(())
}

#[derive(Default)]
struct KillStats {
    kills: u32,
    /// Kills of a node that named itself leader right before it died.
    leader_kills: u32,
}

/// Kills a random node every kill interval and restarts it after the
/// downtime. Prints each kill and restart as it happens, so a hung run shows
/// where it stopped.
async fn run_monkey(
    harness: &mut TestHarness,
    config: &ChaosConfig,
    started: Instant,
    violations: &mut Vec<String>,
    report: &mut Report,
) -> KillStats {
    let mut rng = StdRng::seed_from_u64(config.seed);
    let chaos_end = started + config.duration;
    let mut stats = KillStats::default();
    let mut next_kill = started + config.kill_interval;
    while next_kill + config.downtime <= chaos_end {
        if !watch_nodes(harness, next_kill, None, started, violations, report).await {
            return stats;
        }
        let victim = rng.random_range(0..CLUSTER_NODES);
        let leader = leader_seen_by(harness, victim).await;
        let pid = harness.node(victim).pid();
        report
            .record(
                "kill_requested",
                json!({
                    "node": victim, "pid": pid,
                    "scheduled_elapsed_ms": next_kill.duration_since(started).as_millis(),
                    "chaos_elapsed_ms": started.elapsed().as_millis(),
                    "leader": leader.as_ref().ok(), "leader_error": leader.as_ref().err(),
                }),
            )
            .expect("write kill request");
        harness.kill_node(victim).expect("SIGKILL a node");
        report
            .record("killed", json!({"node": victim, "pid": pid}))
            .expect("write kill result");
        stats.kills += 1;
        if leader == Ok(victim) {
            stats.leader_kills += 1;
        }
        println!(
            "{} kill node {victim} ({})",
            stamp(started),
            describe_leader(victim, &leader)
        );
        let back_at = Instant::now() + config.downtime;
        if !watch_nodes(harness, back_at, Some(victim), started, violations, report).await {
            return stats;
        }
        // A panic before the kill shows only in this incarnation's stderr,
        // which the restart truncates.
        if let Some(stderr) = panic_report(harness, victim) {
            violations.push(format!(
                "{} node {victim} panicked before it was killed:\n{stderr}",
                stamp(started)
            ));
        }
        let restarting = Instant::now();
        if !restart(harness, victim, started, violations, report) {
            return stats;
        }
        println!(
            "{} restart node {victim} (up after {:.1}s)",
            stamp(started),
            restarting.elapsed().as_secs_f64()
        );
        next_kill += config.kill_interval;
    }
    watch_nodes(harness, chaos_end, None, started, violations, report).await;
    stats
}

/// Sleeps until `until`, checking every node except `down` once a second. A
/// node that died on its own is a violation. It is restarted, so the run
/// keeps its fault budget. Returns false when a node cannot be restarted.
async fn watch_nodes(
    harness: &mut TestHarness,
    until: Instant,
    down: Option<usize>,
    started: Instant,
    violations: &mut Vec<String>,
    report: &mut Report,
) -> bool {
    loop {
        for node in (0..CLUSTER_NODES).filter(|&node| Some(node) != down) {
            if harness.node(node).is_running() {
                continue;
            }
            let (_, stderr) = harness.node(node).collect_logs();
            report
                .record(
                    "unexpected_exit",
                    json!({
                        "node": node,
                        "pid": harness.node(node).pid(),
                        "stderr_tail": tail(&stderr),
                    }),
                )
                .expect("write unexpected exit");
            violations.push(format!(
                "{} node {node} exited without being killed:\n{}",
                stamp(started),
                tail(&stderr)
            ));
            if !restart(harness, node, started, violations, report) {
                return false;
            }
        }
        let now = Instant::now();
        if now >= until {
            return true;
        }
        sleep((until - now).min(HEALTH_CHECK_INTERVAL)).await;
    }
}

fn restart(
    harness: &mut TestHarness,
    node: usize,
    started: Instant,
    violations: &mut Vec<String>,
    report: &mut Report,
) -> bool {
    report
        .record("restart_requested", json!({"node": node}))
        .expect("write restart request");
    match harness.restart_node(node) {
        Ok(()) => {
            report
                .record(
                    "restarted",
                    json!({"node": node, "pid": harness.node(node).pid()}),
                )
                .expect("write restart result");
            true
        }
        Err(error) => {
            report
                .record(
                    "restart_failed",
                    json!({"node": node, "error": error.to_string()}),
                )
                .expect("write restart failure");
            violations.push(format!(
                "{} node {node} failed to restart: {error}",
                stamp(started)
            ));
            false
        }
    }
}

/// The leader as `node` sees it, asked over a fresh HTTP client. A long-lived
/// TCP client does not work here: the SDK never reconnects for a metadata
/// call, so the client stays disconnected once its node dies.
async fn leader_seen_by(harness: &TestHarness, node: usize) -> Result<usize, String> {
    let lookup = async {
        let client = connect_http(harness, node)
            .await
            .map_err(|error| error.to_string())?;
        client
            .get_cluster_metadata()
            .await
            .map_err(|error| error.to_string())
    };
    let metadata = timeout(LEADER_LOOKUP_TIMEOUT, lookup)
        .await
        .map_err(|_| "lookup timed out".to_owned())??;
    let leader = metadata
        .nodes
        .iter()
        .find(|node| node.role == ClusterNodeRole::Leader)
        .ok_or_else(|| "no node is marked leader".to_owned())?;
    (0..CLUSTER_NODES)
        .find(|&index| {
            harness
                .node(index)
                .tcp_addr()
                .is_some_and(|address| address.port() == leader.endpoints.tcp)
        })
        .ok_or_else(|| format!("leader port {} belongs to no node", leader.endpoints.tcp))
}

fn describe_leader(victim: usize, leader: &Result<usize, String>) -> String {
    match leader {
        Ok(leader) if *leader == victim => "the leader".to_owned(),
        Ok(leader) => format!("leader is node {leader}"),
        Err(reason) => format!("leader unknown: {reason}"),
    }
}

/// One HTTP client per node that answers, with the node's index. HTTP reads
/// default to `serializable`, which a node serves from its own state instead
/// of redirecting to the leader. A node without a client is a violation, and
/// the checks go on with the other nodes, so the report still prints.
async fn connect_verifiers(
    harness: &TestHarness,
    violations: &mut Vec<String>,
) -> Vec<(usize, IggyClient)> {
    let mut verifiers = Vec::with_capacity(CLUSTER_NODES);
    for node in 0..CLUSTER_NODES {
        match timeout(CALL_TIMEOUT, connect_http(harness, node)).await {
            Ok(Ok(client)) => verifiers.push((node, client)),
            Ok(Err(error)) => violations.push(format!("node {node}: no HTTP verifier: {error}")),
            Err(_) => violations.push(format!(
                "node {node}: no HTTP verifier within {CALL_TIMEOUT:?}"
            )),
        }
    }
    verifiers
}

async fn connect_http(harness: &TestHarness, node: usize) -> Result<IggyClient, TestBinaryError> {
    harness
        .node(node)
        .http_client()?
        .with_root_login()
        .connect()
        .await
}

/// A node's own answer about the topic, the stored offsets and the group.
#[derive(Debug, PartialEq, Eq, Serialize)]
struct NodeView {
    /// `None` when the node does not know the topic.
    partitions: Option<Vec<PartitionView>>,
    /// The history topic's partitions, `None` when the node does not know it.
    history: Option<Vec<PartitionView>>,
    /// `(member id, partitions)`, or `None` when the node does not know the
    /// group.
    members: Option<Vec<(u32, Vec<u32>)>>,
}

#[derive(Debug, PartialEq, Eq, Serialize)]
struct PartitionView {
    current_offset: u64,
    messages_count: u64,
    segments_count: u32,
    /// What each of the partition's `http_readers` stored, in reader order.
    stored_offsets: Vec<Option<u64>>,
}

/// The readers of `partition` whose stored offset a node reports over HTTP.
/// The group is not one of them: HTTP drops `Consumer::kind`, so the read
/// would name a standalone consumer called like the group. The drain reads
/// the group's offsets over TCP instead.
fn http_readers(readers: &[Arc<Reader>], partition: u32) -> impl Iterator<Item = &Arc<Reader>> {
    readers.iter().filter(move |reader| {
        reader.consumer.kind == ConsumerKind::Consumer && reader.reads(partition)
    })
}

async fn node_view(client: &IggyClient, readers: &[Arc<Reader>]) -> Result<NodeView, Failure> {
    let target = Target::new();
    let members = call(client.get_consumer_group(&target.stream, &target.topic, &target.group))
        .await?
        .map(|group| {
            let mut members: Vec<(u32, Vec<u32>)> = group
                .members
                .into_iter()
                .map(|member| {
                    let mut partitions = member.partitions;
                    partitions.sort_unstable();
                    (member.id, partitions)
                })
                .collect();
            members.sort_unstable();
            members
        });
    Ok(NodeView {
        partitions: partition_views(client, &target, readers).await?,
        history: partition_views(client, &Target::history(), &[]).await?,
        members,
    })
}

/// A node's own answer about one topic and the stored offsets of the readers
/// of each partition.
async fn partition_views(
    client: &IggyClient,
    target: &Target,
    readers: &[Arc<Reader>],
) -> Result<Option<Vec<PartitionView>>, Failure> {
    let Some(topic) = call(client.get_topic(&target.stream, &target.topic)).await? else {
        return Ok(None);
    };
    let mut partitions = Vec::with_capacity(topic.partitions.len());
    for partition in topic.partitions {
        let mut stored_offsets = Vec::new();
        for reader in http_readers(readers, partition.id) {
            let stored = call(client.get_consumer_offset(
                &reader.consumer,
                &target.stream,
                &target.topic,
                Some(partition.id),
            ))
            .await?;
            stored_offsets.push(stored.map(|offset| offset.stored_offset));
        }
        partitions.push(PartitionView {
            current_offset: partition.current_offset,
            messages_count: partition.messages_count,
            segments_count: partition.segments_count,
            stored_offsets,
        });
    }
    Ok(Some(partitions))
}

/// Waits until the nodes reach the end state that `end_state_problems`
/// describes. Returns the first node's view for the log checks.
async fn wait_for_convergence(
    verifiers: &[(usize, IggyClient)],
    readers: &[Arc<Reader>],
    started: Instant,
    violations: &mut Vec<String>,
    report: &mut Report,
) -> Option<NodeView> {
    let deadline = Instant::now() + CONVERGENCE_TIMEOUT;
    loop {
        let mut views = Vec::with_capacity(verifiers.len());
        for (node, verifier) in verifiers {
            views.push((*node, node_view(verifier, readers).await));
        }
        let mut problems = end_state_problems(&views, readers);
        if problems.is_empty() {
            if verifiers.len() == CLUSTER_NODES {
                println!("{} nodes converged", stamp(started));
            }
        } else if Instant::now() >= deadline {
            violations.push(format!(
                "nodes did not reach the end state within {CONVERGENCE_TIMEOUT:?}:\n{}",
                problems.join("\n")
            ));
        } else {
            sleep(CONVERGENCE_RETRY).await;
            continue;
        }
        if verifiers.len() != CLUSTER_NODES {
            problems.push(format!(
                "only {} of {CLUSTER_NODES} nodes have HTTP verifiers",
                verifiers.len()
            ));
        }
        for (node, view) in &views {
            report
                .record(
                    "node_view",
                    json!({
                        "node": node,
                        "view": view.as_ref().ok(),
                        "error": view.as_ref().err().map(ToString::to_string),
                    }),
                )
                .expect("write node view");
        }
        report
            .record(
                "check",
                json!({
                    "name": "convergence",
                    "outcome": if problems.is_empty() { "passed" } else { "failed" },
                    "nodes": verifiers.len(),
                    "expected_nodes": CLUSTER_NODES,
                    "violations": problems,
                }),
            )
            .expect("write convergence result");
        return views.into_iter().find_map(|(_, view)| view.ok());
    }
}

/// What keeps the nodes from the end state. Every node answers the same, and
/// both topics exist. The group is empty: every member left, and the members
/// stranded on killed nodes expired. Every standalone reader stored the end
/// of its partitions. Each partition of the main topic keeps only its active
/// segment, because every sealed one expired.
fn end_state_problems(
    views: &[(usize, Result<NodeView, Failure>)],
    readers: &[Arc<Reader>],
) -> Vec<String> {
    let mut problems = Vec::new();
    let mut reference: Option<(usize, &NodeView)> = None;
    for (node, view) in views {
        match (view, reference) {
            (Err(failure), _) => problems.push(format!("node {node} cannot be read: {failure}")),
            (Ok(view), None) => reference = Some((*node, view)),
            (Ok(view), Some((first, expected))) => {
                if view != expected {
                    problems.push(format!("node {node}: {view:?}, node {first}: {expected:?}"));
                }
            }
        }
    }
    let Some((_, view)) = reference else {
        return problems;
    };
    if view.history.is_none() {
        problems.push(format!("topic {HISTORY_TOPIC_NAME} is gone"));
    }
    match &view.members {
        None => problems.push(format!("consumer group {GROUP_NAME} is gone")),
        Some(members) if !members.is_empty() => {
            problems.push(format!("the group still has members {members:?}"));
        }
        Some(_) => {}
    }
    let Some(partitions) = &view.partitions else {
        problems.push(format!("topic {TOPIC_NAME} is gone"));
        return problems;
    };
    for (partition, view) in (0u32..).zip(partitions) {
        for (reader, stored) in http_readers(readers, partition).zip(&view.stored_offsets) {
            if *stored != Some(view.current_offset) {
                problems.push(format!(
                    "partition {partition}: {} stored offset {stored:?}, the partition ends at {}",
                    reader.name, view.current_offset
                ));
            }
        }
        if view.segments_count != 1 {
            problems.push(format!(
                "partition {partition} keeps {} segments, but expiry leaves only the active one",
                view.segments_count
            ));
        }
    }
    problems
}

#[derive(Debug, PartialEq, Eq)]
struct LogEntry {
    offset: u64,
    id: u128,
    payload: Bytes,
}

impl From<&IggyMessage> for LogEntry {
    fn from(message: &IggyMessage) -> Self {
        Self {
            offset: message.header.offset,
            id: message.header.id,
            payload: message.payload.clone(),
        }
    }
}

/// Reads what a node retains of each partition. The read from offset 0
/// starts at the oldest message that expiry left.
async fn read_log(client: &IggyClient, target: &Target) -> Result<Vec<Vec<LogEntry>>, Failure> {
    let consumer = Consumer::new(
        Identifier::numeric(VERIFIER_CONSUMER_ID).expect("valid verifier consumer id"),
    );
    let mut log = Vec::with_capacity(PARTITIONS as usize);
    for partition in 0..PARTITIONS {
        let mut entries: Vec<LogEntry> = Vec::new();
        loop {
            let next = entries.last().map_or(0, |entry| entry.offset + 1);
            let polled = call(client.poll_messages(
                &target.stream,
                &target.topic,
                Some(partition),
                &consumer,
                &PollingStrategy::offset(next),
                VERIFY_BATCH,
                false,
            ))
            .await?;
            // A reply that does not move forward ends the read. The offset
            // check reports what it returned.
            let progressed = polled
                .messages
                .last()
                .is_some_and(|message| message.header.offset >= next);
            entries.extend(polled.messages.iter().map(LogEntry::from));
            if !progressed {
                break;
            }
        }
        log.push(entries);
    }
    Ok(log)
}

/// Reads both topics from every node and requires the nodes to agree. What
/// they retain joins each topic's observed log, which is then checked against
/// what the producers sent and what every reader received.
async fn verify_logs(
    verifiers: &[(usize, IggyClient)],
    view: Option<&NodeView>,
    outcomes: &mut Outcomes,
    violations: &mut Vec<String>,
    report: &mut Report,
) -> [LogStats; 2] {
    let partitions = view.and_then(|view| view.partitions.as_deref());
    let history = view.and_then(|view| view.history.as_deref());
    read_retained(
        verifiers,
        &Target::new(),
        partitions,
        &mut outcomes.log,
        violations,
        report,
    )
    .await;
    read_retained(
        verifiers,
        &Target::history(),
        history,
        &mut outcomes.history_log,
        violations,
        report,
    )
    .await;
    if let Some(partitions) = partitions
        && partitions
            .iter()
            .all(|partition| partition.messages_count == partition.current_offset + 1)
    {
        violations.push(format!(
            "expiry deleted no segment of {TOPIC_NAME}, so the run did not test expiry"
        ));
    }
    [
        check_log(
            TOPIC_NAME,
            &outcomes.log,
            &outcomes.producers,
            &outcomes.readers,
            partitions,
            violations,
        ),
        check_log(
            HISTORY_TOPIC_NAME,
            &outcomes.history_log,
            &outcomes.history_producers,
            &[],
            history,
            violations,
        ),
    ]
}

/// Reads what every node retains of one topic and requires the nodes to
/// agree. The first copy that a node returned joins the topic's observed log.
async fn read_retained(
    verifiers: &[(usize, IggyClient)],
    target: &Target,
    partitions: Option<&[PartitionView]>,
    log: &mut ObservedLog,
    violations: &mut Vec<String>,
    report: &mut Report,
) {
    let topic = target.name;
    let mut reference = None;
    for (node, verifier) in verifiers {
        let retained = match read_log(verifier, target).await {
            Ok(retained) => retained,
            Err(failure) => {
                violations.push(format!("node {node}: cannot read {topic}: {failure}"));
                continue;
            }
        };
        for (partition, entries) in retained.iter().enumerate() {
            report
                .record(
                    "retained_range",
                    json!({
                        "node": node, "topic": topic, "partition": partition,
                        "messages_count": entries.len(),
                        "first_offset": entries.first().map(|entry| entry.offset),
                        "last_offset": entries.last().map(|entry| entry.offset),
                    }),
                )
                .expect("write retained range");
        }
        match &reference {
            None => reference = Some(retained),
            Some(reference) => compare_logs(topic, *node, reference, &retained, violations),
        }
    }
    let Some(retained) = reference else {
        return;
    };
    if let Some(partitions) = partitions {
        check_retained(topic, &retained, partitions, violations);
    }
    for (partition, entries) in (0u32..).zip(retained) {
        for entry in entries {
            log.observe("the final read", partition, entry);
        }
    }
}

fn compare_logs(
    topic: &str,
    node: usize,
    reference: &[Vec<LogEntry>],
    log: &[Vec<LogEntry>],
    violations: &mut Vec<String>,
) {
    for (partition, (expected, actual)) in reference.iter().zip(log).enumerate() {
        let longest = expected.len().max(actual.len());
        if let Some(position) = (0..longest).find(|&i| expected.get(i) != actual.get(i)) {
            violations.push(format!(
                "{topic} partition {partition}: node {node} holds {} messages, the first node \
                 read holds {}; first difference at position {position}: {:?} vs {:?}",
                actual.len(),
                expected.len(),
                actual.get(position),
                expected.get(position)
            ));
        }
    }
}

/// After expiry a node keeps the messages from its oldest retained one
/// through the end of the partition, without a gap, as many as it reports.
fn check_retained(
    topic: &str,
    log: &[Vec<LogEntry>],
    partitions: &[PartitionView],
    violations: &mut Vec<String>,
) {
    for (partition, (entries, view)) in (0u32..).zip(log.iter().zip(partitions)) {
        let first = (view.current_offset + 1).saturating_sub(view.messages_count);
        if !entries
            .iter()
            .map(|entry| entry.offset)
            .eq(first..=view.current_offset)
        {
            violations.push(format!(
                "{topic} partition {partition}: the nodes retain {} messages from offset {:?}, \
                 but report {} messages up to offset {}",
                entries.len(),
                entries.first().map(|entry| entry.offset),
                view.messages_count,
                view.current_offset
            ));
        }
    }
}

struct LogStats {
    topic: &'static str,
    messages: usize,
    duplicates: usize,
}

fn check_log(
    topic: &'static str,
    log: &ObservedLog,
    producers: &[ProducerOutcome],
    readers: &[Arc<Reader>],
    partitions: Option<&[PartitionView]>,
    violations: &mut Vec<String>,
) -> LogStats {
    report(
        violations,
        &format!("{topic}: reads contradict the topic or an earlier read of the same offset"),
        &log.conflicts,
    );

    let mut occurrences: HashMap<u128, usize> = HashMap::new();
    let mut last_first_seq: HashMap<u32, u64> = HashMap::new();
    let mut unread = Vec::new();
    let mut broken = Vec::new();
    for (partition, entries) in (0u32..).zip(&log.partitions) {
        let end = partitions
            .and_then(|views| views.get(partition as usize))
            .map(|view| view.current_offset);
        let mut next = 0;
        for (&offset, entry) in entries {
            if offset > next {
                unread.push(format!(
                    "partition {partition}: offsets {next}..={}",
                    offset - 1
                ));
            }
            next = offset + 1;
            if end.is_some_and(|end| offset > end) {
                broken.push(format!(
                    "partition {partition}: offset {offset} lies past the end of the partition"
                ));
            }
            let occurrence = occurrences.entry(entry.id).or_default();
            *occurrence += 1;
            if let Err(problem) = check_entry(
                entry,
                partition,
                *occurrence,
                producers,
                &mut last_first_seq,
            ) {
                broken.push(problem);
            }
        }
        if let Some(end) = end
            && next <= end
        {
            unread.push(format!("partition {partition}: offsets {next}..={end}"));
        }
    }
    report(
        violations,
        &format!("{topic}: offset ranges that no reader and no node returned"),
        &unread,
    );
    report(
        violations,
        &format!("{topic}: log entries break the producer contract"),
        &broken,
    );

    // A batch lands whole, so every message of a batch has as many copies as
    // the others.
    let mut partial = Vec::new();
    for producer in producers {
        for batch in 0..producer.sent_batches() {
            let copies: Vec<usize> = batch_keys(producer.producer, batch)
                .map(|key| {
                    occurrences
                        .get(&u128::from(key))
                        .copied()
                        .unwrap_or_default()
                })
                .collect();
            if copies.iter().min() != copies.iter().max() {
                partial.push(format!(
                    "producer {} batch {batch}: copies per message {copies:?}",
                    producer.producer
                ));
            }
        }
    }
    report(
        violations,
        &format!("{topic}: batches landed only in part"),
        &partial,
    );

    let missing: Vec<String> = producers
        .iter()
        .flat_map(ProducerOutcome::acked_keys)
        .filter(|key| !occurrences.contains_key(&u128::from(*key)))
        .map(|key| key.to_string())
        .collect();
    report(
        violations,
        &format!("{topic}: acknowledged messages are missing"),
        &missing,
    );

    let misplaced: Vec<String> = producers
        .iter()
        .flat_map(|producer| {
            producer
                .confirmations
                .iter()
                .filter(|confirmation| !confirmation_matches(log, producer.producer, confirmation))
                .map(|confirmation| {
                    format!(
                        "producer {} batch {} acked at partition {} offset {}",
                        producer.producer,
                        confirmation.batch,
                        confirmation.partition,
                        confirmation.base_offset
                    )
                })
        })
        .collect();
    report(
        violations,
        &format!("{topic}: acknowledgements point at the wrong offsets"),
        &misplaced,
    );

    for reader in readers {
        let received = reader.received.lock().expect("received ids lock");
        let undelivered: Vec<String> = producers
            .iter()
            .filter(|producer| reader.reads(producer.partition))
            .flat_map(ProducerOutcome::acked_keys)
            .filter(|key| !received.ids.contains(&u128::from(*key)))
            .map(|key| key.to_string())
            .collect();
        report(
            violations,
            &format!(
                "{topic}: acknowledged messages never reached {}",
                reader.name
            ),
            &undelivered,
        );
    }

    let messages: usize = occurrences.values().sum();
    LogStats {
        topic,
        messages,
        duplicates: messages - occurrences.len(),
    }
}

/// One entry against its producer's record. It must come from a batch that
/// was sent, sit in its producer's partition and carry the payload its id
/// implies. It may appear again only if its batch had a failed attempt, at
/// most once per attempt, and first occurrences must follow the producer's
/// sequence order.
fn check_entry(
    entry: &LogEntry,
    partition: u32,
    occurrence: usize,
    producers: &[ProducerOutcome],
    last_first_seq: &mut HashMap<u32, u64>,
) -> Result<(), String> {
    let key = MessageKey::try_from(entry.id)
        .map_err(|id| format!("partition {partition}: unknown message id {id:#x}"))?;
    let producer = producers
        .iter()
        .find(|producer| producer.producer == key.producer)
        .ok_or_else(|| format!("partition {partition}: {key} has no producer"))?;
    let Some(&attempts) = producer.attempts.get(key.batch() as usize) else {
        return Err(format!("partition {partition}: {key} was never sent"));
    };
    if partition != producer.partition {
        return Err(format!(
            "{key} sits in partition {partition}, its producer sends to {}",
            producer.partition
        ));
    }
    if entry.payload != key.payload() {
        return Err(format!("partition {partition}: {key} has a wrong payload"));
    }
    if occurrence > attempts {
        return Err(format!(
            "partition {partition}: {key} appears {occurrence} times, but its batch had \
             {attempts} send attempt(s)"
        ));
    }
    if occurrence > 1 {
        return Ok(());
    }
    match last_first_seq.insert(key.producer, key.seq) {
        Some(previous) if key.seq < previous => Err(format!(
            "partition {partition}: {key} comes after seq {previous} of the same producer"
        )),
        _ => Ok(()),
    }
}

/// An acknowledgement names where its batch landed, so the batch's messages
/// must sit at consecutive offsets from there.
fn confirmation_matches(log: &ObservedLog, producer: u32, confirmation: &Confirmation) -> bool {
    (confirmation.base_offset..)
        .zip(batch_keys(producer, confirmation.batch))
        .all(|(offset, key)| entry_id(log, confirmation.partition, offset) == Some(u128::from(key)))
}

fn entry_id(log: &ObservedLog, partition: u32, offset: u64) -> Option<u128> {
    log.partitions
        .get(partition as usize)?
        .get(&offset)
        .map(|entry| entry.id)
}

/// One violation per kind of problem, with the first few examples, so a
/// systematic failure does not bury the rest.
fn report(violations: &mut Vec<String>, what: &str, problems: &[String]) {
    if problems.is_empty() {
        return;
    }
    let examples: Vec<&str> = problems
        .iter()
        .take(REPORTED_EXAMPLES)
        .map(String::as_str)
        .collect();
    violations.push(format!(
        "{} {what}, first: {}",
        problems.len(),
        examples.join("; ")
    ));
}

fn panic_report(harness: &TestHarness, node: usize) -> Option<String> {
    harness
        .node(node)
        .stderr_panic_report()
        .map(|stderr| tail(&stderr))
}

fn tail(text: &str) -> String {
    let lines: Vec<&str> = text.lines().collect();
    lines[lines.len().saturating_sub(STDERR_TAIL_LINES)..].join("\n")
}

fn stamp(started: Instant) -> String {
    format!("[{:>6.1}s]", started.elapsed().as_secs_f64())
}

fn record_summary(
    report: &mut Report,
    kills: &KillStats,
    outcomes: &Outcomes,
    view: Option<&NodeView>,
    stats: &[LogStats],
) {
    let producers = [
        (TOPIC_NAME, &outcomes.producers),
        (HISTORY_TOPIC_NAME, &outcomes.history_producers),
    ];
    for (topic, producers) in producers {
        for producer in producers {
            report
                .record(
                    "producer",
                    json!({
                        "topic": topic,
                        "producer": producer.producer,
                        "partition": producer.partition,
                        "acked_batches": producer.acked_batches,
                        "retried_batches": producer
                            .attempts
                            .iter()
                            .filter(|&&batch_attempts| batch_attempts > 1)
                            .count(),
                        "unconfirmed_batches": producer
                            .acked_batches
                            .saturating_sub(producer.confirmations.len() as u64),
                        "longest_wait_secs": producer.longest_wait.as_secs_f64(),
                        "errors": producer.errors.counts,
                    }),
                )
                .expect("write producer outcome");
        }
    }
    for consumer in &outcomes.consumers {
        report
            .record(
                "consumer",
                json!({
                    "name": consumer.name,
                    "deliveries": consumer.deliveries,
                    "joins": consumer.churn.as_ref().map(|churn| churn.joins),
                    "leaves": consumer.churn.as_ref().map(|churn| churn.leaves),
                    "errors": consumer.errors.counts,
                }),
            )
            .expect("write consumer outcome");
    }
    for reader in &outcomes.readers {
        let received = reader.received.lock().expect("received ids lock");
        report
            .record(
                "reader",
                json!({
                    "name": reader.name,
                    "received": received.ids.len(),
                    "redelivered": received.deliveries - received.ids.len(),
                }),
            )
            .expect("write reader outcome");
    }
    for stats in stats {
        report
            .record(
                "log",
                json!({
                    "topic": stats.topic,
                    "messages": stats.messages,
                    "duplicates": stats.duplicates,
                }),
            )
            .expect("write log outcome");
    }
    report
        .record(
            "summary",
            json!({
                "kills": kills.kills, "leader_kills": kills.leader_kills,
                "group_samples": outcomes.sampler.samples,
                "group_sample_errors": outcomes.sampler.errors.counts,
                "view": view,
            }),
        )
        .expect("write run summary");
}

fn run_metadata(config: &ChaosConfig) -> serde_json::Value {
    let mut system = System::new();
    system.refresh_all();
    let recorded_unix_ms = SystemTime::now()
        .duration_since(SystemTime::UNIX_EPOCH)
        .expect("system clock after epoch")
        .as_millis();
    json!({
        "recorded_unix_ms": recorded_unix_ms,
        "seed": config.seed,
        "duration_secs": config.duration.as_secs(),
        "kill_interval_secs": config.kill_interval.as_secs(),
        "downtime_secs": config.downtime.as_secs(),
        "command_env": config.to_string(),
        "workload": {
            "nodes": CLUSTER_NODES, "partitions": PARTITIONS,
            "producers": PRODUCERS, "history_producers": HISTORY_PRODUCERS,
            "group_members": GROUP_MEMBERS,
            "segment_size_bytes": SEGMENT_SIZE_BYTES,
            "message_expiry_secs": MESSAGE_EXPIRY.as_secs(),
            "producer_batch": PRODUCER_BATCH,
            "producer_pause_ms": PRODUCER_PAUSE.as_millis(),
            "heartbeat_interval": GROUP_HEARTBEAT_INTERVAL,
            "session_timeout": GROUP_SESSION_TIMEOUT,
            "cleaner_interval": CLEANER_INTERVAL,
        },
        "host": {
            "os": System::name(), "os_version": System::os_version(),
            "kernel": System::kernel_version(), "arch": std::env::consts::ARCH,
            "cpu": system.cpus().first().map(|cpu| cpu.brand()),
            "logical_cpus": system.cpus().len(),
            "available_parallelism": std::thread::available_parallelism().ok().map(|count| count.get()),
            "total_memory_bytes": system.total_memory(),
            "available_memory_bytes": system.available_memory(),
        },
        "server_binary": server_binary_metadata(),
    })
}

fn server_binary_metadata() -> serde_json::Value {
    let command = Command::cargo_bin("iggy-server").expect("locate the prebuilt server");
    let path = PathBuf::from(command.get_program());
    let fingerprint = || -> std::io::Result<String> {
        let mut file = File::open(&path)?;
        let mut digest = Context::new(&SHA256);
        let mut buffer = [0; DIGEST_BUFFER_SIZE];
        loop {
            let read = file.read(&mut buffer)?;
            if read == 0 {
                break;
            }
            digest.update(&buffer[..read]);
        }
        Ok(hex::encode(digest.finish().as_ref()))
    };
    match fingerprint() {
        Ok(sha256) => json!({"path": path, "sha256": sha256}),
        Err(error) => json!({"path": path, "error": error.to_string()}),
    }
}

fn report_nodes(harness: &TestHarness, report: &mut Report, event: &str) {
    for (node, server) in harness.all_servers().iter().enumerate() {
        let data_path = server.data_path();
        report
            .record(
                event,
                json!({
                    "node": node, "pid": server.pid(), "running": server.is_running(),
                    "data_path": data_path,
                    "effective_config_path": data_path.join("runtime/current_config.toml"),
                    "tcp_addr": server.tcp_addr().map(|address| address.to_string()),
                    "http_addr": server.http_addr().map(|address| address.to_string()),
                }),
            )
            .expect("write node state");
    }
}
