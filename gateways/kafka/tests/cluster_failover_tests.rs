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

//! Three gateways beside three Iggy nodes, under load from real Kafka clients, while gateways and
//! nodes stop, crash and come back.
//!
//! The stack is real end to end. Three `iggy-server` processes form a cluster, the compiled
//! `iggy-gateway-kafka` runs beside each node, and the clients run in Docker. An idempotent Java
//! producer writes numbered records, and a Java consumer group of two members reads them. At the
//! end, kcat reads back what each partition stores.
//!
//! The clients bootstrap from the gateway that coordinates the Java group, and from no other.
//! Metadata names the answering gateway as leader of every partition, so their Produce and Fetch
//! requests go to that gateway until it stops. Each client logs every connection that it loses.
//!
//! A probe holds a member in a group of each of the 16 coordinator partitions. Five times a
//! second, it sends each member's heartbeat to every gateway and asks every gateway to describe
//! the Java group. So a stop shows when each group of the stopped gateway moves, and the probe sees
//! two gateways that answer for one group at once.
//!
//! Each test checks the same things at the end. Every stored record is one that a producer got
//! acknowledged, stored once at the offset that the producer got, and the group reads it once.
//! Every commit succeeds, and the coordinator returns the last commit of each partition, which is
//! the offset after the last record read. The group does not rebalance after it settled.
//!
//! Two gateways answer as coordinator of one group only while it moves: a gateway that lost a
//! partition learns it from the log, so it answers until it reads the newer claim
//! (`src/coordinator/partition.rs`). The tests allow that for [`OVERLAP_LIMIT`], never three
//! coordinators, and no answer that makes a member join again.
//!
//! Prerequisites are Docker, the two client images and an `iggy-server` built into the same target
//! directory. Missing any skips, unless `KAFKA_E2E_REQUIRED=1` is set, as in
//! `kafka_client_e2e_tests.rs`.

#[path = "common/codec.rs"]
mod codec;
#[path = "common/deployment.rs"]
mod deployment;
#[path = "common/docker_clients.rs"]
mod docker_clients;
#[path = "common/gateways.rs"]
mod gateways;
#[path = "common/iggy_server.rs"]
mod iggy_server;
#[path = "common/tcp.rs"]
mod tcp;
#[path = "common/wire.rs"]
mod wire;

use std::collections::{BTreeMap, BTreeSet};
use std::fs;
use std::future::Future;
use std::net::SocketAddr;
use std::ops::RangeInclusive;
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex, MutexGuard, PoisonError};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use bytes::Bytes;
use futures::future::join_all;
use kafka_protocol::messages::offset_fetch_request::OffsetFetchRequestTopic;
use kafka_protocol::messages::{
    DescribeGroupsRequest, DescribeGroupsResponse, FindCoordinatorRequest, FindCoordinatorResponse,
    GroupId, HeartbeatRequest, HeartbeatResponse, MetadataRequest, MetadataResponse,
    OffsetFetchRequest, OffsetFetchResponse, TopicName,
};
use kafka_protocol::protocol::{Decodable, StrBytes};
use serde_json::Value;
use serial_test::serial;
use tempfile::TempDir;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;
use tokio::task::JoinHandle;
use tokio::time::{Instant, MissedTickBehavior};
use tokio_util::sync::CancellationToken;

use iggy_gateway_kafka::bridge::{COORDINATOR_LOG_TOPIC, cluster_id_stream};
use iggy_gateway_kafka::coordinator::{PARTITIONS as COORDINATOR_PARTITIONS, partition_of_group};
use iggy_gateway_kafka::group::{GROUP_STATE_PREPARING_REBALANCE, GROUP_STATE_STABLE};
use iggy_gateway_kafka::protocol::api::{
    API_KEY_DESCRIBE_GROUPS, API_KEY_FIND_COORDINATOR, API_KEY_HEARTBEAT, API_KEY_METADATA,
    API_KEY_OFFSET_FETCH, ERROR_FENCED_INSTANCE_ID, ERROR_ILLEGAL_GENERATION, ERROR_NONE,
    ERROR_REBALANCE_IN_PROGRESS, ERROR_UNKNOWN_MEMBER_ID,
};

use deployment::{Deployment, KAFKA_STREAM, NODES, TAIL_LINES, tail};
use docker_clients::{Container, missing_prerequisite, run_to_end, skip};

const KAFKA_IMAGE: &str = "apache/kafka:3.9.0";
const KCAT_IMAGE: &str = "edenhill/kcat:1.7.1";

/// The topic of the load. Two partitions per node, so a stop of any node moves primaries.
const TOPIC: &str = "failover";
const PARTITIONS: u32 = 6;

/// The Java consumer group: two members of `kafka-verifiable-consumer.sh`. The rebalance test adds
/// a third. A test whose group must start beside the metadata primary picks another name with
/// this prefix.
const GROUP: &str = "failover-readers";
const MEMBERS: usize = 2;

/// The prefix of the probe's groups, one in each coordinator partition.
const PROBE_GROUP: &str = "failover-probe";

/// Records per second that the producer writes, with keys that spread them over every partition.
const RATE: u32 = 100;
const KEYS: &str = "60";

/// The burst of a crash test. A second idempotent producer writes this many records through the
/// coordinator, as fast as the gateway takes them, with the values `1.0`, `1.1` and so on. The
/// coordinator crashes once [`BURST_ACKS_BEFORE_CRASH`] of them are acknowledged, so Produce
/// requests are in flight at the crash, and the producer sends them again through another gateway.
const BURST_RECORDS: usize = 100_000;
const BURST_PREFIX: &str = "1";
const BURST_ACKS_BEFORE_CRASH: usize = 10_000;

/// How long after a kill a producer may still take in an answer that the dead gateway sent before
/// it died: one came 3 ms after. An acknowledgement later than that came through another gateway,
/// and the first one there came 1.4 s or more after the kill.
const LATE_ANSWER: Duration = Duration::from_millis(200);

/// The idempotent Java producers. The first four settings are the defaults since Kafka 3.0, written
/// out because `kafka-verifiable-producer.sh` sets `retries=0` before it reads this file. The
/// delivery timeout outlasts every step, so no record expires in the producer. The Metadata of the
/// bootstrap gateway lasts as long as the container, 10 minutes, so the producer stays on that
/// gateway until it stops.
const PRODUCER_CONFIG: &str = "enable.idempotence=true\n\
                               acks=all\n\
                               retries=2147483647\n\
                               max.in.flight.requests.per.connection=5\n\
                               request.timeout.ms=10000\n\
                               delivery.timeout.ms=300000\n\
                               metadata.max.age.ms=600000\n";

/// The members keep the Metadata of the bootstrap gateway as long as the producers do.
const CONSUMER_CONFIG: &str = "metadata.max.age.ms=600000\n";

/// The log configuration of the Kafka tools, with one line more: `Node <id> disconnected.` for each
/// connection that a client loses.
const CLIENT_LOG_CONFIG: &str = "log4j.rootLogger=WARN, stderr\n\
                                 log4j.appender.stderr=org.apache.log4j.ConsoleAppender\n\
                                 log4j.appender.stderr.layout=org.apache.log4j.PatternLayout\n\
                                 log4j.appender.stderr.layout.ConversionPattern=[%d] %p %m (%c)%n\n\
                                 log4j.appender.stderr.Target=System.err\n\
                                 log4j.logger.org.apache.kafka.clients.NetworkClient=INFO\n";

/// The client configuration files, in the log dir of a test and under `/tmp` in each container.
const PRODUCER_CONFIG_FILE: &str = "producer.properties";
const CONSUMER_CONFIG_FILE: &str = "consumer.properties";
const CLIENT_LOG_CONFIG_FILE: &str = "client-log4j.properties";

/// Acknowledgements, and reads of each member, that a step waits for, so the next step starts
/// under load.
const ACK_PROGRESS: usize = 300;
const READ_PROGRESS: usize = 100;

/// The session settings of the nodes in a test.
#[derive(Debug, Clone, Copy)]
struct Sessions {
    /// The `IGGY_*` settings of every node.
    settings: &'static [(&'static str, &'static str)],
    /// The session timeout: 1.2 x `heartbeat.interval`.
    timeout: Duration,
    /// `consumer_group.heartbeat_interval`. Each node reports its sessions to the metadata primary
    /// once per interval. The primary ends each session that no node reported for the timeout, in
    /// a pass of its own once per interval.
    report: Duration,
}

/// The server defaults: a 30 s `heartbeat.interval` and a 5 s `consumer_group.heartbeat_interval`.
const DEFAULT_SESSIONS: Sessions = Sessions {
    settings: &[],
    timeout: Duration::from_secs(36),
    report: Duration::from_secs(5),
};

/// A 1 s `consumer_group.heartbeat_interval` and an 8 s `heartbeat.interval`. The server needs 1.2 x
/// `heartbeat.interval` above three report intervals plus 6.2 s, so with 1 s reports
/// `heartbeat.interval` must exceed about 7.67 s. 8 s is the next whole second, and gives a 9.6 s
/// timeout.
const SHORT_SESSIONS: Sessions = Sessions {
    settings: &[
        ("IGGY_HEARTBEAT_INTERVAL", "8s"),
        ("IGGY_CONSUMER_GROUP_HEARTBEAT_INTERVAL", "1s"),
    ],
    timeout: Duration::from_millis(9_600),
    report: Duration::from_secs(1),
};

/// What a takeover may take after Iggy ends the session of a crashed gateway: the successor reads
/// its lease once a second, claims the partition and loads it, and the probe asks every 200 ms.
const TAKEOVER_SLACK: Duration = Duration::from_secs(4);

/// How long the metadata plane takes to elect a primary for a lost one: its 5 s
/// `cluster.heartbeat_timeout`, and an election.
const VIEW_CHANGE: Duration = Duration::from_secs(10);

/// How long the groups of a gateway may take to move after its SIGTERM. A successor reads its lease
/// once a second, then loads the partition, after a view change when the partition had its primary
/// on a stopped node. A move that waits for the session timeout fails this.
const HANDOVER_LIMIT: Duration = Duration::from_secs(15);

/// How long the producer may take to get [`ACK_PROGRESS`] more acknowledgements after the crash of
/// its gateway: 3 s of load, and its move to another gateway. A data path that waited for the
/// takeover fails this, since the takeover comes later in every crash test.
const DATA_PATH_LIMIT: Duration = Duration::from_secs(8);
const _: () = assert!(
    DATA_PATH_LIMIT.as_millis()
        < SHORT_SESSIONS.timeout.as_millis() - SHORT_SESSIONS.report.as_millis()
);

/// How long a step may take to recover: view changes, reconnects and group moves.
const RECOVERY: Duration = Duration::from_secs(90);

/// How long a restarted node may take to catch up and get a primary back from the balancer of
/// #4619. It moves one primary at a time, after the cluster is stable for 5 s.
const PRIMARY_MOVE_LIMIT: Duration = Duration::from_secs(120);

/// How long two gateways may both answer as coordinator of one group while it moves. The old
/// owner answers until it reads the newer claim, which took 0.3 s while its node stopped.
const OVERLAP_LIMIT: Duration = Duration::from_secs(2);

/// How long a whole test may run. The slowest takes about 160 s. A test fails itself at this
/// limit, before nextest's 5 minute cap kills it, so it still reports its logs.
const TEST_LIMIT: Duration = Duration::from_secs(270);

/// When the running test reaches [`TEST_LIMIT`]. `Run::start` sets it, and the tests run one at a
/// time.
static TEST_DEADLINE: Mutex<Option<Instant>> = Mutex::new(None);

/// How often the probe asks every gateway, and how long one answer may take.
const PROBE_INTERVAL: Duration = Duration::from_millis(200);
const PROBE_TIMEOUT: Duration = Duration::from_secs(2);

/// The largest answer that the probe reads.
const MAX_FRAME: usize = 8 * 1024 * 1024;

/// Probe rounds that a test must see, so a probe that died early cannot pass the checks.
const MIN_ROUNDS: usize = 50;

/// The versions of the requests the probe sends.
const HEARTBEAT_VERSION: i16 = 2;
const DESCRIBE_GROUPS_VERSION: i16 = 5;
const FIND_COORDINATOR_VERSION: i16 = 4;
const METADATA_VERSION: i16 = 9;
const OFFSET_FETCH_VERSION: i16 = 6;

/// The answers that make a member join again, so their arrival is a rebalance.
const REJOIN: [i16; 4] = [
    ERROR_ILLEGAL_GENERATION,
    ERROR_UNKNOWN_MEMBER_ID,
    ERROR_REBALANCE_IN_PROGRESS,
    ERROR_FENCED_INSTANCE_ID,
];

/// The cap of each client container, which the `timeout` inside it enforces.
const CLIENT_LIFETIME: Duration = Duration::from_mins(10);

/// How long a client may take to exit after SIGTERM, and kcat to read the topic.
const CLIENT_EXIT_LIMIT: Duration = Duration::from_mins(1);

/// The events of the clients that the tests read.
const ACKED: &str = r#""name":"producer_send_success""#;
const READ: &str = r#""name":"record_data""#;
const ASSIGNED: &str = r#""name":"partitions_assigned""#;
const REVOKED: &str = r#""name":"partitions_revoked""#;
const COMMITTED: &str = r#""name":"offsets_committed""#;

/// A rolling restart. Each gateway gets SIGTERM, as an orchestrator sends it, and comes back before
/// the next one stops. Each step stops the coordinator of the Java group while it did not stop yet,
/// so the group moves as often as it can. The groups of the stopped gateway move to the others at
/// once, with their members and offsets, so the Java group reads on without a rebalance.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[serial]
async fn given_load_through_three_gateways_when_each_restarts_in_turn_should_write_and_read_each_record_once()
 {
    let Some(mut run) = Run::start(DEFAULT_SESSIONS, Home::Any).await else {
        return;
    };
    let mut stopped = Vec::with_capacity(NODES);
    while stopped.len() < NODES {
        let node_id = run.next_gateway_to_stop(&stopped).await;
        stopped.push(node_id);
        run.terminate(node_id, Target::Gateway).await;
        run.deployment.start_gateway(node_id).await;
        run.wait_listed(node_id).await;
        run.wait_for_progress(&format!("the load after gateway {node_id} came back"))
            .await;
    }
    run.finish().await.assert_all();
}

/// Each Iggy node stops with SIGTERM and comes back: first the metadata primary, then the node
/// that holds the primary of the group's coordinator partition, then each node not stopped yet.
/// Once a node is back, the balancer of #4619 moves partition primaries to it, under load.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[serial]
async fn given_load_through_three_gateways_when_each_node_stops_in_turn_should_write_and_read_each_record_once()
 {
    let Some(mut run) = Run::start(DEFAULT_SESSIONS, Home::Any).await else {
        return;
    };
    // The node that holds the coordinator partition can be the one that stopped first, so the
    // test can take four steps.
    let mut stopped = Vec::with_capacity(NODES + 1);
    while (0..NODES).any(|node| !stopped.contains(&node)) {
        let (node, role) = run.next_node_to_stop(&stopped).await;
        run.note(&format!("SIGTERM to node {node}, {role}"));
        run.deployment.cluster.stop(node);
        stopped.push(node);
        run.wait_no_primary_on(node).await;
        run.wait_for_progress(&format!("the load while node {node} is down"))
            .await;
        run.assert_offsets_kept(&format!("while node {node} is down"))
            .await;
        run.deployment.cluster.start(node).await;
        run.wait_primary_on(node).await;
        run.wait_for_progress(&format!("the load after node {node} came back"))
            .await;
    }
    run.finish().await.assert_all();
}

/// Each gateway stops together with its node, with SIGTERM to both, as when their machine shuts
/// down. Both come back before the next pair stops. The order is that of the rolling restart.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[serial]
async fn given_load_through_three_gateways_when_each_gateway_stops_with_its_node_should_write_and_read_each_record_once()
 {
    let Some(mut run) = Run::start(DEFAULT_SESSIONS, Home::Any).await else {
        return;
    };
    let mut stopped = Vec::with_capacity(NODES);
    while stopped.len() < NODES {
        let node_id = run.next_gateway_to_stop(&stopped).await;
        stopped.push(node_id);
        let node = Deployment::node_of(node_id);
        run.terminate(node_id, Target::GatewayAndNode).await;
        run.wait_no_primary_on(node).await;
        run.wait_for_progress(&format!("the load while gateway {node_id} is down"))
            .await;
        run.deployment.cluster.start(node).await;
        run.deployment.start_gateway(node_id).await;
        run.wait_listed(node_id).await;
        run.wait_primary_on(node).await;
        run.wait_for_progress(&format!("the load after gateway {node_id} came back"))
            .await;
    }
    run.finish().await.assert_all();
}

/// The gateway that coordinates the Java group stops with SIGTERM during a rebalance. A third
/// member joins, and a paused member holds the group in its join phase, so the stop lands inside
/// the rebalance. The gateway that takes the group over finishes the rebalance with three members,
/// and the group reads each record once.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[serial]
async fn given_load_through_three_gateways_when_the_coordinator_stops_during_a_rebalance_should_finish_it_on_another_gateway()
 {
    let Some(mut run) = Run::start(DEFAULT_SESSIONS, Home::Any).await else {
        return;
    };
    let node_id = run.coordinator().await;
    run.note("pausing member 2, so the rebalance waits for it to join again");
    run.members[1].pause();
    run.add_member();
    run.wait_rebalancing(node_id).await;
    run.terminate(node_id, Target::Gateway).await;
    run.note("resuming member 2");
    run.members[1].unpause();
    run.settle_again().await;
    run.deployment.start_gateway(node_id).await;
    run.wait_listed(node_id).await;
    run.wait_for_progress(&format!("the load after gateway {node_id} came back"))
        .await;
    run.finish().await.assert_all();
}

/// The gateway that coordinates the Java group crashes with SIGKILL, during a burst of Produce
/// requests. The producers write on through the other gateways at once, and each batch that was in
/// flight at the crash is written once. The groups move only when Iggy ends the sessions of the dead
/// gateway, around the default session timeout of 36 s, and keep their members, generation and
/// offsets.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[serial]
async fn given_load_through_three_gateways_when_the_coordinator_crashes_should_move_its_groups_after_the_session_timeout()
 {
    let Some(mut run) = Run::start(DEFAULT_SESSIONS, Home::Any).await else {
        return;
    };
    run.crash_coordinator(Target::Gateway)
        .await
        .assert_within_window();
    run.finish().await.assert_all();
}

/// As the crash above, on nodes with a 9.6 s session timeout and 1 s session reports.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[serial]
async fn given_a_short_session_timeout_when_the_coordinator_crashes_should_move_its_groups_after_it()
 {
    let Some(mut run) = Run::start(SHORT_SESSIONS, Home::Any).await else {
        return;
    };
    run.crash_coordinator(Target::Gateway)
        .await
        .assert_within_window();
    run.finish().await.assert_all();
}

/// The gateway that coordinates the Java group crashes together with its node, as when their
/// machine loses power, on nodes with a 9.6 s session timeout and 1 s session reports. The group
/// starts beside the metadata primary, so the crash takes the metadata primary too. A view change
/// comes first, and the new primary starts the session timeout again. The node recovers from its
/// log when it comes back.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[serial]
async fn given_a_short_session_timeout_when_the_coordinator_crashes_with_the_metadata_primary_should_move_its_groups_after_it()
 {
    let Some(mut run) = Run::start(SHORT_SESSIONS, Home::BesideMetadataPrimary).await else {
        return;
    };
    let takeover = run.crash_coordinator(Target::GatewayAndNode).await;
    assert!(
        takeover.view_change,
        "the node of the crashed coordinator no longer held the metadata primary"
    );
    takeover.assert_within_window();
    run.finish().await.assert_all();
}

/// What a stop or a crash takes down.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Target {
    Gateway,
    /// The gateway and the node beside it.
    GatewayAndNode,
}

/// Which gateway the Java group starts on, which the clients bootstrap from.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Home {
    /// The gateway that owns the coordinator partition of [`GROUP`].
    Any,
    /// The gateway beside the node that holds the metadata primary, with a group of a coordinator
    /// partition that the gateway owns.
    BesideMetadataPrimary,
}

impl Sessions {
    /// When the takeover after a crash must come, from the crash. The last report of the crashed
    /// gateway's session came at most one `report` before the crash. The metadata primary ends the
    /// session in its first pass at least `timeout` after that report, and a pass comes once per
    /// `report`. The successor then needs [`TAKEOVER_SLACK`]. A crash that took the metadata
    /// primary adds a view change, after which the new primary starts the timeout again.
    fn takeover_window(self, view_change: bool) -> RangeInclusive<Duration> {
        let earliest = self.timeout.saturating_sub(self.report);
        let mut latest = self.timeout + self.report + TAKEOVER_SLACK;
        if view_change {
            latest += VIEW_CHANGE;
        }
        earliest..=latest
    }
}

/// How long after a crash another gateway took the groups of the crashed one over, and the window
/// that the takeover must fall in.
struct Takeover {
    after: Duration,
    window: RangeInclusive<Duration>,
    /// Whether the crash took the metadata primary, which adds a view change.
    view_change: bool,
}

impl Takeover {
    /// Before the window, a successor took the groups while Iggy still held the session of the
    /// crashed gateway, so the lease is broken. After it, the takeover is slower than the README
    /// and `CONSUMER_GROUPS.md` say.
    fn assert_within_window(&self) {
        assert!(
            self.window.contains(&self.after),
            "the takeover came {:?} after the crash, outside {:?}",
            self.after,
            self.window
        );
    }
}

/// One test's deployment, under load from the clients, watched by the probe.
struct Run {
    deployment: Deployment,
    sessions: Sessions,
    clients: Clients,
    /// The Java group.
    group: String,
    /// The gateway that the clients bootstrap from. It coordinated the group at the start.
    home: u16,
    producer: Container,
    /// The burst producer of a crash test, once it ended.
    bursts: Vec<Container>,
    members: Vec<Container>,
    probe: Probe,
    /// The lines each member printed when the group last settled. An assignment after that is a
    /// rebalance.
    settled: Vec<usize>,
    /// When the group last settled. The probe must describe it as stable from then on.
    settled_at: Instant,
    started: Instant,
    /// Last, so the processes stop before a failed test reports its logs.
    work_dir: WorkDir,
}

impl Run {
    /// Deploys the nodes with the settings of `sessions`, and their gateways. Picks the group for
    /// `home`, bootstraps the members from its coordinator and settles the group, then starts the
    /// probe and the producer. `None` when the host cannot run the clients.
    async fn start(sessions: Sessions, home: Home) -> Option<Self> {
        if let Some(reason) =
            missing_prerequisite(&[KAFKA_IMAGE, KCAT_IMAGE]).or_else(missing_server)
        {
            skip(&reason);
            return None;
        }
        let started = Instant::now();
        *TEST_DEADLINE.lock().unwrap_or_else(PoisonError::into_inner) = Some(started + TEST_LIMIT);
        let work_dir = WorkDir::new();
        // Printed now, since a test that nextest kills reports nothing at its end.
        note(
            started,
            &format!("the logs go to {}", work_dir.path().join("logs").display()),
        );
        let deployment =
            Deployment::start(work_dir.path(), sessions.settings, &[(TOPIC, PARTITIONS)]).await;
        note(started, "deployed three nodes and three gateways");
        // A gateway lists another once Iggy gave that one coordinator partitions. Once every
        // gateway lists all three, the partitions are shared out, and the group stays where it is.
        for node_id in deployment.gateway_ids() {
            wait_listed(&deployment, node_id).await;
        }
        let group = match home {
            Home::Any => GROUP.to_string(),
            Home::BesideMetadataPrimary => group_beside_metadata_primary(&deployment).await,
        };
        let home = wait_until(
            RECOVERY,
            PROBE_INTERVAL,
            "agreement on the coordinator",
            || agreed_coordinator(&deployment, &group),
        )
        .await;
        note(
            started,
            &format!(
                "gateway {home} coordinates the group {group}, and the clients bootstrap from it"
            ),
        );
        let clients = Clients::new(deployment.log_dir(), deployment.addr_of(home), &group);
        let members: Vec<Container> = (1..=MEMBERS).map(|member| clients.member(member)).collect();
        let settled = settle(&members).await;
        let settled_at = Instant::now();
        note(started, "the group settled");
        let probe = Probe::start(deployment.addrs(), group.clone()).await;
        let producer = clients.producer();
        let run = Self {
            deployment,
            sessions,
            clients,
            group,
            home,
            producer,
            bursts: Vec::new(),
            members,
            probe,
            settled,
            settled_at,
            started,
            work_dir,
        };
        run.wait_for_progress("the first records").await;
        run.note("the load runs");
        Some(run)
    }

    fn note(&self, what: &str) {
        note(self.started, what);
    }

    /// The gateway that coordinates the group, once every running gateway names the same one.
    async fn coordinator(&self) -> u16 {
        wait_until(
            RECOVERY,
            PROBE_INTERVAL,
            "agreement on the coordinator",
            || agreed_coordinator(&self.deployment, &self.group),
        )
        .await
    }

    /// The gateway to stop next in turn: the coordinator of the group while it did not stop yet,
    /// else the first gateway that did not stop yet.
    async fn next_gateway_to_stop(&self, stopped: &[u16]) -> u16 {
        let coordinator = self.coordinator().await;
        if !stopped.contains(&coordinator) {
            return coordinator;
        }
        self.deployment
            .gateway_ids()
            .into_iter()
            .find(|node_id| !stopped.contains(node_id))
            .expect("a gateway not stopped yet")
    }

    /// The node to stop next, with its role: first the metadata primary, then the node that holds
    /// the primary of the group's coordinator partition, then each node not stopped yet.
    async fn next_node_to_stop(&self, stopped: &[usize]) -> (usize, &'static str) {
        if stopped.is_empty() {
            let primary = wait_until(RECOVERY, PROBE_INTERVAL, "a metadata primary", || {
                self.deployment.metadata_primary()
            })
            .await;
            return (primary, "the metadata primary");
        }
        if stopped.len() == 1 {
            let stream = cluster_id_stream(KAFKA_STREAM);
            let partition = partition_of_group(&self.group);
            let primary = wait_until(
                RECOVERY,
                PROBE_INTERVAL,
                "the primary of the coordinator partition",
                || async {
                    let primaries = self
                        .deployment
                        .primaries(&stream, COORDINATOR_LOG_TOPIC, [partition])
                        .await?;
                    primaries.first().copied()
                },
            )
            .await;
            return (primary, "the primary of the coordinator partition");
        }
        let node = (0..NODES)
            .find(|node| !stopped.contains(node))
            .expect("a node not stopped yet");
        (node, "a node not stopped yet")
    }

    /// Stops gateway `node_id` with SIGTERM, with its node for [`Target::GatewayAndNode`]. Each
    /// group that the gateway coordinated must move to another gateway within [`HANDOVER_LIMIT`],
    /// and the coordinator must return every offset committed so far.
    async fn terminate(&mut self, node_id: u16, target: Target) {
        let partitions = self.coordinated_by(node_id).await;
        let role = if self.coordinator().await == node_id {
            ", the coordinator of the group"
        } else {
            ""
        };
        let what = match target {
            Target::Gateway => format!("gateway {node_id}"),
            Target::GatewayAndNode => {
                format!(
                    "gateway {node_id} and node {}",
                    Deployment::node_of(node_id)
                )
            }
        };
        self.note(&format!(
            "SIGTERM to {what}{role}, with the groups of coordinator partitions {partitions:?}"
        ));
        let stopped_at = Instant::now();
        match target {
            Target::Gateway => {
                self.deployment.terminate_gateway(node_id).await;
            }
            Target::GatewayAndNode => self.deployment.terminate_gateway_and_node(node_id).await,
        }
        let moved = self.wait_moved(stopped_at, node_id, &partitions).await;
        self.note(&format!("its groups moved {moved:?} after the SIGTERM"));
        assert!(
            moved <= HANDOVER_LIMIT,
            "the groups of gateway {node_id} moved {moved:?} after its SIGTERM, above \
             {HANDOVER_LIMIT:?}"
        );
        self.assert_offsets_kept(&format!("after the SIGTERM to {what}"))
            .await;
    }

    /// Kills the gateway that coordinates the group with SIGKILL, with its node for
    /// [`Target::GatewayAndNode`], during a burst of Produce requests through it. Restarts what it
    /// killed once another gateway took the groups over.
    ///
    /// Checks on the way that each client lost a connection to the gateway, so it was on the
    /// gateway at the crash, and that the burst sent requests again that were in flight at the
    /// crash. For [`Target::Gateway`], the producer must write on through another gateway within
    /// [`DATA_PATH_LIMIT`]. After the takeover, the coordinator must return every offset committed
    /// so far. The burst must end with every record acknowledged.
    async fn crash_coordinator(&mut self, target: Target) -> Takeover {
        let node_id = self.coordinator().await;
        assert_eq!(
            node_id, self.home,
            "the group left gateway {}, which the clients bootstrap from, before the crash",
            self.home
        );
        let node = Deployment::node_of(node_id);
        let view_change = target == Target::GatewayAndNode
            && self.deployment.metadata_primary().await == Some(node);
        let partitions = self.coordinated_by(node_id).await;
        let burst = self.start_burst().await;
        let losses_before = self.connection_losses(&burst, node_id);
        let what = match target {
            Target::Gateway => format!("gateway {node_id}"),
            Target::GatewayAndNode if view_change => {
                format!("gateway {node_id} and node {node}, the metadata primary")
            }
            Target::GatewayAndNode => format!("gateway {node_id} and node {node}"),
        };
        self.note(&format!(
            "SIGKILL to {what}, during a burst of Produce requests through the coordinator"
        ));
        let crash_started = unix_micros();
        let crashed_at = Instant::now();
        self.deployment.kill_gateway(node_id);
        let killed = unix_micros();
        if target == Target::GatewayAndNode {
            self.deployment.cluster.crash(node);
        }
        // A lost metadata primary stalls every partition until the next one is elected, since a
        // node refuses partition requests on metadata evidence older than 8 s.
        let data_path_limit = match target {
            Target::Gateway => DATA_PATH_LIMIT,
            Target::GatewayAndNode => RECOVERY,
        };
        let wrote_on = self
            .wait_for_acks("writes through the other gateways", data_path_limit)
            .await
            .saturating_duration_since(crashed_at);
        self.note(&format!(
            "the producer wrote on through another gateway {wrote_on:?} after the crash"
        ));
        self.assert_resent(&burst, node_id);
        let after = self.wait_moved(crashed_at, node_id, &partitions).await;
        self.note(&format!("its groups moved {after:?} after the crash"));
        self.assert_clients_were_on(&burst, node_id, &losses_before);
        self.assert_offsets_kept("after the takeover").await;
        if target == Target::GatewayAndNode {
            self.deployment.cluster.start(node).await;
        }
        self.deployment.start_gateway(node_id).await;
        self.wait_listed(node_id).await;
        if target == Target::GatewayAndNode {
            self.wait_primary_on(node).await;
        }
        self.end_burst(burst, crash_started, killed).await;
        self.wait_for_progress("the load after the crash").await;
        Takeover {
            after,
            window: self.sessions.takeover_window(view_change),
            view_change,
        }
    }

    /// Starts the burst producer, and returns once it got [`BURST_ACKS_BEFORE_CRASH`] records
    /// acknowledged.
    async fn start_burst(&self) -> Container {
        let burst = self.clients.burst();
        wait_until(
            RECOVERY,
            Duration::from_millis(10),
            "the first records of the burst",
            || async { (burst.count(ACKED) >= BURST_ACKS_BEFORE_CRASH).then_some(()) },
        )
        .await;
        burst
    }

    /// Fails the test unless the burst sent a batch again that was in flight to gateway `node_id`
    /// when it crashed.
    fn assert_resent(&self, burst: &Container, node_id: u16) {
        let resent =
            burst.stderr_count(&format!("Error Message: Disconnected from node {node_id}"));
        assert!(
            resent > 0,
            "the burst had no Produce request in flight at the crash, so it sent none again:\n{}",
            burst.stderr_tail(TAIL_LINES)
        );
        self.note(&format!(
            "the burst sent {resent} batches again that were in flight at the crash"
        ));
    }

    /// Fails the test unless each client lost a connection to gateway `node_id` since it counted
    /// `before`, so each was on the gateway when it crashed.
    fn assert_clients_were_on(&self, burst: &Container, node_id: u16, before: &[usize]) {
        let after = self.connection_losses(burst, node_id);
        assert!(
            after
                .iter()
                .zip(before)
                .all(|(after, before)| after > before),
            "a client lost no connection to gateway {node_id} at its crash, so it was not on it. \
             Losses of the producer, each member and the burst, before {before:?} and after \
             {after:?}"
        );
    }

    /// Waits until the burst producer ends, and fails the test unless it got every record
    /// acknowledged and Iggy answered one of its resends with the first offset.
    async fn end_burst(&mut self, mut burst: Container, crash_started: u64, killed: u64) {
        assert!(
            burst.wait_exit(RECOVERY).await,
            "the burst producer ends within {RECOVERY:?}"
        );
        let sent = burst.with_stdout(Produced::parse);
        assert!(
            sent.errors.is_empty(),
            "{} sends of the burst failed: {:#?}",
            sent.errors.len(),
            sent.errors
        );
        assert_eq!(
            sent.acked.len(),
            BURST_RECORDS,
            "the burst producer gets each record acknowledged"
        );
        self.assert_resends_deduplicated(&burst, crash_started, killed)
            .await;
        self.bursts.push(burst);
    }

    /// Fails the test unless Iggy answered a resend of the burst with the first offset of its
    /// batch: a record that the producer got acknowledged after the kill, at an offset that Iggy
    /// stored before it. Only another gateway can answer after the kill, and it answers a resend
    /// of a batch that Iggy holds with the first offset. A batch written twice gets a later offset.
    /// `crash_started` and `killed` are in microseconds since the Unix epoch.
    async fn assert_resends_deduplicated(
        &self,
        burst: &Container,
        crash_started: u64,
        killed: u64,
    ) {
        let later = killed.saturating_add(micros(LATE_ANSWER));
        let (first_ack, first_later) = burst.with_stdout(|lines| {
            (
                first_ack_after(lines, killed),
                first_acked_after(lines, later),
            )
        });
        self.note(&format!(
            "the first acknowledgement of the burst after the kill came {:?} after it",
            first_ack.map(|at| Duration::from_micros(at.saturating_sub(killed)))
        ));
        let mut deduplicated = Vec::new();
        for (partition, offset) in first_later {
            let (Ok(partition_id), Ok(position)) =
                (u32::try_from(partition), u64::try_from(offset))
            else {
                continue;
            };
            let stored = wait_until(
                RECOVERY,
                PROBE_INTERVAL,
                "the time Iggy stored a record",
                || {
                    self.deployment
                        .stored_at(KAFKA_STREAM, TOPIC, partition_id, position)
                },
            )
            .await;
            if stored < crash_started {
                deduplicated.push((partition, offset));
            }
        }
        assert!(
            !deduplicated.is_empty(),
            "no resend of the burst came back at an offset that Iggy stored before the crash, so \
             the crash tested no resend of a batch that Iggy wrote"
        );
        self.note(&format!(
            "Iggy answered resends with the first offset of their batch, as partition and offset: \
             {deduplicated:?}"
        ));
    }

    /// How often each client logged the loss of a connection to gateway `node_id`: the producer,
    /// each member, then `burst`.
    fn connection_losses(&self, burst: &Container, node_id: u16) -> Vec<usize> {
        let lost = format!("Node {node_id} disconnected.");
        std::iter::once(&self.producer)
            .chain(&self.members)
            .chain([burst])
            .map(|client| client.stderr_count(&lost))
            .collect()
    }

    /// Starts one more member of the group, bootstrapped as the others.
    fn add_member(&mut self) {
        let member = self.members.len() + 1;
        self.note(&format!("member {member} joins the group"));
        self.members.push(self.clients.member(member));
    }

    /// Waits until the group settles again with every member. An assignment after that is a
    /// rebalance.
    async fn settle_again(&mut self) {
        self.settled = settle(&self.members).await;
        self.settled_at = Instant::now();
        self.note(&format!(
            "the group settled with {} members",
            self.members.len()
        ));
    }

    /// Waits until gateway `node_id` describes the group as preparing a rebalance.
    async fn wait_rebalancing(&self, node_id: u16) {
        let what = format!("gateway {node_id} preparing a rebalance of the group");
        wait_until(RECOVERY, PROBE_INTERVAL, &what, || async {
            let (code, state) = self.probe.latest_description(gateway_index(node_id))?;
            (code == ERROR_NONE && state == GROUP_STATE_PREPARING_REBALANCE).then_some(())
        })
        .await;
    }

    /// Waits until the producer gets more acknowledgements and every member reads more records.
    async fn wait_for_progress(&self, what: &str) {
        self.wait_for_acks(what, RECOVERY).await;
        let before: Vec<usize> = self
            .members
            .iter()
            .map(|member| member.count(READ))
            .collect();
        wait_until(RECOVERY, PROBE_INTERVAL, what, || async {
            self.members
                .iter()
                .zip(&before)
                .all(|(member, before)| member.count(READ) >= before + READ_PROGRESS)
                .then_some(())
        })
        .await;
    }

    /// Waits until the producer gets [`ACK_PROGRESS`] more acknowledgements, at most `limit`, and
    /// returns when it saw them.
    async fn wait_for_acks(&self, what: &str, limit: Duration) -> Instant {
        let before = self.producer.count(ACKED);
        wait_until(limit, PROBE_INTERVAL, what, || async {
            (self.producer.count(ACKED) >= before + ACK_PROGRESS).then(Instant::now)
        })
        .await
    }

    /// The coordinator partitions whose probe groups gateway `node_id` answers for, in a probe
    /// round that starts after this call. Fails the test when there are none, since a stop of the
    /// gateway would then move no group.
    async fn coordinated_by(&self, node_id: u16) -> BTreeSet<usize> {
        let after = Instant::now();
        let partitions = wait_until(RECOVERY, PROBE_INTERVAL, "a probe round", || async {
            self.probe.coordinated_by(after, gateway_index(node_id))
        })
        .await;
        assert!(
            !partitions.is_empty(),
            "gateway {node_id} coordinates no group, so its stop moves none"
        );
        partitions
    }

    /// Waits until a gateway other than `gone` answers as coordinator of the probe group of each
    /// of `partitions`, in rounds that started at `since` or later. Returns how long after `since`
    /// the round ended that saw the last of them move.
    async fn wait_moved(
        &self,
        since: Instant,
        gone: u16,
        partitions: &BTreeSet<usize>,
    ) -> Duration {
        let what = format!("the groups of gateway {gone} on another gateway");
        wait_until(RECOVERY, PROBE_INTERVAL, &what, || async {
            self.probe
                .moved(since, gateway_index(gone), partitions)
                .map(|at| at.saturating_duration_since(since))
        })
        .await
    }

    /// Fails the test when a commit failed so far, or when the coordinator of the group returns an
    /// offset below one that a member committed with success.
    async fn assert_offsets_kept(&self, what: &str) {
        let commits = Commits::of(&self.members);
        commits.assert_none_failed(what);
        let returned = returned_offsets(&self.deployment, &self.group).await;
        let behind: Vec<_> = commits
            .last
            .iter()
            .filter(|(partition, offset)| returned.get(partition).is_none_or(|got| got < offset))
            .map(|(partition, offset)| (partition, offset, returned.get(partition)))
            .collect();
        assert!(
            behind.is_empty(),
            "{what}, the coordinator returns offsets below acknowledged commits, as partition, \
             commit and returned offset: {behind:?}"
        );
        self.note(&format!(
            "{what}, the coordinator returns each acknowledged commit, as partition and offset: \
             {returned:?}"
        ));
    }

    /// Waits until every running gateway lists gateway `node_id` in `Metadata`.
    async fn wait_listed(&self, node_id: u16) {
        wait_listed(&self.deployment, node_id).await;
    }

    /// Waits until node `node`, which stopped, leads no partition under load: its partitions
    /// changed view and have primaries elsewhere.
    async fn wait_no_primary_on(&self, node: usize) {
        let what = format!("new primaries for the partitions of node {node}");
        wait_until(RECOVERY, Duration::from_secs(1), &what, || async {
            let primaries = self.loaded_primaries().await?;
            (!primaries.contains(&node)).then_some(())
        })
        .await;
    }

    /// Waits until the balancer of #4619 moves a primary of a partition under load to node
    /// `node`, which came back with none. The balancer evens out the count of primaries over all
    /// partitions, so the move can be of the topic or of the coordinator log.
    async fn wait_primary_on(&self, node: usize) {
        let what = format!("a primary move to node {node}");
        let primaries = wait_until(
            PRIMARY_MOVE_LIMIT,
            Duration::from_secs(1),
            &what,
            || async {
                let primaries = self.loaded_primaries().await?;
                primaries.contains(&node).then_some(primaries)
            },
        )
        .await;
        let held: Vec<usize> = (0..NODES)
            .map(|replica| {
                primaries
                    .iter()
                    .filter(|primary| **primary == replica)
                    .count()
            })
            .collect();
        self.note(&format!(
            "primaries of the topic and the coordinator log per node: {held:?}"
        ));
    }

    /// The primary of each partition under load: those of the topic, then those of the
    /// coordinator log.
    async fn loaded_primaries(&self) -> Option<Vec<usize>> {
        let mut primaries = self
            .deployment
            .primaries(KAFKA_STREAM, TOPIC, 0..PARTITIONS)
            .await?;
        let coordinator_log = self
            .deployment
            .primaries(
                &cluster_id_stream(KAFKA_STREAM),
                COORDINATOR_LOG_TOPIC,
                0..COORDINATOR_PARTITIONS,
            )
            .await?;
        primaries.extend(coordinator_log);
        Some(primaries)
    }

    /// Stops the producer, waits until the group read everything that the producers got
    /// acknowledged, stops the probe and the members, and reads back what the partitions store
    /// and what the coordinator returns as committed.
    async fn finish(self) -> Outcome {
        let Self {
            deployment,
            group,
            mut producer,
            bursts,
            mut members,
            probe,
            settled,
            settled_at,
            started,
            work_dir,
            ..
        } = self;
        note(started, "stopping the producer");
        producer.signal("TERM");
        assert!(
            producer.wait_exit(CLIENT_EXIT_LIMIT).await,
            "the producer exits after SIGTERM"
        );
        let mut produced = producer.with_stdout(Produced::parse);
        for burst in &bursts {
            produced.merge(burst.with_stdout(Produced::parse));
        }
        wait_until(
            RECOVERY,
            Duration::from_secs(1),
            "the group reading every acknowledged record",
            || async {
                let read: BTreeSet<String> = members
                    .iter()
                    .flat_map(|member| member.with_stdout(records_read))
                    .map(|record| record.value)
                    .collect();
                produced
                    .acked
                    .keys()
                    .all(|value| read.contains(value))
                    .then_some(())
            },
        )
        .await;
        let rounds = probe.stop().await;
        // A member revokes its partitions when it stops, so the check for a rebalance ends here.
        let stopping: Vec<usize> = members
            .iter()
            .map(|member| member.with_stdout(<[String]>::len))
            .collect();
        for member in &members {
            member.signal("TERM");
        }
        for member in &mut members {
            assert!(
                member.wait_exit(CLIENT_EXIT_LIMIT).await,
                "a member exits after SIGTERM"
            );
        }
        let printed: Vec<Vec<String>> = members
            .iter()
            .map(|member| member.with_stdout(<[String]>::to_vec))
            .collect();
        let commits = Commits::of(&members);
        let returned = returned_offsets(&deployment, &group).await;
        note(started, "reading the partitions back with kcat");
        let stored = stored_records(&deployment.bootstrap()).await;
        let listings = broker_listings(&deployment).await;
        note(started, "done");
        Outcome {
            produced,
            stored,
            read: printed
                .iter()
                .flat_map(|lines| records_read(lines))
                .collect(),
            commits,
            returned,
            rebalances: rebalances(&printed, &settled, &stopping),
            rounds,
            settled_at,
            listings,
            _work_dir: work_dir,
        }
    }
}

/// How the tests start their Kafka clients: in Docker, bootstrapped from one gateway alone, with
/// their configuration files in the log dir of the test.
struct Clients {
    bootstrap: String,
    logs: PathBuf,
    group: String,
}

impl Clients {
    /// Writes the configuration files to `logs`, for clients that bootstrap from `home`, with
    /// members in `group`.
    fn new(logs: &Path, home: SocketAddr, group: &str) -> Self {
        for (file, content) in [
            (PRODUCER_CONFIG_FILE, PRODUCER_CONFIG),
            (CONSUMER_CONFIG_FILE, CONSUMER_CONFIG),
            (CLIENT_LOG_CONFIG_FILE, CLIENT_LOG_CONFIG),
        ] {
            fs::write(logs.join(file), content).expect("write a client configuration");
        }
        Self {
            bootstrap: home.to_string(),
            logs: logs.to_path_buf(),
            group: group.to_string(),
        }
    }

    /// Member `member` of the group. It commits each batch after it reads it, and prints a JSON
    /// event for each step and one for each record.
    fn member(&self, member: usize) -> Container {
        let config = guest(CONSUMER_CONFIG_FILE);
        self.start(
            &[
                "/opt/kafka/bin/kafka-verifiable-consumer.sh",
                "--bootstrap-server",
                &self.bootstrap,
                "--topic",
                TOPIC,
                "--group-id",
                &self.group,
                "--reset-policy",
                "earliest",
                "--assignment-strategy",
                "org.apache.kafka.clients.consumer.RangeAssignor",
                "--consumer.config",
                &config,
                "--verbose",
            ],
            &format!("member-{member}"),
        )
    }

    /// The producer. It writes the numbers from 0 as values, [`RATE`] a second, until it gets
    /// SIGTERM, and prints a JSON event for each record it sent.
    fn producer(&self) -> Container {
        let rate = RATE.to_string();
        let config = guest(PRODUCER_CONFIG_FILE);
        self.start(
            &[
                "/opt/kafka/bin/kafka-verifiable-producer.sh",
                "--bootstrap-server",
                &self.bootstrap,
                "--topic",
                TOPIC,
                "--throughput",
                &rate,
                "--repeating-keys",
                KEYS,
                "--producer.config",
                &config,
            ],
            "producer",
        )
    }

    /// The burst producer of a crash test. It writes [`BURST_RECORDS`] records as fast as the
    /// gateway takes them, then ends.
    fn burst(&self) -> Container {
        let records = BURST_RECORDS.to_string();
        let config = guest(PRODUCER_CONFIG_FILE);
        self.start(
            &[
                "/opt/kafka/bin/kafka-verifiable-producer.sh",
                "--bootstrap-server",
                &self.bootstrap,
                "--topic",
                TOPIC,
                "--max-messages",
                &records,
                "--value-prefix",
                BURST_PREFIX,
                "--repeating-keys",
                KEYS,
                "--producer.config",
                &config,
            ],
            "burst",
        )
    }

    /// Starts `args` in the Kafka image, with the configuration files and the log configuration.
    /// Its output goes to `<name>.out` and `<name>.err` in the log dir.
    fn start(&self, args: &[&str], name: &str) -> Container {
        let files: Vec<(PathBuf, String)> = [
            PRODUCER_CONFIG_FILE,
            CONSUMER_CONFIG_FILE,
            CLIENT_LOG_CONFIG_FILE,
        ]
        .iter()
        .map(|file| (self.logs.join(file), guest(file)))
        .collect();
        let mounts: Vec<(&Path, &str)> = files
            .iter()
            .map(|(host, guest)| (host.as_path(), guest.as_str()))
            .collect();
        let log_options = format!(
            "-Dlog4j.configuration=file:{}",
            guest(CLIENT_LOG_CONFIG_FILE)
        );
        Container::start(
            KAFKA_IMAGE,
            args,
            &mounts,
            &[("KAFKA_LOG4J_OPTS", &log_options)],
            CLIENT_LIFETIME,
            &self.logs.join(name),
        )
    }
}

/// Where a configuration file of the log dir appears in a client container.
fn guest(file: &str) -> String {
    format!("/tmp/{file}")
}

/// The time of the first acknowledgement in `lines` after `after`, both in microseconds since the
/// Unix epoch.
fn first_ack_after(lines: &[String], after: u64) -> Option<u64> {
    acks(lines)
        .map(|(at, _, _)| at)
        .filter(|at| *at > after)
        .min()
}

/// The lowest offset of each partition among the records of `lines` that the producer got
/// acknowledged after `after`, in microseconds since the Unix epoch.
fn first_acked_after(lines: &[String], after: u64) -> BTreeMap<i64, i64> {
    let mut first: BTreeMap<i64, i64> = BTreeMap::new();
    for (_, partition, offset) in acks(lines).filter(|(at, _, _)| *at > after) {
        first
            .entry(partition)
            .and_modify(|lowest| *lowest = (*lowest).min(offset))
            .or_insert(offset);
    }
    first
}

/// Each acknowledgement in `lines`, as the time the producer printed it, in microseconds since the
/// Unix epoch, and the partition and offset of the record.
fn acks(lines: &[String]) -> impl Iterator<Item = (u64, i64, i64)> + '_ {
    lines
        .iter()
        .filter(|line| line.contains(ACKED))
        .filter_map(|line| {
            let event = parse_event(line)?;
            Some((
                event["timestamp"].as_u64()?.saturating_mul(1_000),
                event["partition"].as_i64()?,
                event["offset"].as_i64()?,
            ))
        })
}

/// The wall clock in microseconds since the Unix epoch, as Iggy stamps a message.
fn unix_micros() -> u64 {
    micros(
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default(),
    )
}

fn micros(duration: Duration) -> u64 {
    u64::try_from(duration.as_micros()).unwrap_or(u64::MAX)
}

/// What a test saw, checked once every client stopped.
struct Outcome {
    produced: Produced,
    /// Every stored record, as `(partition, offset, value)`.
    stored: Vec<(i64, i64, String)>,
    /// Each record the group read, once per read.
    read: Vec<Read>,
    commits: Commits,
    /// The committed offset of each partition, -1 for none, as the coordinator returns it once the
    /// members stopped.
    returned: BTreeMap<i64, i64>,
    /// The assignment events that came after the group last settled.
    rebalances: Vec<String>,
    rounds: Vec<Round>,
    settled_at: Instant,
    /// What `kcat -L` printed through each gateway, with the line for each gateway it must list.
    listings: Vec<(String, Vec<String>)>,
    _work_dir: WorkDir,
}

impl Outcome {
    fn assert_all(&self) {
        self.assert_each_record_once();
        self.assert_offsets_returned();
        self.assert_no_rebalance();
        self.assert_one_coordinator_but_while_moving();
        self.assert_every_gateway_listed();
    }

    /// Every acknowledged record is stored once, at the partition and offset that the producer
    /// got, every stored record is one that a producer got acknowledged, and the group reads each
    /// once.
    fn assert_each_record_once(&self) {
        let errors = &self.produced.errors;
        assert!(
            errors.is_empty(),
            "{} sends failed: {errors:#?}",
            errors.len()
        );
        let acked = &self.produced.acked;
        assert!(!acked.is_empty(), "the producer must write records");
        let mut stored: BTreeMap<&str, Vec<(i64, i64)>> = BTreeMap::new();
        for (partition, offset, value) in &self.stored {
            stored
                .entry(value.as_str())
                .or_default()
                .push((*partition, *offset));
        }
        let twice: Vec<_> = stored
            .iter()
            .filter(|(_, at)| at.len() > 1)
            .take(20)
            .collect();
        assert!(
            twice.is_empty(),
            "records stored more than once, as value and (partition, offset): {twice:?}"
        );
        let misplaced: Vec<_> = acked
            .iter()
            .filter(|(value, at)| {
                stored
                    .get(value.as_str())
                    .is_none_or(|stored| stored != &[**at])
            })
            .map(|(value, at)| (value, at, stored.get(value.as_str())))
            .take(20)
            .collect();
        assert!(
            misplaced.is_empty(),
            "acknowledged records not stored where the producer was told, as value, acknowledged \
             (partition, offset) and stored ones: {misplaced:?}"
        );
        let unacked: Vec<_> = stored
            .iter()
            .filter(|(value, _)| !acked.contains_key(**value))
            .take(20)
            .collect();
        assert!(
            unacked.is_empty(),
            "stored records that no producer got acknowledged, as value and (partition, offset): \
             {unacked:?}"
        );
        let mut reads: BTreeMap<&str, usize> = BTreeMap::new();
        for read in &self.read {
            *reads.entry(read.value.as_str()).or_default() += 1;
        }
        let read_twice: Vec<_> = reads
            .iter()
            .filter(|(_, count)| **count > 1)
            .take(20)
            .collect();
        assert!(
            read_twice.is_empty(),
            "records the group read more than once, as value and reads: {read_twice:?}"
        );
        let unread: Vec<&String> = acked
            .keys()
            .filter(|value| !reads.contains_key(value.as_str()))
            .take(20)
            .collect();
        assert!(
            unread.is_empty(),
            "acknowledged records the group never read: {unread:?}"
        );
        eprintln!("{} records acknowledged, stored and read once", acked.len());
    }

    /// Every commit succeeded, and the coordinator returns the last commit of each partition. That
    /// is the offset after the last record that the group read there, so each read was committed.
    fn assert_offsets_returned(&self) {
        self.commits.assert_none_failed("by the end");
        let committed: BTreeMap<i64, i64> = (0..i64::from(PARTITIONS))
            .map(|partition| {
                let offset = self.commits.last.get(&partition).copied().unwrap_or(-1);
                (partition, offset)
            })
            .collect();
        assert_eq!(
            self.returned, committed,
            "the coordinator returns other offsets than the last commit of each partition, as \
             partition and offset"
        );
        let after_read: BTreeMap<i64, i64> = (0..i64::from(PARTITIONS))
            .map(|partition| {
                let last = self
                    .read
                    .iter()
                    .filter(|read| read.partition == partition)
                    .map(|read| read.offset + 1)
                    .max()
                    .unwrap_or(-1);
                (partition, last)
            })
            .collect();
        assert_eq!(
            self.returned, after_read,
            "the coordinator returns other offsets than the one after the last record read in each \
             partition, as partition and offset"
        );
        eprintln!("the coordinator returns the last commit of each partition: {committed:?}");
    }

    /// The group keeps its members and generation after it last settled: no member gets a new
    /// assignment, and the coordinator describes the group as stable every time.
    fn assert_no_rebalance(&self) {
        assert!(
            self.rebalances.is_empty(),
            "the group rebalanced after it settled: {:#?}",
            self.rebalances
        );
        let unstable: Vec<&(i16, String)> = self
            .rounds
            .iter()
            .filter(|round| round.at >= self.settled_at)
            .flat_map(|round| round.descriptions.iter().flatten())
            .filter(|(code, state)| *code == ERROR_NONE && state != GROUP_STATE_STABLE)
            .collect();
        assert!(
            unstable.is_empty(),
            "the coordinator described the group as not stable: {unstable:?}"
        );
    }

    /// Two gateways answer as coordinator of one group only while it moves, for at most
    /// [`OVERLAP_LIMIT`], and three never. No answer makes a probe member join again.
    fn assert_one_coordinator_but_while_moving(&self) {
        assert!(
            self.rounds.len() >= MIN_ROUNDS,
            "the probe ran only {} rounds",
            self.rounds.len()
        );
        let mut longest = Duration::ZERO;
        for partition in 0..COORDINATOR_PARTITIONS {
            let index = usize::try_from(partition).expect("a partition index");
            let overlap = self.assert_overlap_ends(
                &format!("the probe group of coordinator partition {partition}"),
                |round| coordinating(round.heartbeats.iter().map(|codes| codes[index])),
            );
            longest = longest.max(overlap);
        }
        let overlap = self.assert_overlap_ends("the Java group", |round| {
            coordinating(
                round
                    .descriptions
                    .iter()
                    .map(|answer| answer.as_ref().map(|(code, _)| *code)),
            )
        });
        longest = longest.max(overlap);
        for round in &self.rounds {
            assert!(
                !round
                    .heartbeats
                    .iter()
                    .flatten()
                    .flatten()
                    .any(|code| REJOIN.contains(code)),
                "a gateway told a probe member to join again: {round:?}"
            );
        }
        eprintln!("the longest time two gateways answered for one group: {longest:?}");
    }

    /// Fails the test when `coordinators` counts three in a round, or two in rounds that span more
    /// than [`OVERLAP_LIMIT`], from the start of the first to the end of the last. Returns the
    /// longest span.
    fn assert_overlap_ends(&self, what: &str, coordinators: impl Fn(&Round) -> usize) -> Duration {
        let mut longest = Duration::ZERO;
        let mut first: Option<&Round> = None;
        for round in &self.rounds {
            let count = coordinators(round);
            assert!(
                count <= 2,
                "three gateways answered as coordinator of {what} at once: {round:?}"
            );
            if count < 2 {
                first = None;
                continue;
            }
            let start = *first.get_or_insert(round);
            let span = round.done.saturating_duration_since(start.at);
            assert!(
                span <= OVERLAP_LIMIT,
                "two gateways answered as coordinator of {what} for {span:?}, above \
                 {OVERLAP_LIMIT:?}, from {start:?} to {round:?}"
            );
            longest = longest.max(span);
        }
        longest
    }

    /// Each gateway lists all three at the end, as kcat sees it.
    fn assert_every_gateway_listed(&self) {
        for (listing, expected) in &self.listings {
            for line in expected {
                assert!(
                    listing.contains(line.as_str()),
                    "`kcat -L` must show `{line}`:\n{listing}"
                );
            }
        }
    }
}

/// What a producer printed.
#[derive(Default)]
struct Produced {
    /// Where each acknowledged record went, by value, as `(partition, offset)`.
    acked: BTreeMap<String, (i64, i64)>,
    errors: Vec<String>,
}

impl Produced {
    fn parse(lines: &[String]) -> Self {
        let mut produced = Self::default();
        for line in lines {
            let Some(event) = parse_event(line) else {
                continue;
            };
            match event["name"].as_str() {
                Some("producer_send_success") => {
                    if let (Some(value), Some(partition), Some(offset)) = (
                        event["value"].as_str(),
                        event["partition"].as_i64(),
                        event["offset"].as_i64(),
                    ) {
                        produced
                            .acked
                            .insert(value.to_string(), (partition, offset));
                    }
                }
                Some("producer_send_error") => produced.errors.push(line.clone()),
                _ => {}
            }
        }
        produced
    }

    fn merge(&mut self, other: Self) {
        self.acked.extend(other.acked);
        self.errors.extend(other.errors);
    }
}

/// What the members printed about their commits.
#[derive(Debug, Default)]
struct Commits {
    /// The highest offset that each partition committed with success.
    last: BTreeMap<i64, i64>,
    /// Each commit that failed, as a member printed it.
    failed: Vec<String>,
}

impl Commits {
    fn of(members: &[Container]) -> Self {
        let mut commits = Self::default();
        for member in members {
            member.with_stdout(|lines| commits.add(lines));
        }
        commits
    }

    fn add(&mut self, lines: &[String]) {
        for line in lines.iter().filter(|line| line.contains(COMMITTED)) {
            let Some(event) = parse_event(line) else {
                continue;
            };
            if event["success"].as_bool() != Some(true) {
                self.failed.push(line.clone());
                continue;
            }
            for committed in event["offsets"].as_array().into_iter().flatten() {
                if let (Some(partition), Some(offset)) = (
                    committed["partition"].as_i64(),
                    committed["offset"].as_i64(),
                ) {
                    let last = self.last.entry(partition).or_insert(offset);
                    *last = (*last).max(offset);
                }
            }
        }
    }

    fn assert_none_failed(&self, what: &str) {
        assert!(
            self.failed.is_empty(),
            "commits failed {what}: {:#?}",
            self.failed
        );
    }
}

/// The answers of one probe round, one entry per gateway in node id order.
#[derive(Debug, Clone)]
struct Round {
    at: Instant,
    /// When the last answer of the round came.
    done: Instant,
    /// The error code of each probe member's heartbeat, by coordinator partition. `None` where the
    /// gateway did not answer.
    heartbeats: Vec<Vec<Option<i16>>>,
    /// The error code and state of the Java group in `DescribeGroups`.
    descriptions: Vec<Option<(i16, String)>>,
}

/// One member in a group of each coordinator partition, which heartbeats at every gateway, and
/// asks every gateway to describe the Java group, until it stops.
struct Probe {
    rounds: Arc<Mutex<Vec<Round>>>,
    stop: CancellationToken,
    task: JoinHandle<()>,
}

impl Probe {
    /// Joins a member to a group of each coordinator partition, at its coordinator, then starts the
    /// rounds, which also describe the Java group `described`.
    async fn start(addrs: Vec<SocketAddr>, described: String) -> Self {
        let members = join_all(groups_by_partition(PROBE_GROUP).into_iter().map(|group| {
            let addrs = &addrs;
            async move {
                let coordinator = wait_until(
                    RECOVERY,
                    PROBE_INTERVAL,
                    "the coordinator of a probe group",
                    || coordinator_addr(addrs, &group),
                )
                .await;
                let (member_id, generation) = gateways::join_and_sync(coordinator, &group).await;
                ProbeMember {
                    group,
                    member_id,
                    generation,
                }
            }
        }))
        .await;
        let rounds = Arc::new(Mutex::new(Vec::new()));
        let stop = CancellationToken::new();
        let task = tokio::spawn(probe_rounds(
            addrs,
            members,
            described,
            Arc::clone(&rounds),
            stop.clone(),
        ));
        Self { rounds, stop, task }
    }

    /// The coordinator partitions whose probe groups gateway `gateway`, an index in node id order,
    /// answered for in the first round that started at `after` or later.
    fn coordinated_by(&self, after: Instant, gateway: usize) -> Option<BTreeSet<usize>> {
        self.rounds()
            .iter()
            .find(|round| round.at >= after)
            .map(|round| {
                round.heartbeats[gateway]
                    .iter()
                    .enumerate()
                    .filter(|(_, code)| **code == Some(ERROR_NONE))
                    .map(|(partition, _)| partition)
                    .collect()
            })
    }

    /// When the round ended that saw the last of `partitions` answered by a gateway other than the
    /// one at index `gone`, among the rounds that started at `since` or later. `None` until a
    /// round saw each of them.
    fn moved(&self, since: Instant, gone: usize, partitions: &BTreeSet<usize>) -> Option<Instant> {
        let rounds = self.rounds();
        let last = partitions.iter().try_fold(since, |last, partition| {
            let round = rounds
                .iter()
                .filter(|round| round.at >= since)
                .find(|round| {
                    round.heartbeats.iter().enumerate().any(|(gateway, codes)| {
                        gateway != gone && codes[*partition] == Some(ERROR_NONE)
                    })
                })?;
            Some(last.max(round.done))
        });
        drop(rounds);
        last
    }

    /// What gateway `gateway` answered about the Java group in the latest round.
    fn latest_description(&self, gateway: usize) -> Option<(i16, String)> {
        self.rounds().last()?.descriptions[gateway].clone()
    }

    fn rounds(&self) -> MutexGuard<'_, Vec<Round>> {
        self.rounds.lock().unwrap_or_else(PoisonError::into_inner)
    }

    async fn stop(self) -> Vec<Round> {
        self.stop.cancel();
        self.task.await.expect("the probe does not panic");
        std::mem::take(&mut *self.rounds.lock().unwrap_or_else(PoisonError::into_inner))
    }
}

struct ProbeMember {
    group: String,
    member_id: StrBytes,
    generation: i32,
}

async fn probe_rounds(
    addrs: Vec<SocketAddr>,
    members: Vec<ProbeMember>,
    described: String,
    rounds: Arc<Mutex<Vec<Round>>>,
    stop: CancellationToken,
) {
    let mut links: Vec<Link> = addrs.into_iter().map(Link::new).collect();
    let mut ticks = tokio::time::interval(PROBE_INTERVAL);
    ticks.set_missed_tick_behavior(MissedTickBehavior::Delay);
    loop {
        tokio::select! {
            () = stop.cancelled() => return,
            _ = ticks.tick() => {}
        }
        let at = Instant::now();
        let answers = join_all(links.iter_mut().map(|link| ask(link, &members, &described))).await;
        let (heartbeats, descriptions) = answers.into_iter().unzip();
        let done = Instant::now();
        rounds
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .push(Round {
                at,
                done,
                heartbeats,
                descriptions,
            });
    }
}

/// Sends the heartbeat of every probe member to the gateway of `link`, then asks it to describe
/// the Java group `described`. Once a request gets no answer, the round skips the rest for that
/// gateway.
async fn ask(
    link: &mut Link,
    members: &[ProbeMember],
    described: &str,
) -> (Vec<Option<i16>>, Option<(i16, String)>) {
    let mut heartbeats = Vec::with_capacity(members.len());
    let mut answering = true;
    for member in members {
        let code = if answering {
            heartbeat(link, member).await
        } else {
            None
        };
        answering = code.is_some();
        heartbeats.push(code);
    }
    let description = if answering {
        describe(link, described).await
    } else {
        None
    };
    (heartbeats, description)
}

/// A group named `<prefix>-<n>` in each coordinator partition, in partition order.
fn groups_by_partition(prefix: &str) -> Vec<String> {
    (0..COORDINATOR_PARTITIONS)
        .map(|partition| {
            // A thousand names cover the 16 partitions many times over.
            (0..1_000)
                .map(|n| format!("{prefix}-{n}"))
                .find(|group| partition_of_group(group) == partition)
                .expect("a group in every partition")
        })
        .collect()
}

/// A group whose coordinator is the gateway beside the node that holds the metadata primary.
async fn group_beside_metadata_primary(deployment: &Deployment) -> String {
    let primary = wait_until(RECOVERY, PROBE_INTERVAL, "a metadata primary", || {
        deployment.metadata_primary()
    })
    .await;
    let beside = deployment.gateway_ids()[primary];
    let groups = groups_by_partition(GROUP);
    let what = format!("a coordinator partition of gateway {beside}");
    wait_until(RECOVERY, PROBE_INTERVAL, &what, || async {
        for group in &groups {
            if agreed_coordinator(deployment, group).await? == beside {
                return Some(group.clone());
            }
        }
        None
    })
    .await
}

/// How many of `answers` name their gateway the coordinator.
fn coordinating(answers: impl Iterator<Item = Option<i16>>) -> usize {
    answers.filter(|code| *code == Some(ERROR_NONE)).count()
}

/// The index of gateway `node_id` among the gateways, in node id order.
fn gateway_index(node_id: u16) -> usize {
    usize::from(node_id) - 1
}

/// The test's directory. A failed test keeps the logs there, drops the node data, and prints where
/// they are with the end of each gateway and client log.
struct WorkDir(Option<TempDir>);

impl WorkDir {
    fn new() -> Self {
        let dir = tempfile::Builder::new()
            .prefix("iggy-kafka-failover-")
            .tempdir()
            .expect("create the work dir");
        Self(Some(dir))
    }

    fn path(&self) -> &Path {
        self.0.as_ref().expect("a work dir").path()
    }
}

impl Drop for WorkDir {
    fn drop(&mut self) {
        if !std::thread::panicking() {
            return;
        }
        let Some(dir) = self.0.take() else {
            return;
        };
        let path = dir.keep();
        let _ = fs::remove_dir_all(path.join("data"));
        let logs = path.join("logs");
        let mut report = vec![format!(
            "the failed test kept its logs in {}",
            logs.display()
        )];
        for node_id in 1..=NODES {
            let log = logs.join(format!("gateway-{node_id}.log"));
            report.push(format!(
                "--- gateway {node_id} ---\n{}",
                tail(&log, TAIL_LINES)
            ));
        }
        for client in ["producer", "burst", "member-1", "member-2", "member-3"] {
            let log = logs.join(format!("{client}.err"));
            if log.exists() {
                report.push(format!("--- {client} stderr ---\n{}", tail(&log, 10)));
            }
        }
        eprintln!("{}", report.join("\n"));
    }
}

fn note(started: Instant, what: &str) {
    eprintln!("[{:>6.1} s] {what}", started.elapsed().as_secs_f64());
}

/// Why the cluster cannot start, or `None` when it can: `iggy-server` must be built into the
/// target directory of this test binary.
fn missing_server() -> Option<String> {
    let binary = format!("iggy-server{}", std::env::consts::EXE_SUFFIX);
    let built = std::env::current_exe()
        .ok()?
        .ancestors()
        .any(|dir| dir.join(&binary).is_file());
    (!built).then(|| "iggy-server is not built; run `cargo build --bin iggy-server` first".into())
}

/// Polls `check` every `interval` until it returns a value, and fails the test after `limit`.
async fn wait_until<T, F: Future<Output = Option<T>>>(
    limit: Duration,
    interval: Duration,
    what: &str,
    mut check: impl FnMut() -> F,
) -> T {
    let deadline = Instant::now() + limit;
    let test_deadline = *TEST_DEADLINE.lock().unwrap_or_else(PoisonError::into_inner);
    loop {
        if let Some(value) = check().await {
            return value;
        }
        let now = Instant::now();
        assert!(now < deadline, "{what} did not happen within {limit:?}");
        assert!(
            test_deadline.is_none_or(|end| now < end),
            "{what} did not happen before the test ran for {TEST_LIMIT:?}"
        );
        tokio::time::sleep(interval).await;
    }
}

/// Waits until each member's latest assignment is its share of the partitions, and returns how
/// many lines each had printed by then.
async fn settle(members: &[Container]) -> Vec<usize> {
    wait_until(
        RECOVERY,
        PROBE_INTERVAL,
        "an assignment of every partition to one member",
        || async {
            let mut held = BTreeSet::new();
            let mut printed = Vec::with_capacity(members.len());
            for member in members {
                let (lines, assigned) =
                    member.with_stdout(|lines| (lines.len(), last_assignment(lines)));
                for partition in assigned? {
                    if !held.insert(partition) {
                        return None;
                    }
                }
                printed.push(lines);
            }
            (held.len() == usize::try_from(PARTITIONS).ok()?).then_some(printed)
        },
    )
    .await
}

/// The partitions of the last `partitions_assigned` event in `lines`.
fn last_assignment(lines: &[String]) -> Option<Vec<i64>> {
    let event = lines
        .iter()
        .rev()
        .filter(|line| line.contains(ASSIGNED))
        .find_map(|line| parse_event(line))?;
    event["partitions"]
        .as_array()?
        .iter()
        .map(|partition| partition["partition"].as_i64())
        .collect()
}

/// The value of each record a member read, in the order read.
fn records_read(lines: &[String]) -> Vec<Read> {
    lines
        .iter()
        .filter(|line| line.contains(READ))
        .map(|line| {
            Read::parse(line)
                .unwrap_or_else(|| panic!("a member printed a record that does not parse: {line}"))
        })
        .collect()
}

/// A record that a member read.
#[derive(Debug)]
struct Read {
    value: String,
    partition: i64,
    offset: i64,
}

impl Read {
    fn parse(line: &str) -> Option<Self> {
        let event = parse_event(line)?;
        Some(Self {
            value: event["value"].as_str()?.to_string(),
            partition: event["partition"].as_i64()?,
            offset: event["offset"].as_i64()?,
        })
    }
}

/// The assignment events that each member printed between the last settled group, `settled`
/// lines, and its SIGTERM, `stopping` lines.
fn rebalances(printed: &[Vec<String>], settled: &[usize], stopping: &[usize]) -> Vec<String> {
    printed
        .iter()
        .zip(settled.iter().zip(stopping))
        .enumerate()
        .flat_map(|(member, (lines, (settled, stopping)))| {
            lines[*settled..*stopping]
                .iter()
                .filter(|line| line.contains(ASSIGNED) || line.contains(REVOKED))
                .map(move |line| format!("member {}: {line}", member + 1))
        })
        .collect()
}

fn parse_event(line: &str) -> Option<Value> {
    line.starts_with('{')
        .then(|| serde_json::from_str(line).ok())
        .flatten()
}

/// Every record that the partitions store, as `(partition, offset, value)`, read with kcat
/// through the gateways.
async fn stored_records(bootstrap: &str) -> Vec<(i64, i64, String)> {
    let args: Vec<String> = [
        "-b",
        bootstrap,
        "-C",
        "-t",
        TOPIC,
        "-o",
        "beginning",
        "-e",
        "-q",
        "-f",
        "%p %o %s\n",
    ]
    .map(String::from)
    .to_vec();
    let read =
        tokio::task::spawn_blocking(move || run_to_end(KCAT_IMAGE, &args, CLIENT_EXIT_LIMIT))
            .await
            .expect("kcat runs");
    assert!(read.succeeded, "kcat reads the topic:\n{}", read.stderr);
    read.stdout
        .lines()
        .map(|line| {
            stored_record(line).unwrap_or_else(|| {
                panic!("kcat printed a line that is not `partition offset value`: {line:?}")
            })
        })
        .collect()
}

/// A line of `kcat -f '%p %o %s\n'`. The value is the rest of the line, also when empty.
fn stored_record(line: &str) -> Option<(i64, i64, String)> {
    let mut fields = line.splitn(3, ' ');
    Some((
        fields.next()?.parse().ok()?,
        fields.next()?.parse().ok()?,
        fields.next()?.to_string(),
    ))
}

/// What `kcat -L` prints through each gateway, with the line for each gateway it must list.
async fn broker_listings(deployment: &Deployment) -> Vec<(String, Vec<String>)> {
    let addrs = deployment.addrs();
    let expected: Vec<String> = deployment
        .gateway_ids()
        .iter()
        .zip(&addrs)
        .map(|(node_id, addr)| format!("broker {node_id} at {addr}"))
        .collect();
    let mut listings = Vec::with_capacity(addrs.len());
    for addr in addrs {
        let args = vec!["-b".to_string(), addr.to_string(), "-L".to_string()];
        let listed =
            tokio::task::spawn_blocking(move || run_to_end(KCAT_IMAGE, &args, CLIENT_EXIT_LIMIT))
                .await
                .expect("kcat runs");
        assert!(
            listed.succeeded,
            "kcat -L through {addr}:\n{}",
            listed.stderr
        );
        listings.push((listed.stdout, expected.clone()));
    }
    listings
}

/// Waits until every running gateway lists gateway `node_id` in `Metadata`. A gateway lists another
/// once that one owns coordinator partitions, so a restarted gateway is back then.
async fn wait_listed(deployment: &Deployment, node_id: u16) {
    let addrs = deployment.running_addrs();
    let what = format!("every gateway listing gateway {node_id}");
    wait_until(RECOVERY, PROBE_INTERVAL, &what, || async {
        for addr in &addrs {
            if !listed_brokers(*addr).await?.contains(&i32::from(node_id)) {
                return None;
            }
        }
        Some(())
    })
    .await;
}

/// The gateway that every running gateway names as coordinator of `group`, when they agree.
async fn agreed_coordinator(deployment: &Deployment, group: &str) -> Option<u16> {
    let mut named = BTreeSet::new();
    for addr in deployment.running_addrs() {
        named.insert(named_coordinator(addr, group).await?);
    }
    let node_id = *named.first()?;
    (named.len() == 1).then_some(u16::try_from(node_id).ok()?)
}

/// The committed offset of each partition of the topic for `group`, -1 for none, as its
/// coordinator returns them.
async fn returned_offsets(deployment: &Deployment, group: &str) -> BTreeMap<i64, i64> {
    wait_until(
        RECOVERY,
        PROBE_INTERVAL,
        "the committed offsets of the group",
        || async {
            let node_id = agreed_coordinator(deployment, group).await?;
            committed_offsets(deployment.addr_of(node_id), group).await
        },
    )
    .await
}

/// The committed offset of each partition of the topic for `group`, as the gateway at `addr`
/// returns them. `None` unless the gateway answers as coordinator of the group.
async fn committed_offsets(addr: SocketAddr, group: &str) -> Option<BTreeMap<i64, i64>> {
    let partitions = (0..i32::try_from(PARTITIONS).ok()?).collect();
    let request = OffsetFetchRequest::default()
        .with_group_id(GroupId(StrBytes::from_string(group.to_string())))
        .with_topics(Some(vec![
            OffsetFetchRequestTopic::default()
                .with_name(TopicName(StrBytes::from_static_str(TOPIC)))
                .with_partition_indexes(partitions),
        ]));
    let body = wire::encode(&request, OFFSET_FETCH_VERSION);
    let answer: OffsetFetchResponse =
        query(addr, API_KEY_OFFSET_FETCH, OFFSET_FETCH_VERSION, &body).await?;
    if answer.error_code != ERROR_NONE {
        return None;
    }
    answer
        .topics
        .first()?
        .partitions
        .iter()
        .map(|partition| {
            (partition.error_code == ERROR_NONE).then_some((
                i64::from(partition.partition_index),
                partition.committed_offset,
            ))
        })
        .collect()
}

/// The node ids that the gateway at `addr` lists in `Metadata`.
async fn listed_brokers(addr: SocketAddr) -> Option<Vec<i32>> {
    let request = MetadataRequest::default().with_topics(Some(Vec::new()));
    let body = wire::encode(&request, METADATA_VERSION);
    let answer: MetadataResponse = query(addr, API_KEY_METADATA, METADATA_VERSION, &body).await?;
    Some(
        answer
            .brokers
            .iter()
            .map(|broker| broker.node_id.0)
            .collect(),
    )
}

/// The node id that the gateway at `addr` names as coordinator of `group`.
async fn named_coordinator(addr: SocketAddr, group: &str) -> Option<i32> {
    let request = FindCoordinatorRequest::default()
        .with_coordinator_keys(vec![StrBytes::from_string(group.to_string())]);
    let body = wire::encode(&request, FIND_COORDINATOR_VERSION);
    let answer: FindCoordinatorResponse = query(
        addr,
        API_KEY_FIND_COORDINATOR,
        FIND_COORDINATOR_VERSION,
        &body,
    )
    .await?;
    let coordinator = answer.coordinators.first()?;
    (coordinator.error_code == ERROR_NONE).then_some(coordinator.node_id.0)
}

/// The address of the gateway that coordinates `group`, as the first gateway that names one says.
async fn coordinator_addr(addrs: &[SocketAddr], group: &str) -> Option<SocketAddr> {
    for addr in addrs {
        if let Some(node_id) = named_coordinator(*addr, group).await {
            let index = usize::try_from(node_id).ok()?.checked_sub(1)?;
            return addrs.get(index).copied();
        }
    }
    None
}

async fn heartbeat(link: &mut Link, member: &ProbeMember) -> Option<i16> {
    let request = HeartbeatRequest::default()
        .with_group_id(GroupId(StrBytes::from_string(member.group.clone())))
        .with_generation_id(member.generation)
        .with_member_id(member.member_id.clone());
    let body = wire::encode(&request, HEARTBEAT_VERSION);
    let answer: HeartbeatResponse = link
        .exchange(API_KEY_HEARTBEAT, HEARTBEAT_VERSION, &body)
        .await?;
    Some(answer.error_code)
}

async fn describe(link: &mut Link, group: &str) -> Option<(i16, String)> {
    let request = DescribeGroupsRequest::default()
        .with_groups(vec![GroupId(StrBytes::from_string(group.to_string()))]);
    let body = wire::encode(&request, DESCRIBE_GROUPS_VERSION);
    let answer: DescribeGroupsResponse = link
        .exchange(API_KEY_DESCRIBE_GROUPS, DESCRIBE_GROUPS_VERSION, &body)
        .await?;
    let described = answer.groups.first()?;
    Some((described.error_code, described.group_state.to_string()))
}

/// Sends one request on a connection of its own. See [`Link::exchange`].
async fn query<A: Decodable>(addr: SocketAddr, key: i16, version: i16, body: &[u8]) -> Option<A> {
    Link::new(addr).exchange(key, version, body).await
}

/// A connection to one gateway, opened again after any failure.
struct Link {
    addr: SocketAddr,
    stream: Option<TcpStream>,
    correlation_id: i32,
}

impl Link {
    const fn new(addr: SocketAddr) -> Self {
        Self {
            addr,
            stream: None,
            correlation_id: 0,
        }
    }

    /// Sends one request and decodes the answer. `None` when nothing accepts at the address, the
    /// gateway closes the connection, or no answer comes within [`PROBE_TIMEOUT`]. The connection
    /// closes then, so a late answer cannot pass for the next one.
    async fn exchange<A: Decodable>(&mut self, key: i16, version: i16, body: &[u8]) -> Option<A> {
        self.correlation_id = self.correlation_id.wrapping_add(1);
        let correlation_id = self.correlation_id;
        let frame =
            tcp::build_request_frame(key, version, correlation_id, Some("failover-probe"), body);
        let addr = self.addr;
        let stream = &mut self.stream;
        let payload = tokio::time::timeout(PROBE_TIMEOUT, async {
            if stream.is_none() {
                *stream = Some(TcpStream::connect(addr).await.ok()?);
            }
            let connection = stream.as_mut()?;
            connection.write_all(&frame).await.ok()?;
            let mut length = [0; 4];
            connection.read_exact(&mut length).await.ok()?;
            let length = usize::try_from(i32::from_be_bytes(length))
                .ok()
                .filter(|length| *length <= MAX_FRAME)?;
            let mut payload = vec![0; length];
            connection.read_exact(&mut payload).await.ok()?;
            Some(payload)
        })
        .await
        .ok()
        .flatten();
        let answer = payload.and_then(|payload| {
            let (answered, mut body) =
                tcp::parse_response_payload(key, version, Bytes::from(payload));
            (answered == correlation_id)
                .then(|| A::decode(&mut body, version).ok())
                .flatten()
        });
        if answer.is_none() {
            self.stream = None;
        }
        answer
    }
}
