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
//! producer writes numbered records through the three gateways, and a Java consumer group of two
//! members reads them. At the end, kcat reads back what each partition stores.
//!
//! A probe keeps a member of a second group alive with a heartbeat to every gateway, five times a
//! second, and asks every gateway to describe the Java group. Both groups hash to one coordinator
//! partition, so one gateway coordinates both.
//!
//! Each test checks the same things at the end. Every acknowledged record is stored once, at the
//! offset that the producer got, and the group reads it once. The group does not rebalance, and no
//! two gateways answer as coordinator of a group at once.
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
use std::path::Path;
use std::sync::{Arc, Mutex, PoisonError};
use std::time::Duration;

use bytes::Bytes;
use futures::future::join_all;
use kafka_protocol::messages::{
    DescribeGroupsRequest, DescribeGroupsResponse, FindCoordinatorRequest, FindCoordinatorResponse,
    GroupId, HeartbeatRequest, HeartbeatResponse, MetadataRequest, MetadataResponse,
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
use iggy_gateway_kafka::protocol::api::{
    API_KEY_DESCRIBE_GROUPS, API_KEY_FIND_COORDINATOR, API_KEY_HEARTBEAT, API_KEY_METADATA,
    ERROR_FENCED_INSTANCE_ID, ERROR_ILLEGAL_GENERATION, ERROR_NONE, ERROR_REBALANCE_IN_PROGRESS,
    ERROR_UNKNOWN_MEMBER_ID,
};

use deployment::{Deployment, KAFKA_STREAM, NODES, TAIL_LINES, tail};
use docker_clients::{Container, missing_prerequisite, run_to_end, skip};

const KAFKA_IMAGE: &str = "apache/kafka:3.9.0";
const KCAT_IMAGE: &str = "edenhill/kcat:1.7.1";

/// The topic of the load. Two partitions per node, so a stop of any node moves primaries.
const TOPIC: &str = "failover";
const PARTITIONS: u32 = 6;

/// The Java consumer group, two members of `kafka-verifiable-consumer.sh`.
const GROUP: &str = "failover-readers";
const MEMBERS: usize = 2;

/// Records per second that the producer writes, with keys that spread them over every partition.
const RATE: u32 = 100;
const KEYS: &str = "60";

/// The idempotent Java producer. These are the defaults since Kafka 3.0, written out because
/// `kafka-verifiable-producer.sh` sets `retries=0` before it reads this file. The delivery timeout
/// outlasts every step, so no record expires in the producer.
const PRODUCER_CONFIG: &str = "enable.idempotence=true\n\
                               acks=all\n\
                               retries=2147483647\n\
                               max.in.flight.requests.per.connection=5\n\
                               request.timeout.ms=10000\n\
                               delivery.timeout.ms=300000\n";

/// Acknowledgements, and reads of each member, that a step waits for, so the next step starts
/// under load.
const ACK_PROGRESS: usize = 300;
const READ_PROGRESS: usize = 100;

/// The Iggy session timeout by default: 1.2 x the 30 s `heartbeat.interval`.
const SESSION_TIMEOUT: Duration = Duration::from_secs(36);

/// Server settings for the shortest session timeout that a 1 s report interval allows. The server
/// needs 1.2 x `heartbeat.interval` above three report intervals plus 6.2 s, so 8 s is the next
/// whole second, and the timeout is 9.6 s.
const SHORT_SESSION: [(&str, &str); 2] = [
    ("IGGY_HEARTBEAT_INTERVAL", "8s"),
    ("IGGY_CONSUMER_GROUP_HEARTBEAT_INTERVAL", "1s"),
];
const SHORT_SESSION_TIMEOUT: Duration = Duration::from_millis(9_600);

/// What a takeover after a crash may take beyond the session timeout: the report that ends the
/// session, the lease read of the successor once a second, and its load of the partition.
const TAKEOVER_SLACK: Duration = Duration::from_secs(15);

/// How long the cluster takes to elect a new primary for a lost one: its `cluster.heartbeat_timeout`
/// of 5 s, with an election.
const VIEW_CHANGE: Duration = Duration::from_secs(10);

/// How long the groups of a gateway may take to move after its SIGTERM. A successor reads its lease
/// once a second, then loads the partition. A move that waits for the session timeout fails this.
const HANDOVER_LIMIT: Duration = Duration::from_secs(15);

/// How long the producer may take to write again through another gateway after one crashed. Below
/// the session timeout, so the data path does not wait for it.
const DATA_PATH_LIMIT: Duration = Duration::from_secs(20);

/// How long a step may take to recover: view changes, reconnects and group moves.
const RECOVERY: Duration = Duration::from_secs(90);

/// How long a restarted node may take to catch up and get a primary back from the balancer of
/// #4619. It moves one primary at a time, after the cluster is stable for 5 s.
const PRIMARY_MOVE_LIMIT: Duration = Duration::from_secs(120);

/// How often the probe asks every gateway, and how long one answer may take.
const PROBE_INTERVAL: Duration = Duration::from_millis(200);
const PROBE_TIMEOUT: Duration = Duration::from_secs(2);

/// Probe rounds that a test must see, so a probe that died early cannot pass the checks.
const MIN_ROUNDS: usize = 50;

/// The versions of the requests the probe sends.
const HEARTBEAT_VERSION: i16 = 2;
const DESCRIBE_GROUPS_VERSION: i16 = 5;
const FIND_COORDINATOR_VERSION: i16 = 4;
const METADATA_VERSION: i16 = 9;

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

/// The events of the clients that the tests count.
const ACKED: &str = r#""name":"producer_send_success""#;
const READ: &str = r#""name":"record_data""#;

/// A rolling restart. Each gateway gets SIGTERM, as an orchestrator sends it, and comes back
/// before the next one stops. Its groups move to the other gateways at once, with their members,
/// so the Java group reads on without a rebalance.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[serial]
async fn given_load_through_three_gateways_when_each_restarts_in_turn_should_write_and_read_each_record_once()
 {
    let Some(mut run) = Run::start(&[]).await else {
        return;
    };
    for node_id in run.rotation().await {
        run.note(&format!("SIGTERM to gateway {node_id}"));
        let stopped_at = Instant::now();
        run.deployment.terminate_gateway(node_id).await;
        let moved = run.wait_served(stopped_at, node_id).await;
        run.note(&format!("its groups moved {moved:?} after the SIGTERM"));
        assert!(
            moved <= HANDOVER_LIMIT,
            "the groups of gateway {node_id} moved {moved:?} after its SIGTERM, above \
             {HANDOVER_LIMIT:?}"
        );
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
    let Some(mut run) = Run::start(&[]).await else {
        return;
    };
    let mut stopped = Vec::with_capacity(NODES + 1);
    while (0..NODES).any(|node| !stopped.contains(&node)) {
        let (node, role) = run.next_node_to_stop(&stopped).await;
        run.note(&format!("SIGTERM to node {node}, {role}"));
        run.deployment.cluster.stop(node);
        stopped.push(node);
        run.wait_no_primary_on(node).await;
        run.wait_for_progress(&format!("the load while node {node} is down"))
            .await;
        run.deployment.cluster.start(node).await;
        run.wait_primary_on(node).await;
        run.wait_for_progress(&format!("the load after node {node} came back"))
            .await;
    }
    run.finish().await.assert_all();
}

/// Each gateway stops together with its node, with SIGTERM to both, as when their machine shuts
/// down. Both come back before the next pair stops.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[serial]
async fn given_load_through_three_gateways_when_each_gateway_stops_with_its_node_should_write_and_read_each_record_once()
 {
    let Some(mut run) = Run::start(&[]).await else {
        return;
    };
    for node_id in run.rotation().await {
        let node = Deployment::node_of(node_id);
        run.note(&format!("SIGTERM to gateway {node_id} and node {node}"));
        let stopped_at = Instant::now();
        run.deployment.terminate_gateway_and_node(node_id).await;
        let moved = run.wait_served(stopped_at, node_id).await;
        run.note(&format!("its groups moved {moved:?} after the SIGTERM"));
        // A coordinator partition with its primary on the stopped node waits for a view change.
        assert!(
            moved <= HANDOVER_LIMIT,
            "the groups of gateway {node_id} moved {moved:?} after its SIGTERM, above \
             {HANDOVER_LIMIT:?}"
        );
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

/// The gateway that coordinates the group crashes, with SIGKILL. The producer writes on through
/// the other gateways at once. The group moves only when Iggy ends the sessions of the dead
/// gateway, after the default session timeout of 36 s, and keeps its members and generation.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[serial]
async fn given_load_through_three_gateways_when_the_coordinator_crashes_should_move_its_groups_after_the_session_timeout()
 {
    let Some(mut run) = Run::start(&[]).await else {
        return;
    };
    let takeover = run.crash_coordinator(Crash::Gateway).await;
    assert!(
        takeover <= SESSION_TIMEOUT + TAKEOVER_SLACK,
        "the takeover took {takeover:?}, above the session timeout {SESSION_TIMEOUT:?} and \
         {TAKEOVER_SLACK:?}"
    );
    run.finish().await.assert_all();
}

/// As the crash above, on nodes with the shortest session timeout that a 1 s
/// `consumer_group.heartbeat_interval` allows: 9.6 s, from an 8 s `heartbeat.interval`.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[serial]
async fn given_a_short_session_timeout_when_the_coordinator_crashes_should_move_its_groups_within_it()
 {
    let Some(mut run) = Run::start(&SHORT_SESSION).await else {
        return;
    };
    let takeover = run.crash_coordinator(Crash::Gateway).await;
    assert!(
        takeover <= SHORT_SESSION_TIMEOUT + TAKEOVER_SLACK,
        "the takeover took {takeover:?}, above the session timeout {SHORT_SESSION_TIMEOUT:?} \
         and {TAKEOVER_SLACK:?}"
    );
    run.finish().await.assert_all();
}

/// The gateway that coordinates the group crashes together with its node, as when their machine
/// loses power. When that node held the metadata primary, a view change comes first, and the new
/// primary starts the session timeout again. The node recovers from its log when it comes back.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[serial]
async fn given_a_short_session_timeout_when_the_coordinator_crashes_with_its_node_should_move_its_groups()
 {
    let Some(mut run) = Run::start(&SHORT_SESSION).await else {
        return;
    };
    let takeover = run.crash_coordinator(Crash::GatewayAndNode).await;
    assert!(
        takeover <= SHORT_SESSION_TIMEOUT + VIEW_CHANGE + TAKEOVER_SLACK,
        "the takeover took {takeover:?}, above the session timeout {SHORT_SESSION_TIMEOUT:?}, \
         a view change {VIEW_CHANGE:?} and {TAKEOVER_SLACK:?}"
    );
    run.finish().await.assert_all();
}

/// What crashes with SIGKILL in a crash test.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Crash {
    Gateway,
    /// The gateway and the node beside it.
    GatewayAndNode,
}

/// One test's deployment, under load from the clients, watched by the probe.
struct Run {
    deployment: Deployment,
    producer: Container,
    members: Vec<Container>,
    probe: Probe,
    /// The lines each member printed when the group settled. An assignment after that is a
    /// rebalance.
    settled: Vec<usize>,
    started: Instant,
    /// Last, so the processes stop before a failed test reports its logs.
    work_dir: WorkDir,
}

impl Run {
    /// Deploys the nodes with `settings` and their gateways, settles the group, then starts the
    /// probe and the producer. `None` when the host cannot run the clients.
    async fn start(settings: &[(&str, &str)]) -> Option<Self> {
        if let Some(reason) =
            missing_prerequisite(&[KAFKA_IMAGE, KCAT_IMAGE]).or_else(missing_server)
        {
            skip(&reason);
            return None;
        }
        let started = Instant::now();
        let work_dir = WorkDir::new();
        let deployment = Deployment::start(work_dir.path(), settings, &[(TOPIC, PARTITIONS)]).await;
        note(started, "deployed three nodes and three gateways");
        let bootstrap = deployment.bootstrap();
        let logs = deployment.log_dir().to_path_buf();
        let members: Vec<Container> = (1..=MEMBERS)
            .map(|member| start_member(&bootstrap, &logs.join(format!("member-{member}"))))
            .collect();
        let settled = settle(&members).await;
        note(started, "the group settled");
        let probe = Probe::start(deployment.addrs()).await;
        let producer = start_producer(&bootstrap, &logs);
        let run = Self {
            deployment,
            producer,
            members,
            probe,
            settled,
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

    /// The gateways in the order that a rolling restart takes them: the coordinator of the group
    /// first, so that the group moves at the first step.
    async fn rotation(&self) -> Vec<u16> {
        let first = self.coordinator().await;
        let mut ids = self.deployment.gateway_ids();
        let at = ids
            .iter()
            .position(|node_id| *node_id == first)
            .expect("the coordinator is a gateway");
        ids.rotate_left(at);
        ids
    }

    /// The gateway that coordinates the group, once every running gateway names the same one.
    async fn coordinator(&self) -> u16 {
        let addrs = self.deployment.running_addrs();
        wait_until(
            RECOVERY,
            PROBE_INTERVAL,
            "agreement on the coordinator",
            || async {
                let mut named = BTreeSet::new();
                for addr in &addrs {
                    named.insert(named_coordinator(*addr, GROUP).await?);
                }
                let node_id = *named.first()?;
                (named.len() == 1).then_some(u16::try_from(node_id).ok()?)
            },
        )
        .await
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
            let partition = partition_of_group(GROUP);
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

    /// Kills the gateway that coordinates the group, with its node for [`Crash::GatewayAndNode`],
    /// and restarts what it killed once another gateway took the group over. Returns how long
    /// after the kill the takeover came.
    async fn crash_coordinator(&mut self, crash: Crash) -> Duration {
        let node_id = self.coordinator().await;
        let node = Deployment::node_of(node_id);
        self.note(&format!(
            "SIGKILL to gateway {node_id}, the coordinator ({crash:?})"
        ));
        let crashed_at = Instant::now();
        self.deployment.kill_gateway(node_id);
        // A lost metadata primary stalls every partition until the next one is elected, since a
        // node refuses partition requests on metadata evidence older than 8 s.
        let data_path_limit = match crash {
            Crash::Gateway => DATA_PATH_LIMIT,
            Crash::GatewayAndNode => {
                self.deployment.cluster.crash(node);
                RECOVERY
            }
        };
        self.wait_for_acks("writes through the other gateways", data_path_limit)
            .await;
        let takeover = self.wait_served(crashed_at, node_id).await;
        self.note(&format!("its groups moved {takeover:?} after the crash"));
        if crash == Crash::GatewayAndNode {
            self.deployment.cluster.start(node).await;
        }
        self.deployment.start_gateway(node_id).await;
        self.wait_listed(node_id).await;
        if crash == Crash::GatewayAndNode {
            self.wait_primary_on(node).await;
        }
        self.wait_for_progress("the load after the crash").await;
        takeover
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

    /// Waits until the producer gets [`ACK_PROGRESS`] more acknowledgements, at most `limit`.
    async fn wait_for_acks(&self, what: &str, limit: Duration) {
        let before = self.producer.count(ACKED);
        wait_until(limit, PROBE_INTERVAL, what, || async {
            (self.producer.count(ACKED) >= before + ACK_PROGRESS).then_some(())
        })
        .await;
    }

    /// Waits until a gateway other than `gone` answers the probe member as its coordinator, in a
    /// round that started at `since` or later. Returns how long after `since` that round started.
    async fn wait_served(&self, since: Instant, gone: u16) -> Duration {
        let what = format!("a coordinator other than gateway {gone}");
        wait_until(RECOVERY, PROBE_INTERVAL, &what, || async {
            self.probe
                .first_served(since, usize::from(gone) - 1)
                .map(|at| at.saturating_duration_since(since))
        })
        .await
    }

    /// Waits until every running gateway lists gateway `node_id` in `Metadata`. A gateway lists
    /// another once that one owns coordinator partitions, so a restarted gateway is back then.
    async fn wait_listed(&self, node_id: u16) {
        let addrs = self.deployment.running_addrs();
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

    /// Stops the producer, waits until the group read everything it acknowledged, stops the
    /// probe and the members, and reads back what the partitions store.
    async fn finish(self) -> Outcome {
        let Self {
            deployment,
            mut producer,
            mut members,
            probe,
            settled,
            started,
            work_dir,
        } = self;
        note(started, "stopping the producer");
        producer.signal("TERM");
        assert!(
            producer.wait_exit(CLIENT_EXIT_LIMIT).await,
            "the producer exits after SIGTERM"
        );
        let produced = producer.with_stdout(Produced::parse);
        wait_until(
            RECOVERY,
            Duration::from_secs(1),
            "the group reading every acknowledged record",
            || async {
                let read: BTreeSet<u64> = members
                    .iter()
                    .flat_map(|member| member.with_stdout(read_values))
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
        let printed: Vec<Vec<String>> = members
            .iter()
            .map(|member| member.with_stdout(<[String]>::to_vec))
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
        note(started, "reading the partitions back with kcat");
        let stored = stored_records(&deployment.bootstrap()).await;
        let listings = broker_listings(&deployment).await;
        note(started, "done");
        Outcome {
            produced,
            stored,
            read: printed
                .iter()
                .flat_map(|lines| read_values(lines))
                .collect(),
            rebalances: rebalances(&printed, &settled),
            rounds,
            listings,
            _work_dir: work_dir,
        }
    }
}

/// What a test saw, checked once every client stopped.
struct Outcome {
    produced: Produced,
    /// Every stored record, as `(partition, offset, value)`.
    stored: Vec<(i64, i64, u64)>,
    /// The value of each record the group read, once per read.
    read: Vec<u64>,
    /// The assignment events that came after the group settled.
    rebalances: Vec<String>,
    rounds: Vec<Round>,
    /// What `kcat -L` printed through each gateway, with the line for each gateway it must list.
    listings: Vec<(String, Vec<String>)>,
    _work_dir: WorkDir,
}

impl Outcome {
    fn assert_all(&self) {
        self.assert_each_record_once();
        self.assert_no_rebalance();
        self.assert_one_coordinator_at_a_time();
        self.assert_every_gateway_listed();
    }

    /// Every acknowledged record is stored once, at the partition and offset that the producer
    /// got, and the group reads it once.
    fn assert_each_record_once(&self) {
        let errors = &self.produced.errors;
        assert!(
            errors.is_empty(),
            "{} sends failed: {errors:#?}",
            errors.len()
        );
        let acked = &self.produced.acked;
        assert!(!acked.is_empty(), "the producer must write records");
        let mut stored: BTreeMap<u64, Vec<(i64, i64)>> = BTreeMap::new();
        for &(partition, offset, value) in &self.stored {
            stored.entry(value).or_default().push((partition, offset));
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
            .filter(|(value, at)| stored.get(*value).is_none_or(|stored| stored != &[**at]))
            .map(|(value, at)| (value, at, stored.get(value)))
            .take(20)
            .collect();
        assert!(
            misplaced.is_empty(),
            "acknowledged records not stored where the producer was told, as value, acknowledged \
             (partition, offset) and stored ones: {misplaced:?}"
        );
        let mut reads: BTreeMap<u64, usize> = BTreeMap::new();
        for value in &self.read {
            *reads.entry(*value).or_default() += 1;
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
        let unread: Vec<&u64> = acked
            .keys()
            .filter(|value| !reads.contains_key(value))
            .take(20)
            .collect();
        assert!(
            unread.is_empty(),
            "acknowledged records the group never read: {unread:?}"
        );
        eprintln!("{} records acknowledged, stored and read once", acked.len());
    }

    /// The group keeps its members and generation: no member gets a new assignment, and the
    /// coordinator describes the group as stable every time.
    fn assert_no_rebalance(&self) {
        assert!(
            self.rebalances.is_empty(),
            "the group rebalanced after it settled: {:#?}",
            self.rebalances
        );
        let unstable: Vec<&(i16, String)> = self
            .rounds
            .iter()
            .flat_map(|round| round.descriptions.iter().flatten())
            .filter(|(code, state)| *code == ERROR_NONE && state != "Stable")
            .collect();
        assert!(
            unstable.is_empty(),
            "the coordinator described the group as not stable: {unstable:?}"
        );
    }

    /// No two gateways answer as coordinator of a group in one probe round, and no answer makes
    /// the probe member join again.
    fn assert_one_coordinator_at_a_time(&self) {
        assert!(
            self.rounds.len() >= MIN_ROUNDS,
            "the probe ran only {} rounds",
            self.rounds.len()
        );
        let coordinating = |answers: &[Option<i16>]| {
            answers
                .iter()
                .filter(|code| **code == Some(ERROR_NONE))
                .count()
        };
        for round in &self.rounds {
            let described: Vec<Option<i16>> = round
                .descriptions
                .iter()
                .map(|answer| answer.as_ref().map(|(code, _)| *code))
                .collect();
            assert!(
                coordinating(&round.heartbeats) <= 1 && coordinating(&described) <= 1,
                "two gateways answered as coordinator at once: {round:?}"
            );
            assert!(
                !round
                    .heartbeats
                    .iter()
                    .flatten()
                    .any(|code| REJOIN.contains(code)),
                "a gateway told the probe member to join again: {round:?}"
            );
        }
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

/// What the producer printed.
#[derive(Default)]
struct Produced {
    /// Where each acknowledged record went, by value, as `(partition, offset)`.
    acked: BTreeMap<u64, (i64, i64)>,
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
                        event["value"].as_str().and_then(|value| value.parse().ok()),
                        event["partition"].as_i64(),
                        event["offset"].as_i64(),
                    ) {
                        produced.acked.insert(value, (partition, offset));
                    }
                }
                Some("producer_send_error") => produced.errors.push(line.clone()),
                _ => {}
            }
        }
        produced
    }
}

/// The answers of one probe round, one per gateway in node id order. `None` where a gateway did
/// not answer.
#[derive(Debug, Clone)]
struct Round {
    at: Instant,
    /// The error code of the probe member's heartbeat.
    heartbeats: Vec<Option<i16>>,
    /// The error code and state of the Java group in `DescribeGroups`.
    descriptions: Vec<Option<(i16, String)>>,
}

/// A member of a second group that heartbeats at every gateway, and asks every gateway to describe
/// the Java group, until it stops.
struct Probe {
    rounds: Arc<Mutex<Vec<Round>>>,
    stop: CancellationToken,
    task: JoinHandle<()>,
}

impl Probe {
    /// Joins a member to a group in the coordinator partition of the Java group, then starts the
    /// rounds.
    async fn start(addrs: Vec<SocketAddr>) -> Self {
        let group = probe_group();
        let coordinator = wait_until(
            RECOVERY,
            PROBE_INTERVAL,
            "the coordinator of the probe group",
            || coordinator_addr(&addrs, &group),
        )
        .await;
        let (member_id, generation) = gateways::join_and_sync(coordinator, &group).await;
        let rounds = Arc::new(Mutex::new(Vec::new()));
        let stop = CancellationToken::new();
        let member = ProbeMember {
            group,
            member_id,
            generation,
        };
        let task = tokio::spawn(probe_rounds(
            addrs,
            member,
            Arc::clone(&rounds),
            stop.clone(),
        ));
        Self { rounds, stop, task }
    }

    /// When the first round at `since` or later started in which a gateway other than the one at
    /// index `gone` answered the probe member as its coordinator.
    fn first_served(&self, since: Instant, gone: usize) -> Option<Instant> {
        self.rounds
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .iter()
            .filter(|round| round.at >= since)
            .find(|round| {
                round
                    .heartbeats
                    .iter()
                    .enumerate()
                    .any(|(index, code)| index != gone && *code == Some(ERROR_NONE))
            })
            .map(|round| round.at)
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
    member: ProbeMember,
    rounds: Arc<Mutex<Vec<Round>>>,
    stop: CancellationToken,
) {
    let mut ticks = tokio::time::interval(PROBE_INTERVAL);
    ticks.set_missed_tick_behavior(MissedTickBehavior::Delay);
    loop {
        tokio::select! {
            () = stop.cancelled() => return,
            _ = ticks.tick() => {}
        }
        let at = Instant::now();
        let heartbeats = join_all(addrs.iter().map(|addr| heartbeat(*addr, &member))).await;
        let descriptions = join_all(addrs.iter().map(|addr| describe(*addr, GROUP))).await;
        rounds
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .push(Round {
                at,
                heartbeats,
                descriptions,
            });
    }
}

/// A group in the coordinator partition of the Java group, so one gateway coordinates both.
fn probe_group() -> String {
    let partition = partition_of_group(GROUP);
    // A thousand names cover the 16 partitions many times over.
    (0..1_000)
        .map(|n| format!("failover-probe-{n}"))
        .find(|group| partition_of_group(group) == partition)
        .expect("a group in every partition")
}

/// The test's directory. A failed test keeps the logs there, drops the node data, and prints where
/// they are with the end of each gateway log.
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
        for client in ["producer", "member-1", "member-2"] {
            let log = logs.join(format!("{client}.err"));
            report.push(format!("--- {client} stderr ---\n{}", tail(&log, 10)));
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
    loop {
        if let Some(value) = check().await {
            return value;
        }
        assert!(
            Instant::now() < deadline,
            "{what} did not happen within {limit:?}"
        );
        tokio::time::sleep(interval).await;
    }
}

/// A member of the group, which commits each batch after it reads it, prints a JSON event for each
/// step and one for each record.
fn start_member(bootstrap: &str, log: &Path) -> Container {
    Container::start(
        KAFKA_IMAGE,
        &[
            "/opt/kafka/bin/kafka-verifiable-consumer.sh",
            "--bootstrap-server",
            bootstrap,
            "--topic",
            TOPIC,
            "--group-id",
            GROUP,
            "--reset-policy",
            "earliest",
            "--assignment-strategy",
            "org.apache.kafka.clients.consumer.RangeAssignor",
            "--verbose",
        ],
        &[],
        CLIENT_LIFETIME,
        log,
    )
}

/// The idempotent producer, which writes the numbers from 0 as values until it gets SIGTERM, and
/// prints a JSON event for each record it sent.
fn start_producer(bootstrap: &str, logs: &Path) -> Container {
    let config = logs.join("producer.properties");
    fs::write(&config, PRODUCER_CONFIG).expect("write the producer config");
    let rate = RATE.to_string();
    Container::start(
        KAFKA_IMAGE,
        &[
            "/opt/kafka/bin/kafka-verifiable-producer.sh",
            "--bootstrap-server",
            bootstrap,
            "--topic",
            TOPIC,
            "--throughput",
            &rate,
            "--repeating-keys",
            KEYS,
            "--producer.config",
            "/tmp/producer.properties",
        ],
        &[(&config, "/tmp/producer.properties")],
        CLIENT_LIFETIME,
        &logs.join("producer"),
    )
}

/// Waits until each member's latest assignment is its share of the partitions, and returns how
/// many lines each had printed by then.
async fn settle(members: &[Container]) -> Vec<usize> {
    wait_until(
        RECOVERY,
        PROBE_INTERVAL,
        "the group's first assignment",
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
        .filter(|line| line.contains(r#""name":"partitions_assigned""#))
        .find_map(|line| parse_event(line))?;
    event["partitions"]
        .as_array()?
        .iter()
        .map(|partition| partition["partition"].as_i64())
        .collect()
}

/// The value of each record a member read, in the order read.
fn read_values(lines: &[String]) -> Vec<u64> {
    lines
        .iter()
        .filter(|line| line.contains(READ))
        .filter_map(|line| parse_event(line)?["value"].as_str()?.parse().ok())
        .collect()
}

/// The assignment events that each member printed between the settled group and its stop.
fn rebalances(printed: &[Vec<String>], settled: &[usize]) -> Vec<String> {
    printed
        .iter()
        .zip(settled)
        .enumerate()
        .flat_map(|(member, (lines, settled))| {
            lines[*settled..]
                .iter()
                .filter(|line| {
                    line.contains(r#""name":"partitions_assigned""#)
                        || line.contains(r#""name":"partitions_revoked""#)
                })
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
async fn stored_records(bootstrap: &str) -> Vec<(i64, i64, u64)> {
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
        .filter_map(|line| {
            let mut fields = line.split_whitespace();
            Some((
                fields.next()?.parse().ok()?,
                fields.next()?.parse().ok()?,
                fields.next()?.parse().ok()?,
            ))
        })
        .collect()
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

/// The node ids that the gateway at `addr` lists in `Metadata`.
async fn listed_brokers(addr: SocketAddr) -> Option<Vec<i32>> {
    let request = MetadataRequest::default().with_topics(Some(Vec::new()));
    let body = wire::encode(&request, METADATA_VERSION);
    let answer: MetadataResponse =
        exchange(addr, API_KEY_METADATA, METADATA_VERSION, &body).await?;
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
    let answer: FindCoordinatorResponse = exchange(
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

async fn heartbeat(addr: SocketAddr, member: &ProbeMember) -> Option<i16> {
    let request = HeartbeatRequest::default()
        .with_group_id(GroupId(StrBytes::from_string(member.group.clone())))
        .with_generation_id(member.generation)
        .with_member_id(member.member_id.clone());
    let body = wire::encode(&request, HEARTBEAT_VERSION);
    let answer: HeartbeatResponse =
        exchange(addr, API_KEY_HEARTBEAT, HEARTBEAT_VERSION, &body).await?;
    Some(answer.error_code)
}

async fn describe(addr: SocketAddr, group: &str) -> Option<(i16, String)> {
    let request = DescribeGroupsRequest::default()
        .with_groups(vec![GroupId(StrBytes::from_string(group.to_string()))]);
    let body = wire::encode(&request, DESCRIBE_GROUPS_VERSION);
    let answer: DescribeGroupsResponse = exchange(
        addr,
        API_KEY_DESCRIBE_GROUPS,
        DESCRIBE_GROUPS_VERSION,
        &body,
    )
    .await?;
    let described = answer.groups.first()?;
    Some((described.error_code, described.group_state.to_string()))
}

/// Sends one request on a connection of its own and decodes the answer. `None` when nothing
/// accepts at `addr`, the gateway closes the connection, or no answer comes within
/// [`PROBE_TIMEOUT`].
async fn exchange<A: Decodable>(
    addr: SocketAddr,
    key: i16,
    version: i16,
    body: &[u8],
) -> Option<A> {
    let frame = tcp::build_request_frame(key, version, 1, Some("failover-probe"), body);
    let answer = async {
        let mut stream = TcpStream::connect(addr).await.ok()?;
        stream.write_all(&frame).await.ok()?;
        let mut length = [0; 4];
        stream.read_exact(&mut length).await.ok()?;
        let mut payload = vec![0; usize::try_from(i32::from_be_bytes(length)).ok()?];
        stream.read_exact(&mut payload).await.ok()?;
        let (_, mut body) = tcp::parse_response_payload(key, version, Bytes::from(payload));
        A::decode(&mut body, version).ok()
    };
    tokio::time::timeout(PROBE_TIMEOUT, answer)
        .await
        .ok()
        .flatten()
}
