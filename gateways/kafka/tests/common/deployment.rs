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

//! Three Iggy nodes and a gateway process beside each, deployed as the gateway documentation
//! describes it. Gateway `n` dials node `n - 1`, and every gateway lists all of them in
//! `IGGY_KAFKA_BROKERS`. Every gateway runs with `IGGY_KAFKA_IDEMPOTENT_PRODUCERS=true`, so a batch
//! that an idempotent producer sends again through another gateway is written once. The gateways
//! run the compiled binary, so a stop is a SIGTERM and a crash a SIGKILL, as an orchestrator sends
//! them.
//!
//! Callers must also declare the `codec`, `gateways`, `iggy_server`, `tcp` and `wire` modules at
//! their crate root, as for `gateways.rs`.
#![allow(dead_code)]

use std::fs::{self, OpenOptions};
use std::net::SocketAddr;
use std::path::{Path, PathBuf};
use std::process::{Child, Command};
use std::time::Duration;

use iggy::prelude::{
    AutoLogin, Client, ClusterClient, ClusterNodeRole, Consumer, Credentials, Identifier,
    IggyClient, IggyClientBuilder, MessageClient, PollOptions, PollingStrategy, UserClient,
};
use iggy_binary_protocol::codes::GET_PARTITION_ROUTE_CODE;
use iggy_binary_protocol::requests::messages::GetPartitionRouteRequest;
use iggy_binary_protocol::responses::messages::PartitionRouteResponse;
use iggy_binary_protocol::{WireDecode, WireEncode, WireIdentifier};
use secrecy::SecretString;
use tokio::time::Instant;

use super::gateways;
use super::iggy_server::{PortGuard, TestCluster, graceful_kill};

/// Iggy nodes, each with a gateway beside it.
pub const NODES: usize = 3;

/// The stream of the Kafka topics under the default topic mapping.
pub const KAFKA_STREAM: &str = "kafka";

/// How long a gateway may take to listen. Its bridge waits up to 60 s for the cluster's metadata
/// primary (`IGGY_STARTUP_LIMIT` in `main.rs`).
const START_LIMIT: Duration = Duration::from_secs(70);

/// How long a gateway may take to exit after SIGTERM: the hand-over of its groups, the drain of
/// its connections and the sign-out of its Iggy clients. The default drain alone may take 25 s.
const EXIT_LIMIT: Duration = Duration::from_secs(40);

/// How long one question to the cluster may take.
const QUERY_LIMIT: Duration = Duration::from_secs(10);

/// The log lines of each gateway that a failure report shows.
pub const TAIL_LINES: usize = 40;

/// The nodes and their gateways, until the test drops it.
pub struct Deployment {
    /// Dropped first. A gateway's SIGTERM hands its groups over through Iggy, so the nodes must
    /// still run. Stopped after them, it waits for them and gets SIGKILL.
    gateways: Vec<GatewayProcess>,
    pub cluster: TestCluster,
    log_dir: PathBuf,
}

impl Deployment {
    /// Starts the nodes with `settings`, as `IGGY_*` variables, creates each `(topic,
    /// partitions)` of `topics`, then starts a gateway beside each node. The node data goes to
    /// `data/` in `work_dir`, and every log to `logs/`.
    pub async fn start(work_dir: &Path, settings: &[(&str, &str)], topics: &[(&str, u32)]) -> Self {
        let log_dir = work_dir.join("logs");
        fs::create_dir_all(&log_dir).expect("create the log dir");
        let cluster =
            TestCluster::spawn_with(&work_dir.join("data"), NODES, settings, Some(&log_dir)).await;
        let bridge = gateways::connect(cluster.bridge_config(0)).await;
        for &(topic, partitions) in topics {
            bridge
                .create_kafka_topic(topic, partitions)
                .await
                .expect("create a topic");
        }
        bridge.close().await.expect("close the bridge");

        let ports: Vec<PortGuard> = (0..NODES).map(|_| PortGuard::acquire()).collect();
        let brokers = (1_u16..)
            .zip(&ports)
            .map(|(node_id, port)| format!("{node_id}@127.0.0.1:{}", port.port))
            .collect::<Vec<_>>()
            .join(",");
        let mut gateways: Vec<GatewayProcess> = (1_u16..)
            .zip(ports)
            .enumerate()
            .map(|(node, (node_id, port))| GatewayProcess {
                node_id,
                addr: SocketAddr::from(([127, 0, 0, 1], port.port)),
                iggy_addr: cluster.address(node),
                brokers: brokers.clone(),
                log: log_dir.join(format!("gateway-{node_id}.log")),
                child: None,
                _port: port,
            })
            .collect();
        for gateway in &mut gateways {
            gateway.spawn();
        }
        for gateway in &mut gateways {
            gateway.wait_listening().await;
        }
        Self {
            gateways,
            cluster,
            log_dir,
        }
    }

    pub fn log_dir(&self) -> &Path {
        &self.log_dir
    }

    /// The node ids of the gateways, in order.
    pub fn gateway_ids(&self) -> Vec<u16> {
        self.gateways
            .iter()
            .map(|gateway| gateway.node_id)
            .collect()
    }

    /// The address of every gateway, stopped ones included, in node id order.
    pub fn addrs(&self) -> Vec<SocketAddr> {
        self.gateways.iter().map(|gateway| gateway.addr).collect()
    }

    /// The address of gateway `node_id`, also while it is stopped.
    pub fn addr_of(&self, node_id: u16) -> SocketAddr {
        self.gateways
            .iter()
            .find(|gateway| gateway.node_id == node_id)
            .expect("a listed gateway")
            .addr
    }

    /// The address of each running gateway.
    pub fn running_addrs(&self) -> Vec<SocketAddr> {
        self.gateways
            .iter()
            .filter(|gateway| gateway.child.is_some())
            .map(|gateway| gateway.addr)
            .collect()
    }

    /// `bootstrap.servers` for a client: every gateway, as the deployment guide says.
    pub fn bootstrap(&self) -> String {
        self.addrs()
            .iter()
            .map(ToString::to_string)
            .collect::<Vec<_>>()
            .join(",")
    }

    /// The node of gateway `node_id`: gateway `n` runs beside node `n - 1`.
    pub fn node_of(node_id: u16) -> usize {
        usize::from(node_id) - 1
    }

    /// Stops gateway `node_id` with SIGTERM and waits until it exits. Returns how long the exit
    /// took. Fails the test when the gateway is still running after [`EXIT_LIMIT`].
    pub async fn terminate_gateway(&mut self, node_id: u16) -> Duration {
        let gateway = self.gateway_mut(node_id);
        gateway.signal(libc::SIGTERM);
        gateway.wait_exit().await
    }

    /// Stops gateway `node_id` and its node with SIGTERM at once, as when their machine shuts
    /// down, and waits until both exit.
    pub async fn terminate_gateway_and_node(&mut self, node_id: u16) {
        self.gateway_mut(node_id).signal(libc::SIGTERM);
        self.cluster.stop(Self::node_of(node_id));
        self.gateway_mut(node_id).wait_exit().await;
    }

    /// Kills gateway `node_id` with SIGKILL, as a crash would. Its connections close, but Iggy
    /// keeps its sessions until their timeout.
    pub fn kill_gateway(&mut self, node_id: u16) {
        let gateway = self.gateway_mut(node_id);
        let mut child = gateway.child.take().expect("a running gateway");
        child.kill().expect("SIGKILL the gateway");
        child.wait().expect("reap the gateway");
    }

    /// Starts gateway `node_id` again on its port, and waits until it listens.
    pub async fn start_gateway(&mut self, node_id: u16) {
        let gateway = self.gateway_mut(node_id);
        gateway.spawn();
        gateway.wait_listening().await;
    }

    /// The replica id of the metadata primary, as a running node reports it, or `None` when no
    /// node answers.
    pub async fn metadata_primary(&self) -> Option<usize> {
        let client = self.client().await?;
        let metadata = tokio::time::timeout(QUERY_LIMIT, client.get_cluster_metadata()).await;
        sign_out(&client).await;
        let leader = metadata
            .ok()?
            .ok()?
            .nodes
            .into_iter()
            .find(|node| node.role == ClusterNodeRole::Leader)?;
        Some(usize::from(leader.replica_id))
    }

    /// The replica id of the primary of each of `partition_ids` of `topic` in `stream`, as a
    /// running node reports them, or `None` when no node answers.
    pub async fn primaries(
        &self,
        stream: &str,
        topic: &str,
        partition_ids: impl IntoIterator<Item = u32>,
    ) -> Option<Vec<usize>> {
        let client = self.client().await?;
        let routes = async {
            let mut primaries = Vec::new();
            for partition_id in partition_ids {
                let request = GetPartitionRouteRequest {
                    stream_id: WireIdentifier::named(stream).ok()?,
                    topic_id: WireIdentifier::named(topic).ok()?,
                    partition_id,
                }
                .to_bytes();
                let reply = tokio::time::timeout(
                    QUERY_LIMIT,
                    client.send_binary_request(GET_PARTITION_ROUTE_CODE, request),
                )
                .await
                .ok()?
                .ok()?;
                let route = PartitionRouteResponse::decode_from(&reply).ok()?;
                primaries.push(usize::from(route.primary_replica));
            }
            Some(primaries)
        }
        .await;
        sign_out(&client).await;
        routes
    }

    /// When Iggy stored the message at `offset` of partition `partition_id` of `topic` in `stream`:
    /// the clock of the partition's primary at the prepare, in microseconds since the Unix epoch.
    /// `None` when no node answers, or the partition holds no message at `offset`.
    pub async fn stored_at(
        &self,
        stream: &str,
        topic: &str,
        partition_id: u32,
        offset: u64,
    ) -> Option<u64> {
        let client = self.client().await?;
        let polled = async {
            tokio::time::timeout(
                QUERY_LIMIT,
                client.poll_messages(
                    &Identifier::named(stream).ok()?,
                    &Identifier::named(topic).ok()?,
                    partition_id,
                    &Consumer::default(),
                    &PollingStrategy::offset(offset),
                    PollOptions {
                        count: 1,
                        ..PollOptions::default()
                    },
                ),
            )
            .await
            .ok()?
            .ok()
        }
        .await;
        sign_out(&client).await;
        let message = polled?.messages.into_iter().next()?;
        (message.header.offset == offset).then_some(message.header.timestamp)
    }

    /// The last [`TAIL_LINES`] lines of the log of each gateway.
    pub fn gateway_log_tails(&self) -> String {
        self.gateways
            .iter()
            .map(|gateway| {
                format!(
                    "--- gateway {} ---\n{}",
                    gateway.node_id,
                    tail(&gateway.log, TAIL_LINES)
                )
            })
            .collect::<Vec<_>>()
            .join("\n")
    }

    fn gateway_mut(&mut self, node_id: u16) -> &mut GatewayProcess {
        self.gateways
            .iter_mut()
            .find(|gateway| gateway.node_id == node_id)
            .expect("a listed gateway")
    }

    /// A client signed in to the first running node that lets it in.
    async fn client(&self) -> Option<IggyClient> {
        let running = (0..self.cluster.node_count()).filter(|node| self.cluster.is_running(*node));
        for node in running {
            let client = IggyClientBuilder::new()
                .with_tcp()
                .with_server_address(self.cluster.address(node))
                .with_auto_sign_in(AutoLogin::Enabled(Credentials::UsernamePassword(
                    "iggy".to_string(),
                    SecretString::from("iggy"),
                )))
                .build()
                .ok()?;
            if matches!(
                tokio::time::timeout(QUERY_LIMIT, client.connect()).await,
                Ok(Ok(()))
            ) {
                return Some(client);
            }
            let _ = tokio::time::timeout(QUERY_LIMIT, client.shutdown()).await;
        }
        None
    }
}

/// The last `lines` lines of the file at `path`.
pub fn tail(path: &Path, lines: usize) -> String {
    let text = fs::read(path).map_or_else(
        |error| format!("cannot read {}: {error}", path.display()),
        |bytes| String::from_utf8_lossy(&bytes).into_owned(),
    );
    let all: Vec<&str> = text.lines().collect();
    all[all.len().saturating_sub(lines)..].join("\n")
}

/// Ends the session of `client` in Iggy, so the clients table keeps no slot for it.
async fn sign_out(client: &IggyClient) {
    let _ = tokio::time::timeout(QUERY_LIMIT, client.logout_user()).await;
    let _ = tokio::time::timeout(QUERY_LIMIT, client.shutdown()).await;
}

/// One `iggy-gateway-kafka` process, started again on the same port after a stop.
struct GatewayProcess {
    node_id: u16,
    addr: SocketAddr,
    iggy_addr: String,
    brokers: String,
    log: PathBuf,
    child: Option<Child>,
    _port: PortGuard,
}

impl GatewayProcess {
    fn spawn(&mut self) {
        assert!(self.child.is_none(), "gateway {} runs", self.node_id);
        let log = OpenOptions::new()
            .create(true)
            .append(true)
            .open(&self.log)
            .expect("open the gateway log");
        let mut command = Command::new(env!("CARGO_BIN_EXE_iggy-gateway-kafka"));
        // The gateway refuses to start on an unknown inherited IGGY_KAFKA_* variable, and a known
        // one would change what the test runs.
        command.env_clear();
        if let Some(llvm_profile_file) = std::env::var_os("LLVM_PROFILE_FILE") {
            command.env("LLVM_PROFILE_FILE", llvm_profile_file);
        }
        let child = command
            .env("IGGY_KAFKA_BIND_ADDR", self.addr.to_string())
            .env("IGGY_KAFKA_BRIDGE_ENABLED", "true")
            .env("IGGY_KAFKA_IGGY_ADDR", &self.iggy_addr)
            .env("IGGY_KAFKA_IGGY_USERNAME", "iggy")
            .env("IGGY_KAFKA_IGGY_PASSWORD", "iggy")
            .env("IGGY_KAFKA_NODE_ID", self.node_id.to_string())
            .env("IGGY_KAFKA_BROKERS", &self.brokers)
            .env("IGGY_KAFKA_COORDINATOR_LOG", "true")
            .env("IGGY_KAFKA_IDEMPOTENT_PRODUCERS", "true")
            .stdout(log.try_clone().expect("share the gateway log"))
            .stderr(log)
            .spawn()
            .expect("spawn iggy-gateway-kafka");
        self.child = Some(child);
    }

    /// Waits until the gateway accepts connections. It binds its listener once its bridge signed
    /// in, and joins the coordinator lease after that.
    async fn wait_listening(&mut self) {
        let deadline = Instant::now() + START_LIMIT;
        loop {
            let child = self.child.as_mut().expect("a started gateway");
            if let Some(status) = child.try_wait().expect("poll the gateway") {
                self.child = None;
                panic!(
                    "gateway {} exited during startup with {status}:\n{}",
                    self.node_id,
                    tail(&self.log, TAIL_LINES)
                );
            }
            if tokio::net::TcpStream::connect(self.addr).await.is_ok() {
                return;
            }
            assert!(
                Instant::now() < deadline,
                "gateway {} did not listen within {START_LIMIT:?}:\n{}",
                self.node_id,
                tail(&self.log, TAIL_LINES)
            );
            tokio::time::sleep(Duration::from_millis(100)).await;
        }
    }

    fn signal(&self, signal: libc::c_int) {
        let child = self.child.as_ref().expect("a running gateway");
        let pid = libc::pid_t::try_from(child.id()).expect("a pid");
        // SAFETY: `pid` is a child of this process that nothing reaped yet, since only `wait_exit`
        // and `kill_gateway` reap it, and both take it out of `self.child` first.
        let sent = unsafe { libc::kill(pid, signal) };
        assert_eq!(sent, 0, "signal gateway {}", self.node_id);
    }

    /// Waits until the gateway exits, and returns how long that took. Fails the test with its log
    /// when it still runs after [`EXIT_LIMIT`].
    async fn wait_exit(&mut self) -> Duration {
        let mut child = self.child.take().expect("a running gateway");
        let started = Instant::now();
        loop {
            if child.try_wait().expect("poll the gateway").is_some() {
                return started.elapsed();
            }
            if started.elapsed() > EXIT_LIMIT {
                let _ = child.kill();
                let _ = child.wait();
                panic!(
                    "gateway {} still ran {EXIT_LIMIT:?} after SIGTERM:\n{}",
                    self.node_id,
                    tail(&self.log, TAIL_LINES)
                );
            }
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
    }
}

impl Drop for GatewayProcess {
    fn drop(&mut self) {
        if let Some(mut child) = self.child.take() {
            graceful_kill(&mut child);
            let _ = child.wait();
        }
    }
}
