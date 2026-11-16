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

//! Real Kafka brokers as oracles for the gateway, and a wire client that talks to all of them.
//!
//! Two oracles, each a black box behind the Kafka wire:
//!
//! - Apache Kafka in its official container, `KRaft` single node ([`KafkaOracle`]). The default,
//!   and the one CI runs on every pull request.
//! - Redpanda in its official container ([`RedpandaOracle`]). Opt-in with
//!   `KAFKA_ORACLE_REDPANDA=1`, which the nightly job sets. Redpanda's source is BSL: only its
//!   container is used, never its code.
//!
//! [`WireClient`] encodes requests with `kafka-protocol`'s client half and negotiates each API's
//! version down to what the target advertises, so one scenario runs unchanged on every target.
#![allow(dead_code)]

use std::collections::BTreeMap;
use std::fmt;
use std::process::{Command, Stdio};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use bytes::{BufMut, Bytes, BytesMut};
use kafka_protocol::messages::{
    ApiKey, ApiVersionsRequest, MetadataRequest, RequestHeader, ResponseHeader,
};
use kafka_protocol::protocol::{Decodable, Encodable, HeaderVersion, Request, StrBytes};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;

/// The Apache Kafka oracle image. Pinned; CI pulls every `*_IMAGE` constant under `tests/`.
pub const KAFKA_ORACLE_IMAGE: &str = "apache/kafka:4.3.1";

/// The Redpanda oracle image. Deliberately not named `*_IMAGE`: pull requests do not pull it,
/// only the nightly job does.
pub const REDPANDA_ORACLE_TAG: &str = "redpandadata/redpanda:v26.1.18";

/// Client id every oracle request carries, so broker logs can tell harness traffic apart.
pub const CLIENT_ID: &str = "iggy-oracle";

/// Prefix of every container, topic, group and transactional id the harness creates.
pub const RESOURCE_PREFIX: &str = "iggy-oracle-";

/// Budget for a container broker to answer Metadata with a broker in it.
const ORACLE_READY_TIMEOUT: Duration = Duration::from_secs(120);

/// Budget for one request's response.
const RESPONSE_TIMEOUT: Duration = Duration::from_secs(30);

/// Largest response frame the harness accepts.
const MAX_RESPONSE_BYTES: usize = 64 * 1024 * 1024;

static NEXT_CONTAINER: AtomicUsize = AtomicUsize::new(0);

/// Which broker a target is. Only the gateway is the system under test.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum TargetKind {
    Gateway,
    Kafka,
    Redpanda,
}

impl TargetKind {
    #[must_use]
    pub const fn name(self) -> &'static str {
        match self {
            Self::Gateway => "gateway",
            Self::Kafka => "kafka",
            Self::Redpanda => "redpanda",
        }
    }
}

impl fmt::Display for TargetKind {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.name())
    }
}

/// One broker the scenarios run against, at a plaintext loopback `host:port`.
#[derive(Debug, Clone)]
pub struct Target {
    pub kind: TargetKind,
    pub address: String,
}

/// Why a target is not available, and whether that is fatal.
#[derive(Debug)]
pub enum Unavailable {
    /// Not asked for, or its prerequisite is absent. A skip, never a failure.
    Skipped(String),
    /// Asked for and broken.
    Failed(String),
}

/// Whether `name` is set to `1`.
#[must_use]
pub fn env_flag(name: &str) -> bool {
    std::env::var(name).as_deref() == Ok("1")
}

/// The container CLI, `docker` unless `KAFKA_ORACLE_CONTAINER_CLI` names another (`podman`).
fn container_cli() -> String {
    std::env::var("KAFKA_ORACLE_CONTAINER_CLI").unwrap_or_else(|_| "docker".to_string())
}

/// Whether the container CLI answers and `image` is in its local store. Containers run with
/// `--pull never`, so a missing image is a skip that names it rather than a slow pull mid-test.
pub fn image_ready(image: &str) -> Result<(), String> {
    let cli = container_cli();
    let present = Command::new(&cli)
        .args(["image", "inspect", image])
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .status()
        .map_err(|error| format!("`{cli}` is unavailable: {error}"))?;
    if present.success() {
        Ok(())
    } else {
        Err(format!(
            "image {image} is not pulled; run `{cli} pull {image}`"
        ))
    }
}

/// A free loopback port. The window between release and the broker's bind is acceptable here:
/// the oracle suites run in a serialized nextest group.
#[must_use]
pub fn free_port() -> u16 {
    std::net::TcpListener::bind("127.0.0.1:0")
        .and_then(|listener| listener.local_addr())
        .expect("bind an ephemeral port")
        .port()
}

/// A running container, removed on drop.
pub struct Container {
    name: String,
}

impl Container {
    /// Starts `image` detached on the host network with `env` and `args`.
    pub fn start(
        image: &str,
        label: &str,
        env: &[(String, String)],
        args: &[String],
    ) -> Result<Self, String> {
        let name = format!(
            "{RESOURCE_PREFIX}{label}-{}-{}",
            std::process::id(),
            NEXT_CONTAINER.fetch_add(1, Ordering::Relaxed)
        );
        let mut command = Command::new(container_cli());
        command.args([
            "run",
            "-d",
            "--rm",
            "--network",
            "host",
            "--pull",
            "never",
            "--name",
            &name,
        ]);
        for (key, value) in env {
            command.arg("-e").arg(format!("{key}={value}"));
        }
        command.arg(image).args(args);
        let output = command
            .output()
            .map_err(|error| format!("start {label}: {error}"))?;
        if !output.status.success() {
            return Err(format!(
                "start {label}: {}",
                String::from_utf8_lossy(&output.stderr).trim()
            ));
        }
        Ok(Self { name })
    }

    /// The container's recent output, for a failure message.
    #[must_use]
    pub fn logs_tail(&self) -> String {
        Command::new(container_cli())
            .args(["logs", "--tail", "40", &self.name])
            .output()
            .map_or_default(|output| {
                let mut text = String::from_utf8_lossy(&output.stdout).into_owned();
                text.push_str(&String::from_utf8_lossy(&output.stderr));
                text
            })
    }
}

impl Drop for Container {
    fn drop(&mut self) {
        let _ = Command::new(container_cli())
            .args(["rm", "-f", &self.name])
            .stdout(Stdio::null())
            .stderr(Stdio::null())
            .status();
    }
}

/// Polls `address` until Metadata lists a broker, or the budget runs out.
async fn wait_for_broker(address: &str, container: &Container) -> Result<(), String> {
    let deadline = Instant::now() + ORACLE_READY_TIMEOUT;
    let mut last = String::new();
    while Instant::now() < deadline {
        match WireClient::connect(address).await {
            Ok(mut client) => {
                let version = client.version(ApiKey::Metadata, 9).unwrap_or(1);
                match client
                    .send(
                        &MetadataRequest::default().with_topics(Some(Vec::new())),
                        version,
                    )
                    .await
                {
                    Ok(response) if !response.brokers.is_empty() => return Ok(()),
                    Ok(_) => last = "metadata lists no broker yet".to_string(),
                    Err(error) => last = error,
                }
            }
            Err(error) => last = error,
        }
        tokio::time::sleep(Duration::from_millis(500)).await;
    }
    Err(format!(
        "broker at {address} not ready within {ORACLE_READY_TIMEOUT:?} ({last}); logs:\n{}",
        container.logs_tail()
    ))
}

/// Apache Kafka, `KRaft` combined mode, one node, on loopback.
pub struct KafkaOracle {
    pub target: Target,
    _container: Container,
}

impl KafkaOracle {
    pub async fn start() -> Result<Self, Unavailable> {
        image_ready(KAFKA_ORACLE_IMAGE).map_err(Unavailable::Skipped)?;
        let port = free_port();
        let controller = free_port();
        let address = format!("127.0.0.1:{port}");
        let env: Vec<(String, String)> = [
            ("KAFKA_NODE_ID", "1".to_string()),
            ("KAFKA_PROCESS_ROLES", "broker,controller".to_string()),
            (
                "KAFKA_LISTENERS",
                format!("PLAINTEXT://127.0.0.1:{port},CONTROLLER://127.0.0.1:{controller}"),
            ),
            (
                "KAFKA_ADVERTISED_LISTENERS",
                format!("PLAINTEXT://{address}"),
            ),
            ("KAFKA_CONTROLLER_LISTENER_NAMES", "CONTROLLER".to_string()),
            (
                "KAFKA_LISTENER_SECURITY_PROTOCOL_MAP",
                "CONTROLLER:PLAINTEXT,PLAINTEXT:PLAINTEXT".to_string(),
            ),
            (
                "KAFKA_CONTROLLER_QUORUM_VOTERS",
                format!("1@127.0.0.1:{controller}"),
            ),
            ("KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR", "1".to_string()),
            ("KAFKA_OFFSETS_TOPIC_NUM_PARTITIONS", "1".to_string()),
            (
                "KAFKA_TRANSACTION_STATE_LOG_REPLICATION_FACTOR",
                "1".to_string(),
            ),
            ("KAFKA_TRANSACTION_STATE_LOG_MIN_ISR", "1".to_string()),
            (
                "KAFKA_SHARE_COORDINATOR_STATE_TOPIC_REPLICATION_FACTOR",
                "1".to_string(),
            ),
            (
                "KAFKA_SHARE_COORDINATOR_STATE_TOPIC_MIN_ISR",
                "1".to_string(),
            ),
            ("KAFKA_GROUP_INITIAL_REBALANCE_DELAY_MS", "0".to_string()),
            ("KAFKA_AUTO_CREATE_TOPICS_ENABLE", "false".to_string()),
            ("KAFKA_HEAP_OPTS", "-Xmx512m -Xms256m".to_string()),
        ]
        .into_iter()
        .map(|(key, value)| (key.to_string(), value))
        .collect();
        let container = Container::start(KAFKA_ORACLE_IMAGE, "kafka", &env, &[])
            .map_err(Unavailable::Failed)?;
        wait_for_broker(&address, &container)
            .await
            .map_err(Unavailable::Failed)?;
        Ok(Self {
            target: Target {
                kind: TargetKind::Kafka,
                address,
            },
            _container: container,
        })
    }
}

/// Redpanda in dev-container mode, one shard, on loopback.
pub struct RedpandaOracle {
    pub target: Target,
    _container: Container,
}

impl RedpandaOracle {
    pub async fn start() -> Result<Self, Unavailable> {
        if !env_flag("KAFKA_ORACLE_REDPANDA") {
            return Err(Unavailable::Skipped(
                "Redpanda runs nightly; set KAFKA_ORACLE_REDPANDA=1".to_string(),
            ));
        }
        image_ready(REDPANDA_ORACLE_TAG).map_err(Unavailable::Failed)?;
        let port = free_port();
        let rpc = free_port();
        let proxy = free_port();
        let registry = free_port();
        let admin = free_port();
        let address = format!("127.0.0.1:{port}");
        let args: Vec<String> = [
            "redpanda".to_string(),
            "start".to_string(),
            "--mode".to_string(),
            "dev-container".to_string(),
            "--smp".to_string(),
            "1".to_string(),
            "--memory".to_string(),
            "1G".to_string(),
            "--overprovisioned".to_string(),
            "--node-id".to_string(),
            "0".to_string(),
            "--kafka-addr".to_string(),
            format!("PLAINTEXT://{address}"),
            "--advertise-kafka-addr".to_string(),
            format!("PLAINTEXT://{address}"),
            "--rpc-addr".to_string(),
            format!("127.0.0.1:{rpc}"),
            "--advertise-rpc-addr".to_string(),
            format!("127.0.0.1:{rpc}"),
            "--pandaproxy-addr".to_string(),
            format!("127.0.0.1:{proxy}"),
            "--schema-registry-addr".to_string(),
            format!("127.0.0.1:{registry}"),
            // No spaces: the image's entrypoint splits its arguments again.
            "--set".to_string(),
            format!(r#"redpanda.admin=[{{"address":"127.0.0.1","port":{admin}}}]"#),
        ]
        .into();
        let container = Container::start(REDPANDA_ORACLE_TAG, "redpanda", &[], &args)
            .map_err(Unavailable::Failed)?;
        wait_for_broker(&address, &container)
            .await
            .map_err(Unavailable::Failed)?;
        Ok(Self {
            target: Target {
                kind: TargetKind::Redpanda,
                address,
            },
            _container: container,
        })
    }
}

#[must_use]
pub fn unix_now() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_or(0, |elapsed| elapsed.as_secs())
}

/// A run id, unique per process start, for resource names on shared oracles.
#[must_use]
pub fn run_id() -> String {
    let nanos = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_or(0, |elapsed| elapsed.subsec_nanos());
    format!("{}-{:x}", unix_now(), nanos ^ std::process::id())
}

/// One Kafka connection that speaks to any target.
pub struct WireClient {
    io: TcpStream,
    next_correlation: i32,
    /// `(min, max)` per API key, from the target's `ApiVersions`.
    versions: BTreeMap<i16, (i16, i16)>,
}

impl WireClient {
    /// Connects to `address` and reads the target's versions.
    pub async fn connect(address: &str) -> Result<Self, String> {
        let io = tokio::time::timeout(RESPONSE_TIMEOUT, TcpStream::connect(address))
            .await
            .map_err(|_| format!("connect {address}: timed out"))?
            .map_err(|error| format!("connect {address}: {error}"))?;
        let mut client = Self {
            io,
            next_correlation: 1,
            versions: BTreeMap::new(),
        };
        let response = client
            .send(
                &ApiVersionsRequest::default()
                    .with_client_software_name(StrBytes::from_static_str(CLIENT_ID))
                    .with_client_software_version(StrBytes::from_static_str("1")),
                3,
            )
            .await?;
        if response.error_code != 0 {
            return Err(format!("ApiVersions answered {}", response.error_code));
        }
        client.versions = response
            .api_keys
            .iter()
            .map(|key| (key.api_key, (key.min_version, key.max_version)))
            .collect();
        Ok(client)
    }

    /// The advertised `(min, max)` of every key.
    #[must_use]
    pub const fn advertised(&self) -> &BTreeMap<i16, (i16, i16)> {
        &self.versions
    }

    /// `preferred`, lowered to what the target advertises; `None` when it does not serve `key`.
    #[must_use]
    pub fn version(&self, key: ApiKey, preferred: i16) -> Option<i16> {
        let (min, max) = *self.versions.get(&(key as i16))?;
        let version = preferred.min(max);
        (version >= min).then_some(version)
    }

    /// Sends `request` at `version` and decodes its response.
    pub async fn send<R: Request + Sync>(
        &mut self,
        request: &R,
        version: i16,
    ) -> Result<R::Response, String> {
        let correlation_id = self.next_correlation;
        self.next_correlation += 1;
        let header = RequestHeader::default()
            .with_request_api_key(R::KEY)
            .with_request_api_version(version)
            .with_correlation_id(correlation_id)
            .with_client_id(Some(StrBytes::from_static_str(CLIENT_ID)));
        let mut body = BytesMut::new();
        header
            .encode(&mut body, R::header_version(version))
            .map_err(|error| format!("encode header for key {}: {error}", R::KEY))?;
        request
            .encode(&mut body, version)
            .map_err(|error| format!("encode key {} v{version}: {error}", R::KEY))?;
        let length = i32::try_from(body.len()).map_err(|_| "request too large".to_string())?;
        let mut frame = BytesMut::with_capacity(body.len() + 4);
        frame.put_i32(length);
        frame.extend_from_slice(&body);
        let mut payload = tokio::time::timeout(RESPONSE_TIMEOUT, async {
            self.io
                .write_all(&frame)
                .await
                .map_err(|error| format!("write key {}: {error}", R::KEY))?;
            self.io
                .flush()
                .await
                .map_err(|error| format!("flush key {}: {error}", R::KEY))?;
            read_frame(&mut self.io).await
        })
        .await
        .map_err(|_| {
            format!(
                "key {} v{version}: no response within {RESPONSE_TIMEOUT:?}",
                R::KEY
            )
        })??;
        let response_header =
            ResponseHeader::decode(&mut payload, R::Response::header_version(version))
                .map_err(|error| format!("decode response header for key {}: {error}", R::KEY))?;
        if response_header.correlation_id != correlation_id {
            return Err(format!(
                "key {}: correlation id {} answered {correlation_id}",
                R::KEY,
                response_header.correlation_id
            ));
        }
        R::Response::decode(&mut payload, version)
            .map_err(|error| format!("decode key {} v{version}: {error}", R::KEY))
    }
}

async fn read_frame(io: &mut TcpStream) -> Result<Bytes, String> {
    let mut length = [0u8; 4];
    io.read_exact(&mut length)
        .await
        .map_err(|error| format!("read response length: {error}"))?;
    let length = usize::try_from(i32::from_be_bytes(length))
        .map_err(|_| "negative response length".to_string())?;
    if length > MAX_RESPONSE_BYTES {
        return Err(format!(
            "response of {length} bytes is over the harness cap"
        ));
    }
    let mut payload = vec![0u8; length];
    io.read_exact(&mut payload)
        .await
        .map_err(|error| format!("read response body: {error}"))?;
    Ok(Bytes::from(payload))
}
