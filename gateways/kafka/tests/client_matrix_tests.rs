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

//! The client compatibility matrix (#4531): real Kafka clients, each run against the gateway
//! and against Apache Kafka, with what each observed compared fact by fact.
//!
//! Every client runs one scenario: produce a batch of keyed records with headers, consume it in a
//! group, commit, close; produce a second batch, start a new consumer in the same group and check
//! it resumes where the first committed. The probes live in `tests/clients/` and print the event
//! lines `tests/common/matrix.rs` describes. A difference fails the run unless
//! `tests/clients/allowed_differences.toml` declares it with the issue that owns it.
//!
//! - **Smoke** (every pull request): kcat 1.7 (librdkafka 1.7), confluent-kafka-python
//!   (librdkafka 2.x), franz-go, and the Java client 3.9 and 4.3.
//! - **Nightly** (`KAFKA_CLIENT_MATRIX_FULL=1`): Vector, Fluent Bit, `RisingWave`, Flink and
//!   Kafka Connect with Debezium, in `client_matrix_nightly` below.
//!
//! The matrix is printed, and written to `$KAFKA_CLIENT_MATRIX_DIR` as Markdown for the CI job
//! summary. Missing Docker, images, a JDK or the `iggy-server` binary skip the suite, unless
//! `KAFKA_E2E_REQUIRED=1` turns the skip into a failure.

use std::collections::{BTreeMap, BTreeSet};
use std::fmt::Write as _;
use std::sync::Arc;
use std::time::{Duration, Instant};

use kafka_protocol::messages::create_topics_request::{CreatableTopic, CreatableTopicConfig};
use kafka_protocol::messages::metadata_request::MetadataRequestTopic;
use kafka_protocol::messages::{ApiKey, CreateTopicsRequest, MetadataRequest, TopicName};
use kafka_protocol::protocol::StrBytes;
use tokio::sync::broadcast;

use iggy_gateway_kafka::bridge::IggyBridge;
use iggy_gateway_kafka::listener::{ListenerConfig, SecurityProtocol};
use iggy_gateway_kafka::server::{BoundListener, bind_listener};
use iggy_gateway_kafka::{GatewayConfig, KafkaGateway};

#[path = "common/facts.rs"]
mod facts;
#[path = "common/iggy_server.rs"]
mod iggy_server;
#[path = "common/matrix.rs"]
mod matrix;
#[path = "common/oracle.rs"]
mod oracle;

use facts::{Allowed, Facts, Verdict, parse_allowed};
use matrix::{
    CLIENT_TIMEOUT, COUNT, Event, PARTITIONS, Row, Run, Values, cache_dir, clients_dir, digest,
    facts_from_events, host_user_args, judge_client, parse_events, path_str, publish, render, row,
    run_container, unescape,
};
use oracle::{
    KAFKA_ORACLE_IMAGE, KafkaOracle, Target, TargetKind, Unavailable, WireClient, env_flag,
    image_ready, run_id,
};

const ALLOWED_DIFFERENCES: &str = include_str!("clients/allowed_differences.toml");

/// kcat's last official image; it links librdkafka 1.7.
pub const KCAT_MATRIX_IMAGE: &str = "docker.io/edenhill/kcat:1.7.1@sha256:8f16a5fed099931ce1122420b7473efe467ff9841d53680b99db25dd1723d711";

/// Runs the confluent-kafka-python probe; the package comes from `PyPI` at `CONFLUENT_KAFKA_PYTHON`.
pub const PYTHON_IMAGE: &str = "docker.io/library/python:3.13.17-slim-trixie@sha256:fe14fd57a4c0361110eaa0c284112b189750bcb43fb04bb5d1ee7b7744027ccf";

/// confluent-kafka-python, whose wheels bundle librdkafka of the same version.
const CONFLUENT_KAFKA_PYTHON: &str = "2.16.0";

/// Builds and runs the franz-go probe (`tests/clients/franz/go.mod` pins franz-go).
pub const GOLANG_IMAGE: &str = "docker.io/library/golang:1.26.10-trixie@sha256:909771fef53e3dcb0950255f0c9a8b4728c3b841e61509c7b29b098de7ee02d4";

/// The Java client 3.9, from Apache Kafka's own image.
pub const JAVA_39_IMAGE: &str = "docker.io/apache/kafka:3.9.0@sha256:fbc7d7c428e3755cf36518d4976596002477e4c052d1f80b5b9eafd06d0fff2f";

/// The Java client 4.3, from the same image as the Kafka oracle, by digest.
pub const JAVA_43_IMAGE: &str = "docker.io/apache/kafka:4.3.1@sha256:77e3df9054047a88b520d0cc46e16696d3b22022e1d580aeccd2632df6532837";

/// Request timeout for the topics the matrix creates.
const TIMEOUT_MS: i32 = 30_000;

/// Skips, or fails under `KAFKA_E2E_REQUIRED=1`.
fn skip(reason: &str) {
    assert!(
        !env_flag("KAFKA_E2E_REQUIRED"),
        "KAFKA_E2E_REQUIRED=1 but the client matrix cannot run: {reason}"
    );
    eprintln!("skipping the client matrix: {reason}");
}

fn iggy_server_built() -> bool {
    let name = format!("iggy-server{}", std::env::consts::EXE_SUFFIX);
    std::env::current_exe()
        .is_ok_and(|exe| exe.ancestors().skip(1).any(|dir| dir.join(&name).is_file()))
}

/// The gateway, bridged to a spawned `iggy-server`, until dropped.
struct Gateway {
    target: Target,
    _shutdown: broadcast::Sender<()>,
    _server: iggy_server::TestServer,
    _data_dir: tempfile::TempDir,
}

async fn start_gateway() -> Gateway {
    let data_dir = tempfile::tempdir().expect("create the server data dir");
    let server = iggy_server::TestServer::spawn(data_dir.path()).await;
    let bridge = IggyBridge::connect(server.test_config())
        .await
        .expect("the bridge connects to a ready server");
    let listener = bind_listener("127.0.0.1:0").expect("bind an ephemeral port");
    let address = listener.local_addr().expect("local address").to_string();
    let listener = BoundListener::new(
        ListenerConfig::new(SecurityProtocol::Plaintext, address.clone()),
        listener,
    );
    let mut config = GatewayConfig::default();
    // As the oracle runs: Kafka's group.initial.rebalance.delay.ms is 0 there.
    config.group.initial_rebalance_delay = Duration::ZERO;
    let gateway = KafkaGateway::new(config).with_bridge(Some(Arc::new(bridge)));
    let (shutdown, receiver) = broadcast::channel(1);
    tokio::spawn(async move {
        let _ = gateway.run(vec![listener], receiver).await;
    });
    Gateway {
        target: Target {
            kind: TargetKind::Gateway,
            address,
        },
        _shutdown: shutdown,
        _server: server,
        _data_dir: data_dir,
    }
}

/// The two brokers every client runs against, until dropped.
struct Brokers {
    gateway: Gateway,
    kafka: KafkaOracle,
    run: String,
}

impl Brokers {
    /// Both targets, gateway first.
    fn targets(&self) -> [Target; 2] {
        [self.gateway.target.clone(), self.kafka.target.clone()]
    }

    /// A resource name unique to this run and client.
    fn name(&self, client: &str, suffix: &str) -> String {
        format!("matrix-{}-{client}-{suffix}", self.run)
    }
}

/// Starts both brokers, or reports why the matrix cannot run (after `skip`).
async fn brokers() -> Option<Brokers> {
    if !cfg!(target_os = "linux") {
        skip("containers reach the host loopback with --network host only on Linux");
        return None;
    }
    if !iggy_server_built() {
        skip("iggy-server is not built; run `cargo build --bin iggy-server` first");
        return None;
    }
    let kafka = match KafkaOracle::start().await {
        Ok(kafka) => kafka,
        Err(Unavailable::Skipped(reason)) => {
            skip(&reason);
            return None;
        }
        Err(Unavailable::Failed(reason)) => panic!("Apache Kafka oracle: {reason}"),
    };
    let gateway = start_gateway().await;
    Some(Brokers {
        gateway,
        kafka,
        run: run_id(),
    })
}

/// Creates `name` with `PARTITIONS` partitions and `configs` on `target`, and waits for leaders.
async fn create_topic(target: &Target, name: &str, configs: &[(&str, &str)]) -> Result<(), String> {
    let mut client = WireClient::connect(&target.address).await?;
    let request = CreateTopicsRequest::default()
        .with_timeout_ms(TIMEOUT_MS)
        .with_topics(vec![
            CreatableTopic::default()
                .with_name(TopicName(StrBytes::from_string(name.to_string())))
                .with_num_partitions(PARTITIONS)
                .with_replication_factor(-1)
                .with_configs(
                    configs
                        .iter()
                        .map(|(key, value)| {
                            CreatableTopicConfig::default()
                                .with_name(StrBytes::from_string((*key).to_string()))
                                .with_value(Some(StrBytes::from_string((*value).to_string())))
                        })
                        .collect(),
                ),
        ]);
    let version = client
        .version(ApiKey::CreateTopics, 5)
        .ok_or("the target does not serve CreateTopics")?;
    let response = client.send(&request, version).await?;
    let error = response.topics.first().map_or(-1, |topic| topic.error_code);
    if error != 0 {
        return Err(format!(
            "CreateTopics {name} on {} answered {error}",
            target.kind
        ));
    }
    for _ in 0..120 {
        let request = MetadataRequest::default()
            .with_topics(Some(vec![MetadataRequestTopic::default().with_name(Some(
                TopicName(StrBytes::from_string(name.to_string())),
            ))]))
            .with_allow_auto_topic_creation(false);
        let version = client.version(ApiKey::Metadata, 9).unwrap_or(1);
        let response = client.send(&request, version).await?;
        let ready = response.topics.first().is_some_and(|topic| {
            topic.error_code == 0
                && topic.partitions.len() == usize::try_from(PARTITIONS).unwrap_or(0)
                && topic
                    .partitions
                    .iter()
                    .all(|partition| partition.error_code == 0 && partition.leader_id.0 >= 0)
        });
        if ready {
            return Ok(());
        }
        tokio::time::sleep(Duration::from_millis(250)).await;
    }
    Err(format!("topic {name} on {} never got leaders", target.kind))
}

/// Facts every client's run on Apache Kafka must show; anything else means the probe, not the
/// gateway, is broken.
fn oracle_sane(facts: &Facts) -> Result<(), String> {
    let count = COUNT.to_string();
    let mut wrong = Vec::new();
    for (fact, want) in [
        ("done", "true"),
        ("consume.count", count.as_str()),
        ("consume.intact", count.as_str()),
        ("resume.count", count.as_str()),
        ("resume.intact", count.as_str()),
        ("resume.first_index", count.as_str()),
    ] {
        if facts.get(fact) != Some(want) {
            wrong.push(format!("{fact} = {:?}, want {want}", facts.get(fact)));
        }
    }
    if wrong.is_empty() {
        Ok(())
    } else {
        Err(wrong.join("; "))
    }
}

/// The smoke clients.
#[derive(Debug, Clone, Copy)]
enum Smoke {
    Kcat,
    ConfluentPython,
    FranzGo,
    Java39,
    Java43,
}

impl Smoke {
    const ALL: [Self; 5] = [
        Self::Kcat,
        Self::ConfluentPython,
        Self::FranzGo,
        Self::Java39,
        Self::Java43,
    ];

    const fn name(self) -> &'static str {
        match self {
            Self::Kcat => "kcat",
            Self::ConfluentPython => "confluent-kafka-python",
            Self::FranzGo => "franz-go",
            Self::Java39 => "java-3.9",
            Self::Java43 => "java-4.3",
        }
    }

    fn version(self) -> String {
        match self {
            Self::Kcat => "kcat 1.7.1, librdkafka 1.7.0".to_string(),
            Self::ConfluentPython => {
                format!(
                    "confluent-kafka {CONFLUENT_KAFKA_PYTHON} (librdkafka {CONFLUENT_KAFKA_PYTHON})"
                )
            }
            Self::FranzGo => format!("franz-go {}", franz_version()),
            Self::Java39 => "kafka-clients 3.9.0".to_string(),
            Self::Java43 => "kafka-clients 4.3.1".to_string(),
        }
    }

    const fn image(self) -> &'static str {
        match self {
            Self::Kcat => KCAT_MATRIX_IMAGE,
            Self::ConfluentPython => PYTHON_IMAGE,
            Self::FranzGo => GOLANG_IMAGE,
            Self::Java39 => JAVA_39_IMAGE,
            Self::Java43 => JAVA_43_IMAGE,
        }
    }

    /// Builds or fetches what the probe needs; returns the `docker run` arguments after the
    /// environment (mounts, image, command).
    fn prepare(self) -> Result<Vec<String>, String> {
        let mount = |host: &std::path::Path, guest: &str| {
            vec!["-v".to_string(), format!("{}:{guest}:ro", path_str(host))]
        };
        let mut args = Vec::new();
        match self {
            Self::Kcat => {
                args.extend(mount(&clients_dir().join("kcat"), "/probe"));
                args.extend(["--entrypoint".into(), "sh".into(), self.image().into()]);
                args.push("/probe/probe.sh".into());
            }
            Self::ConfluentPython => {
                let deps = python_deps()?;
                args.extend(mount(&deps, "/deps"));
                args.extend(mount(&clients_dir().join("python"), "/probe"));
                args.extend(["-e".into(), "PYTHONPATH=/deps".into(), self.image().into()]);
                args.extend(["python".into(), "/probe/probe.py".into(), "scenario".into()]);
            }
            Self::FranzGo => {
                let binary = franz_probe()?;
                args.extend(mount(binary.parent().ok_or("probe dir")?, "/probe"));
                args.push(self.image().into());
                args.push(format!(
                    "/probe/{}",
                    binary
                        .file_name()
                        .and_then(|name| name.to_str())
                        .ok_or("probe name")?
                ));
            }
            Self::Java39 | Self::Java43 => {
                let classes = java_probe(self.image())?;
                args.extend(mount(&classes, "/probe"));
                args.extend(["--entrypoint".into(), "java".into(), self.image().into()]);
                args.extend([
                    "-cp".into(),
                    "/opt/kafka/libs/*:/probe".into(),
                    "ClientMatrixProbe".into(),
                ]);
            }
        }
        Ok(args)
    }
}

/// franz-go's version, as `tests/clients/franz/go.mod` requires it.
fn franz_version() -> String {
    include_str!("clients/franz/go.mod")
        .lines()
        .find_map(|line| line.trim().strip_prefix("github.com/twmb/franz-go v"))
        .unwrap_or("?")
        .to_string()
}

/// confluent-kafka-python installed from `PyPI` into the matrix cache, once per version.
fn python_deps() -> Result<std::path::PathBuf, String> {
    let dir = cache_dir(&format!("python-confluent-kafka-{CONFLUENT_KAFKA_PYTHON}"));
    if dir.join("confluent_kafka").is_dir() {
        return Ok(dir);
    }
    let mut args = host_user_args();
    args.extend([
        "-v".to_string(),
        format!("{}:/deps", path_str(&dir)),
        "-e".to_string(),
        "HOME=/tmp".to_string(),
        PYTHON_IMAGE.to_string(),
        "pip".to_string(),
        "install".to_string(),
        "--quiet".to_string(),
        "--no-cache-dir".to_string(),
        "--disable-pip-version-check".to_string(),
        "--root-user-action=ignore".to_string(),
        "--only-binary=:all:".to_string(),
        "--target".to_string(),
        "/deps".to_string(),
        format!("confluent-kafka=={CONFLUENT_KAFKA_PYTHON}"),
    ]);
    let run = run_container("pip", &args, Duration::from_secs(300));
    if run.ok && dir.join("confluent_kafka").is_dir() {
        Ok(dir)
    } else {
        let _ = std::fs::remove_dir_all(&dir);
        Err(format!(
            "pip install confluent-kafka failed: {}",
            run.tail()
        ))
    }
}

/// The franz-go probe, built in the pinned Go image, cached by the digest of its sources.
fn franz_probe() -> Result<std::path::PathBuf, String> {
    let source = clients_dir().join("franz");
    let read = |file: &str| std::fs::read(source.join(file)).unwrap_or_default();
    let key = digest(&[
        &read("main.go"),
        &read("go.mod"),
        &read("go.sum"),
        GOLANG_IMAGE.as_bytes(),
    ]);
    let out = cache_dir("franz");
    let binary = out.join(format!("franz-probe-{key}"));
    if binary.is_file() {
        return Ok(binary);
    }
    let go_cache = cache_dir("go");
    let mut args = host_user_args();
    args.extend([
        "-v".to_string(),
        format!("{}:/src:ro", path_str(&source)),
        "-v".to_string(),
        format!("{}:/out", path_str(&out)),
        "-v".to_string(),
        format!("{}:/gocache", path_str(&go_cache)),
        "-w".to_string(),
        "/src".to_string(),
        "-e".to_string(),
        "HOME=/tmp".to_string(),
        "-e".to_string(),
        "GOCACHE=/gocache/build".to_string(),
        "-e".to_string(),
        "GOMODCACHE=/gocache/mod".to_string(),
        "-e".to_string(),
        "GOFLAGS=-mod=readonly -modcacherw".to_string(),
        "-e".to_string(),
        "CGO_ENABLED=0".to_string(),
        GOLANG_IMAGE.to_string(),
        "go".to_string(),
        "build".to_string(),
        "-trimpath".to_string(),
        "-o".to_string(),
        format!("/out/franz-probe-{key}"),
        ".".to_string(),
    ]);
    let run = run_container("go-build", &args, Duration::from_secs(400));
    if run.ok && binary.is_file() {
        Ok(binary)
    } else {
        Err(format!(
            "go build of the franz-go probe failed: {}",
            run.tail()
        ))
    }
}

/// `tests/clients/java/ClientMatrixProbe.java` compiled with the host JDK against the
/// `kafka-clients` jar of `image`, cached by source and image.
fn java_probe(image: &str) -> Result<std::path::PathBuf, String> {
    let source = clients_dir().join("java/ClientMatrixProbe.java");
    let text = std::fs::read(&source).map_err(|error| format!("read the Java probe: {error}"))?;
    let dir = cache_dir(&format!("java-{}", digest(&[&text, image.as_bytes()])));
    if dir.join("ClientMatrixProbe.class").is_file() {
        return Ok(dir);
    }
    let version = image
        .split('@')
        .next()
        .and_then(|reference| reference.rsplit_once(':'))
        .map(|(_, tag)| tag)
        .ok_or("the Java image is pinned to a version tag")?;
    let jar = dir.join("kafka-clients.jar");
    let extracted = std::process::Command::new(matrix::container_cli())
        .args([
            "run",
            "--rm",
            "--pull",
            "never",
            "--entrypoint",
            "cat",
            image,
        ])
        .arg(format!("/opt/kafka/libs/kafka-clients-{version}.jar"))
        .output()
        .map_err(|error| format!("extract kafka-clients: {error}"))?;
    if !extracted.status.success() || extracted.stdout.is_empty() {
        return Err(format!(
            "extract kafka-clients {version}: {}",
            String::from_utf8_lossy(&extracted.stderr)
        ));
    }
    std::fs::write(&jar, extracted.stdout).map_err(|error| format!("write the jar: {error}"))?;
    let compiled = std::process::Command::new("javac")
        .args(["--release", "17", "-nowarn", "-cp"])
        .arg(&jar)
        .arg("-d")
        .arg(&dir)
        .arg(&source)
        .output()
        .map_err(|error| format!("javac is unavailable: {error}"))?;
    let _ = std::fs::remove_file(&jar);
    if compiled.status.success() {
        Ok(dir)
    } else {
        Err(format!(
            "javac failed: {}",
            String::from_utf8_lossy(&compiled.stderr)
        ))
    }
}

/// Runs one probe on `target` and turns what it printed into facts.
fn run_probe(
    label: &str,
    prepared: &[String],
    target: &Target,
    topic: &str,
    group: &str,
) -> (Facts, Run) {
    let mut args = host_user_args();
    for (key, value) in [
        ("BOOTSTRAP", target.address.as_str()),
        ("TOPIC", topic),
        ("GROUP", group),
        ("COUNT", &COUNT.to_string()),
    ] {
        args.push("-e".to_string());
        args.push(format!("{key}={value}"));
    }
    args.extend(prepared.iter().cloned());
    let run = run_container(label, &args, CLIENT_TIMEOUT);
    let mut facts = Facts::default();
    facts.set("exit", if run.ok { "0" } else { "failed" });
    facts_from_events(&parse_events(&run.stdout), Values::Probe, &mut facts);
    (facts, run)
}

/// One client's run on both brokers, as a matrix row and its verdict.
struct Outcome {
    row: Row,
    verdict: Verdict,
}

/// Runs `client` with `prepare`d arguments against both brokers and judges it.
async fn smoke_client(brokers: &Brokers, client: Smoke, allowed: &[Allowed]) -> Outcome {
    let name = client.name();
    let broken = |reason: String| Outcome {
        row: Row {
            client: name.to_string(),
            version: client.version(),
            cells: std::collections::BTreeMap::new(),
            broken: Some(reason),
            seconds: (0.0, 0.0),
        },
        verdict: Verdict::default(),
    };
    if let Err(reason) = image_ready(client.image()) {
        return broken(reason);
    }
    let prepared = match client.prepare() {
        Ok(prepared) => prepared,
        Err(reason) => return broken(reason),
    };
    let topic = brokers.name(name, "t");
    let group = brokers.name(name, "g");
    let mut observed = Vec::new();
    for target in brokers.targets() {
        if let Err(reason) = create_topic(&target, &topic, &[]).await {
            if target.kind == TargetKind::Kafka {
                return broken(reason);
            }
            // The gateway failing to create a topic is a gateway result, recorded as facts.
            let mut facts = Facts::default();
            facts.set("exit", format!("create topic: {reason}"));
            observed.push((facts, None));
            continue;
        }
        let started = Instant::now();
        let probe =
            tokio::task::block_in_place(|| run_probe(name, &prepared, &target, &topic, &group));
        eprintln!(
            "client matrix: {name} on {} took {:.1}s",
            target.kind,
            started.elapsed().as_secs_f64()
        );
        observed.push((probe.0, Some(probe.1)));
    }
    let (kafka, kafka_run) = observed.pop().expect("kafka ran");
    let (gateway, gateway_run) = observed.pop().expect("the gateway ran");
    let seconds = |run: &Option<Run>| run.as_ref().map_or(0.0, |run| run.elapsed.as_secs_f64());
    if let Err(reason) = oracle_sane(&kafka) {
        let tail = kafka_run.as_ref().map_or_default(Run::tail);
        return broken(format!(
            "the probe misbehaves on Apache Kafka: {reason}\n{tail}"
        ));
    }
    let verdict = judge_client(name, &gateway, &kafka, allowed);
    if let (false, Some(run)) = (verdict.passed(), &gateway_run) {
        eprintln!("{name} on the gateway, last output:\n{}", run.tail());
    }
    Outcome {
        row: row(
            name,
            &client.version(),
            &gateway,
            &kafka,
            &verdict,
            (seconds(&gateway_run), seconds(&kafka_run)),
        ),
        verdict,
    }
}

/// Judges every outcome, prints and publishes the matrix, and fails on anything unexpected.
fn conclude(title: &str, file: &str, outcomes: Vec<Outcome>) {
    let mut rows = Vec::new();
    let mut details = Vec::new();
    let mut failures = Vec::new();
    for outcome in outcomes {
        if let Some(reason) = &outcome.row.broken {
            failures.push(format!("{} is broken: {reason}", outcome.row.client));
        }
        if !outcome.verdict.passed() {
            failures.push(format!(
                "{} differs from Apache Kafka:\n{}",
                outcome.row.client,
                outcome.verdict.report()
            ));
        }
        details.push((outcome.row.client.clone(), outcome.verdict));
        rows.push(outcome.row);
    }
    let markdown = render(title, &rows, &details);
    eprintln!("{markdown}");
    publish(file, &markdown);
    assert!(failures.is_empty(), "{}", failures.join("\n"));
}

fn allowed() -> Vec<Allowed> {
    parse_allowed(ALLOWED_DIFFERENCES).expect("tests/clients/allowed_differences.toml parses")
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn client_matrix_smoke() {
    let Some(brokers) = brokers().await else {
        return;
    };
    let allowed = allowed();
    let mut outcomes = Vec::new();
    for client in Smoke::ALL {
        outcomes.push(smoke_client(&brokers, client, &allowed).await);
    }
    conclude(
        &format!("Client matrix, smoke (gateway vs {KAFKA_ORACLE_IMAGE})"),
        "smoke.md",
        outcomes,
    );
}

/// Every client `allowed_differences.toml` may name: the smoke clients and the nightly ones.
fn known_clients() -> Vec<&'static str> {
    let mut clients: Vec<&'static str> = Smoke::ALL.iter().map(|client| client.name()).collect();
    clients.extend([
        "vector",
        "fluent-bit",
        "risingwave",
        "flink",
        "connect-debezium",
    ]);
    clients
}

#[test]
fn every_declared_client_difference_names_a_known_client_and_an_issue() {
    let known = known_clients();
    for entry in allowed() {
        let client = entry.scenario.strip_prefix("client/").unwrap_or_default();
        assert!(
            known.contains(&client),
            "{}: scenario is client/<client> with a client the matrix runs",
            entry.scenario
        );
        assert!(
            entry.issue.starts_with('#') || entry.issue.starts_with("KW-"),
            "{}/{}: issue `{}` should be an issue number or a plan task",
            entry.scenario,
            entry.fact,
            entry.issue
        );
    }
}

// ---------------------------------------------------------------------------------------------
// Nightly: frameworks and agents, each driven through its own configuration.
// ---------------------------------------------------------------------------------------------

/// Vector (librdkafka inside), the `kafka` sink and source.
pub const VECTOR_NIGHTLY: &str = "docker.io/timberio/vector:0.59.0-debian@sha256:98e7b4dfe50750e61470697bbd2fbeca643d12bea97f351005038ef506eab630";

/// Fluent Bit (librdkafka inside), `out_kafka` and `in_kafka`.
pub const FLUENT_BIT_NIGHTLY: &str = "docker.io/fluent/fluent-bit:5.0.11@sha256:07716ab389f360fbcf1e69b98b8a0dfe2ab1995ec45971557a1b3060990d6b89";

/// `RisingWave` in single-node mode, a Kafka source and a Kafka sink.
pub const RISINGWAVE_NIGHTLY: &str = "docker.io/risingwavelabs/risingwave:v3.0.5@sha256:ced07439d3dffbeb62223e486ff4d9a6dc6faaf033a8259ca3b370fd5349ab42";

/// Flink, a session cluster in one container, with the Kafka SQL connector from Maven Central.
pub const FLINK_NIGHTLY: &str = "docker.io/library/flink:2.2.1-java17@sha256:c5124717afe3829cc2d7c9b538bfe0b9d66cae3107a174d8f0e0392abe6375b8";

/// `flink-sql-connector-kafka` for Flink 2.2, and the SHA-256 of the jar Maven Central serves.
const FLINK_CONNECTOR_VERSION: &str = "5.0.0-2.2";
const FLINK_CONNECTOR_SHA256: &str =
    "5605c691d11a501382c383fecba37a7a552467da5ab7ba904ef5d6f3d62c5616";

/// Kafka Connect distributed with the Debezium connectors, Debezium's own image.
pub const DEBEZIUM_CONNECT_NIGHTLY: &str = "quay.io/debezium/connect:3.6.2.Final@sha256:3f8513649a1acda5130a01550a26c670fee5c5787d5ec799b609d01b3dea2f4d";

/// The database Debezium captures from, and the `psql` the `RisingWave` scenario uses.
pub const POSTGRES_NIGHTLY: &str = "docker.io/library/postgres:17.10@sha256:7958605b474b3d264a969cb3a123d6aa00ad1e1fe9da8a69984dabb704d93317";

/// Whether the nightly clients run. Off, they pass without running: a pull request never pulls
/// their images.
fn nightly_enabled(client: &str) -> bool {
    if env_flag("KAFKA_CLIENT_MATRIX_FULL") {
        return true;
    }
    eprintln!("{client}: nightly client matrix; set KAFKA_CLIENT_MATRIX_FULL=1 to run it");
    false
}

/// The Python probe in `mode` against `target`, with `env` on top of `BOOTSTRAP`.
fn python(target: &Target, mode: &str, env: &[(&str, &str)]) -> Result<Run, String> {
    let deps = python_deps()?;
    let mut args = host_user_args();
    args.extend(["-e".to_string(), format!("BOOTSTRAP={}", target.address)]);
    args.extend(["-e".to_string(), format!("COUNT={COUNT}")]);
    for (key, value) in env {
        args.extend(["-e".to_string(), format!("{key}={value}")]);
    }
    args.extend([
        "-v".to_string(),
        format!("{}:/deps:ro", path_str(&deps)),
        "-v".to_string(),
        format!("{}:/probe:ro", path_str(&clients_dir().join("python"))),
        "-e".to_string(),
        "PYTHONPATH=/deps".to_string(),
        PYTHON_IMAGE.to_string(),
        "python".to_string(),
        "/probe/probe.py".to_string(),
        mode.to_string(),
    ]);
    let run = run_container("python", &args, CLIENT_TIMEOUT);
    if run.ok {
        Ok(run)
    } else {
        Err(format!("python probe {mode}: {}", run.tail()))
    }
}

/// Reads `topic` from the start until `done` holds over the records or `limit` passes,
/// re-reading each time; returns the last read's events.
async fn read_until(
    target: &Target,
    topic: &str,
    phase: &str,
    limit: Duration,
    done: impl Fn(&[Event]) -> bool,
) -> Result<Vec<Event>, String> {
    let deadline = Instant::now() + limit;
    loop {
        let run = tokio::task::block_in_place(|| {
            python(
                target,
                "read",
                &[("TOPIC", topic), ("PHASE", phase), ("DEADLINE_S", "20")],
            )
        })?;
        let events: Vec<Event> = parse_events(&run.stdout)
            .into_iter()
            .filter(|event| matches!(event, Event::Rec { .. }))
            .collect();
        if done(&events) || Instant::now() >= deadline {
            return Ok(events);
        }
        tokio::time::sleep(Duration::from_secs(2)).await;
    }
}

fn record_count(events: &[Event]) -> usize {
    events
        .iter()
        .filter(|event| matches!(event, Event::Rec { .. }))
        .count()
}

/// The matrix's records as JSON lines (`key`, `value`, `idx`), for clients that read a file.
fn records_file(first: usize) -> std::path::PathBuf {
    let path = cache_dir("records").join(format!("records-{first}-{COUNT}.jsonl"));
    let mut text = String::new();
    for index in first..first + COUNT {
        let (key, value, _) = matrix::expected_record(index);
        let _ = writeln!(
            text,
            r#"{{"key":"{key}","value":"{value}","idx":"{index}"}}"#
        );
    }
    std::fs::write(&path, text).expect("write the records file");
    path
}

/// A nightly client's run on one target: its facts and how long it took.
type Observed = (Facts, f64);

/// Runs `scenario` on the gateway and then on Kafka, judges it, and concludes the client's
/// matrix row. `sane` checks the Kafka run is what a working client gives.
async fn nightly<F, Fut>(client: &str, version: &str, sane: &[(&str, String)], scenario: F)
where
    F: Fn(Target, String) -> Fut,
    Fut: std::future::Future<Output = Result<Facts, String>>,
{
    let Some(brokers) = brokers().await else {
        return;
    };
    let allowed = allowed();
    let mut observed: Vec<Result<Observed, String>> = Vec::new();
    for target in brokers.targets() {
        let started = Instant::now();
        let prefix = brokers.name(client, target.kind.name());
        let result = scenario(target.clone(), prefix).await;
        let seconds = started.elapsed().as_secs_f64();
        eprintln!(
            "client matrix: {client} on {} took {seconds:.1}s",
            target.kind
        );
        observed.push(result.map(|facts| (facts, seconds)));
    }
    let kafka = observed.pop().expect("kafka ran");
    let gateway = observed.pop().expect("the gateway ran");
    let broken = |reason: String| Outcome {
        row: Row {
            client: client.to_string(),
            version: version.to_string(),
            cells: std::collections::BTreeMap::new(),
            broken: Some(reason),
            seconds: (0.0, 0.0),
        },
        verdict: Verdict::default(),
    };
    let outcome = match kafka {
        Err(reason) => broken(format!("on Apache Kafka: {reason}")),
        Ok((kafka, kafka_seconds)) => {
            let wrong: Vec<String> = sane
                .iter()
                .filter(|(fact, want)| kafka.get(fact) != Some(want.as_str()))
                .map(|(fact, want)| format!("{fact} = {:?}, want {want}", kafka.get(fact)))
                .collect();
            if wrong.is_empty() {
                let (gateway, gateway_seconds) = gateway.unwrap_or_else(|reason| {
                    let mut facts = Facts::default();
                    facts.set("exit", format!("failed: {reason}"));
                    (facts, 0.0)
                });
                let verdict = judge_client(client, &gateway, &kafka, &allowed);
                Outcome {
                    row: row(
                        client,
                        version,
                        &gateway,
                        &kafka,
                        &verdict,
                        (gateway_seconds, kafka_seconds),
                    ),
                    verdict,
                }
            } else {
                broken(format!(
                    "the client misbehaves on Apache Kafka: {}",
                    wrong.join("; ")
                ))
            }
        }
    };
    conclude(
        &format!("Client matrix, nightly: {client}"),
        &format!("nightly-{client}.md"),
        vec![outcome],
    );
    drop(brokers);
}

/// A record a streaming agent printed: partition, offset, key, value, headers.
type Printed = (i32, i64, String, String, String);

/// How a streaming agent (Vector, Fluent Bit) is run, as a sink and as a group consumer.
struct Agent {
    label: &'static str,
    /// Arguments after the environment for the sink run (mounts, image, command).
    sink: fn(&std::path::Path) -> Vec<String>,
    /// Arguments after the environment for the source run.
    source: fn() -> Vec<String>,
    /// One output line of the source run, as `(partition, offset, key, value, headers)`.
    parse: fn(&str) -> Option<Printed>,
    values: Values,
}

fn config_mount(client: &str) -> Vec<String> {
    vec![
        "-v".to_string(),
        format!("{}:/cfg:ro", path_str(&clients_dir().join(client))),
    ]
}

fn json_str(value: &serde_json::Value, field: &str) -> Option<String> {
    value
        .get(field)
        .and_then(serde_json::Value::as_str)
        .map(str::to_string)
}

fn vector_agent() -> Agent {
    Agent {
        label: "vector",
        sink: |records| {
            let mut args = config_mount("vector");
            args.extend([
                "-v".to_string(),
                format!("{}:/records.jsonl:ro", path_str(records)),
                "--entrypoint".to_string(),
                "sh".to_string(),
                VECTOR_NIGHTLY.to_string(),
                "-c".to_string(),
                "vector --quiet --dangerously-allow-env-var-interpolation --config /cfg/sink.yaml \
                 < /records.jsonl"
                    .to_string(),
            ]);
            args
        },
        source: || {
            let mut args = config_mount("vector");
            args.extend([
                VECTOR_NIGHTLY.to_string(),
                "--quiet".to_string(),
                "--dangerously-allow-env-var-interpolation".to_string(),
                "--config".to_string(),
                "/cfg/source.yaml".to_string(),
            ]);
            args
        },
        parse: |line| {
            let event: serde_json::Value = serde_json::from_str(line).ok()?;
            let headers = event
                .get("headers")
                .and_then(serde_json::Value::as_object)
                .map_or_else(
                    || "-".to_string(),
                    |headers| {
                        headers
                            .iter()
                            .map(|(name, value)| {
                                format!("{name}={}", value.as_str().unwrap_or("?"))
                            })
                            .collect::<Vec<_>>()
                            .join(",")
                    },
                );
            Some((
                i32::try_from(event.get("partition")?.as_i64()?).ok()?,
                event.get("offset")?.as_i64()?,
                json_str(&event, "message_key").unwrap_or_else(|| "-".to_string()),
                json_str(&event, "message").unwrap_or_else(|| "-".to_string()),
                if headers.is_empty() {
                    "-".to_string()
                } else {
                    headers
                },
            ))
        },
        values: Values::Probe,
    }
}

fn fluent_bit_agent() -> Agent {
    Agent {
        label: "fluent-bit",
        sink: |records| {
            let mut args = config_mount("fluent-bit");
            args.extend([
                "-v".to_string(),
                format!("{}:/records.jsonl:ro", path_str(records)),
                FLUENT_BIT_NIGHTLY.to_string(),
                "-c".to_string(),
                "/cfg/sink.yaml".to_string(),
            ]);
            args
        },
        source: || {
            let mut args = config_mount("fluent-bit");
            args.extend([
                FLUENT_BIT_NIGHTLY.to_string(),
                "-c".to_string(),
                "/cfg/source.yaml".to_string(),
            ]);
            args
        },
        parse: |line| {
            let event: serde_json::Value = serde_json::from_str(line).ok()?;
            Some((
                i32::try_from(event.get("partition")?.as_i64()?).ok()?,
                event.get("offset")?.as_i64()?,
                json_str(&event, "key").unwrap_or_else(|| "-".to_string()),
                json_str(&event, "payload").unwrap_or_else(|| "-".to_string()),
                "-".to_string(),
            ))
        },
        // out_kafka writes the record as JSON and sets no headers; in_kafka prints no headers.
        values: Values::KeyOnly,
    }
}

/// One group consumer run of `agent`: reads until it has printed `COUNT` records (plus a moment
/// for anything extra), lets a commit interval pass, then stops it as an operator would.
fn agent_consume(
    agent: &Agent,
    target: &Target,
    topic: &str,
    group: &str,
    phase: &str,
    events: &mut Vec<Event>,
) -> Result<(), String> {
    let mut args = host_user_args();
    for (key, value) in [
        ("BOOTSTRAP", target.address.as_str()),
        ("TOPIC", topic),
        ("GROUP", group),
    ] {
        args.extend(["-e".to_string(), format!("{key}={value}")]);
    }
    args.extend((agent.source)());
    let mut running = matrix::Streaming::start(agent.label, &args)?;
    let parse = agent.parse;
    let records = |lines: &[String]| lines.iter().filter(|line| parse(line).is_some()).count();
    running.wait_for(Duration::from_secs(90), |lines| records(lines) >= COUNT);
    // Anything past COUNT (a replay) shows up within this, and so does one commit interval.
    std::thread::sleep(Duration::from_secs(4));
    let lines = running.stop(Duration::from_secs(20));
    for line in &lines {
        if let Some((partition, offset, key, value, headers)) = parse(line) {
            events.push(Event::Rec {
                phase: phase.to_string(),
                partition,
                offset,
                key,
                value,
                headers,
            });
        }
    }
    Ok(())
}

/// The agent scenario on one target: sink, then a group consumer, its commit, and a restart.
async fn agent_scenario(agent: Agent, target: Target, prefix: String) -> Result<Facts, String> {
    let sink_topic = format!("{prefix}-sink");
    let source_topic = format!("{prefix}-source");
    let group = format!("{prefix}-group");
    create_topic(&target, &sink_topic, &[]).await?;
    create_topic(&target, &source_topic, &[]).await?;
    let mut facts = Facts::default();
    let mut events = Vec::new();

    let mut args = host_user_args();
    args.extend(["-e".to_string(), format!("BOOTSTRAP={}", target.address)]);
    args.extend(["-e".to_string(), format!("TOPIC={sink_topic}")]);
    args.extend((agent.sink)(&records_file(0)));
    let run = tokio::task::block_in_place(|| run_container(agent.label, &args, CLIENT_TIMEOUT));
    facts.set(
        "sink.exit",
        if run.ok {
            "0".to_string()
        } else {
            run.status.clone()
        },
    );
    events.extend(
        read_until(
            &target,
            &sink_topic,
            "sink",
            Duration::from_secs(30),
            |events| record_count(events) >= COUNT,
        )
        .await?,
    );

    tokio::task::block_in_place(|| {
        python(
            &target,
            "produce",
            &[("TOPIC", &source_topic), ("PHASE", "produce")],
        )
    })
    .map(|run| events.extend(parse_events(&run.stdout)))?;
    tokio::task::block_in_place(|| {
        agent_consume(
            &agent,
            &target,
            &source_topic,
            &group,
            "source",
            &mut events,
        )
    })?;
    let committed = |phase: &str| {
        tokio::task::block_in_place(|| {
            python(
                &target,
                "committed",
                &[
                    ("TOPIC", &source_topic),
                    ("GROUP", &group),
                    ("PHASE", phase),
                ],
            )
        })
    };
    events.extend(parse_events(&committed("source")?.stdout));

    tokio::task::block_in_place(|| {
        python(
            &target,
            "produce",
            &[
                ("TOPIC", &source_topic),
                ("PHASE", "produce2"),
                ("BATCH", "2"),
            ],
        )
    })
    .map(|run| events.extend(parse_events(&run.stdout)))?;
    tokio::task::block_in_place(|| {
        agent_consume(
            &agent,
            &target,
            &source_topic,
            &group,
            "resume",
            &mut events,
        )
    })?;
    events.extend(parse_events(&committed("resume")?.stdout));
    events.retain(|event| *event != Event::Done);
    facts_from_events(&events, agent.values, &mut facts);
    Ok(facts)
}

fn agent_sane() -> Vec<(&'static str, String)> {
    let count = COUNT.to_string();
    vec![
        ("sink.count", count.clone()),
        ("sink.intact", count.clone()),
        ("source.count", count.clone()),
        ("source.intact", count.clone()),
        ("resume.count", count.clone()),
        ("resume.first_index", count),
    ]
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn client_matrix_nightly_vector() {
    if !nightly_enabled("vector") {
        return;
    }
    nightly(
        "vector",
        "Vector 0.59.0",
        &agent_sane(),
        |target, prefix| agent_scenario(vector_agent(), target, prefix),
    )
    .await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn client_matrix_nightly_fluent_bit() {
    if !nightly_enabled("fluent-bit") {
        return;
    }
    nightly(
        "fluent-bit",
        "Fluent Bit 5.0.11",
        &agent_sane(),
        |target, prefix| agent_scenario(fluent_bit_agent(), target, prefix),
    )
    .await;
}

/// The Flink Kafka SQL connector jar from Maven Central, checked against its pinned digest.
fn flink_connector() -> Result<std::path::PathBuf, String> {
    let dir = cache_dir("flink");
    let jar = dir.join(format!(
        "flink-sql-connector-kafka-{FLINK_CONNECTOR_VERSION}.jar"
    ));
    let sha256 = |path: &std::path::Path| {
        std::process::Command::new("sha256sum")
            .arg(path)
            .output()
            .ok()
            .and_then(|output| {
                String::from_utf8_lossy(&output.stdout)
                    .split_whitespace()
                    .next()
                    .map(str::to_string)
            })
    };
    if jar.is_file() && sha256(&jar).as_deref() == Some(FLINK_CONNECTOR_SHA256) {
        return Ok(jar);
    }
    let url = format!(
        "https://repo1.maven.org/maven2/org/apache/flink/flink-sql-connector-kafka/\
         {FLINK_CONNECTOR_VERSION}/flink-sql-connector-kafka-{FLINK_CONNECTOR_VERSION}.jar"
    );
    let fetched = std::process::Command::new("curl")
        .args(["-sfL", "--retry", "3", "-o"])
        .arg(&jar)
        .arg(&url)
        .status()
        .map_err(|error| format!("curl is unavailable: {error}"))?;
    if !fetched.success() {
        return Err(format!("download {url} failed"));
    }
    match sha256(&jar) {
        Some(digest) if digest == FLINK_CONNECTOR_SHA256 => Ok(jar),
        other => {
            let _ = std::fs::remove_file(&jar);
            Err(format!(
                "{url}: SHA-256 {other:?}, want {FLINK_CONNECTOR_SHA256}"
            ))
        }
    }
}

/// Flink on one target: a bounded `INSERT INTO sink SELECT * FROM source` over JSON records,
/// `KafkaSource` in, an `AT_LEAST_ONCE` `KafkaSink` (an idempotent producer) out.
async fn flink_scenario(target: Target, prefix: String) -> Result<Facts, String> {
    let source = format!("{prefix}-source");
    let sink = format!("{prefix}-sink");
    let group = format!("{prefix}-group");
    create_topic(&target, &source, &[]).await?;
    create_topic(&target, &sink, &[]).await?;
    let mut facts = Facts::default();
    let mut events = Vec::new();
    tokio::task::block_in_place(|| {
        python(
            &target,
            "produce",
            &[("TOPIC", &source), ("PHASE", "produce"), ("VALUES", "json")],
        )
    })
    .map(|run| events.extend(parse_events(&run.stdout)))?;

    let jar = flink_connector()?;
    let job = include_str!("clients/flink/job.sql")
        .replace("@BOOTSTRAP@", &target.address)
        .replace("@SOURCE@", &source)
        .replace("@SINK@", &sink)
        .replace("@GROUP@", &group);
    let job_file = cache_dir("flink").join(format!("{prefix}.sql"));
    std::fs::write(&job_file, job).map_err(|error| format!("write the Flink job: {error}"))?;
    let args = vec![
        "-e".to_string(),
        format!("REST_PORT={}", oracle::free_port()),
        "-e".to_string(),
        format!("RPC_PORT={}", oracle::free_port()),
        "-v".to_string(),
        format!(
            "{}:/job/run.sh:ro",
            path_str(&clients_dir().join("flink/run.sh"))
        ),
        "-v".to_string(),
        format!("{}:/job/job.sql:ro", path_str(&job_file)),
        "-v".to_string(),
        format!(
            "{}:/opt/flink/lib/flink-sql-connector-kafka.jar:ro",
            path_str(&jar)
        ),
        "--entrypoint".to_string(),
        "sh".to_string(),
        FLINK_NIGHTLY.to_string(),
        "/job/run.sh".to_string(),
    ];
    let run =
        tokio::task::block_in_place(|| run_container("flink", &args, Duration::from_secs(300)));
    let _ = std::fs::remove_file(&job_file);
    let finished = run
        .stdout
        .contains("Complete execution of the SQL update statement");
    facts.set(
        "sink.job",
        if finished { "finished" } else { "not finished" },
    );
    if !finished {
        eprintln!("flink on {}:\n{}", target.kind, run.tail());
    }
    events.extend(
        read_until(&target, &sink, "sink", Duration::from_secs(20), |events| {
            record_count(events) >= COUNT
        })
        .await?,
    );
    events.retain(|event| *event != Event::Done);
    facts_from_events(&events, Values::JsonUnordered, &mut facts);
    Ok(facts)
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn client_matrix_nightly_flink() {
    if !nightly_enabled("flink") {
        return;
    }
    let count = COUNT.to_string();
    let sane = [
        ("sink.job", "finished".to_string()),
        ("sink.count", count.clone()),
        ("sink.intact", count.clone()),
        ("sink.duplicates", "0".to_string()),
        ("produce.acked", count),
    ];
    nightly(
        "flink",
        &format!("Flink 2.2.1, flink-sql-connector-kafka {FLINK_CONNECTOR_VERSION}"),
        &sane,
        flink_scenario,
    )
    .await;
}

/// The integer `field` a record's JSON value carries, looking in Debezium's envelope
/// (`payload.after`, or `after` without schemas) and at the top level.
fn json_field(event: &Event, field: &str) -> Option<i64> {
    let Event::Rec { value, .. } = event else {
        return None;
    };
    let value = serde_json::from_str::<serde_json::Value>(&unescape(value)).ok()?;
    let envelope = value.get("payload").unwrap_or(&value);
    envelope
        .get("after")
        .and_then(|after| after.get(field))
        .or_else(|| envelope.get(field))
        .and_then(serde_json::Value::as_i64)
}

/// Every `field` the JSON values of `events` carry, with repetitions.
fn json_id_values(events: &[Event], field: &str) -> Vec<i64> {
    events
        .iter()
        .filter_map(|event| json_field(event, field))
        .collect()
}

/// Every distinct integer `field` the JSON values of `events` carry.
fn json_ids(events: &[Event], field: &str) -> BTreeSet<i64> {
    json_id_values(events, field).into_iter().collect()
}

/// The next offset of every partition in `events`.
fn end_offsets(events: &[Event]) -> BTreeMap<i32, i64> {
    let mut ends = BTreeMap::new();
    for event in events {
        if let Event::Rec {
            partition, offset, ..
        } = event
        {
            let end = ends.entry(*partition).or_insert(0);
            *end = (*end).max(*offset + 1);
        }
    }
    ends
}

/// The IDs of the records in `events` at or after `ends` on their partition.
fn ids_after(events: &[Event], ends: &BTreeMap<i32, i64>) -> Vec<i64> {
    events
        .iter()
        .filter(|event| match event {
            Event::Rec {
                partition, offset, ..
            } => *offset >= ends.get(partition).copied().unwrap_or(0),
            _ => false,
        })
        .filter_map(|event| json_field(event, "id"))
        .collect()
}

/// A sorted, de-duplicated list of `ids` as a fact value.
fn ids_fact(ids: &[i64]) -> String {
    let unique: BTreeSet<i64> = ids.iter().copied().collect();
    unique
        .iter()
        .map(ToString::to_string)
        .collect::<Vec<_>>()
        .join(",")
}

/// How many IDs in `ids` repeat.
fn duplicate_ids(ids: &[i64]) -> usize {
    let mut counts: BTreeMap<i64, usize> = BTreeMap::new();
    for id in ids {
        *counts.entry(*id).or_default() += 1;
    }
    counts.values().map(|count| count.saturating_sub(1)).sum()
}

/// The IDs the rows inserted after the restart carry.
fn restart_ids() -> BTreeSet<i64> {
    (2 * COUNT..3 * COUNT)
        .map(|index| i64::try_from(index).expect("COUNT fits i64"))
        .collect()
}

/// Polls `check` every second until it holds or `limit` passes. Returns whether it held.
async fn eventually(limit: Duration, mut check: impl FnMut() -> bool) -> bool {
    let deadline = Instant::now() + limit;
    loop {
        if tokio::task::block_in_place(&mut check) {
            return true;
        }
        if Instant::now() >= deadline {
            return false;
        }
        tokio::time::sleep(Duration::from_secs(1)).await;
    }
}

/// A `PostgreSQL` 17 with logical decoding on, on a loopback port.
struct Postgres {
    service: matrix::Service,
    port: u16,
}

impl Postgres {
    async fn start() -> Result<Self, String> {
        image_ready(POSTGRES_NIGHTLY)?;
        let port = oracle::free_port();
        let service = matrix::Service::start(
            "postgres",
            &[
                "-e".to_string(),
                "POSTGRES_PASSWORD=matrix".to_string(),
                POSTGRES_NIGHTLY.to_string(),
                "-c".to_string(),
                "wal_level=logical".to_string(),
                "-c".to_string(),
                format!("port={port}"),
                "-c".to_string(),
                "listen_addresses=127.0.0.1".to_string(),
            ],
        )?;
        let postgres = Self { service, port };
        let ready = eventually(Duration::from_secs(60), || postgres.sql("SELECT 1").is_ok()).await;
        if ready {
            Ok(postgres)
        } else {
            Err(format!(
                "PostgreSQL never became ready:\n{}",
                postgres.service.logs_tail(20)
            ))
        }
    }

    fn sql(&self, statement: &str) -> Result<String, String> {
        let port = self.port.to_string();
        let run = self.service.exec(&[
            "psql",
            "-X",
            "-q",
            "-t",
            "-A",
            "-v",
            "ON_ERROR_STOP=1",
            "-U",
            "postgres",
            "-h",
            "127.0.0.1",
            "-p",
            &port,
            "-c",
            statement,
        ]);
        if run.ok {
            Ok(run.stdout.trim().to_string())
        } else {
            Err(run.tail())
        }
    }

    /// Inserts rows `first..first + COUNT` into `items`.
    fn insert(&self, first: usize) -> Result<(), String> {
        self.sql(&format!(
            "INSERT INTO items SELECT g, 'n' || g FROM generate_series({first}, {}) g",
            first + COUNT - 1
        ))
        .map(|_| ())
    }
}

/// A Kafka Connect distributed worker (Debezium's image) for `target`.
fn start_connect(target: &Target, prefix: &str, rest: u16) -> Result<matrix::Service, String> {
    let env = [
        ("BOOTSTRAP_SERVERS", target.address.clone()),
        ("GROUP_ID", format!("{prefix}-connect")),
        ("CONFIG_STORAGE_TOPIC", format!("{prefix}-connect-configs")),
        ("OFFSET_STORAGE_TOPIC", format!("{prefix}-connect-offsets")),
        ("STATUS_STORAGE_TOPIC", format!("{prefix}-connect-status")),
        ("CONNECT_CONFIG_STORAGE_REPLICATION_FACTOR", "1".to_string()),
        ("CONNECT_OFFSET_STORAGE_REPLICATION_FACTOR", "1".to_string()),
        ("CONNECT_STATUS_STORAGE_REPLICATION_FACTOR", "1".to_string()),
        ("REST_HOST_NAME", "127.0.0.1".to_string()),
        ("REST_PORT", rest.to_string()),
        ("ADVERTISED_HOST_NAME", "127.0.0.1".to_string()),
        ("ADVERTISED_PORT", rest.to_string()),
        ("CONNECT_LISTENERS", format!("http://127.0.0.1:{rest}")),
        ("HEAP_OPTS", "-Xms256m -Xmx768m".to_string()),
        ("OFFSET_FLUSH_INTERVAL_MS", "1000".to_string()),
    ];
    let mut args = Vec::new();
    for (key, value) in env {
        args.extend(["-e".to_string(), format!("{key}={value}")]);
    }
    args.push(DEBEZIUM_CONNECT_NIGHTLY.to_string());
    matrix::Service::start("connect", &args)
}

fn connect_rest(connect: &matrix::Service, rest: u16, path: &str, body: Option<&str>) -> Run {
    let url = format!("http://127.0.0.1:{rest}{path}");
    let mut command = vec!["curl", "-sf", "--max-time", "30"];
    if let Some(body) = body {
        command.extend([
            "-X",
            "POST",
            "-H",
            "Content-Type: application/json",
            "--data",
            body,
        ]);
    }
    command.push(&url);
    connect.exec(&command)
}

/// Waits for a worker's REST API, then for it to have joined its group (it answers
/// `/connectors` only once it has read its config topic).
async fn connect_ready(connect: &matrix::Service, rest: u16) -> bool {
    eventually(Duration::from_secs(180), || {
        connect_rest(connect, rest, "/connectors", None).ok
    })
    .await
}

/// Kafka Connect with Debezium on one target: the worker starts on its internal topics, the
/// connector snapshots a table, streams later inserts, and a new worker resumes from the stored
/// offsets without losing any.
async fn connect_scenario(target: Target, prefix: String) -> Result<Facts, String> {
    let mut facts = Facts::default();
    let postgres = Postgres::start().await?;
    postgres.sql("CREATE TABLE items (id int PRIMARY KEY, name text)")?;
    postgres.insert(0)?;
    let rest = oracle::free_port();
    let connect = start_connect(&target, &prefix, rest)?;
    let ready = connect_ready(&connect, rest).await;
    facts.set("start.ready", ready);
    if !ready {
        eprintln!(
            "Connect on {} never became ready:\n{}",
            target.kind,
            connect.logs_tail(400)
        );
        return Ok(facts);
    }
    let topic = format!("{prefix}.public.items");
    let connector = serde_json::json!({
        "name": format!("{prefix}-debezium"),
        "config": {
            "connector.class": "io.debezium.connector.postgresql.PostgresConnector",
            "database.hostname": "127.0.0.1",
            "database.port": postgres.port.to_string(),
            "database.user": "postgres",
            "database.password": "matrix",
            "database.dbname": "postgres",
            "topic.prefix": prefix,
            "plugin.name": "pgoutput",
            "slot.name": format!("slot_{}", digest(&[prefix.as_bytes()])),
            "publication.autocreate.mode": "filtered",
            "table.include.list": "public.items",
            "topic.creation.default.replication.factor": "-1",
            "topic.creation.default.partitions": "1",
            "tasks.max": "1",
        }
    });
    let registered = connect_rest(&connect, rest, "/connectors", Some(&connector.to_string()));
    facts.set("snapshot.registered", registered.ok);
    let ids = |events: &[Event]| json_ids(events, "id").len();
    let snapshot = read_until(
        &target,
        &topic,
        "snapshot",
        Duration::from_secs(120),
        |events| ids(events) >= COUNT,
    )
    .await?;
    facts.set("snapshot.ids", ids(&snapshot));

    postgres.insert(COUNT)?;
    let stream = read_until(
        &target,
        &topic,
        "stream",
        Duration::from_secs(90),
        |events| ids(events) >= 2 * COUNT,
    )
    .await?;
    facts.set("stream.ids", ids(&stream));

    connect.stop(Duration::from_secs(30));
    drop(connect);
    restart_connect(
        &target, &prefix, &topic, rest, &postgres, &stream, &mut facts,
    )
    .await?;
    Ok(facts)
}

/// Starts a new worker, then checks that only the rows inserted after the recorded offsets
/// arrived: a worker that replays its snapshot must not look like a successful resume.
async fn restart_connect(
    target: &Target,
    prefix: &str,
    topic: &str,
    rest: u16,
    postgres: &Postgres,
    stream: &[Event],
    facts: &mut Facts,
) -> Result<(), String> {
    let before = read_until(
        target,
        topic,
        "before-restart",
        Duration::from_secs(60),
        |_| true,
    )
    .await?;
    let mut ends = end_offsets(stream);
    for (partition, end) in end_offsets(&before) {
        ends.entry(partition)
            .and_modify(|current| *current = (*current).max(end))
            .or_insert(end);
    }
    let connect = start_connect(target, prefix, rest)?;
    let ready = connect_ready(&connect, rest).await;
    facts.set("restart.ready", ready);
    postgres.insert(2 * COUNT)?;
    let expected = restart_ids();
    let restart = read_until(
        target,
        topic,
        "restart",
        Duration::from_secs(120),
        |events| {
            let observed: BTreeSet<i64> = ids_after(events, &ends).into_iter().collect();
            expected.is_subset(&observed)
        },
    )
    .await?;
    let after = ids_after(&restart, &ends);
    facts.set("restart.ids", ids_fact(&after));
    facts.set("restart.duplicates", duplicate_ids(&after));
    facts.set(
        "restart.missing",
        expected.iter().filter(|id| !after.contains(*id)).count(),
    );
    let status = connect_rest(
        &connect,
        rest,
        &format!("/connectors/{prefix}-debezium/status"),
        None,
    );
    let running = serde_json::from_str::<serde_json::Value>(&status.stdout)
        .ok()
        .and_then(|status| {
            status
                .pointer("/tasks/0/state")
                .and_then(serde_json::Value::as_str)
                .map(str::to_string)
        })
        .unwrap_or_else(|| "unknown".to_string());
    facts.set("restart.task_state", running);
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn client_matrix_nightly_connect_debezium() {
    if !nightly_enabled("connect-debezium") {
        return;
    }
    let expected: Vec<i64> = restart_ids().into_iter().collect();
    let sane = [
        ("start.ready", "true".to_string()),
        ("snapshot.ids", COUNT.to_string()),
        ("stream.ids", (2 * COUNT).to_string()),
        ("restart.ids", ids_fact(&expected)),
        ("restart.duplicates", "0".to_string()),
        ("restart.missing", "0".to_string()),
        ("restart.task_state", "RUNNING".to_string()),
    ];
    nightly(
        "connect-debezium",
        "Kafka Connect (Debezium 3.6.2.Final image), Debezium PostgreSQL connector, PostgreSQL 17.10",
        &sane,
        connect_scenario,
    )
    .await;
}

/// `RisingWave` single-node, in memory, its frontend on a loopback port.
struct RisingWave {
    service: matrix::Service,
    port: u16,
}

impl RisingWave {
    async fn start() -> Result<Self, String> {
        image_ready(RISINGWAVE_NIGHTLY)?;
        image_ready(POSTGRES_NIGHTLY)?;
        let port = oracle::free_port();
        let service = matrix::Service::start(
            "risingwave",
            &[
                "--entrypoint".to_string(),
                "/risingwave/bin/risingwave".to_string(),
                RISINGWAVE_NIGHTLY.to_string(),
                "single_node".to_string(),
                "--in-memory".to_string(),
                "--listen-addr".to_string(),
                format!("127.0.0.1:{port}"),
                "--prometheus-listener-addr".to_string(),
                format!("127.0.0.1:{}", oracle::free_port()),
                "--parallelism".to_string(),
                "1".to_string(),
                "--total-memory-bytes".to_string(),
                "2147483648".to_string(),
            ],
        )?;
        let risingwave = Self { service, port };
        let ready = eventually(Duration::from_secs(120), || {
            risingwave.sql("SELECT 1").is_ok()
        })
        .await;
        if ready {
            Ok(risingwave)
        } else {
            Err(format!(
                "RisingWave never became ready:\n{}",
                risingwave.service.logs_tail(30)
            ))
        }
    }

    /// Runs `statement` with `psql` from the pinned `PostgreSQL` image.
    fn sql(&self, statement: &str) -> Result<String, String> {
        let run = run_container(
            "psql",
            &[
                "-e".to_string(),
                "PGCONNECT_TIMEOUT=5".to_string(),
                "--entrypoint".to_string(),
                "psql".to_string(),
                POSTGRES_NIGHTLY.to_string(),
                "-X".to_string(),
                "-q".to_string(),
                "-t".to_string(),
                "-A".to_string(),
                "-v".to_string(),
                "ON_ERROR_STOP=1".to_string(),
                "-h".to_string(),
                "127.0.0.1".to_string(),
                "-p".to_string(),
                self.port.to_string(),
                "-U".to_string(),
                "root".to_string(),
                "-d".to_string(),
                "dev".to_string(),
                "-c".to_string(),
                statement.to_string(),
            ],
            Duration::from_secs(60),
        );
        if run.ok {
            Ok(run.stdout.trim().to_string())
        } else {
            Err(run.tail())
        }
    }

    /// Polls `query` (one number) until it reaches `want` or `limit` passes; the last answer.
    async fn count(&self, query: &str, want: usize, limit: Duration) -> String {
        let mut last = String::new();
        eventually(limit, || {
            last = self
                .sql(query)
                .unwrap_or_else(|error| format!("error: {error}"));
            last == want.to_string()
        })
        .await;
        last
    }
}

/// `RisingWave` on one target: a Kafka source into a materialized view, a Kafka sink out of it,
/// and a second batch flowing through both.
async fn risingwave_scenario(target: Target, prefix: String) -> Result<Facts, String> {
    let source = format!("{prefix}-source");
    let sink = format!("{prefix}-sink");
    create_topic(&target, &source, &[]).await?;
    create_topic(&target, &sink, &[]).await?;
    let mut facts = Facts::default();
    tokio::task::block_in_place(|| {
        python(
            &target,
            "produce",
            &[("TOPIC", &source), ("PHASE", "produce"), ("VALUES", "json")],
        )
    })?;
    let risingwave = RisingWave::start().await?;
    let created = risingwave.sql(&format!(
        "CREATE SOURCE matrix_source (i int, v varchar) INCLUDE key AS k WITH (\
         connector = 'kafka', topic = '{source}', properties.bootstrap.server = '{}', \
         scan.startup.mode = 'earliest') FORMAT PLAIN ENCODE JSON",
        target.address
    ));
    facts.set("source.created", created.is_ok());
    if let Err(error) = &created {
        eprintln!("RisingWave CREATE SOURCE on {}: {error}", target.kind);
        return Ok(facts);
    }
    risingwave.sql(
        "CREATE MATERIALIZED VIEW matrix_view AS \
         SELECT i, v, convert_from(k, 'utf8') AS key FROM matrix_source",
    )?;
    let rows = "SELECT count(*) FROM matrix_view";
    let keyed = "SELECT count(*) FROM matrix_view WHERE key = 'k' || lpad(i::varchar, 3, '0') \
                 AND v = 'v' || lpad(i::varchar, 3, '0')";
    facts.set(
        "source.rows",
        risingwave.count(rows, COUNT, Duration::from_secs(90)).await,
    );
    facts.set("source.intact", risingwave.sql(keyed).unwrap_or_default());

    let sink_created = risingwave.sql(&format!(
        "CREATE SINK matrix_sink FROM matrix_view WITH (connector = 'kafka', \
         properties.bootstrap.server = '{}', topic = '{sink}') \
         FORMAT PLAIN ENCODE JSON (force_append_only = 'true')",
        target.address
    ));
    facts.set("sink.created", sink_created.is_ok());
    if let Err(error) = &sink_created {
        eprintln!("RisingWave CREATE SINK on {}: {error}", target.kind);
    }
    let ids = |events: &[Event]| json_ids(events, "i").len();
    let out = read_until(&target, &sink, "sink", Duration::from_secs(60), |events| {
        ids(events) >= COUNT
    })
    .await?;
    facts.set("sink.ids", ids(&out));

    tokio::task::block_in_place(|| {
        python(
            &target,
            "produce",
            &[
                ("TOPIC", &source),
                ("PHASE", "produce2"),
                ("BATCH", "2"),
                ("VALUES", "json"),
            ],
        )
    })?;
    facts.set(
        "stream.rows",
        risingwave
            .count(rows, 2 * COUNT, Duration::from_secs(90))
            .await,
    );
    let out = read_until(
        &target,
        &sink,
        "stream",
        Duration::from_secs(60),
        |events| ids(events) >= 2 * COUNT,
    )
    .await?;
    facts.set("stream.sink_ids", ids(&out));
    Ok(facts)
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn client_matrix_nightly_risingwave() {
    if !nightly_enabled("risingwave") {
        return;
    }
    let sane = [
        ("source.rows", COUNT.to_string()),
        ("source.intact", COUNT.to_string()),
        ("sink.ids", COUNT.to_string()),
        ("stream.rows", (2 * COUNT).to_string()),
        ("stream.sink_ids", (2 * COUNT).to_string()),
    ];
    nightly(
        "risingwave",
        "RisingWave 3.0.5 (single node)",
        &sane,
        risingwave_scenario,
    )
    .await;
}
