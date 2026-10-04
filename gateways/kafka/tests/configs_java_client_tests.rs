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

//! Kafka 3.9 `AdminClient` describe/alter against the gateway, on the host JDK.
//!
//! The docker `--network host` suite cannot reach a loopback gateway from macOS.
//! This test publishes nothing into a container: it binds `127.0.0.1` and runs
//! `java` on the host. Jars come from Maven Central when they are not cached.

use std::io::Read;
use std::net::SocketAddr;
use std::path::{Path, PathBuf};
use std::process::{Command, Output, Stdio};
use std::sync::Arc;
use std::time::{Duration, Instant};

use tokio::sync::broadcast;

use iggy_gateway_kafka::bridge::IggyBridge;
use iggy_gateway_kafka::server::bind_listener;
use iggy_gateway_kafka::{GatewayConfig, KafkaGateway};

#[path = "common/iggy_server.rs"]
mod iggy_server;

use iggy_server::TestServer;

const TOPIC: &str = "orders";

/// Bounds a child the way `kafka_client_e2e_tests.rs` bounds a client run, without GNU `timeout`
/// (this suite runs on the host JDK, including macOS). A hung `java` or `curl` used to block the
/// process until the runner itself was killed.
const CHILD_LIMIT: Duration = Duration::from_secs(120);

/// Reports why the suite cannot run, and whether that is fatal.
///
/// Returns `true` when the caller should skip. `JAVA_E2E_REQUIRED=1` makes it panic instead, so a
/// CI job that means to run this fails loudly rather than reporting a pass over zero assertions -
/// same convention as `kafka_client_e2e_tests.rs`'s own `skip`/`KAFKA_E2E_REQUIRED`.
fn skip(reason: &str) -> bool {
    assert!(
        std::env::var("JAVA_E2E_REQUIRED").as_deref() != Ok("1"),
        "JAVA_E2E_REQUIRED=1 but the suite cannot run: {reason}"
    );
    eprintln!("skipping Java admin-client end-to-end test: {reason}");
    true
}

fn java_missing() -> bool {
    let available = Command::new("java")
        .arg("-version")
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .status()
        .is_ok_and(|status| status.success());
    if available {
        false
    } else {
        skip("java is unavailable")
    }
}

fn output_bounded(command: &mut Command, what: &str) -> Output {
    try_output_bounded(command, what).unwrap_or_else(|error| panic!("{error}"))
}

fn try_output_bounded(command: &mut Command, what: &str) -> Result<Output, String> {
    let mut child = command
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .map_err(|error| format!("spawn {what}: {error}"))?;
    let mut stdout = child
        .stdout
        .take()
        .ok_or_else(|| format!("{what} stdout"))?;
    let mut stderr = child
        .stderr
        .take()
        .ok_or_else(|| format!("{what} stderr"))?;
    let stdout_thread = std::thread::spawn(move || {
        let mut buf = Vec::new();
        stdout.read_to_end(&mut buf).map(|_read| buf)
    });
    let stderr_thread = std::thread::spawn(move || {
        let mut buf = Vec::new();
        stderr.read_to_end(&mut buf).map(|_read| buf)
    });
    let deadline = Instant::now() + CHILD_LIMIT;
    let status = loop {
        match child.try_wait() {
            Ok(Some(status)) => break status,
            Ok(None) if Instant::now() >= deadline => {
                let _ = child.kill();
                let _ = child.wait();
                return Err(format!("{what} exceeded {CHILD_LIMIT:?}"));
            }
            Ok(None) => std::thread::sleep(Duration::from_millis(50)),
            Err(error) => return Err(format!("poll {what}: {error}")),
        }
    };
    let stdout = stdout_thread
        .join()
        .map_err(|_| format!("{what} stdout thread"))?
        .map_err(|error| format!("read {what} stdout: {error}"))?;
    let stderr = stderr_thread
        .join()
        .map_err(|_| format!("{what} stderr thread"))?
        .map_err(|error| format!("read {what} stderr: {error}"))?;
    Ok(Output {
        status,
        stdout,
        stderr,
    })
}

const JARS: &[(&str, &str)] = &[
    (
        "kafka-clients-3.9.0.jar",
        "https://repo1.maven.org/maven2/org/apache/kafka/kafka-clients/3.9.0/kafka-clients-3.9.0.jar",
    ),
    (
        "slf4j-api-1.7.36.jar",
        "https://repo1.maven.org/maven2/org/slf4j/slf4j-api/1.7.36/slf4j-api-1.7.36.jar",
    ),
    (
        "slf4j-simple-1.7.36.jar",
        "https://repo1.maven.org/maven2/org/slf4j/slf4j-simple/1.7.36/slf4j-simple-1.7.36.jar",
    ),
    (
        "lz4-java-1.8.0.jar",
        "https://repo1.maven.org/maven2/org/lz4/lz4-java/1.8.0/lz4-java-1.8.0.jar",
    ),
    (
        "snappy-java-1.1.10.5.jar",
        "https://repo1.maven.org/maven2/org/xerial/snappy/snappy-java/1.1.10.5/snappy-java-1.1.10.5.jar",
    ),
    (
        "zstd-jni-1.5.6-4.jar",
        "https://repo1.maven.org/maven2/com/github/luben/zstd-jni/1.5.6-4/zstd-jni-1.5.6-4.jar",
    ),
];

fn jar_dir() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../../target/kafka-3.9-admin-jars")
}

fn ensure_jars(dir: &Path) -> Result<(), String> {
    std::fs::create_dir_all(dir).map_err(|error| format!("create jar cache: {error}"))?;
    for (name, url) in JARS {
        let path = dir.join(name);
        if path.exists() {
            continue;
        }
        let output = match try_output_bounded(
            Command::new("curl")
                .args([
                    "--fail",
                    "--silent",
                    "--show-error",
                    "--location",
                    "--max-time",
                    "60",
                    "-o",
                ])
                .arg(&path)
                .arg(url),
            name,
        ) {
            Ok(output) => output,
            Err(error) => {
                let _ = std::fs::remove_file(&path);
                return Err(error);
            }
        };
        if !output.status.success() {
            let _ = std::fs::remove_file(&path);
            return Err(format!(
                "download {url} failed: {}{}",
                output.status,
                String::from_utf8_lossy(&output.stderr)
            ));
        }
    }
    Ok(())
}

fn classpath(jars: &Path, classes: &Path) -> String {
    let mut parts = vec![classes.display().to_string()];
    for (name, _) in JARS {
        parts.push(jars.join(name).display().to_string());
    }
    parts.join(":")
}

async fn spawn_gateway(server: &TestServer) -> SocketAddr {
    let bridge = IggyBridge::connect(server.test_config())
        .await
        .expect("bridge connects");
    bridge
        .ensure_stream_and_topic(TOPIC, 1)
        .await
        .expect("create topic");
    let listener = bind_listener("127.0.0.1:0").expect("bind");
    let addr = listener.local_addr().expect("local addr");
    let gateway = KafkaGateway::new(GatewayConfig {
        bind_addr: addr.to_string(),
        ..GatewayConfig::default()
    })
    .with_bridge(Some(Arc::new(bridge)));
    let (shutdown, receiver) = broadcast::channel::<()>(1);
    tokio::spawn(async move {
        let _ = gateway.run(listener, receiver).await;
    });
    std::mem::forget(shutdown);
    addr
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn kafka_39_admin_client_describes_and_alters_topic_configs() {
    if java_missing() {
        return;
    }

    let jars = jar_dir();
    if let Err(reason) = ensure_jars(&jars)
        && skip(&reason)
    {
        return;
    }
    let classes = tempfile::tempdir().expect("class dir");
    let source = PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("tests/java/ConfigsProbe.java");
    let javac = output_bounded(
        Command::new("javac")
            .arg("--release")
            .arg("17")
            .arg("-cp")
            .arg(classpath(&jars, classes.path()))
            .arg("-d")
            .arg(classes.path())
            .arg(&source),
        "javac",
    );
    assert!(
        javac.status.success(),
        "javac failed\n{}",
        String::from_utf8_lossy(&javac.stderr)
    );

    let data_dir = tempfile::tempdir().expect("tempdir");
    let server = TestServer::spawn(data_dir.path()).await;
    let addr = spawn_gateway(&server).await;

    let output = output_bounded(
        Command::new("java")
            .arg("-cp")
            .arg(classpath(&jars, classes.path()))
            .arg("ConfigsProbe")
            .arg(addr.to_string())
            .arg(TOPIC),
        "java ConfigsProbe",
    );
    let text = format!(
        "{}{}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    );
    println!("{text}");
    assert!(
        output.status.success(),
        "ConfigsProbe failed ({:?})\n{text}",
        output.status.code()
    );
    assert!(text.contains("CONFIGS_PROBE_OK"), "{text}");
    assert!(
        text.contains("describe_batch_50_elapsed_ms="),
        "missing timing line\n{text}"
    );
    assert!(
        text.contains("retention.ms=-1 source=DEFAULT_CONFIG"),
        "{text}"
    );
    assert!(
        text.contains("cleanup.policy=delete read_only=true source=DEFAULT_CONFIG"),
        "{text}"
    );
    assert!(
        text.contains("unknown_key_rejected=InvalidConfigurationException"),
        "{text}"
    );
    assert!(text.contains("validate_only_retention.ms=-1"), "{text}");
    assert!(
        text.contains("altered_retention.ms=8000 source=DYNAMIC_TOPIC_CONFIG"),
        "{text}"
    );
}
