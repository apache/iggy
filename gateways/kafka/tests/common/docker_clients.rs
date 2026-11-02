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

//! Kafka clients in Docker containers, which a test leaves running while it stops and starts
//! gateways and nodes.
//!
//! The containers use the host network, so a client reaches the gateways on the host's loopback.
//! Only Linux does that. They also run with `--pull never`, so a registry download never eats into
//! a test's budget: CI pulls the images before the tests start.
#![allow(dead_code)]

use std::fs::File;
use std::io::{BufRead, BufReader, Read, Write};
use std::path::{Path, PathBuf};
use std::process::{Child, Command, Stdio};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex, PoisonError};
use std::thread::JoinHandle;
use std::time::Duration;

use tokio::time::Instant;

/// Numbers the containers of one test process, so their names differ.
static NEXT_CONTAINER: AtomicUsize = AtomicUsize::new(0);

/// Why the clients cannot run on this host, or `None` when they can.
pub fn missing_prerequisite(images: &[&str]) -> Option<String> {
    if !cfg!(target_os = "linux") {
        return Some("`docker run --network host` reaches the host loopback only on Linux".into());
    }
    if !succeeds(Command::new("docker").arg("info")) {
        return Some("docker is unavailable".into());
    }
    if let Some(image) = images
        .iter()
        .find(|image| !succeeds(Command::new("docker").args(["image", "inspect", image])))
    {
        return Some(format!(
            "client image {image} is not pulled; run `docker pull {image}`"
        ));
    }
    if !succeeds(Command::new("timeout").args(["--kill-after=1s", "1s", "true"])) {
        return Some("GNU `timeout` with `--kill-after` is unavailable".into());
    }
    None
}

/// Reports why a test skips. Fails it instead when `KAFKA_E2E_REQUIRED=1`, as CI sets it, so a
/// broken prerequisite cannot pass a test over zero assertions.
pub fn skip(reason: &str) {
    assert!(
        std::env::var("KAFKA_E2E_REQUIRED").as_deref() != Ok("1"),
        "KAFKA_E2E_REQUIRED=1 but the suite cannot run: {reason}"
    );
    eprintln!("skipping: {reason}");
}

fn succeeds(command: &mut Command) -> bool {
    command
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .status()
        .is_ok_and(|status| status.success())
}

/// A client container that runs until it exits or the test drops it. It keeps every line the
/// client prints, and writes them to log files as well.
pub struct Container {
    name: String,
    process: Child,
    stdout: Arc<Mutex<Vec<String>>>,
    stderr: Arc<Mutex<Vec<String>>>,
    readers: Vec<JoinHandle<()>>,
}

impl Container {
    /// Starts `args` in `image`, with each `(host file, guest path)` of `mounts` read-only and each
    /// `(name, value)` of `env` set. `timeout` inside the container ends it after `lifetime`, also
    /// when this process dies before it can remove the container. Its output goes to `<log>.out`
    /// and `<log>.err` as well.
    pub fn start(
        image: &str,
        args: &[&str],
        mounts: &[(&Path, &str)],
        env: &[(&str, &str)],
        lifetime: Duration,
        log: &Path,
    ) -> Self {
        let name = format!(
            "iggy-kafka-failover-{}-{}",
            std::process::id(),
            NEXT_CONTAINER.fetch_add(1, Ordering::Relaxed)
        );
        let mut command = Command::new("docker");
        command.args([
            "run",
            "--rm",
            "--pull",
            "never",
            "--network",
            "host",
            "--name",
            &name,
        ]);
        for (host, guest) in mounts {
            command
                .arg("-v")
                .arg(format!("{}:{guest}:ro", host.display()));
        }
        for (name, value) in env {
            command.arg("-e").arg(format!("{name}={value}"));
        }
        let mut process = command
            .arg(image)
            .args([
                "timeout",
                "--kill-after=10s",
                &format!("{}s", lifetime.as_secs()),
            ])
            .args(args)
            .stdout(Stdio::piped())
            .stderr(Stdio::piped())
            .spawn()
            .expect("start a client container");
        let (stdout, stdout_reader) = collect(
            process.stdout.take().expect("piped stdout"),
            log.with_extension("out"),
        );
        let (stderr, stderr_reader) = collect(
            process.stderr.take().expect("piped stderr"),
            log.with_extension("err"),
        );
        Self {
            name,
            process,
            stdout,
            stderr,
            readers: vec![stdout_reader, stderr_reader],
        }
    }

    /// Calls `read` with every line that the client printed to stdout so far.
    pub fn with_stdout<T>(&self, read: impl FnOnce(&[String]) -> T) -> T {
        read(&self.stdout.lock().unwrap_or_else(PoisonError::into_inner))
    }

    /// The lines on stdout so far that contain `needle`.
    pub fn count(&self, needle: &str) -> usize {
        self.with_stdout(|lines| lines.iter().filter(|line| line.contains(needle)).count())
    }

    /// The last `lines` lines that the client printed to stderr.
    pub fn stderr_tail(&self, lines: usize) -> String {
        let stderr = self.stderr.lock().unwrap_or_else(PoisonError::into_inner);
        stderr[stderr.len().saturating_sub(lines)..].join("\n")
    }

    /// The lines on stderr so far that contain `needle`.
    pub fn stderr_count(&self, needle: &str) -> usize {
        self.stderr
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .iter()
            .filter(|line| line.contains(needle))
            .count()
    }

    /// Sends `signal` to the client, as `docker kill --signal` does. `timeout`, which runs as the
    /// container's first process, passes it on to the client.
    pub fn signal(&self, signal: &str) {
        self.docker(&["kill", "--signal", signal]);
    }

    /// Freezes every process of the container, as `docker pause` does. The client sends nothing,
    /// heartbeats included, until [`Self::unpause`].
    pub fn pause(&self) {
        self.docker(&["pause"]);
    }

    pub fn unpause(&self) {
        self.docker(&["unpause"]);
    }

    /// Runs `docker <args> <name>`, and fails the test when it fails.
    fn docker(&self, args: &[&str]) {
        let done = Command::new("docker")
            .args(args)
            .arg(&self.name)
            .stdout(Stdio::null())
            .stderr(Stdio::null())
            .status()
            .is_ok_and(|status| status.success());
        assert!(done, "docker {} {}", args.join(" "), self.name);
    }

    /// Waits until the client exits, at most `limit`, and returns whether it did. Every line it
    /// printed is read by then.
    pub async fn wait_exit(&mut self, limit: Duration) -> bool {
        let deadline = Instant::now() + limit;
        while self
            .process
            .try_wait()
            .expect("poll the client container")
            .is_none()
        {
            if Instant::now() >= deadline {
                return false;
            }
            tokio::time::sleep(Duration::from_millis(100)).await;
        }
        for reader in self.readers.drain(..) {
            let _ = reader.join();
        }
        true
    }
}

impl Drop for Container {
    fn drop(&mut self) {
        // After a clean exit `--rm` already removed it, and this fails harmlessly.
        let _ = Command::new("docker")
            .args(["rm", "--force", &self.name])
            .stdout(Stdio::null())
            .stderr(Stdio::null())
            .status();
        let _ = self.process.kill();
        let _ = self.process.wait();
    }
}

/// Keeps each line of `pipe`, and writes it to `log`, on a thread of its own until the pipe
/// closes.
fn collect(
    pipe: impl Read + Send + 'static,
    log: PathBuf,
) -> (Arc<Mutex<Vec<String>>>, JoinHandle<()>) {
    let lines = Arc::new(Mutex::new(Vec::new()));
    let kept = Arc::clone(&lines);
    let reader = std::thread::spawn(move || {
        let mut file = File::create(&log).ok();
        for line in BufReader::new(pipe).lines().map_while(Result::ok) {
            if let Some(file) = file.as_mut() {
                let _ = writeln!(file, "{line}");
            }
            kept.lock()
                .unwrap_or_else(PoisonError::into_inner)
                .push(line);
        }
    });
    (lines, reader)
}

/// What a client that ran to its end printed.
pub struct Finished {
    pub succeeded: bool,
    pub stdout: String,
    pub stderr: String,
}

/// Runs `args` in `image` until the client exits, at most `limit`. Blocks the calling thread.
pub fn run_to_end(image: &str, args: &[String], limit: Duration) -> Finished {
    let output = Command::new("timeout")
        .args(["--kill-after=10s", &format!("{}s", limit.as_secs())])
        .args([
            "docker",
            "run",
            "--rm",
            "--pull",
            "never",
            "--network",
            "host",
            image,
        ])
        .args(args)
        .output()
        .expect("run a client container");
    Finished {
        succeeded: output.status.success(),
        stdout: String::from_utf8_lossy(&output.stdout).into_owned(),
        stderr: String::from_utf8_lossy(&output.stderr).into_owned(),
    }
}
