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

//! The client compatibility matrix's plumbing: client containers, the probe contract's event
//! lines, the facts they turn into, and the matrix report.
//!
//! A probe (`tests/clients/<client>/`) prints one event per line on stdout:
//!
//! ```text
//! ACK <phase> <partition|-> <offset|->          one record the broker acknowledged
//! NACK <phase> <reason>                          one record or call that failed
//! REC <phase> <partition> <offset> <key> <value> <name=value,...|->
//! COMMITTED <phase> <partition> <offset>         a group's committed offset (-1: none)
//! DONE                                           the probe reached its end
//! ```
//!
//! [`facts_from_events`] turns those into facts (`tests/common/facts.rs`) that are the same on
//! every broker that behaves like Kafka: counts, per-partition offset ranges, committed offsets,
//! whether every record came back intact and in order. Nothing broker-chosen is recorded.

use std::collections::{BTreeMap, BTreeSet};
use std::fmt::Write as _;
use std::path::{Path, PathBuf};
use std::process::{Child, Command, Stdio};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::{Duration, Instant};

use super::facts::{Allowed, Difference, Facts, Verdict};
use super::oracle::TargetKind;

/// Records per produced batch. Small: the matrix tests compatibility, not throughput.
pub const COUNT: usize = 12;

/// Partitions of every topic the matrix creates.
pub const PARTITIONS: i32 = 3;

/// Wall-clock cap on one client container that is expected to exit by itself.
pub const CLIENT_TIMEOUT: Duration = Duration::from_secs(150);

static NEXT_CONTAINER: AtomicUsize = AtomicUsize::new(0);

/// The container CLI: `docker` unless `KAFKA_ORACLE_CONTAINER_CLI` names another (`podman`).
#[must_use]
pub fn container_cli() -> String {
    std::env::var("KAFKA_ORACLE_CONTAINER_CLI").unwrap_or_else(|_| "docker".to_string())
}

/// Whether the CLI is rootless Podman, where root in a container is the invoking user. Under
/// Docker, containers that write into a host directory run as the invoking user instead, so
/// the files they leave stay removable.
fn rootless_podman() -> bool {
    Command::new(container_cli())
        .arg("--version")
        .output()
        .is_ok_and(|output| String::from_utf8_lossy(&output.stdout).contains("podman"))
}

/// `--user uid:gid` under Docker; nothing under Podman.
#[must_use]
pub fn host_user_args() -> Vec<String> {
    if rootless_podman() {
        return Vec::new();
    }
    let id = |flag: &str| {
        Command::new("id")
            .arg(flag)
            .output()
            .ok()
            .map(|output| String::from_utf8_lossy(&output.stdout).trim().to_string())
            .filter(|value| !value.is_empty())
    };
    match (id("-u"), id("-g")) {
        (Some(uid), Some(gid)) => vec!["--user".to_string(), format!("{uid}:{gid}")],
        _ => Vec::new(),
    }
}

/// What one client container did.
#[derive(Debug, Clone)]
pub struct Run {
    pub ok: bool,
    pub status: String,
    pub stdout: String,
    pub stderr: String,
    pub elapsed: Duration,
}

impl Run {
    /// The last lines of its output, for a failure message.
    #[must_use]
    pub fn tail(&self) -> String {
        let mut lines: Vec<&str> = self.stdout.lines().chain(self.stderr.lines()).collect();
        let keep = lines.len().saturating_sub(40);
        lines.drain(..keep);
        lines.join("\n")
    }
}

/// A unique container name.
#[must_use]
pub fn container_name(label: &str) -> String {
    format!(
        "iggy-matrix-{label}-{}-{}",
        std::process::id(),
        NEXT_CONTAINER.fetch_add(1, Ordering::Relaxed)
    )
}

/// `docker run --rm --network host --pull never` with `args` (options, image, command), bounded
/// by `limit`. The container is removed whatever happens.
#[must_use]
pub fn run_container(label: &str, args: &[String], limit: Duration) -> Run {
    let name = container_name(label);
    let started = Instant::now();
    let output = Command::new("timeout")
        .arg("--kill-after=10s")
        .arg(format!("{}s", limit.as_secs()))
        .arg(container_cli())
        .args([
            "run",
            "--rm",
            "--network",
            "host",
            "--pull",
            "never",
            "--name",
        ])
        .arg(&name)
        .args(args)
        .stdin(Stdio::null())
        .output();
    let _ = Command::new(container_cli())
        .args(["rm", "-f", &name])
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .status();
    match output {
        Ok(output) => Run {
            ok: output.status.success(),
            status: output.status.to_string(),
            stdout: String::from_utf8_lossy(&output.stdout).into_owned(),
            stderr: String::from_utf8_lossy(&output.stderr).into_owned(),
            elapsed: started.elapsed(),
        },
        Err(error) => Run {
            ok: false,
            status: format!("could not run {}: {error}", container_cli()),
            stdout: String::new(),
            stderr: String::new(),
            elapsed: started.elapsed(),
        },
    }
}

/// A detached container (a broker-side service such as Connect or `PostgreSQL`), removed on drop.
pub struct Service {
    pub name: String,
}

impl Service {
    pub fn start(label: &str, args: &[String]) -> Result<Self, String> {
        let name = container_name(label);
        let output = Command::new(container_cli())
            // No --rm: a service that exits early keeps its logs for the failure message. Drop
            // removes it.
            .args([
                "run",
                "-d",
                "--network",
                "host",
                "--pull",
                "never",
                "--name",
            ])
            .arg(&name)
            .args(args)
            .output()
            .map_err(|error| format!("start {label}: {error}"))?;
        if output.status.success() {
            Ok(Self { name })
        } else {
            Err(format!(
                "start {label}: {}",
                String::from_utf8_lossy(&output.stderr).trim()
            ))
        }
    }

    /// Runs `command` inside the service container.
    #[must_use]
    pub fn exec(&self, command: &[&str]) -> Run {
        let started = Instant::now();
        let output = Command::new("timeout")
            .args(["--kill-after=5s", "60s"])
            .arg(container_cli())
            .arg("exec")
            .arg(&self.name)
            .args(command)
            .output();
        match output {
            Ok(output) => Run {
                ok: output.status.success(),
                status: output.status.to_string(),
                stdout: String::from_utf8_lossy(&output.stdout).into_owned(),
                stderr: String::from_utf8_lossy(&output.stderr).into_owned(),
                elapsed: started.elapsed(),
            },
            Err(error) => Run {
                ok: false,
                status: error.to_string(),
                stdout: String::new(),
                stderr: String::new(),
                elapsed: started.elapsed(),
            },
        }
    }

    #[must_use]
    pub fn logs_tail(&self, lines: usize) -> String {
        Command::new(container_cli())
            .args(["logs", "--tail", &lines.to_string(), &self.name])
            .output()
            .map_or_default(|output| {
                let mut text = String::from_utf8_lossy(&output.stdout).into_owned();
                text.push_str(&String::from_utf8_lossy(&output.stderr));
                text
            })
    }

    /// Stops the container gracefully (SIGTERM, then a kill after `grace`).
    pub fn stop(&self, grace: Duration) {
        let _ = Command::new(container_cli())
            .args(["stop", "-t", &grace.as_secs().to_string(), &self.name])
            .stdout(Stdio::null())
            .stderr(Stdio::null())
            .status();
    }
}

impl Drop for Service {
    fn drop(&mut self) {
        let _ = Command::new(container_cli())
            .args(["rm", "-f", &self.name])
            .stdout(Stdio::null())
            .stderr(Stdio::null())
            .status();
    }
}

/// A client left running whose stdout is collected, removed on drop.
pub struct Streaming {
    name: String,
    child: Child,
    lines: std::sync::mpsc::Receiver<String>,
    pub seen: Vec<String>,
}

impl Streaming {
    pub fn start(label: &str, args: &[String]) -> Result<Self, String> {
        let name = container_name(label);
        let mut child = Command::new(container_cli())
            .args([
                "run",
                "--rm",
                "--network",
                "host",
                "--pull",
                "never",
                "--name",
            ])
            .arg(&name)
            .args(args)
            .stdin(Stdio::null())
            .stdout(Stdio::piped())
            .stderr(Stdio::null())
            .spawn()
            .map_err(|error| format!("start {label}: {error}"))?;
        let stdout = child.stdout.take().ok_or("stdout is piped")?;
        let (sender, lines) = std::sync::mpsc::channel();
        std::thread::spawn(move || {
            use std::io::BufRead as _;
            for line in std::io::BufReader::new(stdout)
                .lines()
                .map_while(Result::ok)
            {
                if sender.send(line).is_err() {
                    break;
                }
            }
        });
        Ok(Self {
            name,
            child,
            lines,
            seen: Vec::new(),
        })
    }

    /// Collects lines until `done` holds over them or `limit` passes. Returns whether it held.
    pub fn wait_for(&mut self, limit: Duration, done: impl Fn(&[String]) -> bool) -> bool {
        let deadline = Instant::now() + limit;
        while !done(&self.seen) {
            let Some(left) = deadline.checked_duration_since(Instant::now()) else {
                return false;
            };
            match self
                .lines
                .recv_timeout(left.min(Duration::from_millis(500)))
            {
                Ok(line) => self.seen.push(line),
                Err(std::sync::mpsc::RecvTimeoutError::Timeout) => {}
                Err(std::sync::mpsc::RecvTimeoutError::Disconnected) => return done(&self.seen),
            }
        }
        true
    }

    /// Stops the client with SIGTERM, so it shuts down (and commits) as an operator's stop would.
    pub fn stop(mut self, grace: Duration) -> Vec<String> {
        let _ = Command::new(container_cli())
            .args(["stop", "-t", &grace.as_secs().to_string(), &self.name])
            .stdout(Stdio::null())
            .stderr(Stdio::null())
            .status();
        let _ = self.child.wait();
        // The reader thread can still be handing over lines the container wrote just before it
        // exited. Drain until it closes the channel, not until the channel is momentarily empty.
        while let Ok(line) = self.lines.recv() {
            self.seen.push(line);
        }
        std::mem::take(&mut self.seen)
    }
}

impl Drop for Streaming {
    fn drop(&mut self) {
        let _ = Command::new(container_cli())
            .args(["rm", "-f", &self.name])
            .stdout(Stdio::null())
            .stderr(Stdio::null())
            .status();
        let _ = self.child.kill();
        let _ = self.child.wait();
    }
}

/// Where the matrix keeps what it builds (Python packages, the franz-go binary, Java classes):
/// under Cargo's target directory, never `/tmp`.
#[must_use]
pub fn cache_dir(sub: &str) -> PathBuf {
    let dir = PathBuf::from(env!("CARGO_TARGET_TMPDIR"))
        .join("client-matrix")
        .join(sub);
    std::fs::create_dir_all(&dir).expect("create the client matrix cache dir");
    dir
}

/// The crate's `tests/clients` directory.
#[must_use]
pub fn clients_dir() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("tests/clients")
}

#[must_use]
pub fn path_str(path: &Path) -> &str {
    path.to_str().expect("paths under the target dir are UTF-8")
}

/// A short, stable digest of `parts`, for cache keys (FNV-1a).
#[must_use]
pub fn digest(parts: &[&[u8]]) -> String {
    let mut hash: u64 = 0xcbf2_9ce4_8422_2325;
    for part in parts {
        for byte in *part {
            hash ^= u64::from(*byte);
            hash = hash.wrapping_mul(0x0100_0000_01b3);
        }
        hash ^= 0xff;
        hash = hash.wrapping_mul(0x0100_0000_01b3);
    }
    format!("{hash:016x}")
}

/// One parsed event line.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Event {
    Ack {
        phase: String,
        partition: Option<i32>,
        offset: Option<i64>,
    },
    Nack {
        phase: String,
        reason: String,
    },
    Rec {
        phase: String,
        partition: i32,
        offset: i64,
        key: String,
        value: String,
        headers: String,
    },
    Committed {
        phase: String,
        partition: i32,
        offset: i64,
    },
    Done,
}

/// The events in `stdout`; lines that are not events (client logging) are ignored.
#[must_use]
pub fn parse_events(stdout: &str) -> Vec<Event> {
    stdout.lines().filter_map(parse_event).collect()
}

fn number<T: std::str::FromStr>(word: Option<&str>) -> Option<T> {
    word.and_then(|word| word.parse().ok())
}

fn parse_event(line: &str) -> Option<Event> {
    let mut words = line.split(' ');
    match words.next()? {
        "ACK" => Some(Event::Ack {
            phase: words.next()?.to_string(),
            partition: number(words.next()),
            offset: number(words.next()),
        }),
        "NACK" => Some(Event::Nack {
            phase: words.next()?.to_string(),
            reason: words.collect::<Vec<_>>().join("_"),
        }),
        "REC" => Some(Event::Rec {
            phase: words.next()?.to_string(),
            partition: number(words.next())?,
            offset: number(words.next())?,
            key: words.next()?.to_string(),
            value: words.next()?.to_string(),
            headers: words.next().unwrap_or("-").to_string(),
        }),
        "COMMITTED" => Some(Event::Committed {
            phase: words.next()?.to_string(),
            partition: number(words.next())?,
            offset: number(words.next())?,
        }),
        "DONE" => Some(Event::Done),
        _ => None,
    }
}

/// What a record of index `index` carries, as the probes write it.
#[must_use]
pub fn expected_record(index: usize) -> (String, String, String) {
    (
        format!("k{index:03}"),
        format!("v{index:03}"),
        format!("idx={index},src=matrix"),
    )
}

/// The index a key `kNNN` names.
#[must_use]
pub fn key_index(key: &str) -> Option<usize> {
    key.strip_prefix('k')?.parse().ok()
}

/// A value the Python probe printed, with its percent-escapes undone.
#[must_use]
pub fn unescape(word: &str) -> String {
    if word == "%" {
        return String::new();
    }
    word.replace("%20", " ")
        .replace("%0A", "\n")
        .replace("%25", "%")
}

/// How a record's value is checked.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Values {
    /// `vNNN`, with the probe headers.
    Probe,
    /// Only the key is checked; the value and headers are the client's own encoding.
    KeyOnly,
    /// A JSON `{"i": N, "v": "vNNN"}` value, from a stream processor that interleaves its input
    /// partitions, so the order of indexes within an output partition is not a fact.
    JsonUnordered,
}

/// An acknowledged record's partition and offset, when the client reports them.
type Acked = (Option<i32>, Option<i64>);

/// Records facts about every phase in `events`.
pub fn facts_from_events(events: &[Event], values: Values, facts: &mut Facts) {
    let mut acks: BTreeMap<&str, Vec<Acked>> = BTreeMap::new();
    let mut nacks: BTreeMap<String, BTreeSet<String>> = BTreeMap::new();
    let mut nack_counts: BTreeMap<String, usize> = BTreeMap::new();
    let mut records: BTreeMap<&str, Vec<&Event>> = BTreeMap::new();
    let mut committed: BTreeMap<&str, BTreeMap<i32, i64>> = BTreeMap::new();
    let mut done = false;
    for event in events {
        match event {
            Event::Ack {
                phase,
                partition,
                offset,
            } => acks.entry(phase).or_default().push((*partition, *offset)),
            Event::Nack { phase, reason } => {
                nacks
                    .entry(phase.clone())
                    .or_default()
                    .insert(reason.clone());
                *nack_counts.entry(phase.clone()).or_default() += 1;
            }
            Event::Rec { phase, .. } => records.entry(phase).or_default().push(event),
            Event::Committed {
                phase,
                partition,
                offset,
            } => {
                committed
                    .entry(phase)
                    .or_default()
                    .insert(*partition, *offset);
            }
            Event::Done => done = true,
        }
    }
    facts.set("done", done);
    for (phase, acked) in &acks {
        facts.set(format!("{phase}.acked"), acked.len());
        let mut by_partition: BTreeMap<i32, Vec<i64>> = BTreeMap::new();
        for (partition, offset) in acked {
            if let (Some(partition), Some(offset)) = (partition, offset) {
                by_partition.entry(*partition).or_default().push(*offset);
            }
        }
        for (partition, offsets) in by_partition {
            facts.set(format!("{phase}.partition.{partition}"), span(&offsets));
        }
    }
    for (phase, reasons) in &nacks {
        facts.set(
            format!("{phase}.failed"),
            format!(
                "{} ({})",
                nack_counts[phase],
                reasons.iter().cloned().collect::<Vec<_>>().join(",")
            ),
        );
    }
    for (phase, seen) in &records {
        record_facts(phase, seen, values, facts);
    }
    for (phase, offsets) in &committed {
        for partition in 0..PARTITIONS {
            facts.set(
                format!("{phase}.committed.{partition}"),
                offsets.get(&partition).copied().unwrap_or(-1),
            );
        }
    }
}

fn span(offsets: &[i64]) -> String {
    match (offsets.iter().min(), offsets.iter().max()) {
        (Some(first), Some(last)) => format!("{} at {first}-{last}", offsets.len()),
        _ => "0".to_string(),
    }
}

/// The record indexes a phase carries. Every scenario produces a first batch `0..COUNT` and a
/// second `COUNT..2 * COUNT`; a resumed consumer reads the second, and a reader that sees the
/// whole topic sees every batch so far.
fn expected_indexes(phase: &str) -> std::ops::Range<usize> {
    match phase {
        "resume" => COUNT..2 * COUNT,
        "stream" => 0..2 * COUNT,
        "restart" => 0..3 * COUNT,
        _ => 0..COUNT,
    }
}

/// Whether a JSON sink value carries `index` and the value of `want_value` for it.
fn json_record_matches(value: &str, index: usize, want_value: &str) -> bool {
    let Ok(value) = serde_json::from_str::<serde_json::Value>(&unescape(value)) else {
        return false;
    };
    value.get("i").and_then(serde_json::Value::as_u64) == u64::try_from(index).ok()
        && value.get("v").and_then(serde_json::Value::as_str) == Some(want_value)
}

fn record_facts(phase: &str, seen: &[&Event], values: Values, facts: &mut Facts) {
    let expected = expected_indexes(phase);
    let mut intact = 0;
    let mut keys = BTreeMap::<String, usize>::new();
    let mut by_partition: BTreeMap<i32, Vec<(i64, Option<usize>)>> = BTreeMap::new();
    let mut lowest = usize::MAX;
    for event in seen {
        let Event::Rec {
            partition,
            offset,
            key,
            value,
            headers,
            ..
        } = event
        else {
            continue;
        };
        *keys.entry(key.clone()).or_default() += 1;
        let index = key_index(key).filter(|index| expected.contains(index));
        if let Some(index) = index {
            lowest = lowest.min(index);
            let (_, want_value, want_headers) = expected_record(index);
            let matches = match values {
                Values::Probe => *value == want_value && *headers == want_headers,
                Values::KeyOnly => true,
                Values::JsonUnordered => json_record_matches(value, index, &want_value),
            };
            if matches {
                intact += 1;
            }
        }
        by_partition
            .entry(*partition)
            .or_default()
            .push((*offset, index));
    }
    facts.set(format!("{phase}.count"), seen.len());
    facts.set(format!("{phase}.intact"), intact);
    facts.set(
        format!("{phase}.duplicates"),
        keys.values().map(|count| count - 1).sum::<usize>(),
    );
    let ordered = by_partition.values().all(|records| {
        records.windows(2).all(|pair| {
            pair[0].0 < pair[1].0 && (values == Values::JsonUnordered || pair[0].1 < pair[1].1)
        })
    });
    facts.set(format!("{phase}.ordered"), ordered);
    if lowest != usize::MAX {
        facts.set(format!("{phase}.first_index"), lowest);
    }
    for (partition, records) in by_partition {
        let offsets: Vec<i64> = records.iter().map(|(offset, _)| *offset).collect();
        facts.set(format!("{phase}.partition.{partition}"), span(&offsets));
    }
}

/// The matrix column a fact belongs to.
#[must_use]
pub fn step_of(fact: &str) -> &'static str {
    let phase = fact.split('.').next().unwrap_or("");
    if fact.contains(".committed.") || fact.contains(".commit") {
        return "commit";
    }
    match phase {
        "produce" | "produce2" => "produce",
        "consume" => "consume",
        "resume" => "resume",
        "done" | "exit" => "run",
        other => {
            // Clients with their own steps (Connect, Flink, ...) name facts `<step>.<what>`.
            STEPS
                .iter()
                .copied()
                .find(|step| *step == other)
                .unwrap_or("other")
        }
    }
}

/// Every column, in report order. A client shows `n/a` where it records no fact.
pub const STEPS: [&str; 11] = [
    "run", "produce", "consume", "commit", "resume", "sink", "source", "start", "snapshot",
    "stream", "restart",
];

/// The result of one step of one client on the gateway.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Cell {
    Pass,
    /// Differs from Kafka in a way `allowed_differences.toml` declares, with its issues.
    Declared(BTreeSet<String>),
    Fail,
    NotApplicable,
}

impl Cell {
    #[must_use]
    pub fn render(&self) -> String {
        match self {
            Self::Pass => "pass".to_string(),
            Self::Declared(issues) => {
                format!(
                    "fail, known ({})",
                    issues.iter().cloned().collect::<Vec<_>>().join(", ")
                )
            }
            Self::Fail => "**FAIL**".to_string(),
            Self::NotApplicable => "n/a".to_string(),
        }
    }
}

/// One client's row: its version string and a cell per step.
#[derive(Debug, Clone)]
pub struct Row {
    pub client: String,
    pub version: String,
    pub cells: BTreeMap<&'static str, Cell>,
    /// Why the client did not get as far as a comparison (oracle run broken, setup failed).
    pub broken: Option<String>,
    pub seconds: (f64, f64),
}

/// Builds a client's row from its gateway and Kafka facts and the run's verdict.
#[must_use]
pub fn row(
    client: &str,
    version: &str,
    gateway: &Facts,
    kafka: &Facts,
    verdict: &Verdict,
    seconds: (f64, f64),
) -> Row {
    let mut cells = BTreeMap::new();
    for step in STEPS {
        let present = gateway
            .inner()
            .keys()
            .chain(kafka.inner().keys())
            .any(|fact| step_of(fact) == step);
        if !present {
            cells.insert(step, Cell::NotApplicable);
            continue;
        }
        let failed = verdict
            .unexpected
            .iter()
            .any(|difference| step_of(&difference.fact) == step);
        let declared: BTreeSet<String> = verdict
            .declared
            .iter()
            .filter(|(difference, _)| step_of(&difference.fact) == step)
            .map(|(_, issue)| issue.clone())
            .collect();
        let cell = if failed {
            Cell::Fail
        } else if declared.is_empty() {
            Cell::Pass
        } else {
            Cell::Declared(declared)
        };
        cells.insert(step, cell);
    }
    Row {
        client: client.to_string(),
        version: version.to_string(),
        cells,
        broken: None,
        seconds,
    }
}

/// The steps any row in `rows` has a result for, in report order.
fn used_steps(rows: &[Row]) -> Vec<&'static str> {
    STEPS
        .iter()
        .copied()
        .filter(|step| {
            rows.iter().any(|row| {
                row.cells
                    .get(step)
                    .is_some_and(|cell| *cell != Cell::NotApplicable)
            })
        })
        .collect()
}

/// The matrix as Markdown: one row per client, one column per step, then every difference.
#[must_use]
pub fn render(title: &str, rows: &[Row], details: &[(String, Verdict)]) -> String {
    let steps = used_steps(rows);
    let mut out = String::new();
    let _ = writeln!(out, "### {title}\n");
    let _ = write!(out, "| Client | Version |");
    for step in &steps {
        let _ = write!(out, " {step} |");
    }
    let _ = writeln!(out, " Seconds (gateway / kafka) |");
    let _ = write!(out, "| --- | --- |");
    for _ in &steps {
        let _ = write!(out, " --- |");
    }
    let _ = writeln!(out, " --- |");
    for row in rows {
        let _ = write!(out, "| {} | {} |", row.client, row.version);
        for step in &steps {
            let cell = match &row.broken {
                Some(_) => "**broken**".to_string(),
                None => row
                    .cells
                    .get(step)
                    .map_or_else(|| "n/a".to_string(), Cell::render),
            };
            let _ = write!(out, " {cell} |");
        }
        let _ = writeln!(out, " {:.0} / {:.0} |", row.seconds.0, row.seconds.1);
    }
    for row in rows {
        if let Some(reason) = &row.broken {
            let _ = writeln!(out, "\n**{} broken:** {reason}", row.client);
        }
    }
    let differences: Vec<String> = details
        .iter()
        .filter(|(_, verdict)| {
            !verdict.unexpected.is_empty()
                || !verdict.declared.is_empty()
                || !verdict.stale.is_empty()
        })
        .map(|(client, verdict)| format!("{client}:\n{}", verdict.report()))
        .collect();
    if !differences.is_empty() {
        let _ = writeln!(out, "\n```text");
        for block in differences {
            let _ = write!(out, "{block}");
        }
        let _ = writeln!(out, "```");
    }
    out
}

/// Writes `markdown` to `$KAFKA_CLIENT_MATRIX_DIR/<file>` when that is set.
pub fn publish(file: &str, markdown: &str) {
    if let Some(dir) = std::env::var_os("KAFKA_CLIENT_MATRIX_DIR") {
        let dir = PathBuf::from(dir);
        let _ = std::fs::create_dir_all(&dir);
        let _ = std::fs::write(dir.join(file), markdown);
    }
}

/// The scenario name a client's facts are judged under in `allowed_differences.toml`.
#[must_use]
pub fn scenario(client: &str) -> String {
    format!("client/{client}")
}

/// Every difference between the gateway's and Kafka's facts of `client`, judged.
#[must_use]
pub fn judge_client(client: &str, gateway: &Facts, kafka: &Facts, allowed: &[Allowed]) -> Verdict {
    let name = scenario(client);
    let differences: Vec<Difference> =
        super::facts::compare(&name, gateway, TargetKind::Kafka, kafka);
    super::facts::judge(&differences, allowed, &[(name, TargetKind::Kafka)])
}

#[test]
fn events_turn_into_facts_that_ignore_broker_choices() {
    let stdout = "noise\nACK produce 0 0\nACK produce 0 1\nACK produce 2 0\n\
                  REC consume 0 0 k000 v000 idx=0,src=matrix\n\
                  REC consume 0 1 k001 v001 idx=1,src=matrix\n\
                  REC consume 2 0 k002 vXXX -\n\
                  COMMITTED consume 0 2\nNACK resume.commit Timeout\nDONE\n";
    let mut facts = Facts::default();
    facts_from_events(&parse_events(stdout), Values::Probe, &mut facts);
    assert_eq!(facts.get("produce.acked"), Some("3"));
    assert_eq!(facts.get("produce.partition.0"), Some("2 at 0-1"));
    assert_eq!(facts.get("consume.count"), Some("3"));
    assert_eq!(facts.get("consume.intact"), Some("2"));
    assert_eq!(facts.get("consume.ordered"), Some("true"));
    assert_eq!(facts.get("consume.committed.0"), Some("2"));
    assert_eq!(facts.get("consume.committed.1"), Some("-1"));
    assert_eq!(facts.get("resume.commit.failed"), Some("1 (Timeout)"));
    assert_eq!(facts.get("done"), Some("true"));
    assert_eq!(step_of("consume.committed.1"), "commit");
    assert_eq!(step_of("resume.commit.failed"), "commit");
    assert_eq!(step_of("produce2.acked"), "produce");
    assert_eq!(step_of("snapshot.count"), "snapshot");
}

#[test]
fn a_duplicate_or_a_reordering_is_a_fact() {
    let stdout = "REC resume 1 5 k013 v013 idx=13,src=matrix\n\
                  REC resume 1 4 k012 v012 idx=12,src=matrix\n\
                  REC resume 1 5 k013 v013 idx=13,src=matrix\n";
    let mut facts = Facts::default();
    facts_from_events(&parse_events(stdout), Values::Probe, &mut facts);
    assert_eq!(facts.get("resume.duplicates"), Some("1"));
    assert_eq!(facts.get("resume.ordered"), Some("false"));
    assert_eq!(facts.get("resume.first_index"), Some("12"));
}

#[test]
fn span_reads_the_extrema_without_reordering() {
    assert_eq!(span(&[5, 1, 9, 3]), "4 at 1-9");
    assert_eq!(span(&[]), "0");
}

#[test]
fn an_unexpected_key_is_not_intact() {
    let stdout = "REC sink 0 0 k000 {\"i\":0,\"v\":\"v000\"} -\n\
                  REC sink 0 1 k012 {\"i\":12,\"v\":\"v012\"} -\n";
    let mut facts = Facts::default();
    facts_from_events(&parse_events(stdout), Values::JsonUnordered, &mut facts);
    assert_eq!(facts.get("sink.count"), Some("2"));
    assert_eq!(facts.get("sink.intact"), Some("1"));
}

#[test]
fn a_json_value_that_does_not_match_its_key_is_not_intact() {
    let stdout = "REC sink 0 0 k000 {\"i\":0,\"v\":\"v999\"} -\n\
                  REC sink 0 1 k001 {\"i\":2,\"v\":\"v001\"} -\n\
                  REC sink 0 2 k002 {\"i\":2,\"v\":\"v002\"} -\n";
    let mut facts = Facts::default();
    facts_from_events(&parse_events(stdout), Values::JsonUnordered, &mut facts);
    assert_eq!(facts.get("sink.intact"), Some("1"));
}

#[test]
fn the_connect_start_step_is_rendered() {
    assert_eq!(step_of("start.ready"), "start");
    let mut gateway = Facts::default();
    gateway.set("start.ready", true);
    let mut kafka = Facts::default();
    kafka.set("start.ready", true);
    let verdict = judge_client("connect-debezium", &gateway, &kafka, &[]);
    let markdown = render(
        "connect",
        &[row(
            "connect-debezium",
            "1",
            &gateway,
            &kafka,
            &verdict,
            (1.0, 1.0),
        )],
        &[],
    );
    assert!(markdown.contains("| start |"), "{markdown}");
    assert!(markdown.contains("| pass |"), "{markdown}");
}
