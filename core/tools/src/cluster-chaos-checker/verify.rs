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

use std::collections::{BTreeMap, HashMap, HashSet};
use std::fs::File;
use std::io::{BufRead, BufReader};
use std::path::Path;
use std::str::FromStr;
use std::sync::Arc;
use std::time::Duration;

use anyhow::{Context, Result, anyhow, bail};
use iggy::prelude::{
    AutoLogin, Client, Consumer, HeaderKey, Identifier, IggyMessage, MessageClient,
    PollingStrategy, TcpClient, TcpClientConfig, TopicClient, TopicDetails, UserClient,
};
use serde_json::{Value, json};

use crate::produce::{KEY_HEADER, PAD_BYTE};
use crate::{CONNECT_TIMEOUT, Event, Target, connect_any, credentials};

const POLL_TIMEOUT: Duration = Duration::from_secs(30);
const POLL_BATCH: u32 = 1000;
const MAX_VIOLATIONS: usize = 50;
const MAX_SAMPLES: usize = 20;

#[derive(clap::Args)]
pub struct VerifyArgs {
    #[command(flatten)]
    target: Target,
    /// Directory `produce` wrote its logs to.
    #[arg(long)]
    out: String,
    /// Stop reading after this many consecutive rounds that found nothing new.
    #[arg(long, default_value_t = 3)]
    quiet_rounds: u32,
    #[arg(long, default_value_t = 10)]
    round_pause_seconds: u64,
    /// Give up reading after this many rounds, and fail, when data keeps
    /// arriving or polls keep failing.
    #[arg(long, default_value_t = 30)]
    max_rounds: u32,
    /// Also write the summary JSON to this file.
    #[arg(long)]
    summary: Option<String>,
}

pub async fn run(args: VerifyArgs) -> Result<()> {
    let (attempted, acked) = read_produce_logs(Path::new(&args.out))?;
    if acked.is_empty() {
        bail!("no acknowledged events in {}", args.out);
    }
    let client = connect_any(&args.target.addresses(), 0).await?;
    let stream_id = args.target.stream_id()?;
    let topic_id = args.target.topic_id()?;
    let mut cursors: BTreeMap<u32, u64> = client
        .get_topic(&stream_id, &topic_id)
        .await?
        .ok_or_else(|| anyhow!("topic {}/{} missing", args.target.stream, args.target.topic))?
        .partitions
        .iter()
        .map(|partition| (partition.id, 0))
        .collect();
    let consumer = Consumer::new(Identifier::numeric(1)?);
    let mut log = LogCheck::new(&attempted)?;

    // Neither an empty poll nor the topic's listed partition end proves the
    // end of a partition right after a recovery: both have been seen stale.
    // So the whole log is read in rounds until several rounds in a row find
    // nothing new without a failed poll. Every empty poll is recorded: one
    // below the partition's final read end polled empty over data.
    let mut empty_polls: Vec<(u32, u64)> = Vec::new();
    let mut quiet_rounds = 0;
    let mut rounds = 0;
    let mut read_incomplete = false;
    loop {
        rounds += 1;
        let mut progressed = false;
        let mut failed_polls = false;
        for (partition_id, cursor) in &mut cursors {
            loop {
                let polled = tokio::time::timeout(
                    POLL_TIMEOUT,
                    client.poll_messages(
                        &stream_id,
                        &topic_id,
                        Some(*partition_id),
                        &consumer,
                        &PollingStrategy::offset(*cursor),
                        POLL_BATCH,
                        false,
                    ),
                )
                .await;
                let messages = match polled {
                    Ok(Ok(polled)) => polled.messages,
                    Ok(Err(error)) => {
                        failed_polls = true;
                        log.poll_errors += 1;
                        eprintln!("partition {partition_id}: poll at {cursor} failed: {error}");
                        break;
                    }
                    Err(_) => {
                        failed_polls = true;
                        log.poll_errors += 1;
                        eprintln!("partition {partition_id}: poll at {cursor} timed out");
                        break;
                    }
                };
                if messages.is_empty() {
                    empty_polls.push((*partition_id, *cursor));
                    break;
                }
                let polled_from = *cursor;
                for message in &messages {
                    log.check(*partition_id, cursor, message);
                }
                // Offsets at or below the requested one would make this loop
                // poll the same range forever.
                if *cursor <= polled_from {
                    log.offset_gaps += 1;
                    log.violation(format!(
                        "partition {partition_id}: a poll at offset {polled_from} did not move past it"
                    ));
                    break;
                }
                progressed = true;
            }
        }
        if progressed || failed_polls {
            quiet_rounds = 0;
        } else {
            quiet_rounds += 1;
        }
        if quiet_rounds >= args.quiet_rounds {
            break;
        }
        if rounds >= args.max_rounds {
            read_incomplete = true;
            log.violation(format!(
                "reads did not settle within {} rounds: data was still arriving or polls kept failing",
                args.max_rounds
            ));
            break;
        }
        if !progressed {
            tokio::time::sleep(Duration::from_secs(args.round_pause_seconds)).await;
        }
    }

    let listed = client
        .get_topic(&stream_id, &topic_id)
        .await?
        .ok_or_else(|| anyhow!("topic {}/{} missing", args.target.stream, args.target.topic))?;
    let mut unreadable_tail = 0u64;
    for partition in &listed.partitions {
        let read_end = cursors.get(&partition.id).copied().unwrap_or(0);
        let listed_end = listed_end(partition.messages_count, partition.current_offset);
        if listed_end > read_end {
            unreadable_tail += listed_end - read_end;
            log.violation(format!(
                "partition {}: the topic lists offsets up to {listed_end} but reads stop at {read_end}",
                partition.id
            ));
        }
    }
    let empty_polls_mid_log: Vec<String> = empty_polls
        .iter()
        .filter(|(partition_id, offset)| cursors.get(partition_id).is_some_and(|end| offset < end))
        .map(|(partition_id, offset)| format!("partition {partition_id} offset {offset}"))
        .collect();
    let topic_stats = topic_stats_per_node(&args.target, &stream_id, &topic_id, &cursors).await;
    let topic_stats_mismatches = topic_stats["mismatches"].as_u64().unwrap_or(0);
    let lost: Vec<&String> = acked
        .iter()
        .filter(|key| !log.seen.contains_key(*key))
        .collect();
    for key in lost.iter().take(20) {
        log.violation(format!("{key}: acknowledged but missing"));
    }
    let unacked_but_present = log.seen.keys().filter(|key| !acked.contains(*key)).count();
    let ok = lost.is_empty()
        && !read_incomplete
        && unreadable_tail == 0
        && log.offset_gaps == 0
        && log.reordered == 0
        && log.misplaced == 0
        && log.phantoms == 0;
    let summary = json!({
        "ok": ok,
        "acked_events": acked.len(),
        "attempted_events": attempted.len(),
        "messages_in_log": log.messages,
        "distinct_events_in_log": log.seen.len(),
        "lost_acked": lost.len(),
        "read_incomplete": read_incomplete,
        "unreadable_tail": unreadable_tail,
        "offset_gaps": log.offset_gaps,
        "reordered": log.reordered,
        "misplaced": log.misplaced,
        "phantoms": log.phantoms,
        "duplicates": log.duplicates,
        "unacked_but_present": unacked_but_present,
        "empty_polls_mid_log": empty_polls_mid_log.len(),
        "empty_polls_mid_log_sample": empty_polls_mid_log.iter().take(MAX_SAMPLES).collect::<Vec<_>>(),
        "topic_stats_mismatches": topic_stats_mismatches,
        "topic_stats": topic_stats,
        "poll_errors": log.poll_errors,
        "violations": log.violations,
    });
    let rendered = serde_json::to_string_pretty(&summary)?;
    println!("{rendered}");
    if let Some(path) = &args.summary {
        std::fs::write(path, &rendered)?;
    }
    let _ = client.shutdown().await;
    if !ok {
        bail!("verification failed");
    }
    Ok(())
}

/// One past the last offset a partition lists, 0 when it lists none.
fn listed_end(messages_count: u64, current_offset: u64) -> u64 {
    if messages_count == 0 {
        0
    } else {
        current_offset + 1
    }
}

/// `GetTopic` as each node answers it from its own metadata, next to what was
/// actually read. Its counts have been seen at 0 messages / 0 B on a topic
/// that served millions after staggered crash-restarts, so they are reported,
/// never trusted.
async fn topic_stats_per_node(
    target: &Target,
    stream_id: &Identifier,
    topic_id: &Identifier,
    read_ends: &BTreeMap<u32, u64>,
) -> Value {
    let read_total: u64 = read_ends.values().sum();
    let mut mismatches = 0u64;
    let mut nodes = serde_json::Map::new();
    for address in target.addresses() {
        let report = match get_topic_on(&address, stream_id, topic_id).await {
            Ok(Some(topic)) => {
                let topic_matches = topic.messages_count == read_total;
                mismatches += u64::from(!topic_matches);
                let partitions: Vec<Value> = topic
                    .partitions
                    .iter()
                    .map(|partition| {
                        let read_end = read_ends.get(&partition.id).copied().unwrap_or(0);
                        // Nothing expires during a run, so both the count and
                        // the end must equal the offsets read.
                        let matches = partition.messages_count == read_end
                            && listed_end(partition.messages_count, partition.current_offset)
                                == read_end;
                        mismatches += u64::from(!matches);
                        json!({
                            "id": partition.id,
                            "messages_count": partition.messages_count,
                            "current_offset": partition.current_offset,
                            "size_bytes": partition.size.as_bytes_u64(),
                            "read_end": read_end,
                            "matches_read": matches,
                        })
                    })
                    .collect();
                json!({
                    "messages_count": topic.messages_count,
                    "size_bytes": topic.size.as_bytes_u64(),
                    "matches_read": topic_matches,
                    "partitions": partitions,
                })
            }
            Ok(None) => json!({ "error": "topic missing" }),
            Err(error) => json!({ "error": format!("{error:#}") }),
        };
        nodes.insert(address, report);
    }
    json!({ "read_total": read_total, "mismatches": mismatches, "nodes": nodes })
}

/// `GetTopic` on one node. A plain `TcpClient` that signs in by hand stays on
/// the node it dialed, while `IggyClient` follows the metadata leader.
async fn get_topic_on(
    address: &str,
    stream_id: &Identifier,
    topic_id: &Identifier,
) -> Result<Option<TopicDetails>> {
    let client = TcpClient::create(Arc::new(TcpClientConfig {
        server_address: address.to_string(),
        auto_login: AutoLogin::Disabled,
        ..TcpClientConfig::default()
    }))?;
    tokio::time::timeout(CONNECT_TIMEOUT, client.connect())
        .await
        .map_err(|_| anyhow!("connect to {address} timed out"))??;
    let (username, password) = credentials();
    let topic = async {
        client.login_user(&username, &password).await?;
        client.get_topic(stream_id, topic_id).await
    };
    let topic = tokio::time::timeout(POLL_TIMEOUT, topic)
        .await
        .map_err(|_| anyhow!("GetTopic on {address} timed out"));
    let _ = client.shutdown().await;
    Ok(topic??)
}

/// Every attempted event by key, and the keys of the acknowledged ones, over
/// all runs that wrote to `dir`.
fn read_produce_logs(dir: &Path) -> Result<(HashMap<String, Event>, HashSet<String>)> {
    let mut attempted = HashMap::new();
    let mut acked = HashSet::new();
    for entry in std::fs::read_dir(dir).with_context(|| format!("read {}", dir.display()))? {
        let path = entry?.path();
        let name = path
            .file_name()
            .map(|name| name.to_string_lossy().into_owned())
            .unwrap_or_default();
        if name.starts_with("attempted-") {
            for event in read_events(&path)? {
                attempted.insert(event.key.clone(), event);
            }
        } else if name.starts_with("acked-") {
            acked.extend(read_events(&path)?.into_iter().map(|event| event.key));
        }
    }
    Ok((attempted, acked))
}

fn read_events(path: &Path) -> Result<Vec<Event>> {
    let file = File::open(path).with_context(|| format!("open {}", path.display()))?;
    BufReader::new(file)
        .lines()
        .map(|line| Ok(serde_json::from_str(&line?)?))
        .collect()
}

/// Checks the log message by message, in offset order per partition.
struct LogCheck<'a> {
    attempted: &'a HashMap<String, Event>,
    key_header: HeaderKey,
    seen: HashMap<String, u32>,
    symbol_partition: HashMap<String, u32>,
    last_seq: HashMap<String, u64>,
    messages: u64,
    offset_gaps: u64,
    reordered: u64,
    misplaced: u64,
    phantoms: u64,
    duplicates: u64,
    poll_errors: u64,
    violations: Vec<String>,
}

impl<'a> LogCheck<'a> {
    fn new(attempted: &'a HashMap<String, Event>) -> Result<Self> {
        Ok(Self {
            attempted,
            key_header: HeaderKey::from_str(KEY_HEADER)?,
            seen: HashMap::new(),
            symbol_partition: HashMap::new(),
            last_seq: HashMap::new(),
            messages: 0,
            offset_gaps: 0,
            reordered: 0,
            misplaced: 0,
            phantoms: 0,
            duplicates: 0,
            poll_errors: 0,
            violations: Vec::new(),
        })
    }

    fn violation(&mut self, text: String) {
        if self.violations.len() < MAX_VIOLATIONS {
            self.violations.push(text);
        }
    }

    fn check(&mut self, partition_id: u32, cursor: &mut u64, message: &IggyMessage) {
        self.messages += 1;
        let offset = message.header.offset;
        if offset != *cursor {
            self.offset_gaps += 1;
            self.violation(format!(
                "partition {partition_id}: expected offset {cursor}, got {offset}"
            ));
        }
        *cursor = offset + 1;
        let event: Event = match serde_json::from_slice(&message.payload) {
            Ok(event) => event,
            Err(error) => {
                self.phantoms += 1;
                self.violation(format!(
                    "partition {partition_id} offset {offset}: undecodable payload: {error}"
                ));
                return;
            }
        };
        let header = message
            .user_headers_map()
            .ok()
            .flatten()
            .and_then(|headers| headers.get(&self.key_header).cloned())
            .and_then(|value| value.as_raw().ok().map(<[u8]>::to_vec));
        if header.as_deref() != Some(event.key.as_bytes()) {
            self.phantoms += 1;
            self.violation(format!("{}: header does not match payload", event.key));
        }
        let sent_as_read = self.attempted.get(&event.key).is_some_and(|sent| {
            sent.run == event.run
                && sent.producer == event.producer
                && sent.symbol == event.symbol
                && sent.seq == event.seq
        }) && event.pad.bytes().all(|byte| byte == PAD_BYTE);
        if !sent_as_read {
            self.phantoms += 1;
            self.violation(format!("{}: never sent with this content", event.key));
        }
        let first_partition = *self
            .symbol_partition
            .entry(event.symbol.clone())
            .or_insert(partition_id);
        if first_partition != partition_id {
            self.misplaced += 1;
            self.violation(format!(
                "{}: key {} is in partitions {first_partition} and {partition_id}",
                event.key, event.symbol
            ));
        }
        let count = self.seen.entry(event.key.clone()).or_insert(0);
        *count += 1;
        if *count > 1 {
            // Expected from client retries without server-side deduplication,
            // so later copies are counted but not order-checked.
            self.duplicates += 1;
            return;
        }
        // A producer sends one key's events in order and never sends the next
        // batch before the previous one was acknowledged.
        let last = self.last_seq.entry(event.symbol.clone()).or_insert(0);
        if event.seq <= *last {
            let previous = *last;
            self.reordered += 1;
            self.violation(format!(
                "{}: seq {} after {previous} on {}",
                event.key, event.seq, event.symbol
            ));
        } else {
            *last = event.seq;
        }
    }
}
