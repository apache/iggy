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

//! The rolling SIGKILL schedule reported on PR #4433, without Docker: three
//! local server processes, a persisted topic, producers that retry every batch
//! until it is acked, and each node killed and restarted in turn under load.
//! Every acked message must be readable afterwards.

use std::collections::{HashMap, HashSet};
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};
use std::time::{Duration, Instant};

use iggy::prelude::*;
use integration::iggy_harness;
use tokio::sync::Mutex;
use tokio::time::sleep;

const STREAM_NAME: &str = "repro-stream";
const TOPIC_NAME: &str = "repro-topic";
const PARTITIONS: u32 = 6;
const PRODUCERS: u32 = 6;
const BATCH: u32 = 20;
const PAYLOAD_BYTES: usize = 256;
/// Six producers at this pace send about 2,000 messages per second in total.
const SEND_INTERVAL: Duration = Duration::from_millis(60);
const SEND_TIMEOUT: Duration = Duration::from_secs(3);
const LOAD: Duration = Duration::from_secs(90);
/// A batch still unacked this long after the load ends is reported, not retried.
const ACK_GRACE: Duration = Duration::from_secs(180);
const VERIFY_BUDGET: Duration = Duration::from_secs(120);
/// Kill and restart times for each node, measured from the start of load.
const SCHEDULE: [(usize, u64, u64); 3] = [(0, 15, 30), (1, 45, 60), (2, 75, 90)];

type Acked = Arc<Mutex<HashMap<u32, HashSet<String>>>>;

#[derive(Default)]
struct Counters {
    acked_batches: AtomicUsize,
    send_errors: AtomicUsize,
    unacked_batches: AtomicUsize,
    max_latency_ms: AtomicU64,
}

#[iggy_harness(cluster_nodes = 3)]
#[ignore = "long-running reproduction of the PR #4433 rolling SIGKILL report"]
async fn given_load_on_a_persisted_topic_when_every_node_is_sigkilled_in_turn_should_keep_every_acked_message(
    harness: &mut TestHarness,
) {
    let addresses: Vec<String> = harness
        .all_servers()
        .iter()
        .map(|server| server.raw_tcp_addr().expect("tcp address"))
        .collect();
    let setup = connect_any(&addresses).await.expect("cluster serves");
    setup.create_stream(STREAM_NAME).await.unwrap();
    setup
        .create_topic(
            &Identifier::named(STREAM_NAME).unwrap(),
            TOPIC_NAME,
            &TopicCreateOptions {
                partitions_count: Some(PARTITIONS),
                durability: Durability::Persisted,
                ..TopicCreateOptions::default()
            },
        )
        .await
        .unwrap();
    drop(setup);

    let acked: Acked = Arc::default();
    let counters = Arc::new(Counters::default());
    let start = Instant::now();
    // Restarting a node blocks this thread until it boots, so the producers run
    // on their own runtime and keep writing through every restart.
    let load = {
        let (addresses, acked, counters) =
            (addresses.clone(), Arc::clone(&acked), Arc::clone(&counters));
        std::thread::spawn(move || {
            tokio::runtime::Builder::new_multi_thread()
                .worker_threads(4)
                .enable_all()
                .build()
                .unwrap()
                .block_on(async move {
                    let producers: Vec<_> = (0..PRODUCERS)
                        .map(|producer| {
                            tokio::spawn(produce(
                                producer,
                                addresses.clone(),
                                Arc::clone(&acked),
                                Arc::clone(&counters),
                                start,
                            ))
                        })
                        .collect();
                    for producer in producers {
                        producer.await.unwrap();
                    }
                });
        })
    };

    for (node, kill_at, restart_at) in SCHEDULE {
        sleep_until(start, kill_at).await;
        harness.kill_node(node).expect("SIGKILL node");
        eprintln!(
            "REPRO t={:?} killed node {node} (acked batches so far {})",
            start.elapsed(),
            counters.acked_batches.load(Ordering::Relaxed)
        );
        sleep_until(start, restart_at).await;
        eprintln!(
            "REPRO t={:?} restarting node {node} (acked batches so far {})",
            start.elapsed(),
            counters.acked_batches.load(Ordering::Relaxed)
        );
        harness.restart_node(node).expect("restart node");
        eprintln!("REPRO t={:?} restarted node {node}", start.elapsed());
    }
    tokio::task::spawn_blocking(move || load.join().unwrap())
        .await
        .unwrap();
    eprintln!(
        "REPRO t={:?} load done: acked batches {}, unacked batches {}, send errors {}, max ack latency {} ms",
        start.elapsed(),
        counters.acked_batches.load(Ordering::Relaxed),
        counters.unacked_batches.load(Ordering::Relaxed),
        counters.send_errors.load(Ordering::Relaxed),
        counters.max_latency_ms.load(Ordering::Relaxed),
    );

    let acked = acked.lock().await;
    let mut lost = 0usize;
    let mut total = 0usize;
    for partition in 0..PARTITIONS {
        let expected = acked.get(&partition).cloned().unwrap_or_default();
        total += expected.len();
        let present = read_partition(&addresses, partition).await;
        let missing = expected.difference(&present).count();
        eprintln!(
            "REPRO partition {partition}: {} acked, {} readable, {missing} acked missing",
            expected.len(),
            present.len()
        );
        lost += missing;
    }
    assert_eq!(
        counters.unacked_batches.load(Ordering::Relaxed),
        0,
        "some batches were never acked"
    );
    assert_eq!(lost, 0, "{lost} of {total} acked messages are not readable");
    assert!(
        std::env::var_os("REPRO_KEEP_LOGS").is_none(),
        "REPRO_KEEP_LOGS is set: failing on purpose so the harness keeps server logs"
    );
}

async fn sleep_until(start: Instant, at_secs: u64) {
    let at = Duration::from_secs(at_secs);
    if let Some(remaining) = at.checked_sub(start.elapsed()) {
        sleep(remaining).await;
    }
}

async fn produce(
    producer: u32,
    addresses: Vec<String>,
    acked: Acked,
    counters: Arc<Counters>,
    start: Instant,
) {
    let stream = Identifier::named(STREAM_NAME).unwrap();
    let topic = Identifier::named(TOPIC_NAME).unwrap();
    let mut client = None;
    let mut sequence = 0u64;
    while start.elapsed() < LOAD {
        let partition =
            (producer + u32::try_from(sequence % u64::from(PARTITIONS)).unwrap()) % PARTITIONS;
        let payloads: Vec<String> = (0..BATCH)
            .map(|index| {
                let mut payload = format!("p{producer}-s{sequence}-m{index}-");
                payload.extend(std::iter::repeat_n('x', PAYLOAD_BYTES - payload.len()));
                payload
            })
            .collect();
        let sent_at = Instant::now();
        loop {
            if start.elapsed() > LOAD + ACK_GRACE {
                counters.unacked_batches.fetch_add(1, Ordering::Relaxed);
                return;
            }
            if client.is_none() {
                let first = usize::try_from(producer).unwrap() + usize::try_from(sequence).unwrap();
                client = connect_any(&rotated(&addresses, first)).await;
                if client.is_none() {
                    sleep(Duration::from_millis(250)).await;
                    continue;
                }
            }
            let mut messages: Vec<IggyMessage> = payloads
                .iter()
                .map(|payload| {
                    IggyMessage::builder()
                        .payload(payload.clone().into())
                        .build()
                        .unwrap()
                })
                .collect();
            let attempt = tokio::time::timeout(
                SEND_TIMEOUT,
                client.as_ref().unwrap().send_messages(
                    &stream,
                    &topic,
                    &Partitioning::partition_id(partition),
                    &mut messages,
                ),
            )
            .await;
            match attempt {
                Ok(Ok(_)) => {
                    let latency = u64::try_from(sent_at.elapsed().as_millis()).unwrap_or(u64::MAX);
                    counters
                        .max_latency_ms
                        .fetch_max(latency, Ordering::Relaxed);
                    counters.acked_batches.fetch_add(1, Ordering::Relaxed);
                    acked
                        .lock()
                        .await
                        .entry(partition)
                        .or_default()
                        .extend(payloads);
                    break;
                }
                _ => {
                    counters.send_errors.fetch_add(1, Ordering::Relaxed);
                    client = None;
                    sleep(Duration::from_millis(250)).await;
                }
            }
        }
        sequence += 1;
        sleep(SEND_INTERVAL).await;
    }
}

fn rotated(addresses: &[String], first: usize) -> Vec<String> {
    let mut rotated = addresses.to_vec();
    rotated.rotate_left(first % addresses.len());
    rotated
}

async fn connect_any(addresses: &[String]) -> Option<IggyClient> {
    for address in addresses {
        let config = TcpClientConfig {
            server_address: address.clone(),
            ..TcpClientConfig::default()
        };
        let Ok(tcp) = TcpClient::create(Arc::new(config)) else {
            continue;
        };
        if Client::connect(&tcp).await.is_err() {
            continue;
        }
        let client = IggyClient::create(ClientWrapper::Tcp(tcp), None, None);
        if client
            .login_user(DEFAULT_ROOT_USERNAME, DEFAULT_ROOT_PASSWORD)
            .await
            .is_ok()
        {
            return Some(client);
        }
    }
    None
}

/// Every payload readable from `partition`, up to the `current_offset` the
/// topic reports. Empty polls are retried until `VERIFY_BUDGET` runs out.
async fn read_partition(addresses: &[String], partition: u32) -> HashSet<String> {
    let stream = Identifier::named(STREAM_NAME).unwrap();
    let topic = Identifier::named(TOPIC_NAME).unwrap();
    let deadline = Instant::now() + VERIFY_BUDGET;
    let mut present = HashSet::new();
    let mut next = 0u64;
    loop {
        if Instant::now() > deadline {
            return present;
        }
        let Some(client) = connect_any(addresses).await else {
            sleep(Duration::from_millis(500)).await;
            continue;
        };
        let current = match client.get_topic(&stream, &topic).await {
            Ok(Some(details)) => details
                .partitions
                .iter()
                .find(|details| details.id == partition)
                .map(|details| details.current_offset),
            _ => None,
        };
        let Some(current) = current else {
            sleep(Duration::from_millis(500)).await;
            continue;
        };
        while next <= current && Instant::now() <= deadline {
            match client
                .poll_messages(
                    &stream,
                    &topic,
                    Some(partition),
                    &Consumer::default(),
                    &PollingStrategy::offset(next),
                    1_000,
                    false,
                )
                .await
            {
                Ok(polled) if !polled.messages.is_empty() => {
                    for message in &polled.messages {
                        present.insert(String::from_utf8_lossy(&message.payload).into_owned());
                        next = next.max(message.header.offset + 1);
                    }
                }
                _ => sleep(Duration::from_millis(500)).await,
            }
        }
        if next > current {
            return present;
        }
    }
}
