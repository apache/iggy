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

//! The disk usage accounting of a running server.
//!
//! The burst tests compare what the write sites charged (`disk_charged_bytes`,
//! which samples never move) with what the data directory allocated over the
//! same burst. Both belong to this server alone, so the sampler keeps running
//! and the tests may run beside others.
//!
//! The foreign file test compares the `disk_used_bytes` estimate with the
//! whole volume, so it must run alone: a `#[serial]` excludes the other tests
//! under `cargo test`, and `.config/nextest.toml` gives it every thread under
//! nextest.

use std::path::Path;
use std::time::{Duration, Instant};

use iggy::prelude::*;
use integration::harness::TestHarness;
use integration::harness::disk::{allocated_bytes, volume_used_bytes};
use integration::harness::seeds::{self, names};
use serial_test::{parallel, serial};
use tokio::time::sleep;

use super::http_client::HttpClient;

const KIB: u64 = 1024;
const MIB: u64 = 1024 * KIB;
const BURST_TOPIC: &str = "burst";

/// Small enough that the burst fills four segments and opens a fifth, so
/// reservations, the batch that crosses each segment end and a reservation
/// still mostly unwritten are all part of it.
const SEGMENT_BYTES: u64 = 4 * MIB;
const MESSAGE_BYTES: usize = 64 * KIB as usize;
const MESSAGES_PER_BATCH: usize = 4;

/// 18 MiB of bodies in 72 prepares. Every prepare adds a 4 KiB WAL record, so
/// the WAL grows by 288 KiB, and the WAL checkpoint (half the 256 MiB budget)
/// stays out of reach: the counter never sees the frees a checkpoint makes.
const BATCHES: usize = 72;

/// What the write sites rightly leave out: the end of every segment and index
/// file rounded up to a filesystem block, and the server's own log lines.
/// Below the burst's 288 KiB of WAL records, so a missing WAL charge fails.
const BURST_BOUND: u64 = 128 * KIB;

/// Quiet scrapes in a row before the charges count as settled. Sends return
/// once committed, which can be before the WAL writer and materialization
/// have written what they charge.
const SETTLED_SCRAPES: usize = 5;
const SETTLE_DEADLINE: Duration = Duration::from_secs(10);

/// What else moves the volume between a sample and the snapshot after it
/// (other processes, the server's own log lines), plus block rounding.
const TOLERANCE: u64 = MIB;
const FOREIGN_FILE_BYTES: usize = 16 * MIB as usize;
/// Comfortably more than one sampling interval (1 s).
const SAMPLE_DEADLINE: Duration = Duration::from_secs(5);
const POLL_INTERVAL: Duration = Duration::from_millis(100);

#[tokio::test]
#[parallel]
async fn given_a_burst_when_the_charges_settle_should_match_the_data_directory_growth() {
    assert_burst_is_charged_with_preallocation(false).await;
}

#[tokio::test]
#[parallel]
async fn given_a_burst_into_preallocated_segments_when_the_charges_settle_should_match_the_data_directory_growth()
 {
    assert_burst_is_charged_with_preallocation(true).await;
}

#[tokio::test]
#[serial]
async fn given_a_file_written_outside_iggy_when_the_next_sample_runs_should_count_it_until_deleted()
{
    let harness = start_server().await;
    let http = HttpClient::login_root(&harness).await;
    let data_path = harness.server().data_path();
    // Beside the data directory, so on the same volume but never charged.
    let foreign_path = data_path.with_extension("foreign");
    let foreign_bytes = i128::from(FOREIGN_FILE_BYTES as u64);
    let idle = UsageSnapshot::take(&http, &data_path).await;

    std::fs::write(&foreign_path, vec![0u8; FOREIGN_FILE_BYTES]).expect("write foreign file");
    let with_file = idle.wait_for_sample(&http, &data_path, foreign_bytes).await;
    // Removed before asserting, so a failure leaves no 16 MiB file behind.
    std::fs::remove_file(&foreign_path).expect("remove foreign file");
    let with_file = with_file.unwrap_or_else(|last| {
        panic!(
            "within {SAMPLE_DEADLINE:?} a sample must move the estimate by what the volume \
             moved when another process wrote {foreign_bytes} bytes, within {TOLERANCE} \
             bytes; last change: {last}"
        )
    });
    let without_file = with_file
        .wait_for_sample(&http, &data_path, -foreign_bytes)
        .await;

    if let Err(last) = without_file {
        panic!(
            "within {SAMPLE_DEADLINE:?} a sample must move the estimate by what the volume \
             moved when the {foreign_bytes} bytes were deleted, within {TOLERANCE} bytes; \
             last change: {last}"
        );
    }
}

/// Sends a burst into a topic of its own and checks that the bytes charged
/// over it match what the data directory allocated over it.
async fn assert_burst_is_charged_with_preallocation(preallocate: bool) {
    let harness = start_server().await;
    let client = harness.tcp_root_client().await.expect("tcp root client");
    let http = HttpClient::login_root(&harness).await;
    let data_path = harness.server().data_path();
    let stream = Identifier::named(names::STREAM).expect("stream identifier");
    client
        .create_topic(
            &stream,
            BURST_TOPIC,
            &TopicCreateOptions {
                partitions_count: Some(1),
                segment_size: Some(IggyByteSize::from(SEGMENT_BYTES)),
                preallocate_segments: Some(preallocate),
                ..TopicCreateOptions::default()
            },
        )
        .await
        .expect("create burst topic");
    let topic = Identifier::named(BURST_TOPIC).expect("topic identifier");

    let charged_before = settled_charges(&http).await;
    let allocated_before = allocated_bytes(&data_path);
    for batch in 0..BATCHES {
        let mut messages: Vec<IggyMessage> = (0..MESSAGES_PER_BATCH)
            .map(|_| {
                IggyMessage::builder()
                    .payload(vec![batch as u8; MESSAGE_BYTES].into())
                    .build()
                    .expect("build message")
            })
            .collect();
        client
            .send_messages(
                &stream,
                &topic,
                &Partitioning::partition_id(0),
                &mut messages,
            )
            .await
            .expect("send messages");
    }
    let charged = settled_charges(&http).await - charged_before;
    let allocated = i128::from(allocated_bytes(&data_path)) - i128::from(allocated_before);

    let difference = i128::from(charged) - allocated;
    assert!(
        difference.unsigned_abs() <= u128::from(BURST_BOUND),
        "the burst must be charged within {BURST_BOUND} bytes of what the data directory \
         allocated (preallocate_segments = {preallocate}): charged {charged}, allocated \
         {allocated}, difference {difference}"
    );
}

/// One node with the standard stream and topic: the harness defaults to a
/// three-replica cluster, and every replica writes its own copy while the
/// accounting under test belongs to one of them.
///
/// Built by hand instead of with `#[iggy_harness]`, which marks every test it
/// generates `#[serial_test::parallel]` and so cannot carry the `#[serial]`
/// the foreign file test needs under `cargo test`.
async fn start_server() -> TestHarness {
    let mut harness = TestHarness::builder()
        .default_server()
        .cluster_nodes(1)
        .build()
        .expect("build harness");
    harness
        .start_with_seed(|client| async move { seeds::stream_with_topic(&client).await })
        .await
        .expect("start server");
    harness
}

/// The value of the unlabeled sample `name` on `/metrics`.
async fn scrape(http: &HttpClient, name: &str) -> u64 {
    let exposition = http
        .get("/metrics")
        .await
        .text()
        .await
        .expect("metrics text");
    exposition
        .lines()
        .find_map(|line| line.strip_prefix(name)?.strip_prefix(' '))
        .and_then(|value| value.parse().ok())
        .unwrap_or_else(|| panic!("no {name} sample in /metrics:\n{exposition}"))
}

/// The charged total once [`SETTLED_SCRAPES`] scrapes in a row agree.
async fn settled_charges(http: &HttpClient) -> u64 {
    let deadline = Instant::now() + SETTLE_DEADLINE;
    let mut charged = scrape(http, "disk_charged_bytes_total").await;
    let mut unchanged_scrapes = 0;
    while unchanged_scrapes < SETTLED_SCRAPES {
        assert!(
            Instant::now() < deadline,
            "the charged total must stop moving within {SETTLE_DEADLINE:?} (last {charged})"
        );
        sleep(POLL_INTERVAL).await;
        let next = scrape(http, "disk_charged_bytes_total").await;
        unchanged_scrapes = if next == charged {
            unchanged_scrapes + 1
        } else {
            0
        };
        charged = next;
    }
    charged
}

/// The `disk_used_bytes` estimate and what `statvfs` reports for the volume,
/// read one after the other.
#[derive(Clone, Copy)]
struct UsageSnapshot {
    estimate: u64,
    volume: u64,
}

impl UsageSnapshot {
    async fn take(http: &HttpClient, data_path: &Path) -> Self {
        let estimate = scrape(http, "disk_used_bytes").await;
        Self {
            estimate,
            volume: volume_used_bytes(data_path),
        }
    }

    /// The first snapshot whose volume moved by `expected` bytes from `self`
    /// and whose estimate moved by the same amount, both within
    /// [`TOLERANCE`]. That takes a sample, since nothing charges the change.
    /// On timeout, the last change seen.
    async fn wait_for_sample(
        self,
        http: &HttpClient,
        data_path: &Path,
        expected: i128,
    ) -> Result<Self, UsageChange> {
        let deadline = Instant::now() + SAMPLE_DEADLINE;
        let tolerance = i128::from(TOLERANCE);
        loop {
            let snapshot = Self::take(http, data_path).await;
            let change = UsageChange::between(self, snapshot);
            if (change.volume - expected).abs() <= tolerance
                && (change.estimate - change.volume).abs() <= tolerance
            {
                return Ok(snapshot);
            }
            if Instant::now() >= deadline {
                return Err(change);
            }
            sleep(POLL_INTERVAL).await;
        }
    }
}

/// How far the estimate and the volume moved between two snapshots.
#[derive(Debug)]
struct UsageChange {
    estimate: i128,
    volume: i128,
}

impl UsageChange {
    fn between(from: UsageSnapshot, to: UsageSnapshot) -> Self {
        Self {
            estimate: i128::from(to.estimate) - i128::from(from.estimate),
            volume: i128::from(to.volume) - i128::from(from.volume),
        }
    }
}

impl std::fmt::Display for UsageChange {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            formatter,
            "estimate {:+}, volume {:+} bytes",
            self.estimate, self.volume
        )
    }
}
