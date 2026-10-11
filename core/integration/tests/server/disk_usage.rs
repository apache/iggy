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

//! The disk usage estimate behind `disk_used_bytes` against `statvfs` of the
//! data volume. Both read the whole volume, so these tests must run alone: a
//! `#[serial]` excludes the harness tests under `cargo test`, and
//! `.config/nextest.toml` gives them every thread under nextest.

use std::time::{Duration, Instant};

use iggy::prelude::*;
use integration::harness::TestHarness;
use integration::harness::disk::volume_used_bytes;
use integration::harness::seeds::{self, names};
use serial_test::serial;
use tokio::time::sleep;

use super::http_client::HttpClient;

const KIB: u64 = 1024;
const MIB: u64 = 1024 * KIB;
/// Per round the server writes about 2 MiB of messages, and more than that
/// once the WAL and the segment both hold them. An estimate that missed the
/// write sites would be off by at least that much until the next sample.
const MESSAGES_PER_ROUND: usize = 16;
const MESSAGE_BYTES: usize = 128 * KIB as usize;
const ROUNDS: usize = 8;
/// Block rounding, file metadata and the server's own log lines, none of
/// which the write sites charge.
const TOLERANCE: u64 = MIB;
const FOREIGN_FILE_BYTES: usize = 16 * MIB as usize;
/// Comfortably more than one sampling interval (1 s).
const SAMPLE_DEADLINE: Duration = Duration::from_secs(5);
const POLL_INTERVAL: Duration = Duration::from_millis(100);

#[tokio::test]
#[serial]
async fn given_iggy_writes_between_samples_when_scraping_should_track_the_volume_within_tolerance()
{
    let harness = start_server().await;
    let client = harness.tcp_root_client().await.expect("tcp root client");
    let http = HttpClient::login_root(&harness).await;
    let data_path = harness.server().data_path();
    let stream = Identifier::named(names::STREAM).expect("stream identifier");
    let topic = Identifier::named(names::TOPIC).expect("topic identifier");

    let mut rounds = Vec::with_capacity(ROUNDS);
    for round in 0..ROUNDS {
        let mut messages: Vec<IggyMessage> = (0..MESSAGES_PER_ROUND)
            .map(|_| {
                IggyMessage::builder()
                    .payload(vec![round as u8; MESSAGE_BYTES].into())
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

        // Bracketing the scrape absorbs writes the server finishes while it
        // runs, such as a segment flush that follows the WAL write.
        let before = volume_used_bytes(&data_path);
        let estimate = scrape_estimate(&http).await;
        let after = volume_used_bytes(&data_path);
        rounds.push((before, estimate, after));
    }

    let outside: Vec<_> = rounds
        .iter()
        .enumerate()
        .filter(|(_, (before, estimate, after))| {
            *estimate + TOLERANCE < *before || *estimate > *after + TOLERANCE
        })
        .collect();
    assert!(
        outside.is_empty(),
        "the estimate must stay within {TOLERANCE} bytes of statvfs, rounds outside \
         (round, (statvfs before, estimate, statvfs after)): {outside:?}"
    );
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
    let idle = scrape_estimate(&http).await;

    std::fs::write(&foreign_path, vec![0u8; FOREIGN_FILE_BYTES]).expect("write foreign file");
    let foreign_bytes = FOREIGN_FILE_BYTES as u64;
    let with_file = wait_for_estimate(&http, |estimate| {
        estimate + TOLERANCE >= idle + foreign_bytes
    })
    .await;
    // Removed before asserting, so a failure leaves no 16 MiB file behind.
    std::fs::remove_file(&foreign_path).expect("remove foreign file");
    let with_file = with_file.unwrap_or_else(|| {
        panic!(
            "a sample must add the {foreign_bytes} bytes another process wrote \
             (idle estimate {idle}) within {SAMPLE_DEADLINE:?}"
        )
    });
    let without_file = wait_for_estimate(&http, |estimate| {
        estimate <= with_file - foreign_bytes + TOLERANCE
    })
    .await;

    assert!(
        without_file.is_some(),
        "a sample must drop the deleted file again within {SAMPLE_DEADLINE:?} \
         (estimate with the file {with_file})"
    );
}

/// One node with the standard stream and topic: the harness defaults to a
/// three-replica cluster, and every replica writes its own copy to the same
/// volume while the estimate under test belongs to one of them.
///
/// Built by hand instead of with `#[iggy_harness]`, which marks every test it
/// generates `#[serial_test::parallel]` and so cannot carry the `#[serial]`
/// these tests need under `cargo test`.
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

/// The `disk_used_bytes` sample from `/metrics`.
async fn scrape_estimate(http: &HttpClient) -> u64 {
    let exposition = http
        .get("/metrics")
        .await
        .text()
        .await
        .expect("metrics text");
    exposition
        .lines()
        .find_map(|line| line.strip_prefix("disk_used_bytes "))
        .and_then(|value| value.parse().ok())
        .unwrap_or_else(|| panic!("no disk_used_bytes sample in /metrics:\n{exposition}"))
}

/// The first estimate that satisfies `accept`, or `None` when none does
/// within [`SAMPLE_DEADLINE`].
async fn wait_for_estimate(http: &HttpClient, accept: impl Fn(u64) -> bool) -> Option<u64> {
    let deadline = Instant::now() + SAMPLE_DEADLINE;
    while Instant::now() < deadline {
        let estimate = scrape_estimate(http).await;
        if accept(estimate) {
            return Some(estimate);
        }
        sleep(POLL_INTERVAL).await;
    }
    None
}
