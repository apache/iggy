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

//! A poll with `auto_commit = true` must replicate the consumer offset through
//! the partition consensus, not merely persist it on the serving node -- else a
//! failover to a backup silently loses the committed offset.
//!
//! Discriminator: drive the poll+auto_commit on the primary (node 0) and read
//! the offset from a backup's (node 1) on-disk store. The backup holds it only
//! if the commit rode consensus; the pre-fix node-local persist never reaches
//! it, so the file stays absent until the deadline and the test fails.

use iggy::prelude::*;
use integration::harness::TestHarness;
use integration::iggy_harness;
use std::path::{Path, PathBuf};
use std::time::Duration;
use tokio::time::sleep;

const STREAM_NAME: &str = "auto-commit-repl-stream";
const TOPIC_NAME: &str = "auto-commit-repl-topic";
const PARTITION_ID: u32 = 0;
// Explicit numeric consumer id shared by the poll and the offset read;
// `Consumer::default()` would carry id 0.
const CONSUMER_ID: u32 = 7;
const MESSAGE_COUNT: u32 = 10;
// Offset of the last message after polling all of `0..MESSAGE_COUNT` from 0.
const EXPECTED_OFFSET: u64 = (MESSAGE_COUNT - 1) as u64;
// The primary answers the poll before the auto-commit replicates, so allow the
// backup a bounded window to catch up rather than a fixed settle sleep.
const REPLICATION_TIMEOUT: Duration = Duration::from_secs(15);
const REPLICATION_POLL_INTERVAL: Duration = Duration::from_millis(100);

#[iggy_harness(cluster_nodes = 3)]
async fn given_cluster_when_poll_auto_commits_then_offset_replicates_to_backup(
    harness: &TestHarness,
) {
    run(harness).await;
}

async fn run(harness: &TestHarness) {
    let stream = Identifier::named(STREAM_NAME).unwrap();
    let topic = Identifier::named(TOPIC_NAME).unwrap();
    let consumer = Consumer::new(Identifier::numeric(CONSUMER_ID).unwrap());

    // Primary (node 0, the view-0 primary): create the topic, produce, then poll
    // with auto_commit so the offset is committed server-side.
    let primary = harness.tcp_root_client().await.unwrap();
    primary.create_stream(STREAM_NAME).await.unwrap();
    primary
        .create_topic(
            &stream,
            TOPIC_NAME,
            1,
            CompressionAlgorithm::None,
            None,
            IggyExpiry::NeverExpire,
            MaxTopicSize::ServerDefault,
        )
        .await
        .unwrap();

    let mut messages: Vec<IggyMessage> = (0..MESSAGE_COUNT)
        .map(|i| {
            IggyMessage::builder()
                .payload(format!("auto-commit-{i}").into())
                .build()
                .unwrap()
        })
        .collect();
    primary
        .send_messages(
            &stream,
            &topic,
            &Partitioning::partition_id(PARTITION_ID),
            &mut messages,
        )
        .await
        .unwrap();

    let polled = primary
        .poll_messages(
            &stream,
            &topic,
            Some(PARTITION_ID),
            &consumer,
            &PollingStrategy::offset(0),
            MESSAGE_COUNT,
            true,
        )
        .await
        .unwrap();
    assert_eq!(
        polled.messages.len(),
        MESSAGE_COUNT as usize,
        "poll must return every produced message so the auto-commit has an offset to store",
    );

    // Sanity: the serving node holds its own committed offset (eager in-memory
    // apply). Passes on both the buggy and fixed paths -- the discriminator is
    // the backup below.
    let on_primary = primary
        .get_consumer_offset(&consumer, &stream, &topic, Some(PARTITION_ID))
        .await
        .unwrap();
    assert_eq!(
        on_primary.map(|info| info.stored_offset),
        Some(EXPECTED_OFFSET),
        "the serving node must hold the auto-committed offset",
    );

    // Discriminator: the backup's on-disk consumer-offset store. A backup does
    // not serve client logins (Register is a primary-only metadata op), so read
    // its data dir directly. Only a consensus-replicated auto-commit lands there
    // -- the pre-fix node-local persist never reaches the backup, so the file
    // stays absent until the deadline and the test fails.
    let backup_dir = harness.node(1).data_path();
    let deadline = tokio::time::Instant::now() + REPLICATION_TIMEOUT;
    loop {
        if let Some(stored) = read_replicated_consumer_offset(&backup_dir) {
            assert_eq!(
                stored, EXPECTED_OFFSET,
                "backup replicated a wrong auto-commit offset",
            );
            return;
        }
        assert!(
            tokio::time::Instant::now() < deadline,
            "auto-commit offset never replicated to the backup within {REPLICATION_TIMEOUT:?}; \
             it stayed node-local (the bug)",
        );
        sleep(REPLICATION_POLL_INTERVAL).await;
    }
}

/// The u64 offset persisted under any `offsets/consumers/<id>` file in a node's
/// data dir, or `None` when no such file has been written yet. Walks the tree so
/// it is robust to the stream/topic/partition id layout; the test drives exactly
/// one consumer, so at most one such file exists. A zero-length read (persist
/// truncates before writing the 8 bytes) is treated as not-yet-written.
fn read_replicated_consumer_offset(data_dir: &Path) -> Option<u64> {
    let mut stack: Vec<PathBuf> = vec![data_dir.to_path_buf()];
    while let Some(dir) = stack.pop() {
        let Ok(entries) = std::fs::read_dir(&dir) else {
            continue;
        };
        for entry in entries.flatten() {
            let path = entry.path();
            let Ok(file_type) = entry.file_type() else {
                continue;
            };
            if file_type.is_dir() {
                stack.push(path);
                continue;
            }
            let is_consumer_offset = path
                .parent()
                .and_then(Path::file_name)
                .is_some_and(|name| name == "consumers")
                && path
                    .parent()
                    .and_then(Path::parent)
                    .and_then(Path::file_name)
                    .is_some_and(|name| name == "offsets");
            if is_consumer_offset
                && let Ok(bytes) = std::fs::read(&path)
                && let Ok(array) = <[u8; 8]>::try_from(bytes.as_slice())
            {
                return Some(u64::from_le_bytes(array));
            }
        }
    }
    None
}
