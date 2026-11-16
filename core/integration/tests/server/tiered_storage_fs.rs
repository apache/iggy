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

//! A server with the `fs` tiered storage backend and a tiered topic on it.

use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::time::Duration;

use iggy::prelude::*;
use iggy_object_store::{FsStore, ObjectStore, PartitionManifest};
use integration::harness::{TestHarness, TestServerConfig};
use tokio::time::{Instant, sleep};

const STREAM: &str = "tiered-stream";
const TOPIC: &str = "tiered-topic";
const PARTITION_ID: u32 = 0;
const SEGMENT_BYTES: u64 = 1024 * 1024;
const MESSAGE_BYTES: usize = 100 * 1024;
const MESSAGES: u64 = 50;
/// How long the cluster may take to accept a tiered topic: the node claims
/// the store first, and the metadata primary then has to see it ready.
const READY_DEADLINE: Duration = Duration::from_secs(60);
/// How long the uploads and the local eviction of the test's segments may
/// take.
const TIERING_DEADLINE: Duration = Duration::from_secs(60);
const POLL_INTERVAL: Duration = Duration::from_millis(200);

#[tokio::test]
async fn given_a_tiered_topic_when_its_segments_leave_the_disk_should_read_them_back_from_the_store()
 {
    let store = tempfile::tempdir().expect("a directory for the object store");
    let mut harness = server_with_fs_store(store.path());
    harness.start().await.expect("the server boots");
    let client = harness.tcp_root_client().await.expect("tcp root client");
    let stream = Identifier::named(STREAM).expect("stream identifier");
    let topic = Identifier::named(TOPIC).expect("topic identifier");
    client
        .create_stream(STREAM, &StreamCreateOptions::default())
        .await
        .expect("create stream");
    create_tiered_topic(&client, &stream).await;

    for index in 0..MESSAGES {
        let mut messages = vec![
            IggyMessage::builder()
                .payload(payload(index).into())
                .build()
                .expect("build message"),
        ];
        client
            .send_messages(
                &stream,
                &topic,
                &Partitioning::partition_id(PARTITION_ID),
                &mut messages,
            )
            .await
            .expect("send messages");
    }
    let data_path = harness.server().data_path();
    let uploaded = wait_for_eviction(store.path(), &data_path).await;

    assert_reads_every_message(&client, &stream, &topic).await;
    assert_manifest_lists_stored_segments(store.path()).await;

    drop(client);
    harness.restart_server().await.expect("the server restarts");
    let client = harness.tcp_root_client().await.expect("tcp root client");

    assert_reads_every_message(&client, &stream, &topic).await;
    assert!(
        segment_logs(store.path()).len() >= uploaded,
        "the store lost segments over the restart"
    );
}

fn server_with_fs_store(store: &Path) -> TestHarness {
    let extra_envs = HashMap::from([
        ("IGGY_TIERED_STORAGE_BACKEND".to_owned(), "fs".to_owned()),
        (
            "IGGY_TIERED_STORAGE_PATH".to_owned(),
            store.to_string_lossy().into_owned(),
        ),
        (
            "IGGY_DATA_MAINTENANCE_MESSAGES_CLEANER_ENABLED".to_owned(),
            "true".to_owned(),
        ),
        (
            "IGGY_DATA_MAINTENANCE_MESSAGES_INTERVAL".to_owned(),
            "100ms".to_owned(),
        ),
    ]);
    TestHarness::builder()
        .server(TestServerConfig::builder().extra_envs(extra_envs).build())
        .build()
        .expect("the harness")
}

/// Creates the topic once the cluster accepts `tiered_storage`.
async fn create_tiered_topic(client: &IggyClient, stream: &Identifier) {
    let options = TopicCreateOptions {
        partitions_count: Some(1),
        segment_size: Some(IggyByteSize::from(SEGMENT_BYTES)),
        tiered_storage: Some(true),
        // Every uploaded segment may leave the disk at once.
        local_retention_age: Some(IggyExpiry::ExpireDuration(IggyDuration::from(
            Duration::from_secs(1),
        ))),
        ..TopicCreateOptions::default()
    };
    let deadline = Instant::now() + READY_DEADLINE;
    loop {
        match client.create_topic(stream, TOPIC, &options).await {
            Ok(_) => return,
            Err(IggyError::FeatureUnavailable) if Instant::now() < deadline => {
                sleep(POLL_INTERVAL).await;
            }
            Err(error) => panic!("create the tiered topic: {error}"),
        }
    }
}

/// Waits until the store holds segments that the data directory no longer
/// does, and returns how many the store holds.
async fn wait_for_eviction(store: &Path, data_path: &Path) -> usize {
    let deadline = Instant::now() + TIERING_DEADLINE;
    loop {
        let uploaded = segment_logs(store);
        let local = segment_logs(data_path);
        let evicted = uploaded.iter().filter(|name| !local.contains(name)).count();
        if evicted > 0 {
            return uploaded.len();
        }
        assert!(
            Instant::now() < deadline,
            "within {TIERING_DEADLINE:?} no uploaded segment left the disk: the store holds \
             {uploaded:?}, the data directory {local:?}"
        );
        sleep(POLL_INTERVAL).await;
    }
}

async fn assert_reads_every_message(client: &IggyClient, stream: &Identifier, topic: &Identifier) {
    let mut offset = 0;
    while offset < MESSAGES {
        let polled = client
            .poll_messages(
                stream,
                topic,
                PARTITION_ID,
                &Consumer::default(),
                &PollingStrategy::offset(offset),
                PollOptions {
                    count: 10,
                    ..PollOptions::default()
                },
            )
            .await
            .unwrap_or_else(|error| panic!("poll from offset {offset}: {error}"));
        assert!(
            !polled.messages.is_empty(),
            "the poll from offset {offset} returned no message"
        );
        for message in &polled.messages {
            assert_eq!(message.header.offset, offset);
            assert_eq!(
                message.payload.as_ref(),
                payload(offset).as_slice(),
                "the payload at offset {offset}"
            );
            offset += 1;
        }
    }
}

/// Waits for a manifest that lists a segment, then checks it against the
/// objects in the store: a reader with the bucket alone finds every segment
/// the manifest names, and they cover the offsets from 0 to its floor.
async fn assert_manifest_lists_stored_segments(store: &Path) {
    let deadline = Instant::now() + TIERING_DEADLINE;
    let manifest = loop {
        match read_manifest(store.to_owned()).await {
            Some(manifest) if !manifest.segments.is_empty() => break manifest,
            read => assert!(
                Instant::now() < deadline,
                "within {TIERING_DEADLINE:?} the store got no manifest that lists a segment: \
                 {read:?}"
            ),
        }
        sleep(POLL_INTERVAL).await;
    };

    let stored = segment_logs(store);
    let mut next_offset = 0;
    for segment in &manifest.segments {
        assert_eq!(
            segment.start_offset, next_offset,
            "the manifest leaves a gap: {manifest:?}"
        );
        let object = format!("{:020}.log", segment.start_offset);
        assert!(
            stored.contains(&object),
            "the manifest lists {object}, and the store holds {stored:?}"
        );
        next_offset = segment.end_offset + 1;
    }
    assert_eq!(manifest.floor, Some(next_offset - 1));
    assert_eq!(manifest.trim_point, None);
}

/// The manifest of the one partition, read through the provider on a compio
/// runtime, as the provider does not run on the harness's.
async fn read_manifest(store: PathBuf) -> Option<PartitionManifest> {
    let path = files_named(&store, "manifest.bin").pop()?;
    let key = path
        .strip_prefix(&store)
        .ok()?
        .to_string_lossy()
        .into_owned();
    tokio::task::spawn_blocking(move || {
        compio::runtime::Runtime::new()
            .expect("a compio runtime")
            .block_on(async {
                let store = FsStore::new(&store);
                let size = store.head(&key).await.ok()?.size;
                let object = store.get_range(&key, 0..size).await.ok()?;
                Some(PartitionManifest::decode(&object.bytes).expect("a manifest that decodes"))
            })
    })
    .await
    .expect("the read does not panic")
}

fn payload(index: u64) -> Vec<u8> {
    let mut payload = vec![(index % 251) as u8; MESSAGE_BYTES];
    payload[..8].copy_from_slice(&index.to_le_bytes());
    payload
}

/// The file names of the segment logs under `root`, at any depth.
fn segment_logs(root: &Path) -> Vec<String> {
    let mut logs: Vec<String> = files(root)
        .into_iter()
        .filter(|path| path.extension().is_some_and(|extension| extension == "log"))
        .filter_map(|path| Some(path.file_name()?.to_string_lossy().into_owned()))
        .collect();
    logs.sort();
    logs
}

fn files_named(root: &Path, name: &str) -> Vec<PathBuf> {
    files(root)
        .into_iter()
        .filter(|path| path.file_name().is_some_and(|file| file == name))
        .collect()
}

/// Every file under `root`, at any depth.
fn files(root: &Path) -> Vec<PathBuf> {
    let mut files = Vec::new();
    let mut pending = vec![root.to_owned()];
    while let Some(directory) = pending.pop() {
        let Ok(entries) = std::fs::read_dir(&directory) else {
            continue;
        };
        for entry in entries.flatten() {
            let path = entry.path();
            if path.is_dir() {
                pending.push(path);
            } else {
                files.push(path);
            }
        }
    }
    files
}
