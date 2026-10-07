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

//! A partition write that finds no free file descriptor stops the server with
//! exit status 4, after the shutdown flush. The tests run the server under a
//! small `RLIMIT_NOFILE`, leave a few messages outside the durable WAL frontier, fill the
//! descriptor table with idle HTTP sockets, and write consumer offsets until
//! an offset file cannot be opened. A restart under the normal limit then
//! serves the messages that only the flush could have published durably.

use std::net::SocketAddr;
use std::path::PathBuf;
use std::time::Duration;

use bytes::Bytes;
use iggy::prelude::*;
use integration::harness::{ServerHandle, TestBinary, TestHarness};
use integration::iggy_harness;
use journal::partition_journal::{FRONTIER_FILE_NAME, PARTITION_WAL_BLOCK_SIZE};
use tokio::net::TcpStream;
use tokio::time::{sleep, timeout};

const STREAM_NAME: &str = "descriptor-exhaustion-stream";
const TOPIC_NAME: &str = "descriptor-exhaustion-topic";
const PARTITION_ID: u32 = 0;
/// Room for the boot of a two-shard server, and few enough sockets to fill
/// the rest quickly. More sockets than this cannot fit, so opening this many
/// fills the table whatever the boot used.
const OPEN_FILES_LIMIT: u64 = 128;
/// Far below `messages_required_to_save`, so their acknowledgement does not flush.
const PAYLOADS: [&str; 3] = ["first", "second", "third"];
const CONNECT_TIMEOUT: Duration = Duration::from_secs(2);
/// Each attempt uses a new consumer, so each one opens a new offset file.
const WRITE_ATTEMPTS: u32 = 20;
const WRITE_TIMEOUT: Duration = Duration::from_secs(5);
/// The HTTP accept loop retries a second after `EMFILE`, so a descriptor
/// that frees up goes to a waiting socket between two writes.
const WRITE_PAUSE: Duration = Duration::from_millis(500);
const EXIT_TIMEOUT: Duration = Duration::from_secs(60);
const WAL_FRONTIER_SLOTS: usize = 2;
const WAL_FRONTIER_MAGIC: &[u8; 8] = b"IGGYWAL3";
/// The head field in the checked `IGGYWAL3` frontier slot layout.
const WAL_HEAD_OFFSET: usize = 72;

/// How a test writes the offset of a new consumer.
#[derive(Clone, Copy)]
enum OffsetWrite {
    /// A client store, which opens the offset file for a write.
    Store,
    /// A poll with auto-commit. The commit of a consumer that has no offset in
    /// memory reads its offset file first.
    AutoCommitPoll,
}

#[iggy_harness(
    cluster_nodes = 1,
    server(
        message_bus.connections_max = "0",
        sharding.cpu_allocation = "0..2"
    )
)]
async fn given_no_free_descriptor_when_a_partition_write_fails_should_flush_and_exit_4(
    harness: &mut TestHarness,
) {
    stop_on_offset_write_without_descriptor(harness, OffsetWrite::Store).await;
}

#[iggy_harness(
    cluster_nodes = 1,
    server(
        message_bus.connections_max = "0",
        sharding.cpu_allocation = "0..2"
    )
)]
async fn given_no_free_descriptor_when_an_auto_commit_reads_its_offset_file_should_flush_and_exit_4(
    harness: &mut TestHarness,
) {
    stop_on_offset_write_without_descriptor(harness, OffsetWrite::AutoCommitPoll).await;
}

async fn stop_on_offset_write_without_descriptor(harness: &mut TestHarness, write: OffsetWrite) {
    harness
        .server_mut()
        .set_open_files_limit(Some(OPEN_FILES_LIMIT));
    harness
        .restart_server()
        .await
        .expect("restart under the descriptor limit");

    let client = harness.tcp_root_client().await.expect("TCP root client");
    let stream = client
        .create_stream(STREAM_NAME)
        .await
        .expect("create stream");
    let topic = client
        .create_topic(
            &Identifier::numeric(stream.id).expect("stream identifier"),
            TOPIC_NAME,
            &TopicCreateOptions {
                partitions_count: Some(1),
                ..TopicCreateOptions::default()
            },
        )
        .await
        .expect("create topic");
    let stream_id = Identifier::numeric(stream.id).expect("stream identifier");
    let topic_id = Identifier::numeric(topic.id).expect("topic identifier");
    let mut messages: Vec<IggyMessage> = PAYLOADS
        .iter()
        .map(|payload| {
            IggyMessage::builder()
                .payload(Bytes::from_static(payload.as_bytes()))
                .build()
                .expect("message")
        })
        .collect();
    client
        .send_messages(
            &stream_id,
            &topic_id,
            &Partitioning::partition_id(PARTITION_ID),
            &mut messages,
        )
        .await
        .expect("send messages");
    let frontier = partition_frontier(harness.server(), stream.id, topic.id);
    let before = std::fs::read(&frontier).expect("read the WAL frontier after sending");
    assert_eq!(
        before.len(),
        WAL_FRONTIER_SLOTS * PARTITION_WAL_BLOCK_SIZE,
        "unexpected partition WAL frontier size"
    );
    assert!(
        before
            .as_chunks::<PARTITION_WAL_BLOCK_SIZE>()
            .0
            .iter()
            .all(|slot| {
                assert_eq!(
                    &slot[..WAL_FRONTIER_MAGIC.len()],
                    WAL_FRONTIER_MAGIC,
                    "unsupported partition WAL frontier format"
                );
                u64::from_le_bytes(
                    slot[WAL_HEAD_OFFSET..WAL_HEAD_OFFSET + size_of::<u64>()]
                        .try_into()
                        .expect("WAL head field"),
                ) == 0
            }),
        "Replicated acknowledgements must leave the messages outside the durable WAL frontier"
    );

    let held = fill_descriptor_table(harness.server().http_addr().expect("HTTP listener")).await;
    harness.server_mut().expect_exit();
    for consumer_id in 1..=WRITE_ATTEMPTS {
        let consumer =
            Consumer::new(Identifier::numeric(consumer_id).expect("consumer identifier"));
        let written = match write {
            OffsetWrite::Store => timeout(
                WRITE_TIMEOUT,
                client.store_consumer_offset(
                    &consumer,
                    &stream_id,
                    &topic_id,
                    Some(PARTITION_ID),
                    0,
                ),
            )
            .await
            .is_ok_and(|stored| stored.is_ok()),
            OffsetWrite::AutoCommitPoll => timeout(
                WRITE_TIMEOUT,
                client.poll_messages(
                    &stream_id,
                    &topic_id,
                    Some(PARTITION_ID),
                    &consumer,
                    &PollingStrategy::offset(0),
                    1,
                    true,
                ),
            )
            .await
            .is_ok_and(|polled| polled.is_ok()),
        };
        if !written {
            break;
        }
        sleep(WRITE_PAUSE).await;
    }
    let status = harness
        .server_mut()
        .wait_for_exit(EXIT_TIMEOUT)
        .expect("the server stops after the failed offset write");
    assert_eq!(
        status.code(),
        Some(4),
        "a write that ran out of descriptors must stop the server with exit status 4, got {status}"
    );
    assert!(
        std::fs::read(&frontier).expect("read the WAL frontier after shutdown") != before,
        "the shutdown flush must publish the buffered messages before the exit"
    );
    assert_first_exhaustion_opened_an_offset_file(harness.server());

    drop(held);
    drop(client);
    harness.server_mut().set_open_files_limit(None);
    harness
        .server_mut()
        .start()
        .expect("restart under the normal limit");
    let client = harness.tcp_root_client().await.expect("TCP root client");
    let polled = client
        .poll_messages(
            &stream_id,
            &topic_id,
            Some(PARTITION_ID),
            &Consumer::default(),
            &PollingStrategy::offset(0),
            10,
            false,
        )
        .await
        .expect("poll after the restart");
    let payloads: Vec<&[u8]> = polled
        .messages
        .iter()
        .map(|message| message.payload.as_ref())
        .collect();
    assert_eq!(
        payloads,
        PAYLOADS.map(str::as_bytes),
        "the restart must serve the flushed messages"
    );
}

/// Idle sockets to the HTTP listener, which serves them on the accepting
/// shard, so each one it takes keeps a descriptor. A TCP socket is handed to
/// its owning shard through a duplicated descriptor, and shard 0 closes it
/// again when the duplicate fails, which would free a descriptor each time.
async fn fill_descriptor_table(http: SocketAddr) -> Vec<TcpStream> {
    let mut held = Vec::new();
    for _ in 0..OPEN_FILES_LIMIT {
        // Past the free descriptors, a socket waits in the listen backlog,
        // and past the backlog, the connect stops completing.
        match timeout(CONNECT_TIMEOUT, TcpStream::connect(http)).await {
            Ok(Ok(stream)) => held.push(stream),
            Ok(Err(_)) | Err(_) => break,
        }
    }
    held
}

/// Any open that finds no free descriptor sets the exit status, such as the
/// superblock persist on the way down, so the status alone cannot tell that
/// the offset write set it. The server logs only the first such open.
fn assert_first_exhaustion_opened_an_offset_file(server: &ServerHandle) {
    let path = server.data_path().join("logs/iggy-server.log");
    let log = std::fs::read_to_string(&path)
        .unwrap_or_else(|error| panic!("read {}: {error}", path.display()));
    let first = log
        .lines()
        .find(|line| line.contains("no free file descriptor while"))
        .expect("the server logs the first open that found no free descriptor");
    assert!(
        first.contains("/offsets/consumers/"),
        "the offset file must be the first open without a free descriptor, got: {first}"
    );
}

fn partition_frontier(server: &ServerHandle, stream_id: u32, topic_id: u32) -> PathBuf {
    let partition = server.data_path().join(format!(
        "streams/{stream_id}/topics/{topic_id}/partitions/{PARTITION_ID}"
    ));
    let directories: Vec<PathBuf> = std::fs::read_dir(&partition)
        .expect("read the partition directory")
        .map(|entry| entry.expect("read a partition entry").path())
        .filter(|path| {
            path.is_dir()
                && path
                    .file_name()
                    .is_some_and(|name| name.to_string_lossy().starts_with("prepares-"))
        })
        .collect();
    assert_eq!(directories.len(), 1, "one current partition incarnation");
    directories[0].join(FRONTIER_FILE_NAME)
}
