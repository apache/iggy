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

use std::ops::Range;
use std::str::FromStr;
use std::time::Duration;

use iggy::prelude::*;
use integration::iggy_harness;
use tokio::time::{sleep, timeout};

const STREAM_NAME: &str = "reader-positions-stream";
const TOPIC_NAME: &str = "reader-positions-topic";
const SEED_CONSUMER: &str = "reader-positions-seed";
const TIMESTAMP_START: &str = "timestamp-start";
const LAST_START: &str = "last-start";
const TIMESTAMP_SEEK: &str = "timestamp-seek";
const RESTART: &str = "restart";
const PARTITION_ID: u32 = 0;
const FIRST_BATCH: Range<u64> = 0..5;
/// Its first message is the one at the timestamp that the cases read from.
const SECOND_BATCH: Range<u64> = 5..10;
const BATCH_GAP: Duration = Duration::from_millis(20);
const READ_COUNT: u32 = 3;
const MAX_WAIT: Duration = Duration::from_secs(1);
const READ_TIMEOUT: Duration = Duration::from_secs(10);
const POLICIES: [CommitPolicy; 2] = [CommitPolicy::OnRead, CommitPolicy::Manual];

/// The server fetches only from an offset or from the next message, so the reader resolves a
/// Timestamp or a Last position with a single-partition poll first. A fake server that accepts
/// the other fetch targets cannot show that.
#[iggy_harness]
async fn given_timestamp_and_last_positions_when_reading_should_start_at_their_resolved_offsets(
    harness: &TestHarness,
) {
    let client = harness.root_client().await.unwrap();
    let at_timestamp = PollingStrategy::timestamp(seed(&client).await);
    let last: Vec<u64> = (SECOND_BATCH.end - u64::from(READ_COUNT)..SECOND_BATCH.end).collect();

    for commit in POLICIES {
        let mut reader = build(&client, TIMESTAMP_START, commit, at_timestamp).await;
        assert_eq!(
            read_offsets(&mut reader, 1).await,
            [SECOND_BATCH.start],
            "{commit:?}: a Timestamp start delivers the message at the timestamp first"
        );
        reader.close().await.unwrap();

        let mut reader = build(&client, LAST_START, commit, PollingStrategy::last()).await;
        assert_eq!(
            read_offsets(&mut reader, last.len()).await,
            last,
            "{commit:?}: a Last start delivers the last count messages first"
        );
        reader.close().await.unwrap();

        let mut reader = build(&client, TIMESTAMP_SEEK, commit, PollingStrategy::next()).await;
        assert_eq!(
            read_offsets(&mut reader, 1).await,
            [FIRST_BATCH.start],
            "{commit:?}: a Next start without a stored offset delivers the first message"
        );
        reader.seek(PARTITION_ID, at_timestamp).await.unwrap();
        assert_eq!(
            read_offsets(&mut reader, 1).await,
            [SECOND_BATCH.start],
            "{commit:?}: a timestamp seek delivers the message at the timestamp next"
        );
        reader.close().await.unwrap();
    }
    client.delete_stream(&stream_id()).await.unwrap();
}

/// With `OnRead` the stored offset wins over the start. So the second reader shows both that and
/// the offset that the close of the first reader left.
#[iggy_harness]
async fn given_a_closed_on_read_reader_when_restarted_at_the_timestamp_should_resume_after_it(
    harness: &TestHarness,
) {
    let client = harness.root_client().await.unwrap();
    let at_timestamp = PollingStrategy::timestamp(seed(&client).await);
    let rest: Vec<u64> = SECOND_BATCH.collect();

    let mut reader = build(&client, RESTART, CommitPolicy::OnRead, at_timestamp).await;
    assert_eq!(
        read_offsets(&mut reader, rest.len()).await,
        rest,
        "the first reader reads from the timestamp to the last message"
    );
    reader.close().await.unwrap();
    send(&client, SECOND_BATCH.end..SECOND_BATCH.end + 1).await;

    let mut reader = build(&client, RESTART, CommitPolicy::OnRead, at_timestamp).await;
    assert_eq!(
        read_offsets(&mut reader, 1).await,
        [SECOND_BATCH.end],
        "the new reader resumes after the stored offset, not at the timestamp"
    );
    reader.close().await.unwrap();
    client.delete_stream(&stream_id()).await.unwrap();
}

/// Sends the two batches and returns the timestamp that the server gave the first message of
/// the second one.
async fn seed(client: &IggyClient) -> IggyTimestamp {
    client
        .create_stream(STREAM_NAME, &StreamCreateOptions::default())
        .await
        .unwrap();
    client
        .create_topic(
            &stream_id(),
            TOPIC_NAME,
            &TopicCreateOptions {
                partitions_count: Some(1),
                ..TopicCreateOptions::default()
            },
        )
        .await
        .unwrap();
    send(client, FIRST_BATCH).await;
    // The second batch gets a later timestamp than each message of the first one, so the
    // timestamp of its first message names that message alone.
    sleep(BATCH_GAP).await;
    send(client, SECOND_BATCH).await;

    // The default options commit nothing, so no case starts with a stored offset.
    let polled = client
        .poll_messages(
            &stream_id(),
            &topic_id(),
            PARTITION_ID,
            &consumer(SEED_CONSUMER),
            &PollingStrategy::offset(FIRST_BATCH.start),
            PollOptions::default(),
        )
        .await
        .unwrap();
    let timestamp_of = |offset: u64| {
        polled
            .messages
            .iter()
            .find(|message| message.header.offset == offset)
            .map(|message| message.header.timestamp)
            .unwrap_or_else(|| panic!("the poll returns offset {offset}"))
    };
    let timestamp = timestamp_of(SECOND_BATCH.start);
    assert!(
        timestamp_of(FIRST_BATCH.end - 1) < timestamp,
        "the gap orders the batches in time"
    );
    IggyTimestamp::from(timestamp)
}

async fn send(client: &IggyClient, offsets: Range<u64>) {
    let mut messages: Vec<IggyMessage> = offsets
        .map(|offset| IggyMessage::from_str(&offset.to_string()).unwrap())
        .collect();
    client
        .send_messages(
            &stream_id(),
            &topic_id(),
            &Partitioning::partition_id(PARTITION_ID),
            &mut messages,
        )
        .await
        .unwrap();
}

/// A reader of the consumer of `case` under `commit`, so no two cases share a stored offset.
async fn build(
    client: &IggyClient,
    case: &str,
    commit: CommitPolicy,
    start: PollingStrategy,
) -> PartitionReader {
    client
        .reader(
            &stream_id(),
            &topic_id(),
            &consumer(&format!("{case}-{commit:?}")),
        )
        .partitions([PARTITION_ID])
        .start(start)
        .options(PollOptions {
            count: READ_COUNT,
            commit,
            ..PollOptions::wait(MAX_WAIT.into())
        })
        .build()
        .await
        .unwrap()
}

/// The offsets of the first `count` messages that the reader delivers.
async fn read_offsets(reader: &mut PartitionReader, count: usize) -> Vec<u64> {
    let mut offsets = Vec::with_capacity(count);
    while offsets.len() < count {
        let Ok(read) = timeout(READ_TIMEOUT, reader.next()).await else {
            panic!("the reader delivered only {offsets:?} in {READ_TIMEOUT:?}");
        };
        let batch = read.unwrap();
        offsets.extend(batch.messages.iter().map(|message| message.header.offset));
    }
    offsets.truncate(count);
    offsets
}

fn consumer(name: &str) -> Consumer {
    Consumer::new(Identifier::named(name).unwrap())
}

fn stream_id() -> Identifier {
    Identifier::named(STREAM_NAME).unwrap()
}

fn topic_id() -> Identifier {
    Identifier::named(TOPIC_NAME).unwrap()
}
