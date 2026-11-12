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

//! Fixtures that the tests of the consumer share.

mod commits;
mod polling;

use std::fmt::Debug;
use std::sync::Arc;
use std::time::Duration;

use futures::StreamExt;
use iggy_binary_protocol::codes::{
    DELETE_CONSUMER_OFFSET_CODE, FETCH_MESSAGES_CODE, GET_STREAM_CODE, GET_TOPIC_CODE,
    POLL_MESSAGES_DEFERRED_CODE, STORE_CONSUMER_OFFSET_CODE,
};
use iggy_binary_protocol::requests::consumer_offsets::{
    DeleteConsumerOffsetRequest, StoreConsumerOffsetRequest,
};
use iggy_binary_protocol::responses::consumer_groups::{
    ConsumerGroupDetailsResponse, ConsumerGroupResponse, SyncConsumerGroupResponse,
};
use iggy_binary_protocol::responses::messages::{
    PartitionRouteResponse, PollMessagesResponseHeader,
};
use iggy_binary_protocol::responses::streams::{GetStreamResponse, StreamResponse};
use iggy_binary_protocol::{WireDecode, WireEncode, WireName, WireOptions};
use iggy_common::locking::{IggyRwLock, IggyRwLockFn};
use iggy_common::{
    CommitPolicy, Consumer, Identifier, IggyDuration, NonZeroIggyDuration, PollOptions,
    PollingStrategy,
};

use crate::client_wrappers::client_wrapper::ClientWrapper;
use crate::clients::consumer::{IggyConsumer, ReceivedMessage};
use crate::clients::consumer_builder::IggyConsumerBuilder;
use crate::clients::reader::fixtures::{Section, fetch_body, served};
use crate::links::ReconnectPolicy;
use crate::links::fake_cluster::{
    Answer, FakeCluster, TEST_BUDGET, connected_with, endpoint, eventually, policy, topic_body,
};
use crate::tcp::tcp_client::TcpClient;
use tokio::task::yield_now;
use tokio::time::timeout;
use tracing::field::Field;
use tracing::span::{Attributes, Id, Record};
use tracing::{Event, Level, Metadata, Subscriber};

const POLLED_PARTITION: u32 = 1;
/// The replica that leads every partition of `connect_to_three_replicas`.
const PARTITION_PRIMARY: u8 = 1;
/// The incarnation of every partition in the route of `connect_to_three_replicas`.
const ROUTE_INCARNATION: u64 = 1;
const POLL_RETRY_INTERVAL: Duration = Duration::from_millis(10);
const POLL_TIMEOUT: Duration = Duration::from_secs(2);

fn standalone() -> Consumer {
    Consumer::new(Identifier::numeric(1).unwrap())
}

fn group() -> Consumer {
    Consumer::group(Identifier::numeric(1).unwrap())
}

/// A consumer of stream 1 and topic 1 on `client`, which retries a refused partition after
/// `POLL_RETRY_INTERVAL`.
fn builder_on(client: TcpClient, consumer: Consumer) -> IggyConsumerBuilder {
    IggyConsumerBuilder::new(
        IggyRwLock::new(ClientWrapper::Tcp(client)),
        "consumer".to_owned(),
        consumer,
        Identifier::numeric(1).unwrap(),
        Identifier::numeric(1).unwrap(),
        None,
    )
    .polling_retry_interval(NonZeroIggyDuration::new(POLL_RETRY_INTERVAL).unwrap())
}

/// A consumer on a client that never connects.
fn builder_for(consumer: Consumer) -> IggyConsumerBuilder {
    builder_on(TcpClient::default(), consumer)
}

/// A signed-in client of three ready replicas. Replica 0 is the metadata primary, and
/// `PARTITION_PRIMARY` leads every partition.
async fn connect_to_three_replicas() -> (TcpClient, Arc<FakeCluster>) {
    connect_to_three_replicas_with(policy()).await
}

async fn connect_to_three_replicas_with(policy: ReconnectPolicy) -> (TcpClient, Arc<FakeCluster>) {
    let cluster = FakeCluster::new(3);
    *cluster.route.lock().unwrap() = Some(PartitionRouteResponse {
        incarnation: ROUTE_INCARNATION,
        metadata_op: 1,
        view: 0,
        partitions_count: POLLED_PARTITION + 1,
        primary_replica: PARTITION_PRIMARY,
    });
    let links = connected_with(&cluster, 3, policy).await;
    (TcpClient::from_links(links), cluster)
}

/// Answers the stream and topic lookups of one `init`.
fn answer_lookups(cluster: &FakeCluster) {
    let stream = GetStreamResponse {
        stream: StreamResponse {
            id: 1,
            created_at: 0,
            topics_count: 0,
            size_bytes: 0,
            messages_count: 0,
            name: WireName::new("stream").unwrap(),
            options: WireOptions::empty(),
            derived_options: WireOptions::empty(),
        },
        topics: Vec::new(),
    };
    cluster.script(
        &endpoint(0),
        GET_STREAM_CODE,
        [Answer::Body(stream.to_bytes().to_vec())],
    );
    cluster.script(
        &endpoint(0),
        GET_TOPIC_CODE,
        [Answer::Body(topic_body(POLLED_PARTITION + 1))],
    );
}

/// The reply of a fetch that serves `served`.
fn fetch_of(served: &[Section]) -> Answer {
    Answer::Body(fetch_body(served).to_vec())
}

/// The reply of a fetch that serves `offsets` of `POLLED_PARTITION`, which continues after them.
fn poll_reply_of(offsets: &[u64]) -> Answer {
    let (section, records) = served(POLLED_PARTITION, offsets, offsets.last().unwrap() + 1);
    let mut body = PollMessagesResponseHeader {
        partition_id: POLLED_PARTITION,
        current_offset: section.current_offset,
        messages_count: section.messages_count,
        context: section.context,
    }
    .to_bytes()
    .to_vec();
    body.extend(records);
    Answer::Body(body)
}

fn reply_of(offsets: &[u64]) -> Answer {
    let next_offset = offsets.last().unwrap() + 1;
    fetch_of(&[served(POLLED_PARTITION, offsets, next_offset)])
}

/// Queues `answers` for the next requests of `code` to the primary of every partition.
fn script_primary(cluster: &FakeCluster, code: u32, answers: impl IntoIterator<Item = Answer>) {
    cluster.script(&endpoint(PARTITION_PRIMARY), code, answers);
}

async fn next_message(consumer: &mut IggyConsumer) -> ReceivedMessage {
    timeout(TEST_BUDGET, consumer.next())
        .await
        .unwrap()
        .unwrap()
        .unwrap()
}

/// Polls `consumer` until its next fetch reaches `cluster`, and leaves that fetch in flight.
async fn start_fetch(consumer: &mut IggyConsumer, cluster: &FakeCluster) {
    let sent = cluster.count(FETCH_MESSAGES_CODE);
    let mut next = consumer.next();
    timeout(TEST_BUDGET, async {
        while cluster.count(FETCH_MESSAGES_CODE) == sent {
            assert!(futures::poll!(&mut next).is_pending());
            yield_now().await;
        }
    })
    .await
    .unwrap();
}

/// An initialized consumer of `POLLED_PARTITION` under `commit`, which reads from the first
/// offset. Its fetches serve `replies`, and the fetch after them stays unanswered, so the reader
/// reads nothing more. It returns once that fetch is in flight, so every reply waits in the
/// reader.
async fn reading_consumer(
    commit: CommitPolicy,
    replies: &[&[u64]],
) -> (IggyConsumer, Arc<FakeCluster>) {
    let (client, cluster) = connect_to_three_replicas().await;
    answer_lookups(&cluster);
    script_primary(
        &cluster,
        FETCH_MESSAGES_CODE,
        replies
            .iter()
            .map(|offsets| reply_of(offsets))
            .chain([Answer::Silence]),
    );
    let mut consumer = builder_on(client, standalone())
        .partitions([POLLED_PARTITION])
        .polling_strategy(PollingStrategy::first())
        .poll_options(PollOptions {
            commit,
            ..PollOptions::wait(IggyDuration::ONE_SECOND)
        })
        .build();
    consumer.init().await.unwrap();
    eventually("the reader fetched every reply", || {
        cluster.count(FETCH_MESSAGES_CODE) == replies.len() + 1
    })
    .await;
    (consumer, cluster)
}

/// An initialized consumer of `POLLED_PARTITION` whose every fetch commits its batch. It reads
/// only when it is asked for a message, so a test scripts the fetches after `init`.
async fn committing_consumer(first_offset: u64) -> (IggyConsumer, Arc<FakeCluster>) {
    let (client, cluster) = connect_to_three_replicas().await;
    answer_lookups(&cluster);
    script_primary(
        &cluster,
        POLL_MESSAGES_DEFERRED_CODE,
        [poll_reply_of(&[first_offset])],
    );
    let mut consumer = builder_on(client, standalone())
        .partitions([POLLED_PARTITION])
        .poll_options(PollOptions {
            commit: CommitPolicy::OnRead,
            ..PollOptions::default()
        })
        .build();
    consumer.init().await.unwrap();
    (consumer, cluster)
}

/// The assignment of a group member in `generation`.
fn assignment(generation: u64, partitions: Vec<u32>) -> Answer {
    Answer::Body(
        SyncConsumerGroupResponse {
            generation,
            partitions,
        }
        .to_bytes()
        .to_vec(),
    )
}

fn existing_consumer_group() -> Answer {
    Answer::Body(
        ConsumerGroupDetailsResponse {
            group: ConsumerGroupResponse {
                id: 1,
                partitions_count: POLLED_PARTITION + 1,
                members_count: 0,
                name: WireName::new("consumer").unwrap(),
            },
            members: Vec::new(),
        }
        .to_bytes()
        .to_vec(),
    )
}

/// The partition and the offset of each store that reached `cluster`, in arrival order.
fn stored_offsets(cluster: &FakeCluster) -> Vec<(u32, u64)> {
    cluster
        .received(STORE_CONSUMER_OFFSET_CODE)
        .iter()
        .map(|request| {
            let store = StoreConsumerOffsetRequest::decode_from(&request.body).unwrap();
            (store.partition_id.unwrap(), store.offset)
        })
        .collect()
}

/// The partition of each delete that reached `cluster`, in arrival order.
fn deleted_partitions(cluster: &FakeCluster) -> Vec<u32> {
    cluster
        .received(DELETE_CONSUMER_OFFSET_CODE)
        .iter()
        .map(|request| {
            DeleteConsumerOffsetRequest::decode_from(&request.body)
                .unwrap()
                .partition_id
                .unwrap()
        })
        .collect()
}

/// Records the events of one level emitted on the test thread. The current-thread runtime of
/// `#[tokio::test]` polls the tasks it spawns on that thread, so their events count too.
#[derive(Clone)]
struct LevelReports {
    level: Level,
    events: Arc<std::sync::Mutex<Vec<String>>>,
}

impl Subscriber for LevelReports {
    fn enabled(&self, metadata: &Metadata<'_>) -> bool {
        *metadata.level() == self.level
    }

    fn new_span(&self, _: &Attributes<'_>) -> Id {
        Id::from_u64(1)
    }

    fn record(&self, _: &Id, _: &Record<'_>) {}

    fn record_follows_from(&self, _: &Id, _: &Id) {}

    fn event(&self, event: &Event<'_>) {
        let mut fields = Vec::new();
        event.record(&mut |field: &Field, value: &dyn Debug| {
            fields.push(format!("{field}: {value:?}"));
        });
        self.events.lock().unwrap().push(fields.join(", "));
    }

    fn enter(&self, _: &Id) {}

    fn exit(&self, _: &Id) {}
}

impl LevelReports {
    fn warnings() -> Self {
        Self {
            level: Level::WARN,
            events: Arc::default(),
        }
    }
}
