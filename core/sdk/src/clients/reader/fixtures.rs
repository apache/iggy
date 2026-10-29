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

//! Fixtures that the tests of the reader handle and of its fetch loop share.

use std::str::FromStr;
use std::sync::Arc;

use bytes::Bytes;
use iggy_binary_protocol::STATUS_OK;
use iggy_binary_protocol::WireDecode;
use iggy_binary_protocol::batch::{BATCH_HEADER_SIZE, BATCH_MESSAGE_HEADER_SIZE, BatchHeader};
use iggy_binary_protocol::codes::STORE_CONSUMER_OFFSET_CODE;
use iggy_binary_protocol::primitives::partition_history::PartitionContext;
use iggy_binary_protocol::requests::consumer_offsets::StoreConsumerOffsetRequest;
use iggy_binary_protocol::responses::messages::{
    FetchMessagesResponse, FetchMessagesSection, FetchMessagesSectionHeader, PartitionRouteResponse,
};
use iggy_common::locking::{IggyRwLock, IggyRwLockFn};
use iggy_common::{
    Consumer, FetchedSection, Identifier, IggyDuration, IggyError, IggyMessage, PollOptions,
    PolledMessages, ServerCommitPolicy,
};

use crate::client_wrappers::client_wrapper::ClientWrapper;
use crate::clients::reader_builder::{DEFAULT_MAX_WAIT, ReaderBuilder};
use crate::links::fake_cluster::{FakeCluster, connected};
use crate::tcp::tcp_client::TcpClient;

pub(super) const STREAM_ID: u32 = 1;
pub(super) const TOPIC_ID: u32 = 1;
pub(super) const CONSUMER_ID: u32 = 1;
pub(super) const GROUP_ID: u32 = 1;
/// Above every partition of the tests.
const PARTITIONS_COUNT: u32 = 4;
const REPLICAS: u8 = 1;
pub(super) const PRIMARY: u8 = 0;
const VIEW: u32 = 0;
/// The history of a batch until a test changes it. Metadata revisions start at 1, so no real
/// partition has the incarnation 0 of an uncaptured context.
pub(super) const CONTEXT: PartitionContext = PartitionContext {
    incarnation: 1,
    owner_generation: 0,
    metadata_op: 1,
};

/// A section as the server writes it: its table entry and its records.
pub(crate) type Section = (FetchMessagesSectionHeader, Vec<u8>);

/// A reader on a client that never connects.
pub(super) fn builder_for(consumer: Consumer) -> ReaderBuilder {
    builder_on(TcpClient::default(), consumer)
}

pub(super) fn builder_on(client: TcpClient, consumer: Consumer) -> ReaderBuilder {
    ReaderBuilder::new(
        IggyRwLock::new(ClientWrapper::Tcp(client)),
        identifier(STREAM_ID),
        identifier(TOPIC_ID),
        consumer,
        None,
    )
}

/// A signed-in client of one replica, which leads every partition and accepts each offset
/// store and delete that a test does not script.
pub(super) async fn connect_to_one_replica() -> (TcpClient, Arc<FakeCluster>) {
    let cluster = FakeCluster::new(REPLICAS);
    *cluster.route.lock().unwrap() = Some(PartitionRouteResponse {
        incarnation: CONTEXT.incarnation,
        metadata_op: CONTEXT.metadata_op,
        view: VIEW,
        partitions_count: PARTITIONS_COUNT,
        primary_replica: PRIMARY,
    });
    let links = connected(&cluster, usize::from(REPLICAS)).await;
    (TcpClient::from_links(links), cluster)
}

pub(super) fn identifier(id: u32) -> Identifier {
    Identifier::numeric(id).unwrap()
}

pub(super) fn group() -> Consumer {
    Consumer::group(identifier(GROUP_ID))
}

/// `builder` with `ServerCommitPolicy::OnRead` and the default wait.
pub(super) fn committing(builder: ReaderBuilder) -> ReaderBuilder {
    builder.options(PollOptions {
        server_commit: ServerCommitPolicy::OnRead,
        ..PollOptions::wait(IggyDuration::from(DEFAULT_MAX_WAIT))
    })
}

/// The offsets of the stores that `cluster` received, in order.
pub(super) fn stored_offsets(cluster: &FakeCluster) -> Vec<u64> {
    cluster
        .received(STORE_CONSUMER_OFFSET_CODE)
        .iter()
        .map(|store| {
            StoreConsumerOffsetRequest::decode_from(&store.body)
                .unwrap()
                .offset
        })
        .collect()
}

pub(super) fn partition_and_offsets(batch: &PolledMessages) -> (u32, Vec<u64>) {
    let offsets = batch
        .messages
        .iter()
        .map(|message| message.header.offset)
        .collect();
    (batch.partition_id, offsets)
}

pub(crate) fn message_at(offset: u64) -> IggyMessage {
    let mut message = IggyMessage::from_str("payload").unwrap();
    message.header.offset = offset;
    message
}

/// An unstamped batch of `partition` with one message at each of `offsets`.
pub(crate) fn batch(
    partition: u32,
    offsets: impl IntoIterator<Item = u64>,
    current_offset: u64,
) -> PolledMessages {
    let messages: Vec<_> = offsets.into_iter().map(message_at).collect();
    PolledMessages::new(
        PartitionContext::default(),
        partition,
        current_offset,
        u32::try_from(messages.len()).unwrap(),
        messages,
    )
}

/// A section of `partition` with one empty message at each of `offsets`, after which the
/// partition continues at `next_offset`. A zeroed frame is such a message, and the client does
/// not check its checksum.
pub(crate) fn served(partition: u32, offsets: &[u64], next_offset: u64) -> Section {
    let mut records = Vec::new();
    for offset in offsets {
        let mut batch = vec![0; BATCH_HEADER_SIZE + BATCH_MESSAGE_HEADER_SIZE];
        let mut header = BatchHeader::new(u64::from(partition), 0, batch.len() as u64, 1);
        header.base_offset = *offset;
        header.encode_into(&mut batch);
        records.extend(batch);
    }
    let header = FetchMessagesSectionHeader {
        partition_id: partition,
        status: STATUS_OK,
        current_offset: next_offset.saturating_sub(1),
        messages_count: u32::try_from(offsets.len()).unwrap(),
        context: PartitionContext::default(),
        next_offset,
        records_len: u32::try_from(records.len()).unwrap(),
    };
    (header, records)
}

/// A section that refuses `partition` with `error`.
pub(crate) fn refused(partition: u32, error: &IggyError) -> Section {
    let header = FetchMessagesSectionHeader {
        partition_id: partition,
        status: error.as_code(),
        current_offset: 0,
        messages_count: 0,
        context: PartitionContext::default(),
        next_offset: 0,
        records_len: 0,
    };
    (header, Vec::new())
}

/// The body of a fetch reply with `served`, as the server writes it.
pub(crate) fn fetch_body(served: &[Section]) -> Bytes {
    FetchMessagesResponse {
        sections: served
            .iter()
            .map(|(header, records)| FetchMessagesSection {
                header: *header,
                records,
            })
            .collect(),
    }
    .to_bytes()
    .unwrap()
}

/// The sections of a fetch reply with `served`, as the client returns them.
pub(crate) fn sections(served: &[Section]) -> Vec<FetchedSection> {
    FetchedSection::from_reply(&fetch_body(served)).unwrap()
}
