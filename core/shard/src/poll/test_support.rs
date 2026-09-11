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

use std::sync::Arc;

use consensus::{LocalPipeline, Sequencer, VsrConsensus, oneshot_channel};
use futures::FutureExt;
use iggy_binary_protocol::{Command, Operation, RoutedRequestHeader};
use iggy_common::{IggyByteSize, PartitionStats, variadic};
use message_bus::MessageBus;
use metadata::MuxStateMachine;
use metadata::stm::stream::Streams;
use metadata::stm::user::Users;
use partitions::{IggyPartition, Partition, PartitionPathLayout, PartitionsConfig};
use server_common::send_messages::{
    IggyMessage, IggyMessageHeader, IggyMessages, SendMessagesOwned,
};
use server_common::sharding::IggyNamespace;

pub(super) type PollTestMetadata = MuxStateMachine<variadic!(Users, Streams)>;

/// Commit one batch starting at offset zero and keep it resident. Each call
/// creates an independent partition history, even for the same namespace.
#[allow(clippy::future_not_send)]
pub(super) async fn partition_with_messages<B: MessageBus + Clone>(
    bus: &B,
    namespace: IggyNamespace,
    payloads: &[&str],
) -> (IggyPartition<B>, PartitionsConfig) {
    let cluster_id = 1;
    let replica_id = 0;
    let replica_count = 3;
    let segment_size = IggyByteSize::from(1_048_576_u64);
    let config = PartitionsConfig {
        messages_required_to_save: 100,
        size_of_messages_required_to_save: segment_size,
        validate_checksum: true,
        segment_size,
        preallocate_segments: false,
        encryptor: None,
        path_layout: PartitionPathLayout::default(),
    };
    let consensus = VsrConsensus::new(
        cluster_id,
        replica_id,
        replica_count,
        namespace.inner(),
        bus.clone(),
        LocalPipeline::new(),
    );
    consensus.init();
    let mut partition = Box::new(IggyPartition::with_in_memory_storage(
        Arc::new(PartitionStats::default()),
        consensus,
        segment_size,
    ));

    assert!(!payloads.is_empty(), "fixture requires messages");
    let mut messages = IggyMessages::with_capacity(payloads.len());
    for (index, payload) in payloads.iter().enumerate() {
        messages.push(IggyMessage {
            header: IggyMessageHeader {
                id: index as u128 + 1,
                payload_length: u32::try_from(payload.len()).expect("fixture payload fits u32"),
                ..Default::default()
            },
            payload: payload.as_bytes().to_vec().into(),
            user_headers: None,
        });
    }
    let append = SendMessagesOwned::from_messages(namespace, &messages)
        .expect("encode fixture messages")
        .encode_request(RoutedRequestHeader {
            command: Command::Request,
            operation: Operation::SendMessages,
            client: 1,
            session: 1,
            request: 1,
            group: namespace.inner(),
            ..Default::default()
        })
        .expect("encode append request");
    let (append_reply, append_result) = oneshot_channel();
    partition.on_request(append, Some(append_reply)).await;
    assert_eq!(partition.consensus().sequencer().current_sequence(), 1);
    partition.consensus().advance_commit_max(1);
    partition.commit_journal(&config).await;
    assert_eq!(partition.consensus().commit_min(), 1);
    assert!(
        matches!(append_result.now_or_never(), Some(Ok(_))),
        "fixture append was committed"
    );
    assert_eq!(partition.offsets().commit_offset, payloads.len() as u64 - 1);

    (*partition, config)
}
