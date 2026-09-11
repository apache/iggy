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

use std::cell::RefCell;
use std::rc::Rc;
use std::sync::Arc;
use std::time::Duration;

use consensus::{LocalPipeline, PartitionsHandle, Sequencer, VsrConsensus, oneshot_channel};
use futures::FutureExt;
use futures::channel::oneshot;
use iggy_binary_protocol::{Command, Operation, RoutedRequestHeader};
use iggy_common::{IggyByteSize, PartitionStats, PollingStrategy, variadic};
use message_bus::{
    BusMessage, ClientForwardFn, ConnectionLostFn, JoinHandle, MessageBus, ReplicaForwardFn,
    SendError,
};
use metadata::stm::stream::Streams;
use metadata::stm::user::Users;
use metadata::{IggyMetadata, MuxStateMachine};
use partitions::{
    IggyPartition, IggyPartitions, Partition, PartitionPathLayout, PartitionsConfig, PollFragments,
    PollingArgs, PollingConsumer,
};
use server_common::MESSAGE_ALIGN;
use server_common::iobuf::Frozen;
use server_common::send_messages::{
    IggyMessage, IggyMessageHeader, IggyMessages, SendMessagesOwned, decode_batch_slice,
};
use server_common::sharding::{IggyNamespace, PartitionLocation, ShardId};

use super::completion::PollCompletionSender;
use crate::shards_table::{PapayaShardsTable, ShardsTable};
use crate::{
    IggyShard, LifecycleFrame, PARTITION_READ_TIMEOUT, PartitionConsensusConfig, PartitionRead,
    PartitionReadReply, ReplicaTopology, ShardFrame, ShardIdentity, shard_channel,
};

/// Characterize the current timeout behavior: losing the caller does not cancel
/// owner acceptance. This records the unseen messages that a later Next skips;
/// it does not prescribe the eventual cancellation contract.
#[compio::test]
#[allow(clippy::too_many_lines)]
async fn given_auto_commit_poll_when_completion_arrives_after_timeout_should_skip_unseen_messages_on_next()
 {
    let namespace = IggyNamespace::new(1, 1, 0);
    let consumer = PollingConsumer::Consumer(7, 0);
    let (expire_timeout, timeout_elapsed) = oneshot::channel();
    let bus = PollTimeoutBus {
        next_timeout: Rc::new(RefCell::new(Some(timeout_elapsed))),
    };
    let mut owner = owner_with_messages(&bus, namespace).await;
    let (owner_sender, owner_inbox, _owner_reply_lane) = shard_channel(0, 2, 1);
    owner.attach_senders(vec![owner_sender.clone()]);
    let partitions = owner.plane.partitions();
    let (stored_offset, partition_offset) = partitions
        .consumer_offset_read(&namespace, consumer)
        .expect("fixture partition exists");
    assert_eq!(stored_offset, None);
    assert_eq!(
        partition_offset, 3,
        "four messages are available at offsets 0 through 3"
    );

    // Start a real routed request, but let the test control when its completed
    // read reaches the owner. The fourth message will identify the next batch.
    let first_poll = owner.partition_read(
        namespace,
        PartitionRead::Poll {
            consumer,
            args: PollingArgs {
                strategy: PollingStrategy::next(),
                count: 3,
                auto_commit: true,
            },
        },
    );
    futures::pin_mut!(first_poll);
    assert!(futures::poll!(first_poll.as_mut()).is_pending());
    let ShardFrame::Lifecycle(LifecycleFrame::PartitionRead {
        namespace: requested_namespace,
        read:
            PartitionRead::Poll {
                consumer: requested_consumer,
                args,
            },
        reply,
    }) = owner_inbox
        .try_recv()
        .expect("poll reached the owner inbox")
    else {
        panic!("expected the routed poll request");
    };
    let read_plan = partitions
        .build_poll_snapshot(&requested_namespace, requested_consumer, &args)
        .expect("poll has a read snapshot");
    // Resident bytes keep disk scheduling out of this test. Holding the result
    // here models the delay before acceptance shared with detached disk reads.
    assert!(!read_plan.needs_off_pump_io());
    let delayed_result = read_plan.execute_resident();

    // The reply sender is still alive, so None must come from the timeout.
    expire_timeout
        .send(())
        .expect("requester is waiting on its timer");
    assert!(
        first_poll.await.is_none(),
        "the caller received no successful poll reply"
    );
    assert!(
        matches!(
            reply.try_send(PartitionReadReply::Ack),
            Err(crossfire::TrySendError::Disconnected(_))
        ),
        "timeout closed the reply channel before completion"
    );
    let (stored_offset, _) = partitions
        .consumer_offset_read(&namespace, consumer)
        .unwrap();
    assert_eq!(
        stored_offset, None,
        "reading alone has not accepted progress"
    );

    // Deliver the held result through the completion sender and owner inbox.
    // Current behavior admits its automatic commit even though delivery fails.
    PollCompletionSender::new(
        Some(owner_sender),
        namespace,
        reply,
        owner.metrics().clone(),
    )
    .complete(delayed_result);
    let ShardFrame::Lifecycle(LifecycleFrame::PollCompleted(completion)) = owner_inbox
        .try_recv()
        .expect("late completion reached the owner inbox")
    else {
        panic!("expected the completed poll");
    };
    owner.on_poll_completed(*completion).await;
    let (stored_offset, _) = partitions
        .consumer_offset_read(&namespace, consumer)
        .unwrap();
    assert_eq!(
        stored_offset,
        Some(2),
        "the nonempty late result advances local progress through the three unseen messages"
    );

    // A new Next now returns only offset 3. Disabling its automatic commit
    // keeps the final cursor attributable to the timed out poll alone.
    let next_poll = owner.partition_read(
        namespace,
        PartitionRead::Poll {
            consumer,
            args: PollingArgs {
                strategy: PollingStrategy::next(),
                count: 4,
                auto_commit: false,
            },
        },
    );
    futures::pin_mut!(next_poll);
    assert!(futures::poll!(next_poll.as_mut()).is_pending());
    let ShardFrame::Lifecycle(LifecycleFrame::PartitionRead {
        namespace,
        read,
        reply,
    }) = owner_inbox
        .try_recv()
        .expect("subsequent Next reached the owner inbox")
    else {
        panic!("expected the subsequent poll request");
    };
    owner.on_partition_read(namespace, read, reply).await;
    let Some(PartitionReadReply::Poll { fragments, .. }) = next_poll.await else {
        panic!("subsequent Next must return messages successfully");
    };
    assert_eq!(
        message_offsets(&fragments),
        vec![3],
        "Next skips offsets 0 through 2, which the caller never received"
    );
    let (stored_offset, _) = partitions
        .consumer_offset_read(&namespace, consumer)
        .unwrap();
    assert_eq!(stored_offset, Some(2));
}

fn message_offsets(fragments: &PollFragments) -> Vec<u64> {
    // A partial batch has separate header and payload fragments. Reassemble
    // the fixture's single batch before decoding its actual message offsets.
    let bytes: Vec<u8> = fragments
        .iter()
        .flat_map(|fragment| fragment.as_slice().iter().copied())
        .collect();
    let batch = decode_batch_slice(&bytes).expect("decode polled batch");
    batch
        .iter()
        .map(|message| batch.header.base_offset + u64::from(message.header.offset_delta))
        .collect()
}

type PollTestMetadata = MuxStateMachine<variadic!(Users, Streams)>;
type PollTestShard = IggyShard<PollTimeoutBus, (), (), PollTestMetadata, PapayaShardsTable>;

/// Commit four messages at offsets 0 through 3 while retaining their bytes in
/// the resident journal. The regression controls completion acceptance, so
/// setup needs neither disk files nor a running shard pump.
#[allow(clippy::future_not_send)]
async fn owner_with_messages(bus: &PollTimeoutBus, namespace: IggyNamespace) -> PollTestShard {
    let shard_id = ShardId::new(0);
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

    let mut messages = IggyMessages::with_capacity(4);
    for message_id in 1_u128..=4 {
        messages.push(IggyMessage {
            header: IggyMessageHeader {
                id: message_id,
                payload_length: 1,
                ..Default::default()
            },
            payload: "x".into(),
            user_headers: None,
        });
    }
    let append = SendMessagesOwned::from_messages(namespace, &messages)
        .expect("encode four messages")
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
    assert_eq!(partition.offsets().commit_offset, 3);

    let partitions = IggyPartitions::new(shard_id, config);
    partitions.insert(namespace, *partition);
    let metadata = IggyMetadata::new(None, None, None, None, PollTestMetadata::default(), None);
    let routes = PapayaShardsTable::new();
    routes.insert(namespace, PartitionLocation::new(shard_id, 0));
    PollTestShard::without_inbox(
        ShardIdentity::new(0, "poll-timeout-test".to_string()),
        bus.clone(),
        metadata,
        partitions,
        routes,
        PartitionConsensusConfig::new(
            cluster_id,
            ReplicaTopology::new(replica_id, replica_count),
            bus.clone(),
        ),
    )
}

/// Only the first request's timeout is controlled. Later timers stay pending;
/// neither request relies on elapsed wall time or a shortened production budget.
#[derive(Clone)]
struct PollTimeoutBus {
    next_timeout: Rc<RefCell<Option<oneshot::Receiver<()>>>>,
}

#[allow(clippy::future_not_send)]
impl MessageBus for PollTimeoutBus {
    fn sleep(&self, duration: Duration) -> impl Future<Output = ()> {
        assert_eq!(duration, PARTITION_READ_TIMEOUT);
        let timeout = self.next_timeout.borrow_mut().take();
        async move {
            if let Some(timeout) = timeout {
                timeout
                    .await
                    .expect("test controls when the timeout expires");
            } else {
                std::future::pending::<()>().await;
            }
        }
    }

    async fn send_to_client(
        &self,
        _client_id: u128,
        _data: impl Into<BusMessage>,
    ) -> Result<(), SendError> {
        panic!("partition reads reply through their channel");
    }

    fn send_to_replica(
        &self,
        _replica: u8,
        _data: Frozen<MESSAGE_ALIGN>,
    ) -> impl Future<Output = Result<(), SendError>> {
        // This test observes local progress, without acknowledging replication.
        std::future::ready(Ok(()))
    }

    fn set_connection_lost_fn(&self, _f: ConnectionLostFn) {}
    fn set_replica_forward_fn(&self, _f: ReplicaForwardFn) {}
    fn set_client_forward_fn(&self, _f: ClientForwardFn) {}
    fn track_background(&self, _handle: JoinHandle<()>) {}
}
