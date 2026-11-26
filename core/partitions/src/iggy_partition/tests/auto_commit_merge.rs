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

//! Commit-on-read stores of one consumer that wait behind its store in
//! flight. Three replicas and no peers: nothing commits until a test commits
//! the log, as a store whose flush has not completed.

use super::*;
use crate::state_machine::CommandKind;
use crate::state_machine::registry::tests::CounterMachine;
use consensus::PIPELINE_REQUEST_QUEUE_MAX;
use futures::FutureExt;

const CONSUMER_ID: u32 = 7;
const CLIENT_ID: u128 = 42;
const MACHINE: u16 = 1;
const KIND: u16 = 10;
/// More than the request queue holds, so a store per poll would refuse polls.
const POLLS: u64 = PIPELINE_REQUEST_QUEUE_MAX as u64 + 36;

/// One queued request, as the queue holds it: its operation, the offset it
/// stores, and whether a poll queued it.
type Queued = (Operation, Option<u64>, bool);

/// A primary of three with messages up to `POLLS`, and the counter machine.
fn primary() -> Box<IggyPartition<IggyMessageBus>> {
    let mut partition = Box::new(test_partition_with_replicas(3));
    partition.stats.increment_messages_count(POLLS + 1);
    partition.stats.set_current_offset(POLLS);
    partition
        .register_state_machine(Box::new(CounterMachine::new(MACHINE, &[KIND])))
        .unwrap();
    partition
}

/// A poll of the consumer that read up to `offset` and commits it.
async fn poll(partition: &mut IggyPartition<IggyMessageBus>, offset: u64) -> Option<IggyError> {
    let read = poll_read_result(
        partition,
        PollingConsumer::Consumer(CONSUMER_ID as usize, 0),
        true,
        Some(offset),
    );
    match partition.complete_poll(read) {
        Ok(completion) => {
            if let Some(replication) = completion.replication {
                partition.replicate_poll_completion(replication).await;
            }
            None
        }
        Err(error) => Some(error),
    }
}

fn queued(partition: &IggyPartition<IggyMessageBus>) -> Vec<Queued> {
    partition.consensus.with_pipeline(|pipeline| {
        pipeline
            .pending_requests()
            .map(|request| {
                let operation = request.message.header().operation;
                let offset = IggyPartition::<IggyMessageBus>::parse_consumer_offset_request(
                    operation,
                    &request.message,
                )
                .ok()
                .and_then(|(_, _, offset, _)| offset);
                (operation, offset, request.auto_commit().is_some())
            })
            .collect()
    })
}

/// Commit what the pipeline holds, and what the queue promotes behind it,
/// until both are empty.
async fn commit_everything(partition: &mut IggyPartition<IggyMessageBus>) {
    for _ in 0..16 {
        commit_log(partition).await;
        if partition.consensus.request_queue_len() == 0 && partition.consensus.pipeline_len() == 0 {
            return;
        }
    }
    panic!("the queue never drained: {:?}", queued(partition));
}

fn committed(partition: &IggyPartition<IggyMessageBus>) -> Option<u64> {
    partition.committed_offset(ConsumerKind::Consumer, CONSUMER_ID)
}

#[compio::test]
async fn given_polls_committing_on_read_while_the_first_store_waits_for_its_flush_when_admitted_should_hold_one_store_in_flight_and_one_queued_and_refuse_none()
 {
    let mut partition = primary();

    for offset in 0..POLLS {
        assert_eq!(poll(&mut partition, offset).await, None, "poll of {offset}");
    }

    assert_eq!(partition.consensus.pipeline_len(), 1);
    assert_eq!(
        queued(&partition),
        [(Operation::StoreConsumerOffset, Some(POLLS - 1), true)]
    );
    commit_everything(&mut partition).await;
    assert_eq!(committed(&partition), Some(POLLS - 1));
}

/// A store that commits on read applies as a maximum, so a poll that read
/// less than the queued store leaves it as it is.
#[compio::test]
async fn given_a_queued_store_when_a_poll_reads_less_should_keep_the_higher_offset() {
    let mut partition = primary();
    assert_eq!(poll(&mut partition, 0).await, None);
    assert_eq!(poll(&mut partition, 5).await, None);

    assert_eq!(poll(&mut partition, 3).await, None);

    assert_eq!(
        queued(&partition),
        [(Operation::StoreConsumerOffset, Some(5), true)]
    );
    commit_everything(&mut partition).await;
    assert_eq!(committed(&partition), Some(5));
}

/// An explicit store or delete of the consumer queued after its store that
/// commits on read stays between that store and the next poll's, and is
/// never rewritten.
#[compio::test]
async fn given_explicit_operations_queued_after_a_store_when_polls_commit_on_read_should_keep_them_in_order()
 {
    let mut partition = primary();
    assert_eq!(poll(&mut partition, 0).await, None);
    assert_eq!(poll(&mut partition, 1).await, None);
    partition
        .on_request(
            store_offset_request(
                CLIENT_ID,
                1,
                ConsumerKind::Consumer,
                CONSUMER_ID,
                0,
                AckLevel::Quorum,
            ),
            None,
        )
        .await;
    assert_eq!(poll(&mut partition, 2).await, None);
    assert_eq!(poll(&mut partition, 3).await, None);
    partition
        .on_request(delete_offset_request(CLIENT_ID + 1, 1, CONSUMER_ID), None)
        .await;
    assert_eq!(poll(&mut partition, 4).await, None);

    assert_eq!(
        queued(&partition),
        [
            (Operation::StoreConsumerOffset, Some(1), true),
            (Operation::StoreConsumerOffset, Some(0), false),
            (Operation::StoreConsumerOffset, Some(3), true),
            (Operation::DeleteConsumerOffset, None, false),
            (Operation::StoreConsumerOffset, Some(4), true),
        ]
    );
    commit_everything(&mut partition).await;
    assert_eq!(committed(&partition), Some(4));
}

/// A position note does not wait for a free queue: it waits behind the
/// stores, in order, and commits after them.
#[compio::test]
async fn given_a_queued_store_when_a_state_machine_command_arrives_should_queue_it_behind_and_apply_it()
 {
    let mut partition = primary();
    assert_eq!(poll(&mut partition, 0).await, None);
    assert_eq!(poll(&mut partition, 1).await, None);
    let (sender, receiver) = consensus::oneshot_channel();
    let command = partition
        .command_request(CommandKind(KIND), &3u64.to_le_bytes(), 1)
        .unwrap();

    partition.on_request(command, Some(sender)).await;

    assert_eq!(
        queued(&partition),
        [
            (Operation::StoreConsumerOffset, Some(1), true),
            (Operation::StateMachineCommand, None, false),
        ]
    );
    commit_everything(&mut partition).await;
    assert_eq!(receiver.await.unwrap().header().status, 0);
    assert_eq!(
        partition
            .state_machine::<CounterMachine>(crate::state_machine::MachineId(MACHINE))
            .unwrap()
            .total,
        3
    );
    assert_eq!(committed(&partition), Some(1));
}

/// Commands that wait in the queue hold their share of the pipeline as
/// prepared ones do.
#[compio::test]
async fn given_queued_commands_when_they_fill_their_share_should_refuse_the_next() {
    const DEPTH: usize = 4;
    let mut partition = Box::new(test_partition_with_pipeline(
        3,
        LocalPipeline::with_capacities(DEPTH, PIPELINE_REQUEST_QUEUE_MAX),
    ));
    partition.stats.increment_messages_count(2);
    partition.stats.set_current_offset(1);
    partition
        .register_state_machine(Box::new(CounterMachine::new(MACHINE, &[KIND])))
        .unwrap();
    assert_eq!(partition.command_pipeline_slots(), 1);
    assert_eq!(poll(&mut partition, 0).await, None);
    assert_eq!(poll(&mut partition, 1).await, None);
    let command = |partition: &IggyPartition<IggyMessageBus>, request: u64| {
        partition
            .command_request(CommandKind(KIND), &1u64.to_le_bytes(), request)
            .unwrap()
    };
    let first = command(&partition, 1);
    partition.on_request(first, None).await;

    let (sender, receiver) = consensus::oneshot_channel();
    let second = command(&partition, 2);
    partition.on_request(second, Some(sender)).await;

    let refusal = receiver
        .now_or_never()
        .expect("a refusal answers at once")
        .unwrap();
    assert_eq!(
        refusal.header().status,
        IggyError::TransientNotAccepted.as_code()
    );
    assert_eq!(
        queued(&partition)
            .iter()
            .filter(|(operation, _, _)| *operation == Operation::StateMachineCommand)
            .count(),
        1
    );
}
