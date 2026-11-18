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

//! Heap that a shard holds for each idle partition. The tests count
//! allocations, because an RSS slope under mimalloc is too noisy to catch a
//! regression of a few KB per partition.
//!
//! Every partition here uses in-memory storage. The server builds its
//! partitions on file storage (`build_partition_fresh`), and these tests do
//! not count the heap of that storage.

use consensus::{LocalPipeline, PipelineEntry, PlaneKind, Sequencer, TimeoutManager, VsrConsensus};
use iggy_binary_protocol::{Command, Operation, PrepareOkHeader, RoutedRequestHeader};
use iggy_common::{DEFAULT_SEGMENT_SIZE, IggyByteSize, PartitionStats};
use message_bus::IggyMessageBus;
use partitions::{
    IggyPartition, IggyPartitions, PartitionPathLayout, PartitionsConfig, RepairRingBudget,
};
use server_common::Message;
use server_common::send_messages::{
    IggyMessage, IggyMessageHeader, IggyMessages, SendMessagesOwned,
};
use server_common::sharding::{IggyNamespace, ShardId};
use std::alloc::{GlobalAlloc, Layout, System};
use std::cell::Cell;
use std::rc::Rc;
use std::sync::Arc;
use std::thread::LocalKey;

const PARTITIONS: usize = 1024;
const WRITTEN_PARTITIONS: usize = 16;
const REPLICAS: u8 = 3;
/// `[partition] prepare_queue_depth` as shipped.
const PREPARE_QUEUE_DEPTH: usize = 32;
/// Measured at 8561 B: the 5.5 KB `IggyPartition` slot of the shard plus
/// 3.1 KB of heap. The 655 B of headroom catches the 3 KB view-change table
/// held inline, or all 1.2 KB of the boxed session state. It does not catch
/// one boxed field held inline, because the largest takes 360 B. Const
/// asserts next to `IggyPartition` and `VsrConsensus` pin each of those
/// fields to the size of one pointer.
const IDLE_PARTITION_BYTES_MAX: usize = 9 * 1024;
/// A primary frees its queues at the second commit heartbeat after its last
/// write.
const IDLE_TICKS: u64 = 2 * TimeoutManager::COMMIT_MESSAGE_TICKS;
const PAYLOAD: &[u8] = b"record";

type ReplicatedPartition = IggyPartition<Rc<IggyMessageBus>>;

#[global_allocator]
static ALLOCATOR: LiveBytes = LiveBytes;

/// Counts the bytes a watching thread allocates and frees.
struct LiveBytes;

thread_local! {
    static WATCHING: Cell<bool> = const { Cell::new(false) };
    static ALLOCATED: Cell<usize> = const { Cell::new(0) };
    static FREED: Cell<usize> = const { Cell::new(0) };
}

#[test]
fn given_never_written_partitions_on_a_shard_when_counting_heap_should_stay_within_the_idle_budget()
{
    let bus = Rc::new(IggyMessageBus::new(0));
    let (partitions, live) = live_bytes_during(|| {
        let partitions = IggyPartitions::with_capacity(ShardId::new(0), config(), PARTITIONS)
            .with_repair_rings(Rc::new(RepairRingBudget::new(usize::MAX)));
        for index in 0..PARTITIONS {
            let namespace = IggyNamespace::new(1, 1, index);
            partitions.insert(namespace, never_written_partition(namespace, &bus));
        }
        partitions
    });
    assert_eq!(partitions.len(), PARTITIONS);
    let per_partition = live / PARTITIONS;
    assert!(
        per_partition <= IDLE_PARTITION_BYTES_MAX,
        "a never-written partition holds {per_partition} bytes, above {IDLE_PARTITION_BYTES_MAX}"
    );
}

#[compio::test]
async fn given_partitions_that_filled_the_prepare_queue_when_idle_should_free_the_queue() {
    let bus = Rc::new(IggyMessageBus::new(0));
    let config = config();
    let mut partitions = Vec::with_capacity(WRITTEN_PARTITIONS);
    for index in 0..WRITTEN_PARTITIONS {
        let namespace = IggyNamespace::new(1, 1, index);
        let mut partition = never_written_partition(namespace, &bus);
        for request in (1..).take(PREPARE_QUEUE_DEPTH) {
            let append = append(namespace, &partition, request);
            partition.on_request(append, None).await;
        }
        assert!(partition.consensus().pipeline_is_full());
        commit_pipelined(&mut partition, &config).await;
        partitions.push(partition);
    }

    let ((), freed) = freed_bytes_during(|| {
        for partition in &partitions {
            for _ in 0..IDLE_TICKS {
                drop(partition.consensus().tick(PlaneKind::Partitions));
            }
        }
    });

    let per_partition = freed / WRITTEN_PARTITIONS;
    let prepare_queue_bytes = PREPARE_QUEUE_DEPTH * size_of::<PipelineEntry>();
    assert!(
        per_partition >= prepare_queue_bytes,
        "an idle partition freed {per_partition} bytes, below the {prepare_queue_bytes} bytes of \
         its prepare queue"
    );
}

/// A replicated partition on in-memory storage that never took a write: one
/// empty segment and the shipped pipeline depth.
fn never_written_partition(
    namespace: IggyNamespace,
    bus: &Rc<IggyMessageBus>,
) -> ReplicatedPartition {
    let consensus = VsrConsensus::new(
        1,
        0,
        REPLICAS,
        namespace.inner(),
        Rc::clone(bus),
        LocalPipeline::with_capacities(PREPARE_QUEUE_DEPTH, 2 * PREPARE_QUEUE_DEPTH),
    );
    consensus.init();
    IggyPartition::with_in_memory_storage(
        Arc::new(PartitionStats::default()),
        consensus,
        IggyByteSize::from(DEFAULT_SEGMENT_SIZE),
    )
}

fn config() -> PartitionsConfig {
    PartitionsConfig {
        messages_required_to_save: 100,
        size_of_messages_required_to_save: IggyByteSize::from(DEFAULT_SEGMENT_SIZE),
        validate_checksum: true,
        segment_size: IggyByteSize::from(DEFAULT_SEGMENT_SIZE),
        preallocate_segments: false,
        key_context: Arc::default(),
        path_layout: PartitionPathLayout::default(),
    }
}

/// One `SendMessages` request, from a client of its own.
fn append(
    namespace: IggyNamespace,
    partition: &ReplicatedPartition,
    request: u64,
) -> Message<RoutedRequestHeader> {
    let mut messages = IggyMessages::with_capacity(1);
    messages.push(IggyMessage {
        header: IggyMessageHeader {
            id: u128::from(request),
            payload_length: u32::try_from(PAYLOAD.len()).unwrap(),
            ..Default::default()
        },
        payload: PAYLOAD.to_vec().into(),
        user_headers: None,
    });
    SendMessagesOwned::from_messages(namespace, &messages)
        .unwrap()
        .encode_request(RoutedRequestHeader {
            command: Command::Request,
            operation: Operation::SendMessages,
            client: u128::from(request),
            session: 1,
            request,
            group: namespace.inner(),
            partition_incarnation: partition.created_revision(),
            ..Default::default()
        })
        .unwrap()
}

/// Commits every pipelined prepare without a remote replica. The primary
/// acknowledges its own writes, then the commit point moves to the head, as
/// the acknowledgments of a quorum move it.
#[allow(clippy::future_not_send)]
async fn commit_pipelined(partition: &mut ReplicatedPartition, config: &PartitionsConfig) {
    let mut acknowledgments = Vec::new();
    partition
        .consensus()
        .drain_loopback_into(&mut acknowledgments);
    for acknowledgment in acknowledgments {
        let mut prepare_ok = Message::<PrepareOkHeader>::new(acknowledgment.as_slice().len());
        prepare_ok
            .as_mut_slice()
            .copy_from_slice(acknowledgment.as_slice());
        partition.on_ack(prepare_ok, config).await;
    }
    let head = partition.consensus().sequencer().current_sequence();
    partition.consensus().advance_commit_max(head);
    partition.commit_journal(config).await;
    assert_eq!(partition.consensus().commit_min(), head);
    assert!(partition.consensus().pipeline_is_empty());
}

/// Runs `work` with this thread watched. Returns its output and the bytes it
/// left allocated.
fn live_bytes_during<T>(work: impl FnOnce() -> T) -> (T, usize) {
    let (output, allocated, freed) = watch(work);
    (output, allocated.saturating_sub(freed))
}

/// Runs `work` with this thread watched. Returns its output and the bytes it
/// freed. Not net of what it allocated: the ticks of an idle primary also
/// allocate, for the heartbeats they send.
fn freed_bytes_during<T>(work: impl FnOnce() -> T) -> (T, usize) {
    let (output, _, freed) = watch(work);
    (output, freed)
}

/// Runs `work` with this thread watched. Returns its output, then the bytes
/// it allocated and the bytes it freed.
fn watch<T>(work: impl FnOnce() -> T) -> (T, usize, usize) {
    ALLOCATED.with(|bytes| bytes.set(0));
    FREED.with(|bytes| bytes.set(0));
    WATCHING.with(|watching| watching.set(true));
    let output = work();
    WATCHING.with(|watching| watching.set(false));
    (output, ALLOCATED.with(Cell::get), FREED.with(Cell::get))
}

fn count(total: &'static LocalKey<Cell<usize>>, bytes: usize) {
    // `try_with`: the allocator also runs while a thread drops its locals.
    if WATCHING.try_with(Cell::get).unwrap_or(false) {
        let _ = total.try_with(|total| total.set(total.get() + bytes));
    }
}

// SAFETY: every call forwards to `System` with the caller's arguments. The
// default `realloc` goes through `alloc` and `dealloc`, so it is counted too.
unsafe impl GlobalAlloc for LiveBytes {
    unsafe fn alloc(&self, layout: Layout) -> *mut u8 {
        count(&ALLOCATED, layout.size());
        // SAFETY: forwarded with the caller's layout.
        unsafe { System.alloc(layout) }
    }

    unsafe fn dealloc(&self, ptr: *mut u8, layout: Layout) {
        count(&FREED, layout.size());
        // SAFETY: the block came from `alloc` above with this layout.
        unsafe { System.dealloc(ptr, layout) }
    }
}
