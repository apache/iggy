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

//! Heap that a shard holds for each idle partition. The test counts
//! allocations, because an RSS slope under mimalloc is too noisy to catch a
//! regression of a few KB per partition.

use consensus::{LocalPipeline, VsrConsensus};
use iggy_common::{DEFAULT_SEGMENT_SIZE, IggyByteSize, PartitionStats};
use message_bus::IggyMessageBus;
use partitions::{
    IggyPartition, IggyPartitions, PartitionPathLayout, PartitionsConfig, RepairRingBudget,
};
use server_common::sharding::{IggyNamespace, ShardId};
use std::alloc::{GlobalAlloc, Layout, System};
use std::cell::Cell;
use std::rc::Rc;
use std::sync::Arc;
use std::thread::LocalKey;

const PARTITIONS: usize = 1024;
const REPLICAS: u8 = 3;
/// `[partition] prepare_queue_depth` as shipped.
const PREPARE_QUEUE_DEPTH: usize = 32;
/// Measured at 12.7 KB: the 9.7 KB `IggyPartition` slot of the shard plus
/// 3 KB of heap. The headroom stays below the 13.8 KB that pipeline queues
/// allocated up front would add, so their return fails this test.
const IDLE_PARTITION_BYTES_MAX: usize = 16 * 1024;

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
fn given_idle_partitions_on_a_shard_when_counting_heap_should_stay_within_the_idle_budget() {
    let bus = Rc::new(IggyMessageBus::new(0));
    let (partitions, live) = live_bytes_during(|| {
        let partitions = IggyPartitions::with_capacity(ShardId::new(0), config(), PARTITIONS)
            .with_repair_rings(Rc::new(RepairRingBudget::new(u64::MAX)));
        for index in 0..PARTITIONS {
            let namespace = IggyNamespace::new(1, 1, index);
            partitions.insert(namespace, idle_partition(namespace, &bus));
        }
        partitions
    });
    assert_eq!(partitions.len(), PARTITIONS);
    let per_partition = live / PARTITIONS;
    assert!(
        per_partition <= IDLE_PARTITION_BYTES_MAX,
        "an idle partition holds {per_partition} bytes, above {IDLE_PARTITION_BYTES_MAX}"
    );
}

/// A replicated partition as `build_partition_fresh` leaves it: one empty
/// segment, the shipped pipeline depth, and no write yet.
fn idle_partition(
    namespace: IggyNamespace,
    bus: &Rc<IggyMessageBus>,
) -> IggyPartition<Rc<IggyMessageBus>> {
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

/// Runs `work` with this thread watched. Returns its output and the bytes it
/// left allocated.
fn live_bytes_during<T>(work: impl FnOnce() -> T) -> (T, usize) {
    ALLOCATED.with(|bytes| bytes.set(0));
    FREED.with(|bytes| bytes.set(0));
    WATCHING.with(|watching| watching.set(true));
    let output = work();
    WATCHING.with(|watching| watching.set(false));
    let live = ALLOCATED
        .with(Cell::get)
        .saturating_sub(FREED.with(Cell::get));
    (output, live)
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
