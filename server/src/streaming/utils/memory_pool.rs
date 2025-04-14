/* Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

use crate::configs::system::SystemConfig;
use bytes::BytesMut;
use crossbeam::queue::ArrayQueue;
use human_repr::HumanCount;
use once_cell::sync::OnceCell;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::Arc;
use tracing::{info, trace, warn};

pub static MEMORY_POOL: OnceCell<MemoryPool> = OnceCell::new();

const NUM_BUCKETS: usize = 28;
const BUCKET_CAPACITY: usize = 512;
const BUCKET_SIZES: [usize; NUM_BUCKETS] = [
    4 * 1024,
    8 * 1024,
    16 * 1024,
    32 * 1024,
    64 * 1024,
    128 * 1024,
    256 * 1024,
    512 * 1024,
    768 * 1024,
    1024 * 1024,
    1536 * 1024,
    2 * 1024 * 1024,
    3 * 1024 * 1024,
    4 * 1024 * 1024,
    6 * 1024 * 1024,
    8 * 1024 * 1024,
    12 * 1024 * 1024,
    16 * 1024 * 1024,
    24 * 1024 * 1024,
    32 * 1024 * 1024,
    48 * 1024 * 1024,
    64 * 1024 * 1024,
    96 * 1024 * 1024,
    128 * 1024 * 1024,
    192 * 1024 * 1024,
    256 * 1024 * 1024,
    384 * 1024 * 1024,
    512 * 1024 * 1024,
];

pub fn memory_pool() -> &'static MemoryPool {
    MEMORY_POOL.get().unwrap()
}

#[derive(Clone)]
pub struct MemoryPool {
    is_enabled: bool,
    memory_limit: usize,

    /// Reusable buffer buckets
    /// `buffer_bucket[i].len()` measures how many buffers are stored in that bucket for reuse (i.e. free buffers)
    buckets: [Arc<ArrayQueue<BytesMut>>; NUM_BUCKETS],

    /// Tracks the number of buffers currently in use, this value can grow and shrink.
    /// `buffer_in_use[i]` measures how many buffers are currently in use for that bucket size
    in_use: [Arc<AtomicUsize>; NUM_BUCKETS],

    /// Tracks the number of allocated buffers, this value can only grow.
    allocations: [Arc<AtomicUsize>; NUM_BUCKETS], // can only grow
    returned: [Arc<AtomicUsize>; NUM_BUCKETS], // can only grow

    external_allocations: Arc<AtomicUsize>,   // can only grow
    external_deallocations: Arc<AtomicUsize>, // can only grow

    resize_events: Arc<AtomicUsize>,   // can only grow
    dropped_returns: Arc<AtomicUsize>, // can only grow

    capacity_warning: Arc<AtomicBool>,
}

impl MemoryPool {
    pub fn new(is_enabled: bool, memory_limit: usize) -> Self {
        let buffer_buckets = [0; NUM_BUCKETS].map(|_| Arc::new(ArrayQueue::new(BUCKET_CAPACITY)));

        if is_enabled {
            info!(
                "Initializing MemoryPool with {NUM_BUCKETS} buckets, each capacity={BUCKET_CAPACITY}."
            );
        } else {
            info!("MemoryPool is disabled.");
        }

        Self {
            is_enabled,
            memory_limit,
            buckets: buffer_buckets,
            in_use: [0; NUM_BUCKETS].map(|_| Arc::new(AtomicUsize::new(0))),
            allocations: [0; NUM_BUCKETS].map(|_| Arc::new(AtomicUsize::new(0))),
            returned: [0; NUM_BUCKETS].map(|_| Arc::new(AtomicUsize::new(0))),
            external_allocations: Arc::new(AtomicUsize::new(0)),
            external_deallocations: Arc::new(AtomicUsize::new(0)),
            resize_events: Arc::new(AtomicUsize::new(0)),
            dropped_returns: Arc::new(AtomicUsize::new(0)),
            capacity_warning: Arc::new(AtomicBool::new(false)),
        }
    }

    pub fn init_pool(config: Arc<SystemConfig>) {
        let is_enabled = config.memory_pool.enabled;
        let memory_limit = config.memory_pool.size.as_bytes_usize();

        let pool = MemoryPool::new(is_enabled, memory_limit);
        if MEMORY_POOL.set(pool).is_err() {
            panic!("BytesMut pool already initialized.");
        }
    }

    pub fn acquire_buffer(&self, capacity: usize) -> BytesMut {
        if !self.is_enabled {
            return BytesMut::with_capacity(capacity);
        }

        let current = self.pool_current_size();

        match self.best_fit(capacity) {
            Some(idx) => {
                if let Some(mut buf) = self.buckets[idx].pop() {
                    buf.clear();
                    self.inc_bucket_in_use(idx);
                    trace!("Reused buffer, capacity: {}", BUCKET_SIZES[idx]);
                    return buf;
                }

                let new_size = BUCKET_SIZES[idx];

                if current + new_size > self.memory_limit {
                    self.capacity_warning.store(true, Ordering::Relaxed);
                    trace!(
                        "Pool is at capacity. Allocating outside the pool: requested {} B, current usage {} B, limit {} B",
                        new_size, current, self.memory_limit
                    );
                    self.inc_external_allocations();
                    return BytesMut::with_capacity(new_size);
                }

                self.inc_bucket_alloc(idx);
                self.inc_bucket_in_use(idx);
                BytesMut::with_capacity(new_size)
            }
            None => {
                if current + capacity > self.memory_limit {
                    trace!(
                        "Pool is at capacity. Allocating outside the pool: requested {} B, current usage {} B, limit {} B",
                        capacity, current, self.memory_limit
                    );
                    self.inc_external_allocations();
                    return BytesMut::with_capacity(capacity);
                }

                self.inc_external_allocations();
                BytesMut::with_capacity(capacity)
            }
        }
    }

    pub fn release_buffer(&self, buffer: BytesMut, original_capacity: usize) {
        if !self.is_enabled {
            return;
        }

        let current_capacity = buffer.capacity();
        if current_capacity != original_capacity {
            self.inc_resize_events();
            trace!(
                "Buffer capacity {} != original {} when returning",
                current_capacity,
                original_capacity
            );
        }

        match self.best_fit(current_capacity) {
            Some(idx) => {
                self.dec_bucket_in_use(idx);
                self.inc_bucket_return(idx);

                if self.buckets[idx].push(buffer).is_err() {
                    self.inc_dropped_returns();
                    trace!(
                        "Pool full for size: {} B, dropping buffer",
                        BUCKET_SIZES[idx]
                    );
                }
            }
            None => {
                self.inc_external_deallocations();
                trace!("Returned outside-of-pool buffer, capacity: {current_capacity}, dropping");
            }
        }
    }

    #[inline]
    fn best_fit(&self, capacity: usize) -> Option<usize> {
        for (i, &sz) in BUCKET_SIZES.iter().enumerate() {
            if sz >= capacity {
                return Some(i);
            }
        }
        None
    }

    pub fn log_stats(&self) {
        if !self.is_enabled || self.pool_current_size() == 0 {
            return;
        }

        let bucket_stats = (0..NUM_BUCKETS)
            .filter_map(|i| {
                let bucket_curr_el = self.bucket_current_elements(i);
                let bucket_alloc_el = self.bucket_allocated_elements(i);
                if bucket_curr_el > 0 || bucket_alloc_el > 0 {
                    Some(format!(
                        "{bucket_label}:[{bucket_curr_el}/{bucket_curr_size}/{bucket_alloc_el}/{bucket_alloc_size}/{bucket_returns}]",
                        bucket_label = if BUCKET_SIZES[i] >= 1024 * 1024 {
                            format!("{}MiB", BUCKET_SIZES[i] / (1024 * 1024))
                        } else {
                            format!("{}KiB", BUCKET_SIZES[i] / 1024)
                        },
                        bucket_curr_el = bucket_curr_el.human_count_bare(),
                        bucket_curr_size = size_str(self.bucket_current_size(i)),
                        bucket_alloc_el = bucket_alloc_el.human_count_bare(),
                        bucket_alloc_size = size_str(self.bucket_allocated_size(i)),
                        bucket_returns = self.bucket_returns(i).human_count_bare(),
                    ))
                } else {
                    None
                }
            })
            .collect::<Vec<String>>()
            .join(", ");

        info!(
            "Pool Buckets: {bucket_stats} (BucketLabel:[BucketEl/BucketSize/AllocEl/AllocSize/Returns])"
        );
        info!(
            "Pool Summary: Curr:{current}/Alloc:{allocated}/Util:{util:.1}%/Limit:{limit}/ExtAlloc:{ext_alloc}/ExtDealloc:{ext_dealloc}/DropRet:{drop_ret}/Resizes:{resize_events}/BucketCap:{bucket_cap}",
            current = size_str(self.pool_current_size()),
            allocated = size_str(self.pool_allocated_size()),
            util = self.pool_utilization(),
            limit = size_str(self.pool_maximum_size()),
            ext_alloc = self.external_allocations(),
            ext_dealloc = self.external_deallocations(),
            drop_ret = self.dropped_returns(),
            resize_events = self.resize_events(),
            bucket_cap = BUCKET_CAPACITY,
        );

        if self.capacity_warning.load(Ordering::Relaxed) {
            warn!("Memory pool is at capacity! Consider increasing the system.memory_pool.size.");
            self.capacity_warning.store(false, Ordering::Relaxed);
        }
    }

    fn pool_maximum_size(&self) -> usize {
        self.memory_limit
    }

    fn pool_current_size(&self) -> usize {
        let mut size = 0;
        for i in 0..NUM_BUCKETS {
            size += self.bucket_current_size(i);
        }
        size
    }

    fn pool_allocated_size(&self) -> usize {
        let mut size = 0;
        for i in 0..NUM_BUCKETS {
            size += self.bucket_allocated_size(i);
        }
        size
    }

    fn pool_utilization(&self) -> f64 {
        (self.pool_current_size() as f64 / self.pool_maximum_size() as f64) * 100.0
    }

    fn bucket_current_elements(&self, idx: usize) -> usize {
        self.in_use[idx].load(Ordering::Relaxed)
    }

    fn bucket_current_size(&self, idx: usize) -> usize {
        self.bucket_current_elements(idx) * BUCKET_SIZES[idx]
    }

    fn bucket_allocated_elements(&self, idx: usize) -> usize {
        self.allocations[idx].load(Ordering::Relaxed)
    }

    fn bucket_allocated_size(&self, idx: usize) -> usize {
        self.bucket_allocated_elements(idx) * BUCKET_SIZES[idx]
    }

    fn bucket_returns(&self, idx: usize) -> usize {
        self.returned[idx].load(Ordering::Relaxed)
    }

    fn resize_events(&self) -> usize {
        self.resize_events.load(Ordering::Relaxed)
    }

    fn dropped_returns(&self) -> usize {
        self.dropped_returns.load(Ordering::Relaxed)
    }

    fn external_allocations(&self) -> usize {
        self.external_allocations.load(Ordering::Relaxed)
    }

    fn external_deallocations(&self) -> usize {
        self.external_deallocations.load(Ordering::Relaxed)
    }
    fn inc_resize_events(&self) {
        self.resize_events.fetch_add(1, Ordering::Relaxed);
    }

    fn inc_dropped_returns(&self) {
        self.dropped_returns.fetch_add(1, Ordering::Relaxed);
    }

    fn inc_external_allocations(&self) {
        self.external_allocations.fetch_add(1, Ordering::Relaxed);
    }

    fn inc_external_deallocations(&self) {
        self.external_deallocations.fetch_add(1, Ordering::Relaxed);
    }

    fn inc_bucket_alloc(&self, idx: usize) {
        self.allocations[idx].fetch_add(1, Ordering::Relaxed);
    }

    fn inc_bucket_return(&self, idx: usize) {
        self.returned[idx].fetch_add(1, Ordering::Relaxed);
    }

    fn inc_bucket_in_use(&self, idx: usize) {
        self.in_use[idx].fetch_add(1, Ordering::Relaxed);
    }

    fn dec_bucket_in_use(&self, idx: usize) {
        self.in_use[idx].fetch_sub(1, Ordering::Relaxed);
    }
}

pub trait BytesMutExt {
    fn return_to_pool(self, original_capacity: usize);
}

impl BytesMutExt for BytesMut {
    fn return_to_pool(self, original_capacity: usize) {
        memory_pool().release_buffer(self, original_capacity);
    }
}

fn size_str(size: usize) -> String {
    if size >= 1024 * 1024 {
        format!("{}MiB", size / (1024 * 1024))
    } else if size >= 1024 {
        format!("{}KiB", size / 1024)
    } else {
        format!("{}B", size)
    }
}
