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
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use tracing::{info, trace, warn};

/// Global memory pool instance. Use `memory_pool()` to access it.
pub static MEMORY_POOL: OnceCell<MemoryPool> = OnceCell::new();

/// Total number of distinct bucket sizes.
const NUM_BUCKETS: usize = 32;

/// Array of bucket sizes in ascending order. Each entry is a distinct buffer size (in bytes).
const BUCKET_SIZES: [usize; NUM_BUCKETS] = [
    256,
    512,
    1024,
    2 * 1024,
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
    2 * 1024 * 1024, // Above 2MiB everything should be rounded up to the next power of 2 to take advantage of hugepages
    4 * 1024 * 1024, // (environment variables MIMALLOC_ALLOW_LARGE_OS_PAGES=1 and MIMALLOC_LARGE_OS_PAGES=1).
    6 * 1024 * 1024,
    8 * 1024 * 1024,
    10 * 1024 * 1024,
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

/// Retrieve the global `MemoryPool` instance. Panics if not yet initialized.
pub fn memory_pool() -> &'static MemoryPool {
    MEMORY_POOL
        .get()
        .expect("Memory pool not initialized - MemoryPool::init_pool should be called first")
}

/// A memory pool that maintains fixed-size buckets for reusing `BytesMut` buffers.
///
/// Each bucket corresponds to a particular size in `BUCKET_SIZES`. The pool tracks:
/// - Buffers currently in use (`in_use`)
/// - Buffers allocated historically (`allocations`)
/// - Buffers returned to the pool (`returned`)
/// - External allocations/deallocations (buffers allocated outside the pool limit)
/// - Resizes and dropped returns
#[derive(Clone)]
pub struct MemoryPool {
    /// Whether the pool is enabled.
    pub is_enabled: bool,

    /// Configured maximum bytes for which the pool is responsible.
    pub memory_limit: usize,

    /// Configured maximum number of buffers in each bucket.
    pub bucket_capacity: usize,

    /// Array of queues for reusable buffers. Each queue can store up to `bucket_capacity` buffers.
    /// The length of each queue (`buckets[i].len()`) is how many **free** buffers are currently available.
    /// Free doesn't mean the buffer is allocated, it just means it's not in use.
    buckets: [Arc<ArrayQueue<BytesMut>>; NUM_BUCKETS],

    /// Number of buffers **in use** for each bucket size (grow/shrink as they are acquired/released).
    in_use: [Arc<AtomicUsize>; NUM_BUCKETS],

    /// Total number of buffers **ever allocated** in each bucket. Monotonically increasing.
    allocations: [Arc<AtomicUsize>; NUM_BUCKETS],

    /// Total number of buffers **ever returned** to each bucket. Monotonically increasing.
    returned: [Arc<AtomicUsize>; NUM_BUCKETS],

    /// Count of buffers allocated outside the pool limit (e.g., if usage exceeds `memory_limit` or no matching bucket).
    external_allocations: Arc<AtomicUsize>,

    /// Count of buffers deallocated outside the pool (e.g., returning a buffer that doesn't match any bucket).
    external_deallocations: Arc<AtomicUsize>,

    /// Number of resize events detected (the buffer capacity changed between acquire and release).
    resize_events: Arc<AtomicUsize>,

    /// Number of returns that couldn't be stored because the relevant bucket queue was at capacity.
    dropped_returns: Arc<AtomicUsize>,

    /// Flag set when the pool usage first exceeds `memory_limit`. Used to log a single warning.
    capacity_warning: Arc<AtomicBool>,
}

impl MemoryPool {
    /// Create a new memory pool. Usually called from `init_pool` below.
    pub fn new(is_enabled: bool, memory_limit: usize, bucket_capacity: usize) -> Self {
        let buckets = [0; NUM_BUCKETS].map(|_| Arc::new(ArrayQueue::new(bucket_capacity)));

        if is_enabled {
            info!(
                "Initializing MemoryPool with {NUM_BUCKETS} buckets, each will have capacity: {bucket_capacity}."
            );
        } else {
            info!("MemoryPool is disabled.");
        }

        Self {
            is_enabled,
            memory_limit,
            bucket_capacity,
            buckets,
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

    /// Initialize the global pool from the given config.
    pub fn init_pool(config: Arc<SystemConfig>) {
        let is_enabled = config.memory_pool.enabled;
        let memory_limit = config.memory_pool.size.as_bytes_usize();
        let bucket_capacity = config.memory_pool.bucket_capacity as usize;

        let _ =
            MEMORY_POOL.get_or_init(|| MemoryPool::new(is_enabled, memory_limit, bucket_capacity));
    }

    /// Acquire a `BytesMut` buffer with at least `capacity` bytes.
    ///
    /// - If a bucket can fit `capacity`, try to pop from its free buffer queue; otherwise create a new buffer.
    /// - If `memory_limit` would be exceeded, allocate outside the pool.
    ///
    /// Returns a tuple of (buffer, was_pool_allocated) where was_pool_allocated indicates if the buffer
    /// was allocated from the pool (true) or externally (false).
    pub fn acquire_buffer(&self, capacity: usize) -> (BytesMut, bool) {
        if !self.is_enabled {
            return (BytesMut::with_capacity(capacity), false);
        }

        let current = self.pool_current_size();

        match self.best_fit(capacity) {
            Some(idx) => {
                if let Some(mut buf) = self.buckets[idx].pop() {
                    buf.clear();
                    self.inc_bucket_in_use(idx);
                    return (buf, true);
                }

                let new_size = BUCKET_SIZES[idx];
                if current + new_size > self.memory_limit {
                    self.set_capacity_warning(true);
                    trace!(
                        "Pool is at capacity. Allocating outside the pool: requested {} B, current usage {} B, limit {} B",
                        new_size, current, self.memory_limit
                    );
                    self.inc_external_allocations();
                    return (BytesMut::with_capacity(new_size), false);
                }

                self.inc_bucket_alloc(idx);
                self.inc_bucket_in_use(idx);
                (BytesMut::with_capacity(new_size), true)
            }
            None => {
                if current + capacity > self.memory_limit {
                    trace!(
                        "Pool is at capacity. Allocating outside the pool: requested {} B, current usage {} B, limit {} B",
                        capacity, current, self.memory_limit
                    );
                    self.inc_external_allocations();
                    return (BytesMut::with_capacity(capacity), false);
                }

                self.inc_external_allocations();
                (BytesMut::with_capacity(capacity), false)
            }
        }
    }

    /// Return a `BytesMut` buffer previously acquired from the pool.
    ///
    /// - If `current_capacity` differs from `original_capacity`, increments `resize_events`.
    /// - If a matching bucket exists, place it back in that bucket's queue (if space is available).
    /// - Otherwise, treat it as an external deallocation.
    /// - The `was_pool_allocated` flag indicates if this buffer was originally allocated from the pool.
    pub fn release_buffer(
        &self,
        buffer: BytesMut,
        original_capacity: usize,
        was_pool_allocated: bool,
    ) {
        if !self.is_enabled {
            return;
        }

        let current_capacity = buffer.capacity();
        if current_capacity != original_capacity {
            self.inc_resize_events();
            trace!(
                "Buffer capacity {} != original {} when returning",
                current_capacity, original_capacity
            );
        }

        if was_pool_allocated && let Some(orig_idx) = self.best_fit(original_capacity) {
            self.dec_bucket_in_use(orig_idx);
        }

        match self.best_fit(current_capacity) {
            Some(idx) => {
                self.inc_bucket_return(idx);

                if self.buckets[idx].push(buffer).is_err() {
                    self.inc_dropped_returns();
                    trace!(
                        "Pool full for size: {} B, dropping buffer",
                        BUCKET_SIZES[idx]
                    );
                    self.set_capacity_warning(true);
                }
            }
            None => {
                self.inc_external_deallocations();
                trace!("Returned outside-of-pool buffer, capacity: {current_capacity}, dropping");
            }
        }
    }

    /// Write a log message summarizing current usage, allocations, etc.
    /// Only logs if the pool is enabled and `pool_current_size()` is non-zero.
    /// Also logs a warning if the pool usage has exceeded `memory_limit`.
    pub fn log_stats(&self) {
        if !self.is_enabled || self.pool_current_size() == 0 {
            return;
        }

        let bucket_stats = (0..NUM_BUCKETS)
            .filter_map(|i| {
                let current_el = self.bucket_current_elements(i);
                let allocated_el = self.bucket_allocated_elements(i);

                if current_el > 0 || allocated_el > 0 {
                    Some(format!(
                        "{label}:[{current_el}/{current_size}/{allocated_el}/{allocated_size}/{returns}]",
                        label = size_str(BUCKET_SIZES[i]),
                        current_el = current_el.human_count_bare(),
                        current_size = size_str(self.bucket_current_size(i)),
                        allocated_el = allocated_el.human_count_bare(),
                        allocated_size = size_str(self.bucket_allocated_size(i)),
                        returns = self.bucket_returns(i).human_count_bare(),
                    ))
                } else {
                    None
                }
            })
            .collect::<Vec<String>>()
            .join(", ");

        info!(
            "Pool Buckets: {bucket_stats} (BucketLabel:[InUseCount/InUseSize/AllocCount/AllocSize/Returns])"
        );
        info!(
            "Pool Summary: Curr:{current}/Alloc:{allocated}/Util:{util:.1}%/Limit:{limit}/ExtAlloc:{ext_alloc}/ExtDealloc:{ext_dealloc}/DropRet:{drop_ret}/Resizes:{resize_events}/BucketCap:{cap}",
            current = size_str(self.pool_current_size()),
            allocated = size_str(self.pool_allocated_size()),
            util = self.pool_utilization(),
            limit = size_str(self.pool_maximum_size()),
            ext_alloc = self.external_allocations(),
            ext_dealloc = self.external_deallocations(),
            drop_ret = self.dropped_returns(),
            resize_events = self.resize_events(),
            cap = self.bucket_capacity,
        );

        if self.should_print_warning() {
            warn!("Memory pool usage exceeded limit! Consider adjusting memory_pool.size.");
            self.set_capacity_warning(false);
        }
    }

    #[inline]
    pub fn best_fit(&self, capacity: usize) -> Option<usize> {
        match BUCKET_SIZES.binary_search(&capacity) {
            Ok(idx) => Some(idx),
            Err(idx) => {
                if idx < NUM_BUCKETS {
                    Some(idx)
                } else {
                    None
                }
            }
        }
    }

    /// Returns the configured maximum size of the pool, usually from config, in bytes.
    fn pool_maximum_size(&self) -> usize {
        self.memory_limit
    }

    /// Sums the sizes (in bytes) of all buffers currently in use across the pool.
    fn pool_current_size(&self) -> usize {
        (0..NUM_BUCKETS)
            .filter(|&i| self.bucket_current_elements(i) > 0)
            .map(|i| self.bucket_current_size(i))
            .sum()
    }
    /// Sums the sizes (in bytes) of all buffers ever allocated across the pool (historical).
    fn pool_allocated_size(&self) -> usize {
        let mut size = 0;
        for i in 0..NUM_BUCKETS {
            size += self.bucket_allocated_size(i);
        }
        size
    }

    /// Returns pool utilization percentage.
    fn pool_utilization(&self) -> f64 {
        (self.pool_current_size() as f64 / self.pool_maximum_size() as f64) * 100.0
    }

    fn bucket_current_elements(&self, idx: usize) -> usize {
        self.in_use[idx].load(Ordering::Acquire)
    }

    fn bucket_current_size(&self, idx: usize) -> usize {
        self.bucket_current_elements(idx) * BUCKET_SIZES[idx]
    }

    fn bucket_allocated_elements(&self, idx: usize) -> usize {
        self.allocations[idx].load(Ordering::Acquire)
    }

    fn bucket_allocated_size(&self, idx: usize) -> usize {
        self.bucket_allocated_elements(idx) * BUCKET_SIZES[idx]
    }

    fn bucket_returns(&self, idx: usize) -> usize {
        self.returned[idx].load(Ordering::Acquire)
    }

    fn resize_events(&self) -> usize {
        self.resize_events.load(Ordering::Acquire)
    }

    fn dropped_returns(&self) -> usize {
        self.dropped_returns.load(Ordering::Acquire)
    }

    fn external_allocations(&self) -> usize {
        self.external_allocations.load(Ordering::Acquire)
    }

    fn external_deallocations(&self) -> usize {
        self.external_deallocations.load(Ordering::Acquire)
    }

    pub(super) fn inc_resize_events(&self) {
        self.resize_events.fetch_add(1, Ordering::Release);
    }

    pub(super) fn inc_dropped_returns(&self) {
        self.dropped_returns.fetch_add(1, Ordering::Release);
    }

    pub(super) fn inc_external_allocations(&self) {
        self.external_allocations.fetch_add(1, Ordering::Release);
    }

    pub(super) fn inc_external_deallocations(&self) {
        self.external_deallocations.fetch_add(1, Ordering::Release);
    }

    pub(super) fn inc_bucket_alloc(&self, idx: usize) {
        self.allocations[idx].fetch_add(1, Ordering::Release);
    }

    pub(super) fn inc_bucket_return(&self, idx: usize) {
        self.returned[idx].fetch_add(1, Ordering::Release);
    }

    pub(super) fn inc_bucket_in_use(&self, idx: usize) {
        self.in_use[idx].fetch_add(1, Ordering::Release);
    }

    pub(super) fn dec_bucket_in_use(&self, idx: usize) {
        self.in_use[idx].fetch_sub(1, Ordering::Release);
    }

    fn should_print_warning(&self) -> bool {
        self.capacity_warning.load(Ordering::Acquire)
    }

    fn set_capacity_warning(&self, value: bool) {
        self.capacity_warning.store(value, Ordering::Release);
    }
}

/// Return a buffer to the pool by calling `release_buffer` with the original capacity.
/// This extension trait makes it easy to do `some_bytes.return_to_pool(orig_cap, was_pool_allocated)`.
pub trait BytesMutExt {
    fn return_to_pool(self, original_capacity: usize, was_pool_allocated: bool);
}

impl BytesMutExt for BytesMut {
    fn return_to_pool(self, original_capacity: usize, was_pool_allocated: bool) {
        memory_pool().release_buffer(self, original_capacity, was_pool_allocated);
    }
}

/// Convert a size in bytes to a string like “8KiB” or “2MiB”.
fn size_str(size: usize) -> String {
    if size >= 1024 * 1024 {
        format!("{}MiB", size / (1024 * 1024))
    } else if size >= 1024 {
        format!("{}KiB", size / 1024)
    } else {
        format!("{size}B")
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{configs::system::MemoryPoolConfig, streaming::utils::PooledBuffer};
    use iggy_common::IggyByteSize;
    use serial_test::serial;
    use std::{str::FromStr, sync::Once};

    static TEST_INIT: Once = Once::new();

    fn initialize_pool_for_tests() {
        TEST_INIT.call_once(|| {
            let config = Arc::new(SystemConfig {
                memory_pool: MemoryPoolConfig {
                    enabled: true,
                    size: IggyByteSize::from_str("4GiB").unwrap(),
                    bucket_capacity: 8192,
                },
                ..SystemConfig::default()
            });
            MemoryPool::init_pool(config);
        });
    }

    #[test]
    #[serial(memory_pool)]
    fn test_pooled_buffer_resize_tracking() {
        initialize_pool_for_tests();

        let pool = memory_pool();
        let initial_resize_events = pool.resize_events();

        let small_bucket_idx = 2;
        let small_bucket_size = BUCKET_SIZES[small_bucket_idx];

        let mut buffer = PooledBuffer::with_capacity(small_bucket_size);

        assert_eq!(
            buffer.capacity(),
            small_bucket_size,
            "Initial capacity should match requested size"
        );

        let original_in_use_before = pool.bucket_current_elements(small_bucket_idx);

        // Force a resize with data significantly larger than current bucket
        // Use 256KiB data to ensure it goes to a much larger bucket
        let large_bucket_idx = 6;
        let large_data_size = BUCKET_SIZES[large_bucket_idx] - 1024;
        buffer.put_slice(&vec![0u8; large_data_size]);

        assert!(
            buffer.capacity() >= large_data_size,
            "Buffer should resize to fit data"
        );

        assert_eq!(
            pool.resize_events(),
            initial_resize_events + 1,
            "Resize event should be recorded"
        );

        assert_eq!(
            pool.bucket_current_elements(small_bucket_idx),
            original_in_use_before - 1,
            "Original bucket in-use count should decrease by 1"
        );

        let new_bucket_idx = pool.best_fit(buffer.capacity()).unwrap();
        assert!(
            new_bucket_idx > small_bucket_idx,
            "Buffer should move to a larger bucket"
        );

        let new_bucket_in_use = pool.bucket_current_elements(new_bucket_idx);
        assert!(
            new_bucket_in_use > 0,
            "New bucket should have buffers in use"
        );

        drop(buffer);

        assert_eq!(
            pool.bucket_current_elements(small_bucket_idx),
            original_in_use_before - 1,
            "Small bucket count should remain decreased after drop"
        );

        assert_eq!(
            pool.bucket_current_elements(new_bucket_idx),
            new_bucket_in_use - 1,
            "New bucket in-use count should decrease after drop"
        );
    }

    #[test]
    #[serial(memory_pool)]
    fn test_multiple_resize_operations() {
        initialize_pool_for_tests();

        let pool = memory_pool();
        let initial_resize_events = pool.resize_events();

        let mut buffer = PooledBuffer::with_capacity(BUCKET_SIZES[1]); // 8KiB
        let original_bucket_idx = pool.best_fit(buffer.capacity()).unwrap();
        let original_bucket_in_use = pool.bucket_current_elements(original_bucket_idx);

        let first_resize_size = BUCKET_SIZES[4]; // 64KiB
        buffer.put_slice(&vec![0u8; first_resize_size]);

        assert!(
            buffer.capacity() >= first_resize_size,
            "Buffer should increase capacity after first resize"
        );
        assert_eq!(
            pool.resize_events(),
            initial_resize_events + 1,
            "One resize event should be recorded"
        );

        let mid_bucket_idx = pool.best_fit(buffer.capacity()).unwrap();
        assert!(
            mid_bucket_idx > original_bucket_idx,
            "Buffer should move to a larger bucket after first resize"
        );

        let mid_bucket_in_use = pool.bucket_current_elements(mid_bucket_idx);
        assert_eq!(
            pool.bucket_current_elements(original_bucket_idx),
            original_bucket_in_use - 1,
            "Original bucket count should decrease after first resize"
        );

        let second_resize_size = BUCKET_SIZES[9]; // 1MiB
        buffer.put_slice(&vec![0u8; second_resize_size]);

        assert!(
            buffer.capacity() >= second_resize_size,
            "Buffer should increase capacity after second resize"
        );
        assert_eq!(
            pool.resize_events(),
            initial_resize_events + 2,
            "Two resize events should be recorded"
        );

        let final_bucket_idx = pool.best_fit(buffer.capacity()).unwrap();
        assert!(
            final_bucket_idx > mid_bucket_idx,
            "Buffer should move to an even larger bucket after second resize"
        );

        let final_bucket_in_use = pool.bucket_current_elements(final_bucket_idx);
        assert_eq!(
            pool.bucket_current_elements(mid_bucket_idx),
            mid_bucket_in_use - 1,
            "Mid bucket count should decrease after second resize"
        );

        drop(buffer);

        assert_eq!(
            pool.bucket_current_elements(original_bucket_idx),
            original_bucket_in_use - 1,
            "Original bucket should remain decreased"
        );

        assert_eq!(
            pool.bucket_current_elements(mid_bucket_idx),
            mid_bucket_in_use - 1,
            "Mid bucket should remain decreased"
        );

        assert_eq!(
            pool.bucket_current_elements(final_bucket_idx),
            final_bucket_in_use - 1,
            "Final bucket count should decrease after drop"
        );
    }

    #[test]
    #[serial(memory_pool)]
    fn test_different_resize_methods() {
        initialize_pool_for_tests();

        let pool = memory_pool();

        // Test put_slice
        {
            let initial_events = pool.resize_events();
            let mut buffer = PooledBuffer::with_capacity(4 * 1024);
            let orig_bucket_idx = pool.best_fit(buffer.capacity()).unwrap();
            let orig_in_use = pool.bucket_current_elements(orig_bucket_idx);

            buffer.put_slice(&vec![0u8; 64 * 1024]);

            assert_eq!(
                pool.resize_events(),
                initial_events + 1,
                "put_slice should trigger resize event"
            );
            assert_eq!(
                pool.bucket_current_elements(orig_bucket_idx),
                orig_in_use - 1,
                "put_slice should update bucket accounting"
            );
        }

        // Test put_bytes
        {
            let initial_events = pool.resize_events();
            let mut buffer = PooledBuffer::with_capacity(4 * 1024);
            let orig_bucket_idx = pool.best_fit(buffer.capacity()).unwrap();
            let orig_in_use = pool.bucket_current_elements(orig_bucket_idx);

            buffer.put_bytes(0, 64 * 1024); // 64KiB of zeros

            assert_eq!(
                pool.resize_events(),
                initial_events + 1,
                "put_bytes should trigger resize event"
            );
            assert_eq!(
                pool.bucket_current_elements(orig_bucket_idx),
                orig_in_use - 1,
                "put_bytes should update bucket accounting"
            );
        }

        // Test extend_from_slice
        {
            let initial_events = pool.resize_events();
            let mut buffer = PooledBuffer::with_capacity(4 * 1024);
            let orig_bucket_idx = pool.best_fit(buffer.capacity()).unwrap();
            let orig_in_use = pool.bucket_current_elements(orig_bucket_idx);

            buffer.extend_from_slice(&vec![0u8; 64 * 1024]);

            assert_eq!(
                pool.resize_events(),
                initial_events + 1,
                "extend_from_slice should trigger resize event"
            );
            assert_eq!(
                pool.bucket_current_elements(orig_bucket_idx),
                orig_in_use - 1,
                "extend_from_slice should update bucket accounting"
            );
        }

        // Test reserve
        {
            let initial_events = pool.resize_events();
            let mut buffer = PooledBuffer::with_capacity(4 * 1024);
            let orig_bucket_idx = pool.best_fit(buffer.capacity()).unwrap();
            let orig_in_use = pool.bucket_current_elements(orig_bucket_idx);

            buffer.reserve(64 * 1024);

            if buffer.capacity() > 4 * 1024 {
                assert_eq!(
                    pool.resize_events(),
                    initial_events + 1,
                    "reserve should trigger resize event when capacity changes"
                );
                assert_eq!(
                    pool.bucket_current_elements(orig_bucket_idx),
                    orig_in_use - 1,
                    "reserve should update bucket accounting when capacity changes"
                );
            }
        }
    }
}
