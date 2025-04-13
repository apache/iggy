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

use bytes::BytesMut;
use crossbeam::queue::ArrayQueue;
use iggy::prelude::IggyByteSize;
use once_cell::sync::Lazy;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use tracing::{debug, info, trace};

pub static BYTES_MUT_POOL: Lazy<BytesMutPool> = Lazy::new(BytesMutPool::default);

/// A pool for reusing BytesMut buffers
#[derive(Clone)]
pub struct BytesMutPool {
    // Buffers
    small_buffers: Arc<ArrayQueue<BytesMut>>,
    medium_buffers: Arc<ArrayQueue<BytesMut>>,
    large_buffers: Arc<ArrayQueue<BytesMut>>,
    extra_large_buffers: Arc<ArrayQueue<BytesMut>>,
    max_buffers: Arc<ArrayQueue<BytesMut>>,

    // Creation stats
    small_created: Arc<AtomicUsize>,
    medium_created: Arc<AtomicUsize>,
    large_created: Arc<AtomicUsize>,
    extra_large_created: Arc<AtomicUsize>,
    max_created: Arc<AtomicUsize>,

    // Return stats
    small_returned: Arc<AtomicUsize>,
    medium_returned: Arc<AtomicUsize>,
    large_returned: Arc<AtomicUsize>,
    extra_large_returned: Arc<AtomicUsize>,
    max_returned: Arc<AtomicUsize>,
}

impl BytesMutPool {
    // TODO(hubcio): make BytesMutPool bucket sizes configurable via server.toml
    const SMALL_BUFFER_SIZE: usize = 4 * 1024;
    const MEDIUM_BUFFER_SIZE: usize = 32 * 1024;
    const LARGE_BUFFER_SIZE: usize = 512 * 1024;
    const EXTRA_LARGE_BUFFER_SIZE: usize = 2 * 1024 * 1024;
    const MAX_BUFFER_SIZE: usize = 16 * 1024 * 1024;

    const SMALL_POOL_SIZE: usize = 16384;
    const MEDIUM_POOL_SIZE: usize = 4096;
    const LARGE_POOL_SIZE: usize = 1024;
    const EXTRA_LARGE_POOL_SIZE: usize = 256;
    const MAX_POOL_SIZE: usize = 128;

    /// Initialize the bytes pool
    pub fn init_pool() {
        Lazy::force(&BYTES_MUT_POOL);
    }

    /// Get a buffer with at least the specified capacity
    pub fn get_buffer(&self, capacity: usize) -> BytesMut {
        if capacity <= Self::SMALL_BUFFER_SIZE {
            if let Some(mut buffer) = self.small_buffers.pop() {
                buffer.clear();
                trace!("Reused small buffer with capacity: {}", buffer.capacity());
                return buffer;
            }
            self.small_created.fetch_add(1, Ordering::Relaxed);
            let buffer = BytesMut::with_capacity(Self::SMALL_BUFFER_SIZE);
            trace!(
                "Created new small buffer with capacity: {}",
                buffer.capacity()
            );
            buffer
        } else if capacity <= Self::MEDIUM_BUFFER_SIZE {
            if let Some(mut buffer) = self.medium_buffers.pop() {
                buffer.clear();
                trace!("Reused medium buffer with capacity: {}", buffer.capacity());
                return buffer;
            }
            self.medium_created.fetch_add(1, Ordering::Relaxed);
            let buffer = BytesMut::with_capacity(Self::MEDIUM_BUFFER_SIZE);
            trace!(
                "Created new medium buffer with capacity: {}",
                buffer.capacity()
            );
            buffer
        } else if capacity <= Self::LARGE_BUFFER_SIZE {
            if let Some(mut buffer) = self.large_buffers.pop() {
                buffer.clear();
                trace!("Reused large buffer with capacity: {}", buffer.capacity());
                return buffer;
            }
            self.large_created.fetch_add(1, Ordering::Relaxed);
            let buffer = BytesMut::with_capacity(Self::LARGE_BUFFER_SIZE);
            trace!(
                "Created new large buffer with capacity: {}",
                buffer.capacity()
            );
            buffer
        } else if capacity <= Self::EXTRA_LARGE_BUFFER_SIZE {
            if let Some(mut buffer) = self.extra_large_buffers.pop() {
                buffer.clear();
                trace!(
                    "Reused extra large buffer with capacity: {}",
                    buffer.capacity()
                );
                return buffer;
            }
            self.extra_large_created.fetch_add(1, Ordering::Relaxed);
            let buffer = BytesMut::with_capacity(Self::EXTRA_LARGE_BUFFER_SIZE);
            trace!(
                "Created new extra large buffer with capacity: {}",
                buffer.capacity()
            );
            buffer
        } else if capacity <= Self::MAX_BUFFER_SIZE {
            if let Some(mut buffer) = self.max_buffers.pop() {
                buffer.clear();
                trace!("Reused max buffer with capacity: {}", buffer.capacity());
                return buffer;
            }
            self.max_created.fetch_add(1, Ordering::Relaxed);
            let buffer = BytesMut::with_capacity(Self::MAX_BUFFER_SIZE);
            trace!(
                "Created new max buffer with capacity: {}",
                buffer.capacity()
            );
            buffer
        } else {
            // For very large buffers that exceed our max size, just allocate directly
            debug!("Created oversized buffer with capacity: {} B", capacity);
            BytesMut::with_capacity(capacity)
        }
    }

    /// Return a buffer to the pool
    fn return_buffer(&self, buffer: BytesMut) {
        let capacity = buffer.capacity();
        if capacity == Self::SMALL_BUFFER_SIZE {
            if self.small_buffers.push(buffer).is_err() {
                trace!("Small buffer pool full, dropping buffer");
            } else {
                self.small_returned.fetch_add(1, Ordering::Relaxed);
            }
        } else if capacity == Self::MEDIUM_BUFFER_SIZE {
            if self.medium_buffers.push(buffer).is_err() {
                trace!("Medium buffer pool full, dropping buffer");
            } else {
                self.medium_returned.fetch_add(1, Ordering::Relaxed);
            }
        } else if capacity == Self::LARGE_BUFFER_SIZE {
            if self.large_buffers.push(buffer).is_err() {
                trace!("Large buffer pool full, dropping buffer");
            } else {
                self.large_returned.fetch_add(1, Ordering::Relaxed);
            }
        } else if capacity == Self::EXTRA_LARGE_BUFFER_SIZE {
            if self.extra_large_buffers.push(buffer).is_err() {
                trace!("Extra large buffer pool full, dropping buffer");
            } else {
                self.extra_large_returned.fetch_add(1, Ordering::Relaxed);
            }
        } else if capacity == Self::MAX_BUFFER_SIZE {
            if self.max_buffers.push(buffer).is_err() {
                trace!("Max buffer pool full, dropping buffer");
            } else {
                self.max_returned.fetch_add(1, Ordering::Relaxed);
            }
        } else if capacity != 0 {
            trace!(
                "Returned buffer to pool with unknown capacity: {}",
                capacity
            );
        }
    }

    /// Log stats about buffer allocation and reuse
    pub fn log_stats(&self) {
        // How many times a buffer was created
        let sm_created = self.small_created.load(Ordering::Relaxed);
        let md_created = self.medium_created.load(Ordering::Relaxed);
        let lg_created = self.large_created.load(Ordering::Relaxed);
        let xl_created = self.extra_large_created.load(Ordering::Relaxed);
        let mx_created = self.max_created.load(Ordering::Relaxed);

        // Limit of each buffer pool
        let sm_limit = Self::SMALL_POOL_SIZE;
        let md_limit = Self::MEDIUM_POOL_SIZE;
        let lg_limit = Self::LARGE_POOL_SIZE;
        let xl_limit = Self::EXTRA_LARGE_POOL_SIZE;
        let mx_limit = Self::MAX_POOL_SIZE;

        // How many buffers are currently in use
        let sm_used = self.small_buffers.len();
        let md_used = self.medium_buffers.len();
        let lg_used = self.large_buffers.len();
        let xl_used = self.extra_large_buffers.len();
        let mx_used = self.max_buffers.len();

        // Utilization of each buffer pool
        let sm_util = sm_used as f64 / sm_limit as f64 * 100.0;
        let md_util = md_used as f64 / md_limit as f64 * 100.0;
        let lg_util = lg_used as f64 / lg_limit as f64 * 100.0;
        let xl_util = xl_used as f64 / xl_limit as f64 * 100.0;
        let mx_util = mx_used as f64 / mx_limit as f64 * 100.0;

        // Size of each buffer pool
        let sm_size = IggyByteSize::from((sm_used * Self::SMALL_BUFFER_SIZE) as u64);
        let md_size = IggyByteSize::from((md_used * Self::MEDIUM_BUFFER_SIZE) as u64);
        let lg_size = IggyByteSize::from((lg_used * Self::LARGE_BUFFER_SIZE) as u64);
        let xl_size = IggyByteSize::from((xl_used * Self::EXTRA_LARGE_BUFFER_SIZE) as u64);
        let mx_size = IggyByteSize::from((mx_used * Self::MAX_BUFFER_SIZE) as u64);

        // Total limit of the buffer pool
        let total_limit_size = IggyByteSize::from(
            (Self::SMALL_BUFFER_SIZE * Self::SMALL_POOL_SIZE
                + Self::MEDIUM_BUFFER_SIZE * Self::MEDIUM_POOL_SIZE
                + Self::LARGE_BUFFER_SIZE * Self::LARGE_POOL_SIZE
                + Self::EXTRA_LARGE_BUFFER_SIZE * Self::EXTRA_LARGE_POOL_SIZE
                + Self::MAX_BUFFER_SIZE * Self::MAX_POOL_SIZE) as u64,
        );

        // Total size of the buffer pool
        let current_total_size = IggyByteSize::from(
            (sm_created * Self::SMALL_BUFFER_SIZE
                + md_created * Self::MEDIUM_BUFFER_SIZE
                + lg_created * Self::LARGE_BUFFER_SIZE
                + xl_created * Self::EXTRA_LARGE_BUFFER_SIZE
                + mx_created * Self::MAX_BUFFER_SIZE) as u64,
        );

        // Utilization of the buffer pool
        let total_util = current_total_size.as_bytes_u64() as f64
            / total_limit_size.as_bytes_u64() as f64
            * 100.0;

        // How many times a buffer was returned to pool
        let sm_returned = self.small_returned.load(Ordering::Relaxed);
        let md_returned = self.medium_returned.load(Ordering::Relaxed);
        let lg_returned = self.large_returned.load(Ordering::Relaxed);
        let xl_returned = self.extra_large_returned.load(Ordering::Relaxed);
        let mx_returned = self.max_returned.load(Ordering::Relaxed);

        info!("BytesPool: {}/{}/{:.1}% (Current/Limit/Utilization), Small[{}/{}|{:.1}%|{}|{}] Medium[{}/{}|{:.1}%|{}|{}] Large[{}/{}|{:.1}%|{}|{}] XLarge[{}/{}|{:.1}%|{}|{}] Max[{}/{}|{:.1}%|{}|{}] [Created/Limit|Utilization|Current|Returns]",
           current_total_size, total_limit_size, total_util,
           sm_created, sm_limit, sm_util, sm_size, sm_returned,
           md_created, md_limit, md_util, md_size, md_returned,
           lg_created, lg_limit, lg_util, lg_size, lg_returned,
           xl_created, xl_limit, xl_util, xl_size, xl_returned,
           mx_created, mx_limit, mx_util, mx_size, mx_returned,
        );
    }
}

impl Default for BytesMutPool {
    fn default() -> Self {
        Self {
            small_buffers: Arc::new(ArrayQueue::new(Self::SMALL_POOL_SIZE)),
            medium_buffers: Arc::new(ArrayQueue::new(Self::MEDIUM_POOL_SIZE)),
            large_buffers: Arc::new(ArrayQueue::new(Self::LARGE_POOL_SIZE)),
            extra_large_buffers: Arc::new(ArrayQueue::new(Self::EXTRA_LARGE_POOL_SIZE)),
            max_buffers: Arc::new(ArrayQueue::new(Self::MAX_POOL_SIZE)),
            small_created: Arc::new(AtomicUsize::new(0)),
            medium_created: Arc::new(AtomicUsize::new(0)),
            large_created: Arc::new(AtomicUsize::new(0)),
            extra_large_created: Arc::new(AtomicUsize::new(0)),
            max_created: Arc::new(AtomicUsize::new(0)),
            small_returned: Arc::new(AtomicUsize::new(0)),
            medium_returned: Arc::new(AtomicUsize::new(0)),
            large_returned: Arc::new(AtomicUsize::new(0)),
            extra_large_returned: Arc::new(AtomicUsize::new(0)),
            max_returned: Arc::new(AtomicUsize::new(0)),
        }
    }
}

/// Extension trait for more ergonomic buffer return
pub trait BytesMutExt {
    fn return_to_pool(self);
}

impl BytesMutExt for BytesMut {
    fn return_to_pool(self) {
        BYTES_MUT_POOL.return_buffer(self);
    }
}
