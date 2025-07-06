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

use super::memory_pool::{BytesMutExt, memory_pool};
use bytes::{Buf, BufMut, BytesMut};
use compio::buf::{IoBuf, IoBufMut, SetBufInit};
use std::ops::{Deref, DerefMut};

#[derive(Debug)]
pub struct PooledBuffer {
    from_pool: bool,
    original_capacity: usize,
    original_bucket_idx: Option<usize>,
    inner: BytesMut,
}

impl Default for PooledBuffer {
    fn default() -> Self {
        Self::empty()
    }
}

impl PooledBuffer {
    /// Creates a new pooled buffer with the specified capacity.
    ///
    /// # Arguments
    ///
    /// * `capacity` - The capacity of the buffer
    pub fn with_capacity(capacity: usize) -> Self {
        let (buffer, was_pool_allocated) = memory_pool().acquire_buffer(capacity);
        let original_capacity = buffer.capacity();
        let original_bucket_idx = if was_pool_allocated {
            memory_pool().best_fit(original_capacity)
        } else {
            None
        };
        Self {
            from_pool: was_pool_allocated,
            original_capacity,
            original_bucket_idx,
            inner: buffer,
        }
    }

    /// Creates a new pooled buffer from an existing `BytesMut`.
    ///
    /// # Arguments
    ///
    /// * `existing` - The existing `BytesMut` buffer
    pub fn from_existing(existing: BytesMut) -> Self {
        Self {
            from_pool: false,
            original_capacity: existing.capacity(),
            original_bucket_idx: None,
            inner: existing,
        }
    }

    /// Creates an empty pooled buffer.
    pub fn empty() -> Self {
        Self {
            from_pool: false,
            original_capacity: 0,
            original_bucket_idx: None,
            inner: BytesMut::new(),
        }
    }

    /// Checks if the buffer needs to be resized and updates the memory pool accordingly.
    /// This shall be called after operations that might cause a resize.
    pub fn check_for_resize(&mut self) {
        if !self.from_pool {
            return;
        }

        let current_capacity = self.inner.capacity();
        if current_capacity != self.original_capacity {
            memory_pool().inc_resize_events();

            if let Some(orig_idx) = self.original_bucket_idx {
                memory_pool().dec_bucket_in_use(orig_idx);

                if let Some(new_idx) = memory_pool().best_fit(current_capacity) {
                    // Track as a new allocation in the new bucket
                    memory_pool().inc_bucket_alloc(new_idx);
                    memory_pool().inc_bucket_in_use(new_idx);
                    self.original_bucket_idx = Some(new_idx);
                } else {
                    // Track as an external allocation if no bucket fits
                    memory_pool().inc_external_allocations();
                    self.original_bucket_idx = None;
                }
            }

            self.original_capacity = current_capacity;
        }
    }

    /// Wrapper for reserve which might cause resize
    pub fn reserve(&mut self, additional: usize) {
        let before_cap = self.inner.capacity();
        self.inner.reserve(additional);

        if self.inner.capacity() != before_cap {
            self.check_for_resize();
        }
    }

    /// Wrapper for extend_from_slice which might cause resize
    pub fn extend_from_slice(&mut self, extend_from: &[u8]) {
        let before_cap = self.inner.capacity();
        self.inner.extend_from_slice(extend_from);

        if self.inner.capacity() != before_cap {
            self.check_for_resize();
        }
    }

    /// Wrapper for put_bytes which might cause resize
    pub fn put_bytes(&mut self, byte: u8, len: usize) {
        let before_cap = self.inner.capacity();
        self.inner.put_bytes(byte, len);

        if self.inner.capacity() != before_cap {
            self.check_for_resize();
        }
    }

    /// Wrapper for put_slice which might cause resize
    pub fn put_slice(&mut self, src: &[u8]) {
        let before_cap = self.inner.capacity();
        self.inner.put_slice(src);

        if self.inner.capacity() != before_cap {
            self.check_for_resize();
        }
    }

    /// Wrapper for put_u32_le which might cause resize
    pub fn put_u32_le(&mut self, value: u32) {
        let before_cap = self.inner.capacity();
        self.inner.put_u32_le(value);

        if self.inner.capacity() != before_cap {
            self.check_for_resize();
        }
    }

    /// Wrapper for put_u64_le which might cause resize
    pub fn put_u64_le(&mut self, value: u64) {
        let before_cap = self.inner.capacity();
        self.inner.put_u64_le(value);

        if self.inner.capacity() != before_cap {
            self.check_for_resize();
        }
    }
}

impl Deref for PooledBuffer {
    type Target = BytesMut;

    fn deref(&self) -> &Self::Target {
        &self.inner
    }
}

impl DerefMut for PooledBuffer {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.inner
    }
}

impl Drop for PooledBuffer {
    fn drop(&mut self) {
        if self.from_pool {
            let buf = std::mem::take(&mut self.inner);
            buf.return_to_pool(self.original_capacity, true);
        }
    }
}

impl From<&[u8]> for PooledBuffer {
    fn from(slice: &[u8]) -> Self {
        let mut buf = PooledBuffer::with_capacity(slice.len());
        buf.inner.extend_from_slice(slice);
        buf
    }
}

impl Buf for PooledBuffer {
    fn remaining(&self) -> usize {
        self.inner.remaining()
    }

    fn chunk(&self) -> &[u8] {
        self.inner.chunk()
    }

    fn advance(&mut self, cnt: usize) {
        self.inner.advance(cnt)
    }

    fn chunks_vectored<'t>(&'t self, dst: &mut [std::io::IoSlice<'t>]) -> usize {
        self.inner.chunks_vectored(dst)
    }
}

impl SetBufInit for PooledBuffer {
    unsafe fn set_buf_init(&mut self, len: usize) {
        if self.inner.len() <= len {
            unsafe {
                self.inner.set_len(len);
            }
        }
    }
}

unsafe impl IoBufMut for PooledBuffer {
    fn as_buf_mut_ptr(&mut self) -> *mut u8 {
        self.inner.as_mut_ptr()
    }
}

unsafe impl IoBuf for PooledBuffer {
    fn as_buf_ptr(&self) -> *const u8 {
        self.inner.as_buf_ptr()
    }

    fn buf_len(&self) -> usize {
        self.inner.len()
    }

    fn buf_capacity(&self) -> usize {
        self.inner.capacity()
    }
}
