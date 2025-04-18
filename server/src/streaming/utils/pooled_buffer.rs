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

use super::memory_pool::{memory_pool, BytesMutExt};
use bytes::{Buf, BufMut, BytesMut};
use std::ops::{Deref, DerefMut};

#[derive(Debug)]
pub struct PooledBuffer {
    from_pool: bool,
    original_capacity: usize,
    original_bucket_idx: Option<usize>,
    inner: BytesMut,
}

impl Default for PooledBuffer {
    /// Creates a default pooled buffer.
    fn default() -> Self {
        Self::empty()
    }
}

impl PooledBuffer {
    /// Creates a new pooled buffer with the specified minimum capacity.
    /// Acquires a buffer from the memory pool.
    pub fn with_capacity(capacity: usize) -> Self {
        let inner = memory_pool().acquire_buffer(capacity);
        let original_capacity = inner.capacity();
        let original_bucket_idx = memory_pool().best_fit(original_capacity);

        Self {
            from_pool: original_bucket_idx.is_some(),
            original_capacity,
            original_bucket_idx,
            inner,
        }
    }

    /// Creates a buffer wrapper around an existing BytesMut without using the memory pool.
    pub fn from_existing(existing: BytesMut) -> Self {
        Self {
            from_pool: false,
            original_capacity: existing.capacity(),
            original_bucket_idx: None,
            inner: existing,
        }
    }

    /// Creates an empty non-pooled buffer.
    pub fn empty() -> Self {
        Self {
            from_pool: false,
            original_capacity: 0,
            original_bucket_idx: None,
            inner: BytesMut::new(),
        }
    }

    /// Ensures the buffer has capacity for additional bytes without BytesMut reallocation.
    /// Copies data to a larger pooled buffer if needed.
    fn ensure_capacity(&mut self, additional: usize) {
        let need = self.len().saturating_add(additional);
        if need <= self.inner.capacity() {
            return;
        }

        let mut new_buf = memory_pool().acquire_buffer(need);
        new_buf.extend_from_slice(&self.inner);

        let old_cap = self.original_capacity;
        let old_buf = std::mem::replace(&mut self.inner, new_buf);

        if self.from_pool {
            old_buf.return_to_pool(old_cap);
        }

        self.from_pool =
            memory_pool().best_fit(self.inner.capacity()).is_some() && memory_pool().is_enabled;
        self.original_capacity = self.inner.capacity();
        self.original_bucket_idx = memory_pool().best_fit(self.original_capacity);

        memory_pool().inc_resize_events();
    }

    /// Reserves capacity for at least `additional` more bytes.
    /// Uses the memory pool for efficient buffer management.
    pub fn reserve(&mut self, additional: usize) {
        if additional > 0 {
            self.ensure_capacity(additional);
            self.inner.reserve(additional);
        }
    }

    /// Extends the buffer with the given slice.
    /// Ensures capacity before extending.
    pub fn extend_from_slice(&mut self, src: &[u8]) {
        self.ensure_capacity(src.len());
        self.inner.extend_from_slice(src);
    }

    /// Writes `len` copies of `byte` to the buffer.
    /// Ensures capacity before writing.
    pub fn put_bytes(&mut self, byte: u8, len: usize) {
        self.ensure_capacity(len);
        self.inner.put_bytes(byte, len);
    }

    /// Writes the contents of `src` to the buffer.
    /// Ensures capacity before writing.
    pub fn put_slice(&mut self, src: &[u8]) {
        self.ensure_capacity(src.len());
        self.inner.put_slice(src);
    }

    /// Writes a u32 in little endian format to the buffer.
    /// Ensures capacity before writing.
    pub fn put_u32_le(&mut self, value: u32) {
        self.ensure_capacity(std::mem::size_of::<u32>());
        self.inner.put_u32_le(value);
    }

    /// Writes a u64 in little endian format to the buffer.
    /// Ensures capacity before writing.
    pub fn put_u64_le(&mut self, value: u64) {
        self.ensure_capacity(std::mem::size_of::<u64>());
        self.inner.put_u64_le(value);
    }
}

impl Deref for PooledBuffer {
    type Target = BytesMut;

    /// Returns a reference to the inner BytesMut buffer.
    fn deref(&self) -> &Self::Target {
        &self.inner
    }
}

impl DerefMut for PooledBuffer {
    /// Returns a mutable reference to the inner BytesMut buffer.
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.inner
    }
}

impl Drop for PooledBuffer {
    /// Returns the buffer to the memory pool if it was acquired from there.
    fn drop(&mut self) {
        if self.from_pool {
            let buf = std::mem::take(&mut self.inner);
            buf.return_to_pool(self.original_capacity);
        }
    }
}

impl From<&[u8]> for PooledBuffer {
    /// Creates a pooled buffer from a byte slice.
    fn from(slice: &[u8]) -> Self {
        let mut buf = PooledBuffer::with_capacity(slice.len());
        buf.inner.extend_from_slice(slice);
        buf
    }
}

impl Buf for PooledBuffer {
    /// Returns the number of bytes between the cursor and the end of the buffer.
    fn remaining(&self) -> usize {
        self.inner.remaining()
    }

    /// Returns a slice of bytes starting at the current position.
    fn chunk(&self) -> &[u8] {
        self.inner.chunk()
    }

    /// Advances the cursor position by `cnt` bytes.
    fn advance(&mut self, cnt: usize) {
        self.inner.advance(cnt)
    }

    /// Fills `dst` with references to the bytes in this buffer.
    fn chunks_vectored<'t>(&'t self, dst: &mut [std::io::IoSlice<'t>]) -> usize {
        self.inner.chunks_vectored(dst)
    }
}
