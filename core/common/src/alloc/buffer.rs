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

use crate::alloc::memory_pool::{ALIGNMENT, AlignedBuffer};

use super::memory_pool::{AlignedBufferExt, memory_pool};
use bytes::Bytes;
use compio::buf::{IoBuf, IoBufMut, SetLen};
use std::{
    mem::MaybeUninit,
    ops::{Deref, DerefMut},
};

/// A buffer wrapper that participates in memory pooling.
///
/// This buffer automatically acquires memory from the global memory pool
/// and returns it when dropped. It also tracks resize events to keep
/// pool accounting accurate.
#[derive(Debug)]
pub struct PooledBuffer {
    from_pool: bool,
    original_capacity: usize,
    original_bucket_idx: Option<usize>,
    inner: AlignedBuffer,
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
        let (buffer, was_pool_allocated) = memory_pool().acquire_buffer(capacity.max(ALIGNMENT));
        let original_capacity = buffer.capacity();
        let original_bucket_idx = if was_pool_allocated {
            memory_pool().best_fit(original_capacity)
        } else {
            None
        };

        debug_assert_eq!(
            buffer.as_ptr() as usize % ALIGNMENT,
            0,
            "PooledBuffer not aligned to {} bytes",
            ALIGNMENT
        );

        Self {
            from_pool: was_pool_allocated,
            original_capacity,
            original_bucket_idx,
            inner: buffer,
        }
    }

    /// Creates a new pooled buffer from an existing `AlignedBuffer`.
    ///
    /// # Arguments
    ///
    /// * `existing` - The existing `AlignedBuffer` buffer
    pub fn from_existing(existing: AlignedBuffer) -> Self {
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
            inner: AlignedBuffer::new(ALIGNMENT),
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

    /// Split the buffer at given position, returning a new PooledBuffer
    /// containing byte [0, at) and leaving [at, len)
    ///
    /// # Panic
    /// Panics if at > len
    pub fn split_to(&mut self, at: usize) -> PooledBuffer {
        assert!(
            at <= self.len(),
            "split_to out of bounds: at={}, len={}",
            at,
            self.len()
        );

        let mut new_buff = PooledBuffer::with_capacity(at);
        new_buff.inner.extend_from_slice(&self.inner[..at]);

        // SAFETY:
        // - `self.inner.as_ptr().add(at)` is valid for `new_len` because
        // `at + new_len === old_len <= cap`. Similar with `self.inner.as_mut_ptr()`
        //
        // -  source range is `[at, at + new_len)` and the destination is
        //   `[0, new_len)`.  These ranges do not overlap when `at > 0`.
        // - when `at == 0`, the operation is noop
        let new_len = self.len() - at;
        if new_len > 0 {
            unsafe {
                // move [at..] to [0..]
                std::ptr::copy(
                    self.inner.as_ptr().add(at),
                    self.inner.as_mut_ptr(),
                    new_len,
                );

                self.inner.set_len(new_len);
            }
        } else {
            self.inner.clear();
        }

        new_buff
    }

    pub fn put<T: AsRef<[u8]>>(&mut self, src: T) {
        self.extend_from_slice(src.as_ref());
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

        let start = self.inner.len();
        self.inner.resize(start + len, byte);

        if self.inner.capacity() != before_cap {
            self.check_for_resize();
        }
    }

    /// Wrapper for put_slice which might cause resize
    pub fn put_slice(&mut self, src: &[u8]) {
        self.extend_from_slice(src);
        // let before_cap = self.inner.capacity();
        //
        // if self.inner.capacity() != before_cap {
        //     self.check_for_resize();
        // }
    }

    /// Wrapper for put_u32_le which might cause resize
    pub fn put_u32_le(&mut self, value: u32) {
        let before_cap = self.inner.capacity();
        self.inner.extend_from_slice(&value.to_le_bytes());

        if self.inner.capacity() != before_cap {
            self.check_for_resize();
        }
    }

    /// Wrapper for put_u64_le which might cause resize
    pub fn put_u64_le(&mut self, value: u64) {
        let before_cap = self.inner.capacity();
        self.inner.extend_from_slice(&value.to_le_bytes());

        if self.inner.capacity() != before_cap {
            self.check_for_resize();
        }
    }

    /// Returns the capacity of the inner buffer
    pub fn capacity(&self) -> usize {
        self.inner.capacity()
    }

    /// Returns the length of the inner buffer
    pub fn len(&self) -> usize {
        self.inner.len()
    }

    /// Returns true if the buffer is empty
    pub fn is_empty(&self) -> bool {
        self.inner.is_empty()
    }

    /// Consumes the PooledBuffer and returns the inner AlignedBuffer.
    /// Note: This bypasses pool return logic, use with caution.
    pub fn into_inner(self) -> AlignedBuffer {
        let mut this = std::mem::ManuallyDrop::new(self);

        std::mem::replace(&mut this.inner, AlignedBuffer::new(ALIGNMENT))
    }

    /// Freezes the buffer, converting it to an immutable `Bytes`.
    ///
    /// After calling this method, the PooledBuffer becomes empty and will not
    /// return memory to the pool on drop (the frozen Bytes owns the allocation).
    /// The returned `Bytes` is Arc-backed, allowing cheap clones.
    pub fn freeze(&mut self) -> Bytes {
        let buf = std::mem::replace(&mut self.inner, AlignedBuffer::new(ALIGNMENT));

        // Update pool accounting
        if self.from_pool
            && let Some(bucket_idx) = self.original_bucket_idx
        {
            memory_pool().dec_bucket_in_use(bucket_idx);
        }
        self.from_pool = false;
        self.original_capacity = 0;
        self.original_bucket_idx = None;

        // Zero copy: Bytes takes ownership of the AlignedBuffer
        // and will drop it when refcount reaches zero
        Bytes::from_owner(buf)
    }
}

impl Deref for PooledBuffer {
    type Target = AlignedBuffer;

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
            let buf = std::mem::replace(&mut self.inner, AlignedBuffer::new(ALIGNMENT));
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

impl AsRef<[u8]> for PooledBuffer {
    fn as_ref(&self) -> &[u8] {
        &self.inner
    }
}

impl From<AlignedBuffer> for PooledBuffer {
    fn from(buffer: AlignedBuffer) -> Self {
        Self::from_existing(buffer)
    }
}

impl SetLen for PooledBuffer {
    unsafe fn set_len(&mut self, len: usize) {
        unsafe { self.inner.set_len(len) };
    }
}

impl IoBuf for PooledBuffer {
    fn as_init(&self) -> &[u8] {
        &self.inner[..]
    }
}

impl IoBufMut for PooledBuffer {
    fn as_uninit(&mut self) -> &mut [MaybeUninit<u8>] {
        let ptr = self.inner.as_mut_ptr().cast::<MaybeUninit<u8>>();
        let cap = self.inner.capacity();
        unsafe { std::slice::from_raw_parts_mut(ptr, cap) }
    }
}

#[cfg(test)]
mod miri_tests {
    //! Miri targets the 3 unsafe sites: `IoBufMut::as_uninit`
    //! (`from_raw_parts_mut` ptr cast), `SetLen::set_len`, `split_to`
    //! (`ptr::copy` forward-overlap + `set_len`).
    //!
    //! Pool-free helper avoids the global `MEMORY_POOL` (a `OnceCell` whose
    //! `ArrayQueue` buckets leak under Miri) and `serial_test` (pulls
    //! `sdd`/`scc` with int→ptr casts rejected by `-Zmiri-strict-provenance`).
    //! `split_to` tests need the pool internally; gated `#[cfg(not(miri))]`.

    use super::*;
    use aligned_vec::{AVec, ConstAlign};

    /// Build `PooledBuffer` with `from_pool == false` to skip the global pool.
    fn pool_free_with_capacity(cap: usize) -> PooledBuffer {
        let v: AVec<u8, ConstAlign<ALIGNMENT>> = AVec::with_capacity(ALIGNMENT, cap);
        PooledBuffer::from_existing(v)
    }

    // IoBufMut::as_uninit

    #[test]
    fn as_uninit_returns_slice_of_full_capacity() {
        let mut buf = pool_free_with_capacity(256);
        let cap_before = buf.capacity();
        let uninit = buf.as_uninit();
        assert_eq!(
            uninit.len(),
            cap_before,
            "as_uninit must expose the full capacity, not just initialized len",
        );
    }

    #[test]
    fn as_uninit_pointer_is_4096_aligned() {
        let mut buf = pool_free_with_capacity(8192);
        let addr = buf.as_uninit().as_mut_ptr() as usize;
        assert_eq!(addr % ALIGNMENT, 0);
    }

    #[test]
    fn as_uninit_write_then_set_len_observes_writes() {
        let mut buf = pool_free_with_capacity(128);
        {
            let uninit = buf.as_uninit();
            for (i, slot) in uninit.iter_mut().take(16).enumerate() {
                slot.write(u8::try_from(i).unwrap());
            }
        }
        // SAFETY: 16 bytes initialized above.
        unsafe { <PooledBuffer as SetLen>::set_len(&mut buf, 16) };
        assert_eq!(
            buf.as_init(),
            &[0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15]
        );
    }

    // SetLen::set_len

    #[test]
    fn set_len_to_full_capacity_after_uninit_fill() {
        // Fill entire capacity then `set_len(cap)`: Miri verifies every
        // byte initialized before read via `as_init()`.
        let mut buf = pool_free_with_capacity(256);
        let cap = buf.capacity();
        {
            let uninit = buf.as_uninit();
            assert_eq!(uninit.len(), cap);
            for (i, slot) in uninit.iter_mut().enumerate() {
                slot.write(u8::try_from(i & 0xff).unwrap());
            }
        }
        // SAFETY: 0..cap initialized above.
        unsafe { <PooledBuffer as SetLen>::set_len(&mut buf, cap) };
        assert_eq!(buf.len(), cap);
        assert_eq!(buf.as_init()[0], 0);
        assert_eq!(
            buf.as_init()[cap - 1],
            u8::try_from((cap - 1) & 0xff).unwrap()
        );
    }

    // split_to: `ptr::copy` (forward-overlap capable) + `set_len`.
    // Needs global pool; skip Miri (ArrayQueue retention reads as leak,
    // serial_test fails strict-provenance).

    #[cfg(not(miri))]
    mod split_to {
        use super::*;
        use crate::IggyByteSize;
        use crate::alloc::memory_pool::{MemoryPool, MemoryPoolConfigOther};
        use serial_test::serial;
        use std::str::FromStr;
        use std::sync::Once;

        static MIRI_POOL_INIT: Once = Once::new();

        fn init_pool_for_split_to_tests() {
            MIRI_POOL_INIT.call_once(|| {
                let config = MemoryPoolConfigOther {
                    enabled: true,
                    size: IggyByteSize::from_str("64MiB").unwrap(),
                    bucket_capacity: 16,
                };
                MemoryPool::init_pool(&config);
            });
        }

        #[test]
        #[serial(memory_pool)]
        fn basic_split() {
            init_pool_for_split_to_tests();
            let mut buf = pool_free_with_capacity(64);
            buf.extend_from_slice(b"abcdefghij");
            let prefix = buf.split_to(4);
            assert_eq!(prefix.as_ref(), b"abcd");
            assert_eq!(buf.as_ref(), b"efghij");
        }

        #[test]
        #[serial(memory_pool)]
        fn at_zero_yields_empty_prefix_and_unchanged_self() {
            init_pool_for_split_to_tests();
            let mut buf = pool_free_with_capacity(32);
            buf.extend_from_slice(b"abcd");
            let prefix = buf.split_to(0);
            assert!(prefix.is_empty());
            assert_eq!(buf.as_ref(), b"abcd");
        }

        #[test]
        #[serial(memory_pool)]
        fn at_len_yields_full_prefix_and_empty_self() {
            init_pool_for_split_to_tests();
            let mut buf = pool_free_with_capacity(32);
            buf.extend_from_slice(b"abcd");
            let prefix = buf.split_to(4);
            assert_eq!(prefix.as_ref(), b"abcd");
            assert!(buf.is_empty());
        }

        #[test]
        #[serial(memory_pool)]
        fn forward_overlap_preserves_bytes() {
            // at=2, len=8: source [2..8) overlaps destination [0..6).
            // `ptr::copy` handles overlap; `copy_nonoverlapping` would be UB.
            init_pool_for_split_to_tests();
            let mut buf = pool_free_with_capacity(32);
            buf.extend_from_slice(b"ABCDEFGH");
            let prefix = buf.split_to(2);
            assert_eq!(prefix.as_ref(), b"AB");
            assert_eq!(buf.as_ref(), b"CDEFGH");
        }
    }

    // Drop short-circuit for `from_pool == false`. Miri leak detector enforces.
    #[test]
    fn pool_free_buffer_drop_does_not_leak() {
        let mut buf = pool_free_with_capacity(256);
        buf.extend_from_slice(&[0xaa; 200]);
        drop(buf);
    }
}
