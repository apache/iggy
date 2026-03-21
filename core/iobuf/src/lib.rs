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

use std::mem::ManuallyDrop;
use std::ptr::NonNull;
use std::slice;
use std::sync::atomic::{AtomicUsize, Ordering, fence};

use aligned_vec::{AVec, ConstAlign};

#[derive(Debug)]
pub struct Owned<const ALIGN: usize = 4096> {
    inner: AVec<u8, ConstAlign<ALIGN>>,
}

impl<const ALIGN: usize> From<AVec<u8, ConstAlign<ALIGN>>> for Owned<ALIGN> {
    fn from(vec: AVec<u8, ConstAlign<ALIGN>>) -> Self {
        Self { inner: vec }
    }
}

impl<const ALIGN: usize> From<Owned<ALIGN>> for AVec<u8, ConstAlign<ALIGN>> {
    fn from(value: Owned<ALIGN>) -> Self {
        value.inner
    }
}

impl<const ALIGN: usize> Owned<ALIGN> {
    pub fn as_slice(&self) -> &[u8] {
        &self.inner
    }

    pub fn as_mut_slice(&mut self) -> &mut [u8] {
        &mut self.inner
    }

    /// Split `Owned` buffer into two halves
    ///
    /// # Panics
    /// Panics if `split_at > self.len()`.
    pub fn split_at(self, split_at: usize) -> TwoHalves<ALIGN> {
        assert!(split_at <= self.inner.len());

        // Take ownership of the AVec's allocation. After this, we are responsible
        // for deallocating via `AVec::from_raw_parts` or equivalent.
        let (ptr, _, len, capacity) = self.inner.into_raw_parts();

        // SAFETY: both pointers are constructed from the same `Inner` allocation, the split_at bounds are validated.
        // The control block captures original `Inner` metadata to allow reconstructing the original frame for merging/dropping.
        // The ptr provenance rules are maintained by the use of `NonNull` apis.
        let base: NonNull<u8> = unsafe { NonNull::new_unchecked(ptr) };
        let tail = unsafe { NonNull::new_unchecked(ptr.add(split_at)) };
        let ctrlb = ControlBlock::new(base, len, capacity);
        // We need to increment the ref_count as the resulting halves will both point to the same control block.
        unsafe { ctrlb.as_ref().ref_count.fetch_add(1, Ordering::Relaxed) };

        TwoHalves {
            inner: (
                Extent {
                    ptr: base,
                    len: split_at,
                    ctrlb,
                    _pad: 0,
                },
                Extent {
                    ptr: tail,
                    len: len - split_at,
                    ctrlb,
                    _pad: 0,
                },
            ),
        }
    }
}

pub struct TwoHalves<const ALIGN: usize> {
    inner: (Extent<ALIGN>, Extent<ALIGN>),
}

// SAFETY: `TwoHalves` can be sent across threads as long as the caller ensures that the head half is not shared between
// threads (e.g. by cloning, which creates a new head half),
// and that the tail half is only shared immutably (e.g. by cloning, which shares the tail half immutably).
unsafe impl<const ALIGN: usize> Send for TwoHalves<ALIGN> {}

impl<const ALIGN: usize> TwoHalves<ALIGN> {
    pub fn head(&self) -> &[u8] {
        self.inner.0.as_slice()
    }

    pub fn head_mut(&mut self) -> &mut [u8] {
        // SAFETY: We are accessing the head half mutably, this is the only correct operation, as the head is not shared between clones,
        // instead it gets copied.
        unsafe { self.inner.0.as_mut_slice() }
    }

    pub fn tail(&self) -> &[u8] {
        self.inner.1.as_slice()
    }

    pub fn split_point(&self) -> usize {
        self.inner.0.len
    }

    pub fn total_len(&self) -> usize {
        self.inner.0.len + self.inner.1.len
    }

    pub fn try_merge(self) -> Result<Owned<ALIGN>, Self> {
        let ctrlb_eq = std::ptr::addr_eq(self.inner.0.ctrlb.as_ptr(), self.inner.1.ctrlb.as_ptr());
        // SAFETY: `inner.1.ctrlb` points to a live control block while `self` is alive.
        let ref_count = unsafe {
            self.inner
                .1
                .ctrlb
                .as_ref()
                .ref_count
                .load(Ordering::Acquire)
        };
        // When ctrlb_eq, both extents share the same control block with refcount 2.
        // When !ctrlb_eq (after clone), the tail has its own refcount.
        let is_unique = if ctrlb_eq {
            ref_count == 2
        } else {
            ref_count == 1
        };

        if !is_unique {
            return Err(self);
        }

        // Transfer ownership to prevent Extent::drop from running.
        // SAFETY: We read the inner tuple out of ManuallyDrop, which won't run the compiler-generated drop.
        let this = ManuallyDrop::new(self);
        let (head, tail) = unsafe { std::ptr::read(&this.inner) };
        let split_at = head.len;

        // SAFETY: `tail.ctrlb` is unique at this point (is_unique checked above),
        // If `head.ctrlb != tail.ctrlb`, the head owns a standalone allocation
        // that must be released after copying.
        unsafe {
            if !ctrlb_eq {
                let dst_ctrlb = tail.ctrlb.as_ref();

                // We are patching up the original allocation, with the current head data, so that the resulting `Owned` has correct content.
                let dst = slice::from_raw_parts_mut(dst_ctrlb.base.as_ptr(), split_at);
                dst.copy_from_slice(head.as_slice());
            }

            // Dropping the head in `ctrlb_eq` case, should decrease the refcount to 1, so it's safe to reuse tail control block.
            // In case when head was it's own allocation, we guarantee that it's always unique.
            drop(head);
            let tail_ctrlb = tail.ctrlb;
            // Prevent tail Drop from running, we're taking ownership of the control block.
            std::mem::forget(tail);

            let ctrlb = reclaim_unique_control_block(tail_ctrlb);
            // SAFETY: `ctrlb.base,capacity` were captured from an `AVec<u8>` allocation and
            // are now exclusively owned by this path.
            let inner = AVec::from_raw_parts(ctrlb.base.as_ptr(), ALIGN, ctrlb.len, ctrlb.capacity);
            Ok(Owned { inner })
        }
    }
}

impl<const ALIGN: usize> Clone for TwoHalves<ALIGN> {
    fn clone(&self) -> Self {
        Self {
            inner: (
                Extent::<ALIGN>::copy_from_slice(self.head()),
                self.inner.1.clone(),
            ),
        }
    }
}

impl<const ALIGN: usize> std::fmt::Debug for TwoHalves<ALIGN> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("TwoHalves")
            .field("split_point", &self.split_point())
            .field("head_len", &self.inner.0.len)
            .field("tail_len", &self.inner.1.len)
            .field("halves_alias", &(self.inner.0.ctrlb == self.inner.1.ctrlb))
            .finish()
    }
}

#[derive(Clone)]
pub struct Frozen<const ALIGN: usize> {
    inner: Extent<ALIGN>,
}

impl<const ALIGN: usize> From<Owned<ALIGN>> for Frozen<ALIGN> {
    fn from(value: Owned<ALIGN>) -> Self {
        let inner = value.inner;
        let (ptr, _, len, capacity) = inner.into_raw_parts();

        // SAFETY: The `Owned` buffer is guaranteed to have a valid allocation, and we are taking ownership of it, so it's safe to construct the control block and extent.
        let base: NonNull<u8> = unsafe { NonNull::new_unchecked(ptr) };
        let ctrlb = ControlBlock::new(base, len, capacity);
        Self {
            inner: Extent {
                ptr: base,
                len,
                ctrlb,
                _pad: 0,
            },
        }
    }
}

impl<const ALIGN: usize> Frozen<ALIGN> {
    pub fn as_slice(&self) -> &[u8] {
        self.inner.as_slice()
    }
}

#[repr(C, align(64))]
struct ControlBlock {
    ref_count: AtomicUsize,
    base: NonNull<u8>,
    len: usize,
    capacity: usize,
}

impl ControlBlock {
    fn new(base: NonNull<u8>, len: usize, capacity: usize) -> NonNull<Self> {
        let ctrl = Box::new(ControlBlock {
            ref_count: AtomicUsize::new(1),
            base,
            len,
            capacity,
        });
        // SAFETY: Box::into_raw returns a valid pointer
        unsafe { NonNull::new_unchecked(Box::into_raw(ctrl)) }
    }
}

struct Extent<const ALIGN: usize> {
    ptr: NonNull<u8>,
    len: usize,
    ctrlb: NonNull<ControlBlock>,
    // Padded to 32 bytes in order to avoid false sharing when used by `TwoHalves`
    // If `Extent` would be smaller than 32 bytes, two `Extent`s that are adjacent in memory
    // could potentially share the same cache line + some extra
    // that extra could lead to false sharing, in case of invalidation of extra
    _pad: usize,
}

impl<const ALIGN: usize> Drop for Extent<ALIGN> {
    fn drop(&mut self) {
        // SAFETY: `self.ctrlb` points to a live control block while `self` is alive.
        unsafe { release_control_block_w_allocation::<ALIGN>(self.ctrlb) }
    }
}

impl<const ALIGN: usize> Extent<ALIGN> {
    fn as_slice(&self) -> &[u8] {
        // SAFETY: ptr and len describe a valid allocation
        unsafe { slice::from_raw_parts(self.ptr.as_ptr(), self.len) }
    }

    unsafe fn as_mut_slice(&mut self) -> &mut [u8] {
        // SAFETY: caller guarantees exclusive access
        unsafe { slice::from_raw_parts_mut(self.ptr.as_ptr(), self.len) }
    }

    fn copy_from_slice(src: &[u8]) -> Self {
        let mut v: AVec<u8, ConstAlign<ALIGN>> = AVec::new(ALIGN);
        v.extend_from_slice(src);

        let (ptr, _, len, capacity) = v.into_raw_parts();
        let data = unsafe { NonNull::new_unchecked(ptr) };

        let ctrlb = ControlBlock::new(data, len, capacity);

        Extent {
            ptr: data,
            len,
            ctrlb,
            _pad: 0,
        }
    }
}

impl<const ALIGN: usize> Clone for Extent<ALIGN> {
    fn clone(&self) -> Self {
        // SAFETY: `self.ctrlb` points to a live control block while `self` is alive.
        unsafe {
            self.ctrlb
                .as_ref()
                .ref_count
                .fetch_add(1, Ordering::Relaxed);
        }
        Self {
            ptr: self.ptr,
            len: self.len,
            ctrlb: self.ctrlb,
            _pad: 0,
        }
    }
}

unsafe fn release_control_block_w_allocation<const ALIGN: usize>(ctrlb: NonNull<ControlBlock>) {
    // SAFETY: ctrlb is valid per function preconditions
    let old = unsafe { ctrlb.as_ref() }
        .ref_count
        .fetch_sub(1, Ordering::Release);
    debug_assert!(old > 0, "control block refcount underflow");

    if old != 1 {
        return;
    }

    // This fence is needed to prevent reordering of use of the data and
    // deletion of the data. Because it is marked `Release`, the decreasing
    // of the reference count synchronizes with this `Acquire` fence. This
    // means that use of the data happens before decreasing the reference
    // count, which happens before this fence, which happens before the
    // deletion of the data.
    //
    // As explained in the [Boost documentation][1],
    //
    // > It is important to enforce any possible access to the object in one
    // > thread (through an existing reference) to *happen before* deleting
    // > the object in a different thread. This is achieved by a "release"
    // > operation after dropping a reference (any access to the object
    // > through this reference must obviously happened before), and an
    // > "acquire" operation before deleting the object.
    //
    // [1]: (www.boost.org/doc/libs/1_55_0/doc/html/atomic/usage_examples.html)
    //
    fence(Ordering::Acquire);

    // SAFETY: refcount is zero, we have exclusive ownership
    let ctrlb = unsafe { Box::from_raw(ctrlb.as_ptr()) };

    // SAFETY: `ctrlb.base`, `ctrlb.len` and `ctrlb.capacity` were captured from an `AVec`
    // allocation. We reconstruct the AVec and let it deallocate properly.
    let _ = unsafe {
        AVec::<u8, ConstAlign<ALIGN>>::from_raw_parts(
            ctrlb.base.as_ptr(),
            ALIGN,
            ctrlb.len,
            ctrlb.capacity,
        )
    };
}

unsafe fn reclaim_unique_control_block(ctrlb: NonNull<ControlBlock>) -> ControlBlock {
    assert_eq!(
        // SAFETY: caller guarantees `ctrlb` points to a live control block.
        unsafe { ctrlb.as_ref() }.ref_count.load(Ordering::Acquire),
        1
    );

    // SAFETY: caller guarantees uniqueness, so ownership of the control block can be reclaimed directly.
    unsafe { *Box::from_raw(ctrlb.as_ptr()) }
}

// TODO: Better tests & miri.
#[cfg(test)]
mod tests {
    use super::Owned;
    use aligned_vec::AVec;
    use aligned_vec::ConstAlign;

    fn make_owned(data: &[u8]) -> Owned {
        let mut v: AVec<u8, ConstAlign<4096>> = AVec::new(4096);
        v.extend_from_slice(data);
        v.into()
    }

    #[test]
    fn split_exposes_head_and_tail() {
        let owned = make_owned(&[1, 2, 3, 4, 5]);
        let mut buffer = owned.split_at(2);

        assert_eq!(buffer.head(), &[1, 2]);
        assert_eq!(buffer.tail(), &[3, 4, 5]);
        assert_eq!(buffer.split_point(), 2);
        assert_eq!(buffer.total_len(), 5);

        buffer.head_mut().copy_from_slice(&[9, 8]);
        assert_eq!(buffer.head(), &[9, 8]);
        assert_eq!(buffer.tail(), &[3, 4, 5]);
    }

    #[test]
    fn clone_copies_head_and_shares_tail() {
        let owned = make_owned(&[1, 2, 3, 4, 5]);
        let mut original = owned.split_at(2);
        let mut cloned = original.clone();

        original.head_mut().copy_from_slice(&[9, 9]);
        cloned.head_mut().copy_from_slice(&[7, 7]);

        assert_eq!(original.head(), &[9, 9]);
        assert_eq!(cloned.head(), &[7, 7]);
        assert_eq!(original.tail(), &[3, 4, 5]);
        assert_eq!(cloned.tail(), &[3, 4, 5]);
    }

    #[test]
    fn try_merge_reuses_original_frame_when_unique() {
        let owned = make_owned(&[1, 2, 3, 4, 5]);
        let mut buffer = owned.split_at(2);
        buffer.head_mut().copy_from_slice(&[8, 9]);

        let merged: AVec<u8, ConstAlign<4096>> = buffer.try_merge().unwrap().into();
        assert_eq!(merged.as_slice(), &[8, 9, 3, 4, 5]);
    }

    #[test]
    fn try_merge_fails_while_tail_is_shared() {
        let owned = make_owned(&[1, 2, 3, 4, 5]);
        let buffer = owned.split_at(2);
        let clone = buffer.clone();

        // Merge fails because tail is shared
        let buffer = buffer.try_merge().unwrap_err();

        drop(clone);

        // Now merge succeeds
        let merged: AVec<u8, ConstAlign<4096>> = buffer.try_merge().unwrap().into();
        assert_eq!(merged.as_slice(), &[1, 2, 3, 4, 5]);
    }

    #[test]
    fn merge_after_cloned_head_mutation_writes_back_to_original_frame() {
        let owned = make_owned(&[1, 2, 3, 4, 5]);
        let buffer = owned.split_at(2);
        let mut clone = buffer.clone();

        drop(buffer);

        clone.head_mut().copy_from_slice(&[4, 2]);

        let merged: AVec<u8, ConstAlign<4096>> = clone.try_merge().unwrap().into();
        assert_eq!(merged.as_slice(), &[4, 2, 3, 4, 5]);
    }

    #[test]
    fn zero_length_splits_work() {
        let owned = make_owned(&[1, 2, 3]);
        let left_empty = owned.split_at(0);
        assert_eq!(left_empty.head(), &[]);
        assert_eq!(left_empty.tail(), &[1, 2, 3]);

        let owned = make_owned(&[1, 2, 3]);
        let right_empty = owned.split_at(3);
        assert_eq!(right_empty.head(), &[1, 2, 3]);
        assert_eq!(right_empty.tail(), &[]);
    }

    #[test]
    fn clone_of_clone_shares_tail() {
        let owned = make_owned(&[1, 2, 3, 4, 5]);
        let original = owned.split_at(2);
        let clone1 = original.clone();
        let _clone2 = clone1.clone();

        // All clones share tail, so merge should fail
        assert!(original.try_merge().is_err());
    }

    #[test]
    fn owned_as_slice_returns_correct_data() {
        let owned = make_owned(&[10, 20, 30, 40, 50]);
        assert_eq!(owned.as_slice(), &[10, 20, 30, 40, 50]);
    }

    #[test]
    fn owned_as_slice_empty_buffer() {
        let owned = make_owned(&[]);
        assert_eq!(owned.as_slice(), &[]);
    }

    #[test]
    fn owned_as_mut_slice_allows_modification() {
        let mut owned = make_owned(&[1, 2, 3, 4, 5]);
        let slice = owned.as_mut_slice();
        slice[0] = 100;
        slice[4] = 200;

        assert_eq!(owned.as_slice(), &[100, 2, 3, 4, 200]);
    }

    #[test]
    fn owned_as_mut_slice_full_overwrite() {
        let mut owned = make_owned(&[1, 2, 3]);
        owned.as_mut_slice().copy_from_slice(&[7, 8, 9]);

        assert_eq!(owned.as_slice(), &[7, 8, 9]);
    }

    #[test]
    fn owned_modifications_persist_after_split() {
        let mut owned = make_owned(&[1, 2, 3, 4, 5]);
        owned.as_mut_slice()[0] = 99;
        owned.as_mut_slice()[4] = 88;

        let buffer = owned.split_at(2);
        assert_eq!(buffer.head(), &[99, 2]);
        assert_eq!(buffer.tail(), &[3, 4, 88]);
    }

    #[test]
    fn two_halves_head_returns_correct_slice() {
        let owned = make_owned(&[10, 20, 30, 40, 50]);
        let buffer = owned.split_at(3);

        assert_eq!(buffer.head(), &[10, 20, 30]);
    }

    #[test]
    fn two_halves_tail_returns_correct_slice() {
        let owned = make_owned(&[10, 20, 30, 40, 50]);
        let buffer = owned.split_at(3);

        assert_eq!(buffer.tail(), &[40, 50]);
    }

    #[test]
    fn two_halves_head_mut_allows_modification() {
        let owned = make_owned(&[1, 2, 3, 4, 5]);
        let mut buffer = owned.split_at(3);

        buffer.head_mut()[0] = 100;
        buffer.head_mut()[2] = 200;

        assert_eq!(buffer.head(), &[100, 2, 200]);
        assert_eq!(buffer.tail(), &[4, 5]);
    }

    #[test]
    fn two_halves_head_mut_full_overwrite() {
        let owned = make_owned(&[1, 2, 3, 4, 5]);
        let mut buffer = owned.split_at(3);

        buffer.head_mut().copy_from_slice(&[7, 8, 9]);

        assert_eq!(buffer.head(), &[7, 8, 9]);
        assert_eq!(buffer.tail(), &[4, 5]);
    }

    #[test]
    fn two_halves_head_empty_slice() {
        let owned = make_owned(&[1, 2, 3]);
        let buffer = owned.split_at(0);

        assert_eq!(buffer.head(), &[]);
        assert_eq!(buffer.tail(), &[1, 2, 3]);
    }

    #[test]
    fn two_halves_tail_empty_slice() {
        let owned = make_owned(&[1, 2, 3]);
        let buffer = owned.split_at(3);

        assert_eq!(buffer.head(), &[1, 2, 3]);
        assert_eq!(buffer.tail(), &[]);
    }

    #[test]
    fn two_halves_head_mut_does_not_affect_tail() {
        let owned = make_owned(&[1, 2, 3, 4, 5]);
        let mut buffer = owned.split_at(2);

        let original_tail: Vec<u8> = buffer.tail().to_vec();
        buffer.head_mut().copy_from_slice(&[99, 99]);

        assert_eq!(buffer.tail(), original_tail.as_slice());
    }

    #[test]
    fn two_halves_cloned_head_mut_independent() {
        let owned = make_owned(&[1, 2, 3, 4, 5]);
        let mut original = owned.split_at(2);
        let mut cloned = original.clone();

        original.head_mut().copy_from_slice(&[10, 20]);
        cloned.head_mut().copy_from_slice(&[30, 40]);

        assert_eq!(original.head(), &[10, 20]);
        assert_eq!(cloned.head(), &[30, 40]);
        // Tail is shared, both should see the same data
        assert_eq!(original.tail(), cloned.tail());
    }
}
