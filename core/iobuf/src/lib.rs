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

use std::ops::{Deref, DerefMut, RangeBounds};
use std::ptr::NonNull;
use std::slice;
use std::sync::atomic::{AtomicUsize, Ordering, fence};

use aligned_vec::{AVec, ConstAlign};
use compio_buf::IoBuf;

const fn assert_even_alignment<const ALIGN: usize>() {
    assert!(
        ALIGN.is_multiple_of(2),
        "ALIGN must be divisible by 2 for control-block pointer tagging"
    );
}

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
    pub fn zeroed(len: usize) -> Self {
        const { assert_even_alignment::<ALIGN>() };
        let mut inner: AVec<u8, ConstAlign<ALIGN>> = AVec::new(ALIGN);
        inner.resize(len, 0);
        Self { inner }
    }

    pub fn copy_from_slice(data: &[u8]) -> Self {
        const { assert_even_alignment::<ALIGN>() };
        let mut inner: AVec<u8, ConstAlign<ALIGN>> = AVec::new(ALIGN);
        inner.extend_from_slice(data);
        Self { inner }
    }

    pub fn as_slice(&self) -> &[u8] {
        &self.inner
    }

    pub fn as_mut_slice(&mut self) -> &mut [u8] {
        &mut self.inner
    }

    /// Split `Owned` into a mutable copied-on-clone prefix and an immutable shared tail.
    ///
    /// # Panics
    /// Panics if `split_at > self.len()`.
    pub fn split_at(self, split_at: usize) -> (Prefix<ALIGN>, Frozen<ALIGN>) {
        let (prefix, tail) = self.split_extent_pair(split_at);
        (Prefix { inner: prefix }, Frozen { inner: tail })
    }

    fn split_extent_pair(self, split_at: usize) -> (Extent<ALIGN>, Extent<ALIGN>) {
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

        (
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
        )
    }
}

pub trait TryMerge: Sized {
    type Output;

    /// # Safety
    ///
    /// The caller must guarantee that the second part is the suffix of the
    /// original frame allocation, beginning exactly after the first part.
    unsafe fn try_merge(self) -> Result<Self::Output, Self>;
}

impl<const ALIGN: usize> TryMerge for (Prefix<ALIGN>, Frozen<ALIGN>) {
    type Output = Owned<ALIGN>;

    unsafe fn try_merge(self) -> Result<Self::Output, Self> {
        let (prefix, tail) = self;
        match unsafe { try_merge_extents(prefix.inner, tail.inner) } {
            Ok(owned) => Ok(owned),
            Err((prefix, tail)) => Err((Prefix { inner: prefix }, Frozen { inner: tail })),
        }
    }
}

pub struct Prefix<const ALIGN: usize> {
    inner: Extent<ALIGN>,
}

// SAFETY: `Prefix` owns the mutable front extent. Clone copies the bytes into a new
// allocation, so the mutable prefix is never shared across threads by aliasing.
unsafe impl<const ALIGN: usize> Send for Prefix<ALIGN> {}

impl<const ALIGN: usize> Prefix<ALIGN> {
    pub fn copy_from_slice(src: &[u8]) -> Self {
        Self {
            inner: Extent::copy_from_slice(src),
        }
    }

    pub fn len(&self) -> usize {
        self.inner.len
    }

    pub fn is_empty(&self) -> bool {
        self.inner.len == 0
    }

    fn ensure_mutable(&mut self) {
        if !is_copy_on_write_tagged(self.inner.ctrlb) {
            return;
        }

        let ctrlb = clear_copy_on_write_tag(self.inner.ctrlb);
        // Prefix reconstructed from Frozen may alias other overlapping views.
        // Detach unless the only remaining aliases are the prefix itself and its paired tail.
        let ref_count = unsafe { ctrlb.as_ref().ref_count.load(Ordering::Acquire) };
        if ref_count > 2 {
            self.inner = Extent::copy_from_slice(self.inner.as_slice());
            return;
        }

        self.inner.ctrlb = ctrlb;
    }
}

impl<const ALIGN: usize> Clone for Prefix<ALIGN> {
    fn clone(&self) -> Self {
        Self {
            inner: Extent::copy_from_slice(self.inner.as_slice()),
        }
    }
}

impl<const ALIGN: usize> std::fmt::Debug for Prefix<ALIGN> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Prefix")
            .field("len", &self.len())
            .field("copy_on_write", &is_copy_on_write_tagged(self.inner.ctrlb))
            .finish()
    }
}

impl<const ALIGN: usize> Deref for Prefix<ALIGN> {
    type Target = [u8];

    fn deref(&self) -> &Self::Target {
        self.inner.as_slice()
    }
}

impl<const ALIGN: usize> DerefMut for Prefix<ALIGN> {
    fn deref_mut(&mut self) -> &mut Self::Target {
        self.ensure_mutable();
        // SAFETY: Prefix either comes from `Owned::split_at` and is safe to mutate
        // in place, or it has been detached into its own allocation by `ensure_mutable`.
        unsafe { self.inner.as_mut_slice() }
    }
}

#[derive(Clone)]
pub struct Frozen<const ALIGN: usize> {
    inner: Extent<ALIGN>,
}

// SAFETY: `Frozen` provides immutable access only, and clones share the underlying
// extent immutably.
unsafe impl<const ALIGN: usize> Send for Frozen<ALIGN> {}

impl<const ALIGN: usize> From<Owned<ALIGN>> for Frozen<ALIGN> {
    fn from(value: Owned<ALIGN>) -> Self {
        const { assert_even_alignment::<ALIGN>() };
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

    /// Split `Frozen` into a copy-on-write prefix and immutable tail view.
    pub fn split_at(self, split_at: usize) -> (Prefix<ALIGN>, Frozen<ALIGN>) {
        const { assert_even_alignment::<ALIGN>() };
        assert!(split_at <= self.inner.len);

        let mut prefix = self.inner.clone();
        prefix.len = split_at;
        prefix.ctrlb = set_copy_on_write_tag(prefix.ctrlb);

        let mut tail = self.inner;
        // SAFETY: `split_at` is bounds-checked against the extent length.
        tail.ptr = unsafe { NonNull::new_unchecked(tail.ptr.as_ptr().add(split_at)) };
        tail.len -= split_at;

        (Prefix { inner: prefix }, Frozen { inner: tail })
    }

    pub fn slice(&self, range: impl RangeBounds<usize>) -> Self {
        use std::ops::Bound;

        let begin = match range.start_bound() {
            Bound::Included(&n) => n,
            Bound::Excluded(&n) => n + 1,
            Bound::Unbounded => 0,
        };

        let end = match range.end_bound() {
            Bound::Included(&n) => n.checked_add(1).expect("out of range"),
            Bound::Excluded(&n) => n,
            Bound::Unbounded => self.len(),
        };

        assert!(begin <= self.len());
        assert!(begin <= end);
        assert!(end <= self.len());

        let mut sliced = self.clone();
        // SAFETY: begin/end are bounds-checked against the current extent length.
        let ptr = unsafe { NonNull::new_unchecked(sliced.inner.ptr.as_ptr().add(begin)) };
        sliced.inner.ptr = ptr;
        sliced.inner.len = end - begin;
        sliced
    }

    pub fn len(&self) -> usize {
        self.inner.len
    }

    pub fn is_empty(&self) -> bool {
        self.inner.len == 0
    }
}

impl<const ALIGN: usize> std::fmt::Debug for Frozen<ALIGN> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Frozen").field("len", &self.len()).finish()
    }
}

impl<const ALIGN: usize> Deref for Frozen<ALIGN> {
    type Target = [u8];

    fn deref(&self) -> &Self::Target {
        self.as_slice()
    }
}

impl<const ALIGN: usize> IoBuf for Frozen<ALIGN> {
    fn as_init(&self) -> &[u8] {
        self.as_slice()
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
    // Padded to 32 bytes in order to avoid false sharing when used by split frame pairs.
    // If `Extent` would be smaller than 32 bytes, two `Extent`s that are adjacent in memory
    // could potentially share the same cache line + some extra
    // that extra could lead to false sharing, in case of invalidation of extra
    _pad: usize,
}

impl<const ALIGN: usize> Drop for Extent<ALIGN> {
    fn drop(&mut self) {
        // SAFETY: `self.ctrlb` points to a live control block while `self` is alive.
        unsafe { release_control_block_w_allocation::<ALIGN>(clear_copy_on_write_tag(self.ctrlb)) }
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
        const { assert_even_alignment::<ALIGN>() };
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
        let ctrlb = clear_copy_on_write_tag(self.ctrlb);
        // SAFETY: `self.ctrlb` points to a live control block while `self` is alive.
        unsafe {
            ctrlb.as_ref().ref_count.fetch_add(1, Ordering::Relaxed);
        }
        Self {
            ptr: self.ptr,
            len: self.len,
            ctrlb: self.ctrlb,
            _pad: 0,
        }
    }
}

const COPY_ON_WRITE_TAG: usize = 1;

fn is_copy_on_write_tagged(ctrlb: NonNull<ControlBlock>) -> bool {
    ctrlb.as_ptr().addr() & COPY_ON_WRITE_TAG != 0
}

fn set_copy_on_write_tag(ctrlb: NonNull<ControlBlock>) -> NonNull<ControlBlock> {
    // SAFETY: tagging only toggles the low alignment bit while preserving provenance.
    unsafe { NonNull::new_unchecked(ctrlb.as_ptr().map_addr(|addr| addr | COPY_ON_WRITE_TAG)) }
}

fn clear_copy_on_write_tag(ctrlb: NonNull<ControlBlock>) -> NonNull<ControlBlock> {
    // SAFETY: clearing only strips the low tag bit while preserving provenance.
    unsafe { NonNull::new_unchecked(ctrlb.as_ptr().map_addr(|addr| addr & !COPY_ON_WRITE_TAG)) }
}

unsafe fn release_control_block_w_allocation<const ALIGN: usize>(ctrlb: NonNull<ControlBlock>) {
    const { assert_even_alignment::<ALIGN>() };
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

/// # Safety
///
/// The caller must guarantee that `tail` is the suffix of the original frame
/// allocation and begins exactly after `prefix`.
unsafe fn try_merge_extents<const ALIGN: usize>(
    prefix: Extent<ALIGN>,
    tail: Extent<ALIGN>,
) -> Result<Owned<ALIGN>, (Extent<ALIGN>, Extent<ALIGN>)> {
    const { assert_even_alignment::<ALIGN>() };
    let split_at = prefix.len;
    // SAFETY: `tail.ctrlb` points to a live control block while `tail` is alive.
    let tail_ctrlb = clear_copy_on_write_tag(tail.ctrlb);
    let prefix_ctrlb = clear_copy_on_write_tag(prefix.ctrlb);
    let ctrlb_eq = std::ptr::addr_eq(prefix_ctrlb.as_ptr(), tail_ctrlb.as_ptr());
    let ref_count = unsafe { tail_ctrlb.as_ref().ref_count.load(Ordering::Acquire) };

    // When ctrlb_eq, both extents share the same control block with refcount 2.
    // When !ctrlb_eq, the tail must be unique for the merge to reclaim its allocation.
    let is_unique = if ctrlb_eq {
        ref_count == 2
    } else {
        ref_count == 1
    };
    if !is_unique {
        return Err((prefix, tail));
    }

    unsafe {
        if !ctrlb_eq {
            let dst_ctrlb = tail_ctrlb.as_ref();
            // We are patching up the original allocation, with the current prefix data,
            // so that the resulting `Owned` has correct content.
            let dst = slice::from_raw_parts_mut(dst_ctrlb.base.as_ptr(), split_at);
            dst.copy_from_slice(prefix.as_slice());
        }

        // Dropping the prefix in `ctrlb_eq` case should decrease the refcount to 1,
        // so it's safe to reuse the tail control block. In the case where the prefix
        // owns its own allocation, we guarantee that it's always unique.
        drop(prefix);
        // Prevent tail Drop from running, we're taking ownership of the control block.
        std::mem::forget(tail);

        let ctrlb = reclaim_unique_control_block(tail_ctrlb);
        // SAFETY: `ctrlb.base,capacity` were captured from an `AVec<u8>` allocation and
        // are now exclusively owned by this path.
        let inner = AVec::from_raw_parts(ctrlb.base.as_ptr(), ALIGN, ctrlb.len, ctrlb.capacity);
        Ok(Owned { inner })
    }
}

// TODO: Better tests & miri.
#[cfg(test)]
mod tests {
    use super::{Frozen, Owned, Prefix, TryMerge};
    use aligned_vec::AVec;
    use aligned_vec::ConstAlign;
    use std::mem;

    fn make_owned(data: &[u8]) -> Owned {
        let mut v: AVec<u8, ConstAlign<4096>> = AVec::new(4096);
        v.extend_from_slice(data);
        v.into()
    }

    #[test]
    fn split_exposes_prefix_and_tail() {
        let owned = make_owned(&[1, 2, 3, 4, 5]);
        let (mut prefix, tail) = owned.split_at(2);

        assert_eq!(&*prefix, &[1, 2]);
        assert_eq!(&*tail, &[3, 4, 5]);

        prefix.copy_from_slice(&[9, 8]);
        assert_eq!(&*prefix, &[9, 8]);
        assert_eq!(&*tail, &[3, 4, 5]);
    }

    #[test]
    fn clone_copies_prefix_and_shares_tail() {
        let owned = make_owned(&[1, 2, 3, 4, 5]);
        let (mut original_prefix, original_tail) = owned.split_at(2);
        let (mut cloned_prefix, cloned_tail) = (original_prefix.clone(), original_tail.clone());

        original_prefix.copy_from_slice(&[9, 9]);
        cloned_prefix.copy_from_slice(&[7, 7]);

        assert_eq!(&*original_prefix, &[9, 9]);
        assert_eq!(&*cloned_prefix, &[7, 7]);
        assert_eq!(&*original_tail, &[3, 4, 5]);
        assert_eq!(&*cloned_tail, &[3, 4, 5]);
    }

    #[test]
    fn try_merge_reuses_original_frame_when_unique() {
        let owned = make_owned(&[1, 2, 3, 4, 5]);
        let (mut prefix, tail) = owned.split_at(2);
        prefix.copy_from_slice(&[8, 9]);

        let merged: AVec<u8, ConstAlign<4096>> =
            unsafe { (prefix, tail).try_merge() }.unwrap().into();
        assert_eq!(merged.as_slice(), &[8, 9, 3, 4, 5]);
    }

    #[test]
    fn try_merge_fails_while_tail_is_shared() {
        let owned = make_owned(&[1, 2, 3, 4, 5]);
        let parts = owned.split_at(2);
        let clone = parts.clone();

        // Merge fails because tail is shared
        let parts = unsafe { parts.try_merge() }.unwrap_err();

        drop(clone);

        // Now merge succeeds
        let merged: AVec<u8, ConstAlign<4096>> = unsafe { parts.try_merge() }.unwrap().into();
        assert_eq!(merged.as_slice(), &[1, 2, 3, 4, 5]);
    }

    #[test]
    fn merge_after_cloned_prefix_mutation_writes_back_to_original_frame() {
        let owned = make_owned(&[1, 2, 3, 4, 5]);
        let parts = owned.split_at(2);
        let mut clone = parts.clone();

        drop(parts);

        clone.0.copy_from_slice(&[4, 2]);

        let merged: AVec<u8, ConstAlign<4096>> = unsafe { clone.try_merge() }.unwrap().into();
        assert_eq!(merged.as_slice(), &[4, 2, 3, 4, 5]);
    }

    #[test]
    fn zero_length_splits_work() {
        let owned = make_owned(&[1, 2, 3]);
        let left_empty = owned.split_at(0);
        assert_eq!(&*left_empty.0, &[]);
        assert_eq!(&*left_empty.1, &[1, 2, 3]);

        let owned = make_owned(&[1, 2, 3]);
        let right_empty = owned.split_at(3);
        assert_eq!(&*right_empty.0, &[1, 2, 3]);
        assert_eq!(&*right_empty.1, &[]);
    }

    #[test]
    fn clone_of_clone_shares_tail() {
        let owned = make_owned(&[1, 2, 3, 4, 5]);
        let original = owned.split_at(2);
        let clone1 = original.clone();
        let _clone2 = clone1.clone();

        // All clones share tail, so merge should fail.
        assert!(unsafe { original.try_merge() }.is_err());
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

        let (prefix, tail) = owned.split_at(2);
        assert_eq!(&*prefix, &[99, 2]);
        assert_eq!(&*tail, &[3, 4, 88]);
    }

    #[test]
    fn prefix_returns_correct_slice() {
        let owned = make_owned(&[10, 20, 30, 40, 50]);
        let (prefix, _) = owned.split_at(3);

        assert_eq!(&*prefix, &[10, 20, 30]);
    }

    #[test]
    fn tail_returns_correct_slice() {
        let owned = make_owned(&[10, 20, 30, 40, 50]);
        let (_, tail) = owned.split_at(3);

        assert_eq!(&*tail, &[40, 50]);
    }

    #[test]
    fn prefix_allows_modification() {
        let owned = make_owned(&[1, 2, 3, 4, 5]);
        let (mut prefix, tail) = owned.split_at(3);

        prefix[0] = 100;
        prefix[2] = 200;

        assert_eq!(&*prefix, &[100, 2, 200]);
        assert_eq!(&*tail, &[4, 5]);
    }

    #[test]
    fn prefix_full_overwrite() {
        let owned = make_owned(&[1, 2, 3, 4, 5]);
        let (mut prefix, tail) = owned.split_at(3);

        prefix.copy_from_slice(&[7, 8, 9]);

        assert_eq!(&*prefix, &[7, 8, 9]);
        assert_eq!(&*tail, &[4, 5]);
    }

    #[test]
    fn prefix_empty_slice() {
        let owned = make_owned(&[1, 2, 3]);
        let (prefix, tail) = owned.split_at(0);

        assert_eq!(&*prefix, &[]);
        assert_eq!(&*tail, &[1, 2, 3]);
    }

    #[test]
    fn tail_empty_slice() {
        let owned = make_owned(&[1, 2, 3]);
        let (prefix, tail) = owned.split_at(3);

        assert_eq!(&*prefix, &[1, 2, 3]);
        assert_eq!(&*tail, &[]);
    }

    #[test]
    fn prefix_mutation_does_not_affect_tail() {
        let owned = make_owned(&[1, 2, 3, 4, 5]);
        let (mut prefix, tail) = owned.split_at(2);

        let original_tail: Vec<u8> = tail.to_vec();
        prefix.copy_from_slice(&[99, 99]);

        assert_eq!(&*tail, original_tail.as_slice());
    }

    #[test]
    fn cloned_prefix_mutations_are_independent() {
        let owned = make_owned(&[1, 2, 3, 4, 5]);
        let mut original = owned.split_at(2);
        let mut cloned = original.clone();

        original.0.copy_from_slice(&[10, 20]);
        cloned.0.copy_from_slice(&[30, 40]);

        assert_eq!(&*original.0, &[10, 20]);
        assert_eq!(&*cloned.0, &[30, 40]);
        assert_eq!(&*original.1, &*cloned.1);
    }

    #[test]
    fn prefix_clone_copies_and_frozen_clone_shares() {
        let owned = make_owned(&[1, 2, 3, 4, 5]);
        let (mut prefix, tail) = owned.split_at(2);
        let mut prefix_clone = prefix.clone();
        let tail_clone = tail.clone();

        prefix.copy_from_slice(&[7, 7]);
        prefix_clone.copy_from_slice(&[8, 8]);

        assert_eq!(&*prefix, &[7, 7]);
        assert_eq!(&*prefix_clone, &[8, 8]);
        assert_eq!(&*tail, &[3, 4, 5]);
        assert_eq!(&*tail_clone, &[3, 4, 5]);
    }

    #[test]
    fn split_try_merge_reuses_original_frame_when_unique() {
        let owned = make_owned(&[1, 2, 3, 4, 5]);
        let (mut prefix, tail) = owned.split_at(2);
        prefix.copy_from_slice(&[8, 9]);

        let merged: AVec<u8, ConstAlign<4096>> =
            unsafe { (prefix, tail).try_merge() }.unwrap().into();
        assert_eq!(merged.as_slice(), &[8, 9, 3, 4, 5]);
    }

    #[test]
    fn split_try_merge_fails_while_tail_is_shared() {
        let owned = make_owned(&[1, 2, 3, 4, 5]);
        let parts = owned.split_at(2);
        let tail_clone = parts.1.clone();

        let parts = unsafe { parts.try_merge() }.unwrap_err();
        drop(tail_clone);

        let merged: AVec<u8, ConstAlign<4096>> = unsafe { parts.try_merge() }.unwrap().into();
        assert_eq!(merged.as_slice(), &[1, 2, 3, 4, 5]);
    }

    #[test]
    fn frozen_can_be_split_into_prefix_and_tail() {
        let frozen = Frozen::from(make_owned(&[1, 2, 3, 4, 5]));
        let (prefix, tail) = frozen.split_at(2);

        let merged: AVec<u8, ConstAlign<4096>> =
            unsafe { (prefix, tail).try_merge() }.unwrap().into();
        assert_eq!(merged.as_slice(), &[1, 2, 3, 4, 5]);
    }

    #[test]
    fn frozen_split_prefix_detaches_before_mutation() {
        let frozen = Frozen::from(make_owned(&[1, 2, 3, 4, 5]));
        let alias = frozen.clone();
        let (mut prefix, tail) = frozen.split_at(2);

        prefix.copy_from_slice(&[9, 8]);

        assert_eq!(&alias[..], &[1, 2, 3, 4, 5]);
        assert_eq!(&*prefix, &[9, 8]);
        assert_eq!(&*tail, &[3, 4, 5]);

        drop(alias);

        let merged: AVec<u8, ConstAlign<4096>> =
            unsafe { (prefix, tail).try_merge() }.unwrap().into();
        assert_eq!(merged.as_slice(), &[9, 8, 3, 4, 5]);
    }

    #[test]
    fn prefix_stays_32_bytes() {
        assert_eq!(mem::size_of::<Prefix<4096>>(), 32);
    }
}
