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

use aligned_vec::{AVec, ConstAlign};
use compio_buf::{IoBuf, IoBufMut, ReserveError, ReserveExactError, SetLen};
use std::mem::MaybeUninit;
use std::ops::{Deref, DerefMut, RangeBounds};
use std::ptr::NonNull;
use std::slice;
use std::sync::atomic::{AtomicUsize, Ordering, fence};

#[derive(Debug, Clone)]
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

pub struct Prefix<const ALIGN: usize> {
    inner: Extent<ALIGN>,
}

unsafe impl<const ALIGN: usize> Send for Prefix<ALIGN> {}

impl<const ALIGN: usize> Prefix<ALIGN> {
    pub fn from_aligned_vec(vec: AVec<u8, ConstAlign<ALIGN>>) -> Self {
        Self {
            inner: extent_from_aligned_vec(vec),
        }
    }

    pub fn as_slice(&self) -> &[u8] {
        self.inner.as_slice()
    }

    pub fn as_mut_slice(&mut self) -> &mut [u8] {
        unsafe { self.inner.as_mut_slice() }
    }

    pub fn freeze(self) -> Frozen<ALIGN> {
        freeze_extent(self.inner)
    }

    pub fn aliases(&self, tail: &Frozen<ALIGN>) -> bool {
        std::ptr::addr_eq(self.inner.ctrlb.as_ptr(), tail.inner.ctrlb.as_ptr())
    }

    pub fn can_coalesce_with(&self, tail: &Frozen<ALIGN>) -> bool {
        can_coalesce_prefix_with_tail(&self.inner, tail)
    }

    pub fn try_coalesce_with(
        self,
        tail: Frozen<ALIGN>,
    ) -> Result<Frozen<ALIGN>, (Prefix<ALIGN>, Frozen<ALIGN>)> {
        match try_coalesce_prefix_with_tail(self.inner, tail) {
            Ok(merged) => Ok(merged),
            Err((prefix, tail)) => Err((Prefix { inner: prefix }, tail)),
        }
    }
}

impl<const ALIGN: usize> Owned<ALIGN> {
    pub fn zeroed(len: usize) -> Self {
        let mut inner: AVec<u8, ConstAlign<ALIGN>> = AVec::new(ALIGN);
        inner.resize(len, 0);
        Self { inner }
    }

    /// Allocate a buffer with `capacity` reserved bytes and `len == 0`.
    /// Intended for use with compio's [`IoBufMut`] read path: the caller
    /// hands the `Owned` to `read_exact`, which fills it in-place and
    /// advances the length via [`SetLen::set_len`].
    pub fn with_capacity(capacity: usize) -> Self {
        Self {
            inner: AVec::with_capacity(ALIGN, capacity),
        }
    }

    pub fn copy_from_slice(data: &[u8]) -> Self {
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

    pub fn split_at(self, split_at: usize) -> (Prefix<ALIGN>, Frozen<ALIGN>) {
        assert!(split_at <= self.inner.len());

        let (ptr, _, len, capacity) = self.inner.into_raw_parts();
        let guard = AVecRawGuard::<ALIGN>::new(ptr, len, capacity);
        let base: NonNull<u8> = unsafe { NonNull::new_unchecked(ptr) };
        let tail = unsafe { NonNull::new_unchecked(ptr.add(split_at)) };
        let ctrlb = ControlBlock::new(base, len, capacity);
        // ControlBlock::new succeeded; the buffer is now owned by the
        // control block. Skip the guard's destructor.
        guard.defuse();
        unsafe { ctrlb.as_ref().ref_count.fetch_add(1, Ordering::Relaxed) };

        let prefix = Prefix {
            inner: Extent {
                ptr: base,
                len: split_at,
                ctrlb,
            },
        };
        let tail = Frozen {
            inner: Extent {
                ptr: tail,
                len: len - split_at,
                ctrlb,
            },
        };

        (prefix, tail)
    }
}

// SAFETY contract for the compio IO buf impls below:
// * `as_init` returns the initialized prefix (len bytes).
// * `as_uninit` returns the full allocation up to capacity; compio writes
//   bytes into it during reads and THEN calls `SetLen::set_len` to advance
//   the initialized region, which is exactly what `AVec::set_len` expects.
// * `buf_capacity` is the AVec capacity, not the initialized length.
impl<const ALIGN: usize> IoBuf for Owned<ALIGN> {
    fn as_init(&self) -> &[u8] {
        self.inner.as_slice()
    }
}

impl<const ALIGN: usize> IoBufMut for Owned<ALIGN> {
    fn as_uninit(&mut self) -> &mut [MaybeUninit<u8>] {
        let cap = self.inner.capacity();
        let ptr = self.inner.as_mut_ptr().cast::<MaybeUninit<u8>>();
        unsafe { slice::from_raw_parts_mut(ptr, cap) }
    }

    fn buf_capacity(&mut self) -> usize {
        self.inner.capacity()
    }

    fn reserve(&mut self, len: usize) -> Result<(), ReserveError> {
        // `reserve(additional)` contract: capacity >= current_len + additional
        // after the call. `AVec::reserve` matches this contract exactly.
        if self.inner.capacity() - self.inner.len() >= len {
            return Ok(());
        }
        self.inner.reserve(len);
        Ok(())
    }

    fn reserve_exact(&mut self, len: usize) -> Result<(), ReserveExactError> {
        if self.inner.capacity() - self.inner.len() >= len {
            return Ok(());
        }
        self.inner.reserve_exact(len);
        Ok(())
    }
}

impl<const ALIGN: usize> SetLen for Owned<ALIGN> {
    unsafe fn set_len(&mut self, len: usize) {
        unsafe { self.inner.set_len(len) };
    }
}

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
        f.debug_struct("Prefix").field("len", &self.len()).finish()
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
        self.as_mut_slice()
    }
}

#[derive(Clone)]
pub struct Frozen<const ALIGN: usize> {
    inner: Extent<ALIGN>,
}

unsafe impl<const ALIGN: usize> Send for Frozen<ALIGN> {}

impl<const ALIGN: usize> From<Extent<ALIGN>> for Frozen<ALIGN> {
    fn from(inner: Extent<ALIGN>) -> Self {
        Self { inner }
    }
}

impl<const ALIGN: usize> From<Owned<ALIGN>> for Frozen<ALIGN> {
    fn from(value: Owned<ALIGN>) -> Self {
        let inner = value.inner;
        let (ptr, _, len, capacity) = inner.into_raw_parts();
        let guard = AVecRawGuard::<ALIGN>::new(ptr, len, capacity);

        let base: NonNull<u8> = unsafe { NonNull::new_unchecked(ptr) };
        let ctrlb = ControlBlock::new(base, len, capacity);
        guard.defuse();
        Self {
            inner: Extent {
                ptr: base,
                len,
                ctrlb,
            },
        }
    }
}

impl<const ALIGN: usize> Frozen<ALIGN> {
    pub fn as_slice(&self) -> &[u8] {
        self.inner.as_slice()
    }

    pub fn split_at(self, split_at: usize) -> (Prefix<ALIGN>, Frozen<ALIGN>) {
        assert!(split_at <= self.inner.len);

        let prefix = Prefix::copy_from_slice(&self.inner.as_slice()[..split_at]);

        let mut tail = self.inner;
        tail.ptr = unsafe { NonNull::new_unchecked(tail.ptr.as_ptr().add(split_at)) };
        tail.len -= split_at;

        (prefix, Frozen { inner: tail })
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

impl<const ALIGN: usize> AsRef<[u8]> for Frozen<ALIGN> {
    fn as_ref(&self) -> &[u8] {
        self.as_slice()
    }
}

impl<const ALIGN: usize> IoBuf for Frozen<ALIGN> {
    fn as_init(&self) -> &[u8] {
        self.as_slice()
    }
}

#[repr(C, align(64))]
pub(crate) struct ControlBlock {
    pub(crate) ref_count: AtomicUsize,
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
        unsafe { NonNull::new_unchecked(Box::into_raw(ctrl)) }
    }
}

/// Drop-on-unwind guard for raw `AVec` parts. Plugs the leak window between
/// `AVec::into_raw_parts` (surrenders the buffer) and `ControlBlock::new`
/// (calls `Box::new`, can panic via `handle_alloc_error`). On unwind, `Drop`
/// reconstitutes the `AVec`. Success paths must call [`AVecRawGuard::defuse`]
/// before constructing the `Extent` (ownership transferred, no double-free).
struct AVecRawGuard<const ALIGN: usize> {
    ptr: *mut u8,
    len: usize,
    capacity: usize,
}

impl<const ALIGN: usize> AVecRawGuard<ALIGN> {
    fn new(ptr: *mut u8, len: usize, capacity: usize) -> Self {
        Self { ptr, len, capacity }
    }

    /// Ownership transferred to new `Extent`; skip destructor.
    fn defuse(self) {
        std::mem::forget(self);
    }
}

impl<const ALIGN: usize> Drop for AVecRawGuard<ALIGN> {
    fn drop(&mut self) {
        // SAFETY: `ptr/len/capacity` come from `AVec::into_raw_parts` of an
        // `AVec<u8, ConstAlign<ALIGN>>` whose ownership the caller has just
        // surrendered. While the guard is alive no other handle to the
        // buffer exists, so reconstituting and dropping the `AVec` is the
        // unique deallocation path.
        unsafe {
            drop(AVec::<u8, ConstAlign<ALIGN>>::from_raw_parts(
                self.ptr,
                ALIGN,
                self.len,
                self.capacity,
            ));
        }
    }
}

struct Extent<const ALIGN: usize> {
    pub(crate) ptr: NonNull<u8>,
    pub(crate) len: usize,
    pub(crate) ctrlb: NonNull<ControlBlock>,
}

impl<const ALIGN: usize> Drop for Extent<ALIGN> {
    fn drop(&mut self) {
        unsafe { release_control_block_w_allocation::<ALIGN>(self.ctrlb) }
    }
}

impl<const ALIGN: usize> Extent<ALIGN> {
    pub(crate) fn as_slice(&self) -> &[u8] {
        unsafe { slice::from_raw_parts(self.ptr.as_ptr(), self.len) }
    }

    pub(crate) unsafe fn as_mut_slice(&mut self) -> &mut [u8] {
        unsafe { slice::from_raw_parts_mut(self.ptr.as_ptr(), self.len) }
    }

    pub(crate) fn copy_from_slice(src: &[u8]) -> Self {
        let mut v: AVec<u8, ConstAlign<ALIGN>> = AVec::new(ALIGN);
        v.extend_from_slice(src);

        let (ptr, _, len, capacity) = v.into_raw_parts();
        let guard = AVecRawGuard::<ALIGN>::new(ptr, len, capacity);
        let data = unsafe { NonNull::new_unchecked(ptr) };

        let ctrlb = ControlBlock::new(data, len, capacity);
        guard.defuse();

        Extent {
            ptr: data,
            len,
            ctrlb,
        }
    }
}

impl<const ALIGN: usize> Clone for Extent<ALIGN> {
    fn clone(&self) -> Self {
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
        }
    }
}

pub(crate) fn freeze_extent<const ALIGN: usize>(extent: Extent<ALIGN>) -> Frozen<ALIGN> {
    Frozen { inner: extent }
}

pub(crate) fn extent_from_aligned_vec<const ALIGN: usize>(
    vec: AVec<u8, ConstAlign<ALIGN>>,
) -> Extent<ALIGN> {
    let (ptr, _, len, capacity) = vec.into_raw_parts();
    let guard = AVecRawGuard::<ALIGN>::new(ptr, len, capacity);
    let data = unsafe { NonNull::new_unchecked(ptr) };
    let ctrlb = ControlBlock::new(data, len, capacity);
    guard.defuse();

    Extent {
        ptr: data,
        len,
        ctrlb,
    }
}

pub(crate) fn extent_offset_from_base<const ALIGN: usize>(extent: &Extent<ALIGN>) -> usize {
    // SAFETY: the extent points into the control block allocation while it is alive.
    let offset = unsafe {
        extent
            .ptr
            .as_ptr()
            .offset_from(extent.ctrlb.as_ref().base.as_ptr())
    };
    usize::try_from(offset).expect("extent pointer must not point before control block base")
}

fn extent_ref_count<const ALIGN: usize>(extent: &Extent<ALIGN>) -> usize {
    unsafe { extent.ctrlb.as_ref().ref_count.load(Ordering::Acquire) }
}

/// Decrement refcount; on last reference, drop the box and free the `AVec`.
///
/// # Safety
///
/// * `ctrlb`: live `ControlBlock`; caller owns one refcount share.
/// * Caller must not use `ctrlb` after this call.
/// * `ALIGN` must match the original `AVec<u8, ConstAlign<ALIGN>>` allocation.
unsafe fn release_control_block_w_allocation<const ALIGN: usize>(ctrlb: NonNull<ControlBlock>) {
    let old = unsafe { ctrlb.as_ref() }
        .ref_count
        .fetch_sub(1, Ordering::Release);
    // Underflow = use-after-free. Assert in release: wrap → delayed double-free.
    assert!(old > 0, "control block refcount underflow");

    if old != 1 {
        return;
    }

    fence(Ordering::Acquire);

    let ctrlb = unsafe { Box::from_raw(ctrlb.as_ptr()) };

    let _ = unsafe {
        AVec::<u8, ConstAlign<ALIGN>>::from_raw_parts(
            ctrlb.base.as_ptr(),
            ALIGN,
            ctrlb.len,
            ctrlb.capacity,
        )
    };
}

/// Take ownership of a refcount==1 control block (consumes the heap box).
/// Used by the merge path to recycle `(base, len, capacity)`.
///
/// # Safety
///
/// * `ctrlb`: live `ControlBlock` with refcount exactly 1 (asserted). Caller
///   must guarantee no concurrent `clone` can race the load.
/// * Caller must not use `ctrlb` after this call.
unsafe fn reclaim_unique_control_block(ctrlb: NonNull<ControlBlock>) -> ControlBlock {
    assert_eq!(
        unsafe { ctrlb.as_ref() }.ref_count.load(Ordering::Acquire),
        1
    );
    unsafe { *Box::from_raw(ctrlb.as_ptr()) }
}

/// Merge two extents into a single `Owned`. Succeeds when tail's ctrlb is the
/// only reference (refcount == 2 shared-ctrlb, == 1 separate-ctrlb).
///
/// # Safety
///
/// * `prefix` + `tail` passed by value (consumed); no external clone may race.
/// * Compatible allocations:
///   - Shared ctrlb (from `Owned::split_at`): `prefix.ptr == tail.ctrlb.base`,
///     `prefix.len + tail.len <= tail.ctrlb.len`.
///   - Separate ctrlb: `prefix.len == extent_offset_from_base(tail)`, prefix
///     fits within tail's allocation.
/// * `ALIGN` matches both backing `AVec`s.
unsafe fn try_merge_extents<const ALIGN: usize>(
    prefix: Extent<ALIGN>,
    tail: Extent<ALIGN>,
) -> Result<Owned<ALIGN>, (Extent<ALIGN>, Extent<ALIGN>)> {
    let split_at = prefix.len;
    let tail_ctrlb = tail.ctrlb;
    let prefix_ctrlb = prefix.ctrlb;
    let ctrlb_eq = std::ptr::addr_eq(prefix_ctrlb.as_ptr(), tail_ctrlb.as_ptr());
    let ref_count = unsafe { tail_ctrlb.as_ref().ref_count.load(Ordering::Acquire) };

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
            let dst = slice::from_raw_parts_mut(dst_ctrlb.base.as_ptr(), split_at);
            dst.copy_from_slice(prefix.as_slice());
        }

        drop(prefix);
        std::mem::forget(tail);

        let ctrlb = reclaim_unique_control_block(tail_ctrlb);
        let inner = AVec::from_raw_parts(ctrlb.base.as_ptr(), ALIGN, ctrlb.len, ctrlb.capacity);
        Ok(Owned { inner })
    }
}

pub(crate) fn try_coalesce_prefix_with_tail<const ALIGN: usize>(
    prefix: Extent<ALIGN>,
    tail: Frozen<ALIGN>,
) -> Result<Frozen<ALIGN>, (Extent<ALIGN>, Frozen<ALIGN>)> {
    if !can_coalesce_prefix_with_tail(&prefix, &tail) {
        return Err((prefix, tail));
    }

    match unsafe { try_merge_extents(prefix, tail.inner) } {
        Ok(owned) => Ok(Frozen::from(owned)),
        Err((prefix, tail)) => Err((prefix, Frozen { inner: tail })),
    }
}

fn can_coalesce_prefix_with_tail<const ALIGN: usize>(
    prefix: &Extent<ALIGN>,
    tail: &Frozen<ALIGN>,
) -> bool {
    let split_at = prefix.len;
    let tail_ctrlb = tail.inner.ctrlb;
    let prefix_ctrlb = prefix.ctrlb;
    let ctrlb_eq = std::ptr::addr_eq(prefix_ctrlb.as_ptr(), tail_ctrlb.as_ptr());
    let tail_ref_count = extent_ref_count(&tail.inner);

    if ctrlb_eq {
        return tail_ref_count == 2;
    }

    tail_ref_count == 1 && extent_offset_from_base(&tail.inner) == split_at
}

#[cfg(test)]
mod tests {
    use super::*;

    // Smaller alignment keeps the per-test Miri allocation footprint low while
    // exercising the same control-block + atomic-refcount + pointer-arithmetic
    // logic as the production `MESSAGE_ALIGN = 4096`.
    const A: usize = 64;

    fn owned(data: &[u8]) -> Owned<A> {
        Owned::<A>::copy_from_slice(data)
    }

    #[test]
    fn prefix_from_aligned_vec_roundtrip() {
        let mut v: AVec<u8, ConstAlign<A>> = AVec::new(A);
        v.extend_from_slice(b"abc");
        let prefix = Prefix::<A>::from_aligned_vec(v);
        assert_eq!(prefix.as_slice(), b"abc");
    }

    //  Owned::split_at: shared control block / disjoint views

    #[test]
    fn owned_split_at_shares_ctrlb() {
        let o = owned(b"abcdefgh");
        let (prefix, tail) = o.split_at(4);
        assert!(
            prefix.aliases(&tail),
            "Owned::split_at must produce halves sharing one control block",
        );
    }

    #[test]
    fn owned_split_drop_prefix_first_keeps_tail_valid() {
        let o = owned(b"hello world");
        let (prefix, tail) = o.split_at(5);
        drop(prefix);
        // refcount went 2 -> 1; tail's ctrlb / allocation must still be live.
        assert_eq!(tail.as_slice(), b" world");
    }

    #[test]
    fn owned_split_drop_tail_first_keeps_prefix_mutable() {
        let o = owned(b"hello world");
        let (mut prefix, tail) = o.split_at(5);
        drop(tail);
        assert_eq!(prefix.as_slice(), b"hello");
        prefix.as_mut_slice()[0] = b'H';
        assert_eq!(prefix.as_slice(), b"Hello");
    }

    #[test]
    fn owned_split_disjoint_mut_does_not_corrupt_alias() {
        // Mutate the prefix in-place while a frozen tail still aliases the
        // same allocation. Tree-borrows must accept disjoint child accesses.
        let o = owned(b"abcdefgh");
        let (mut prefix, tail) = o.split_at(4);
        prefix.as_mut_slice().copy_from_slice(b"WXYZ");
        assert_eq!(prefix.as_slice(), b"WXYZ");
        assert_eq!(tail.as_slice(), b"efgh");
    }

    //  Frozen::clone: refcount round-trip

    #[test]
    fn frozen_clone_independent_drop() {
        let f1: Frozen<A> = owned(b"hello").into();
        let f2 = f1.clone();
        let f3 = f1.clone();
        drop(f1);
        assert_eq!(f2.as_slice(), b"hello");
        drop(f2);
        assert_eq!(f3.as_slice(), b"hello");
    }

    //  Frozen::slice: aliased view, parent-independent lifetime

    fn fslice<R: std::ops::RangeBounds<usize>>(f: &Frozen<A>, range: R) -> Frozen<A> {
        f.slice(range)
    }

    #[test]
    fn frozen_slice_survives_parent_drop() {
        let f: Frozen<A> = owned(b"abcdefgh").into();
        let s = fslice(&f, 2..6);
        drop(f);
        assert_eq!(s.as_slice(), b"cdef");
    }

    #[test]
    fn frozen_slice_chain_outlives_intermediates() {
        let f: Frozen<A> = owned(b"abcdefgh").into();
        let s1 = fslice(&f, 2..7); // "cdefg"
        let s2 = fslice(&s1, 1..4); // "def"
        drop(f);
        drop(s1);
        assert_eq!(s2.as_slice(), b"def");
    }

    //  coalesce: shared-ctrlb path (refcount == 2)

    #[test]
    fn coalesce_shared_ctrlb_succeeds() {
        let o = owned(b"hello world");
        let (prefix, tail) = o.split_at(5);
        assert!(prefix.can_coalesce_with(&tail));
        let merged = prefix.try_coalesce_with(tail).expect("coalesce");
        assert_eq!(merged.as_slice(), b"hello world");
    }

    #[test]
    fn coalesce_shared_ctrlb_blocked_by_extra_clone() {
        let o = owned(b"hello world");
        let (prefix, tail) = o.split_at(5);
        let extra = tail.clone(); // refcount 2 -> 3 (prefix + tail + extra)
        let result = prefix.try_coalesce_with(tail);
        assert!(
            result.is_err(),
            "shared-ctrlb coalesce must require refcount == 2",
        );
        let _ = extra;
    }

    //  coalesce: separate-ctrlb path (refcount == 1, offset matches)

    #[test]
    fn coalesce_separate_ctrlb_succeeds_and_overwrites_prefix_region() {
        let f: Frozen<A> = owned(b"abcdefgh").into();
        let (mut prefix, tail) = f.split_at(3);
        // prefix lives in its own allocation; mutate it before coalescing.
        prefix.as_mut_slice().copy_from_slice(b"XYZ");
        assert!(prefix.can_coalesce_with(&tail));
        let merged = prefix.try_coalesce_with(tail).expect("coalesce");
        assert_eq!(merged.as_slice(), b"XYZdefgh");
    }

    #[test]
    fn coalesce_separate_ctrlb_blocked_by_extra_tail_clone() {
        let f: Frozen<A> = owned(b"abcdefgh").into();
        let (prefix, tail) = f.split_at(3);
        let extra = tail.clone(); // tail refcount 1 -> 2; separate-ctrlb path requires 1
        let result = prefix.try_coalesce_with(tail);
        assert!(result.is_err());
        let _ = extra;
    }

    #[test]
    fn coalesce_separate_ctrlb_blocked_by_offset_mismatch() {
        // Re-slice the tail forward so its offset_from_base no longer matches
        // the prefix length. The separate-ctrlb path must reject this.
        let f: Frozen<A> = owned(b"abcdefgh").into();
        let (prefix, tail) = f.split_at(3);
        let resliced = fslice(&tail, 2..); // offset_from_base = 5, prefix.len = 3
        drop(tail); // bring resliced refcount back to 1
        let result = prefix.try_coalesce_with(resliced);
        assert!(result.is_err());
    }

    //  IoBufMut / SetLen plumbing through MaybeUninit

    #[test]
    fn owned_set_len_after_uninit_writes() {
        let mut o: Owned<A> = Owned::with_capacity(64);
        let cap = o.buf_capacity();
        assert!(cap >= 64);
        {
            let uninit = o.as_uninit();
            for (i, slot) in uninit.iter_mut().take(8).enumerate() {
                slot.write(u8::try_from(i).unwrap());
            }
        }
        // SAFETY: 8 bytes were just initialized via the uninit slice above.
        unsafe { o.set_len(8) };
        assert_eq!(o.as_slice(), &[0, 1, 2, 3, 4, 5, 6, 7]);
    }

    //  Concurrent clone/drop: validates atomic ordering on refcount

    #[test]
    fn frozen_clone_drop_concurrently() {
        let f: Frozen<A> = owned(b"shared payload").into();
        let mut handles = Vec::new();
        for _ in 0..4 {
            let f_thread = f.clone();
            handles.push(std::thread::spawn(move || {
                let a = f_thread.clone();
                let b = f_thread.clone();
                assert_eq!(a.as_slice(), b"shared payload");
                assert_eq!(b.as_slice(), b"shared payload");
                drop(a);
                drop(b);
                drop(f_thread);
            }));
        }
        for h in handles {
            h.join().unwrap();
        }
        assert_eq!(f.as_slice(), b"shared payload");
    }

    //  Concurrent atomics on a shared control block
    //
    // Three handles share one control block: `prefix` and `tail` (returned
    // by `Owned::split_at`) plus a clone `extra` held by another thread.
    // Refcount starts at 3. The other thread drops `extra` (Release
    // fetch_sub: 3 -> 2); the calling thread races a `try_coalesce_with`
    // (Acquire load of the same refcount).
    //
    // `prefix` and `tail` are passed to `try_coalesce_with` by value, so
    // the only refcount transition the calling thread does NOT cause is
    // `extra`'s drop. The two values the calling thread can observe in
    // `can_coalesce_prefix_with_tail` are:
    //   - refcount = 3 → shared-ctrlb gate fails → Err((prefix, tail))
    //   - refcount = 2 → gate passes → merge fires → Ok(merged)
    // Both outcomes must be sound and produce the correct bytes; Miri's
    // tree-borrows + race detector validates the Release/Acquire pairing.
    #[test]
    fn concurrent_coalesce_with_dropping_clone() {
        use std::sync::{Arc, Barrier};

        // Few iterations: Miri scales poorly with thread count and loop
        // depth, but its scheduler explores some interleavings each run.
        for _ in 0..4 {
            let o = owned(b"hello world");
            let (prefix, tail) = o.split_at(5);
            let extra = tail.clone(); // refcount: 2 -> 3

            let barrier = Arc::new(Barrier::new(2));
            let b_thread = Arc::clone(&barrier);

            let handle = std::thread::spawn(move || {
                b_thread.wait();
                drop(extra); // refcount: 3 -> 2 (interleaving varies)
            });

            barrier.wait();
            let result = prefix.try_coalesce_with(tail);
            handle.join().unwrap();
            match result {
                Ok(merged) => {
                    // `extra` dropped before our `can_coalesce` load
                    // (observed refcount = 2): shared-ctrlb merge fired
                    // and recovered the original 11-byte buffer.
                    assert_eq!(merged.as_slice(), b"hello world");
                }
                Err((prefix, tail)) => {
                    // `extra` still alive at our `can_coalesce` load
                    // (observed refcount = 3): merge declined, both
                    // halves returned with their original contents.
                    assert_eq!(prefix.as_slice(), b"hello");
                    assert_eq!(tail.as_slice(), b" world");
                    // The racing drop has completed by now (we joined
                    // above), so refcount is 2 and a retry from the
                    // same caller must succeed.
                    let merged = prefix
                        .try_coalesce_with(tail)
                        .expect("retry after racing drop must succeed");
                    assert_eq!(merged.as_slice(), b"hello world");
                }
            }
        }
    }

    //  Drop-on-panic: destructors must clean up under unwinding
    //
    // Miri's leak detector enforces this implicitly: any allocation not
    // freed by the time the test exits fails the run. The companion case
    // where `Box::new(ControlBlock)` panics between `into_raw_parts` and
    // the new `Extent` is covered by the `AVecRawGuard` tests below.
    #[test]
    fn drop_on_panic_with_active_iobuf_does_not_leak() {
        let result = std::panic::catch_unwind(|| {
            let o = owned(b"abcd");
            let (prefix, tail) = o.split_at(2);
            let frozen: Frozen<A> = owned(b"xyz").into();
            let clone = frozen.clone();
            // Anchor each binding with a positive content check so the
            // test would not pass for the wrong reason if a future
            // refactor silently elided one of the four values: the
            // corresponding `assert_eq!` would fail to compile (the
            // identifier disappears) and surface the regression. Without
            // these anchors, an elided value also elides its allocation
            // and Miri's leak detector stays satisfied.
            assert_eq!(prefix.as_slice(), b"ab");
            assert_eq!(tail.as_slice(), b"cd");
            assert_eq!(frozen.as_slice(), b"xyz");
            assert_eq!(clone.as_slice(), b"xyz");
            // Bind everything in a tuple so all four destructors run
            // during unwinding; each decrements one of two control
            // blocks and Miri verifies the buffers are released.
            let _keep = (prefix, tail, frozen, clone);
            panic!("simulated user-code panic");
        });
        assert!(result.is_err());
    }

    //  Boundary splits: Owned::split_at(0) and Owned::split_at(len)
    //
    // At split_at(0): prefix.len == 0, tail.ptr aliases base.
    // At split_at(len): prefix.len == len, tail.ptr is one-past-end with len 0.
    // Both must round-trip through try_coalesce_with cleanly.

    #[test]
    fn coalesce_after_owned_split_at_zero_round_trips() {
        let o = owned(b"hello");
        let (prefix, tail) = o.split_at(0);
        assert!(prefix.is_empty());
        assert_eq!(tail.as_slice(), b"hello");
        let merged = prefix.try_coalesce_with(tail).expect("coalesce");
        assert_eq!(merged.as_slice(), b"hello");
    }

    #[test]
    fn coalesce_after_owned_split_at_full_len_round_trips() {
        let o = owned(b"hello");
        let (prefix, tail) = o.split_at(5);
        assert_eq!(prefix.as_slice(), b"hello");
        assert!(tail.is_empty());
        let merged = prefix.try_coalesce_with(tail).expect("coalesce");
        assert_eq!(merged.as_slice(), b"hello");
    }

    //  AVecRawGuard: panic between into_raw_parts and ControlBlock::new
    //
    // The window between `AVec::into_raw_parts` (which surrenders the
    // backing buffer's destructor) and `ControlBlock::new` (which calls
    // `Box::new`, fallible under allocator pressure) used to leak the
    // buffer if `Box::new` panicked. `AVecRawGuard` reconstitutes the
    // `AVec` on unwind so the buffer is freed.
    //
    // We can't readily inject a panic into `Box::new` itself, so this
    // exercises the guard primitive directly: a `panic!` while the guard
    // is alive must release the buffer. Miri's leak detector enforces it.

    #[test]
    fn avec_raw_guard_releases_buffer_on_unwind() {
        let result = std::panic::catch_unwind(|| {
            let mut v: AVec<u8, ConstAlign<A>> = AVec::new(A);
            v.extend_from_slice(b"buffer that must be freed when the guard drops");
            let (ptr, _, len, capacity) = v.into_raw_parts();
            let _guard = AVecRawGuard::<A>::new(ptr, len, capacity);
            panic!("simulated allocator failure inside ControlBlock::new");
        });
        assert!(result.is_err());
    }

    #[test]
    fn avec_raw_guard_defuse_skips_destructor() {
        // Success path: `defuse` must skip the destructor so the buffer is
        // not double-freed when the new owner (Extent / control block)
        // releases it. Reconstitute the AVec manually after defusing and
        // confirm the bytes are intact: proves the guard did NOT free.
        let mut v: AVec<u8, ConstAlign<A>> = AVec::new(A);
        v.extend_from_slice(b"contents");
        let (ptr, _, len, capacity) = v.into_raw_parts();
        let guard = AVecRawGuard::<A>::new(ptr, len, capacity);
        guard.defuse();
        // SAFETY: defuse forgot the guard, so the raw parts are still
        // ours to reconstitute exactly once.
        let v = unsafe { AVec::<u8, ConstAlign<A>>::from_raw_parts(ptr, A, len, capacity) };
        assert_eq!(v.as_slice(), b"contents");
    }
}
