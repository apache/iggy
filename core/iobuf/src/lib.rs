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
        let base: NonNull<u8> = unsafe { NonNull::new_unchecked(ptr) };
        let tail = unsafe { NonNull::new_unchecked(ptr.add(split_at)) };
        let ctrlb = ControlBlock::new(base, len, capacity);
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

        let base: NonNull<u8> = unsafe { NonNull::new_unchecked(ptr) };
        let ctrlb = ControlBlock::new(base, len, capacity);
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
        let data = unsafe { NonNull::new_unchecked(ptr) };

        let ctrlb = ControlBlock::new(data, len, capacity);

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
    let data = unsafe { NonNull::new_unchecked(ptr) };
    let ctrlb = ControlBlock::new(data, len, capacity);

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

unsafe fn release_control_block_w_allocation<const ALIGN: usize>(ctrlb: NonNull<ControlBlock>) {
    let old = unsafe { ctrlb.as_ref() }
        .ref_count
        .fetch_sub(1, Ordering::Release);
    debug_assert!(old > 0, "control block refcount underflow");

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

unsafe fn reclaim_unique_control_block(ctrlb: NonNull<ControlBlock>) -> ControlBlock {
    assert_eq!(
        unsafe { ctrlb.as_ref() }.ref_count.load(Ordering::Acquire),
        1
    );
    unsafe { *Box::from_raw(ctrlb.as_ptr()) }
}

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
