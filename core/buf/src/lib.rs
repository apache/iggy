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
    /// Panics if `split_at > self.len()` or if `split_at` is not a multiple of `ALIGN` bytes.
    pub fn split_at(self, split_at: usize) -> TwoHalves<ALIGN> {
        assert!(split_at <= self.inner.len());

        // Take ownership of the AVec's allocation. After this, we are responsible
        // for deallocating via `AVec::from_raw_parts` or equivalent.
        let (ptr, _, len, capacity) = self.inner.into_raw_parts();

        // SAFETY: both pointers are constructed from the same `Inner` allocation, the split_at bounds are validated.
        // The control block captures original `Inner` metadata to allow reconstructing the original frame for merging/dropping.
        // The ptr provenence rules are maintained by the use of `NonNull` apis.
        let base: NonNull<u8> = unsafe { NonNull::new_unchecked(ptr) };
        let tail = unsafe { NonNull::new_unchecked(ptr.add(split_at)) };
        let ctrlb = ControlBlock::new(base, len, capacity);

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
    inner: (Extent, Extent),
}

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

    pub fn split_at(&self) -> usize {
        self.inner.0.len
    }

    pub fn total_len(&self) -> usize {
        self.inner.0.len + self.inner.1.len
    }

    pub fn is_unique(&self) -> bool {
        // `inner.1` is the authoritative owner of the original frame allocation.
        // SAFETY: `inner.1.ctrlb` points to a live control block while `self` is alive.
        unsafe {
            self.inner
                .1
                .ctrlb
                .as_ref()
                .ref_count
                .load(Ordering::Acquire)
                == 1
        }
    }

    pub fn try_merge(self) -> Result<Owned<ALIGN>, Self> {
        if !self.is_unique() {
            return Err(self);
        }

        // Transfer ownership to prevent double-free
        let this = ManuallyDrop::new(self);
        let head = this.inner.0;
        let tail = this.inner.1;
        let split_at = head.len;

        // SAFETY: `tail.ctrlb` is unique at this point,
        // If `head.ctrlb != tail.ctrlb`, the head owns a standalone allocation
        // that must be released after copying.
        unsafe {
            let ctrlb_eq = std::ptr::addr_eq(head.ctrlb.as_ptr(), tail.ctrlb.as_ptr());

            if !ctrlb_eq {
                let tail_ctrlb = tail.ctrlb.as_ref();

                // We are patching up the original allocation, with the current head data, so that the resulting `Owned` has correct content.
                let dst = slice::from_raw_parts_mut(tail_ctrlb.base.as_ptr(), split_at);
                dst.copy_from_slice(head.as_slice());
                release_control_block_w_allocation::<ALIGN>(head.ctrlb);
            }

            let ctrlb = reclaim_unique_control_block(tail.ctrlb);
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
                Extent::copy_from_slice::<ALIGN>(self.head()),
                self.inner.1.clone(),
            ),
        }
    }
}

impl<const ALIGN: usize> Drop for TwoHalves<ALIGN> {
    fn drop(&mut self) {
        // SAFETY: `inner.0.ctrlb` / `inner.1.ctrlb` point to live control blocks while `self` is alive.
        let ctrlb_eq = std::ptr::addr_eq(self.inner.0.ctrlb.as_ptr(), self.inner.1.ctrlb.as_ptr());
        unsafe {
            if ctrlb_eq {
                release_control_block_w_allocation::<ALIGN>(self.inner.1.ctrlb);
            } else {
                // Different control blocks, release both
                release_control_block_w_allocation::<ALIGN>(self.inner.0.ctrlb);
                release_control_block_w_allocation::<ALIGN>(self.inner.1.ctrlb);
            }
        }
    }
}

impl<const ALIGN: usize> std::fmt::Debug for TwoHalves<ALIGN> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("TwoHalves")
            .field("split_at", &self.split_at())
            .field("head_len", &self.inner.0.len)
            .field("tail_len", &self.inner.1.len)
            .field("halves_alias", &(self.inner.0.ctrlb == self.inner.1.ctrlb))
            .finish()
    }
}

#[derive(Clone)]
pub struct Frozen<const ALIGN: usize> {
    inner: Extent,
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
    _pad: [u8; 32],
}

impl ControlBlock {
    fn new(base: NonNull<u8>, len: usize, capacity: usize) -> NonNull<Self> {
        let ctrl = Box::new(ControlBlock {
            ref_count: AtomicUsize::new(1),
            base,
            len,
            capacity,
            _pad: [0; 32],
        });
        // SAFETY: Box::into_raw returns a valid pointer
        unsafe { NonNull::new_unchecked(Box::into_raw(ctrl)) }
    }
}

#[derive(Copy)]
struct Extent {
    ptr: NonNull<u8>,
    len: usize,
    ctrlb: NonNull<ControlBlock>,
    _pad: usize,
}

impl Extent {
    fn as_slice(&self) -> &[u8] {
        // SAFETY: ptr and len describe a valid allocation
        unsafe { slice::from_raw_parts(self.ptr.as_ptr(), self.len) }
    }

    unsafe fn as_mut_slice(&mut self) -> &mut [u8] {
        // SAFETY: caller guarantees exclusive access
        unsafe { slice::from_raw_parts_mut(self.ptr.as_ptr(), self.len) }
    }

    fn copy_from_slice<const ALIGN: usize>(src: &[u8]) -> Self {
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

impl Clone for Extent {
    fn clone(&self) -> Self {
        // SAFETY: `self.ctrlb` points to a live control block while `self` is alive.
        unsafe {
            self.ctrlb
                .as_ref()
                .ref_count
                .fetch_add(1, Ordering::Relaxed);
        }
        *self
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
    debug_assert_eq!(
        // SAFETY: caller guarantees `ctrlb` points to a live control block.
        unsafe { ctrlb.as_ref() }.ref_count.load(Ordering::Acquire),
        1
    );

    // SAFETY: caller guarantees uniqueness, so ownership of the control block can be reclaimed directly.
    unsafe { *Box::from_raw(ctrlb.as_ptr()) }
}

// =============================================================================
// Tests
// =============================================================================

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
        assert_eq!(buffer.split_at(), 2);
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

        assert!(!original.is_unique());
        assert!(!cloned.is_unique());

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

        let buffer = buffer.try_merge().unwrap_err();
        assert!(!buffer.is_unique());

        drop(clone);

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
        assert!(clone.is_unique());

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
    fn clone_of_clone_keeps_tail_sharing_semantics() {
        let owned = make_owned(&[1, 2, 3, 4, 5]);
        let original = owned.split_at(2);
        let clone1 = original.clone();
        let clone2 = clone1.clone();

        assert!(!original.is_unique());
        assert!(!clone1.is_unique());
        assert!(!clone2.is_unique());
    }
}
