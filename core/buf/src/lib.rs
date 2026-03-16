use std::mem::ManuallyDrop;
use std::ptr::NonNull;
use std::slice;
use std::sync::atomic::{AtomicUsize, Ordering, fence};

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Owned {
    inner: Vec<u8>,
}

#[derive(Clone, Copy)]
struct Half {
    ptr: NonNull<u8>,
    len: usize,
    ctrlb: NonNull<ControlBlock>,
}

struct ControlBlock {
    ref_count: AtomicUsize,
    base: NonNull<u8>,
    len: usize,
    cap: usize,
}

fn create_control_block(base: NonNull<u8>, len: usize, cap: usize) -> NonNull<ControlBlock> {
    let ctrlb = Box::new(ControlBlock {
        ref_count: AtomicUsize::new(1),
        base,
        len,
        cap,
    });
    // SAFETY: `ctrlb` is a valid control block for the lifetime of the returned halves.
    unsafe { NonNull::new_unchecked(Box::into_raw(ctrlb)) }
}

pub struct TwoHalves {
    buf: (Half, Half),
    split_at: usize,
}

impl From<Vec<u8>> for Owned {
    fn from(vec: Vec<u8>) -> Self {
        Self { inner: vec }
    }
}

impl From<Owned> for Vec<u8> {
    fn from(value: Owned) -> Self {
        value.inner
    }
}

impl Owned {
    pub fn from_vec(vec: Vec<u8>) -> Self {
        Self { inner: vec }
    }

    pub fn as_slice(&self) -> &[u8] {
        &self.inner
    }

    pub fn as_mut_slice(&mut self) -> &mut [u8] {
        &mut self.inner
    }

    pub fn split_at(self, split_at: usize) -> TwoHalves {
        assert!(split_at <= self.inner.len());
        // Transfering ownership of the `Inner` to `TwoHalves`, which is from now on responsible for dropping it.
        let mut inner = ManuallyDrop::new(self.inner);
        let len = inner.len();
        let cap = inner.capacity();

        // SAFETY: both pointers are constructed from the same `Inner` allocation, the split_at bounds are validated.
        // The control block captures original `Inner` metadata to allow reconstructing the original frame for merging/dropping.
        // The ptr provenence rules are maintained by the use of `NonNull` apis.
        let ptr = inner.as_mut_ptr();
        let base = unsafe { NonNull::new_unchecked(ptr) };
        let other = unsafe { NonNull::new_unchecked(ptr.add(split_at)) };

        let ctrlb = create_control_block(base, len, cap);
        TwoHalves {
            buf: (
                Half {
                    ptr: base,
                    len: split_at,
                    ctrlb,
                },
                Half {
                    ptr: other,
                    len: len - split_at,
                    ctrlb,
                },
            ),
            split_at,
        }
    }
}

impl Half {
    fn as_slice(&self) -> &[u8] {
        // SAFETY: `ptr,len` always describe a live allocation owned by `ctrlb`.
        unsafe { slice::from_raw_parts(self.ptr.as_ptr(), self.len) }
    }

    unsafe fn as_mut_slice(&mut self) -> &mut [u8] {
        // SAFETY: caller must provide the safety guarantees for mutable access.
        unsafe { slice::from_raw_parts_mut(self.ptr.as_ptr(), self.len) }
    }

    fn share(&self) -> Self {
        // SAFETY: `ctrlb` points to a valid control block for the lifetime of this half.
        unsafe {
            self.ctrlb
                .as_ref()
                .ref_count
                .fetch_add(1, Ordering::Relaxed);
        }
        *self
    }

    fn copy_from_slice(src: &[u8]) -> Self {
        let mut vec = Vec::with_capacity(src.len());
        vec.extend_from_slice(src);

        // Transfering ownership of the `Inner` to `Half`, which is from now on responsible for dropping it.
        let mut inner = ManuallyDrop::new(vec);
        let ptr = inner.as_mut_ptr();
        let base = unsafe { NonNull::new_unchecked(ptr) };
        let len = inner.len();
        let cap = inner.capacity();

        let ctrlb = create_control_block(base, len, cap);
        Self {
            ptr: base,
            len,
            ctrlb,
        }
    }
}

/// Drops the control block, together with associated allocation if this is the last reference.
/// This is used for both halves, so it must be careful to only drop the shared allocation once.
unsafe fn release_control_block_w_allocation(ctrlb: NonNull<ControlBlock>) {
    // SAFETY: caller guarantees `ctrlb` points to a live control block.
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

    // SAFETY: refcount reached zero, so this control block is uniquely owned here.
    let ctrlb = unsafe { Box::from_raw(ctrlb.as_ptr()) };
    // SAFETY: `base,len,cap` were captured from a `Vec<u8>` allocation and are still valid.
    let _vec = unsafe { Vec::from_raw_parts(ctrlb.base.as_ptr(), ctrlb.len, ctrlb.cap) };
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

impl TwoHalves {
    pub fn head(&self) -> &[u8] {
        self.buf.0.as_slice()
    }

    pub fn head_mut(&mut self) -> &mut [u8] {
        // SAFETY: We are accessing the head half mutably, this is the only correct operation, as the head is not shared between clones,
        // instead it gets copied.
        unsafe { self.buf.0.as_mut_slice() }
    }

    pub fn tail(&self) -> &[u8] {
        self.buf.1.as_slice()
    }

    pub fn split_at(&self) -> usize {
        self.split_at
    }

    pub fn total_len(&self) -> usize {
        self.buf.0.len + self.buf.1.len
    }

    pub fn is_unique(&self) -> bool {
        // `buf.1` is the authoritative owner of the original frame allocation.
        // SAFETY: `buf.1.ctrlb` points to a live control block while `self` is alive.
        unsafe { self.buf.1.ctrlb.as_ref().ref_count.load(Ordering::Acquire) == 1 }
    }

    pub fn try_merge(self) -> Result<Owned, Self> {
        if !self.is_unique() {
            return Err(self);
        }

        // We transfer the ownership to `Owned`, in order to prevent double-free, we must not drop `Self`.
        let this = ManuallyDrop::new(self);
        let head = this.buf.0;
        let tail = this.buf.1;
        let split_at = this.split_at;

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
                release_control_block_w_allocation(head.ctrlb);
            }

            let ctrlb = reclaim_unique_control_block(tail.ctrlb);
            // SAFETY: `ctrlb.base,len,cap` were captured from a `Vec<u8>` allocation and
            // are now exclusively owned by this path.
            let inner = Vec::from_raw_parts(ctrlb.base.as_ptr(), ctrlb.len, ctrlb.cap);
            Ok(Owned { inner })
        }
    }
}

impl Clone for TwoHalves {
    fn clone(&self) -> Self {
        Self {
            buf: (Half::copy_from_slice(self.head()), self.buf.1.share()),
            split_at: self.split_at,
        }
    }
}

impl Drop for TwoHalves {
    fn drop(&mut self) {
        // SAFETY: `buf.0.ctrlb` / `buf.1.ctrlb` point to live control blocks while `self` is alive.
        unsafe {
            if std::ptr::addr_eq(self.buf.0.ctrlb.as_ptr(), self.buf.1.ctrlb.as_ptr()) {
                release_control_block_w_allocation(self.buf.1.ctrlb);
            } else {
                // Two separate control blocks, so we must release both allocations.
                release_control_block_w_allocation(self.buf.0.ctrlb);
                release_control_block_w_allocation(self.buf.1.ctrlb);
            }
        }
    }
}

impl std::fmt::Debug for TwoHalves {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("TwoHalves")
            .field("split_at", &self.split_at)
            .field("head_len", &self.buf.0.len)
            .field("tail_len", &self.buf.1.len)
            .field("halves_alias", &(self.buf.0.ctrlb == self.buf.1.ctrlb))
            .finish()
    }
}

#[cfg(test)]
mod tests {
    use super::Owned;

    #[test]
    fn split_exposes_head_and_tail() {
        let mut buffer = Owned::from_vec(vec![1, 2, 3, 4, 5]).split_at(2);

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
        let mut original = Owned::from_vec(vec![1, 2, 3, 4, 5]).split_at(2);
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
        let mut buffer = Owned::from_vec(vec![1, 2, 3, 4, 5]).split_at(2);
        buffer.head_mut().copy_from_slice(&[8, 9]);

        let merged: Vec<u8> = buffer.try_merge().unwrap().into();
        assert_eq!(merged, vec![8, 9, 3, 4, 5]);
    }

    #[test]
    fn try_merge_fails_while_tail_is_shared() {
        let buffer = Owned::from_vec(vec![1, 2, 3, 4, 5]).split_at(2);
        let clone = buffer.clone();

        let buffer = buffer.try_merge().unwrap_err();
        assert!(!buffer.is_unique());

        drop(clone);

        let merged: Vec<u8> = buffer.try_merge().unwrap().into();
        assert_eq!(merged, vec![1, 2, 3, 4, 5]);
    }

    #[test]
    fn merge_after_cloned_head_mutation_writes_back_to_original_frame() {
        let buffer = Owned::from_vec(vec![1, 2, 3, 4, 5]).split_at(2);
        let mut clone = buffer.clone();

        drop(buffer);

        clone.head_mut().copy_from_slice(&[4, 2]);
        assert!(clone.is_unique());

        let merged: Vec<u8> = clone.try_merge().unwrap().into();
        assert_eq!(merged, vec![4, 2, 3, 4, 5]);
    }

    #[test]
    fn zero_length_splits_work() {
        let left_empty = Owned::from_vec(vec![1, 2, 3]).split_at(0);
        assert_eq!(left_empty.head(), &[]);
        assert_eq!(left_empty.tail(), &[1, 2, 3]);

        let right_empty = Owned::from_vec(vec![1, 2, 3]).split_at(3);
        assert_eq!(right_empty.head(), &[1, 2, 3]);
        assert_eq!(right_empty.tail(), &[]);
    }

    #[test]
    fn clone_of_clone_keeps_tail_sharing_semantics() {
        let original = Owned::from_vec(vec![1, 2, 3, 4, 5]).split_at(2);
        let clone1 = original.clone();
        let clone2 = clone1.clone();

        assert!(!original.is_unique());
        assert!(!clone1.is_unique());
        assert!(!clone2.is_unique());
    }
}
