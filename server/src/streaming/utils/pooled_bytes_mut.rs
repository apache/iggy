use super::memory_pool::{memory_pool, BytesMutExt};
use bytes::{Buf, BytesMut};
use std::ops::{Deref, DerefMut};

#[derive(Debug)]
pub struct PooledBytesMut {
    from_pool: bool,
    original_capacity: usize,
    inner: BytesMut,
}

impl Default for PooledBytesMut {
    fn default() -> Self {
        Self::empty()
    }
}

impl PooledBytesMut {
    pub fn with_capacity(capacity: usize) -> Self {
        let buffer = memory_pool().acquire_buffer(capacity);
        let original_capacity = buffer.capacity();
        Self {
            from_pool: true,
            original_capacity,
            inner: buffer,
        }
    }

    pub fn from_existing(existing: BytesMut) -> Self {
        Self {
            from_pool: false,
            original_capacity: existing.capacity(),
            inner: existing,
        }
    }

    pub fn empty() -> Self {
        Self {
            from_pool: false,
            original_capacity: 0,
            inner: BytesMut::new(),
        }
    }
}

impl Deref for PooledBytesMut {
    type Target = BytesMut;

    fn deref(&self) -> &Self::Target {
        &self.inner
    }
}

impl DerefMut for PooledBytesMut {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.inner
    }
}

impl Drop for PooledBytesMut {
    fn drop(&mut self) {
        if self.from_pool {
            let buf = std::mem::take(&mut self.inner);
            buf.return_to_pool(self.original_capacity);
        }
    }
}

impl From<&[u8]> for PooledBytesMut {
    fn from(slice: &[u8]) -> Self {
        let mut buf = PooledBytesMut::with_capacity(slice.len());
        buf.inner.extend_from_slice(slice);
        buf
    }
}

impl Buf for PooledBytesMut {
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
