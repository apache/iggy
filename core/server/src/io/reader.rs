use std::io::{Seek, SeekFrom};

use monoio::{
    BufResult,
    buf::{IoBufMut, IoVecBufMut},
    io::{AsyncReadRent, BufReader},
};

pub struct IggyReader<R: AsyncReadRent + Seek> {
    inner: BufReader<R>,
}

impl<R: AsyncReadRent + Seek> IggyReader<R> {
    pub fn new(reader: R) -> Self {
        Self {
            inner: BufReader::new(reader),
        }
    }

    pub fn with_capacity(capacity: usize, reader: R) -> Self {
        Self {
            inner: BufReader::with_capacity(capacity, reader),
        }
    }
}

impl<R: AsyncReadRent + Seek> Seek for IggyReader<R> {
    fn seek(&mut self, pos: SeekFrom) -> std::io::Result<u64> {
        self.inner.get_mut().seek(pos)
    }
}

impl<R> AsyncReadRent for IggyReader<R>
where
    R: AsyncReadRent + Seek,
{
    fn read<T: IoBufMut>(&mut self, buf: T) -> impl Future<Output = BufResult<usize, T>> {
        self.inner.read(buf)
    }

    fn readv<T: IoVecBufMut>(&mut self, buf: T) -> impl Future<Output = BufResult<usize, T>> {
        self.inner.readv(buf)
    }
}
