use std::io::{Seek, SeekFrom};

use monoio::{
    BufResult,
    io::{AsyncWriteRent, BufWriter},
};

pub struct IggyWriter<W: AsyncWriteRent + Seek> {
    inner: BufWriter<W>,
}

impl<W: AsyncWriteRent + Seek> IggyWriter<W> {
    pub fn new(writer: W) -> Self {
        Self {
            inner: BufWriter::new(writer),
        }
    }

    pub fn with_capacity(capacity: usize, writer: W) -> Self {
        Self {
            inner: BufWriter::with_capacity(capacity, writer),
        }
    }
}

impl<W: AsyncWriteRent + Seek> Seek for IggyWriter<W> {
    fn seek(&mut self, pos: SeekFrom) -> std::io::Result<u64> {
        self.inner.get_mut().seek(pos)
    }
}

impl<W> AsyncWriteRent for IggyWriter<W>
where
    W: AsyncWriteRent + Seek,
{
    fn write<T: monoio::buf::IoBuf>(
        &mut self,
        buf: T,
    ) -> impl Future<Output = BufResult<usize, T>> {
        self.inner.write(buf)
    }

    fn writev<T: monoio::buf::IoVecBuf>(
        &mut self,
        buf_vec: T,
    ) -> impl Future<Output = BufResult<usize, T>> {
        self.inner.writev(buf_vec)
    }

    fn flush(&mut self) -> impl Future<Output = std::io::Result<()>> {
        self.inner.flush()
    }

    fn shutdown(&mut self) -> impl Future<Output = std::io::Result<()>> {
        self.inner.shutdown()
    }
}
