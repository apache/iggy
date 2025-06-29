use std::io::SeekFrom;

use monoio::{
    BufResult,
    buf::{IoBuf, IoBufMut, IoVecBuf, IoVecBufMut, IoVecWrapper},
    fs::{File, Metadata},
    io::{AsyncReadRent, AsyncWriteRent},
};

/// Wrapper around `monoio::fs::File` to provide a consistent API for reading and writing files
/// in an asynchronous context. This struct maintains the current position in the file and provides
/// methods to read and write data at specific positions.
#[derive(Debug)]
pub struct IggyFile {
    file: File,
    position: u64,
}

impl From<File> for IggyFile {
    fn from(file: File) -> Self {
        Self { file, position: 0 }
    }
}

impl std::io::Seek for IggyFile {
    /// This method doesn't do bound checking aswell as, the `End` variant is not supported.
    fn seek(&mut self, pos: SeekFrom) -> std::io::Result<u64> {
        self.position = match pos {
            SeekFrom::Start(n) => n as i64,
            SeekFrom::End(_) => unreachable!("End variant is not supported in IggyFile"),
            SeekFrom::Current(n) => self.position as i64 + n,
        } as u64;
        Ok(self.position)
    }
}

impl IggyFile {
    pub fn new(file: File) -> Self {
        Self { file, position: 0 }
    }

    pub async fn metadata(&self) -> std::io::Result<Metadata> {
        self.file.metadata().await
    }

    pub async fn write_at<T: IoBuf>(&self, buf: T, pos: usize) -> BufResult<usize, T> {
        self.file.write_at(buf, pos as u64).await
    }

    pub async fn write_all_at<T: IoBuf>(&self, buf: T, pos: u64) -> BufResult<(), T> {
        self.file.write_all_at(buf, pos).await
    }

    pub async fn read_exact_at<T: IoBufMut>(&self, buf: T, pos: u64) -> BufResult<(), T> {
        self.file.read_exact_at(buf, pos).await
    }

    pub async fn read_at<T: IoBufMut>(&self, buf: T, pos: usize) -> BufResult<usize, T> {
        self.file.read_at(buf, pos as u64).await
    }

    pub async fn sync_all(&self) -> std::io::Result<()> {
        self.file.sync_all().await
    }
}

impl AsyncReadRent for IggyFile {
    /// Reads `exactly` `buf.len()` bytes into the buffer.
    async fn read<T: IoBufMut>(&mut self, buf: T) -> BufResult<usize, T> {
        let (res, buf) = self.file.read_at(buf, self.position).await;
        let n = match res {
            Ok(n) => n,
            Err(e) => return (Err(e), buf),
        };
        self.position += n as u64;
        (Ok(n), buf)
    }

    //TODO(numinex) - maybe implement this ?
    fn readv<T: IoVecBufMut>(&mut self, buf: T) -> impl Future<Output = BufResult<usize, T>> {
        async move { (Ok(0), buf) }
    }
}

impl AsyncWriteRent for IggyFile {
    /// Writes entire buffer to the file at the current position.
    async fn write<B: IoBuf>(&mut self, buf: B) -> BufResult<usize, B> {
        let (res, buf) = self.file.write_at(buf, self.position).await;
        let n = match res {
            Ok(n) => n,
            Err(e) => return (Err(e), buf),
        };
        self.position += n as u64;
        (Ok(n), buf)
    }

    // This is bait!!!!
    async fn writev<T: IoVecBuf>(&mut self, buf_vec: T) -> BufResult<usize, T> {
        unimplemented!("writev is not implemented for IggyFile");
    }

    async fn flush(&mut self) -> std::io::Result<()> {
        self.file.sync_all().await
    }

    //TODO(numinex) - How to implement this ?
    async fn shutdown(&mut self) -> std::io::Result<()> {
        panic!("shutdown is not supported for IggyFile");
        /*
                self.file.sync_all().await;
                unsafe {
                    let file = *self.file;
                    file.close().await
                }
        */
    }
}
