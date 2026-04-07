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

use std::cell::RefCell;

use bytes::Bytes;
use compio::{
    fs::{File, OpenOptions},
    io::AsyncWriteAtExt,
};
use tracing::{error, trace};

use crate::{IggyError, PooledBuffer, alloc::memory_pool::ALIGNMENT};

#[cfg(target_os = "linux")]
const O_DIRECT: i32 = 0x4000;
#[cfg(target_os = "linux")]
const O_DSYNC: i32 = 0x1000;

#[cfg(not(target_os = "linux"))]
const O_DIRECT: i32 = 0;
#[cfg(not(target_os = "linux"))]
const O_DSYNC: i32 = 0;

/// Cache line padding to prevent false sharing
#[repr(align(64))]
#[derive(Debug)]
struct Padded<T>(T);

#[derive(Debug)]
pub struct SharedTail {
    inner: RefCell<TailSnapshot>,
}

#[derive(Debug, Clone)]
struct TailSnapshot {
    start: u64,
    end: u64,
    data: Vec<u8>,
}

/// A shared view of the writer's uncommitted tail buffer.
///
/// The writer updates this after every write so that readers can
/// serve reads that fall into the not-yet-flushed region without
/// touching `DirectFile`.
impl SharedTail {
    pub fn new() -> Self {
        Self {
            inner: RefCell::new(TailSnapshot {
                start: 0,
                end: 0,
                data: Vec::new(),
            }),
        }
    }

    pub fn update(&self, position: u64, tail_buf: &[u8], tail_len: usize) {
        let mut snap = self.inner.borrow_mut();
        snap.data.clear();
        if tail_len > 0 {
            snap.data.extend_from_slice(&tail_buf[..tail_len]);
        }
        snap.start = position;
        snap.end = position + tail_len as u64;
    }

    pub fn snapshot(&self) -> (u64, u64, Vec<u8>) {
        let snap = self.inner.borrow();
        (snap.start, snap.end, snap.data.clone())
    }
}

/// Low-level O_DIRECT file writer.
///
/// All I/O is aligned to `ALIGNMENT` (typically 4096). Data smaller than
/// one alignment block is buffered in `tail` until a full block can be
/// flushed
#[derive(Debug)]
pub struct DirectFile {
    /// Filesystem path
    file_path: String,
    /// O_DIRECT + O_DSYNC file handle
    file: File,
    /// Current write pos in the file. Always a multiple of `ALIGNMENT`
    file_position: u64,
    /// Buf that holding the partial (< ALIGNMENT) tail block
    tail: PooledBuffer,
    /// Number of valid bytes in `tail`. Padded to its own cache line to avoid false sharing on the
    /// hot path
    tail_len: Padded<usize>,
    /// Reusable carry-over buffer for `write_vectored`
    spare: PooledBuffer,
}

impl DirectFile {
    pub async fn open(
        file_path: &str,
        initial_position: u64,
        file_exists: bool,
        dsync: bool,
    ) -> Result<Self, IggyError> {
        debug_assert_eq!(
            initial_position % ALIGNMENT as u64,
            0,
            "initial_position must be aligned, got {initial_position}"
        );

        let mut flags = O_DIRECT;

        if dsync {
            flags |= O_DSYNC;
        }

        let file = OpenOptions::new()
            .create(true)
            .write(true)
            .custom_flags(flags as _)
            .open(file_path)
            .await
            .map_err(|err| {
                error!("Failed to open file with O_DIRECT: {file_path}, error: {err}");
                IggyError::CannotReadFile
            })?;

        trace!(
            "Successfully opened DirectIO File: {}, pos: {}, exists: {}",
            file_path, initial_position, file_exists
        );

        Ok(Self {
            file_path: file_path.to_string(),
            file,
            file_position: initial_position,
            tail: PooledBuffer::with_capacity(ALIGNMENT),
            tail_len: Padded(0),
            spare: PooledBuffer::with_capacity(ALIGNMENT * 2),
        })
    }

    #[allow(dead_code)]
    pub async fn get_file_size(&self) -> Result<u64, IggyError> {
        self.file
            .metadata()
            .await
            .map_err(|err| {
                error!(
                    "Failed to get metadata of file: {}, error: {err}",
                    self.file_path
                );
                IggyError::CannotReadFileMetadata
            })
            .map(|metadata| metadata.len())
    }

    /// Write data from an owned `PooledBuffer`
    pub async fn write_all(&mut self, buffer: Bytes) -> Result<usize, IggyError> {
        let initial_len = buffer.len();

        trace!(
            "DirectFile write_all: file={}, data_len={}, pos={}, tail_len={}",
            self.file_path, initial_len, self.file_position, self.tail_len.0
        );

        // Fast path: no buffered tail and buffer is perfectly aligned in both length and pointer.
        if self.tail_len.0 == 0 && buffer.len().is_multiple_of(ALIGNMENT) {
            debug_assert_eq!(
                self.file_position % ALIGNMENT as u64,
                0,
                "file_position must be alignment alined before a direct write"
            );

            let bytes = buffer.len();
            let (result, _) = self
                .file
                .write_all_at(buffer, self.file_position)
                .await
                .into();

            result.map_err(|error| {
                error!(
                    "DirectFile write failed: file={}, pos={}, len={} error={}",
                    self.file_path, self.file_position, bytes, error
                );
                IggyError::CannotWriteToFile
            })?;

            self.file_position += bytes as u64;
            return Ok(initial_len);
        }

        let total = self.tail_len.0 + buffer.len();
        let aligned_total = total & !(ALIGNMENT - 1);
        let new_tail_len = total - aligned_total;

        if aligned_total > 0 {
            let mut write_buf = PooledBuffer::with_capacity(aligned_total);

            if self.tail_len.0 > 0 {
                write_buf.extend_from_slice(&self.tail[..self.tail_len.0]);
                self.tail_len.0 = 0;
            }

            // Append data up to alignment boundary
            let data_for_write = aligned_total - write_buf.len();
            write_buf.extend_from_slice(&buffer[..data_for_write]);

            let bytes = write_buf.len();
            let (result, _) = self
                .file
                .write_all_at(write_buf, self.file_position)
                .await
                .into();

            result.map_err(|error| {
                error!(
                    "DirectFile write failed: file={}, pos={}, len={}, error={}",
                    self.file_path, self.file_position, bytes, error
                );
                IggyError::CannotWriteToFile
            })?;

            self.file_position += bytes as u64;

            if new_tail_len > 0 {
                self.tail.clear();
                self.tail.extend_from_slice(&buffer[data_for_write..]);
                self.tail_len.0 = new_tail_len;
            } else {
                self.tail.clear();
            }
        } else {
            if self.tail_len.0 == 0 {
                self.tail.clear();
            }
            self.tail.extend_from_slice(&buffer);
            self.tail_len.0 = total;
        }

        Ok(initial_len)
    }

    #[allow(dead_code)]
    pub async fn write_from_slice(&mut self, data: &[u8]) -> Result<usize, IggyError> {
        let mut buffer = PooledBuffer::with_capacity(data.len());
        buffer.extend_from_slice(data);
        self.write_all(buffer.freeze_to_bytes()).await
    }

    /// Flush any buffered tail data to disk
    pub async fn flush(&mut self) -> Result<(), IggyError> {
        if self.tail_len.0 > 0 {
            self.tail.resize(ALIGNMENT, 0);
            self.flush_tail().await?;
        }

        Ok(())
    }

    pub async fn truncate(&self, size: u64) -> Result<(), IggyError> {
        self.file.set_len(size).await.map_err(|err| {
            error!("Failed to truncate file: {}, err: {err}", self.file_path);
            IggyError::CannotWriteToFile
        })
    }

    pub fn position(&self) -> u64 {
        self.file_position
    }

    pub fn tail_len(&self) -> usize {
        self.tail_len.0
    }

    pub fn tail_buffer(&self) -> &PooledBuffer {
        &self.tail
    }

    /// Write multiple owned buffers using vectored I/O
    ///
    /// # Return
    /// Total number of logical bytes accepted
    pub async fn write_vectored(&mut self, buffers: Vec<Bytes>) -> Result<usize, IggyError> {
        if buffers.is_empty() {
            return Ok(0);
        }

        let mut total_logical_size: usize = 0;
        let mut write_buffers: Vec<Bytes> = Vec::with_capacity(buffers.len() * 2);

        // Seed carry over from any existing tail data
        let mut carry_over_len: usize = 0;
        if self.tail_len.0 > 0 {
            trace!(
                "write_vectored: seeding {} tail bytes into spare",
                self.tail_len.0
            );

            // Move existing tail bytes into spare so the loop can complete
            // the partial block before writing.
            self.spare.clear();
            self.spare.extend_from_slice(&self.tail[..self.tail_len.0]);
            carry_over_len = self.tail_len.0;
            self.tail_len.0 = 0;
        }

        for buffer in buffers {
            let buffer_len = buffer.len();
            total_logical_size += buffer_len;

            if carry_over_len > 0 {
                let need = ALIGNMENT - carry_over_len;

                if buffer_len >= need {
                    // Complete the current alignment block
                    self.spare.extend_from_slice(&buffer[..need]);

                    // Promote spare to write_buffers
                    let mut completed = std::mem::replace(
                        &mut self.spare,
                        PooledBuffer::with_capacity(ALIGNMENT * 2),
                    );
                    write_buffers.push(completed.freeze_to_bytes());

                    self.spare.clear();
                    carry_over_len = 0;

                    // Handle the portion after the alignment boundary
                    let after_fill = buffer_len - need;
                    let aligned_size = after_fill & !(ALIGNMENT - 1);

                    if aligned_size > 0 {
                        let has_remainder = aligned_size < after_fill;
                        if has_remainder {
                            // Save the sub ALIGNMENT tail to spare
                            self.spare.extend_from_slice(&buffer[need + aligned_size..]);
                            carry_over_len = after_fill - aligned_size;
                        }

                        let mut aligned_buffer = PooledBuffer::with_capacity(aligned_size);
                        aligned_buffer
                            .extend_from_slice(&buffer.as_ref()[need..need + aligned_size]);
                        write_buffers.push(aligned_buffer.freeze_to_bytes());
                    } else if after_fill > 0 {
                        // All remainder is sub ALIGNMENT, move to spare
                        self.spare.extend_from_slice(&buffer[need..]);
                        carry_over_len = after_fill;
                    }
                } else {
                    // Buffer too small to complete the current block, accumulate it
                    self.spare.extend_from_slice(&buffer);
                    carry_over_len += buffer_len;
                }
            } else {
                // No carry over. Process this buffer from scratch
                let aligned_size = buffer_len & !(ALIGNMENT - 1);

                if aligned_size > 0 {
                    if aligned_size == buffer_len {
                        // Perfect aligned, use all buffer
                        write_buffers.push(buffer);
                    } else {
                        // sub ALIGNMENT tail stays in spare
                        self.spare.clear();
                        self.spare.extend_from_slice(&buffer[aligned_size..]);
                        carry_over_len = buffer_len - aligned_size;

                        let aligned_view = buffer.slice(0..aligned_size);
                        write_buffers.push(aligned_view);
                    }
                } else {
                    // All buffer  is sub ALIGNMENT
                    self.spare.clear();
                    self.spare.extend_from_slice(&buffer);
                    carry_over_len = buffer_len;
                }
            }
        }

        // Submit all aligned write buffer in a single vectored syscall
        if !write_buffers.is_empty() {
            let bytes_written: usize = write_buffers.iter().map(|buf| buf.len()).sum();

            debug_assert_eq!(
                self.file_position % ALIGNMENT as u64,
                0,
                "file_position must be aligned before vectored write"
            );

            trace!(
                "write_vectored: submitting {} buffers, {} bytes as position {}",
                write_buffers.len(),
                bytes_written,
                self.file_position
            );

            let (result, _) = self
                .file
                .write_vectored_all_at(write_buffers, self.file_position)
                .await
                .into();

            result.map_err(|err| {
                error!(
                    "Vectored write failed: file={}, error={}",
                    self.file_path, err
                );

                IggyError::CannotWriteToFile
            })?;

            self.file_position += bytes_written as u64;
        }

        // Persist any remaining carry over bytes into the tail buffer
        if carry_over_len > 0 {
            self.tail.clear();
            self.tail.extend_from_slice(&self.spare[..carry_over_len]);
            self.tail_len.0 = carry_over_len;
        }

        Ok(total_logical_size)
    }

    pub fn set_tail(&mut self, data: &[u8]) {
        assert!(
            data.len() < ALIGNMENT,
            "set_tail called with {} bytes, must be < {}",
            data.len(),
            ALIGNMENT
        );
        self.tail.clear();
        self.tail.extend_from_slice(data);
        self.tail_len.0 = data.len();
    }

    /// Write `self.tail` to disk (`ALIGNMENT` byptes) to disk and reset tail state
    async fn flush_tail(&mut self) -> Result<(), IggyError> {
        assert_eq!(
            self.tail.len(),
            ALIGNMENT,
            "flush_tail called with tail.len()={}, expected {}",
            self.tail.len(),
            ALIGNMENT
        );

        debug_assert_eq!(
            self.file_position % ALIGNMENT as u64,
            0,
            "file_position must be ALIGNMENT aligned when flushing tail"
        );

        // Transfer ownership of the buffer to the kernel
        let tail_buffer = std::mem::replace(&mut self.tail, PooledBuffer::with_capacity(ALIGNMENT));

        let (result, returned_buf) = self
            .file
            .write_all_at(tail_buffer, self.file_position)
            .await
            .into();

        result.map_err(|err| {
            error!(
                "Tail flush failed: file={}, pos={}, error={}",
                self.file_path, self.file_position, err
            );
            IggyError::CannotWriteToFile
        })?;

        self.file_position += ALIGNMENT as u64;
        self.tail_len.0 = 0;

        // reclaim that alloc for later reuse
        self.tail = returned_buf;
        self.tail.clear();

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use std::{str::FromStr, sync::Once};

    use crate::{IggyByteSize, MemoryPool, MemoryPoolConfigOther};

    use super::*;
    use tempfile::tempdir;

    fn read_file_bytes(path: &str) -> Vec<u8> {
        std::fs::read(path).expect("failed to read file")
    }

    fn make_buffer(size: usize, fill: u8) -> Bytes {
        let mut buf = PooledBuffer::with_capacity(size);
        buf.extend_from_slice(&vec![fill; size]);
        buf.freeze_to_bytes()
    }

    static TEST_INIT: Once = Once::new();

    fn initialize_pool_for_tests() {
        TEST_INIT.call_once(|| {
            let config = MemoryPoolConfigOther {
                enabled: true,
                size: IggyByteSize::from_str("4GiB").unwrap(),
                bucket_capacity: 8192,
            };
            MemoryPool::init_pool(&config);
        });
    }

    #[compio::test]
    async fn test_open_creates_new_file() {
        initialize_pool_for_tests();
        let dir = tempdir().unwrap();
        let path = dir.path().join("new_file.dat");
        let path_str = path.to_str().unwrap();

        let df = DirectFile::open(path_str, 0, false).await.unwrap();

        assert!(path.exists());
        assert_eq!(df.position(), 0);
        assert_eq!(df.tail_len(), 0);
    }

    #[compio::test]
    async fn test_open_existing_file() {
        initialize_pool_for_tests();
        let dir = tempdir().unwrap();
        let path = dir.path().join("existing.dat");
        let path_str = path.to_str().unwrap();

        let _df1 = DirectFile::open(path_str, 0, false).await.unwrap();
        drop(_df1);

        // re-open as existing
        let df2 = DirectFile::open(path_str, ALIGNMENT as u64, true)
            .await
            .unwrap();

        assert_eq!(df2.position(), ALIGNMENT as u64);
    }

    #[compio::test]
    #[should_panic(expected = "initial_position must be aligned")]
    async fn test_open_unaligned_position_panics() {
        initialize_pool_for_tests();
        let dir = tempdir().unwrap();
        let path = dir.path().join("unaligned.dat");
        let _df = DirectFile::open(path.to_str().unwrap(), 1, false).await;
    }

    #[compio::test]
    async fn test_write_all_fast_path_single_block() {
        initialize_pool_for_tests();

        let dir = tempdir().unwrap();
        let path = dir.path().join("unaligned.dat");
        let path_str = path.to_str().unwrap();

        let mut df = DirectFile::open(path_str, 0, false).await.unwrap();

        let written = df.write_all(make_buffer(ALIGNMENT, 0xAB)).await.unwrap();

        assert_eq!(written, ALIGNMENT);
        assert_eq!(df.position(), ALIGNMENT as u64);
        assert_eq!(df.tail_len(), 0);
    }

    // write_all: slow path (sub-alignment / tail buffering)
    #[compio::test]
    async fn test_write_all_sub_alignment_goes_to_tail() {
        initialize_pool_for_tests();

        let dir = tempdir().unwrap();
        let path = dir.path().join("sub_align.dat");
        let path_str = path.to_str().unwrap();

        let mut df = DirectFile::open(path_str, 0, false).await.unwrap();

        let small_size = ALIGNMENT / 2;
        let written = df.write_all(make_buffer(small_size, 0x11)).await.unwrap();

        assert_eq!(written, small_size);
        assert_eq!(df.tail_len(), small_size);
        assert_eq!(df.position(), 0);
    }

    #[compio::test]
    async fn test_write_all_two_sub_alignment_writes_complete_block() {
        initialize_pool_for_tests();

        let dir = tempdir().unwrap();
        let path = dir.path().join("two_sub.dat");
        let path_str = path.to_str().unwrap();

        let mut df = DirectFile::open(path_str, 0, false).await.unwrap();

        let half = ALIGNMENT / 2;

        df.write_all(make_buffer(half, 0xAA)).await.unwrap();
        assert_eq!(df.tail_len(), half);
        assert_eq!(df.position(), 0);

        df.write_all(make_buffer(half, 0xBB)).await.unwrap();
        assert_eq!(df.tail_len(), 0);
        assert_eq!(df.position(), ALIGNMENT as u64);
    }

    #[compio::test]
    async fn test_write_all_unaligned_length_splits_correctly() {
        initialize_pool_for_tests();

        let dir = tempdir().unwrap();
        let path = dir.path().join("unaligned_len.dat");
        let path_str = path.to_str().unwrap();

        let mut df = DirectFile::open(path_str, 0, false).await.unwrap();

        // 1.5 * ALIGNMENT: one full block written, half-block in tail
        let size = ALIGNMENT + ALIGNMENT / 2;
        let written = df.write_all(make_buffer(size, 0xEE)).await.unwrap();

        assert_eq!(written, size);
        assert_eq!(df.position(), ALIGNMENT as u64);
        assert_eq!(df.tail_len(), ALIGNMENT / 2);
    }

    #[compio::test]
    async fn test_write_all_fills_existing_tail_then_writes_aligned() {
        initialize_pool_for_tests();

        let dir = tempdir().unwrap();
        let path = dir.path().join("fill_tail.dat");
        let path_str = path.to_str().unwrap();

        let mut df = DirectFile::open(path_str, 0, false).await.unwrap();

        let quarter = ALIGNMENT / 4;
        df.write_all(make_buffer(quarter, 0x11)).await.unwrap();
        assert_eq!(df.tail_len(), quarter);

        // Fill the tail + two more full blocks
        let next_size = (ALIGNMENT - quarter) + ALIGNMENT * 2;
        df.write_all(make_buffer(next_size, 0x22)).await.unwrap();

        assert_eq!(df.position(), (ALIGNMENT * 3) as u64);
        assert_eq!(df.tail_len(), 0);
    }

    #[compio::test]
    async fn test_write_all_fills_tail_with_leftover_remainder() {
        initialize_pool_for_tests();

        let dir = tempdir().unwrap();
        let path = dir.path().join("tail_leftover.dat");
        let path_str = path.to_str().unwrap();

        let mut df = DirectFile::open(path_str, 0, false).await.unwrap();

        let quarter = ALIGNMENT / 4;
        df.write_all(make_buffer(quarter, 0x11)).await.unwrap();

        let remainder = 100;
        let next_size = (ALIGNMENT - quarter) + ALIGNMENT + remainder;
        df.write_all(make_buffer(next_size, 0x22)).await.unwrap();

        assert_eq!(df.position(), (ALIGNMENT * 2) as u64);
        assert_eq!(df.tail_len(), remainder);
    }

    #[compio::test]
    async fn test_write_from_slice() {
        initialize_pool_for_tests();

        let dir = tempdir().unwrap();
        let path = dir.path().join("from_slice.dat");
        let path_str = path.to_str().unwrap();

        let mut df = DirectFile::open(path_str, 0, false).await.unwrap();

        let data = vec![0xFFu8; ALIGNMENT];
        let written = df.write_from_slice(&data).await.unwrap();

        assert_eq!(written, ALIGNMENT);
        assert_eq!(df.position(), ALIGNMENT as u64);
    }

    #[compio::test]
    async fn test_flush_empty_tail_is_noop() {
        initialize_pool_for_tests();

        let dir = tempdir().unwrap();
        let path = dir.path().join("flush_noop.dat");
        let path_str = path.to_str().unwrap();

        let mut df = DirectFile::open(path_str, 0, false).await.unwrap();
        df.flush().await.unwrap();

        assert_eq!(df.position(), 0);
        assert_eq!(df.tail_len(), 0);
    }

    #[compio::test]
    async fn test_flush_pads_and_writes_tail() {
        initialize_pool_for_tests();

        let dir = tempdir().unwrap();
        let path = dir.path().join("flush_pad.dat");
        let path_str = path.to_str().unwrap();

        let mut df = DirectFile::open(path_str, 0, false).await.unwrap();

        let small = 100;
        df.write_all(make_buffer(small, 0x55)).await.unwrap();
        assert_eq!(df.tail_len(), small);

        df.flush().await.unwrap();

        assert_eq!(df.tail_len(), 0);
        assert_eq!(df.position(), ALIGNMENT as u64);

        let on_disk = read_file_bytes(path_str);
        assert!(on_disk.len() >= ALIGNMENT);
        assert!(on_disk[..small].iter().all(|&b| b == 0x55));
        assert!(on_disk[small..ALIGNMENT].iter().all(|&b| b == 0x00));
    }

    #[compio::test]
    async fn test_write_vectored_empty_vec() {
        initialize_pool_for_tests();

        let dir = tempdir().unwrap();
        let path = dir.path().join("vec_empty.dat");
        let path_str = path.to_str().unwrap();

        let mut df = DirectFile::open(path_str, 0, false).await.unwrap();
        let written = df.write_vectored(vec![]).await.unwrap();

        assert_eq!(written, 0);
        assert_eq!(df.position(), 0);
    }

    #[compio::test]
    async fn test_write_vectored_all_aligned_buffers() {
        initialize_pool_for_tests();

        let dir = tempdir().unwrap();
        let path = dir.path().join("vec_aligned.dat");
        let path_str = path.to_str().unwrap();

        let mut df = DirectFile::open(path_str, 0, false).await.unwrap();

        let bufs = vec![
            make_buffer(ALIGNMENT, 0xAA),
            make_buffer(ALIGNMENT * 2, 0xBB),
            make_buffer(ALIGNMENT, 0xCC),
            make_buffer(ALIGNMENT, 0xDD),
        ];
        let total = ALIGNMENT * 5;

        let written = df.write_vectored(bufs).await.unwrap();

        assert_eq!(written, total);
        assert_eq!(df.position(), total as u64);
        assert_eq!(df.tail_len(), 0);
    }

    #[compio::test]
    async fn test_write_vectored_single_sub_alignment_buffer() {
        initialize_pool_for_tests();

        let dir = tempdir().unwrap();
        let path = dir.path().join("vec_sub.dat");
        let path_str = path.to_str().unwrap();

        let mut df = DirectFile::open(path_str, 0, false).await.unwrap();

        let small = 500;
        let written = df
            .write_vectored(vec![make_buffer(small, 0xDD)])
            .await
            .unwrap();

        assert_eq!(written, small);
        assert_eq!(df.position(), 0);
        assert_eq!(df.tail_len(), small);
    }

    #[compio::test]
    async fn test_write_vectored_multiple_sub_alignment_buffers_form_block() {
        initialize_pool_for_tests();

        let dir = tempdir().unwrap();
        let path = dir.path().join("vec_multi_sub.dat");
        let path_str = path.to_str().unwrap();

        let mut df = DirectFile::open(path_str, 0, false).await.unwrap();

        let quarter = ALIGNMENT / 4;
        let bufs = vec![
            make_buffer(quarter, 0x11),
            make_buffer(quarter, 0x22),
            make_buffer(quarter, 0x33),
            make_buffer(quarter, 0x44),
        ];

        let written = df.write_vectored(bufs).await.unwrap();

        assert_eq!(written, ALIGNMENT);
        assert_eq!(df.position(), ALIGNMENT as u64);
        assert_eq!(df.tail_len(), 0);
    }

    #[compio::test]
    async fn test_write_vectored_mixed_aligned_and_unaligned() {
        initialize_pool_for_tests();

        let dir = tempdir().unwrap();
        let path = dir.path().join("vec_mixed.dat");
        let path_str = path.to_str().unwrap();

        let mut df = DirectFile::open(path_str, 0, false).await.unwrap();

        let remainder = 300;
        let bufs = vec![
            make_buffer(ALIGNMENT, 0xAA),
            make_buffer(ALIGNMENT + remainder, 0xBB),
        ];
        let total = ALIGNMENT * 2 + remainder;

        let written = df.write_vectored(bufs).await.unwrap();

        assert_eq!(written, total);
        assert_eq!(df.position(), (ALIGNMENT * 2) as u64);
        assert_eq!(df.tail_len(), remainder);
    }

    #[compio::test]
    async fn test_write_vectored_with_existing_tail() {
        initialize_pool_for_tests();

        let dir = tempdir().unwrap();
        let path = dir.path().join("vec_tail.dat");
        let path_str = path.to_str().unwrap();

        let mut df = DirectFile::open(path_str, 0, false).await.unwrap();

        let seed = 100;
        df.write_all(make_buffer(seed, 0x11)).await.unwrap();
        assert_eq!(df.tail_len(), seed);

        let fill = ALIGNMENT - seed;
        let bufs = vec![make_buffer(fill, 0x22), make_buffer(ALIGNMENT, 0x33)];

        let written = df.write_vectored(bufs).await.unwrap();

        assert_eq!(written, fill + ALIGNMENT);
        assert_eq!(df.position(), (ALIGNMENT * 2) as u64);
        assert_eq!(df.tail_len(), 0);
    }

    #[compio::test]
    async fn test_data_integrity_simple_write_and_flush() {
        initialize_pool_for_tests();

        let dir = tempdir().unwrap();
        let path = dir.path().join("integrity.dat");
        let path_str = path.to_str().unwrap();

        let mut df = DirectFile::open(path_str, 0, false).await.unwrap();

        let pattern: Vec<u8> = (0..200u8).collect();
        df.write_from_slice(&pattern).await.unwrap();
        df.flush().await.unwrap();

        let on_disk = read_file_bytes(path_str);
        assert_eq!(&on_disk[..pattern.len()], &pattern[..]);
    }

    #[compio::test]
    async fn test_data_integrity_multiple_sequential_writes() {
        initialize_pool_for_tests();

        let dir = tempdir().unwrap();
        let path = dir.path().join("seq_integrity.dat");
        let path_str = path.to_str().unwrap();

        let mut df = DirectFile::open(path_str, 0, false).await.unwrap();

        df.write_all(make_buffer(ALIGNMENT, 0xAA)).await.unwrap();
        df.write_all(make_buffer(ALIGNMENT, 0xBB)).await.unwrap();
        df.write_all(make_buffer(ALIGNMENT, 0xCC)).await.unwrap();

        let on_disk = read_file_bytes(path_str);

        assert!(on_disk[..ALIGNMENT].iter().all(|&b| b == 0xAA));
        assert!(on_disk[ALIGNMENT..ALIGNMENT * 2].iter().all(|&b| b == 0xBB));
        assert!(
            on_disk[ALIGNMENT * 2..ALIGNMENT * 3]
                .iter()
                .all(|&b| b == 0xCC)
        );
    }

    #[compio::test]
    async fn test_position_tracking_mixed_operations() {
        initialize_pool_for_tests();

        let dir = tempdir().unwrap();
        let path = dir.path().join("pos_track.dat");
        let path_str = path.to_str().unwrap();

        let mut df = DirectFile::open(path_str, 0, false).await.unwrap();
        assert_eq!(df.position(), 0);

        // Aligned write
        df.write_all(make_buffer(ALIGNMENT, 0xAA)).await.unwrap();
        assert_eq!(df.position(), ALIGNMENT as u64);

        // Sub-alignment write
        let small = 200;
        df.write_all(make_buffer(small, 0xBB)).await.unwrap();
        assert_eq!(df.position(), ALIGNMENT as u64);

        // Flush advances position by one block
        df.flush().await.unwrap();
        assert_eq!(df.position(), (ALIGNMENT * 2) as u64);

        // Vectored write of 3 aligned blocks
        let bufs = vec![
            make_buffer(ALIGNMENT, 0xCC),
            make_buffer(ALIGNMENT, 0xDD),
            make_buffer(ALIGNMENT, 0xEE),
        ];
        df.write_vectored(bufs).await.unwrap();
        assert_eq!(df.position(), (ALIGNMENT * 5) as u64);
    }

    // Some edge cases

    #[compio::test]
    async fn test_write_exactly_one_byte() {
        initialize_pool_for_tests();
        let dir = tempdir().unwrap();
        let path = dir.path().join("one_byte.dat");
        let path_str = path.to_str().unwrap();

        let mut df = DirectFile::open(path_str, 0, false).await.unwrap();

        let written = df.write_all(make_buffer(1, 0xFF)).await.unwrap();
        assert_eq!(written, 1);
        assert_eq!(df.tail_len(), 1);
        assert_eq!(df.position(), 0);
    }

    #[compio::test]
    async fn test_write_vectored_many_tiny_buffers() {
        initialize_pool_for_tests();

        let dir = tempdir().unwrap();
        let path = dir.path().join("many_tiny.dat");
        let path_str = path.to_str().unwrap();

        let mut df = DirectFile::open(path_str, 0, false).await.unwrap();

        // ALIGNMENT single-byte buffers → one aligned block
        let bufs: Vec<Bytes> = (0..ALIGNMENT)
            .map(|i| make_buffer(1, (i % 256) as u8))
            .collect();

        let written = df.write_vectored(bufs).await.unwrap();

        assert_eq!(written, ALIGNMENT);
        assert_eq!(df.position(), ALIGNMENT as u64);
        assert_eq!(df.tail_len(), 0);
    }

    #[compio::test]
    async fn test_flush_then_continue_writing() {
        initialize_pool_for_tests();

        let dir = tempdir().unwrap();
        let path = dir.path().join("flush_continue.dat");
        let path_str = path.to_str().unwrap();

        let mut df = DirectFile::open(path_str, 0, false).await.unwrap();

        df.write_all(make_buffer(100, 0xAA)).await.unwrap();
        df.flush().await.unwrap();
        assert_eq!(df.position(), ALIGNMENT as u64);

        df.write_all(make_buffer(ALIGNMENT, 0xBB)).await.unwrap();
        assert_eq!(df.position(), (ALIGNMENT * 2) as u64);

        let on_disk = read_file_bytes(path_str);
        assert!(on_disk[ALIGNMENT..ALIGNMENT * 2].iter().all(|&b| b == 0xBB));
    }

    #[compio::test]
    async fn test_write_vectored_data_integrity() {
        initialize_pool_for_tests();

        let dir = tempdir().unwrap();
        let path = dir.path().join("vec_integrity.dat");
        let path_str = path.to_str().unwrap();

        let mut df = DirectFile::open(path_str, 0, false).await.unwrap();

        let bufs = vec![make_buffer(ALIGNMENT, 0xAA), make_buffer(ALIGNMENT, 0xBB)];

        df.write_vectored(bufs).await.unwrap();

        let on_disk = read_file_bytes(path_str);
        assert!(on_disk[..ALIGNMENT].iter().all(|&b| b == 0xAA));
        assert!(on_disk[ALIGNMENT..ALIGNMENT * 2].iter().all(|&b| b == 0xBB));
    }

    #[compio::test]
    async fn test_large_unaligned_write_all() {
        initialize_pool_for_tests();

        let dir = tempdir().unwrap();
        let path = dir.path().join("large_unaligned.dat");
        let path_str = path.to_str().unwrap();

        let mut df = DirectFile::open(path_str, 0, false).await.unwrap();

        let size = ALIGNMENT * 10 + 137;
        let written = df.write_all(make_buffer(size, 0xDD)).await.unwrap();

        assert_eq!(written, size);
        assert_eq!(df.position(), (ALIGNMENT * 10) as u64);
        assert_eq!(df.tail_len(), 137);

        df.flush().await.unwrap();

        let on_disk = read_file_bytes(path_str);
        assert!(on_disk[..size].iter().all(|&b| b == 0xDD));
    }
}
