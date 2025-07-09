/* Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

use crate::streaming::utils::{ALIGNMENT, PooledBuffer};
use compio::buf::IoBuf;
use compio::fs::{File, OpenOptions};
use compio::io::AsyncWriteAtExt;
use error_set::ErrContext;
use iggy_common::IggyError;

const O_DIRECT: i32 = 0x4000;
const O_DSYNC: i32 = 0x1000;
const SCRATCH_SIZE: usize = ALIGNMENT * 8;
/// Cache line padding to prevent false sharing
/// Most modern CPUs have 64-byte cache lines
#[repr(align(64))]
#[derive(Debug)]
struct Padded<T>(T);

/// Stack-allocated scratch buffer for small writes
/// Aligned to 4KiB for optimal performance
#[repr(align(4096))]
#[derive(Debug)]
struct ScratchBuffer([u8; SCRATCH_SIZE]);

/// A wrapper that allows us to pass a slice of ScratchBuffer as IoBuf
/// This is safe because DirectFile owns the ScratchBuffer for its entire lifetime
struct ScratchSlice {
    ptr: *const u8,
    len: usize,
}

// SAFETY: ScratchSlice is only created from DirectFile's scratch buffer,
// which lives for the entire lifetime of DirectFile. The async I/O operation
// completes before any method returns, ensuring the buffer remains valid.
unsafe impl Send for ScratchSlice {}
unsafe impl Sync for ScratchSlice {}

unsafe impl IoBuf for ScratchSlice {
    fn as_buf_ptr(&self) -> *const u8 {
        self.ptr
    }

    fn buf_len(&self) -> usize {
        self.len
    }

    fn buf_capacity(&self) -> usize {
        self.len
    }
}

#[derive(Debug)]
pub struct DirectFile {
    file_path: String,
    file: File,
    file_position: u64,
    tail: PooledBuffer,
    tail_len: Padded<usize>, // Padded to avoid false sharing on hot path
    spare: PooledBuffer,     // Reusable buffer for carry-over in write_vectored
    scratch: ScratchBuffer,  // Stack-allocated scratch buffer for small writes
}

impl DirectFile {
    pub async fn open(
        file_path: &str,
        initial_position: u64,
        file_exists: bool,
    ) -> Result<Self, IggyError> {
        let mut file = OpenOptions::new()
            .create(true)
            .write(true)
            .custom_flags(O_DIRECT | O_DSYNC)
            .open(file_path)
            .await
            .with_error_context(|err| {
                format!("Failed to open file with O_DIRECT: {file_path}, error: {err}")
            })
            .map_err(|_| IggyError::CannotReadFile)?;

        if !file_exists {
            let init_buffer = PooledBuffer::with_capacity(ALIGNMENT);
            let (write_result, _) = file.write_all_at(init_buffer, 0).await.into();
            write_result
                .with_error_context(|error| {
                    tracing::error!(
                        "Failed to initialize file with dummy block: {file_path}, error: {error}"
                    );
                    format!(
                        "Failed to initialize file with dummy block: {file_path}, error: {error}"
                    )
                })
                .map_err(|_| IggyError::CannotWriteToFile)?;

            tracing::trace!("Successfully initialized new file with dummy block: {file_path}");
        }

        tracing::trace!(
            "Successfully opened DirectFile: {}, position: {}, exists: {}",
            file_path,
            initial_position,
            file_exists
        );

        Ok(Self {
            file_path: file_path.to_string(),
            file,
            file_position: initial_position,
            tail: PooledBuffer::with_capacity(ALIGNMENT),
            tail_len: Padded(0),
            spare: PooledBuffer::with_capacity(ALIGNMENT),
            scratch: ScratchBuffer([0u8; SCRATCH_SIZE]),
        })
    }

    pub async fn get_file_size(&self) -> Result<u64, IggyError> {
        self.file
            .metadata()
            .await
            .with_error_context(|error| {
                format!(
                    "Failed to get metadata of file: {}, error: {error}",
                    self.file_path
                )
            })
            .map_err(|_| IggyError::CannotReadFileMetadata)
            .map(|metadata| metadata.len())
    }

    /// Write data from an owned PooledBuffer with zero-copy optimization
    ///
    /// This method takes ownership of the buffer to satisfy the 'static lifetime
    /// requirement for async I/O operations. The buffer ownership is passed directly
    /// to the kernel for DMA operations, ensuring sound memory management without
    /// requiring unsafe code or risking use-after-free bugs.
    ///
    /// # Memory Model
    /// - Takes ownership of `PooledBuffer` (already aligned and pooled)
    /// - Passes buffer directly to kernel via compio (zero-copy for aligned data)
    /// - Returns buffer to pool automatically when dropped
    ///
    /// # Performance
    /// - Zero-copy for aligned portions (direct DMA transfer)
    /// - Single memcpy only for tail data (< 512 bytes per call)
    pub async fn write_all(&mut self, buffer: PooledBuffer) -> Result<usize, IggyError> {
        let initial_len = buffer.len();
        tracing::trace!(
            "DirectFile write_all called for file: {}, data_len: {}, position: {}, tail_len: {}",
            self.file_path,
            initial_len,
            self.file_position,
            self.tail_len.0
        );

        // Fast path: no tail data and buffer is perfectly aligned
        if self.tail_len.0 == 0 && buffer.len() % ALIGNMENT == 0 {
            // Direct write with owned buffer - zero-copy all the way!
            let (result, _) = self
                .file
                .write_all_at(buffer, self.file_position)
                .await
                .into();

            result.map_err(|e| {
                tracing::error!(
                    "Failed to write to direct file: {} at position {}, buffer_len: {}, error: {}",
                    self.file_path,
                    self.file_position,
                    initial_len,
                    e
                );
                IggyError::CannotWriteToFile
            })?;

            self.file_position += initial_len as u64;
            return Ok(initial_len);
        }

        // Slower path: need to handle tail and/or unaligned data
        let mut written = 0;

        // Handle tail data first
        if self.tail_len.0 > 0 {
            let need = ALIGNMENT - self.tail_len.0;
            let to_copy = need.min(buffer.len());
            self.tail.extend_from_slice(&buffer[..to_copy]);
            self.tail_len.0 += to_copy;
            written += to_copy;

            if self.tail_len.0 == ALIGNMENT {
                self.flush_tail().await?;
            }

            // If we consumed the entire buffer, we're done
            if written >= buffer.len() {
                return Ok(initial_len);
            }
        }

        // Calculate aligned portion
        let remaining = buffer.len() - written;
        let aligned_len = remaining & !(ALIGNMENT - 1);

        if aligned_len > 0 {
            // Optimization: use scratch buffer for small writes to avoid allocation
            if aligned_len <= self.scratch.0.len() {
                // Copy to scratch buffer (hot in L1 cache)
                self.scratch.0[..aligned_len]
                    .copy_from_slice(&buffer[written..written + aligned_len]);
                written += aligned_len;

                // Create a slice wrapper for the scratch buffer
                let scratch_slice = ScratchSlice {
                    ptr: self.scratch.0.as_ptr(),
                    len: aligned_len,
                };

                // Direct write with scratch buffer - no allocation!
                let (result, _) = self
                    .file
                    .write_all_at(scratch_slice, self.file_position)
                    .await
                    .into();

                result.map_err(|e| {
                    tracing::error!(
                        "Failed to write to direct file: {} at position {}, buffer_len: {}, error: {}",
                        self.file_path,
                        self.file_position,
                        aligned_len,
                        e
                    );
                    IggyError::CannotWriteToFile
                })?;
            } else {
                // For larger writes, allocate a new buffer
                let mut aligned_buffer = PooledBuffer::with_capacity(aligned_len);
                aligned_buffer.extend_from_slice(&buffer[written..written + aligned_len]);
                written += aligned_len;

                // Direct write with owned buffer
                let (result, _) = self
                    .file
                    .write_all_at(aligned_buffer, self.file_position)
                    .await
                    .into();

                result.map_err(|e| {
                    tracing::error!(
                        "Failed to write to direct file: {} at position {}, buffer_len: {}, error: {}",
                        self.file_path,
                        self.file_position,
                        aligned_len,
                        e
                    );
                    IggyError::CannotWriteToFile
                })?;
            }

            self.file_position += aligned_len as u64;
        }

        // Store any remainder in tail
        let remainder = &buffer[written..];
        if !remainder.is_empty() {
            self.tail.clear();
            self.tail.extend_from_slice(remainder);
            self.tail_len.0 = remainder.len();
        }

        Ok(initial_len)
    }

    /// Convenience wrapper for callers that have `&[u8]` instead of owned buffers
    ///
    /// This method allocates a new PooledBuffer and copies the data once.
    /// For hot paths, prefer using `write_all` directly with owned PooledBuffers.
    ///
    /// # Performance Note
    /// This incurs one memcpy to create the PooledBuffer. The copy happens in
    /// userspace where caches are hot, then the buffer is passed zero-copy to kernel.
    pub async fn write_from_slice(&mut self, data: &[u8]) -> Result<usize, IggyError> {
        let mut buffer = PooledBuffer::with_capacity(data.len());
        buffer.extend_from_slice(data);
        self.write_all(buffer).await
    }

    pub async fn flush(&mut self) -> Result<(), IggyError> {
        if self.tail_len.0 > 0 {
            self.tail.resize(ALIGNMENT, 0);
            self.flush_tail().await?;
        }
        Ok(())
    }

    pub fn position(&self) -> u64 {
        self.file_position
    }

    pub fn tail_len(&self) -> usize {
        self.tail_len.0
    }

    pub fn file_path(&self) -> &str {
        &self.file_path
    }

    /// Write multiple buffers using vectored I/O with ownership transfer
    ///
    /// This method takes ownership of all buffers to satisfy the 'static lifetime
    /// requirement. Each buffer is consumed and passed to the kernel for DMA.
    ///
    /// # Memory Model
    /// - Takes ownership of Vec<PooledBuffer> - all buffers are consumed
    /// - Aligns data at buffer boundaries with minimal copying (max ALIGNMENT bytes)
    /// - Uses spare buffer to avoid allocations for boundary alignment
    ///
    /// # Performance
    /// - Single syscall for multiple buffers (vectored I/O)
    /// - Only copies at buffer boundaries for alignment (worst case: ALIGNMENT bytes per buffer)
    /// - Reuses spare buffer across calls to avoid allocations
    pub async fn write_vectored(&mut self, buffers: Vec<PooledBuffer>) -> Result<usize, IggyError> {
        if buffers.is_empty() {
            return Ok(0);
        }

        let mut total_logical_size = 0usize;
        let mut write_buffers = Vec::with_capacity(buffers.len() * 2); // Worst case: each buffer splits

        // Initialize carry-over tracking
        let mut carry_over_len = 0usize;

        // If we have existing tail data, move it to spare buffer
        if self.tail_len.0 > 0 {
            tracing::trace!(
                "write_vectored: moving {} tail bytes to spare buffer",
                self.tail_len.0
            );
            self.spare.clear();
            self.spare.extend_from_slice(&self.tail[..self.tail_len.0]);
            carry_over_len = self.tail_len.0;
            self.tail_len.0 = 0;
        }

        for buffer in buffers {
            let buffer_len = buffer.len();
            total_logical_size += buffer_len;

            if carry_over_len > 0 {
                // We have data from previous buffer that needs to be combined
                let need = ALIGNMENT - carry_over_len;

                if buffer_len >= need {
                    // Complete the alignment by copying just what we need
                    self.spare.extend_from_slice(&buffer[..need]);

                    // Create a new buffer from spare and push it
                    write_buffers.push(std::mem::replace(
                        &mut self.spare,
                        PooledBuffer::with_capacity(ALIGNMENT),
                    ));

                    // Clear spare for next use
                    self.spare.clear();
                    carry_over_len = 0;

                    // Process the rest of this buffer
                    let remaining = buffer_len - need;
                    let aligned_size = remaining & !(ALIGNMENT - 1);

                    if aligned_size > 0 {
                        // Check if we can use the whole remaining buffer
                        if aligned_size == remaining {
                            // We can use the slice without remainder
                            if need == 0 {
                                // No slicing needed, use the whole buffer
                                write_buffers.push(buffer);
                            } else {
                                // Need to slice from the beginning
                                let mut aligned_buffer = PooledBuffer::with_capacity(remaining);
                                aligned_buffer.extend_from_slice(&buffer[need..]);
                                write_buffers.push(aligned_buffer);
                            }
                        } else {
                            // We need to split: save remainder to spare
                            self.spare.extend_from_slice(&buffer[need + aligned_size..]);
                            carry_over_len = remaining - aligned_size;

                            // Create a new buffer with just the aligned portion
                            let mut aligned_buffer = PooledBuffer::with_capacity(aligned_size);
                            aligned_buffer.extend_from_slice(&buffer[need..need + aligned_size]);
                            write_buffers.push(aligned_buffer);
                        }
                    } else {
                        // All remaining data goes to spare
                        self.spare.extend_from_slice(&buffer[need..]);
                        carry_over_len = remaining;
                    }
                } else {
                    // This buffer is too small to complete alignment
                    self.spare.extend_from_slice(&buffer);
                    carry_over_len += buffer_len;
                }
            } else {
                // No carry over, process this buffer directly
                let aligned_size = buffer_len & !(ALIGNMENT - 1);

                if aligned_size > 0 {
                    if aligned_size == buffer_len {
                        // Use the whole buffer
                        write_buffers.push(buffer);
                    } else {
                        // Split the buffer: save remainder to spare
                        self.spare.clear();
                        self.spare.extend_from_slice(&buffer[aligned_size..]);
                        carry_over_len = buffer_len - aligned_size;

                        // Create aligned buffer
                        let mut aligned_buffer = PooledBuffer::with_capacity(aligned_size);
                        aligned_buffer.extend_from_slice(&buffer[..aligned_size]);
                        write_buffers.push(aligned_buffer);
                    }
                } else {
                    // Entire buffer goes to spare
                    self.spare.clear();
                    self.spare.extend_from_slice(&buffer);
                    carry_over_len = buffer_len;
                }
            }
        }

        // Perform the vectored write if we have aligned buffers
        if !write_buffers.is_empty() {
            let bytes_written: usize = write_buffers.iter().map(|b| b.len()).sum();

            tracing::trace!(
                "write_vectored: writing {} buffers totaling {} bytes at position {}",
                write_buffers.len(),
                bytes_written,
                self.file_position
            );

            let (result, _) = self
                .file
                .write_vectored_all_at(write_buffers, self.file_position)
                .await
                .into();

            result.map_err(|e| {
                tracing::error!("Failed to write vectored data to {}: {}", self.file_path, e);
                IggyError::CannotWriteToFile
            })?;

            self.file_position += bytes_written as u64;
        }

        // Store any remainder from spare to tail buffer
        if carry_over_len > 0 {
            self.tail.clear();
            self.tail.extend_from_slice(&self.spare[..carry_over_len]);
            self.tail_len.0 = carry_over_len;
        }

        Ok(total_logical_size)
    }

    pub fn tail_buffer(&self) -> &PooledBuffer {
        &self.tail
    }

    pub fn take_tail(&mut self) -> (PooledBuffer, usize) {
        let tail = std::mem::replace(&mut self.tail, PooledBuffer::with_capacity(ALIGNMENT));
        let tail_len = self.tail_len.0;
        self.tail_len.0 = 0;
        (tail, tail_len)
    }

    pub fn set_tail(&mut self, tail: PooledBuffer, tail_len: usize) {
        self.tail = tail;
        self.tail_len.0 = tail_len;
    }

    async fn flush_tail(&mut self) -> Result<(), IggyError> {
        assert_eq!(self.tail.len(), ALIGNMENT);

        let tail_buffer = std::mem::replace(&mut self.tail, PooledBuffer::with_capacity(ALIGNMENT));

        let (result, returned_buf) = self
            .file
            .write_all_at(tail_buffer, self.file_position)
            .await
            .into();

        result.map_err(|e| {
            tracing::error!(
                "Failed to flush tail for file: {} at position {}, tail size: {}, error: {}",
                self.file_path,
                self.file_position,
                ALIGNMENT,
                e
            );
            IggyError::CannotWriteToFile
        })?;

        self.file_position += ALIGNMENT as u64;
        self.tail_len.0 = 0;
        self.tail = returned_buf;
        self.tail.clear();
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::configs::system::SystemConfig;
    use crate::streaming::utils::MemoryPool;
    use compio::fs::OpenOptions;
    use compio::io::{AsyncReadAt, AsyncReadAtExt};
    use std::sync::Arc;
    use tempfile::tempdir;

    #[test]
    fn test_direct_file_small_writes() {
        compio::runtime::Runtime::new().unwrap().block_on(async {
            MemoryPool::init_pool(Arc::new(SystemConfig::default()));
            let temp_dir = tempdir().unwrap();
            let file_path = temp_dir.path().join("test_direct_io.bin");

            let mut direct_file = DirectFile::open(file_path.to_str().unwrap(), 0, false)
                .await
                .unwrap();

            for i in 0..10u64 {
                let buf = i.to_le_bytes();
                direct_file
                    .write_all(PooledBuffer::from(&buf[..]))
                    .await
                    .unwrap();
            }

            direct_file.flush().await.unwrap();

            let file = OpenOptions::new()
                .read(true)
                .custom_flags(0x4000)
                .open(&file_path)
                .await
                .unwrap();

            let mut read_buffer = vec![0u8; 512];
            let (result, buf) = file.read_at(read_buffer, 0).await.into();
            result.unwrap();
            read_buffer = buf;

            for i in 0..10u64 {
                let start = i as usize * 8;
                let num = u64::from_le_bytes(read_buffer[start..start + 8].try_into().unwrap());
                assert_eq!(num, i, "Expected {} at position {}, got {}", i, start, num);
            }
        });
    }

    #[test]
    fn test_direct_file_exact_sector_write() {
        compio::runtime::Runtime::new().unwrap().block_on(async {
            MemoryPool::init_pool(Arc::new(SystemConfig::default()));
            let temp_dir = tempdir().unwrap();
            let file_path = temp_dir.path().join("test_exact_sector.bin");

            let mut direct_file = DirectFile::open(file_path.to_str().unwrap(), 0, false)
                .await
                .unwrap();

            let data = vec![42u8; ALIGNMENT];
            direct_file
                .write_all(PooledBuffer::from(data.as_slice()))
                .await
                .unwrap();

            assert_eq!(direct_file.tail_len(), 0);
            assert_eq!(direct_file.position(), ALIGNMENT as u64);

            let file = OpenOptions::new()
                .read(true)
                .custom_flags(0x4000)
                .open(&file_path)
                .await
                .unwrap();

            let mut read_buffer = vec![0u8; ALIGNMENT];
            let (result, buf) = file.read_at(read_buffer, 0).await.into();
            result.unwrap();
            read_buffer = buf;

            assert_eq!(read_buffer, vec![42u8; ALIGNMENT]);
        });
    }

    #[test]
    fn test_direct_file_multiple_sector_writes() {
        compio::runtime::Runtime::new().unwrap().block_on(async {
            MemoryPool::init_pool(Arc::new(SystemConfig::default()));
            let temp_dir = tempdir().unwrap();
            let file_path = temp_dir.path().join("test_multiple_sectors.bin");

            let mut direct_file = DirectFile::open(file_path.to_str().unwrap(), 0, false)
                .await
                .unwrap();

            let data1 = vec![1u8; ALIGNMENT * 2];
            let data2 = vec![2u8; ALIGNMENT * 3];
            let data3 = vec![3u8; ALIGNMENT];

            direct_file
                .write_all(PooledBuffer::from(data1.as_slice()))
                .await
                .unwrap();
            direct_file
                .write_all(PooledBuffer::from(data2.as_slice()))
                .await
                .unwrap();
            direct_file
                .write_all(PooledBuffer::from(data3.as_slice()))
                .await
                .unwrap();

            let file = OpenOptions::new()
                .read(true)
                .custom_flags(0x4000)
                .open(&file_path)
                .await
                .unwrap();

            let mut read_buffer = vec![0u8; ALIGNMENT * 6];
            let (result, buf) = file.read_at(read_buffer, 0).await.into();
            result.unwrap();
            read_buffer = buf;

            assert_eq!(&read_buffer[0..ALIGNMENT * 2], &vec![1u8; ALIGNMENT * 2]);
            assert_eq!(
                &read_buffer[ALIGNMENT * 2..ALIGNMENT * 5],
                &vec![2u8; ALIGNMENT * 3]
            );
            assert_eq!(
                &read_buffer[ALIGNMENT * 5..ALIGNMENT * 6],
                &vec![3u8; ALIGNMENT]
            );
        });
    }

    #[test]
    fn test_direct_file_unaligned_write() {
        compio::runtime::Runtime::new().unwrap().block_on(async {
            MemoryPool::init_pool(Arc::new(SystemConfig::default()));
            let temp_dir = tempdir().unwrap();
            let file_path = temp_dir.path().join("test_unaligned.bin");

            let mut direct_file = DirectFile::open(file_path.to_str().unwrap(), 0, false)
                .await
                .unwrap();

            let data = vec![77u8; 1000];
            direct_file
                .write_all(PooledBuffer::from(data.as_slice()))
                .await
                .unwrap();

            assert_eq!(direct_file.tail_len(), 1000 % ALIGNMENT);
            assert_eq!(direct_file.position(), ALIGNMENT as u64);

            direct_file.flush().await.unwrap();

            assert_eq!(direct_file.tail_len(), 0);
            assert_eq!(direct_file.position(), (ALIGNMENT * 2) as u64);

            let file = OpenOptions::new()
                .read(true)
                .custom_flags(0x4000)
                .open(&file_path)
                .await
                .unwrap();

            let mut read_buffer = vec![0u8; ALIGNMENT * 2];
            let (result, buf) = file.read_at(read_buffer, 0).await.into();
            result.unwrap();
            read_buffer = buf;

            assert_eq!(&read_buffer[0..1000], &vec![77u8; 1000]);
            assert_eq!(
                &read_buffer[1000..ALIGNMENT * 2],
                &vec![0u8; ALIGNMENT * 2 - 1000]
            );
        });
    }

    #[test]
    fn test_direct_file_cross_sector_boundary() {
        compio::runtime::Runtime::new().unwrap().block_on(async {
            MemoryPool::init_pool(Arc::new(SystemConfig::default()));
            let temp_dir = tempdir().unwrap();
            let file_path = temp_dir.path().join("test_cross_boundary.bin");

            let mut direct_file = DirectFile::open(file_path.to_str().unwrap(), 0, false)
                .await
                .unwrap();

            let data1 = vec![1u8; 400];
            let data2 = vec![2u8; 200];
            let data3 = vec![3u8; 100];

            direct_file
                .write_all(PooledBuffer::from(data1.as_slice()))
                .await
                .unwrap();
            direct_file
                .write_all(PooledBuffer::from(data2.as_slice()))
                .await
                .unwrap();
            direct_file
                .write_all(PooledBuffer::from(data3.as_slice()))
                .await
                .unwrap();

            assert_eq!(direct_file.tail_len(), 700 % ALIGNMENT);

            direct_file.flush().await.unwrap();

            let file = OpenOptions::new()
                .read(true)
                .custom_flags(0x4000)
                .open(&file_path)
                .await
                .unwrap();

            let mut read_buffer = vec![0u8; ALIGNMENT * 2];
            let (result, buf) = file.read_at(read_buffer, 0).await.into();
            result.unwrap();
            read_buffer = buf;

            assert_eq!(&read_buffer[0..400], &vec![1u8; 400]);
            assert_eq!(&read_buffer[400..600], &vec![2u8; 200]);
            assert_eq!(&read_buffer[600..700], &vec![3u8; 100]);
        });
    }

    #[test]
    fn test_direct_file_vectored_write() {
        compio::runtime::Runtime::new().unwrap().block_on(async {
            MemoryPool::init_pool(Arc::new(SystemConfig::default()));
            let temp_dir = tempdir().unwrap();
            let file_path = temp_dir.path().join("test_vectored.bin");

            let mut direct_file = DirectFile::open(file_path.to_str().unwrap(), 0, false)
                .await
                .unwrap();

            // Create multiple unaligned buffers
            let buffers = vec![
                PooledBuffer::from(vec![1u8; 300].as_slice()),
                PooledBuffer::from(vec![2u8; 400].as_slice()),
                PooledBuffer::from(vec![3u8; 324].as_slice()),
            ];

            let written = direct_file.write_vectored(buffers).await.unwrap();
            assert_eq!(written, 1024);

            // Should have data in tail
            assert_eq!(direct_file.tail_len(), 1024 % ALIGNMENT);

            direct_file.flush().await.unwrap();

            let file = OpenOptions::new()
                .read(true)
                .custom_flags(0x4000)
                .open(&file_path)
                .await
                .unwrap();

            let mut read_buffer = vec![0u8; ALIGNMENT * 2];
            let (result, buf) = file.read_at(read_buffer, 0).await.into();
            result.unwrap();
            read_buffer = buf;

            assert_eq!(&read_buffer[0..300], &vec![1u8; 300]);
            assert_eq!(&read_buffer[300..700], &vec![2u8; 400]);
            assert_eq!(&read_buffer[700..1024], &vec![3u8; 324]);
        });
    }

    #[test]
    fn test_direct_file_vectored_aligned_buffers() {
        compio::runtime::Runtime::new().unwrap().block_on(async {
            MemoryPool::init_pool(Arc::new(SystemConfig::default()));
            let temp_dir = tempdir().unwrap();
            let file_path = temp_dir.path().join("test_vectored_aligned.bin");

            let mut direct_file = DirectFile::open(file_path.to_str().unwrap(), 0, false)
                .await
                .unwrap();

            // Create multiple aligned buffers
            let buffers = vec![
                PooledBuffer::from(vec![1u8; ALIGNMENT].as_slice()),
                PooledBuffer::from(vec![2u8; ALIGNMENT * 2].as_slice()),
                PooledBuffer::from(vec![3u8; ALIGNMENT].as_slice()),
            ];

            let written = direct_file.write_vectored(buffers).await.unwrap();
            assert_eq!(written, ALIGNMENT * 4);

            // Should have no tail
            assert_eq!(direct_file.tail_len(), 0);
            assert_eq!(direct_file.position(), (ALIGNMENT * 4) as u64);

            let file = OpenOptions::new()
                .read(true)
                .custom_flags(0x4000)
                .open(&file_path)
                .await
                .unwrap();

            let mut read_buffer = vec![0u8; ALIGNMENT * 4];
            let (result, buf) = file.read_at(read_buffer, 0).await.into();
            result.unwrap();
            read_buffer = buf;

            assert_eq!(&read_buffer[0..ALIGNMENT], &vec![1u8; ALIGNMENT]);
            assert_eq!(
                &read_buffer[ALIGNMENT..ALIGNMENT * 3],
                &vec![2u8; ALIGNMENT * 2]
            );
            assert_eq!(
                &read_buffer[ALIGNMENT * 3..ALIGNMENT * 4],
                &vec![3u8; ALIGNMENT]
            );
        });
    }

    #[test]
    fn test_direct_file_alignment_minus_one_plus_double_alignment_plus_seven() {
        compio::runtime::Runtime::new().unwrap().block_on(async {
            MemoryPool::init_pool(Arc::new(SystemConfig::default()));
            let temp_dir = tempdir().unwrap();
            let file_path = temp_dir.path().join("test_alignment_edge_case.bin");

            let mut direct_file = DirectFile::open(file_path.to_str().unwrap(), 0, false)
                .await
                .unwrap();

            // First write: ALIGNMENT - 1 bytes
            let data1 = vec![0xAA; ALIGNMENT - 1];
            direct_file
                .write_all(PooledBuffer::from(data1.as_slice()))
                .await
                .unwrap();

            // Should have ALIGNMENT - 1 bytes in tail
            assert_eq!(direct_file.tail_len(), ALIGNMENT - 1);
            assert_eq!(direct_file.position(), 0);

            // Second write: 2 * ALIGNMENT + 7 bytes
            let data2 = vec![0xBB; 2 * ALIGNMENT + 7];
            direct_file
                .write_all(PooledBuffer::from(data2.as_slice()))
                .await
                .unwrap();

            // After this write:
            // - First byte from data2 completes the first sector (tail was ALIGNMENT-1, needed 1 more)
            // - Next 2*ALIGNMENT bytes fill exactly 2 sectors
            // - Last 6 bytes go to tail (2*ALIGNMENT + 7 - 1 - 2*ALIGNMENT = 6)
            assert_eq!(direct_file.tail_len(), 6);
            assert_eq!(direct_file.position(), 3 * ALIGNMENT as u64);

            // Flush to write the tail
            direct_file.flush().await.unwrap();

            assert_eq!(direct_file.tail_len(), 0);
            assert_eq!(direct_file.position(), 4 * ALIGNMENT as u64);

            // Verify the written data
            let file = OpenOptions::new()
                .read(true)
                .custom_flags(0x4000)
                .open(&file_path)
                .await
                .unwrap();

            let mut read_buffer = vec![0u8; 4 * ALIGNMENT];
            let (result, buf) = file.read_at(read_buffer, 0).await.into();
            result.unwrap();
            read_buffer = buf;

            // First ALIGNMENT - 1 bytes should be 0xAA
            assert_eq!(&read_buffer[0..ALIGNMENT - 1], &vec![0xAA; ALIGNMENT - 1]);

            // Next 2 * ALIGNMENT + 7 bytes should be 0xBB
            assert_eq!(
                &read_buffer[ALIGNMENT - 1..(ALIGNMENT - 1) + (2 * ALIGNMENT + 7)],
                &vec![0xBB; 2 * ALIGNMENT + 7]
            );

            // Remaining bytes should be 0 (padding)
            let total_written = (ALIGNMENT - 1) + (2 * ALIGNMENT + 7);
            assert_eq!(
                &read_buffer[total_written..4 * ALIGNMENT],
                &vec![0u8; 4 * ALIGNMENT - total_written]
            );
        });
    }

    #[test]
    fn test_direct_file_vectored_alignment_minus_one_plus_double_alignment_plus_seven() {
        compio::runtime::Runtime::new().unwrap().block_on(async {
            MemoryPool::init_pool(Arc::new(SystemConfig::default()));
            let temp_dir = tempdir().unwrap();
            let file_path = temp_dir
                .path()
                .join("test_vectored_alignment_edge_case.bin");

            let mut direct_file = DirectFile::open(file_path.to_str().unwrap(), 0, false)
                .await
                .unwrap();

            // Create buffers for vectored write
            let buffers = vec![
                PooledBuffer::from(vec![0xAA; ALIGNMENT - 1].as_slice()),
                PooledBuffer::from(vec![0xBB; 2 * ALIGNMENT + 7].as_slice()),
            ];

            let written = direct_file.write_vectored(buffers).await.unwrap();
            assert_eq!(written, (ALIGNMENT - 1) + (2 * ALIGNMENT + 7));

            // After vectored write, should have 6 bytes in tail
            assert_eq!(direct_file.tail_len(), 6);
            assert_eq!(direct_file.position(), 3 * ALIGNMENT as u64);

            // Flush to write the tail
            direct_file.flush().await.unwrap();

            assert_eq!(direct_file.tail_len(), 0);
            assert_eq!(direct_file.position(), 4 * ALIGNMENT as u64);

            // Verify the written data
            let file = OpenOptions::new()
                .read(true)
                .custom_flags(0x4000)
                .open(&file_path)
                .await
                .unwrap();

            let mut read_buffer = vec![0u8; 4 * ALIGNMENT];
            let (result, buf) = file.read_at(read_buffer, 0).await.into();
            result.unwrap();
            read_buffer = buf;

            // First ALIGNMENT - 1 bytes should be 0xAA
            assert_eq!(&read_buffer[0..ALIGNMENT - 1], &vec![0xAA; ALIGNMENT - 1]);

            // Next 2 * ALIGNMENT + 7 bytes should be 0xBB
            assert_eq!(
                &read_buffer[ALIGNMENT - 1..(ALIGNMENT - 1) + (2 * ALIGNMENT + 7)],
                &vec![0xBB; 2 * ALIGNMENT + 7]
            );

            // Remaining bytes should be 0 (padding)
            let total_written = (ALIGNMENT - 1) + (2 * ALIGNMENT + 7);
            assert_eq!(
                &read_buffer[total_written..4 * ALIGNMENT],
                &vec![0u8; 4 * ALIGNMENT - total_written]
            );
        });
    }
}
