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
use compio::fs::{File, OpenOptions};
use compio::io::AsyncWriteAtExt;
use error_set::ErrContext;
use iggy_common::IggyError;

#[derive(Debug)]
pub struct DirectFile {
    file_path: String,
    file: File,
    file_position: u64,
    tail: PooledBuffer,
    tail_len: usize,
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
            .custom_flags(0x4000)
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
            tail_len: 0,
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

    fn new(file: File, file_path: String, initial_position: u64) -> Self {
        Self {
            file_path,
            file,
            file_position: initial_position,
            tail: PooledBuffer::with_capacity(ALIGNMENT),
            tail_len: 0,
        }
    }

    pub async fn write_all(&mut self, mut data: &[u8]) -> Result<usize, IggyError> {
        let initial_len = data.len();
        tracing::trace!(
            "DirectFile write_all called for file: {}, data_len: {}, position: {}, tail_len: {}",
            self.file_path,
            initial_len,
            self.file_position,
            self.tail_len
        );

        if self.tail_len > 0 {
            let need = ALIGNMENT - self.tail_len;
            let take = need.min(data.len());
            self.tail.extend_from_slice(&data[..take]);
            self.tail_len += take;
            data = &data[take..];

            if self.tail_len == ALIGNMENT {
                self.flush_tail().await?;
            }
        }

        if !data.is_empty() {
            let whole_sectors_end = data.len() & !(ALIGNMENT - 1);
            if whole_sectors_end > 0 {
                let whole_sectors = &data[..whole_sectors_end];
                let mut written = 0;

                while written < whole_sectors.len() {
                    let chunk_size = (whole_sectors.len() - written).min(128 * 1024 * 1024);
                    let chunk = &whole_sectors[written..written + chunk_size];

                    let chunk_buffer = PooledBuffer::from(chunk);

                    let (result, _) = self
                        .file
                        .write_all_at(chunk_buffer, self.file_position)
                        .await
                        .into();

                    result.map_err(|e| {
                        tracing::error!("Failed to write to direct file: {} at position {}, chunk size: {}, error: {}",
                            self.file_path, self.file_position, chunk_size, e);
                        IggyError::CannotWriteToFile
                    })?;

                    self.file_position += chunk_size as u64;
                    written += chunk_size;
                }

                data = &data[whole_sectors_end..];
            }
        }

        if !data.is_empty() {
            self.tail.clear();
            self.tail.extend_from_slice(data);
            self.tail_len = data.len();
        }

        Ok(initial_len)
    }

    pub async fn flush(&mut self) -> Result<(), IggyError> {
        if self.tail_len > 0 {
            self.tail.resize(ALIGNMENT, 0);
            self.flush_tail().await?;
        }
        Ok(())
    }

    pub fn position(&self) -> u64 {
        self.file_position
    }

    pub fn tail_len(&self) -> usize {
        self.tail_len
    }

    pub fn file_path(&self) -> &str {
        &self.file_path
    }

    pub fn tail_buffer(&self) -> &PooledBuffer {
        &self.tail
    }

    pub fn take_tail(&mut self) -> (PooledBuffer, usize) {
        let tail = std::mem::replace(&mut self.tail, PooledBuffer::with_capacity(ALIGNMENT));
        let tail_len = self.tail_len;
        self.tail_len = 0;
        (tail, tail_len)
    }

    pub fn set_tail(&mut self, tail: PooledBuffer, tail_len: usize) {
        self.tail = tail;
        self.tail_len = tail_len;
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
        self.tail_len = 0;
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
                direct_file.write_all(&buf).await.unwrap();
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
            direct_file.write_all(&data).await.unwrap();

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

            direct_file.write_all(&data1).await.unwrap();
            direct_file.write_all(&data2).await.unwrap();
            direct_file.write_all(&data3).await.unwrap();

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
            direct_file.write_all(&data).await.unwrap();

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

            direct_file.write_all(&data1).await.unwrap();
            direct_file.write_all(&data2).await.unwrap();
            direct_file.write_all(&data3).await.unwrap();

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
}
