use crate::streaming::segments::indexes::IndexRange;
use bytes::{Bytes, BytesMut};
use error_set::ErrContext;
use iggy::{
    error::IggyError,
    models::batch::{IggyBatch, IggyHeader, IGGY_BATCH_OVERHEAD},
    utils::{byte_size::IggyByteSize, timestamp::IggyTimestamp},
};
use std::{
    fmt,
    fs::{File, OpenOptions},
    os::unix::prelude::FileExt,
};
use std::{
    io::ErrorKind,
    sync::{
        atomic::{AtomicU64, Ordering},
        Arc,
    },
};
use tokio::task::spawn_blocking;
use tracing::{error, trace, warn};

/// A dedicated struct for reading from the log file.
#[derive(Debug)]
pub struct SegmentLogReader {
    file_path: String,
    file: Arc<File>,
    log_size_bytes: Arc<AtomicU64>,
}

impl SegmentLogReader {
    /// Opens the log file in read mode.
    pub async fn new(file_path: &str, log_size_bytes: Arc<AtomicU64>) -> Result<Self, IggyError> {
        let file = OpenOptions::new()
            .read(true)
            .open(file_path)
            .with_error_context(|error| format!("Failed to open log file: {file_path}. {error}"))
            .map_err(|_| IggyError::CannotReadFile)?;

        // posix_fadvise() doesn't exist on MacOS
        #[cfg(not(target_os = "macos"))]
        {
            use std::os::unix::io::AsRawFd;
            let fd = file.as_raw_fd();
            let _ = nix::fcntl::posix_fadvise(
                fd,
                0,
                0,
                nix::fcntl::PosixFadviseAdvice::POSIX_FADV_SEQUENTIAL,
            )
            .with_info_context(|error| {
                format!("Failed to set sequential access pattern on log file: {file_path}. {error}")
            });
        }

        let actual_log_size = file
            .metadata()
            .with_error_context(|error| {
                format!("Failed to get metadata of log file: {file_path}. {error}")
            })
            .map_err(|_| IggyError::CannotReadFileMetadata)?
            .len();

        log_size_bytes.store(actual_log_size, Ordering::Release);

        Ok(Self {
            file_path: file_path.to_string(),
            file: Arc::new(file),
            log_size_bytes,
        })
    }

    // TODO: Implement this as a stream/iterator that streams batches and handle filtering level above.
    /// Loads message batches given an index range.
    pub async fn load_batches_by_range_impl(
        &self,
        index_range: &IndexRange,
        start_offset: u64,
        end_offset: u64,
    ) -> Result<Vec<IggyBatch>, IggyError> {
        let mut file_size = self.file_size();
        // TODO: Fix me
        /*
        if file_size == 0 {
            trace!("Log file {} is empty.", self.file_path);
            return Ok(Vec::new());
        }
        */

        let mut position = index_range.start.position as u64;
        let mut batches = Vec::new();
        let mut last_batch_to_read = false;

        while !last_batch_to_read && position < file_size {
            file_size = self.file_size();
            match self.maybe_read_next_batch(position, file_size).await? {
                Some((batch, bytes_read)) => {
                    position += bytes_read;
                    let last_offset_in_batch =
                        batch.header.base_offset + batch.header.last_offset_delta as u64;
                    if last_offset_in_batch >= end_offset as u64 || position >= file_size {
                        last_batch_to_read = true;
                    }
                    batches.push(batch);
                }
                None => {
                    break;
                }
            }
        }
        //TODO: Fix me
        //trace!("Loaded {} message batches.", batches);
        Ok(batches)
    }

    // TODO: This one is most likely not needed anymore.
    /// Loads and returns all message IDs from the log file.
    pub async fn load_message_ids_impl(&self) -> Result<Vec<u128>, IggyError> {
        let mut file_size = self.file_size();
        if file_size == 0 {
            trace!("Log file {} is empty.", self.file_path);
            return Ok(Vec::new());
        }
        //TODO: Fix me
        /*

        let mut offset = 0_u64;
        let mut message_ids = Vec::new();

        while offset < file_size {
            file_size = self.file_size();
            match self.read_next_batch(offset, file_size).await? {
                Some((batch, bytes_read)) => {
                    offset += bytes_read;
                    for msg in batch.into_messages_iter() {
                        message_ids.push(msg.id);
                    }
                }
                None => {
                    // Possibly reached EOF or truncated
                    break;
                }
            }
        }

        trace!("Loaded {} message IDs from the log.", message_ids.len());
        Ok(message_ids)
        */
        todo!()
    }

    /// Loads message batches given an index range and calls the provided callback for each batch.
    pub async fn load_batches_by_range_with_callback<F>(
        &self,
        index_range: &IndexRange,
        mut on_batch: F,
    ) -> Result<(), IggyError>
    where
        F: FnMut(()) -> Result<(), IggyError>,
    {
        //TODO: Fix me
        /*
        let mut file_size = self.file_size();
        if file_size == 0 {
            trace!("Log file {} is empty.", self.file_path);
            return Ok(());
        }

        let mut offset = index_range.start.position as u64;
        let mut last_batch_to_read = false;

        while !last_batch_to_read && offset < file_size {
            file_size = self.file_size();
            match self.read_next_batch(offset, file_size).await? {
                Some((batch, bytes_read)) => {
                    offset += bytes_read;
                    let last_offset_in_batch = batch.base_offset + batch.last_offset_delta as u64;
                    if offset >= file_size || last_offset_in_batch >= index_range.end.offset as u64
                    {
                        last_batch_to_read = true;
                    }
                    on_batch(batch)?;
                }
                None => {
                    break;
                }
            }
        }

        Ok(())
        */
        todo!()
    }

    /// Loads message batches up to a given size and calls the provided callback for each batch
    /// after a threshold is reached.
    pub async fn load_batches_by_size_with_callback(
        &self,
        bytes_to_load: u64,
        mut on_batch: impl FnMut(()) -> Result<(), IggyError>,
    ) -> Result<(), IggyError> {
        // TODO: Fix me
        /*
        let mut file_size = self.file_size();
        if file_size == 0 {
            trace!("Log file {} is empty.", self.file_path);
            return Ok(());
        }

        let mut offset = 0_u64;
        let mut accumulated_size = 0_u64;

        while offset < file_size {
            file_size = self.file_size();
            let threshold = file_size.saturating_sub(bytes_to_load);
            match self.read_next_batch(offset, file_size).await? {
                Some((batch, bytes_read)) => {
                    if accumulated_size >= threshold {
                        on_batch(batch)?;
                    }
                    offset += bytes_read;
                    accumulated_size += bytes_read;
                }
                None => {
                    break;
                }
            }
        }

        Ok(())
        */
        todo!()
    }

    async fn maybe_read_next_batch(
        &self,
        position: u64,
        file_size: u64,
    ) -> Result<Option<(IggyBatch, u64)>, IggyError> {
        let batch_header_size = IGGY_BATCH_OVERHEAD;
        if position + batch_header_size > file_size {
            return Ok(None);
        }

        let header_buf = match self.read_at(position, batch_header_size).await {
            Ok(buf) => buf,
            Err(error) if error.kind() == ErrorKind::UnexpectedEof => return Ok(None),
            Err(error) => {
                error!(
                    "Error reading batch header at position {} in file {}: {error}",
                    position, self.file_path
                );
                return Err(IggyError::CannotReadBatchBaseOffset);
            }
        };
        if header_buf.len() < batch_header_size as usize {
            warn!(
                "Cannot read batch header at position {} in file {}. Possibly truncated.",
                position, self.file_path
            );
            return Ok(None);
        }
        let header = IggyHeader::from_bytes(&header_buf);
        let batch_length = header.batch_length as usize;
        let batch_position = position + batch_header_size;
        if batch_position + batch_length as u64 > file_size {
            warn!(
                "It's not possible to read the full batch payload ({} bytes) at position {} in file {} of size {}. Possibly truncated.",
                batch_length, batch_position, self.file_path, file_size
            );
            return Ok(None);
        }

        let batch_bytes = match self
            .read_bytes_at(batch_position, batch_length as u64)
            .await
        {
            Ok(buf) => buf,
            Err(error) if error.kind() == ErrorKind::UnexpectedEof => return Ok(None),
            Err(error) => {
                error!(
                    "Error reading batch payload at position {} in file {}: {error}",
                    batch_position, self.file_path
                );
                return Err(IggyError::CannotReadBatchPayload);
            }
        };

        let bytes_read = batch_header_size + batch_length as u64;
        let batch = IggyBatch::new(header, batch_bytes);

        Ok(Some((batch, bytes_read)))
    }

    fn file_size(&self) -> u64 {
        self.log_size_bytes.load(Ordering::Acquire)
    }

    async fn read_at(&self, offset: u64, len: u64) -> Result<Vec<u8>, std::io::Error> {
        let file = self.file.clone();
        spawn_blocking(move || {
            let mut buf = vec![0u8; len as usize];
            file.read_exact_at(&mut buf, offset)?;
            Ok(buf)
        })
        .await?
    }

    async fn read_bytes_at(&self, offset: u64, len: u64) -> Result<Bytes, std::io::Error> {
        let file = self.file.clone();
        spawn_blocking(move || {
            let mut buf = BytesMut::with_capacity(len as usize);
            unsafe { buf.set_len(len as usize) };
            file.read_exact_at(&mut buf, offset)?;
            Ok(buf.freeze())
        })
        .await?
    }
}
