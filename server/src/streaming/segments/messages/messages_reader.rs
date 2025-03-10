use bytes::{Bytes, BytesMut};
use error_set::ErrContext;
use iggy::{
    error::IggyError,
    prelude::{BytesSerializable, IggyMessages},
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
pub struct MessagesReader {
    file_path: String,
    file: Arc<File>,
    log_size_bytes: Arc<AtomicU64>,
}

impl MessagesReader {
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

        trace!("Opened messages file for reading: {file_path}, size: {actual_log_size}");

        Ok(Self {
            file_path: file_path.to_string(),
            file: Arc::new(file),
            log_size_bytes,
        })
    }

    /// Loads and returns all message IDs from the log file.
    pub async fn load_message_ids_impl(&self) -> Result<Vec<u128>, IggyError> {
        let file_size = self.file_size();
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

    pub async fn load_messages_impl(
        &self,
        start_pos: u32,
        count_bytes: u32,
        messages_count: u32,
    ) -> Result<IggyMessages, IggyError> {
        let file_size = self.file_size();
        if file_size == 0 {
            trace!("Messages file {} is empty.", self.file_path);
            return Ok(IggyMessages::default());
        }
        let messages_bytes = match self
            .read_bytes_at(start_pos as u64, count_bytes as u64)
            .await
        {
            Ok(buf) => buf,
            Err(error) if error.kind() == ErrorKind::UnexpectedEof => {
                return Ok(IggyMessages::default())
            }
            Err(error) => {
                error!(
                    "Error reading {messages_count} messages at position {start_pos} in file {} of size {}: {error}",
                    self.file_path, file_size
                );
                return Err(IggyError::CannotReadMessage);
            }
        };

        IggyMessages::from_bytes(messages_bytes)
    }

    fn file_size(&self) -> u64 {
        self.log_size_bytes.load(Ordering::Acquire)
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
