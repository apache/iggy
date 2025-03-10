use super::PersisterTask;
use error_set::ErrContext;
use iggy::{
    confirmation::Confirmation,
    error::IggyError,
    prelude::IggyMessages,
    utils::{byte_size::IggyByteSize, duration::IggyDuration},
};
use std::{
    io::IoSlice,
    sync::{
        atomic::{AtomicU64, Ordering},
        Arc,
    },
};
use tokio::{
    fs::{File, OpenOptions},
    io::AsyncWriteExt,
};
use tracing::{error, trace};

/// A dedicated struct for writing to the messages file.
#[derive(Debug)]
pub struct MessagesWriter {
    file_path: String,
    /// Holds the file for synchronous writes; when asynchronous persistence is enabled, this will be None.
    file: Option<File>,
    /// When set, asynchronous writes are handled by this persister task.
    persister_task: Option<PersisterTask>,
    messages_size_bytes: Arc<AtomicU64>,
    fsync: bool,
}

impl MessagesWriter {
    /// Opens the messages file in write mode.
    ///
    /// If the server confirmation is set to `NoWait`, the file handle is transferred to the
    /// persister task (and stored in `persister_task`) so that writes are done asynchronously.
    /// Otherwise, the file is retained in `self.file` for synchronous writes.
    pub async fn new(
        file_path: &str,
        messages_size_bytes: Arc<AtomicU64>,
        fsync: bool,
        server_confirmation: Confirmation,
        max_file_operation_retries: u32,
        retry_delay: IggyDuration,
    ) -> Result<Self, IggyError> {
        let file = OpenOptions::new()
            .write(true)
            .append(true)
            .create(true)
            .open(file_path)
            .await
            .map_err(|_| IggyError::CannotReadFile)?;

        let _ = file.sync_all().await.with_error_context(|error| {
            format!("Failed to fsync messages file after creation: {file_path}. {error}",)
        });

        let actual_messages_size = file
            .metadata()
            .await
            .map_err(|_| IggyError::CannotReadFileMetadata)?
            .len();

        messages_size_bytes.store(actual_messages_size, Ordering::Release);

        trace!("Opened messages file for writing: {file_path}, size: {actual_messages_size}");

        let (file, persister_task) = match server_confirmation {
            Confirmation::NoWait => {
                let persister = PersisterTask::new(
                    file,
                    file_path.to_string(),
                    fsync,
                    messages_size_bytes.clone(),
                    max_file_operation_retries,
                    retry_delay,
                );
                (None, Some(persister))
            }
            Confirmation::Wait => (Some(file), None),
        };

        Ok(Self {
            file_path: file_path.to_string(),
            file,
            persister_task,
            messages_size_bytes,
            fsync,
        })
    }

    /// Append a messages to the messages file.
    pub async fn save_batches(
        &mut self,
        messages: Vec<IggyMessages>,
        confirmation: Confirmation,
    ) -> Result<IggyByteSize, IggyError> {
        let messages_size: usize = messages.iter().map(|m| m.size() as usize).sum();
        trace!(
            "Saving batch of size {messages_size} bytes to messages file: {}",
            self.file_path
        );
        match confirmation {
            Confirmation::Wait => {
                self.write_batch_vectored(messages).await?;
                self.messages_size_bytes
                    .fetch_add(messages_size as u64, Ordering::AcqRel);
                trace!(
                    "Written batch of size {messages_size} bytes to messages file: {}",
                    self.file_path
                );
                if self.fsync {
                    let _ = self.fsync().await;
                }
            }
            Confirmation::NoWait => {
                if let Some(task) = &self.persister_task {
                    task.persist(messages).await;
                } else {
                    panic!(
                        "Confirmation::NoWait is used, but MessagesPersisterTask is not set for messages file: {}",
                        self.file_path
                    );
                }
            }
        }

        Ok(IggyByteSize::from(messages_size as u64))
    }

    /// Write a batch of bytes to the log file.
    async fn write_batch_vectored(&mut self, batches: Vec<IggyMessages>) -> Result<(), IggyError> {
        if let Some(ref mut file) = self.file {
            super::write_batch_vectored(file, &self.file_path, batches).await?;
            Ok(())
        } else {
            error!("File handle is not available for synchronous write.");
            Err(IggyError::CannotWriteToFile)
        }
    }

    pub async fn fsync(&self) -> Result<(), IggyError> {
        if let Some(file) = self.file.as_ref() {
            file.sync_all()
                .await
                .with_error_context(|error| {
                    format!("Failed to fsync messages file: {}. {error}", self.file_path)
                })
                .map_err(|_| IggyError::CannotWriteToFile)?;
        }

        Ok(())
    }

    pub async fn shutdown_persister_task(self) {
        if let Some(task) = self.persister_task {
            task.shutdown().await;
        }
    }
}
