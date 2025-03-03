use super::PersisterTask;
use error_set::ErrContext;
use iggy::{
    confirmation::Confirmation,
    error::IggyError,
    models::batch::{IggyBatch, IggyHeader, IggyMutableBatch, IGGY_BATCH_OVERHEAD},
    utils::{byte_size::IggyByteSize, duration::IggyDuration, sizeable::Sizeable},
};
use std::{
    io::IoSlice,
    sync::{
        atomic::{AtomicU64, Ordering},
        Arc,
    },
    time::Duration,
};
use tokio::{
    fs::{File, OpenOptions},
    io::AsyncWriteExt,
};
use tracing::{error, trace};

/// A dedicated struct for writing to the log file.
#[derive(Debug)]
pub struct SegmentLogWriter {
    file_path: String,
    /// Holds the file for synchronous writes; when asynchronous persistence is enabled, this will be None.
    file: Option<File>,
    /// When set, asynchronous writes are handled by this persister task.
    persister_task: Option<PersisterTask>,
    log_size_bytes: Arc<AtomicU64>,
    fsync: bool,
}

impl SegmentLogWriter {
    /// Opens the log file in write mode.
    ///
    /// If the server confirmation is set to `NoWait`, the file handle is transferred to the
    /// persister task (and stored in `persister_task`) so that writes are done asynchronously.
    /// Otherwise, the file is retained in `self.file` for synchronous writes.
    pub async fn new(
        file_path: &str,
        log_size_bytes: Arc<AtomicU64>,
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
            format!("Failed to fsync log file after creation: {file_path}. {error}",)
        });

        let actual_log_size = file
            .metadata()
            .await
            .map_err(|_| IggyError::CannotReadFileMetadata)?
            .len();

        log_size_bytes.store(actual_log_size, Ordering::Release);

        trace!("Opened log file for writing: {file_path}, size: {actual_log_size}");

        let (file, persister_task) = match server_confirmation {
            Confirmation::NoWait => {
                let persister = PersisterTask::new(
                    file,
                    file_path.to_string(),
                    fsync,
                    log_size_bytes.clone(),
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
            log_size_bytes,
            fsync,
        })
    }

    /// Append a message batch to the log file.
    pub async fn save_batches(
        &mut self,
        header: IggyHeader,
        batches: Vec<IggyMutableBatch>,
        confirmation: Confirmation,
    ) -> Result<IggyByteSize, IggyError> {
        let batch_size = IGGY_BATCH_OVERHEAD + header.batch_length;
        match confirmation {
            Confirmation::Wait => {
                self.write_batch(header, batches).await?;
                self.log_size_bytes.fetch_add(batch_size, Ordering::AcqRel);
                trace!(
                    "Written batch of size {batch_size} bytes to log file: {}",
                    self.file_path
                );
                if self.fsync {
                    let _ = self.fsync().await;
                }
            }
            Confirmation::NoWait => {
                if let Some(task) = &self.persister_task {
                    task.persist(batch_size, header, batches).await;
                } else {
                    panic!(
                        "Confirmation::NoWait is used, but LogPersisterTask is not set for log file: {}",
                        self.file_path
                    );
                }
            }
        }

        Ok(batch_size.into())
    }

    /// Write a batch of bytes to the log file and return the new file position.
    async fn write_batch(
        &mut self,
        header: IggyHeader,
        batches: Vec<IggyMutableBatch>,
    ) -> Result<(), IggyError> {
        if let Some(ref mut file) = self.file {
            let mut slices = Vec::new();

            let header = header.as_bytes();
            slices.push(IoSlice::new(&header));
            batches.iter().for_each(|b| {
                slices.push(IoSlice::new(&b.batch));
            });

            let slices = &mut slices.as_mut_slice();
            while slices.len() > 0 {
                let written = file
                    .write_vectored(slices)
                    .await
                    .with_error_context(|error| {
                        format!("Failed to log to file: {}. {error}", self.file_path)
                    })
                    .map_err(|_| IggyError::CannotWriteToFile)?;
                IoSlice::advance_slices(slices, written);
            }
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
                    format!("Failed to fsync log file: {}. {error}", self.file_path)
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
