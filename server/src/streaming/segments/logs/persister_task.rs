use flume::{unbounded, Receiver};
use iggy::{
    error::IggyError,
    models::batch::{IggyBatch, IggyHeader, IggyMutableBatch, IGGY_BATCH_OVERHEAD},
    utils::duration::IggyDuration,
};
use std::{
    io::IoSlice,
    sync::{
        atomic::{AtomicU64, Ordering},
        Arc,
    },
    time::Duration,
};
use tokio::{fs::File, io::AsyncWriteExt, select, time::sleep};
use tracing::{error, trace, warn};

#[derive(Debug)]
/// A command to the persister task.
enum PersisterTaskCommand {
    WriteRequest(u64, IggyHeader, Vec<IggyMutableBatch>),
    Shutdown,
}

/// A background task that writes data asynchronously.
#[derive(Debug)]
pub struct PersisterTask {
    sender: flume::Sender<PersisterTaskCommand>,
    file_path: String, // used only for logging
    _handle: tokio::task::JoinHandle<()>,
}

impl PersisterTask {
    /// Creates a new persister task that takes ownership of `file`.
    pub fn new(
        file: File,
        file_path: String,
        fsync: bool,
        log_file_size: Arc<AtomicU64>,
        max_retries: u32,
        retry_delay: IggyDuration,
    ) -> Self {
        let (sender, receiver) = unbounded();
        let log_file_size = log_file_size.clone();
        let file_path_clone = file_path.clone();
        let handle = tokio::spawn(async move {
            Self::run(
                file,
                file_path_clone,
                receiver,
                fsync,
                max_retries,
                retry_delay,
                log_file_size,
            )
            .await;
        });
        Self {
            sender,
            file_path,
            _handle: handle,
        }
    }

    /// Sends the batch bytes to the persister task (fire-and-forget).
    pub async fn persist(
        &self,
        batch_size: u64,
        header: IggyHeader,
        batches: Vec<IggyMutableBatch>,
    ) {
        if let Err(e) = self
            .sender
            .send_async(PersisterTaskCommand::WriteRequest(
                batch_size, header, batches,
            ))
            .await
        {
            error!(
                "Failed to send write request to LogPersisterTask for file {}: {:?}",
                self.file_path, e
            );
        }
    }

    /// Sends the shutdown command to the persister task and waits for a response.
    pub async fn shutdown(self) {
        let start_time = tokio::time::Instant::now();

        if let Err(e) = self.sender.send_async(PersisterTaskCommand::Shutdown).await {
            error!(
                "Failed to send shutdown command to LogPersisterTask for file {}: {:?}",
                self.file_path, e
            );
            return;
        }

        let mut handle_future = self._handle;

        select! {
            result = &mut handle_future => {
                match result {
                    Ok(_) => {
                        let elapsed = start_time.elapsed();
                        trace!(
                            "PersisterTask shutdown complete for file {} in {:.2}s",
                            self.file_path,
                            elapsed.as_secs_f64()
                        );
                    }
                    Err(e) => {
                        error!(
                            "Error during joining PersisterTask for file {}: {:?}",
                            self.file_path, e
                        );
                    }
                }
                return;
            }
            _ = sleep(Duration::from_secs(1)) => {
                warn!(
                    "PersisterTask for file {} is still shutting down after 1s",
                    self.file_path
                );
            }
        }

        select! {
            result = &mut handle_future => {
                match result {
                    Ok(_) => {
                        let elapsed = start_time.elapsed();
                        trace!(
                            "PersisterTask shutdown complete for file {} in {:.2}s",
                            self.file_path,
                            elapsed.as_secs_f64()
                        );
                    }
                    Err(e) => {
                        error!(
                            "Error during joining PersisterTask for file {}: {:?}",
                            self.file_path, e
                        );
                    }
                }
                return;
            }
            _ = sleep(Duration::from_secs(4)) => {
                warn!(
                    "PersisterTask for file {} is still shutting down after 5s",
                    self.file_path
                );
            }
        }

        match handle_future.await {
            Ok(_) => {
                let elapsed = start_time.elapsed();
                warn!(
                    "PersisterTask shutdown complete for file {} in {:.2}s",
                    self.file_path,
                    elapsed.as_secs_f64()
                );
            }
            Err(e) => {
                error!(
                    "Error during joining PersisterTask for file {}: {:?}",
                    self.file_path, e
                );
            }
        }
    }

    /// The background task loop. Processes write requests until the channel is closed.
    async fn run(
        mut file: File,
        file_path: String,
        receiver: Receiver<PersisterTaskCommand>,
        fsync: bool,
        max_retries: u32,
        retry_delay: IggyDuration,
        log_file_size: Arc<AtomicU64>,
    ) {
        while let Ok(request) = receiver.recv_async().await {
            match request {
                PersisterTaskCommand::WriteRequest(batch_size, header, batches) => {
                    match Self::write_with_retries(
                        &mut file,
                        &file_path,
                        batch_size,
                        header,
                        batches,
                        fsync,
                        max_retries,
                        retry_delay,
                    )
                    .await
                    {
                        Ok(bytes_written) => {
                            log_file_size.fetch_add(bytes_written, Ordering::AcqRel);
                        }
                        Err(e) => {
                            error!(
                            "Failed to persist data in LogPersisterTask for file {file_path}: {:?}",
                            e
                        )
                        }
                    }
                }
                PersisterTaskCommand::Shutdown => {
                    trace!("LogPersisterTask for file {file_path} received shutdown command");
                    if let Err(e) = file.sync_all().await {
                        error!(
                            "Failed to sync_all() in LogPersisterTask for file {file_path}: {:?}",
                            e
                        );
                    }
                    break;
                }
            }
        }
        trace!("PersisterTask for file {file_path} has finished processing requests");
    }

    /// Writes the provided data to the file using simple retry logic.
    async fn write_with_retries(
        file: &mut File,
        file_path: &str,
        batch_size: u64,
        header: IggyHeader,
        batches: Vec<IggyMutableBatch>,
        fsync: bool,
        max_retries: u32,
        retry_delay: IggyDuration,
    ) -> Result<u64, IggyError> {
        // TODO: Fix me, this logic is repeated, maybe could be encapsulated inside of the batch_accumulator
        // that yields accumulated batches and leading header.
        let mut slices = Vec::new();
        let header = header.as_bytes();
        slices.push(IoSlice::new(&header));
        batches.iter().for_each(|b| {
            slices.push(IoSlice::new(&b.batch));
        });

        let mut attempts = 0;
        // TODO: This "retry" logic should be rewritten.
        // There are certain kind of errors whom retyring is considered harmful, for example fsync failure
        // (https://www.usenix.org/system/files/atc20-rebello.pdf)
        // There are few errors which are worth retrying such as LSE (Latent sector error), as the file system
        // might be able to realocate a new sector for the data and recover, but not every file system supports that.
        // In general this topic should be furthered researched rather than just naively retry when the write fails.
        loop {
            match file.write_vectored(&slices).await {
                Ok(_) => {
                    if fsync {
                        match file.sync_all().await {
                            Ok(_) => return Ok(batch_size),
                            Err(e) => {
                                attempts += 1;
                                error!(
                                    "Error syncing file {file_path}: {:?} (attempt {attempts}/{max_retries})",
                                     e,
                                );
                            }
                        }
                    } else {
                        return Ok(batch_size);
                    }
                }
                Err(e) => {
                    attempts += 1;
                    error!(
                        "Error writing to file {file_path}: {:?} (attempt {attempts}/{max_retries})",
                        e,
                    );
                }
            }
            if attempts >= max_retries {
                error!(
                    "Failed to write to file {file_path} after {max_retries} attempts, something's terribly wrong",
                );
                return Err(IggyError::CannotWriteToFile);
            }
            sleep(retry_delay.get_duration()).await;
        }
    }
}
