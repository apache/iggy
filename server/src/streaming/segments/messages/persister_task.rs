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

use crate::streaming::segments::IggyMessagesBatchSet;
use error_set::ErrContext;
use flume::{unbounded, Receiver};
use std::{
    sync::{
        atomic::{AtomicU64, Ordering},
        Arc,
    },
    time::Duration,
};
use tokio::{fs::File, select, time::sleep};
use tracing::{error, trace, warn};

use super::write_batch;

#[derive(Debug)]
/// A command to the persister task.
enum PersisterTaskCommand {
    WriteRequest(IggyMessagesBatchSet),
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
    pub fn new(file: File, file_path: String, fsync: bool, log_file_size: Arc<AtomicU64>) -> Self {
        let (sender, receiver) = unbounded();
        let log_file_size = log_file_size.clone();
        let file_path_clone = file_path.clone();
        let handle = tokio::spawn(async move {
            Self::run(file, file_path_clone, receiver, fsync, log_file_size).await;
        });
        Self {
            sender,
            file_path,
            _handle: handle,
        }
    }

    /// Sends the batch bytes to the persister task (fire-and-forget).
    pub async fn persist(&self, messages: IggyMessagesBatchSet) {
        if let Err(e) = self
            .sender
            .send_async(PersisterTaskCommand::WriteRequest(messages))
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
        log_file_size: Arc<AtomicU64>,
    ) {
        while let Ok(request) = receiver.recv_async().await {
            match request {
                PersisterTaskCommand::WriteRequest(messages) => {
                    match write_batch(&mut file, &file_path, messages).await {
                        Ok(bytes_written) => {
                            if fsync {
                                file.sync_all()
                                    .await
                                    .with_error_context(|error| {
                                        format!(
                                            "Failed to fsync messages file: {}. {error}",
                                            file_path
                                        )
                                    })
                                    .expect("Failed to fsync messages file");
                            }

                            log_file_size.fetch_add(bytes_written as u64, Ordering::Acquire);
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
}
