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

use crate::streaming::persistence::COMPONENT;
use bytes::Bytes;
use error_set::ErrContext;
use flume::{unbounded, Receiver, Sender};
use iggy::error::IggyError;
use std::{sync::Arc, time::Duration};
use tokio::task;
use tracing::error;

use super::persister::PersisterKind;

#[derive(Debug)]
pub struct LogPersisterTask {
    _sender: Option<Sender<Bytes>>,
    _task_handle: Option<task::JoinHandle<()>>,
}

impl LogPersisterTask {
    pub fn new(
        path: String,
        persister: Arc<PersisterKind>,
        max_retries: u32,
        retry_sleep: Duration,
    ) -> Self {
        let (sender, receiver): (Sender<Bytes>, Receiver<Bytes>) = unbounded();

        let task_handle = task::spawn(async move {
            loop {
                match receiver.recv_async().await {
                    Ok(data) => {
                        if let Err(error) = Self::persist_with_retries(
                            &path,
                            &persister,
                            data,
                            max_retries,
                            retry_sleep,
                        )
                        .await
                        {
                            error!("{COMPONENT} (error: {error}) - Final failure to persist data.");
                        }
                    }
                    Err(error) => {
                        error!("{COMPONENT} (error: {error}) - Error receiving data from channel.");
                        return;
                    }
                }
            }
        });

        LogPersisterTask {
            _sender: Some(sender),
            _task_handle: Some(task_handle),
        }
    }

    async fn persist_with_retries(
        path: &str,
        persister: &Arc<PersisterKind>,
        data: Bytes,
        max_retries: u32,
        retry_sleep: Duration,
    ) -> Result<(), String> {
        let mut retries = 0;

        while retries < max_retries {
            match persister.append(path, &data).await {
                Ok(_) => return Ok(()),
                Err(e) => {
                    error!(
                        "Could not append to persister (attempt {}): {}",
                        retries + 1,
                        e
                    );
                    retries += 1;
                    tokio::time::sleep(retry_sleep).await;
                }
            }
        }

        Err(format!(
            "{COMPONENT} - failed to persist data after {max_retries} retries",
        ))
    }

    pub async fn send(&self, data: Bytes) -> Result<(), IggyError> {
        if let Some(sender) = &self._sender {
            sender
                .send_async(data)
                .await
                .with_error_context(|error| {
                    format!("{COMPONENT} (error: {error}) - failed to send data to async channel")
                })
                .map_err(|_| IggyError::CannotSaveMessagesToSegment)
        } else {
            Err(IggyError::CannotSaveMessagesToSegment)
        }
    }
}

impl Drop for LogPersisterTask {
    fn drop(&mut self) {
        self._sender.take();

        if let Some(handle) = self._task_handle.take() {
            tokio::spawn(async move {
                if let Err(error) = handle.await {
                    error!(
                        "{COMPONENT} (error: {error}) - error while shutting down task in Drop.",
                    );
                }
            });
        }
    }
}
