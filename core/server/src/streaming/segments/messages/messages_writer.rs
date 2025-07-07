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

use crate::streaming::segments::{
    DirectFile, IggyMessagesBatchSet, messages::write_batch_with_direct_file,
};
use error_set::ErrContext;
use iggy_common::{IggyByteSize, IggyError};
use std::sync::{
    Arc,
    atomic::{AtomicU64, Ordering},
};
use tracing::{error, trace};

/// A dedicated struct for writing to the messages file.
#[derive(Debug)]
pub struct MessagesWriter {
    direct_file: Option<DirectFile>,
    messages_size_bytes: Arc<AtomicU64>,
    fsync: bool,
}

impl MessagesWriter {
    pub async fn new(
        file_path: &str,
        messages_size_bytes: Arc<AtomicU64>,
        fsync: bool,
        file_exists: bool,
    ) -> Result<Self, IggyError> {
        let file_position = if file_exists {
            let current_size = messages_size_bytes.load(Ordering::Acquire);
            (current_size + 511) & !511
        } else {
            messages_size_bytes.store(0, Ordering::Release);
            0
        };

        trace!(
            "Opening messages file for writing: {file_path}, file_position: {}",
            file_position
        );

        let mut direct_file = DirectFile::open(file_path, file_position, file_exists).await?;

        if file_exists {
            let actual_messages_size = direct_file.get_file_size().await?;
            messages_size_bytes.store(actual_messages_size, Ordering::Release);

            trace!(
                "Opened existing messages file: {file_path}, size: {}, file_position: {}",
                actual_messages_size,
                direct_file.position()
            );
        }

        Ok(Self {
            direct_file: Some(direct_file),
            messages_size_bytes,
            fsync,
        })
    }

    pub async fn save_batch_set(
        &mut self,
        batch_set: IggyMessagesBatchSet,
    ) -> Result<IggyByteSize, IggyError> {
        let messages_size = batch_set.size();
        let messages_count = batch_set.count();
        let containers_count = batch_set.containers_count();
        let actual_written = if let Some(ref mut direct_file) = self.direct_file {
            trace!(
                "Saving batch set of size {messages_size} bytes ({containers_count} containers, {messages_count} messages) to messages file: {}",
                direct_file.file_path()
            );

            write_batch_with_direct_file(direct_file, batch_set)
                .await
                .with_error_context(|error| {
                    format!(
                        "Failed to write batch to messages file: {}. {error}",
                        direct_file.file_path()
                    )
                })?
        } else {
            tracing::error!("File handle is not available for synchronous write.");
            return Err(IggyError::CannotWriteToFile);
        };

        let logical_size = self.messages_size_bytes.load(Ordering::Relaxed) + actual_written as u64;
        self.messages_size_bytes
            .store(logical_size, Ordering::Release);

        trace!(
            "Written batch set of size {messages_size} bytes to disk. Logical size: {}",
            logical_size
        );

        Ok(IggyByteSize::from(messages_size as u64))
    }
}
