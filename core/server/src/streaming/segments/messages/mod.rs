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

mod messages_reader;
mod messages_writer;
mod persister_task;

use super::IggyMessagesBatchSet;
use error_set::ErrContext;
use iggy_common::IggyError;
use std::io::IoSlice;
use tokio::{fs::File, io::AsyncWriteExt};

pub use messages_reader::MessagesReader;
pub use messages_writer::MessagesWriter;
pub use persister_task::PersisterTask;

/// Vectored write a batches of messages to file
async fn write_batch(
    file: &mut File,
    file_path: &str,
    batches: IggyMessagesBatchSet,
) -> Result<usize, IggyError> {
    let mut slices: Vec<IoSlice> = batches.iter().map(|b| IoSlice::new(b)).collect();

    let slices = &mut slices.as_mut_slice();
    let mut total_written = 0;

    while !slices.is_empty() {
        let bytes_written = file
            .write_vectored(slices)
            .await
            .with_error_context(|error| {
                format!("Failed to write messages to file: {file_path}, error: {error}",)
            })
            .map_err(|_| IggyError::CannotWriteToFile)?;

        total_written += bytes_written;

        IoSlice::advance_slices(slices, bytes_written);
    }

    Ok(total_written)
}
