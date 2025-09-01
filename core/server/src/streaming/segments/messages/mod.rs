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

use super::IggyMessagesBatchSet;
use compio::{fs::File, io::AsyncWriteAtExt};
use iggy_common::IggyError;

pub use messages_reader::MessagesReader;
pub use messages_writer::MessagesWriter;

/// Vectored write a batches of messages to file
async fn write_batch(
    file: &File,
    position: u64,
    mut batches: IggyMessagesBatchSet,
) -> Result<usize, IggyError> {
    let total_written = batches.iter().map(|b| b.size() as usize).sum();
    let batches = batches
        .iter_mut()
        .map(|b| b.take_messages())
        .collect::<Vec<_>>();
    let (result, _) = (&*file)
        .write_vectored_all_at(batches, position)
        .await
        .into();
    result.map_err(|_| IggyError::CannotWriteToFile)?;
    Ok(total_written)
}
