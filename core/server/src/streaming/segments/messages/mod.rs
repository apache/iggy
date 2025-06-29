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

use crate::{io::file::IggyFile};

use super::IggyMessagesBatchSet;
use error_set::ErrContext;
use iggy_common::IggyError;
use monoio::io::AsyncWriteRentExt;

pub use messages_reader::MessagesReader;
pub use messages_writer::MessagesWriter;


/// Vectored write a batches of messages to file
async fn write_batch(
    file: &mut IggyFile,
    file_path: &str,
    mut batches: IggyMessagesBatchSet,
) -> Result<usize, IggyError> {
    //let mut slices = batches.iter().map(|b| to_iovec(&b)).collect::<Vec<iovec>>();
    let mut total_written = 0;
    // TODO: Fork monoio, piece of shit runtime.
    for batch in batches.iter_mut()  {
        let messages = batch.take_messages();
        let writen = file.write_all(messages).await.0.with_error_context(|error| {
            format!(
                "Failed to write messages to file: {file_path}, error: {error}",
            )
            // TODO: Better error variant.
        }).map_err(|_| IggyError::CannotAppendMessage)?;
        total_written += writen;
    }
    Ok(total_written)
}
