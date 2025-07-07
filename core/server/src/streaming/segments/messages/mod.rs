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

use super::{DirectFile, IggyMessagesBatchSet};
use crate::streaming::utils::PooledBuffer;
use iggy_common::IggyError;

pub use messages_reader::MessagesReader;
pub use messages_writer::MessagesWriter;

async fn write_batch_with_direct_file(
    direct_file: &mut DirectFile,
    mut batches: IggyMessagesBatchSet,
) -> Result<usize, IggyError> {
    let total_written: usize = batches.iter().map(|b| b.size() as usize).sum();
    let mut messages_count = 0;

    for batch in batches.iter_mut() {
        messages_count += batch.count();
        let messages = batch.take_messages();
        direct_file.write_all(&messages).await?;
    }

    tracing::trace!("Saved {} messages", messages_count);

    Ok(total_written)
}
