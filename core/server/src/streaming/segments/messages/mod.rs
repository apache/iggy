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

use crate::{io::file::IggyFile, to_iovec};

use super::IggyMessagesBatchSet;
use error_set::ErrContext;
use iggy_common::IggyError;
use monoio::io::AsyncWriteRent;
use std::{io::IoSlice, mem::take};

pub use messages_reader::MessagesReader;
pub use messages_writer::MessagesWriter;

use nix::libc::iovec;

/// Vectored write a batches of messages to file
async fn write_batch(
    file: &mut IggyFile,
    file_path: &str,
    batches: IggyMessagesBatchSet,
) -> Result<usize, IggyError> {
    let mut slices = batches.iter().map(|b| to_iovec(&b)).collect::<Vec<iovec>>();
    let mut total_written = 0;

    loop {
        let (result, vomited) = file.writev(slices).await;
        slices = vomited;
        let bytes_written = result
            .with_error_context(|error| {
                format!("Failed to write messages to file: {file_path}, error: {error}",)
            })
            .map_err(|_| IggyError::CannotWriteToFile)?;

        total_written += bytes_written;
        advance_slices(&mut slices.as_mut_slice(), bytes_written);
        if slices.is_empty() {
            break;
        }
    }

    Ok(total_written)
}

fn advance_slices(mut bufs: &mut [iovec], n: usize) {
    // Number of buffers to remove.
    let mut remove = 0;
    // Remaining length before reaching n. This prevents overflow
    // that could happen if the length of slices in `bufs` were instead
    // accumulated. Those slice may be aliased and, if they are large
    // enough, their added length may overflow a `usize`.
    let mut left = n;
    for buf in bufs.iter() {
        if let Some(remainder) = left.checked_sub(buf.iov_len as _) {
            left = remainder;
            remove += 1;
        } else {
            break;
        }
    }

    bufs = &mut bufs[remove..];
    if bufs.is_empty() {
        assert!(left == 0, "advancing io slices beyond their length");
    } else {
        unsafe {
            bufs[0].iov_len -= n;
            bufs[0].iov_base = bufs[0].iov_base.add(n);
        }
    }
}
