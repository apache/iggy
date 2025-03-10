mod messages_reader;
mod messages_writer;
mod persister_task;

pub use messages_reader::MessagesReader;
pub use messages_writer::MessagesWriter;
pub use persister_task::PersisterTask;

use error_set::ErrContext;
use iggy::{error::IggyError, prelude::IggyMessages};
use std::io::IoSlice;
use tokio::{fs::File, io::AsyncWriteExt};
use tracing::error;

/// Write a batch of messages to a file using vectored I/O
/// 
/// This function writes all the messages in the batch to the file using vectored I/O,
/// which can be more efficient than writing each message individually.
pub async fn write_batch_vectored(
    file: &mut File,
    file_path: &str,
    batches: &[IggyMessages],
) -> Result<usize, IggyError> {
    let mut total_bytes_written = 0;
    let mut slices_vec: Vec<IoSlice<'_>> = batches.iter().map(|b| IoSlice::new(&b.buffer())).collect();
    let mut slices = slices_vec.as_mut_slice();
    
    while !slices.is_empty() {
        let written = file
            .write_vectored(slices)
            .await
            .with_error_context(|error| {
                format!("Failed to write vectored to file: {file_path}. {error}")
            })
            .map_err(|_| IggyError::CannotWriteToFile)?;

        if written == 0 {
            error!("Failed to write batch of messages to file: {file_path}");
            return Err(IggyError::CannotWriteToFile);
        }
        
        total_bytes_written += written;
        IoSlice::advance_slices(&mut slices, written);
    }
    
    Ok(total_bytes_written)
}
