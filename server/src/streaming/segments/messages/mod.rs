mod messages_reader;
mod messages_writer;
mod persister_task;

use std::io::IoSlice;

use error_set::ErrContext;
pub use messages_reader::MessagesReader;
pub use messages_writer::MessagesWriter;
pub use persister_task::PersisterTask;

use super::IggyBatch;
use iggy::error::IggyError;
use tokio::{fs::File, io::AsyncWriteExt};

/// Vectored write a batches of messages to file
async fn write_batch(
    file: &mut File,
    file_path: &str,
    batches: IggyBatch,
) -> Result<usize, IggyError> {
    let mut slices: Vec<IoSlice> = batches.iter().map(|b| IoSlice::new(b)).collect();

    let slices = &mut slices.as_mut_slice();
    let mut written = 0;
    while !slices.is_empty() {
        written += file
            .write_vectored(slices)
            .await
            .with_error_context(|error| {
                format!("Failed to write messages to file: {file_path}, error: {error}",)
            })
            .map_err(|_| IggyError::CannotWriteToFile)?;
        IoSlice::advance_slices(slices, written);
    }
    Ok(written)
}
