// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

use std::sync::{Mutex, MutexGuard, PoisonError};
use std::time::Duration;

use iggy::prelude::{
    PartitionReader as RustPartitionReader, PolledMessages as RustPolledMessages,
    PollingStrategy as RustPollingStrategy, ReadError,
};
use iggy_common::with_binary_deadline;
use tokio::time::Instant;

use crate::{
    RUNTIME, ffi,
    messages::{Checkpoint, Continuation},
};

const CLOSED: &str = "the reader is closed";

/// C++ cannot cancel a blocking commit or seek, so each one ends at this deadline after its
/// start. The read of the owner after a refusal (5009) retries only until then.
const WRITE_TIMEOUT: Duration = Duration::from_secs(10);

/// Runs a commit or a seek of a reader or a consumer, under a deadline of [`WRITE_TIMEOUT`].
pub(crate) fn block_on_write<T>(write: impl Future<Output = T>) -> T {
    RUNTIME.block_on(with_binary_deadline(Instant::now() + WRITE_TIMEOUT, write))
}

/// The C++ handle of a reader. `close` takes the reader out, so every later call fails, except
/// a second `close`, which does nothing.
///
/// C++ can call it from several threads, so the calls run one at a time: a call waits for a
/// running `next` to end, at most for its timeout.
pub struct PartitionReader {
    inner: Mutex<Option<RustPartitionReader>>,
}

impl PartitionReader {
    pub(crate) fn new(reader: RustPartitionReader) -> Self {
        Self {
            inner: Mutex::new(Some(reader)),
        }
    }

    fn reader(&self) -> MutexGuard<'_, Option<RustPartitionReader>> {
        // A panic in a call leaves the reader as it was, so the next call can use it.
        self.inner.lock().unwrap_or_else(PoisonError::into_inner)
    }

    pub fn next(
        &self,
        timeout_micros: u64,
        error_info: &mut ffi::ReadErrorInfo,
    ) -> Result<ffi::PolledMessages, String> {
        *error_info = ffi::ReadErrorInfo::default();
        let mut reader = self.reader();
        let reader = reader
            .as_mut()
            .ok_or_else(|| format!("Could not read messages: {CLOSED}"))?;
        // `timeout` panics when it is built on a thread outside a runtime, such as the C++ caller,
        // so it is built inside `block_on`. `next` is cancel safe, so a batch that arrives after
        // the timeout stays in the reader.
        let read = RUNTIME.block_on(async {
            tokio::time::timeout(Duration::from_micros(timeout_micros), reader.next()).await
        });
        match read {
            Ok(Ok(batch)) => Ok(batch.into()),
            Ok(Err(error)) => {
                error_info.record(&error);
                Err(format!("Could not read messages: {error}"))
            }
            Err(_) => Ok(RustPolledMessages::empty().into()),
        }
    }

    pub fn commit(
        &self,
        checkpoint: &Checkpoint,
        error_info: &mut ffi::ErrorInfo,
    ) -> Result<(), String> {
        *error_info = ffi::ErrorInfo::default();
        let reader = self.reader();
        let reader = reader
            .as_ref()
            .ok_or_else(|| format!("Could not commit checkpoint: {CLOSED}"))?;
        block_on_write(reader.commit(checkpoint.0))
            .inspect_err(|error| error_info.record(error))
            .map_err(|error| format!("Could not commit checkpoint: {error}"))
    }

    pub fn mark(
        &self,
        checkpoint: &Checkpoint,
        error_info: &mut ffi::ErrorInfo,
    ) -> Result<(), String> {
        *error_info = ffi::ErrorInfo::default();
        let reader = self.reader();
        let reader = reader
            .as_ref()
            .ok_or_else(|| format!("Could not mark checkpoint: {CLOSED}"))?;
        reader
            .mark(checkpoint.0)
            .inspect_err(|error| error_info.record(error))
            .map_err(|error| format!("Could not mark checkpoint: {error}"))
    }

    pub fn seek(
        &self,
        partition_id: u32,
        start: ffi::PollingStrategy,
        error_info: &mut ffi::ErrorInfo,
    ) -> Result<(), String> {
        *error_info = ffi::ErrorInfo::default();
        let start = RustPollingStrategy::try_from(start)
            .map_err(|error| format!("Could not seek partition {partition_id}: {error}"))?;
        self.set_position(partition_id, start, error_info)
    }

    pub fn seek_to(
        &self,
        partition_id: u32,
        continuation: &Continuation,
        error_info: &mut ffi::ErrorInfo,
    ) -> Result<(), String> {
        *error_info = ffi::ErrorInfo::default();
        self.set_position(partition_id, continuation.0, error_info)
    }

    fn set_position(
        &self,
        partition_id: u32,
        start: RustPollingStrategy,
        error_info: &mut ffi::ErrorInfo,
    ) -> Result<(), String> {
        let mut reader = self.reader();
        let reader = reader
            .as_mut()
            .ok_or_else(|| format!("Could not seek partition {partition_id}: {CLOSED}"))?;
        block_on_write(reader.seek(partition_id, start))
            .inspect_err(|error| error_info.record(error))
            .map_err(|error| format!("Could not seek partition {partition_id}: {error}"))
    }

    pub fn partitions(&self) -> Result<Vec<u32>, String> {
        self.reader()
            .as_ref()
            .map(RustPartitionReader::partitions)
            .ok_or_else(|| format!("Could not get the reader partitions: {CLOSED}"))
    }

    pub fn close(&self, error_info: &mut ffi::ErrorInfo) -> Result<(), String> {
        *error_info = ffi::ErrorInfo::default();
        let Some(reader) = self.reader().take() else {
            return Ok(());
        };
        RUNTIME
            .block_on(reader.close())
            .inspect_err(|error| error_info.record(error))
            .map_err(|error| format!("Could not close reader: {error}"))
    }
}

impl ffi::ReadErrorInfo {
    pub(crate) fn record(&mut self, error: &ReadError) {
        self.error.record(&error.error);
        self.has_partition_id = error.partition_id.is_some();
        self.partition_id = error.partition_id.unwrap_or_default();
        self.skip = error
            .skip
            .map(|skip| Box::new(Continuation(skip)))
            .into_iter()
            .collect();
        let lost = error.lost.as_ref().map(|lost| (*lost.start(), *lost.end()));
        self.has_lost = lost.is_some();
        (self.lost_first, self.lost_last) = lost.unwrap_or_default();
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use iggy::prelude::{Checkpoint as RustCheckpoint, IggyError, PartitionContext, PollingKind};
    use iggy_common::binary_operation_deadline;
    use std::thread;

    const PARTITION: u32 = 1;
    const CONTEXT: PartitionContext = PartitionContext {
        incarnation: 3,
        owner_generation: 5,
        metadata_op: 7,
    };

    fn continuation_at(offset: u64) -> RustPollingStrategy {
        RustPollingStrategy::continuation(PARTITION, PollingKind::Offset, offset, CONTEXT)
    }

    #[test]
    fn should_refuse_every_call_on_a_closed_reader_except_a_second_close() {
        let reader = PartitionReader {
            inner: Mutex::new(None),
        };
        let mut error_info = ffi::ErrorInfo::default();
        let mut read_error = ffi::ReadErrorInfo::default();
        let start = ffi::PollingStrategy {
            kind: "next".to_owned(),
            value: 0,
        };

        assert!(reader.next(0, &mut read_error).is_err());
        assert!(
            reader
                .commit(
                    &Checkpoint(RustCheckpoint::new(PARTITION, 0, CONTEXT)),
                    &mut error_info
                )
                .is_err()
        );
        assert!(
            reader
                .mark(
                    &Checkpoint(RustCheckpoint::new(PARTITION, 0, CONTEXT)),
                    &mut error_info
                )
                .is_err()
        );
        assert!(reader.seek(PARTITION, start, &mut error_info).is_err());
        assert!(
            reader
                .seek_to(
                    PARTITION,
                    &Continuation(continuation_at(0)),
                    &mut error_info
                )
                .is_err()
        );
        assert!(reader.partitions().is_err());

        error_info.record(&IggyError::InvalidCommand);
        assert_eq!(reader.close(&mut error_info), Ok(()));
        assert_eq!(
            error_info.code, 0,
            "close must clear the error of an earlier call"
        );
    }

    #[test]
    fn given_reader_shared_by_threads_when_calling_should_serve_every_call() {
        const THREADS: usize = 4;
        let reader = PartitionReader {
            inner: Mutex::new(None),
        };

        thread::scope(|scope| {
            for _ in 0..THREADS {
                scope.spawn(|| {
                    let mut error_info = ffi::ErrorInfo::default();
                    let mut read_error = ffi::ReadErrorInfo::default();
                    assert!(reader.next(0, &mut read_error).is_err());
                    assert!(
                        reader
                            .seek_to(
                                PARTITION,
                                &Continuation(continuation_at(0)),
                                &mut error_info
                            )
                            .is_err()
                    );
                    assert_eq!(reader.close(&mut error_info), Ok(()));
                });
            }
        });
    }

    #[test]
    fn should_record_the_skip_of_a_read_error_as_one_continuation() {
        let skip = continuation_at(5);
        let mut info = ffi::ReadErrorInfo::default();
        let mut undecoded = ReadError::new(Some(PARTITION), IggyError::InvalidMessagePayloadLength);
        undecoded.skip = Some(skip);

        info.record(&undecoded);
        assert!(info.has_partition_id);
        assert_eq!(info.partition_id, PARTITION);
        assert_eq!(
            info.skip
                .iter()
                .map(|continuation| continuation.0)
                .collect::<Vec<_>>(),
            [skip]
        );

        info.record(&ReadError::new(None, IggyError::ClientShutdown));
        assert!(!info.has_partition_id);
        assert!(
            info.skip.is_empty(),
            "a later error must not keep the skip of an earlier one"
        );
    }

    #[test]
    fn should_record_the_first_and_last_offset_of_a_lost_range() {
        const FIRST: u64 = 4;
        const LAST: u64 = 9;
        const OPERATION: u64 = 42;
        let mut info = ffi::ReadErrorInfo::default();
        let mut lost = ReadError::new(
            Some(PARTITION),
            IggyError::FetchProgressUncertain(OPERATION),
        );
        lost.skip = Some(continuation_at(LAST + 1));
        lost.lost = Some(FIRST..=LAST);

        info.record(&lost);
        assert!(info.has_lost);
        assert_eq!((info.lost_first, info.lost_last), (FIRST, LAST));

        info.record(&ReadError::new(Some(PARTITION), IggyError::ClientShutdown));
        assert!(
            !info.has_lost,
            "a later error must not keep the lost range of an earlier one"
        );
        assert_eq!((info.lost_first, info.lost_last), (0, 0));
    }

    #[test]
    fn given_a_blocking_commit_or_seek_when_running_should_end_at_the_write_timeout() {
        let start = Instant::now();

        let deadline = block_on_write(async { binary_operation_deadline() });

        assert!(
            deadline >= start + WRITE_TIMEOUT && deadline <= Instant::now() + WRITE_TIMEOUT,
            "the write must run under a deadline {WRITE_TIMEOUT:?} after its start"
        );
    }
}
