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

use std::time::Duration;

use iggy::prelude::{
    PartitionReader as RustPartitionReader, PolledMessages as RustPolledMessages,
    PollingStrategy as RustPollingStrategy, ReadError,
};

use crate::{
    RUNTIME, ffi,
    messages::{Checkpoint, Continuation},
};

const CLOSED: &str = "the reader is closed";

/// The C++ handle of a reader. `close` takes the reader out, so every later call fails, except
/// a second `close`, which does nothing.
pub struct PartitionReader {
    inner: Option<RustPartitionReader>,
}

impl PartitionReader {
    pub(crate) fn new(reader: RustPartitionReader) -> Self {
        Self {
            inner: Some(reader),
        }
    }

    pub fn next(
        &mut self,
        timeout_micros: u64,
        error_info: &mut ffi::ReadErrorInfo,
    ) -> Result<ffi::PolledMessages, String> {
        *error_info = ffi::ReadErrorInfo::default();
        let reader = self
            .inner
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
        let reader = self
            .inner
            .as_ref()
            .ok_or_else(|| format!("Could not commit checkpoint: {CLOSED}"))?;
        RUNTIME
            .block_on(reader.commit(checkpoint.0))
            .inspect_err(|error| error_info.record(error))
            .map_err(|error| format!("Could not commit checkpoint: {error}"))
    }

    pub fn seek(
        &mut self,
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
        &mut self,
        partition_id: u32,
        continuation: &Continuation,
        error_info: &mut ffi::ErrorInfo,
    ) -> Result<(), String> {
        *error_info = ffi::ErrorInfo::default();
        self.set_position(partition_id, continuation.0, error_info)
    }

    fn set_position(
        &mut self,
        partition_id: u32,
        start: RustPollingStrategy,
        error_info: &mut ffi::ErrorInfo,
    ) -> Result<(), String> {
        let reader = self
            .inner
            .as_mut()
            .ok_or_else(|| format!("Could not seek partition {partition_id}: {CLOSED}"))?;
        RUNTIME
            .block_on(reader.seek(partition_id, start))
            .inspect_err(|error| error_info.record(error))
            .map_err(|error| format!("Could not seek partition {partition_id}: {error}"))
    }

    pub fn partitions(&self) -> Result<Vec<u32>, String> {
        self.inner
            .as_ref()
            .map(RustPartitionReader::partitions)
            .ok_or_else(|| format!("Could not get the reader partitions: {CLOSED}"))
    }

    pub fn close(&mut self, error_info: &mut ffi::ErrorInfo) -> Result<(), String> {
        *error_info = ffi::ErrorInfo::default();
        let Some(reader) = self.inner.take() else {
            return Ok(());
        };
        RUNTIME
            .block_on(reader.close())
            .inspect_err(|error| error_info.record(error))
            .map_err(|error| format!("Could not close reader: {error}"))
    }
}

impl ffi::ReadErrorInfo {
    fn record(&mut self, error: &ReadError) {
        self.error.record(&error.error);
        self.has_partition_id = error.partition_id.is_some();
        self.partition_id = error.partition_id.unwrap_or_default();
        self.skip = error
            .skip
            .map(|skip| Box::new(Continuation(skip)))
            .into_iter()
            .collect();
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use iggy::prelude::{Checkpoint as RustCheckpoint, IggyError, PartitionContext, PollingKind};

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
        let mut reader = PartitionReader { inner: None };
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
    fn should_record_the_skip_of_a_read_error_as_one_continuation() {
        let skip = continuation_at(5);
        let mut info = ffi::ReadErrorInfo::default();

        info.record(&ReadError {
            partition_id: Some(PARTITION),
            error: IggyError::InvalidMessagePayloadLength,
            skip: Some(skip),
        });
        assert!(info.has_partition_id);
        assert_eq!(info.partition_id, PARTITION);
        assert_eq!(
            info.skip
                .iter()
                .map(|continuation| continuation.0)
                .collect::<Vec<_>>(),
            [skip]
        );

        info.record(&ReadError {
            partition_id: None,
            error: IggyError::ClientShutdown,
            skip: None,
        });
        assert!(!info.has_partition_id);
        assert!(
            info.skip.is_empty(),
            "a later error must not keep the skip of an earlier one"
        );
    }
}
