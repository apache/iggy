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

use futures::StreamExt;
use iggy::prelude::{
    IggyConsumer as RustIggyConsumer, PollingStrategy as RustPollingStrategy, ReceivedMessage,
};

use crate::{
    RUNTIME, ffi,
    messages::{Checkpoint, Continuation},
};

const CLOSED: &str = "the consumer is closed";

/// The C++ handle of a consumer. `close` takes the consumer out, so every later call fails,
/// except a second `close`, which does nothing.
///
/// C++ can call it from several threads, so the calls run one at a time: a call waits for a
/// running `next` to end, at most for its timeout.
pub struct Consumer {
    inner: Mutex<Option<RustIggyConsumer>>,
}

impl Consumer {
    pub(crate) fn new(consumer: RustIggyConsumer) -> Self {
        Self {
            inner: Mutex::new(Some(consumer)),
        }
    }

    fn consumer(&self) -> MutexGuard<'_, Option<RustIggyConsumer>> {
        // A panic in a call leaves the consumer as it was, so the next call can use it.
        self.inner.lock().unwrap_or_else(PoisonError::into_inner)
    }

    pub fn next(
        &self,
        timeout_micros: u64,
        error_info: &mut ffi::ReadErrorInfo,
    ) -> Result<Vec<ffi::ReceivedMessage>, String> {
        *error_info = ffi::ReadErrorInfo::default();
        let mut consumer = self.consumer();
        let consumer = consumer
            .as_mut()
            .ok_or_else(|| format!("Could not consume a message: {CLOSED}"))?;
        // `timeout` is built inside `block_on`, see the reader. The stream is cancel safe, so a
        // message that arrives after the timeout stays in the consumer.
        let received = RUNTIME.block_on(async {
            tokio::time::timeout(Duration::from_micros(timeout_micros), consumer.next()).await
        });
        match received {
            Ok(Some(Ok(received))) => Ok(vec![received.into()]),
            Ok(Some(Err(error))) => {
                error_info.record(&error);
                Err(format!("Could not consume a message: {error}"))
            }
            Ok(None) => Err(format!("Could not consume a message: {CLOSED}")),
            Err(_) => Ok(Vec::new()),
        }
    }

    pub fn commit(
        &self,
        checkpoint: &Checkpoint,
        error_info: &mut ffi::ErrorInfo,
    ) -> Result<(), String> {
        *error_info = ffi::ErrorInfo::default();
        let consumer = self.consumer();
        let consumer = consumer
            .as_ref()
            .ok_or_else(|| format!("Could not commit checkpoint: {CLOSED}"))?;
        RUNTIME
            .block_on(consumer.commit(checkpoint.0))
            .inspect_err(|error| error_info.record(error))
            .map_err(|error| format!("Could not commit checkpoint: {error}"))
    }

    pub fn mark(
        &self,
        checkpoint: &Checkpoint,
        error_info: &mut ffi::ErrorInfo,
    ) -> Result<(), String> {
        *error_info = ffi::ErrorInfo::default();
        let consumer = self.consumer();
        let consumer = consumer
            .as_ref()
            .ok_or_else(|| format!("Could not mark checkpoint: {CLOSED}"))?;
        consumer
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
        let mut consumer = self.consumer();
        let consumer = consumer
            .as_mut()
            .ok_or_else(|| format!("Could not seek partition {partition_id}: {CLOSED}"))?;
        RUNTIME
            .block_on(consumer.seek(partition_id, start))
            .inspect_err(|error| error_info.record(error))
            .map_err(|error| format!("Could not seek partition {partition_id}: {error}"))
    }

    pub fn close(&self, error_info: &mut ffi::ErrorInfo) -> Result<(), String> {
        *error_info = ffi::ErrorInfo::default();
        let Some(mut consumer) = self.consumer().take() else {
            return Ok(());
        };
        RUNTIME
            .block_on(consumer.shutdown())
            .inspect_err(|error| error_info.record(error))
            .map_err(|error| format!("Could not close consumer: {error}"))
    }
}

impl From<ReceivedMessage> for ffi::ReceivedMessage {
    fn from(received: ReceivedMessage) -> Self {
        let checkpoint = Box::new(Checkpoint(received.checkpoint()));
        ffi::ReceivedMessage {
            partition_id: received.partition_id,
            current_offset: received.current_offset,
            message: received.message.into(),
            checkpoint,
        }
    }
}
