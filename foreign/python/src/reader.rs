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

use std::sync::Arc;

use iggy::prelude::PartitionReader as RustPartitionReader;
use pyo3::exceptions::PyRuntimeError;
use pyo3::prelude::*;
use pyo3_async_runtimes::tokio::future_into_py;
use pyo3_stub_gen::derive::{gen_stub_pyclass, gen_stub_pymethods};
use tokio::sync::Mutex;

use crate::client::{to_read_error, to_runtime_error};
use crate::event_loop::on_event_loop;
use crate::receive_message::{Checkpoint, PolledBatch, PollingStrategy};

/// Reads the partitions of a topic in batches, with routing, prefetch and the position of each
/// partition inside. Build one with `IggyClient.reader()`.
///
/// - A `Consumer.Single` reads the partitions given to `reader()`. A `Consumer.Group` reads the
///   assignment of its member: the reader joins the group, joins it again after a session loss
///   and leaves it in `close()`.
/// - `next()` returns the next batch of one partition, in offset order within one generation of
///   the partition: from its first read, from each `seek()` and from each new assignment to
///   this member.
/// - Without `ServerCommitPolicy.OnRead`, `commit()` stores the offset of a checkpoint. With it,
///   the server stores the offset of each reply, and `close()` moves the offset back to the last
///   delivered message.
/// - The reader stays usable after a `ReadError` and retries by itself. `HistoryUnavailable`
///   (87) stops its partition until `seek()`. With `OnRead`, an unknown outcome stops its
///   partition until `IggyClient.abandon_operation()` and `seek()`.
/// - The calls on one reader run one at a time, so a call waits for a pending `next()`.
///   `next()` is cancel safe: cancel it before `close()`.
/// - Dropping the reader stops its tasks without waiting, and it moves no offset back.
#[gen_stub_pyclass]
#[pyclass]
pub struct PartitionReader {
    inner: Arc<Mutex<Option<RustPartitionReader>>>,
}

impl PartitionReader {
    pub(crate) fn new(reader: RustPartitionReader) -> Self {
        Self {
            inner: Arc::new(Mutex::new(Some(reader))),
        }
    }
}

#[gen_stub_pymethods]
#[pymethods]
impl PartitionReader {
    /// Returns the next batch of one partition. It is never empty, and it has no time limit.
    /// A cancelled call leaves its batch in the reader for the next call.
    ///
    /// Raises `ReadError` for a failed read, and `RuntimeError` after `close()`.
    #[gen_stub(override_return_type(type_repr = "PolledMessages"))]
    async fn next(&self) -> PyResult<PolledBatch> {
        on_event_loop(async {
            let mut reader = self.inner.lock().await;
            let batch = reader
                .as_mut()
                .ok_or_else(closed)?
                .next()
                .await
                .map_err(to_read_error)?;
            Ok(PolledBatch(batch))
        })
        .await
    }

    /// Stores the offset of a checkpoint that this reader delivered, which covers every earlier
    /// message of its partition. A checkpoint of an older generation of its partition, or at or
    /// below the last commit of its generation, stores nothing.
    ///
    /// Raises `IggyError` with `InvalidCommand` (3) under `ServerCommitPolicy.OnRead` and for a
    /// checkpoint that this reader did not deliver, the errors of the offset store, and
    /// `RuntimeError` after `close()`.
    #[gen_stub(override_return_type(type_repr="collections.abc.Awaitable[None]", imports=("collections.abc")))]
    fn commit<'a>(&self, py: Python<'a>, checkpoint: &Checkpoint) -> PyResult<Bound<'a, PyAny>> {
        let checkpoint = checkpoint.inner;
        let inner = self.inner.clone();
        future_into_py(py, async move {
            let reader = inner.lock().await;
            reader
                .as_ref()
                .ok_or_else(closed)?
                .commit(checkpoint)
                .await
                .map_err(to_runtime_error)
        })
    }

    /// Sets the next read position of a partition and starts a new generation of it. It stores
    /// no offset, drops the queued batches of the partition and ignores its late replies. It
    /// also resumes a stopped partition. `start` is a new strategy or a continuation of the same
    /// partition, such as `ReadError.skip`.
    ///
    /// With `ServerCommitPolicy.OnRead`, the new position waits for the end of the committing
    /// read in flight.
    ///
    /// Raises `IggyError` with `InvalidCommand` (3) for a partition that the reader does not read
    /// and for a continuation of another partition, `PendingOperationError` with `OnRead` while
    /// a read of the partition has an unknown outcome (abandon its operation first), and
    /// `RuntimeError` after `close()`.
    #[gen_stub(override_return_type(type_repr="collections.abc.Awaitable[None]", imports=("collections.abc")))]
    fn seek<'a>(
        &self,
        py: Python<'a>,
        partition_id: u32,
        start: &PollingStrategy,
    ) -> PyResult<Bound<'a, PyAny>> {
        let start = start.inner;
        let inner = self.inner.clone();
        future_into_py(py, async move {
            let mut reader = inner.lock().await;
            reader
                .as_mut()
                .ok_or_else(closed)?
                .seek(partition_id, start)
                .await
                .map_err(to_runtime_error)
        })
    }

    /// Returns the partitions that the reader reads: those given to `reader()`, or the current
    /// assignment of a consumer group member.
    ///
    /// Raises `RuntimeError` after `close()`.
    #[gen_stub(override_return_type(type_repr="collections.abc.Awaitable[list[builtins.int]]", imports=("collections.abc")))]
    fn partitions<'a>(&self, py: Python<'a>) -> PyResult<Bound<'a, PyAny>> {
        let inner = self.inner.clone();
        future_into_py(py, async move {
            let reader = inner.lock().await;
            Ok(reader.as_ref().ok_or_else(closed)?.partitions())
        })
    }

    /// Stops the reader. It waits at most until the deadlines of the reads in flight, and it
    /// commits nothing. A group reader then leaves its group. Repeated calls are safe.
    ///
    /// With `ServerCommitPolicy.OnRead`, it moves the offset of each partition with undelivered
    /// batches back to its last delivered message. A partition that delivered nothing and
    /// started at `Next` gets the offset before its first undelivered message, or no offset at
    /// offset 0.
    ///
    /// Raises `IggyError` for the first failed store of the `OnRead` close, or else
    /// `PendingOperationError` for a partition with an unknown outcome: abandon its operation
    /// before a new reader reads the partition.
    #[gen_stub(override_return_type(type_repr="collections.abc.Awaitable[None]", imports=("collections.abc")))]
    fn close<'a>(&self, py: Python<'a>) -> PyResult<Bound<'a, PyAny>> {
        let inner = self.inner.clone();
        future_into_py(py, async move {
            let reader = inner.lock().await.take();
            match reader {
                Some(reader) => reader.close().await.map_err(to_runtime_error),
                None => Ok(()),
            }
        })
    }
}

fn closed() -> PyErr {
    PyRuntimeError::new_err("reader has been closed")
}
