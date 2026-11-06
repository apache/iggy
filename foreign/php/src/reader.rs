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

use ext_php_rs::{exception::PhpResult, php_class, php_impl, types::Zval};
use iggy::prelude::PartitionReader as RustPartitionReader;

use crate::client::optional_unsigned;
use crate::error::to_php_exception;
use crate::receive_message::{Checkpoint, PolledMessages, PollingStrategy};
use crate::runtime::runtime;

const CLOSED: &str = "the reader is closed";

/// Reads the partitions of a topic in batches, with routing, prefetch and the position of each
/// partition inside. Build one with Client::reader() or Client::groupReader().
///
/// Each partition delivers its batches in offset order, from its first read, from each seek()
/// and from each new assignment to a group member. The `commit` of its PollOptions says when
/// offsets are stored. With CommitPolicy::manual() and CommitPolicy::auto(), commit() stores the
/// offset of a checkpoint, and CommitPolicy::auto() also stores the latest counted position by
/// itself, which mark() counts under CommitAfter::Marked. With CommitPolicy::onRead(), the server
/// stores the offset of each reply, the reader reads only while next() waits and then fills its
/// budget (one fetch per primary that may wait for messages, the partitions with messages left
/// behind spread over the other slots), a seek() to an offset stores the offset before it, and
/// close() moves the offset back to the last delivered message. Destroying the reader without
/// close() stops it without waiting, moves no offset back and does not leave the group.
#[php_class]
#[php(name = "Iggy\\PartitionReader")]
pub struct PartitionReader {
    inner: Option<RustPartitionReader>,
}

impl From<RustPartitionReader> for PartitionReader {
    fn from(inner: RustPartitionReader) -> Self {
        Self { inner: Some(inner) }
    }
}

#[php_impl]
impl PartitionReader {
    /// The next batch of one partition. It is never empty. Without `timeout_micros`, the call
    /// blocks until a batch arrives. With it, the call returns null when no batch arrives in
    /// that time, and a later batch stays in the reader for the next call. A timeout of 0 does
    /// not wait: it returns a batch only if one has already arrived.
    ///
    /// A failed read throws Iggy\Exception\ReadException. The reader stays usable and retries
    /// by itself. HistoryUnavailable (87) stops its partition until seek(). Under
    /// CommitPolicy::onRead(), an unknown outcome stops its partition until the operation is
    /// abandoned on the client and the partition is sought.
    pub fn next(&mut self, timeout_micros: Option<&Zval>) -> PhpResult<Option<PolledMessages>> {
        let timeout = optional_unsigned::<u64>(timeout_micros, "timeout_micros")?;
        let reader = self.open_mut()?;
        // The timer needs the runtime context, so it is built inside `block_on`. `next` is
        // cancel safe, so dropping it at the timeout leaves a late batch in the reader.
        let read = runtime().block_on(async {
            match timeout {
                Some(micros) => tokio::time::timeout(Duration::from_micros(micros), reader.next())
                    .await
                    .ok(),
                None => Some(reader.next().await),
            }
        });
        read.transpose()
            .map(|batch| batch.map(PolledMessages::from))
            .map_err(to_php_exception)
    }

    /// Stores the offset of a checkpoint that this reader delivered. The commits of one
    /// partition go out in order.
    ///
    /// A checkpoint of an older generation of its partition, or at or below the last commit of
    /// its generation, stores nothing. A checkpoint that this reader did not deliver, and every
    /// commit under CommitPolicy::onRead(), fail with InvalidCommand (3).
    pub fn commit(&self, checkpoint: &Checkpoint) -> PhpResult {
        let reader = self.open()?;
        runtime()
            .block_on(reader.commit(checkpoint.inner))
            .map_err(to_php_exception)
    }

    /// Counts a checkpoint that this reader delivered under CommitPolicy::auto() and stores
    /// nothing now: the policy stores the latest counted position of its partition. Without
    /// CommitPolicy::auto(), and for a checkpoint that this reader did not deliver, it throws
    /// InvalidCommand (3).
    pub fn mark(&self, checkpoint: &Checkpoint) -> PhpResult {
        self.open()?
            .mark(checkpoint.inner)
            .map_err(to_php_exception)
    }

    /// Sets the next read position of a partition and starts a new generation of it. It drops
    /// the queued batches of the partition, ignores its late replies and resumes a stopped
    /// partition.
    ///
    /// Under CommitPolicy::onRead(), a seek to an offset above 0 first stores the offset before
    /// it, under the history of a continuation or else of the last delivered message, so a
    /// restart resumes at the new position. A failed store fails the seek with its exception and
    /// nothing moves. Every other seek stores nothing. The new position waits for the end of the
    /// committing read in flight, whose reply the new generation drops.
    ///
    /// A partition that the reader does not read, and a continuation of another partition,
    /// fail with InvalidCommand (3). Under CommitPolicy::onRead(), a read with an unknown
    /// outcome fails with PartitionLaneBlocked (14009): abandon its operation first.
    pub fn seek(&mut self, partition_id: u32, start: &PollingStrategy) -> PhpResult {
        let reader = self.open_mut()?;
        runtime()
            .block_on(reader.seek(partition_id, start.into()))
            .map_err(to_php_exception)
    }

    /// The partitions that the reader reads: those of a standalone consumer, or the current
    /// assignment of a group member.
    pub fn partitions(&self) -> PhpResult<Vec<u32>> {
        Ok(self.open()?.partitions())
    }

    /// Stops the reader. It waits at most until the deadlines of the reads in flight. Under
    /// CommitPolicy::manual() it commits nothing, and under CommitPolicy::auto() it stores the
    /// latest counted position of each partition. A group reader then leaves its group. A second call does nothing, and
    /// every other method of a closed reader throws.
    ///
    /// Under CommitPolicy::onRead(), it moves the offset of each partition with undelivered
    /// batches back to its last delivered message, and the first failed store is thrown.
    /// Otherwise a partition with an unknown outcome throws PartitionLaneBlocked (14009):
    /// abandon its operation before a new reader reads the partition.
    pub fn close(&mut self) -> PhpResult {
        match self.inner.take() {
            Some(reader) => runtime().block_on(reader.close()).map_err(to_php_exception),
            None => Ok(()),
        }
    }
}

impl PartitionReader {
    fn open(&self) -> PhpResult<&RustPartitionReader> {
        self.inner.as_ref().ok_or_else(|| to_php_exception(CLOSED))
    }

    fn open_mut(&mut self) -> PhpResult<&mut RustPartitionReader> {
        self.inner.as_mut().ok_or_else(|| to_php_exception(CLOSED))
    }
}
