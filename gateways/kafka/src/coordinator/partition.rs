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

//! One coordinator log partition that this gateway owns.
//!
//! A load reads the partition from its last checkpoint, then appends a claim and reads on to it.
//! When the claim wins, the partition is served: one task reads every record as it lands, and
//! one task appends. An append answers its caller only once the reader has reached it and found
//! no newer claim before it. So the caller learns that its write counts, and a coordinator that
//! lost the partition learns it from its next write, or sooner from the reader.

use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex, PoisonError};
use std::time::Duration;

use bytes::Bytes;
use iggy::prelude::{IGGY_MESSAGE_HEADER_SIZE, IggyTimestamp, MAX_PAYLOAD_SIZE};
use thiserror::Error;
use tokio::sync::{mpsc, oneshot};
use tokio::time::{Instant, MissedTickBehavior, interval, timeout, timeout_at};
use tokio_util::sync::CancellationToken;
use tracing::{debug, info, warn};
use uuid::Uuid;

use crate::bridge::{BridgeError, CoordinatorLink, LoggedRecord};
use crate::coordinator::record::{Body, NO_CHECKPOINT, Record, RecordError};
use crate::coordinator::replay::{Applied, PartitionState};
use crate::protocol::api::{
    ERROR_COORDINATOR_NOT_AVAILABLE, ERROR_NOT_COORDINATOR, ERROR_UNKNOWN_SERVER_ERROR,
};

/// How long a read waits in Iggy for a record to land.
pub const TAIL_WAIT: Duration = Duration::from_secs(5);

/// How long a caller waits for its append to be read back, unless it gives a deadline.
const APPEND_TIMEOUT: Duration = Duration::from_secs(10);

/// How long a load waits to read its own claim back.
const CLAIM_TIMEOUT: Duration = Duration::from_secs(15);

/// How long reads may fail in a row before the partition is given up. Its writes cannot be
/// confirmed meanwhile.
const READ_FAILURE_LIMIT: Duration = Duration::from_secs(10);

/// What one send carries at most, so a burst of small appends shares one send. Iggy's message
/// size caps it too.
const MAX_BATCH_RECORDS: usize = 1_000;
const MAX_BATCH_BYTES: u64 = 8 * 1024 * 1024;

/// Appends that wait for the writer at most. A caller waits for room until its deadline.
const QUEUED_APPENDS: usize = 1_024;

/// The longest time between checkpoints. Well inside the log's retention, so the last
/// checkpoint never expires.
const CHECKPOINT_AGE: Duration = Duration::from_hours(24);

/// How often an idle writer checks whether a checkpoint is due, so a partition without traffic
/// still writes one inside the log's retention.
const CHECKPOINT_CHECK: Duration = Duration::from_hours(1);

/// Why a served partition stopped.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Stop {
    /// Another gateway claimed it.
    Claimed { node_id: u16 },
    /// An append or the reads failed, so this gateway cannot say what the log holds.
    InDoubt,
    /// Iggy no longer gives it to this gateway, or the gateway shuts down.
    Released,
}

/// A served partition stopped, with the claim it held, so a late stop of an earlier claim is
/// told apart.
#[derive(Debug, Clone, Copy)]
pub struct Stopped {
    pub partition: u32,
    pub claim: u64,
    pub stop: Stop,
}

/// Why the writer gives the partition up.
#[derive(Debug, Error)]
enum WriteError {
    #[error(transparent)]
    Send(#[from] BridgeError),
    #[error("a record does not encode: {0}")]
    Encode(#[from] RecordError),
    #[error("the partition was lost")]
    Lost,
    #[error("the record was not read back in time")]
    NotReadBack,
}

/// One append waiting for the writer.
struct Append {
    records: Vec<(u128, Bytes)>,
    /// What the records take of a send, message headers included.
    bytes: u64,
    done: oneshot::Sender<Result<u64, i16>>,
}

/// One coordinator log partition this gateway serves.
pub struct PartitionLog {
    pub partition: u32,
    pub claim: u64,
    node_id: u16,
    /// Valid records after which the writer writes a checkpoint.
    checkpoint_records: u64,
    /// The message bytes of one send at most.
    max_send_bytes: u64,
    /// What the log holds, up to the last record read.
    state: tokio::sync::Mutex<PartitionState>,
    /// The checkpoint field of the next record. Set by the reader when a checkpoint completes.
    checkpoint: AtomicU64,
    appends: mpsc::Sender<Append>,
    /// The appends sent and not yet read, by the id of their last record.
    pending: Mutex<HashMap<u128, oneshot::Sender<Result<u64, i16>>>>,
    stop: CancellationToken,
}

/// A partition that a load claimed, before it is served.
pub struct Claimed {
    pub state: PartitionState,
    /// The offset after the claim.
    pub next: u64,
    pub claim: u64,
}

/// Reads `partition` from its last checkpoint, then appends a claim and reads on to it.
///
/// # Errors
///
/// `Ok(None)` when another gateway's claim came first. [`BridgeError`] when a call to Iggy fails
/// or the claim does not come back in time.
pub async fn claim(
    link: &CoordinatorLink,
    partition: u32,
    node_id: u16,
) -> Result<Option<Claimed>, BridgeError> {
    let mut state = PartitionState::default();
    let mut next = 0;
    if let Some(last) = link.last(partition).await? {
        next = match Record::decode(&last.payload) {
            Ok(record) if record.checkpoint != NO_CHECKPOINT => record.checkpoint,
            _ => 0,
        };
        while next <= last.offset {
            let records = link.read(partition, next, Duration::ZERO).await?;
            if records.is_empty() {
                break;
            }
            for logged in records {
                next = logged.offset + 1;
                apply_logged(&mut state, partition, &logged);
            }
        }
    }

    let claim = state.claim() + 1;
    let id = Uuid::new_v4().as_u128();
    let record = Record {
        claim,
        checkpoint: state.checkpoint,
        body: Body::Claim { node_id },
    };
    let payload = record.encode().map_err(|error| {
        BridgeError::InvalidConfig(format!("coordinator claim does not encode: {error}"))
    })?;
    link.append(partition, vec![(id, payload)]).await?;

    let deadline = Instant::now() + CLAIM_TIMEOUT;
    loop {
        if Instant::now() >= deadline {
            return Err(BridgeError::Timeout);
        }
        for logged in link.read(partition, next, TAIL_WAIT).await? {
            next = logged.offset + 1;
            let applied = apply_logged(&mut state, partition, &logged);
            if logged.id != id {
                continue;
            }
            return Ok(match applied {
                Some(Applied::Claimed {
                    claim: won,
                    node_id: owner,
                }) if won == claim && owner == node_id => Some(Claimed { state, next, claim }),
                _ => None,
            });
        }
    }
}

/// Decodes and applies one record. A record that does not decode is skipped: a load that stopped
/// on it would leave the partition unserved for good.
fn apply_logged(
    state: &mut PartitionState,
    partition: u32,
    logged: &LoggedRecord,
) -> Option<Applied> {
    match Record::decode(&logged.payload) {
        Ok(record) => Some(state.apply(logged.offset, logged.timestamp, record)),
        Err(error) => {
            warn!(
                partition,
                offset = logged.offset,
                %error,
                "skipping a coordinator log record that does not decode"
            );
            None
        }
    }
}

impl PartitionLog {
    /// Serves a partition that `claimed` won: starts its reader and its writer. `stopped` hears
    /// when it stops on its own.
    pub fn serve(
        link: Arc<CoordinatorLink>,
        partition: u32,
        node_id: u16,
        checkpoint_records: u64,
        claimed: Claimed,
        stopped: mpsc::UnboundedSender<Stopped>,
        parent: &CancellationToken,
    ) -> Arc<Self> {
        let (appends, queue) = mpsc::channel(QUEUED_APPENDS);
        let checkpoint = claimed.state.checkpoint;
        let log = Arc::new(Self {
            partition,
            claim: claimed.claim,
            node_id,
            checkpoint_records,
            max_send_bytes: link.max_send_bytes(),
            state: tokio::sync::Mutex::new(claimed.state),
            checkpoint: AtomicU64::new(checkpoint),
            appends,
            pending: Mutex::new(HashMap::new()),
            stop: parent.child_token(),
        });
        tokio::spawn(Arc::clone(&log).read_on(Arc::clone(&link), claimed.next, stopped.clone()));
        tokio::spawn(Arc::clone(&log).write_on(link, queue, stopped));
        info!(
            partition,
            claim = claimed.claim,
            "serving coordinator log partition"
        );
        log
    }

    /// Whether the partition still serves. False once it stopped for any reason.
    pub fn is_serving(&self) -> bool {
        !self.stop.is_cancelled()
    }

    /// Stops serving. Every append still waiting gets `NOT_COORDINATOR`.
    pub fn stop(&self) {
        self.stop.cancel();
        let pending: Vec<_> = self.pending().drain().map(|(_, done)| done).collect();
        for done in pending {
            let _ = done.send(Err(ERROR_NOT_COORDINATOR));
        }
    }

    /// [`Self::append_by`] within [`APPEND_TIMEOUT`], answering `COORDINATOR_NOT_AVAILABLE` when
    /// late.
    ///
    /// # Errors
    ///
    /// As [`Self::append_by`].
    pub async fn append(&self, bodies: Vec<Body>) -> Result<u64, i16> {
        self.append_by(
            bodies,
            Instant::now() + APPEND_TIMEOUT,
            ERROR_COORDINATOR_NOT_AVAILABLE,
        )
        .await
    }

    /// Appends `bodies` as one batch and waits until it is read back, at most until `deadline`.
    /// Returns the offset of the last record. An append not sent by `deadline` is never sent.
    ///
    /// # Errors
    ///
    /// The Kafka code for the caller: `NOT_COORDINATOR` when the partition was lost before the
    /// records, `late` when the outcome is not known by `deadline`, and `UNKNOWN_SERVER_ERROR`
    /// for a record or an append over Iggy's message size, as Kafka answers a group too large
    /// for its log.
    pub async fn append_by(
        &self,
        bodies: Vec<Body>,
        deadline: Instant,
        late: i16,
    ) -> Result<u64, i16> {
        if !self.is_serving() {
            return Err(ERROR_NOT_COORDINATOR);
        }
        let checkpoint = self.checkpoint.load(Ordering::Acquire);
        let max_record = self.max_record_bytes();
        let mut records = Vec::with_capacity(bodies.len());
        let mut bytes = 0;
        for body in bodies {
            let record = Record {
                claim: self.claim,
                checkpoint,
                body,
            };
            let payload = match record.encode() {
                Ok(payload) if payload.len() <= max_record => payload,
                Ok(payload) => {
                    warn!(
                        partition = self.partition,
                        bytes = payload.len(),
                        group = record.body.group_id().unwrap_or_default(),
                        "a coordinator log record is over Iggy's message size; refusing it"
                    );
                    return Err(ERROR_UNKNOWN_SERVER_ERROR);
                }
                Err(error) => {
                    warn!(partition = self.partition, %error, "a coordinator log record does not encode");
                    return Err(ERROR_UNKNOWN_SERVER_ERROR);
                }
            };
            bytes += send_bytes(&payload);
            records.push((Uuid::new_v4().as_u128(), payload));
        }
        if records.is_empty() {
            return Err(ERROR_UNKNOWN_SERVER_ERROR);
        }
        if bytes > self.max_send_bytes {
            warn!(
                partition = self.partition,
                bytes,
                records = records.len(),
                "a coordinator log append is over Iggy's message size; refusing it"
            );
            return Err(ERROR_UNKNOWN_SERVER_ERROR);
        }
        let (done, answer) = oneshot::channel();
        let append = Append {
            records,
            bytes,
            done,
        };
        match timeout_at(deadline, self.appends.send(append)).await {
            Ok(Ok(())) => {}
            Ok(Err(_closed)) => return Err(ERROR_NOT_COORDINATOR),
            Err(_elapsed) => return Err(late),
        }
        // A caller that stops waiting drops `answer`, and the writer then leaves its append out.
        match timeout_at(deadline, answer).await {
            Ok(Ok(result)) => result,
            Ok(Err(_dropped)) => Err(ERROR_NOT_COORDINATOR),
            Err(_elapsed) => Err(late),
        }
    }

    /// Runs `read` against the state the log holds up to the last record read.
    pub async fn with_state<T>(&self, read: impl FnOnce(&PartitionState) -> T) -> T {
        read(&*self.state.lock().await)
    }

    /// Reads every record as it lands, until the partition stops.
    async fn read_on(
        self: Arc<Self>,
        link: Arc<CoordinatorLink>,
        mut next: u64,
        stopped: mpsc::UnboundedSender<Stopped>,
    ) {
        let mut failing_since: Option<Instant> = None;
        let stop = loop {
            let read = tokio::select! {
                () = self.stop.cancelled() => break None,
                read = link.read(self.partition, next, TAIL_WAIT) => read,
            };
            let records = match read {
                Ok(records) => {
                    failing_since = None;
                    records
                }
                Err(error) => {
                    let since = *failing_since.get_or_insert_with(Instant::now);
                    if since.elapsed() >= READ_FAILURE_LIMIT {
                        warn!(partition = self.partition, %error, "coordinator log reads keep failing; giving the partition up");
                        break Some(Stop::InDoubt);
                    }
                    debug!(partition = self.partition, %error, "coordinator log read failed; retrying");
                    tokio::select! {
                        () = self.stop.cancelled() => break None,
                        () = tokio::time::sleep(Duration::from_millis(200)) => continue,
                    }
                }
            };
            if let Some(stop) = self.take_records(&records).await {
                break Some(stop);
            }
            if let Some(last) = records.last() {
                next = last.offset + 1;
            }
        };
        if let Some(stop) = stop {
            self.stop();
            let _ = stopped.send(Stopped {
                partition: self.partition,
                claim: self.claim,
                stop,
            });
        }
    }

    /// Applies `records` and answers the appends among them. Returns why the partition stops, if
    /// a record says so.
    async fn take_records(&self, records: &[LoggedRecord]) -> Option<Stop> {
        let mut state = self.state.lock().await;
        let mut stop = None;
        for logged in records {
            let applied = apply_logged(&mut state, self.partition, logged);
            if state.checkpoint != NO_CHECKPOINT {
                self.checkpoint.store(state.checkpoint, Ordering::Release);
            }
            let done = self.pending().remove(&logged.id);
            match applied {
                Some(Applied::Claimed { claim, node_id }) if claim > self.claim => {
                    info!(
                        partition = self.partition,
                        claim, node_id, "another gateway claimed the coordinator log partition"
                    );
                    if let Some(done) = done {
                        let _ = done.send(Err(ERROR_NOT_COORDINATOR));
                    }
                    stop = Some(Stop::Claimed { node_id });
                    break;
                }
                // Unreachable in order, since the newer claim is read first. Kept so a record of
                // this gateway is never answered as written when it does not count.
                Some(Applied::Void) if done.is_some() => {
                    if let Some(done) = done {
                        let _ = done.send(Err(ERROR_NOT_COORDINATOR));
                    }
                    let node_id = state.owner.map_or(0, |(_, node_id)| node_id);
                    stop = Some(Stop::Claimed { node_id });
                    break;
                }
                _ => {
                    if let Some(done) = done {
                        let _ = done.send(Ok(logged.offset));
                    }
                }
            }
        }
        drop(state);
        stop
    }

    /// Sends the queued appends in batches, until the partition stops. Writes a checkpoint when
    /// one is due.
    async fn write_on(
        self: Arc<Self>,
        link: Arc<CoordinatorLink>,
        mut queue: mpsc::Receiver<Append>,
        stopped: mpsc::UnboundedSender<Stopped>,
    ) {
        // The first tick comes at once, so a log whose last checkpoint is old gets one now.
        let mut check = interval(CHECKPOINT_CHECK);
        check.set_missed_tick_behavior(MissedTickBehavior::Delay);
        let mut held = None;
        let stop = loop {
            let first = if let Some(first) = held.take() {
                Some(first)
            } else {
                tokio::select! {
                    () = self.stop.cancelled() => break None,
                    first = queue.recv() => match first {
                        Some(first) => Some(first),
                        None => break None,
                    },
                    _ = check.tick() => None,
                }
            };
            if let Some(first) = first {
                let (batch, rest) = self.batch(first, &mut queue);
                held = rest;
                if !batch.is_empty()
                    && let Err(error) = self.send(&link, batch).await
                {
                    warn!(partition = self.partition, %error, "a coordinator log append failed; giving the partition up");
                    break Some(Stop::InDoubt);
                }
            }
            let now = IggyTimestamp::now().as_micros();
            let due = self
                .with_state(|state| {
                    state.checkpoint_due(self.checkpoint_records, CHECKPOINT_AGE, now)
                })
                .await;
            if due && let Err(error) = self.write_checkpoint(&link).await {
                warn!(partition = self.partition, %error, "a coordinator log checkpoint failed; giving the partition up");
                break Some(Stop::InDoubt);
            }
        };
        // Whatever is still queued learns that it was not written.
        queue.close();
        let unsent = held
            .into_iter()
            .chain(std::iter::from_fn(|| queue.try_recv().ok()));
        for append in unsent {
            let _ = append.done.send(Err(ERROR_NOT_COORDINATOR));
        }
        if let Some(stop) = stop {
            self.stop();
            let _ = stopped.send(Stopped {
                partition: self.partition,
                claim: self.claim,
                stop,
            });
        }
    }

    /// `first` and the appends queued behind it, up to what one send carries. An append whose
    /// caller stopped waiting is left out, so a retry does not land behind its own late copy.
    /// Also returns the append that did not fit, which starts the next batch.
    fn batch(
        &self,
        first: Append,
        queue: &mut mpsc::Receiver<Append>,
    ) -> (Vec<Append>, Option<Append>) {
        let max_bytes = MAX_BATCH_BYTES.min(self.max_send_bytes);
        let mut batch: Vec<Append> = Vec::new();
        let (mut records, mut bytes) = (0, 0);
        let mut next = Some(first);
        while let Some(append) = next.take().or_else(|| queue.try_recv().ok()) {
            if append.done.is_closed() {
                continue;
            }
            // An append always fits a send of its own: `append_by` refuses a larger one.
            if !batch.is_empty()
                && (records + append.records.len() > MAX_BATCH_RECORDS
                    || bytes + append.bytes > max_bytes)
            {
                return (batch, Some(append));
            }
            records += append.records.len();
            bytes += append.bytes;
            batch.push(append);
        }
        (batch, None)
    }

    /// Sends one batch. Each append is answered when the reader reaches its last record.
    async fn send(&self, link: &CoordinatorLink, batch: Vec<Append>) -> Result<(), BridgeError> {
        let mut records = Vec::with_capacity(batch.iter().map(|append| append.records.len()).sum());
        let mut answers = Vec::with_capacity(batch.len());
        for append in batch {
            if let Some((last, _)) = append.records.last() {
                answers.push((*last, append.done));
            }
            records.extend(append.records);
        }
        self.pending().extend(answers);
        link.append(self.partition, records).await
    }

    /// Rewrites every live group and offset between a begin and an end record, so a later load
    /// starts at the begin. The records go back to back, and only the end is waited for: the
    /// reader reaches it after all of them.
    async fn write_checkpoint(&self, link: &CoordinatorLink) -> Result<(), WriteError> {
        let checkpoint = self.checkpoint.load(Ordering::Acquire);
        let opening = Body::CheckpointBegin {
            node_id: self.node_id,
        };
        let begin = self
            .append_now(link, vec![self.record(opening, checkpoint)?])
            .await?;
        let bodies = self.with_state(PartitionState::checkpoint_bodies).await;
        let max_record = self.max_record_bytes();
        let max_bytes = MAX_BATCH_BYTES.min(self.max_send_bytes);
        let mut chunk = Vec::new();
        let mut bytes = 0;
        for body in bodies {
            let record = Record {
                claim: self.claim,
                checkpoint,
                body,
            };
            let payload = record.encode()?;
            if payload.len() > max_record {
                // Only after Iggy's message size went down. Leaving it out keeps the partition
                // served, and the group's members rejoin after a takeover.
                warn!(
                    partition = self.partition,
                    bytes = payload.len(),
                    group = record.body.group_id().unwrap_or_default(),
                    "a coordinator log record is over Iggy's message size; the checkpoint leaves it out"
                );
                continue;
            }
            let record_bytes = send_bytes(&payload);
            if !chunk.is_empty()
                && (chunk.len() >= MAX_BATCH_RECORDS || bytes + record_bytes > max_bytes)
            {
                link.append(self.partition, std::mem::take(&mut chunk))
                    .await?;
                bytes = 0;
            }
            chunk.push((Uuid::new_v4().as_u128(), payload));
            bytes += record_bytes;
        }
        if !chunk.is_empty() {
            link.append(self.partition, chunk).await?;
        }
        let end = Body::CheckpointEnd { begin };
        self.append_now(link, vec![self.record(end, checkpoint)?])
            .await?;
        debug!(
            partition = self.partition,
            begin, "coordinator log checkpoint written"
        );
        Ok(())
    }

    /// `body` as a record of this claim, with a new id.
    fn record(&self, body: Body, checkpoint: u64) -> Result<(u128, Bytes), RecordError> {
        let payload = Record {
            claim: self.claim,
            checkpoint,
            body,
        }
        .encode()?;
        Ok((Uuid::new_v4().as_u128(), payload))
    }

    /// Appends `records` from the writer itself and waits until they are read back.
    async fn append_now(
        &self,
        link: &CoordinatorLink,
        records: Vec<(u128, Bytes)>,
    ) -> Result<u64, WriteError> {
        let (done, answer) = oneshot::channel();
        if let Some((last, _)) = records.last() {
            self.pending().insert(*last, done);
        }
        link.append(self.partition, records).await?;
        match timeout(APPEND_TIMEOUT, answer).await {
            Ok(Ok(Ok(offset))) => Ok(offset),
            Ok(Ok(Err(_)) | Err(_)) => Err(WriteError::Lost),
            Err(_elapsed) => Err(WriteError::NotReadBack),
        }
    }

    /// The payload bytes of one record at most: a send of its own, and Iggy's payload limit.
    fn max_record_bytes(&self) -> usize {
        usize::try_from(self.max_send_bytes)
            .unwrap_or(usize::MAX)
            .saturating_sub(IGGY_MESSAGE_HEADER_SIZE)
            .min(MAX_PAYLOAD_SIZE as usize)
    }

    fn pending(
        &self,
    ) -> std::sync::MutexGuard<'_, HashMap<u128, oneshot::Sender<Result<u64, i16>>>> {
        self.pending.lock().unwrap_or_else(PoisonError::into_inner)
    }
}

/// What `payload` takes of a send, with its message header.
const fn send_bytes(payload: &Bytes) -> u64 {
    (payload.len() + IGGY_MESSAGE_HEADER_SIZE) as u64
}

#[cfg(test)]
mod tests {
    use super::*;

    /// A served partition with no reader or writer, for the writer's own steps.
    fn unserved(max_send_bytes: u64) -> (PartitionLog, mpsc::Receiver<Append>) {
        let (appends, queue) = mpsc::channel(QUEUED_APPENDS);
        let log = PartitionLog {
            partition: 0,
            claim: 1,
            node_id: 1,
            checkpoint_records: 10,
            max_send_bytes,
            state: tokio::sync::Mutex::new(PartitionState::default()),
            checkpoint: AtomicU64::new(NO_CHECKPOINT),
            appends,
            pending: Mutex::new(HashMap::new()),
            stop: CancellationToken::new(),
        };
        (log, queue)
    }

    fn append(id: u128, bytes: u64) -> (Append, oneshot::Receiver<Result<u64, i16>>) {
        let (done, answer) = oneshot::channel();
        let append = Append {
            records: vec![(id, Bytes::new())],
            bytes,
            done,
        };
        (append, answer)
    }

    #[test]
    fn given_queued_appends_should_fill_one_send_and_leave_abandoned_ones_out() {
        let (log, mut queue) = unserved(100);
        let (first, _first) = append(1, 40);
        let (abandoned, gone) = append(2, 40);
        drop(gone);
        let (second, _second) = append(3, 50);
        let (third, _third) = append(4, 20);
        for queued in [abandoned, second, third] {
            assert!(log.appends.try_send(queued).is_ok());
        }

        let (batch, held) = log.batch(first, &mut queue);
        let ids: Vec<u128> = batch.iter().map(|append| append.records[0].0).collect();
        assert_eq!(ids, [1, 3]);
        assert_eq!(
            held.map(|append| append.records[0].0),
            Some(4),
            "the next send starts with what did not fit"
        );
    }
}
