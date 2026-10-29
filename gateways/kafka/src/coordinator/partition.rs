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
use tokio::sync::{mpsc, oneshot};
use tokio::time::{Instant, timeout};
use tokio_util::sync::CancellationToken;
use tracing::{debug, info, warn};
use uuid::Uuid;

use crate::bridge::{BridgeError, CoordinatorLink, LoggedRecord};
use crate::coordinator::record::{Body, NO_CHECKPOINT, Record};
use crate::coordinator::replay::{Applied, PartitionState};
use crate::protocol::api::{
    ERROR_COORDINATOR_NOT_AVAILABLE, ERROR_NOT_COORDINATOR, ERROR_UNKNOWN_SERVER_ERROR,
};

/// How long a read waits in Iggy for a record to land.
pub const TAIL_WAIT: Duration = Duration::from_secs(5);

/// How long a caller waits for its append to be read back.
const APPEND_TIMEOUT: Duration = Duration::from_secs(10);

/// How long a load waits to read its own claim back.
const CLAIM_TIMEOUT: Duration = Duration::from_secs(15);

/// How long reads may fail in a row before the partition is given up. Its writes cannot be
/// confirmed meanwhile.
const READ_FAILURE_LIMIT: Duration = Duration::from_secs(10);

/// The bytes of one record at most: below Iggy's largest message, with room for its header.
const MAX_RECORD_BYTES: usize = 60 * 1000 * 1000;

/// What one append sends at most, so a burst of small appends shares one send.
const MAX_BATCH_RECORDS: usize = 1_000;
const MAX_BATCH_BYTES: usize = 8 * 1024 * 1024;

/// The longest time between checkpoints. Well inside the log's retention, so the last
/// checkpoint never expires.
const CHECKPOINT_AGE: Duration = Duration::from_hours(24);

/// Records one checkpoint append carries.
const CHECKPOINT_CHUNK: usize = 500;

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

/// One append waiting for the writer.
struct Append {
    records: Vec<(u128, Bytes)>,
    bytes: usize,
    done: oneshot::Sender<Result<u64, i16>>,
}

/// One coordinator log partition this gateway serves.
pub struct PartitionLog {
    pub partition: u32,
    pub claim: u64,
    node_id: u16,
    /// Valid records after which the writer writes a checkpoint.
    checkpoint_records: u64,
    /// What the log holds, up to the last record read.
    state: tokio::sync::Mutex<PartitionState>,
    /// The checkpoint field of the next record. Set by the reader when a checkpoint completes.
    checkpoint: AtomicU64,
    appends: mpsc::UnboundedSender<Append>,
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
            Ok(Some(record)) if record.checkpoint != NO_CHECKPOINT => record.checkpoint,
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
        Ok(Some(record)) => Some(state.apply(logged.offset, record)),
        Ok(None) => None,
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
        let (appends, queue) = mpsc::unbounded_channel();
        let checkpoint = claimed.state.checkpoint;
        let log = Arc::new(Self {
            partition,
            claim: claimed.claim,
            node_id,
            checkpoint_records,
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

    /// Appends `bodies` as one batch and waits until it is read back. Returns the offset of the
    /// last record.
    ///
    /// # Errors
    ///
    /// The Kafka code for the caller: `NOT_COORDINATOR` when the partition was lost before the
    /// records, `COORDINATOR_NOT_AVAILABLE` when the outcome is not known in time, and
    /// `UNKNOWN_SERVER_ERROR` for a record over Iggy's message size, as Kafka answers a group
    /// too large for its log.
    pub async fn append(&self, bodies: Vec<Body>) -> Result<u64, i16> {
        if !self.is_serving() {
            return Err(ERROR_NOT_COORDINATOR);
        }
        let checkpoint = self.checkpoint.load(Ordering::Acquire);
        let mut records = Vec::with_capacity(bodies.len());
        let mut bytes = 0;
        for body in bodies {
            let record = Record {
                claim: self.claim,
                checkpoint,
                body,
            };
            let payload = match record.encode() {
                Ok(payload) if payload.len() <= MAX_RECORD_BYTES => payload,
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
            bytes += payload.len();
            records.push((Uuid::new_v4().as_u128(), payload));
        }
        if records.is_empty() {
            return Err(ERROR_UNKNOWN_SERVER_ERROR);
        }
        let (done, answer) = oneshot::channel();
        if self
            .appends
            .send(Append {
                records,
                bytes,
                done,
            })
            .is_err()
        {
            return Err(ERROR_NOT_COORDINATOR);
        }
        match timeout(APPEND_TIMEOUT, answer).await {
            Ok(Ok(result)) => result,
            Ok(Err(_dropped)) => Err(ERROR_NOT_COORDINATOR),
            Err(_elapsed) => Err(ERROR_COORDINATOR_NOT_AVAILABLE),
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
        mut queue: mpsc::UnboundedReceiver<Append>,
        stopped: mpsc::UnboundedSender<Stopped>,
    ) {
        let mut checkpointed_at = Instant::now();
        let mut check =
            tokio::time::interval_at(Instant::now() + CHECKPOINT_CHECK, CHECKPOINT_CHECK);
        check.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
        let stop = loop {
            let first = tokio::select! {
                () = self.stop.cancelled() => break None,
                first = queue.recv() => match first {
                    Some(first) => Some(first),
                    None => break None,
                },
                _ = check.tick() => None,
            };
            if let Some(first) = first {
                let mut batch = vec![first];
                let mut records = batch[0].records.len();
                let mut bytes = batch[0].bytes;
                while records < MAX_BATCH_RECORDS && bytes < MAX_BATCH_BYTES {
                    let Ok(next) = queue.try_recv() else {
                        break;
                    };
                    records += next.records.len();
                    bytes += next.bytes;
                    batch.push(next);
                }
                if let Err(error) = self.send(&link, batch).await {
                    warn!(partition = self.partition, %error, "a coordinator log append failed; giving the partition up");
                    break Some(Stop::InDoubt);
                }
            }
            let due = self.with_state(|state| state.since_checkpoint).await
                >= self.checkpoint_records
                || checkpointed_at.elapsed() >= CHECKPOINT_AGE;
            if due {
                match self.write_checkpoint(&link).await {
                    Ok(()) => checkpointed_at = Instant::now(),
                    Err(error) => {
                        warn!(partition = self.partition, %error, "a coordinator log checkpoint failed; giving the partition up");
                        break Some(Stop::InDoubt);
                    }
                }
            }
        };
        // Whatever is still queued learns that it was not written.
        queue.close();
        while let Ok(append) = queue.try_recv() {
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

    /// Sends one batch. Each append is answered when the reader reaches its last record.
    async fn send(&self, link: &CoordinatorLink, batch: Vec<Append>) -> Result<(), BridgeError> {
        let mut records = Vec::new();
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
    /// starts at the begin.
    async fn write_checkpoint(&self, link: &CoordinatorLink) -> Result<(), String> {
        let begin = self
            .append_now(
                link,
                vec![Body::CheckpointBegin {
                    node_id: self.node_id,
                }],
            )
            .await?;
        let bodies = self.with_state(PartitionState::checkpoint_bodies).await;
        for chunk in bodies.chunks(CHECKPOINT_CHUNK) {
            self.append_now(link, chunk.to_vec()).await?;
        }
        self.append_now(link, vec![Body::CheckpointEnd { begin }])
            .await?;
        debug!(
            partition = self.partition,
            begin, "coordinator log checkpoint written"
        );
        Ok(())
    }

    /// Appends `bodies` from the writer itself and waits until they are read back.
    async fn append_now(&self, link: &CoordinatorLink, bodies: Vec<Body>) -> Result<u64, String> {
        let checkpoint = self.checkpoint.load(Ordering::Acquire);
        let mut records = Vec::with_capacity(bodies.len());
        for body in bodies {
            let payload = Record {
                claim: self.claim,
                checkpoint,
                body,
            }
            .encode()
            .map_err(|error| format!("a checkpoint record does not encode: {error}"))?;
            records.push((Uuid::new_v4().as_u128(), payload));
        }
        let (done, answer) = oneshot::channel();
        if let Some((last, _)) = records.last() {
            self.pending().insert(*last, done);
        }
        link.append(self.partition, records)
            .await
            .map_err(|error| error.to_string())?;
        match timeout(APPEND_TIMEOUT, answer).await {
            Ok(Ok(Ok(offset))) => Ok(offset),
            Ok(Ok(Err(_)) | Err(_)) => Err("the partition was lost".to_string()),
            Err(_elapsed) => Err("the checkpoint was not read back in time".to_string()),
        }
    }

    fn pending(
        &self,
    ) -> std::sync::MutexGuard<'_, HashMap<u128, oneshot::Sender<Result<u64, i16>>>> {
        self.pending.lock().unwrap_or_else(PoisonError::into_inner)
    }
}
