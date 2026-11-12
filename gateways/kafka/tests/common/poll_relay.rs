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

//! A relay between a bridge and a test `iggy-server` that holds back the polls of some partitions,
//! as a node does that stops answering, or sends replies slowly, as a slow link does.

#![allow(dead_code)]

use std::mem::offset_of;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::Duration;

use iggy_binary_protocol::WireDecode;
use iggy_binary_protocol::codes::{POLL_MESSAGES_CODE, POLL_MESSAGES_DEFERRED_CODE};
use iggy_binary_protocol::consensus::{HEADER_SIZE, Operation, RequestHeader};
use iggy_binary_protocol::requests::messages::{DeferredPollMessagesRequest, PollMessagesRequest};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::tcp::{OwnedReadHalf, OwnedWriteHalf};
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::{mpsc, watch};

use iggy_gateway_kafka::bridge::IggyBridgeConfig;

use super::iggy_server::TestServer;

/// Bytes the relay passes back at a time.
const CHUNK: usize = 64 * 1024;

/// A relay in front of a test server for every Iggy connection of one bridge.
///
/// While [`Self::stall`] holds, it keeps each poll of the stalled partitions, and sends the kept
/// ones on when the stall ends. Iggy then answers them late, on the connections they came in on.
/// While [`Self::pace`] holds a rate, it passes replies back at that many bytes a second.
pub struct PollRelay {
    address: String,
    stall: watch::Sender<bool>,
    pace: watch::Sender<Option<u64>>,
    connections: Arc<AtomicUsize>,
}

impl PollRelay {
    /// A relay to `server` that can stall the polls of the `stalled` partitions.
    pub async fn start(server: &TestServer, stalled: &'static [u32]) -> Self {
        let listener = TcpListener::bind("127.0.0.1:0").await.expect("relay");
        let address = listener.local_addr().expect("relay address").to_string();
        let upstream = server.test_config().address;
        let (stall, stalls) = watch::channel(false);
        let (pace, paces) = watch::channel(None);
        let connections = Arc::new(AtomicUsize::new(0));
        let accepted = Arc::clone(&connections);
        tokio::spawn(async move {
            while let Ok((inbound, _)) = listener.accept().await {
                accepted.fetch_add(1, Ordering::SeqCst);
                let outbound = TcpStream::connect(&upstream).await.expect("upstream");
                let (stalls, paces) = (stalls.clone(), paces.clone());
                tokio::spawn(async move {
                    let (requests, mut replies) = inbound.into_split();
                    let (mut responses, mut writes) = outbound.into_split();
                    tokio::select! {
                        () = pass_requests(requests, &mut writes, stalled, stalls) => {}
                        () = pass_replies(&mut responses, &mut replies, paces) => {}
                    }
                });
            }
        });
        Self {
            address,
            stall,
            pace,
            connections,
        }
    }

    /// `server`'s bridge config, with every Iggy connection through this relay.
    pub fn config(&self, server: &TestServer) -> IggyBridgeConfig {
        let mut config = server.test_config();
        config.address.clone_from(&self.address);
        config
    }

    /// Starts or ends the stall. Ending it sends on every poll it kept.
    pub fn stall(&self, stall: bool) {
        self.stall.send_replace(stall);
    }

    /// Passes replies back at `bytes_per_second`, or as they come with `None`.
    pub fn pace(&self, bytes_per_second: Option<u64>) {
        self.pace.send_replace(bytes_per_second);
    }

    /// Connections opened through the relay so far.
    pub fn connections(&self) -> usize {
        self.connections.load(Ordering::SeqCst)
    }
}

/// Passes request frames on until a side closes. While `stalls` holds `true`, keeps each poll of
/// a `stalled` partition, and sends the kept ones on once it turns `false`.
async fn pass_requests(
    requests: OwnedReadHalf,
    writes: &mut OwnedWriteHalf,
    stalled: &[u32],
    mut stalls: watch::Receiver<bool>,
) {
    // Frames come through a channel, so the end of a stall never wakes the relay mid-frame.
    let (frames, mut incoming) = mpsc::channel(64);
    let reader = tokio::spawn(read_frames(requests, frames));
    let mut kept: Vec<Vec<u8>> = Vec::new();
    loop {
        let frames = tokio::select! {
            frame = incoming.recv() => {
                let Some(frame) = frame else { break };
                let polled = polled_partition(&frame);
                if *stalls.borrow() && polled.is_some_and(|polled| stalled.contains(&polled)) {
                    kept.push(frame);
                    continue;
                }
                vec![frame]
            }
            changed = stalls.changed() => {
                if changed.is_err() {
                    break;
                }
                if *stalls.borrow_and_update() {
                    continue;
                }
                std::mem::take(&mut kept)
            }
        };
        for frame in frames {
            if writes.write_all(&frame).await.is_err() {
                reader.abort();
                return;
            }
        }
    }
    reader.abort();
}

/// Reads whole request frames into `frames` until a side closes.
async fn read_frames(mut requests: OwnedReadHalf, frames: mpsc::Sender<Vec<u8>>) {
    let size_at = offset_of!(RequestHeader, size);
    loop {
        let mut frame = vec![0; HEADER_SIZE];
        if requests.read_exact(&mut frame).await.is_err() {
            return;
        }
        let size = u32::from_le_bytes(frame[size_at..size_at + 4].try_into().expect("4 bytes"));
        frame.resize(usize::try_from(size).expect("a frame size"), 0);
        if requests
            .read_exact(&mut frame[HEADER_SIZE..])
            .await
            .is_err()
            || frames.send(frame).await.is_err()
        {
            return;
        }
    }
}

/// Passes replies back until a side closes, at the bytes a second that `paces` holds, if any.
async fn pass_replies(
    responses: &mut OwnedReadHalf,
    replies: &mut OwnedWriteHalf,
    paces: watch::Receiver<Option<u64>>,
) {
    let mut chunk = vec![0; CHUNK];
    loop {
        let read = match responses.read(&mut chunk).await {
            Ok(0) | Err(_) => return,
            Ok(read) => read,
        };
        if replies.write_all(&chunk[..read]).await.is_err() {
            return;
        }
        let pace = *paces.borrow();
        if let Some(bytes_per_second) = pace {
            let read = u64::try_from(read).expect("a chunk size");
            tokio::time::sleep(Duration::from_micros(read * 1_000_000 / bytes_per_second)).await;
        }
    }
}

/// The partition that a request frame polls, if it is a poll. The SDK sends a poll with options,
/// as the bridge's are, as a deferred poll.
fn polled_partition(frame: &[u8]) -> Option<u32> {
    let (header, body) = frame.split_at_checked(HEADER_SIZE)?;
    if header[offset_of!(RequestHeader, operation)] != Operation::NonReplicated as u8 {
        return None;
    }
    let code_at = offset_of!(RequestHeader, reserved);
    match u32::from_le_bytes(header[code_at..code_at + 4].try_into().ok()?) {
        POLL_MESSAGES_CODE => PollMessagesRequest::decode(body).ok()?.0.partition_id,
        POLL_MESSAGES_DEFERRED_CODE => {
            DeferredPollMessagesRequest::decode(body)
                .ok()?
                .0
                .poll
                .partition_id
        }
        _ => None,
    }
}
