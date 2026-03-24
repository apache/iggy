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

use iggy_common::header::ReplyHeader;
use iggy_common::message::Message;
use std::collections::HashMap;

/// Per-client entry in the clients table (VR paper Section 4, Figure 2).
///
/// Tracks the most recent committed request for a client and caches
/// the reply so it can be re-sent on duplicate requests.
#[derive(Debug)]
pub struct ClientEntry {
    /// The request number of the most recent committed request.
    pub request: u64,
    /// The cached reply for that request (header + body).
    /// `None` if the request has been prepared but not yet committed.
    pub reply: Option<Message<ReplyHeader>>,
}

/// Result of checking a request against the clients table.
pub enum RequestStatus {
    /// Request not seen before — proceed with consensus.
    New,
    /// Request already committed — re-send cached reply.
    Duplicate(Message<ReplyHeader>),
    /// Request is in the pipeline awaiting commit — drop (client should wait).
    InProgress,
}

/// VSR client-table: tracks per-client request state for duplicate detection
/// and reply caching.
///
/// Currently handles the two core responsibilities:
/// 1. **Duplicate detection** (`check_request`): compare incoming request number
///    against the client-table; drop stale/duplicate requests, re-send cached replies.
/// 2. **Reply caching** (`commit_reply`): after commit, store the result so it can
///    be re-sent if the client retransmits.
///
/// ## Future work
///
/// TODO: In-flight request tracking (`register` / `pending` map). When the SDK or
/// connection handler needs to await a committed reply (e.g. for request pipelining
/// or cross-shard request submission), add a mechanism to register interest in a
/// pending request and get notified on commit. The current iggy SDK uses synchronous
/// request-response per connection.
/// So we will get back to this once the SDK evolves.
#[derive(Debug)]
pub struct ClientsTable {
    /// Committed client state, keyed by `client_id` (u128).
    entries: HashMap<u128, ClientEntry>,
    /// Maximum number of clients tracked. When exceeded, the client with
    /// the oldest committed request is evicted (deterministic across replicas).
    max_clients: usize,
}

impl ClientsTable {
    #[must_use]
    pub fn new(max_clients: usize) -> Self {
        Self {
            entries: HashMap::new(),
            max_clients,
        }
    }

    /// Check a request against the table.
    ///
    /// Returns:
    /// - [`RequestStatus::New`] — not seen before, proceed with consensus
    /// - [`RequestStatus::Duplicate`] — already committed, re-send cached reply
    /// - [`RequestStatus::InProgress`] — stale or already-committed without cached reply, drop
    #[must_use]
    pub fn check_request(&self, client_id: u128, request: u64) -> RequestStatus {
        if let Some(entry) = self.entries.get(&client_id) {
            if request < entry.request {
                // Stale request — already superseded by a newer commit.
                return RequestStatus::InProgress;
            }
            if request == entry.request {
                // Already committed — re-send cached reply if available
                if let Some(ref reply) = entry.reply {
                    return RequestStatus::Duplicate(reply.clone());
                }
                // Committed but no cached reply (shouldn't happen in normal flow)
                return RequestStatus::InProgress;
            }
        }

        RequestStatus::New
    }

    /// Record a committed reply and cache it.
    ///
    /// 1. Updates the client entry with the new request number and cached reply
    /// 2. Evicts oldest client if table exceeds `max_clients`
    ///
    /// Called in `on_ack` after `build_reply_message`.
    pub fn commit_reply(&mut self, client_id: u128, reply: Message<ReplyHeader>) {
        let request = reply.header().request;

        self.entries.insert(
            client_id,
            ClientEntry {
                request,
                reply: Some(reply),
            },
        );

        if self.entries.len() > self.max_clients {
            self.evict_oldest();
        }
    }

    /// Evict the client with the oldest committed request.
    /// Deterministic: all replicas evict the same client since they
    /// share the same committed state.
    fn evict_oldest(&mut self) {
        if let Some((&oldest_client, _)) =
            self.entries.iter().min_by_key(|(_, entry)| entry.request)
        {
            self.entries.remove(&oldest_client);
        }
    }

    /// Get the cached reply for a client (for duplicate re-sends).
    #[must_use]
    pub fn get_reply(&self, client_id: u128) -> Option<&Message<ReplyHeader>> {
        self.entries
            .get(&client_id)
            .and_then(|entry| entry.reply.as_ref())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use iggy_common::header::{Command2, Operation};

    fn make_reply(request: u64) -> Message<ReplyHeader> {
        let header_size = std::mem::size_of::<ReplyHeader>();
        let mut buffer = bytes::BytesMut::zeroed(header_size);
        let header =
            bytemuck::checked::try_from_bytes_mut::<ReplyHeader>(&mut buffer[..header_size])
                .expect("zeroed bytes are valid");
        *header = ReplyHeader {
            request,
            command: Command2::Reply,
            operation: Operation::SendMessages,
            ..ReplyHeader::default()
        };
        Message::<ReplyHeader>::from_bytes(buffer.freeze()).expect("test reply must be valid")
    }

    #[test]
    fn check_request_new() {
        let table = ClientsTable::new(10);
        assert!(matches!(table.check_request(1, 1), RequestStatus::New));
    }

    #[test]
    fn check_request_duplicate_after_commit() {
        let mut table = ClientsTable::new(10);
        let reply = make_reply(1);
        table.commit_reply(1, reply);

        match table.check_request(1, 1) {
            RequestStatus::Duplicate(cached) => {
                assert_eq!(cached.header().request, 1);
            }
            _ => panic!("expected Duplicate"),
        }
    }

    #[test]
    fn check_request_stale() {
        let mut table = ClientsTable::new(10);
        let reply = make_reply(5);
        table.commit_reply(1, reply);

        // Request 3 is stale (< committed request 5)
        assert!(matches!(
            table.check_request(1, 3),
            RequestStatus::InProgress
        ));
    }

    #[test]
    fn commit_caches_reply() {
        let mut table = ClientsTable::new(10);
        let reply = make_reply(1);
        table.commit_reply(1, reply);

        let cached = table.get_reply(1).expect("should have cached reply");
        assert_eq!(cached.header().request, 1);
    }

    #[test]
    fn eviction_removes_oldest() {
        let mut table = ClientsTable::new(2);

        // Commit 3 clients with requests 1, 2, 3
        table.commit_reply(100, make_reply(1));
        table.commit_reply(200, make_reply(2));
        table.commit_reply(300, make_reply(3));

        // max_clients=2, so client 100 (oldest request=1) should be evicted
        assert!(table.get_reply(100).is_none());
        assert!(table.get_reply(200).is_some());
        assert!(table.get_reply(300).is_some());
    }

    #[test]
    fn new_request_after_commit_is_new() {
        let mut table = ClientsTable::new(10);
        let reply = make_reply(1);
        table.commit_reply(1, reply);

        // Request 2 is newer than committed request 1
        assert!(matches!(table.check_request(1, 2), RequestStatus::New));
    }
}
