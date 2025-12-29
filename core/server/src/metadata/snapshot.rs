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

use crate::metadata::{StreamMeta, UserMeta};
use ahash::AHashMap;
use iggy_common::Identifier;
use std::net::SocketAddr;

/// Bound addresses for transport protocols.
#[derive(Debug, Clone, Default)]
pub struct BoundAddresses {
    pub tcp: Option<SocketAddr>,
    pub http: Option<SocketAddr>,
    pub quic: Option<SocketAddr>,
    pub websocket: Option<SocketAddr>,
}

/// Immutable metadata snapshot that is atomically swapped.
/// All reads see a consistent view of the entire metadata.
#[derive(Debug, Clone)]
pub struct MetadataSnapshot {
    /// Stream metadata indexed by stream ID
    pub streams: AHashMap<usize, StreamMeta>,

    /// Stream name to ID index
    pub stream_index: AHashMap<String, usize>,

    /// User metadata indexed by user ID
    pub users: AHashMap<u32, UserMeta>,

    /// Username to user ID index
    pub user_index: AHashMap<String, u32>,

    /// Bound addresses for transport protocols
    pub bound_addresses: BoundAddresses,
}

impl Default for MetadataSnapshot {
    fn default() -> Self {
        Self::new()
    }
}

impl MetadataSnapshot {
    pub fn new() -> Self {
        Self {
            streams: AHashMap::new(),
            stream_index: AHashMap::new(),
            users: AHashMap::new(),
            user_index: AHashMap::new(),
            bound_addresses: BoundAddresses::default(),
        }
    }

    // Stream operations

    pub fn stream_exists_by_name(&self, name: &str) -> bool {
        self.stream_index.contains_key(name)
    }

    pub fn stream_exists(&self, id: &Identifier) -> bool {
        match id.kind {
            iggy_common::IdKind::Numeric => {
                let id = id.get_u32_value().unwrap() as usize;
                self.streams.contains_key(&id)
            }
            iggy_common::IdKind::String => {
                let name = id.get_string_value().unwrap();
                self.stream_index.contains_key(&name)
            }
        }
    }

    pub fn get_stream(&self, id: &Identifier) -> Option<&StreamMeta> {
        match id.kind {
            iggy_common::IdKind::Numeric => {
                let id = id.get_u32_value().unwrap() as usize;
                self.streams.get(&id)
            }
            iggy_common::IdKind::String => {
                let name = id.get_string_value().unwrap();
                self.stream_index
                    .get(&name)
                    .and_then(|id| self.streams.get(id))
            }
        }
    }

    pub fn get_stream_id(&self, id: &Identifier) -> Option<usize> {
        match id.kind {
            iggy_common::IdKind::Numeric => {
                let stream_id = id.get_u32_value().unwrap() as usize;
                if self.streams.contains_key(&stream_id) {
                    Some(stream_id)
                } else {
                    None
                }
            }
            iggy_common::IdKind::String => {
                let name = id.get_string_value().unwrap();
                self.stream_index.get(&name).copied()
            }
        }
    }

    pub fn get_streams(&self) -> Vec<&StreamMeta> {
        self.streams.values().collect()
    }

    // User operations

    pub fn user_exists_by_name(&self, username: &str) -> bool {
        self.user_index.contains_key(username)
    }

    pub fn user_exists(&self, id: &Identifier) -> bool {
        match id.kind {
            iggy_common::IdKind::Numeric => {
                let id = id.get_u32_value().unwrap();
                self.users.contains_key(&id)
            }
            iggy_common::IdKind::String => {
                let name = id.get_string_value().unwrap();
                self.user_index.contains_key(&name)
            }
        }
    }

    pub fn get_user(&self, id: &Identifier) -> Option<&UserMeta> {
        match id.kind {
            iggy_common::IdKind::Numeric => {
                let id = id.get_u32_value().unwrap();
                self.users.get(&id)
            }
            iggy_common::IdKind::String => {
                let name = id.get_string_value().unwrap();
                self.user_index.get(&name).and_then(|id| self.users.get(id))
            }
        }
    }

    pub fn get_user_id(&self, id: &Identifier) -> Option<u32> {
        match id.kind {
            iggy_common::IdKind::Numeric => {
                let user_id = id.get_u32_value().unwrap();
                if self.users.contains_key(&user_id) {
                    Some(user_id)
                } else {
                    None
                }
            }
            iggy_common::IdKind::String => {
                let name = id.get_string_value().unwrap();
                self.user_index.get(&name).copied()
            }
        }
    }

    pub fn get_users(&self) -> Vec<&UserMeta> {
        self.users.values().collect()
    }

    pub fn get_user_by_username(&self, username: &str) -> Option<&UserMeta> {
        self.user_index
            .get(username)
            .and_then(|id| self.users.get(id))
    }
}
