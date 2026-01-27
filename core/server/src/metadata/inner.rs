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

use crate::metadata::{StreamId, StreamMeta, UserId, UserMeta};
use ahash::{AHashMap, AHashSet};
use iggy_common::{GlobalPermissions, PersonalAccessToken, StreamPermissions};
use slab::Slab;
use std::sync::Arc;

#[derive(Clone, Default)]
pub struct InnerMetadata {
    /// Streams indexed by StreamId (slab-assigned)
    pub streams: Slab<StreamMeta>,

    /// Users indexed by UserId (slab-assigned)
    pub users: Slab<UserMeta>,

    /// Forward indexes (name → ID)
    pub stream_index: AHashMap<Arc<str>, StreamId>,
    pub user_index: AHashMap<Arc<str>, UserId>,

    /// user_id -> (token_hash -> PAT)
    pub personal_access_tokens: AHashMap<UserId, AHashMap<Arc<str>, PersonalAccessToken>>,

    // Permission indexes (auto-maintained by absorb)
    pub users_global_permissions: AHashMap<UserId, GlobalPermissions>,
    pub users_stream_permissions: AHashMap<(UserId, StreamId), StreamPermissions>,

    // Hot-path optimizations for message send/poll
    pub users_can_poll_all_streams: AHashSet<UserId>,
    pub users_can_send_all_streams: AHashSet<UserId>,
    pub users_can_poll_stream: AHashSet<(UserId, StreamId)>,
    pub users_can_send_stream: AHashSet<(UserId, StreamId)>,
}

impl InnerMetadata {
    pub fn new() -> Self {
        Self::default()
    }
}
