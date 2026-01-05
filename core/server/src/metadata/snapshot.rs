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

use crate::metadata::{SLAB_SEGMENT_SIZE, StreamId, StreamMeta, UserId, UserMeta};
use iggy_common::PersonalAccessToken;
use iggy_common::collections::SegmentedSlab;
use imbl::HashMap as ImHashMap;
use std::sync::Arc;

/// Immutable metadata snapshot with hierarchical structure.
/// Streams contain topics, topics contain partitions and consumer groups.
/// Uses SegmentedSlab for O(1) access with structural sharing.
#[derive(Clone, Default)]
pub struct InnerMetadata {
    /// Streams indexed by StreamId (slab-assigned)
    pub streams: SegmentedSlab<StreamMeta, SLAB_SEGMENT_SIZE>,

    /// Users indexed by UserId (slab-assigned)
    pub users: SegmentedSlab<UserMeta, SLAB_SEGMENT_SIZE>,

    /// Forward indexes (name → ID)
    pub stream_index: ImHashMap<Arc<str>, StreamId>,
    pub user_index: ImHashMap<Arc<str>, UserId>,

    /// user_id -> (token_hash -> PAT)
    pub personal_access_tokens: ImHashMap<UserId, ImHashMap<Arc<str>, PersonalAccessToken>>,

    /// Monotonic revision for cache invalidation
    pub revision: u64,
}

impl InnerMetadata {
    pub fn new() -> Self {
        Self::default()
    }
}
