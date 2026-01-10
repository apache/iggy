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

use crate::metadata::topic::TopicMeta;
use crate::metadata::{SLAB_SEGMENT_SIZE, StreamId, TopicId};
use crate::streaming::stats::StreamStats;
use iggy_common::IggyTimestamp;
use iggy_common::collections::SegmentedSlab;
use imbl::HashMap as ImHashMap;
use std::sync::Arc;

/// Stream metadata stored in the shared snapshot.
#[derive(Clone, Debug)]
pub struct StreamMeta {
    pub id: StreamId,
    pub name: Arc<str>,
    pub created_at: IggyTimestamp,
    pub stats: Arc<StreamStats>,
    pub topics: SegmentedSlab<TopicMeta, SLAB_SEGMENT_SIZE>,
    pub topic_index: ImHashMap<Arc<str>, TopicId>,
}

impl StreamMeta {
    pub fn new(id: StreamId, name: Arc<str>, created_at: IggyTimestamp) -> Self {
        Self {
            id,
            name,
            created_at,
            stats: Arc::new(StreamStats::default()),
            topics: SegmentedSlab::new(),
            topic_index: ImHashMap::new(),
        }
    }

    pub fn with_stats(
        id: StreamId,
        name: Arc<str>,
        created_at: IggyTimestamp,
        stats: Arc<StreamStats>,
    ) -> Self {
        Self {
            id,
            name,
            created_at,
            stats,
            topics: SegmentedSlab::new(),
            topic_index: ImHashMap::new(),
        }
    }
}
