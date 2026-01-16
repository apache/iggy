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

use crate::metadata::{ClientId, ConsumerGroupMemberId, PartitionId};
use std::sync::Arc;
use std::sync::atomic::AtomicUsize;

#[derive(Clone, Debug)]
pub struct ConsumerGroupMemberMeta {
    pub id: ConsumerGroupMemberId,
    pub client_id: ClientId,
    pub partitions: Vec<PartitionId>,
    pub partition_index: Arc<AtomicUsize>,
}

impl ConsumerGroupMemberMeta {
    pub fn new(id: ConsumerGroupMemberId, client_id: ClientId) -> Self {
        Self {
            id,
            client_id,
            partitions: Vec::new(),
            partition_index: Arc::new(AtomicUsize::new(0)),
        }
    }
}
