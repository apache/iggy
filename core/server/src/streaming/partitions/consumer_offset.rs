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

use std::sync::atomic::AtomicU64;

use crate::streaming::polling_consumer::ConsumerGroupId;
use iggy_common::ConsumerKind;

#[derive(Debug)]
pub struct ConsumerOffset {
    pub kind: ConsumerKind,
    pub consumer_id: u32,
    pub offset: AtomicU64,
    pub path: String,
}

impl Clone for ConsumerOffset {
    fn clone(&self) -> Self {
        Self {
            kind: self.kind,
            consumer_id: self.consumer_id,
            offset: AtomicU64::new(0),
            path: self.path.clone(),
        }
    }
}

impl ConsumerOffset {
    pub fn default_for_consumer(consumer_id: u32, path: &str) -> Self {
        Self {
            kind: ConsumerKind::Consumer,
            consumer_id,
            offset: AtomicU64::new(0),
            path: format!("{path}/{consumer_id}"),
        }
    }

    pub fn default_for_consumer_group(consumer_group_id: ConsumerGroupId, path: &str) -> Self {
        Self {
            kind: ConsumerKind::ConsumerGroup,
            consumer_id: consumer_group_id.0 as u32,
            offset: AtomicU64::new(0),
            path: format!("{path}/{}", consumer_group_id.0),
        }
    }

    pub fn new(kind: ConsumerKind, consumer_id: u32, offset: u64, path: String) -> Self {
        Self {
            kind,
            consumer_id,
            offset: AtomicU64::new(offset),
            path,
        }
    }
}
