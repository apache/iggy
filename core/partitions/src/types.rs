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

use iggy_common::PollingStrategy;

/// Arguments for polling messages from a partition.
#[derive(Debug, Clone)]
pub struct PollingArgs {
    pub strategy: PollingStrategy,
    pub count: u32,
    pub auto_commit: bool,
}

impl PollingArgs {
    pub fn new(strategy: PollingStrategy, count: u32, auto_commit: bool) -> Self {
        Self {
            strategy,
            count,
            auto_commit,
        }
    }
}

/// Metadata returned from a poll operation.
#[derive(Debug, Clone)]
pub struct PollMetadata {
    pub partition_id: u32,
    pub current_offset: u64,
}

impl PollMetadata {
    pub fn new(partition_id: u32, current_offset: u64) -> Self {
        Self {
            partition_id,
            current_offset,
        }
    }
}

/// Result of sending messages.
#[derive(Debug)]
pub struct SendMessagesResult {
    pub messages_count: u32,
}

/// Consumer identification for offset operations.
// TODO(hubcio): unify with server's `PollingConsumer` in `streaming/polling_consumer.rs`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PollingConsumer {
    /// Regular consumer with (consumer_id, partition_id)
    Consumer(usize, usize),
    /// Consumer group with (group_id, member_id)
    ConsumerGroup(usize, usize),
}
