/* Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

use super::message_batch::{RetainedMessageBatch, RETAINED_BATCH_HEADER_LEN};
use crate::streaming::models::messages::RetainedMessage;
use bytes::BytesMut;
use iggy::utils::byte_size::IggyByteSize;
use iggy::utils::sizeable::Sizeable;
use std::sync::Arc;

#[derive(Debug)]
pub struct BatchAccumulator {
    base_offset: u64,
    current_size: IggyByteSize,
    current_offset: u64,
    current_timestamp: u64,
    messages: Vec<Arc<RetainedMessage>>,
}

impl BatchAccumulator {
    pub fn new(base_offset: u64, capacity: usize) -> Self {
        Self {
            base_offset,
            current_size: IggyByteSize::from(0),
            current_offset: 0,
            current_timestamp: 0,
            messages: Vec::with_capacity(capacity),
        }
    }

    pub fn append(&mut self, batch_size: IggyByteSize, items: &[Arc<RetainedMessage>]) {
        assert!(!items.is_empty());
        self.current_size += batch_size;
        self.current_offset = items.last().unwrap().offset;
        self.current_timestamp = items.last().unwrap().timestamp;
        self.messages.extend(items.iter().cloned());
    }

    pub fn get_messages_by_offset(
        &self,
        start_offset: u64,
        end_offset: u64,
    ) -> Vec<Arc<RetainedMessage>> {
        let start_idx = self
            .messages
            .partition_point(|msg| msg.offset < start_offset);
        let end_idx = self
            .messages
            .partition_point(|msg| msg.offset <= end_offset);
        self.messages[start_idx..end_idx].to_vec()
    }

    pub fn get_messages_by_timestamp(
        &self,
        start_timestamp: u64,
        count: usize,
    ) -> Vec<Arc<RetainedMessage>> {
        let start_idx = self
            .messages
            .partition_point(|msg| msg.timestamp < start_timestamp);
        let end_idx = std::cmp::min(start_idx + count, self.messages.len());
        self.messages[start_idx..end_idx].to_vec()
    }

    pub fn is_empty(&self) -> bool {
        self.messages.is_empty()
    }

    pub fn unsaved_messages_count(&self) -> usize {
        self.messages.len()
    }

    pub fn batch_max_offset(&self) -> u64 {
        self.current_offset
    }

    pub fn batch_max_timestamp(&self) -> u64 {
        self.current_timestamp
    }

    pub fn batch_base_offset(&self) -> u64 {
        self.base_offset
    }

    pub fn materialize_batch_and_update_state(&mut self) -> RetainedMessageBatch {
        let batch_base_offset = self.base_offset;
        let batch_last_offset_delta = (self.current_offset - self.base_offset) as u32;

        let num_messages = self.messages.len();
        let last_batch_timestamp = if num_messages > 0 {
            self.messages[num_messages - 1].timestamp
        } else {
            0
        };

        let messages = std::mem::take(&mut self.messages);
        let mut bytes = BytesMut::with_capacity(self.current_size.as_bytes_u64() as usize);
        for message in messages {
            message.extend(&mut bytes);
        }

        self.base_offset = 0;
        self.current_size = IggyByteSize::from(0);
        self.current_offset = 0;
        self.current_timestamp = 0;

        let batch_payload = bytes.freeze();
        let batch_payload_len = IggyByteSize::from(batch_payload.len() as u64);
        RetainedMessageBatch::new(
            batch_base_offset,
            batch_last_offset_delta,
            last_batch_timestamp,
            batch_payload_len,
            batch_payload,
        )
    }
}

impl Sizeable for BatchAccumulator {
    fn get_size_bytes(&self) -> IggyByteSize {
        self.current_size + RETAINED_BATCH_HEADER_LEN.into()
    }
}
