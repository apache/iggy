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

use crate::streaming::batching::batch_filter::BatchItemizer;
use crate::streaming::batching::iterator::IntoMessagesIterator;
use crate::streaming::models::messages::RetainedMessage;
use bytes::Bytes;
use iggy::utils::{byte_size::IggyByteSize, sizeable::Sizeable};

pub const RETAINED_BATCH_HEADER_LEN: u64 = 8 + 8 + 4 + 4;

#[derive(Debug)]
pub struct RetainedMessageBatch {
    pub base_offset: u64,
    pub last_offset_delta: u32,
    pub max_timestamp: u64,
    pub length: IggyByteSize,
    pub bytes: Bytes,
}

impl RetainedMessageBatch {
    pub fn new(
        base_offset: u64,
        last_offset_delta: u32,
        max_timestamp: u64,
        length: IggyByteSize,
        bytes: Bytes,
    ) -> Self {
        RetainedMessageBatch {
            base_offset,
            last_offset_delta,
            max_timestamp,
            length,
            bytes,
        }
    }

    pub fn is_contained_or_overlapping_within_offset_range(
        &self,
        start_offset: u64,
        end_offset: u64,
    ) -> bool {
        (self.base_offset <= end_offset && self.get_last_offset() >= end_offset)
            || (self.base_offset <= start_offset && self.get_last_offset() <= end_offset)
            || (self.base_offset <= end_offset && self.get_last_offset() >= start_offset)
    }

    pub fn get_last_offset(&self) -> u64 {
        self.base_offset + self.last_offset_delta as u64
    }

    pub fn header_as_bytes(&self) -> [u8; 24] {
        let mut header: [u8; 24] = [0u8; 24];

        header[0..8].copy_from_slice(&self.base_offset.to_le_bytes());
        header[8..12].copy_from_slice(&(self.length.as_bytes_u64() as u32).to_le_bytes());
        header[12..16].copy_from_slice(&self.last_offset_delta.to_le_bytes());
        header[16..24].copy_from_slice(&self.max_timestamp.to_le_bytes());

        header
    }
}

impl<'a, T, U> BatchItemizer<RetainedMessage, &'a U, T> for T
where
    T: Iterator<Item = &'a U>,
    &'a U: IntoMessagesIterator<Item = RetainedMessage>,
{
    fn to_messages(self) -> Vec<RetainedMessage> {
        self.flat_map(|batch| batch.into_messages_iter().collect::<Vec<_>>())
            .collect()
    }

    fn to_messages_with_filter<F>(self, messages_count: usize, f: &F) -> Vec<RetainedMessage>
    where
        F: Fn(&RetainedMessage) -> bool,
    {
        self.fold(Vec::with_capacity(messages_count), |mut messages, batch| {
            messages.extend(batch.into_messages_iter().filter(f));
            messages
        })
    }
}

impl Sizeable for RetainedMessageBatch {
    fn get_size_bytes(&self) -> IggyByteSize {
        self.length + RETAINED_BATCH_HEADER_LEN.into()
    }
}
