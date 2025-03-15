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

use super::message_batch::RetainedMessageBatch;
use crate::streaming::models::messages::RetainedMessage;
use std::sync::Arc;

pub trait IntoMessagesIterator {
    type Item;
    type IntoIter: Iterator<Item = Self::Item>;
    fn into_messages_iter(self) -> Self::IntoIter;
}

pub struct RetainedMessageBatchIterator<'a> {
    batch: &'a RetainedMessageBatch,
    current_position: u64,
}

impl<'a> RetainedMessageBatchIterator<'a> {
    pub fn new(batch: &'a RetainedMessageBatch) -> Self {
        RetainedMessageBatchIterator {
            batch,
            current_position: 0,
        }
    }
}

// TODO(numinex): Consider using FallibleIterator instead of this
// https://crates.io/crates/fallible-iterator
impl Iterator for RetainedMessageBatchIterator<'_> {
    type Item = RetainedMessage;
    fn next(&mut self) -> Option<Self::Item> {
        if self.current_position < self.batch.length.as_bytes_u64() {
            let start_position = self.current_position as usize;
            let length = u32::from_le_bytes(
                self.batch.bytes[start_position..start_position + 4]
                    .try_into()
                    .ok()?,
            );
            let message = self
                .batch
                .bytes
                .slice(start_position + 4..start_position + 4 + length as usize);
            self.current_position += 4 + length as u64;
            RetainedMessage::try_from_bytes(message).ok()
        } else {
            None
        }
    }
}

impl<'a> IntoMessagesIterator for &'a RetainedMessageBatch {
    type Item = RetainedMessage;
    type IntoIter = RetainedMessageBatchIterator<'a>;

    fn into_messages_iter(self) -> Self::IntoIter {
        RetainedMessageBatchIterator::new(self)
    }
}
impl<'a> IntoMessagesIterator for &'a Arc<RetainedMessageBatch> {
    type Item = RetainedMessage;
    type IntoIter = RetainedMessageBatchIterator<'a>;

    fn into_messages_iter(self) -> Self::IntoIter {
        RetainedMessageBatchIterator::new(self)
    }
}
