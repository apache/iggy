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

#[derive(Clone, Copy)]
pub(crate) struct IggyMessageBoundaries<'a> {
    indexes: &'a [u8],
    messages_len: usize,
    base_position: u32,
    count: usize,
}

impl<'a> IggyMessageBoundaries<'a> {
    pub(crate) fn new(
        indexes: &'a [u8],
        messages_len: usize,
        base_position: u32,
        count: u32,
    ) -> Option<Self> {
        let count = count as usize;
        let required_bytes = count.checked_mul(super::INDEX_SIZE)?;
        if indexes.len() < required_bytes {
            return None;
        }

        Some(Self {
            indexes,
            messages_len,
            base_position,
            count,
        })
    }

    pub(crate) fn count(&self) -> usize {
        self.count
    }

    pub(crate) fn boundaries(&self, index: usize) -> Option<(usize, usize)> {
        if index >= self.count {
            return None;
        }

        let start = if index == 0 {
            0
        } else {
            self.relative_position(index - 1)?
        };

        let end = if index + 1 == self.count {
            self.messages_len
        } else {
            self.relative_position(index)?
        };

        if start > self.messages_len || end > self.messages_len || start > end {
            return None;
        }

        Some((start, end))
    }

    fn relative_position(&self, index: usize) -> Option<usize> {
        let start = index.checked_mul(super::INDEX_SIZE)?.checked_add(4)?;
        let end = start.checked_add(4)?;
        let position_bytes = self.indexes.get(start..end)?;
        let absolute_position = u32::from_le_bytes(position_bytes.try_into().ok()?);
        Some(absolute_position.checked_sub(self.base_position)? as usize)
    }
}
