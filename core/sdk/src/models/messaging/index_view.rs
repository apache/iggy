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

use crate::models::messaging::INDEX_SIZE;
use bytes::Buf;

use super::IggyIndex;

/// View into a single index entry in a binary buffer.
/// Provides zero-copy access to index data.
#[derive(Debug, Clone, Copy)]
pub struct IggyIndexView<'a> {
    data: &'a [u8],
}

impl<'a> IggyIndexView<'a> {
    /// Creates a new index view from a byte slice
    /// Slice must be exactly INDEX_SIZE (16 bytes) long
    pub fn new(data: &'a [u8]) -> Self {
        debug_assert!(
            data.len() == INDEX_SIZE,
            "Index data must be exactly {INDEX_SIZE} bytes"
        );
        Self { data }
    }

    /// Gets the offset value from the view
    pub fn offset(&self) -> u32 {
        let mut buf = &self.data[0..4];
        buf.get_u32_le()
    }

    /// Gets the position value from the view
    pub fn position(&self) -> u32 {
        let mut buf = &self.data[4..8];
        buf.get_u32_le()
    }

    /// Gets the timestamp value from the view
    pub fn timestamp(&self) -> u64 {
        let mut buf = &self.data[8..16];
        buf.get_u64_le()
    }

    /// Converts the view into an `IggyIndex`
    pub fn to_index(&self) -> IggyIndex {
        IggyIndex {
            offset: self.offset(),
            position: self.position(),
            timestamp: self.timestamp(),
        }
    }
}

impl std::fmt::Display for IggyIndexView<'_> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "offset: {}, position: {}, timestamp: {}",
            self.offset(),
            self.position(),
            self.timestamp()
        )
    }
}
