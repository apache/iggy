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
use crate::{
    IGGY_MESSAGE_CHECKSUM_OFFSET_RANGE, IGGY_MESSAGE_HEADERS_LENGTH_OFFSET_RANGE,
    IGGY_MESSAGE_ID_OFFSET_RANGE, IGGY_MESSAGE_OFFSET_OFFSET_RANGE,
    IGGY_MESSAGE_ORIGIN_TIMESTAMP_OFFSET_RANGE, IGGY_MESSAGE_PAYLOAD_LENGTH_OFFSET_RANGE,
    IGGY_MESSAGE_TIMESTAMP_OFFSET_RANGE,
};

/// A typed, in-place view of a raw header in a buffer
#[derive(Debug)]
pub struct IggyMessageHeaderViewMut<'a> {
    /// The header data. Must be at least IGGY_MESSAGE_HEADER_SIZE bytes.
    data: &'a mut [u8],
}

impl<'a> IggyMessageHeaderViewMut<'a> {
    /// Construct a mutable view over the header slice.
    pub fn new(data: &'a mut [u8]) -> Self {
        Self { data }
    }

    pub fn checksum(&self) -> u64 {
        let bytes = &self.data[IGGY_MESSAGE_CHECKSUM_OFFSET_RANGE];
        u64::from_le_bytes(bytes.try_into().unwrap())
    }

    pub fn id(&self) -> u128 {
        let bytes = &self.data[IGGY_MESSAGE_ID_OFFSET_RANGE];
        u128::from_le_bytes(bytes.try_into().unwrap())
    }

    pub fn offset(&self) -> u64 {
        let bytes = &self.data[IGGY_MESSAGE_OFFSET_OFFSET_RANGE];
        u64::from_le_bytes(bytes.try_into().unwrap())
    }

    pub fn timestamp(&self) -> u64 {
        let bytes = &self.data[IGGY_MESSAGE_TIMESTAMP_OFFSET_RANGE];
        u64::from_le_bytes(bytes.try_into().unwrap())
    }

    pub fn origin_timestamp(&self) -> u64 {
        let bytes = &self.data[IGGY_MESSAGE_ORIGIN_TIMESTAMP_OFFSET_RANGE];
        u64::from_le_bytes(bytes.try_into().unwrap())
    }

    pub fn headers_length(&self) -> u32 {
        let bytes = &self.data[IGGY_MESSAGE_HEADERS_LENGTH_OFFSET_RANGE];
        u32::from_le_bytes(bytes.try_into().unwrap())
    }

    pub fn payload_length(&self) -> u32 {
        let bytes = &self.data[IGGY_MESSAGE_PAYLOAD_LENGTH_OFFSET_RANGE];
        u32::from_le_bytes(bytes.try_into().unwrap())
    }

    pub fn set_checksum(&mut self, value: u64) {
        let bytes = value.to_le_bytes();
        self.data[IGGY_MESSAGE_CHECKSUM_OFFSET_RANGE].copy_from_slice(&bytes);
    }

    pub fn set_id(&mut self, value: u128) {
        let bytes = value.to_le_bytes();
        self.data[IGGY_MESSAGE_ID_OFFSET_RANGE].copy_from_slice(&bytes);
    }

    pub fn set_offset(&mut self, value: u64) {
        let bytes = value.to_le_bytes();
        self.data[IGGY_MESSAGE_OFFSET_OFFSET_RANGE].copy_from_slice(&bytes);
    }

    pub fn set_timestamp(&mut self, value: u64) {
        let bytes = value.to_le_bytes();
        self.data[IGGY_MESSAGE_TIMESTAMP_OFFSET_RANGE].copy_from_slice(&bytes);
    }
}
