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

use iggy_common::{IggyByteSize, IggyExpiry, IggyTimestamp};
use std::fmt::Display;

#[derive(Default, Debug, Clone)]
pub struct Segment {
    pub sealed: bool,
    pub message_expiry: IggyExpiry,
    pub start_timestamp: u64,
    pub end_timestamp: u64,
    pub start_offset: u64,
    pub end_offset: u64,
    pub size: IggyByteSize,     // u64
    pub max_size: IggyByteSize, // u64
}

impl Display for Segment {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "Segment {{ sealed: {}, max_size_bytes: {}, message_expiry: {:?}, start_timestamp: {}, end_timestamp: {}, start_offset: {}, end_offset: {}, size: {} }}",
            self.sealed,
            self.max_size,
            self.message_expiry,
            self.start_timestamp,
            self.end_timestamp,
            self.start_offset,
            self.end_offset,
            self.size
        )
    }
}

impl Segment {
    /// Creates a new Segment with the specified parameters
    pub fn new(
        start_offset: u64,
        max_size_bytes: IggyByteSize,
        message_expiry: IggyExpiry,
    ) -> Self {
        Self {
            sealed: false,
            max_size: max_size_bytes,
            message_expiry,
            start_timestamp: 0,
            end_timestamp: 0,
            start_offset,
            end_offset: start_offset,
            size: IggyByteSize::default(),
        }
    }

    pub fn is_full(&self) -> bool {
        if self.size >= self.max_size {
            return true;
        }

        self.is_expired(IggyTimestamp::now())
    }

    pub fn is_sealed(&self) -> bool {
        self.sealed
    }

    pub fn is_expired(&self, now: IggyTimestamp) -> bool {
        if !self.sealed {
            return false;
        }

        match self.message_expiry {
            IggyExpiry::NeverExpire => false,
            IggyExpiry::ServerDefault => false,
            IggyExpiry::ExpireDuration(expiry) => {
                let last_message_timestamp = self.end_timestamp;
                last_message_timestamp + expiry.as_micros() <= now.as_micros()
            }
        }
    }
}
