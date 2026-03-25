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

use crate::{IggyByteSize, IggyExpiry, IggyTimestamp};
use std::fmt::Display;

#[derive(Default, Debug, Clone)]
pub struct Segment {
    pub sealed: bool,
    pub start_timestamp: u64,
    pub end_timestamp: u64,
    pub current_position: u32,
    pub start_offset: u64,
    pub end_offset: u64,
    pub size: IggyByteSize,
    pub max_size: IggyByteSize,
}

impl Display for Segment {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "Segment {{ sealed: {}, max_size: {}, start_timestamp: {}, end_timestamp: {}, start_offset: {}, end_offset: {}, size: {} }}",
            self.sealed,
            self.max_size,
            self.start_timestamp,
            self.end_timestamp,
            self.start_offset,
            self.end_offset,
            self.size
        )
    }
}

impl Segment {
    pub fn new(start_offset: u64, max_size_bytes: IggyByteSize) -> Self {
        Self {
            sealed: false,
            max_size: max_size_bytes,
            start_timestamp: 0,
            end_timestamp: 0,
            start_offset,
            end_offset: start_offset,
            size: IggyByteSize::default(),
            current_position: 0,
        }
    }

    pub fn is_full(&self) -> bool {
        self.size >= self.max_size
    }

    pub fn is_expired(&self, now: IggyTimestamp, expiry: IggyExpiry) -> bool {
        if !self.sealed || self.end_timestamp == 0 {
            return false;
        }

        match expiry {
            IggyExpiry::NeverExpire => false,
            IggyExpiry::ServerDefault => false,
            IggyExpiry::ExpireDuration(duration) => {
                self.end_timestamp + duration.as_micros() <= now.as_micros()
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{IggyDuration, IggyTimestamp};
    use std::time::Duration;

    #[test]
    fn zero_timestamp_segment_should_not_appear_expired() {
        // Reproduce Bug 3 from #2924: during bootstrap, segments with empty
        // indexes get end_timestamp = 0. is_expired() then evaluates
        // 0 + expiry <= now, which is always true, causing immediate deletion.
        let mut seg = Segment::new(5000, IggyByteSize::from(128 * 1024 * 1024u64));
        seg.sealed = true;
        seg.end_timestamp = 0; // simulates bootstrap with empty indexes

        let now = IggyTimestamp::now();
        let expiry = IggyExpiry::ExpireDuration(IggyDuration::from(Duration::from_secs(600)));

        // A segment with unknown timestamp (0) must NOT be considered expired.
        // Currently this FAILS - is_expired returns true because 0 + 600s <= now.
        assert!(
            !seg.is_expired(now, expiry),
            "BUG: segment with end_timestamp=0 appears instantly expired"
        );
    }
}
