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
    pub start_timestamp: u64,
    pub end_timestamp: u64,
    pub max_timestamp: u64,
    pub current_position: u64,
    pub start_offset: u64,
    pub end_offset: u64,
    pub size: IggyByteSize,
    pub max_size: IggyByteSize,
}

impl Display for Segment {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "Segment {{ sealed: {}, max_size: {}, start_timestamp: {}, end_timestamp: {}, max_timestamp: {}, start_offset: {}, end_offset: {}, size: {} }}",
            self.sealed,
            self.max_size,
            self.start_timestamp,
            self.end_timestamp,
            self.max_timestamp,
            self.start_offset,
            self.end_offset,
            self.size
        )
    }
}

impl Segment {
    #[must_use]
    pub fn new(start_offset: u64, max_size_bytes: IggyByteSize) -> Self {
        Self {
            sealed: false,
            max_size: max_size_bytes,
            start_timestamp: 0,
            end_timestamp: 0,
            max_timestamp: 0,
            start_offset,
            end_offset: start_offset,
            size: IggyByteSize::default(),
            current_position: 0,
        }
    }

    #[must_use]
    pub fn is_full(&self) -> bool {
        self.current_position >= self.max_size.as_bytes_u64() || self.size >= self.max_size
    }

    #[must_use]
    pub fn is_expired(&self, now: IggyTimestamp, expiry: IggyExpiry) -> bool {
        // A sealed segment whose newest-message timestamp is unknown (0, e.g.
        // sealed before any index was written) must not count as expired:
        // `0 + duration <= now` is otherwise always true and would delete it
        // instantly. Mirrors the #2924 fix on the common Segment type.
        if !self.sealed || self.max_timestamp == 0 {
            return false;
        }

        match expiry {
            IggyExpiry::NeverExpire | IggyExpiry::ServerDefault => false,
            IggyExpiry::ExpireDuration(duration) => {
                self.max_timestamp + duration.as_micros() <= now.as_micros()
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use iggy_common::IggyDuration;
    use std::time::Duration;

    fn sealed_segment(max_timestamp: u64) -> Segment {
        let mut segment = Segment::new(0, IggyByteSize::from(1024u64));
        segment.sealed = true;
        segment.max_timestamp = max_timestamp;
        segment
    }

    #[test]
    fn unsealed_segment_is_never_expired() {
        let mut segment = sealed_segment(1);
        segment.sealed = false;
        let expiry = IggyExpiry::ExpireDuration(IggyDuration::from(Duration::from_secs(1)));
        assert!(!segment.is_expired(IggyTimestamp::now(), expiry));
    }

    #[test]
    fn zero_max_timestamp_segment_is_not_expired() {
        let segment = sealed_segment(0);
        let expiry = IggyExpiry::ExpireDuration(IggyDuration::from(Duration::from_secs(1)));
        assert!(!segment.is_expired(IggyTimestamp::now(), expiry));
    }

    #[test]
    fn old_sealed_segment_is_expired() {
        let segment = sealed_segment(1);
        let expiry = IggyExpiry::ExpireDuration(IggyDuration::from(Duration::from_secs(1)));
        assert!(segment.is_expired(IggyTimestamp::now(), expiry));
    }

    #[test]
    fn recent_sealed_segment_is_not_expired() {
        let now = IggyTimestamp::now();
        let segment = sealed_segment(now.as_micros());
        let expiry = IggyExpiry::ExpireDuration(IggyDuration::from(Duration::from_hours(1)));
        assert!(!segment.is_expired(now, expiry));
    }

    #[test]
    fn never_expire_and_server_default_never_expire() {
        let segment = sealed_segment(1);
        assert!(!segment.is_expired(IggyTimestamp::now(), IggyExpiry::NeverExpire));
        assert!(!segment.is_expired(IggyTimestamp::now(), IggyExpiry::ServerDefault));
    }
}
