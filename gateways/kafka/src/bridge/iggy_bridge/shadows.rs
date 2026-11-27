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

//! The shadow streams that Iggy holds. A shadow stream holds the read-only copies of another
//! datacenter's geo topics, which Kafka clients see as `<origin>.<topic>`
//! (`TopicMapping::resolve_name`).

use std::collections::HashSet;
use std::sync::{Mutex, PoisonError, RwLock};
use std::time::Duration;

use tokio::time::Instant;

/// The shortest time between two reads of Iggy's streams for a name that may name a shadow.
pub(super) const SHADOW_READ_INTERVAL: Duration = Duration::from_secs(1);

/// The names of the shadow streams that the gateway last saw in Iggy. The change log follower and
/// each listing of every topic replace them.
#[derive(Debug, Default)]
pub(super) struct ShadowStreams {
    names: RwLock<HashSet<String>>,
    last_read: Mutex<Option<Instant>>,
}

impl ShadowStreams {
    /// Whether the gateway saw the shadow stream `stream`.
    pub(super) fn contains(&self, stream: &str) -> bool {
        self.names
            .read()
            .unwrap_or_else(PoisonError::into_inner)
            .contains(stream)
    }

    /// The names the gateway saw.
    pub(super) fn names(&self) -> HashSet<String> {
        self.names
            .read()
            .unwrap_or_else(PoisonError::into_inner)
            .clone()
    }

    pub(super) fn replace(&self, names: HashSet<String>) {
        *self.names.write().unwrap_or_else(PoisonError::into_inner) = names;
    }

    /// Whether a read of Iggy's streams may start at `now`: one each [`SHADOW_READ_INTERVAL`] at
    /// most. A `true` counts as that read.
    pub(super) fn take_read(&self, now: Instant) -> bool {
        let mut last = self
            .last_read
            .lock()
            .unwrap_or_else(PoisonError::into_inner);
        if last.is_some_and(|last| now.duration_since(last) < SHADOW_READ_INTERVAL) {
            return false;
        }
        *last = Some(now);
        true
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn given_replaced_names_should_hold_only_those() {
        let shadows = ShadowStreams::default();
        shadows.replace(HashSet::from(["__shadow_dc-a_kafka".to_string()]));
        shadows.replace(HashSet::from(["__shadow_dc-b_kafka".to_string()]));

        assert!(!shadows.contains("__shadow_dc-a_kafka"));
        assert!(shadows.contains("__shadow_dc-b_kafka"));
        assert_eq!(shadows.names().len(), 1);
    }

    #[tokio::test(start_paused = true)]
    async fn given_a_read_when_another_comes_within_the_interval_should_refuse_it() {
        let shadows = ShadowStreams::default();
        let start = Instant::now();

        assert!(shadows.take_read(start));
        assert!(!shadows.take_read(start + SHADOW_READ_INTERVAL / 2));
        assert!(shadows.take_read(start + SHADOW_READ_INTERVAL));
    }
}
