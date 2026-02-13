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

//! Per-partition send watermarks and purge history.
//!
//! Lives in a separate `Mutex` from `ShadowState` to avoid adding write-lock
//! contention to the hot-path `SendMessages` ops. Workers update watermarks
//! after successful sends - no shadow write-lock needed.

use ahash::AHashMap;

/// stream → topic → partition → messages_sent
type SendMap = AHashMap<String, AHashMap<String, AHashMap<u32, u64>>>;

pub struct WatermarkState {
    /// Messages sent per partition since last reset (purge/delete).
    send: SendMap,
    /// Permanent purge record: (stream, topic) where topic=None means stream-level purge.
    purge_history: Vec<(String, Option<String>)>,
}

impl WatermarkState {
    pub fn new() -> Self {
        Self {
            send: AHashMap::new(),
            purge_history: Vec::new(),
        }
    }

    pub fn record_send(&mut self, stream: &str, topic: &str, partition: u32, count: u32) {
        *self
            .send
            .entry(stream.to_owned())
            .or_default()
            .entry(topic.to_owned())
            .or_default()
            .entry(partition)
            .or_default() += count as u64;
    }

    pub fn send_watermark(&self, stream: &str, topic: &str, partition: u32) -> u64 {
        self.send
            .get(stream)
            .and_then(|t| t.get(topic))
            .and_then(|p| p.get(&partition))
            .copied()
            .unwrap_or(0)
    }

    /// Reset all partition watermarks for a stream (on delete or stream-level purge).
    pub fn reset_for_stream(&mut self, stream: &str) {
        self.send.remove(stream);
    }

    /// Reset all partition watermarks for a specific topic.
    pub fn reset_for_topic(&mut self, stream: &str, topic: &str) {
        if let Some(topics) = self.send.get_mut(stream) {
            topics.remove(topic);
        }
    }

    pub fn record_purge(&mut self, stream: String, topic: Option<String>) {
        self.purge_history.push((stream, topic));
    }

    pub fn purge_history(&self) -> &[(String, Option<String>)] {
        &self.purge_history
    }
}
