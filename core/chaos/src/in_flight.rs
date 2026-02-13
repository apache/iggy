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

use std::collections::HashMap;
use std::sync::Mutex;

/// Tracks resource keys currently being mutated by any worker.
///
/// Bridges the TOCTOU gap between a destructive op executing on the server
/// and its shadow state update arriving: the classifier checks in-flight keys
/// alongside shadow state so that a concurrent not-found error during this
/// window is classified as `ExpectedConcurrent` rather than `ServerBug`.
///
/// Uses reference counting so that overlapping ops on the same key (e.g.
/// PurgeTopic + DeleteTopic on the same topic) don't prematurely remove
/// coverage when the first one completes.
pub struct InFlightOps {
    keys: Mutex<HashMap<String, usize>>,
}

impl InFlightOps {
    pub fn new() -> Self {
        Self {
            keys: Mutex::new(HashMap::new()),
        }
    }

    pub fn register(&self, key: &str) {
        *self.keys.lock().unwrap().entry(key.to_owned()).or_insert(0) += 1;
    }

    pub fn deregister(&self, key: &str) {
        let mut keys = self.keys.lock().unwrap();
        if let Some(count) = keys.get_mut(key) {
            *count -= 1;
            if *count == 0 {
                keys.remove(key);
            }
        }
    }

    pub fn contains(&self, key: &str) -> bool {
        self.keys.lock().unwrap().contains_key(key)
    }
}
