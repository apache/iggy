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

use futures::lock::Mutex;

/// Global filesystem locks to serialize concurrent filesystem operations.
/// These locks prevent race conditions when multiple concurrent tasks try to
/// create/delete streams, topics, or partitions on disk simultaneously.
#[derive(Debug)]
pub struct FsLocks {
    /// Lock for stream filesystem operations (create/delete stream directories)
    pub stream_lock: Mutex<()>,
    /// Lock for topic filesystem operations (create/delete topic directories)
    pub topic_lock: Mutex<()>,
    /// Lock for partition filesystem operations (create/delete partition directories)
    pub partition_lock: Mutex<()>,
}

impl FsLocks {
    pub fn new() -> Self {
        Self {
            stream_lock: Mutex::new(()),
            topic_lock: Mutex::new(()),
            partition_lock: Mutex::new(()),
        }
    }
}

impl Default for FsLocks {
    fn default() -> Self {
        Self::new()
    }
}
