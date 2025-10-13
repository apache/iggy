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

use super::persistence::persister::PersisterKind;
use crate::configs::system::SystemConfig;
use crate::shard::system::storage::FileSystemInfoStorage;
use std::sync::Arc;

#[derive(Debug, Clone)]
pub struct SystemStorage {
    pub info: Arc<FileSystemInfoStorage>,
    pub persister: Arc<PersisterKind>,
}

impl SystemStorage {
    pub fn new(config: Arc<SystemConfig>, persister: Arc<PersisterKind>) -> Self {
        Self {
            info: Arc::new(FileSystemInfoStorage::new(
                config.get_state_info_path(),
                persister.clone(),
            )),
            persister,
        }
    }
}
