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

use crate::configs::system::SystemConfig;
use crate::streaming::storage::SystemStorage;
use crate::streaming::topics::topic::Topic;
use ahash::AHashMap;
use iggy_common::IggyByteSize;
use iggy_common::IggyTimestamp;
use std::fmt::Display;
use std::rc::Rc;
use std::sync::Arc;
use std::sync::atomic::{AtomicU32, AtomicU64, Ordering};

#[derive(Debug)]
pub struct Stream {
    pub stream_id: u32,
    pub name: String,
    pub path: String,
    pub topics_path: String,
    pub created_at: IggyTimestamp,
    pub current_topic_id: AtomicU32,
    pub size_bytes: Arc<AtomicU64>,
    pub messages_count: Arc<AtomicU64>,
    pub segments_count: Arc<AtomicU32>,
    pub(crate) topics: AHashMap<u32, Topic>,
    pub(crate) topics_ids: AHashMap<String, u32>,
    pub(crate) config: Rc<SystemConfig>,
    pub(crate) storage: Rc<SystemStorage>,
}

impl Stream {
    pub fn empty(
        id: u32,
        name: &str,
        config: Rc<SystemConfig>,
        storage: Rc<SystemStorage>,
    ) -> Self {
        Stream::create(id, name, config, storage)
    }

    pub fn create(
        id: u32,
        name: &str,
        config: Rc<SystemConfig>,
        storage: Rc<SystemStorage>,
    ) -> Self {
        let path = config.get_stream_path(id);
        let topics_path = config.get_topics_path(id);

        Stream {
            stream_id: id,
            name: name.to_string(),
            path,
            topics_path,
            config,
            current_topic_id: AtomicU32::new(1),
            size_bytes: Arc::new(AtomicU64::new(0)),
            messages_count: Arc::new(AtomicU64::new(0)),
            segments_count: Arc::new(AtomicU32::new(0)),
            topics: AHashMap::new(),
            topics_ids: AHashMap::new(),
            storage,
            created_at: IggyTimestamp::now(),
        }
    }

    pub fn get_size(&self) -> IggyByteSize {
        IggyByteSize::from(self.size_bytes.load(Ordering::SeqCst))
    }
}

impl Display for Stream {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "Stream {{ stream ID: {}, name: {}, path: {}, topic_path: {}, created_at: {} }}",
            self.stream_id, self.name, self.path, self.topics_path, self.created_at,
        )
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::streaming::{
        persistence::persister::{FileWithSyncPersister, PersisterKind},
        utils::MemoryPool,
    };

    #[test]
    fn should_be_created_given_valid_parameters() {
        let tempdir = tempfile::TempDir::new().unwrap();
        let config = Rc::new(SystemConfig {
            path: tempdir.path().to_str().unwrap().to_string(),
            ..Default::default()
        });
        let storage = Rc::new(SystemStorage::new(
            config.clone(),
            Arc::new(PersisterKind::FileWithSync(FileWithSyncPersister {})),
        ));
        MemoryPool::init_pool(config.clone());
        let id = 1;
        let name = "test";
        let config = Rc::new(SystemConfig::default());
        let path = config.get_stream_path(id);
        let topics_path = config.get_topics_path(id);

        let stream = Stream::create(id, name, config, storage);

        assert_eq!(stream.stream_id, id);
        assert_eq!(stream.name, name);
        assert_eq!(stream.path, path);
        assert_eq!(stream.topics_path, topics_path);
        assert!(stream.topics.is_empty());
    }
}
