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

use std::sync::Arc;

use ext_php_rs::{php_class, php_impl};
use iggy::prelude::ConsumerGroup as RustConsumerGroup;

/// One consumer group of a `ConsumerGroupsPage`.
#[php_class]
#[php(name = "Iggy\\ConsumerGroup")]
#[derive(Clone)]
pub struct ConsumerGroup {
    // The Rust type is not Clone, and each read of the page's items copies the group.
    inner: Arc<RustConsumerGroup>,
}

impl From<RustConsumerGroup> for ConsumerGroup {
    fn from(group: RustConsumerGroup) -> Self {
        Self {
            inner: Arc::new(group),
        }
    }
}

#[php_impl]
impl ConsumerGroup {
    #[php(getter)]
    pub fn id(&self) -> u32 {
        self.inner.id
    }

    #[php(getter)]
    pub fn name(&self) -> String {
        self.inner.name.clone()
    }

    #[php(getter, change_case = "snake_case")]
    pub fn partitions_count(&self) -> u32 {
        self.inner.partitions_count
    }

    #[php(getter, change_case = "snake_case")]
    pub fn members_count(&self) -> u32 {
        self.inner.members_count
    }
}
