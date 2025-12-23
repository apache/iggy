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

use crate::slab::partitions;
use iggy_common::Identifier;

#[derive(Debug)]
pub struct IggyFullNamespace {
    stream: Identifier,
    topic: Identifier,
    partition: partitions::ContainerId,
}

impl IggyFullNamespace {
    pub fn new(stream: Identifier, topic: Identifier, partition: partitions::ContainerId) -> Self {
        Self {
            stream,
            topic,
            partition,
        }
    }

    pub fn stream_id(&self) -> &Identifier {
        &self.stream
    }

    pub fn topic_id(&self) -> &Identifier {
        &self.topic
    }

    pub fn partition_id(&self) -> partitions::ContainerId {
        self.partition
    }

    pub fn decompose(self) -> (Identifier, Identifier, partitions::ContainerId) {
        (self.stream, self.topic, self.partition)
    }
}
