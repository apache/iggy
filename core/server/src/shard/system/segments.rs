use crate::shard::IggyShard;
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
use super::COMPONENT;
use crate::streaming::session::Session;
use error_set::ErrContext;
use iggy_common::Identifier;
use iggy_common::IggyError;
use iggy_common::locking::IggyRwLockFn;

impl IggyShard {
    pub async fn delete_segments(
        &self,
        session: &Session,
        stream_id: &Identifier,
        topic_id: &Identifier,
        partition_id: u32,
        segments_count: u32,
    ) -> Result<(), IggyError> {
        // Assert authentication.
        self.ensure_authenticated(session)?;
        todo!();
    }
}
