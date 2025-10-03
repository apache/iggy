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

use crate::Identifier;
use crate::error::IggyError;
use crate::types::message::IggyMessage;
use std::fmt::Debug;

/// The trait represent the logic responsible for calculating the partition ID and is used by the `IggyClient`.
/// This might be especially useful when the partition ID is not constant and might be calculated based on the stream ID, topic ID and other parameters.
pub trait Partitioner: Send + Sync + Debug {
    fn calculate_partition_id(
        &self,
        stream_id: &Identifier,
        topic_id: &Identifier,
        messages: &[IggyMessage],
    ) -> Result<usize, IggyError>;
}
