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

use crate::utils::byte_size::IggyByteSize;
use crate::{models::topic::Topic, utils::timestamp::IggyTimestamp};
use serde::{Deserialize, Serialize};

/// `Stream` represents the highest level of logical separation of data.
/// It consists of the following fields:
/// - `id`: the unique identifier (numeric) of the stream.
/// - `created_at`: the timestamp when the stream was created.
/// - `name`: the unique name of the stream.
/// - `size_bytes`: the total size of the stream in bytes.
/// - `messages_count`: the total number of messages in the stream.
/// - `topics_count`: the total number of topics in the stream.
#[derive(Debug, Serialize, Deserialize)]
pub struct Stream {
    /// The unique identifier (numeric) of the stream.
    pub id: u32,
    /// The timestamp when the stream was created.
    pub created_at: IggyTimestamp,
    /// The unique name of the stream.
    pub name: String,
    /// The total size of the stream in bytes.
    pub size: IggyByteSize,
    /// The total number of messages in the stream.
    pub messages_count: u64,
    /// The total number of topics in the stream.
    pub topics_count: u32,
}

/// `StreamDetails` represents the detailed information about the stream.
/// It consists of the following fields:
/// - `id`: the unique identifier (numeric) of the stream.
/// - `created_at`: the timestamp when the stream was created.
/// - `name`: the unique name of the stream.
/// - `size_bytes`: the total size of the stream in bytes.
/// - `messages_count`: the total number of messages in the stream.
/// - `topics_count`: the total number of topics in the stream.
/// - `topics`: the list of topics in the stream.
#[derive(Debug, Serialize, Deserialize)]
pub struct StreamDetails {
    /// The unique identifier (numeric) of the stream.
    pub id: u32,
    /// The timestamp when the stream was created.
    pub created_at: IggyTimestamp,
    /// The unique name of the stream.
    pub name: String,
    /// The total size of the stream in bytes.
    pub size: IggyByteSize,
    /// The total number of messages in the stream.
    pub messages_count: u64,
    /// The total number of topics in the stream.
    pub topics_count: u32,
    /// The collection of topics in the stream.
    pub topics: Vec<Topic>,
}
