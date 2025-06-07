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
use std::sync::Arc;

use iggy_common::{IggyByteSize, IggyDuration};

use crate::clients::producer_error_callback::ErrorCallback;
use crate::clients::producer_sharding::Sharding;

#[derive(Debug, Clone)]
/// Determines how the `send_messages` API should behave when problem is encountered
pub enum BackpressureMode {
    /// Block until the send succeeds
    Block,
    /// Block with a timeout, after which the send fails
    BlockWithTimeout(IggyDuration),
    /// Fail immediately without retrying
    FailImmediately,
}

#[derive(Debug)]
pub struct BackgroundConfig {
    pub num_shards: usize,
    pub batch_size: Option<usize>,
    pub batch_length: Option<usize>,
    pub failure_mode: BackpressureMode,
    pub max_buffer_size: Option<IggyByteSize>,
    pub max_in_flight: Option<usize>,
    pub linger_time: IggyDuration,
    pub error_callback: Arc<Box<dyn ErrorCallback + Send + Sync>>,
    pub sharding: Box<dyn Sharding + Send + Sync>,
}

#[derive(Clone)]
pub struct SyncConfig {
    pub batch_length: usize,
    pub linger_time: Option<IggyDuration>,
}
