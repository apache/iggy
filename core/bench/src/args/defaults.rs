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

use iggy::prelude::IggyByteSize;
use nonzero_lit::u32;
use std::num::NonZeroU32;

pub const DEFAULT_HTTP_SERVER_ADDRESS: &str = "127.0.0.1:3000";

pub const DEFAULT_TCP_SERVER_ADDRESS: &str = "127.0.0.1:8090";

pub const DEFAULT_QUIC_CLIENT_ADDRESS: &str = "127.0.0.1:0";
pub const DEFAULT_QUIC_SERVER_ADDRESS: &str = "127.0.0.1:8080";
pub const DEFAULT_QUIC_SERVER_NAME: &str = "localhost";
pub const DEFAULT_QUIC_VALIDATE_CERTIFICATE: bool = false;

pub const DEFAULT_WEBSOCKET_SERVER_ADDRESS: &str = "127.0.0.1:8092";

pub const DEFAULT_MESSAGES_PER_BATCH: NonZeroU32 = u32!(1000);
pub const DEFAULT_MESSAGE_BATCHES: NonZeroU32 = u32!(1000);
pub const DEFAULT_MESSAGE_SIZE: NonZeroU32 = u32!(1000);
pub const DEFAULT_TOTAL_MESSAGES_SIZE: IggyByteSize = IggyByteSize::new(8_000_000);

pub const DEFAULT_PINNED_NUMBER_OF_STREAMS: NonZeroU32 = u32!(8);
pub const DEFAULT_BALANCED_NUMBER_OF_STREAMS: NonZeroU32 = u32!(1);

pub const DEFAULT_PINNED_NUMBER_OF_PARTITIONS: NonZeroU32 = u32!(1);
pub const DEFAULT_BALANCED_NUMBER_OF_PARTITIONS: NonZeroU32 = u32!(24);

pub const DEFAULT_NUMBER_OF_CONSUMERS: NonZeroU32 = u32!(8);
pub const DEFAULT_NUMBER_OF_CONSUMER_GROUPS: NonZeroU32 = u32!(1);
pub const DEFAULT_NUMBER_OF_PRODUCERS: NonZeroU32 = u32!(8);

pub const DEFAULT_PERFORM_CLEANUP: bool = false;
pub const DEFAULT_SERVER_STDOUT_VISIBILITY: bool = false;

pub const DEFAULT_WARMUP_TIME: &str = "0s";
pub const DEFAULT_SKIP_SERVER_START: bool = false;

pub const DEFAULT_SAMPLING_TIME: &str = "10ms";
pub const DEFAULT_MOVING_AVERAGE_WINDOW: u32 = 20;
