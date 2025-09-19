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

mod binary_cluster;
mod binary_consumer_group;
mod binary_consumer_offset;
mod binary_message;
mod binary_partitions;
mod binary_personal_access_tokens;
mod binary_segments;
mod binary_streams;
mod binary_system;
mod binary_topics;
mod binary_users;
pub mod client;
pub mod client_builder;
pub mod consumer;
pub mod consumer_builder;
pub mod producer;
pub mod producer_builder;
pub mod producer_config;
pub mod producer_dispatcher;
pub mod producer_error_callback;
pub mod producer_sharding;

const ORDERING: std::sync::atomic::Ordering = std::sync::atomic::Ordering::SeqCst;
const MAX_BATCH_LENGTH: usize = 1000000;
const MIB: usize = 1_048_576;
