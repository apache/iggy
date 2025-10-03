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

pub mod benchmark;
pub mod common;

pub mod balanced_consumer_group;
pub mod balanced_producer;
pub mod balanced_producer_and_consumer_group;
pub mod end_to_end_producing_consumer;
pub mod end_to_end_producing_consumer_group;
pub mod pinned_consumer;
pub mod pinned_producer;
pub mod pinned_producer_and_consumer;

pub const CONSUMER_GROUP_BASE_ID: usize = 0;
pub const CONSUMER_GROUP_NAME_PREFIX: &str = "cg";
