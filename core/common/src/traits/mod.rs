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

pub(crate) mod binary_auth;
pub(crate) mod binary_client;
mod binary_impls;
pub(crate) mod binary_transport;
pub(crate) mod client;
pub(crate) mod cluster_client;
pub(crate) mod consumer_group_client;
pub(crate) mod consumer_offset_client;
pub(crate) mod message_client;
pub(crate) mod partition_client;
pub(crate) mod partitioner;
pub(crate) mod personal_access_token_client;
pub(crate) mod segment_client;
pub(crate) mod sizeable;
pub(crate) mod stream_client;
pub(crate) mod system_client;
pub(crate) mod topic_client;
pub(crate) mod user_client;
pub(crate) mod validatable;
