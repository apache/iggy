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
pub(crate) mod binary_client;
pub(crate) mod client;
pub(crate) mod cluster_client;
pub(crate) mod consumer_group_client;
pub(crate) mod consumer_offset_client;
pub(crate) mod message_client;
pub(crate) mod partition_client;
pub(crate) mod personal_access_token_client;
pub(crate) mod segment_client;
pub(crate) mod stream_client;
pub(crate) mod system_client;
pub(crate) mod topic_client;
pub(crate) mod user_client;

pub use crate::client::binary_clients::binary_client::BinaryClient;
pub use crate::client::binary_clients::client::Client;
pub use crate::client::binary_clients::cluster_client::ClusterClient;
pub use crate::client::binary_clients::consumer_group_client::ConsumerGroupClient;
pub use crate::client::binary_clients::consumer_offset_client::ConsumerOffsetClient;
pub use crate::client::binary_clients::message_client::MessageClient;
pub use crate::client::binary_clients::partition_client::PartitionClient;
pub use crate::client::binary_clients::personal_access_token_client::PersonalAccessTokenClient;
pub use crate::client::binary_clients::segment_client::SegmentClient;
pub use crate::client::binary_clients::stream_client::StreamClient;
pub use crate::client::binary_clients::system_client::SystemClient;
pub use crate::client::binary_clients::topic_client::TopicClient;
pub use crate::client::binary_clients::user_client::UserClient;
