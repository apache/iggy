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

use crate::client_wrappers::ClientWrapper;
use iggy_binary_protocol::PartitionClient;
use iggy_common::{Identifier, IggyError};

#[cfg(not(feature = "sync"))]
pub mod async_impl {
    use super::*;

    #[async_trait::async_trait]
    impl PartitionClient for ClientWrapper {
        async fn create_partitions(
            &self,
            stream_id: &Identifier,
            topic_id: &Identifier,
            partitions_count: u32,
        ) -> Result<(), IggyError> {
            match self {
                ClientWrapper::Iggy(client) => {
                    client
                        .create_partitions(stream_id, topic_id, partitions_count)
                        .await
                }
                ClientWrapper::Http(client) => {
                    client
                        .create_partitions(stream_id, topic_id, partitions_count)
                        .await
                }
                ClientWrapper::Tcp(client) => {
                    client
                        .create_partitions(stream_id, topic_id, partitions_count)
                        .await
                }
                ClientWrapper::Quic(client) => {
                    client
                        .create_partitions(stream_id, topic_id, partitions_count)
                        .await
                }
            }
        }

        async fn delete_partitions(
            &self,
            stream_id: &Identifier,
            topic_id: &Identifier,
            partitions_count: u32,
        ) -> Result<(), IggyError> {
            match self {
                ClientWrapper::Iggy(client) => {
                    client
                        .delete_partitions(stream_id, topic_id, partitions_count)
                        .await
                }
                ClientWrapper::Http(client) => {
                    client
                        .delete_partitions(stream_id, topic_id, partitions_count)
                        .await
                }
                ClientWrapper::Tcp(client) => {
                    client
                        .delete_partitions(stream_id, topic_id, partitions_count)
                        .await
                }
                ClientWrapper::Quic(client) => {
                    client
                        .delete_partitions(stream_id, topic_id, partitions_count)
                        .await
                }
            }
        }
    }
}

#[cfg(feature = "sync")]
pub mod sync_impl {
    use super::*;

    impl PartitionClient for ClientWrapper {
        fn create_partitions(
            &self,
            stream_id: &Identifier,
            topic_id: &Identifier,
            partitions_count: u32,
        ) -> Result<(), IggyError> {
            match self {
                ClientWrapper::Tcp(client) => {
                    client.create_partitions(stream_id, topic_id, partitions_count)
                }
                ClientWrapper::TcpTls(client) => {
                    client.create_partitions(stream_id, topic_id, partitions_count)
                }
                ClientWrapper::Iggy(_) => Err(IggyError::InvalidConfiguration),
            }
        }

        fn delete_partitions(
            &self,
            stream_id: &Identifier,
            topic_id: &Identifier,
            partitions_count: u32,
        ) -> Result<(), IggyError> {
            match self {
                ClientWrapper::Tcp(client) => {
                    client.delete_partitions(stream_id, topic_id, partitions_count)
                }
                ClientWrapper::TcpTls(client) => {
                    client.delete_partitions(stream_id, topic_id, partitions_count)
                }
                ClientWrapper::Iggy(_) => Err(IggyError::InvalidConfiguration),
            }
        }
    }
}
