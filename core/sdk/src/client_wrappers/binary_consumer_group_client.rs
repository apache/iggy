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
use iggy_binary_protocol::{ConsumerGroupClient, UserClient};
use iggy_common::{ConsumerGroup, ConsumerGroupDetails, Identifier, IggyError};

#[cfg(not(feature = "sync"))]
pub mod async_impl {
    use async_dropper::AsyncDrop;
    use async_trait::async_trait;

    use super::*;

    #[async_trait::async_trait]
    impl ConsumerGroupClient for ClientWrapper {
        async fn get_consumer_group(
            &self,
            stream_id: &Identifier,
            topic_id: &Identifier,
            group_id: &Identifier,
        ) -> Result<Option<ConsumerGroupDetails>, IggyError> {
            match self {
                ClientWrapper::Iggy(client) => {
                    client
                        .get_consumer_group(stream_id, topic_id, group_id)
                        .await
                }
                ClientWrapper::Http(client) => {
                    client
                        .get_consumer_group(stream_id, topic_id, group_id)
                        .await
                }
                ClientWrapper::Tcp(client) => {
                    client
                        .get_consumer_group(stream_id, topic_id, group_id)
                        .await
                }
                ClientWrapper::Quic(client) => {
                    client
                        .get_consumer_group(stream_id, topic_id, group_id)
                        .await
                }
            }
        }

        async fn get_consumer_groups(
            &self,
            stream_id: &Identifier,
            topic_id: &Identifier,
        ) -> Result<Vec<ConsumerGroup>, IggyError> {
            match self {
                ClientWrapper::Iggy(client) => {
                    client.get_consumer_groups(stream_id, topic_id).await
                }
                ClientWrapper::Http(client) => {
                    client.get_consumer_groups(stream_id, topic_id).await
                }
                ClientWrapper::Tcp(client) => client.get_consumer_groups(stream_id, topic_id).await,
                ClientWrapper::Quic(client) => {
                    client.get_consumer_groups(stream_id, topic_id).await
                }
            }
        }

        async fn create_consumer_group(
            &self,
            stream_id: &Identifier,
            topic_id: &Identifier,
            name: &str,
            group_id: Option<u32>,
        ) -> Result<ConsumerGroupDetails, IggyError> {
            match self {
                ClientWrapper::Iggy(client) => {
                    client
                        .create_consumer_group(stream_id, topic_id, name, group_id)
                        .await
                }
                ClientWrapper::Http(client) => {
                    client
                        .create_consumer_group(stream_id, topic_id, name, group_id)
                        .await
                }
                ClientWrapper::Tcp(client) => {
                    client
                        .create_consumer_group(stream_id, topic_id, name, group_id)
                        .await
                }
                ClientWrapper::Quic(client) => {
                    client
                        .create_consumer_group(stream_id, topic_id, name, group_id)
                        .await
                }
            }
        }

        async fn delete_consumer_group(
            &self,
            stream_id: &Identifier,
            topic_id: &Identifier,
            group_id: &Identifier,
        ) -> Result<(), IggyError> {
            match self {
                ClientWrapper::Iggy(client) => {
                    client
                        .delete_consumer_group(stream_id, topic_id, group_id)
                        .await
                }
                ClientWrapper::Http(client) => {
                    client
                        .delete_consumer_group(stream_id, topic_id, group_id)
                        .await
                }
                ClientWrapper::Tcp(client) => {
                    client
                        .delete_consumer_group(stream_id, topic_id, group_id)
                        .await
                }
                ClientWrapper::Quic(client) => {
                    client
                        .delete_consumer_group(stream_id, topic_id, group_id)
                        .await
                }
            }
        }

        async fn join_consumer_group(
            &self,
            stream_id: &Identifier,
            topic_id: &Identifier,
            group_id: &Identifier,
        ) -> Result<(), IggyError> {
            match self {
                ClientWrapper::Iggy(client) => {
                    client
                        .join_consumer_group(stream_id, topic_id, group_id)
                        .await
                }
                ClientWrapper::Http(client) => {
                    client
                        .join_consumer_group(stream_id, topic_id, group_id)
                        .await
                }
                ClientWrapper::Tcp(client) => {
                    client
                        .join_consumer_group(stream_id, topic_id, group_id)
                        .await
                }
                ClientWrapper::Quic(client) => {
                    client
                        .join_consumer_group(stream_id, topic_id, group_id)
                        .await
                }
            }
        }

        async fn leave_consumer_group(
            &self,
            stream_id: &Identifier,
            topic_id: &Identifier,
            group_id: &Identifier,
        ) -> Result<(), IggyError> {
            match self {
                ClientWrapper::Iggy(client) => {
                    client
                        .leave_consumer_group(stream_id, topic_id, group_id)
                        .await
                }
                ClientWrapper::Http(client) => {
                    client
                        .leave_consumer_group(stream_id, topic_id, group_id)
                        .await
                }
                ClientWrapper::Tcp(client) => {
                    client
                        .leave_consumer_group(stream_id, topic_id, group_id)
                        .await
                }
                ClientWrapper::Quic(client) => {
                    client
                        .leave_consumer_group(stream_id, topic_id, group_id)
                        .await
                }
            }
        }
    }

    #[async_trait]
    impl AsyncDrop for ClientWrapper {
        async fn async_drop(&mut self) {
            match self {
                ClientWrapper::Iggy(client) => {
                    let _ = client.logout_user().await;
                }
                ClientWrapper::Http(client) => {
                    let _ = client.logout_user().await;
                }
                ClientWrapper::Tcp(client) => {
                    let _ = client.logout_user().await;
                }
                ClientWrapper::Quic(client) => {
                    let _ = client.logout_user().await;
                }
            }
        }
    }
}

#[cfg(feature = "sync")]
pub mod sync_impl {
    use super::*;

    impl ConsumerGroupClient for ClientWrapper {
        fn get_consumer_group(
            &self,
            stream_id: &Identifier,
            topic_id: &Identifier,
            group_id: &Identifier,
        ) -> Result<Option<ConsumerGroupDetails>, IggyError> {
            match self {
                ClientWrapper::Tcp(client) => {
                    client.get_consumer_group(stream_id, topic_id, group_id)
                }
                ClientWrapper::TcpTls(client) => {
                    client.get_consumer_group(stream_id, topic_id, group_id)
                }
                ClientWrapper::Iggy(_) => Err(IggyError::InvalidConfiguration),
            }
        }

        fn get_consumer_groups(
            &self,
            stream_id: &Identifier,
            topic_id: &Identifier,
        ) -> Result<Vec<ConsumerGroup>, IggyError> {
            match self {
                ClientWrapper::Tcp(client) => client.get_consumer_groups(stream_id, topic_id),
                ClientWrapper::TcpTls(client) => client.get_consumer_groups(stream_id, topic_id),
                ClientWrapper::Iggy(_) => Err(IggyError::InvalidConfiguration),
            }
        }

        fn create_consumer_group(
            &self,
            stream_id: &Identifier,
            topic_id: &Identifier,
            name: &str,
            group_id: Option<u32>,
        ) -> Result<ConsumerGroupDetails, IggyError> {
            match self {
                ClientWrapper::Tcp(client) => {
                    client.create_consumer_group(stream_id, topic_id, name, group_id)
                }
                ClientWrapper::TcpTls(client) => {
                    client.create_consumer_group(stream_id, topic_id, name, group_id)
                }
                ClientWrapper::Iggy(_) => Err(IggyError::InvalidConfiguration),
            }
        }

        fn delete_consumer_group(
            &self,
            stream_id: &Identifier,
            topic_id: &Identifier,
            group_id: &Identifier,
        ) -> Result<(), IggyError> {
            match self {
                ClientWrapper::Tcp(client) => {
                    client.delete_consumer_group(stream_id, topic_id, group_id)
                }
                ClientWrapper::TcpTls(client) => {
                    client.delete_consumer_group(stream_id, topic_id, group_id)
                }
                ClientWrapper::Iggy(_) => Err(IggyError::InvalidConfiguration),
            }
        }

        fn join_consumer_group(
            &self,
            stream_id: &Identifier,
            topic_id: &Identifier,
            group_id: &Identifier,
        ) -> Result<(), IggyError> {
            match self {
                ClientWrapper::Tcp(client) => {
                    client.join_consumer_group(stream_id, topic_id, group_id)
                }
                ClientWrapper::TcpTls(client) => {
                    client.join_consumer_group(stream_id, topic_id, group_id)
                }
                ClientWrapper::Iggy(_) => Err(IggyError::InvalidConfiguration),
            }
        }

        fn leave_consumer_group(
            &self,
            stream_id: &Identifier,
            topic_id: &Identifier,
            group_id: &Identifier,
        ) -> Result<(), IggyError> {
            match self {
                ClientWrapper::Tcp(client) => {
                    client.leave_consumer_group(stream_id, topic_id, group_id)
                }
                ClientWrapper::TcpTls(client) => {
                    client.leave_consumer_group(stream_id, topic_id, group_id)
                }
                ClientWrapper::Iggy(_) => Err(IggyError::InvalidConfiguration),
            }
        }
    }

    impl Drop for ClientWrapper {
        fn drop(&mut self) {
            match self {
                ClientWrapper::Tcp(client) => {
                    let _ = client.logout_user();
                }
                ClientWrapper::TcpTls(client) => {
                    let _ = client.logout_user();
                }
                _ => (),
            }
        }
    }
}
