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
use iggy_binary_protocol::StreamClient;
use iggy_common::{Identifier, IggyError, Stream, StreamDetails};

#[cfg(feature = "async")]
pub mod async_impl {
    use super::*;
    use async_trait::async_trait;

    #[async_trait]
    impl StreamClient for ClientWrapper {
        async fn get_stream(&self, stream_id: &Identifier) -> Result<Option<StreamDetails>, IggyError> {
            match self {
                ClientWrapper::Iggy(client) => client.get_stream(stream_id).await,
                ClientWrapper::Http(client) => client.get_stream(stream_id).await,
                ClientWrapper::Tcp(client) => client.get_stream(stream_id).await,
                ClientWrapper::Quic(client) => client.get_stream(stream_id).await,
            }
        }

        async fn get_streams(&self) -> Result<Vec<Stream>, IggyError> {
            match self {
                ClientWrapper::Iggy(client) => client.get_streams().await,
                ClientWrapper::Http(client) => client.get_streams().await,
                ClientWrapper::Tcp(client) => client.get_streams().await,
                ClientWrapper::Quic(client) => client.get_streams().await,
            }
        }

        async fn create_stream(
            &self,
            name: &str,
            stream_id: Option<u32>,
        ) -> Result<StreamDetails, IggyError> {
            match self {
                ClientWrapper::Iggy(client) => client.create_stream(name, stream_id).await,
                ClientWrapper::Http(client) => client.create_stream(name, stream_id).await,
                ClientWrapper::Tcp(client) => client.create_stream(name, stream_id).await,
                ClientWrapper::Quic(client) => client.create_stream(name, stream_id).await,
            }
        }

        async fn update_stream(&self, stream_id: &Identifier, name: &str) -> Result<(), IggyError> {
            match self {
                ClientWrapper::Iggy(client) => client.update_stream(stream_id, name).await,
                ClientWrapper::Http(client) => client.update_stream(stream_id, name).await,
                ClientWrapper::Tcp(client) => client.update_stream(stream_id, name).await,
                ClientWrapper::Quic(client) => client.update_stream(stream_id, name).await,
            }
        }

        async fn delete_stream(&self, stream_id: &Identifier) -> Result<(), IggyError> {
            match self {
                ClientWrapper::Iggy(client) => client.delete_stream(stream_id).await,
                ClientWrapper::Http(client) => client.delete_stream(stream_id).await,
                ClientWrapper::Tcp(client) => client.delete_stream(stream_id).await,
                ClientWrapper::Quic(client) => client.delete_stream(stream_id).await,
            }
        }

        async fn purge_stream(&self, stream_id: &Identifier) -> Result<(), IggyError> {
            match self {
                ClientWrapper::Iggy(client) => client.purge_stream(stream_id).await,
                ClientWrapper::Http(client) => client.purge_stream(stream_id).await,
                ClientWrapper::Tcp(client) => client.purge_stream(stream_id).await,
                ClientWrapper::Quic(client) => client.purge_stream(stream_id).await,
            }
        }
    }
}

#[cfg(feature = "sync")]
pub mod sync_impl {
    use super::*;

    impl StreamClient for ClientWrapper {
        fn get_stream(&self, stream_id: &Identifier) -> Result<Option<StreamDetails>, IggyError> {
            match self {
                ClientWrapper::TcpSync(client) => client.get_stream(stream_id),
                ClientWrapper::Iggy(_) => Err(IggyError::InvalidConfiguration),
            }
        }

        fn get_streams(&self) -> Result<Vec<Stream>, IggyError> {
            match self {
                ClientWrapper::TcpSync(client) => client.get_streams(),
                ClientWrapper::Iggy(_) => Err(IggyError::InvalidConfiguration),
            }
        }

        fn create_stream(
            &self,
            name: &str,
            stream_id: Option<u32>,
        ) -> Result<StreamDetails, IggyError> {
            match self {
                ClientWrapper::TcpSync(client) => client.create_stream(name, stream_id),
                ClientWrapper::Iggy(_) => Err(IggyError::InvalidConfiguration),
            }
        }

        fn update_stream(&self, stream_id: &Identifier, name: &str) -> Result<(), IggyError> {
            match self {
                ClientWrapper::TcpSync(client) => client.update_stream(stream_id, name),
                ClientWrapper::Iggy(_) => Err(IggyError::InvalidConfiguration),
            }
        }

        fn delete_stream(&self, stream_id: &Identifier) -> Result<(), IggyError> {
            match self {
                ClientWrapper::TcpSync(client) => client.delete_stream(stream_id),
                ClientWrapper::Iggy(_) => Err(IggyError::InvalidConfiguration),
            }
        }

        fn purge_stream(&self, stream_id: &Identifier) -> Result<(), IggyError> {
            match self {
                ClientWrapper::TcpSync(client) => client.purge_stream(stream_id),
                ClientWrapper::Iggy(_) => Err(IggyError::InvalidConfiguration),
            }
        }
    }
}