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

use crate::prelude::IggyClient;
use async_trait::async_trait;
use iggy_common::StreamClient;
use iggy_common::locking::IggyRwLockFn;
use iggy_common::{Identifier, IggyError, Stream, StreamDetails};
use iggy_common::{StreamCreateOptions, StreamUpdateOptions};

#[async_trait]
impl StreamClient for IggyClient {
    async fn get_stream(&self, stream_id: &Identifier) -> Result<Option<StreamDetails>, IggyError> {
        super::with_client!(self, client => client.get_stream(stream_id).await)
    }

    async fn get_streams(&self) -> Result<Vec<Stream>, IggyError> {
        super::with_client!(self, client => client.get_streams().await)
    }

    async fn create_stream(
        &self,
        name: &str,
        options: &StreamCreateOptions,
    ) -> Result<StreamDetails, IggyError> {
        super::with_client!(self, client => client.create_stream(name, options).await)
    }

    async fn update_stream(
        &self,
        stream_id: &Identifier,
        options: &StreamUpdateOptions,
    ) -> Result<(), IggyError> {
        super::with_client!(self, client => client.update_stream(stream_id, options).await)
    }

    async fn delete_stream(&self, stream_id: &Identifier) -> Result<(), IggyError> {
        super::with_client!(self, client => client.delete_stream(stream_id).await)
    }
}
