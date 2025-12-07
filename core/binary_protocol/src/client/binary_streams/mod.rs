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

use iggy_common::{
    Identifier, IggyError, Stream, StreamDetails, create_stream::CreateStream,
    delete_stream::DeleteStream, get_stream::GetStream, get_streams::GetStreams,
    purge_stream::PurgeStream, update_stream::UpdateStream,
};

use crate::{
    BinaryClient, StreamClient,
    utils::{auth::fail_if_not_authenticated, mapper},
};

#[async_trait::async_trait]
impl<B: BinaryClient> StreamClient for B {
    async fn get_stream(&self, stream_id: &Identifier) -> Result<Option<StreamDetails>, IggyError> {
        fail_if_not_authenticated(self).await?;
        let response = self
            .send_with_response(&GetStream {
                stream_id: stream_id.clone(),
            })
            .await?;
        if response.is_empty() {
            return Ok(None);
        }

        mapper::map_stream(response).map(Some)
    }

    async fn get_streams(&self) -> Result<Vec<Stream>, IggyError> {
        fail_if_not_authenticated(self).await?;
        let response = self.send_with_response(&GetStreams {}).await?;
        mapper::map_streams(response)
    }

    async fn create_stream(&self, name: &str) -> Result<StreamDetails, IggyError> {
        fail_if_not_authenticated(self).await?;
        let response = self
            .send_with_response(&CreateStream {
                name: name.to_string(),
            })
            .await?;
        mapper::map_stream(response)
    }

    async fn update_stream(&self, stream_id: &Identifier, name: &str) -> Result<(), IggyError> {
        fail_if_not_authenticated(self).await?;
        self.send_with_response(&UpdateStream {
            stream_id: stream_id.clone(),
            name: name.to_string(),
        })
        .await?;
        Ok(())
    }

    async fn delete_stream(&self, stream_id: &Identifier) -> Result<(), IggyError> {
        fail_if_not_authenticated(self).await?;
        self.send_with_response(&DeleteStream {
            stream_id: stream_id.clone(),
        })
        .await?;
        Ok(())
    }

    async fn purge_stream(&self, stream_id: &Identifier) -> Result<(), IggyError> {
        fail_if_not_authenticated(self).await?;
        self.send_with_response(&PurgeStream {
            stream_id: stream_id.clone(),
        })
        .await?;
        Ok(())
    }
}
