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

use crate::http::http_client::HttpClient;
use crate::http::http_transport::HttpTransport;
use crate::prelude::Identifier;
use crate::prelude::IggyError;
use async_trait::async_trait;
use iggy_binary_protocol::StreamClient;
use iggy_common::create_stream::CreateStream;
use iggy_common::update_stream::UpdateStream;
use iggy_common::{Stream, StreamDetails};

const PATH: &str = "/streams";

#[async_trait]
impl StreamClient for HttpClient {
    async fn get_stream(&self, stream_id: &Identifier) -> Result<Option<StreamDetails>, IggyError> {
        let response = self.get(&get_details_path(&stream_id.as_cow_str())).await;
        if let Err(error) = response {
            if matches!(error, IggyError::ResourceNotFound(_)) {
                return Ok(None);
            }

            return Err(error);
        }

        let stream = response?
            .json()
            .await
            .map_err(|_| IggyError::InvalidJsonResponse)?;
        Ok(Some(stream))
    }

    async fn get_streams(&self) -> Result<Vec<Stream>, IggyError> {
        let response = self.get(PATH).await?;
        let streams = response
            .json()
            .await
            .map_err(|_| IggyError::InvalidJsonResponse)?;
        Ok(streams)
    }

    async fn create_stream(&self, name: &str) -> Result<StreamDetails, IggyError> {
        let response = self
            .post(
                PATH,
                &CreateStream {
                    name: name.to_string(),
                },
            )
            .await?;
        let stream = response
            .json()
            .await
            .map_err(|_| IggyError::InvalidJsonResponse)?;
        Ok(stream)
    }

    async fn update_stream(&self, stream_id: &Identifier, name: &str) -> Result<(), IggyError> {
        self.put(
            &get_details_path(&stream_id.as_cow_str()),
            &UpdateStream {
                stream_id: stream_id.clone(),
                name: name.to_string(),
            },
        )
        .await?;
        Ok(())
    }

    async fn delete_stream(&self, stream_id: &Identifier) -> Result<(), IggyError> {
        self.delete(&get_details_path(&stream_id.as_cow_str()))
            .await?;
        Ok(())
    }

    async fn purge_stream(&self, stream_id: &Identifier) -> Result<(), IggyError> {
        self.delete(&format!(
            "{}/purge",
            get_details_path(&stream_id.as_cow_str())
        ))
        .await?;
        Ok(())
    }
}

fn get_details_path(stream_id: &str) -> String {
    format!("{PATH}/{stream_id}")
}
