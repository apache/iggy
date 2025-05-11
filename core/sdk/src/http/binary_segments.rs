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
use crate::prelude::{Identifier, IggyError};
use async_trait::async_trait;
use iggy_binary_protocol::SegmentClient;
use iggy_common::delete_segments::DeleteSegments;

#[async_trait]
impl SegmentClient for HttpClient {
    async fn delete_segments(
        &self,
        stream_id: &Identifier,
        topic_id: &Identifier,
        partition_id: u32,
        segments_count: u32,
    ) -> Result<(), IggyError> {
        self.delete_with_query(
            &get_path(
                &stream_id.as_cow_str(),
                &topic_id.as_cow_str(),
                partition_id,
            ),
            &DeleteSegments {
                stream_id: stream_id.clone(),
                topic_id: topic_id.clone(),
                partition_id,
                segments_count,
            },
        )
        .await?;
        Ok(())
    }
}

fn get_path(stream_id: &str, topic_id: &str, partition_id: u32) -> String {
    format!("streams/{stream_id}/topics/{topic_id}/partitions/{partition_id}")
}
