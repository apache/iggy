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

use async_trait::async_trait;
use iggy_common::{Identifier, IggyError, Stream, StreamDetails};

/// This trait defines the methods to interact with the stream module.
#[async_trait]
pub trait StreamClient {
    /// Get the info about a specific stream by unique ID or name.
    ///
    /// Authentication is required, and the permission to read the streams.
    async fn get_stream(&self, stream_id: &Identifier) -> Result<Option<StreamDetails>, IggyError>;
    /// Get the info about all the streams.
    ///
    /// Authentication is required, and the permission to read the streams.
    async fn get_streams(&self) -> Result<Vec<Stream>, IggyError>;
    /// Create a new stream.
    ///
    /// Authentication is required, and the permission to manage the streams.
    async fn create_stream(&self, name: &str) -> Result<StreamDetails, IggyError>;
    /// Update a stream by unique ID or name.
    ///
    /// Authentication is required, and the permission to manage the streams.
    async fn update_stream(&self, stream_id: &Identifier, name: &str) -> Result<(), IggyError>;
    /// Delete a stream by unique ID or name.
    ///
    /// Authentication is required, and the permission to manage the streams.
    async fn delete_stream(&self, stream_id: &Identifier) -> Result<(), IggyError>;
    /// Purge a stream by unique ID or name.
    ///
    /// Authentication is required, and the permission to manage the streams.
    async fn purge_stream(&self, stream_id: &Identifier) -> Result<(), IggyError>;
}
