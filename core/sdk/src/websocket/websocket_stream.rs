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
use iggy_common::IggyError;

/// Connection stream trait for WebSocket connections.
/// Similar to the TCP ConnectionStream trait but adapted for WebSocket semantics.
#[async_trait]
pub trait ConnectionStream: Send + Sync {
    /// Read exact amount of bytes from the stream.
    async fn read(&mut self, buf: &mut [u8]) -> Result<usize, IggyError>;

    /// Write all bytes to the stream.
    async fn write(&mut self, buf: &[u8]) -> Result<(), IggyError>;

    /// Flush any buffered data.
    async fn flush(&mut self) -> Result<(), IggyError>;

    /// Shutdown the connection.
    async fn shutdown(&mut self) -> Result<(), IggyError>;
}
