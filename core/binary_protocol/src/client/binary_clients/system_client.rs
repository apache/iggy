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
use iggy_common::{
    ClientInfo, ClientInfoDetails, IggyDuration, IggyError, Snapshot, SnapshotCompression, Stats,
    SystemSnapshotType,
};

/// This trait defines the methods to interact with the system module.
#[async_trait]
pub trait SystemClient {
    /// Get the stats of the system such as PID, memory usage, streams count etc.
    ///
    /// Authentication is required, and the permission to read the server info.
    async fn get_stats(&self) -> Result<Stats, IggyError>;
    /// Get the info about the currently connected client (not to be confused with the user).
    ///
    /// Authentication is required.
    async fn get_me(&self) -> Result<ClientInfoDetails, IggyError>;
    /// Get the info about a specific client by unique ID (not to be confused with the user).
    ///
    /// Authentication is required, and the permission to read the server info.
    async fn get_client(&self, client_id: u32) -> Result<Option<ClientInfoDetails>, IggyError>;
    /// Get the info about all the currently connected clients (not to be confused with the users).
    ///
    /// Authentication is required, and the permission to read the server info.
    async fn get_clients(&self) -> Result<Vec<ClientInfo>, IggyError>;
    /// Ping the server to check if it's alive.
    async fn ping(&self) -> Result<(), IggyError>;
    async fn heartbeat_interval(&self) -> IggyDuration;
    /// Capture and package the current system state as a snapshot.
    ///
    /// Authentication is required.
    async fn snapshot(
        &self,
        compression: SnapshotCompression,
        snapshot_types: Vec<SystemSnapshotType>,
    ) -> Result<Snapshot, IggyError>;
}
