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

using Apache.Iggy.Contracts;

namespace Apache.Iggy.IggyClient;

/// <summary>
///     Defines methods for system and cluster operations in an Iggy client.
///     These methods provide access to server information, client management, and cluster metadata.
/// </summary>
public interface IIggySystem
{
    /// <summary>
    ///     Establishes a connection to the Iggy server.
    /// </summary>
    /// <remarks>
    ///     This method initializes the connection and prepares the client for subsequent operations.
    /// </remarks>
    /// <param name="token">The cancellation token to cancel the operation.</param>
    /// <returns>A task representing the asynchronous operation.</returns>
    Task ConnectAsync(CancellationToken token = default);

    /// <summary>
    ///     Retrieves information about all connected clients.
    /// </summary>
    /// <param name="token">The cancellation token to cancel the operation.</param>
    /// <returns>A task that represents the asynchronous operation and returns a read-only collection of client information.</returns>
    Task<IReadOnlyList<ClientResponse>> GetClientsAsync(CancellationToken token = default);

    /// <summary>
    ///     Retrieves information about a specific client by its identifier.
    /// </summary>
    /// <param name="clientId">The unique identifier of the client.</param>
    /// <param name="token">The cancellation token to cancel the operation.</param>
    /// <returns>A task that represents the asynchronous operation and returns the client information, or null if not found.</returns>
    Task<ClientResponse?> GetClientByIdAsync(uint clientId, CancellationToken token = default);

    /// <summary>
    ///     Retrieves information about the current authenticated client.
    /// </summary>
    /// <remarks>
    ///     This method returns details about the client that made the request, including its identifier and authentication
    ///     status. Available only for TCP.
    /// </remarks>
    /// <param name="token">The cancellation token to cancel the operation.</param>
    /// <returns>
    ///     A task that represents the asynchronous operation and returns the current client information, or null if
    ///     unavailable.
    /// </returns>
    Task<ClientResponse?> GetMeAsync(CancellationToken token = default);

    /// <summary>
    ///     Retrieves server statistics and health information.
    /// </summary>
    /// <param name="token">The cancellation token to cancel the operation.</param>
    /// <returns>A task that represents the asynchronous operation and returns server statistics, or null if unavailable.</returns>
    Task<StatsResponse?> GetStatsAsync(CancellationToken token = default);

    /// <summary>
    ///     Retrieves cluster metadata including node information and connection information.
    /// </summary>
    /// <remarks>
    ///     This provides information about all nodes in the cluster, their roles, and connection information.
    /// </remarks>
    /// <param name="token">The cancellation token to cancel the operation.</param>
    /// <returns>A task that represents the asynchronous operation and returns cluster metadata, or null if unavailable.</returns>
    Task<ClusterMetadata?> GetClusterMetadataAsync(CancellationToken token = default);

    /// <summary>
    ///     Sends a ping request to the server to verify connectivity.
    /// </summary>
    /// <remarks>
    ///     This is a simple health check operation that can be used to verify the connection is active.
    /// </remarks>
    /// <param name="token">The cancellation token to cancel the operation.</param>
    /// <returns>A task representing the asynchronous operation.</returns>
    Task PingAsync(CancellationToken token = default);
}
