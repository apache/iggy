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
using Apache.Iggy.Enums;

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
    ///     Retrieves the option catalog for a resource scope: every key a client can set,
    ///     update, reset or read, with the kind, default, bounds and mutability of each.
    /// </summary>
    /// <remarks>
    ///     This is the discovery surface for options. A key outside the catalog is
    ///     refused, and binary transports carry only the error code, so a client has
    ///     no other way to learn which keys this server knows. Available over TCP and
    ///     HTTP.
    /// </remarks>
    /// <param name="scope">The resource whose catalog to describe.</param>
    /// <param name="token">The cancellation token to cancel the operation.</param>
    /// <returns>
    ///     A task that represents the asynchronous operation and returns the catalog
    ///     entries in catalog order.
    /// </returns>
    Task<IReadOnlyList<OptionSpec>> DescribeOptionsAsync(OptionsScope scope, CancellationToken token = default);

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
    ///     Reads one page of the node's metadata change log, or of its current state.
    /// </summary>
    /// <remarks>
    ///     Every committed metadata operation that changes public state leaves records such as a stream created, a
    ///     topic renamed or a session closed. A mirror keeps a cursor, asks for the records after it and stores the
    ///     returned <see cref="MetadataChanges.Next" />. On <see cref="MetadataChangeStatus.Expired" /> or
    ///     <see cref="MetadataChangeStatus.Reset" /> it rebuilds from current state pages and follows the changes
    ///     after the anchor they return. Requires root or the global manage servers permission. The query is checked
    ///     before anything is sent. Available over TCP and HTTP.
    /// </remarks>
    /// <param name="query">Where to start, which groups, the time range and the page size.</param>
    /// <param name="token">The cancellation token to cancel the operation.</param>
    /// <returns>A task that represents the asynchronous operation and returns the page.</returns>
    /// <exception cref="Exceptions.IggyInvalidStatusCodeException">
    ///     Invalid command (code 3) for a query the rules refuse, unauthorized (code 41) without the manage servers
    ///     permission.
    /// </exception>
    Task<MetadataChanges> GetMetadataChangesAsync(MetadataChangesQuery query, CancellationToken token = default);

    /// <summary>
    ///     Sends a ping request to the server to verify connectivity.
    /// </summary>
    /// <remarks>
    ///     This is a simple health check operation that can be used to verify the connection is active. On the
    ///     VSR wire protocol it also re-syncs the assignment of every consumer group this client has joined, so
    ///     it costs one extra round trip per joined group. The SDK never calls it on its own: an application
    ///     that wants assignments refreshed has to ping on its own cadence.
    /// </remarks>
    /// <param name="token">The cancellation token to cancel the operation.</param>
    /// <returns>A task representing the asynchronous operation.</returns>
    Task PingAsync(CancellationToken token = default);

    /// <summary>
    ///     Captures and packages the current system state as a snapshot.
    /// </summary>
    /// <remarks>
    ///     The snapshot is returned as raw bytes of a ZIP archive. The contents depend on
    ///     the requested snapshot types (filesystem overview, process list, resource usage, etc.).
    ///     Authentication is required.
    /// </remarks>
    /// <param name="compression">The compression method to use for the snapshot archive.</param>
    /// <param name="snapshotTypes">The types of system information to include in the snapshot.</param>
    /// <param name="token">The cancellation token to cancel the operation.</param>
    /// <returns>A task that represents the asynchronous operation and returns the snapshot as raw bytes of a ZIP archive.</returns>
    Task<byte[]> GetSnapshotAsync(SnapshotCompression compression, IList<SystemSnapshotType> snapshotTypes,
        CancellationToken token = default);
}
