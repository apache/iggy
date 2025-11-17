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
///     Defines methods for managing streams in an Iggy client.
///     A stream is a logical container for topics and is the primary organizational unit in Iggy.
/// </summary>
public interface IIggyStream
{
    /// <summary>
    ///     Creates a new stream with the specified name.
    /// </summary>
    /// <remarks>
    ///     The stream name must be unique within the Iggy instance and has a maximum length of 255 characters.
    /// </remarks>
    /// <param name="name">The unique name of the stream to create.</param>
    /// <param name="token">The cancellation token to cancel the operation.</param>
    /// <returns>A task that represents the asynchronous operation and returns the created stream information.</returns>
    Task<StreamResponse?> CreateStreamAsync(string name, CancellationToken token = default);

    /// <summary>
    ///     Retrieves detailed information about a specific stream by its identifier.
    /// </summary>
    /// <param name="streamId">The identifier of the stream to retrieve (numeric ID or name).</param>
    /// <param name="token">The cancellation token to cancel the operation.</param>
    /// <returns>
    ///     A task that represents the asynchronous operation and returns the stream information, or null if the stream
    ///     was not found.
    /// </returns>
    Task<StreamResponse?> GetStreamByIdAsync(Identifier streamId, CancellationToken token = default);

    /// <summary>
    ///     Updates the name of an existing stream.
    /// </summary>
    /// <remarks>
    ///     The new stream name must be unique within the Iggy instance.
    /// </remarks>
    /// <param name="streamId">The identifier of the stream to update (numeric ID or name).</param>
    /// <param name="name">The new name for the stream.</param>
    /// <param name="token">The cancellation token to cancel the operation.</param>
    /// <returns>A task that represents the asynchronous operation.</returns>
    Task UpdateStreamAsync(Identifier streamId, string name, CancellationToken token = default);

    /// <summary>
    ///     Retrieves information about all streams.
    /// </summary>
    /// <param name="token">The cancellation token to cancel the operation.</param>
    /// <returns>A task that represents the asynchronous operation and returns a read-only collection of stream information.</returns>
    Task<IReadOnlyList<StreamResponse>> GetStreamsAsync(CancellationToken token = default);

    /// <summary>
    ///     Purges all messages from all topics in a stream, deleting the stream data but not the stream itself.
    /// </summary>
    /// <remarks>
    ///     This operation removes all messages from all topics and partitions within the stream.
    ///     The stream structure remains intact, allowing new messages to be published afterwards.
    /// </remarks>
    /// <param name="streamId">The identifier of the stream to purge (numeric ID or name).</param>
    /// <param name="token">The cancellation token to cancel the operation.</param>
    /// <returns>A task that represents the asynchronous operation.</returns>
    Task PurgeStreamAsync(Identifier streamId, CancellationToken token = default);

    /// <summary>
    ///     Deletes an existing stream and all its associated topics and messages.
    /// </summary>
    /// <param name="streamId">The identifier of the stream to delete (numeric ID or name).</param>
    /// <param name="token">The cancellation token to cancel the operation.</param>
    /// <returns>A task that represents the asynchronous operation.</returns>
    Task DeleteStreamAsync(Identifier streamId, CancellationToken token = default);
}
