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
///     Defines methods for managing partitions within topics in an Iggy client.
///     Partitions distribute messages across multiple storage units for scalability and parallel processing.
/// </summary>
public interface IIggyPartition
{
    /// <summary>
    ///     Deletes a specified number of partitions from a topic.
    /// </summary>
    /// <param name="streamId">The identifier of the stream containing the topic (numeric ID or name).</param>
    /// <param name="topicId">The identifier of the topic (numeric ID or name).</param>
    /// <param name="partitionsCount">The number of partitions to delete.</param>
    /// <param name="token">The cancellation token to cancel the operation.</param>
    /// <returns>A task representing the asynchronous operation.</returns>
    Task DeletePartitionsAsync(Identifier streamId, Identifier topicId, uint partitionsCount,
        CancellationToken token = default);

    /// <summary>
    ///     Creates and adds new partitions to a topic.
    /// </summary>
    /// <param name="streamId">The identifier of the stream containing the topic (numeric ID or name).</param>
    /// <param name="topicId">The identifier of the topic (numeric ID or name).</param>
    /// <param name="partitionsCount">The number of new partitions to create.</param>
    /// <param name="token">The cancellation token to cancel the operation.</param>
    /// <returns>A task representing the asynchronous operation.</returns>
    Task CreatePartitionsAsync(Identifier streamId, Identifier topicId, uint partitionsCount,
        CancellationToken token = default);

    /// <summary>
    ///     Reads one page of the partitions of a topic.
    /// </summary>
    /// <remarks>
    ///     The query is checked before anything is sent. To read the next page, send the same query with the
    ///     <see cref="Page{T}.Next" /> of this page as its cursor. Partitions get their ids in creation order, so both
    ///     orders read them by id.
    /// </remarks>
    /// <param name="streamId">The identifier of the stream containing the topic (numeric ID or name).</param>
    /// <param name="topicId">The identifier of the topic (numeric ID or name).</param>
    /// <param name="query">The order, the page size and the cursor of the read.</param>
    /// <param name="token">The cancellation token to cancel the operation.</param>
    /// <returns>A task that represents the asynchronous operation and returns the page.</returns>
    /// <exception cref="Exceptions.IggyInvalidStatusCodeException">
    ///     Invalid command (code 3) for a query the page rules refuse, or for a cursor the server cannot use. The
    ///     stream or topic not found error when one of them does not exist.
    /// </exception>
    Task<Page<PartitionResponse>> GetPartitionsPageAsync(Identifier streamId, Identifier topicId, PageQuery query,
        CancellationToken token = default);
}
