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

using Apache.Iggy.Kinds;
using Apache.Iggy.Messages;

namespace Apache.Iggy.IggyClient;

/// <summary>
///     Defines methods for publishing messages to streams and topics in an Iggy client.
/// </summary>
public interface IIggyPublisher
{
    /// <summary>
    ///     Sends messages to the specified stream and topic with the specified partitioning strategy.
    /// </summary>
    /// <remarks>
    ///     The messages are sent to a specific partition based on the partitioning strategy provided.
    ///     The partitioning can be:
    ///     - Balanced: The server automatically selects the partition.
    ///     - PartitionId: Messages are sent to a specific partition.
    ///     - MessagesKey: Partition is selected based on a key value, ensuring all messages with the same key go to the same
    ///     partition.
    /// </remarks>
    /// <param name="streamId">The stream identifier (numeric ID or name).</param>
    /// <param name="topicId">The topic identifier (numeric ID or name).</param>
    /// <param name="partitioning">The partitioning strategy that determines which partition receives the messages.</param>
    /// <param name="messages">The collection of messages to be sent.</param>
    /// <param name="token">The cancellation token to cancel the operation.</param>
    /// <returns>A task representing the asynchronous operation.</returns>
    Task SendMessagesAsync(Identifier streamId, Identifier topicId, Partitioning partitioning, IList<Message> messages,
        CancellationToken token = default);

    /// <summary>
    ///     Forces a flush of the unsaved buffer to disk for a specific partition.
    /// </summary>
    /// <remarks>
    ///     This method ensures that all pending messages in the in-memory buffer for the specified partition are written to
    ///     disk.
    ///     If <paramref name="fsync" /> is true, the data is both flushed to disk and synchronized (fsync), ensuring
    ///     durability.
    ///     If false, the data is only flushed to disk without synchronization.
    /// </remarks>
    /// <param name="streamId">The stream identifier (numeric ID or name).</param>
    /// <param name="topicId">The topic identifier (numeric ID or name).</param>
    /// <param name="partitionId">The partition identifier for which the buffer should be flushed.</param>
    /// <param name="fsync">If true, the data is flushed and synchronized to disk (fsync). If false, only flushed.</param>
    /// <param name="token">The cancellation token to cancel the operation.</param>
    /// <returns>A task representing the asynchronous operation.</returns>
    Task FlushUnsavedBufferAsync(Identifier streamId, Identifier topicId, uint partitionId, bool fsync,
        CancellationToken token = default);
}
