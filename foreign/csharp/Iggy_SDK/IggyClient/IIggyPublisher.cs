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
    ///     <para>
    ///         When the client is configured with an encryptor, each <see cref="Message" /> payload (and user
    ///         headers) is encrypted at the wire boundary while the outgoing buffer is serialized; the caller's
    ///         instances are never mutated and keep their plaintext.
    ///     </para>
    /// </remarks>
    /// <param name="streamId">The stream identifier (numeric ID or name).</param>
    /// <param name="topicId">The topic identifier (numeric ID or name).</param>
    /// <param name="partitioning">The partitioning strategy that determines which partition receives the messages.</param>
    /// <param name="messages">The collection of messages to be sent.</param>
    /// <param name="token">The cancellation token to cancel the operation.</param>
    /// <returns>
    ///     Commit confirmations carrying the partition each batch landed in and its base offset.
    /// </returns>
    Task<SendMessagesResponse> SendMessagesAsync(Identifier streamId, Identifier topicId, Partitioning partitioning,
        IList<Message> messages, CancellationToken token = default);

    /// <summary>
    ///     Sends a single message to the specified stream and topic. See
    ///     <see cref="SendMessagesAsync(Identifier, Identifier, Partitioning, IList{Message}, CancellationToken)" />
    ///     for partitioning semantics.
    /// </summary>
    /// <remarks>
    ///     The payload is copied into the wire buffer before the returned task completes, so caller-owned
    ///     payload memory (e.g. a pooled <see cref="Messages.RentedMessageBatch" /> buffer) may be released
    ///     once it completes.
    /// </remarks>
    Task<SendMessagesResponse> SendMessagesAsync(Identifier streamId, Identifier topicId, Partitioning partitioning,
        Message message, CancellationToken token = default)
    {
        return SendMessagesAsync(streamId, topicId, partitioning, new[] { message }, token);
    }
}
