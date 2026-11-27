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

using System.Text.Json.Serialization;
using Apache.Iggy.Enums;
using Apache.Iggy.Kinds;

namespace Apache.Iggy.Contracts;

/// <summary>
///     Response from the server containing a list of messages and the current offset.
/// </summary>
public sealed class PolledMessages
{
    /// <summary>Captured incarnation and ownership authority for these offsets.</summary>
    public PartitionContext Context { get; init; }

    /// <summary>
    ///     Partition id an empty group poll reports when the member currently owns no partition of the group,
    ///     for example mid-rebalance or when the group has more members than partitions. Matches the Go and
    ///     Node SDKs. Consumers should back off before polling again rather than spin.
    /// </summary>
    public static readonly uint NoAssignedPartition = 0xFFFF_FFFE;

    /// <summary>
    ///     Partition identifier for the messages.
    /// </summary>
    public required uint PartitionId { get; init; }

    /// <summary>
    ///     Current offset for the partition.
    /// </summary>
    public required ulong CurrentOffset { get; init; }

    /// <summary>
    ///     List of messages.
    /// </summary>
    public required IReadOnlyList<MessageResponse> Messages { get; set; }

    /// <summary>The start of the read that returned this batch, which the continuation of an empty batch keeps.</summary>
    internal PollingStrategy? RequestStart { get; set; }

    /// <summary>The position after the last message, to commit the whole batch. Null for an empty batch.</summary>
    [JsonIgnore]
    public Checkpoint? Checkpoint => CheckpointAt(Messages.Count - 1);

    /// <summary>
    ///     The position after the next read of the partition. After the last message it is
    ///     <see cref="PollingStrategy.Offset" /> of the next offset. An empty batch keeps the start of its read:
    ///     <see cref="PollingStrategy.First" /> becomes offset 0, and <see cref="PollingStrategy.Next" /> stays Next, so
    ///     the next read resolves it again from the stored offset. The continuation belongs to the partition of the
    ///     batch, and a read of another partition refuses it. It carries the context of the batch, so a read after a
    ///     change of the partition history fails with status 87 (HistoryUnavailable).
    /// </summary>
    /// <remarks>
    ///     An empty batch with a zero context names no partition history, so its continuation is the start of the
    ///     read as it was.
    /// </remarks>
    [JsonIgnore]
    public PollingStrategy Continuation
    {
        get
        {
            if (Messages.Count > 0)
            {
                return PollingStrategy.Continuation(PartitionId, MessagePolling.Offset,
                    Messages[^1].Header.Offset + 1, Context);
            }

            var start = RequestStart ?? PollingStrategy.Next();
            if (Context == default)
            {
                return start;
            }

            return start.Kind == MessagePolling.First
                ? PollingStrategy.Continuation(PartitionId, MessagePolling.Offset, 0, Context)
                : PollingStrategy.Continuation(PartitionId, start.Kind, start.Value, Context);
        }
    }

    /// <summary>
    ///     The position after <see cref="Messages" /> at <paramref name="index" />, to commit a processed part of the
    ///     batch. Null for an index out of range.
    /// </summary>
    public Checkpoint? CheckpointAt(int index)
    {
        return index >= 0 && index < Messages.Count
            ? new Checkpoint(PartitionId, Messages[index].Header.Offset, Context, Stamp)
            : null;
    }

    /// <summary>The reader and partition generation that delivered the batch, null for a one-shot poll.</summary>
    internal ReaderStamp? Stamp { get; set; }

    /// <summary>
    ///     Empty polled messages.
    /// </summary>
    public static PolledMessages Empty =>
        new()
        {
            Messages = new List<MessageResponse>().AsReadOnly(),
            CurrentOffset = 0,
            PartitionId = 0
        };
}
