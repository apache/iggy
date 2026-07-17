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

namespace Apache.Iggy.Exceptions;

/// <summary>
///     Thrown when a polled message cannot be decrypted. The whole batch is aborted and its offset is not
///     committed, so the consumer stays stuck at the poison message until it is skipped with
///     <c>StoreOffsetAsync(<see cref="Offset" /> + 1, <see cref="PartitionId" />)</c> (effective only for
///     server-tracked strategies such as <c>PollingStrategy.Next()</c> or consumer groups).
///     WARNING: the abort also discards messages before <see cref="Offset" /> that decrypted fine, so skipping
///     past the failure silently skips those too; re-poll the range below <see cref="Offset" /> first to recover
///     them.
/// </summary>
public sealed class MessageDecryptionException : Exception
{
    /// <summary>
    ///     Offset of the message that failed to decrypt.
    /// </summary>
    public ulong Offset { get; }

    /// <summary>
    ///     Partition the failing message was polled from.
    /// </summary>
    public uint PartitionId { get; }

    /// <summary>
    ///     Initializes a new instance of the <see cref="MessageDecryptionException" /> class.
    /// </summary>
    /// <param name="offset">Offset of the message that failed to decrypt.</param>
    /// <param name="partitionId">Partition the failing message was polled from.</param>
    /// <param name="innerException">The underlying cryptographic failure.</param>
    public MessageDecryptionException(ulong offset, uint partitionId, Exception innerException)
        : base($"Failed to decrypt message at offset {offset} in partition {partitionId}.", innerException)
    {
        Offset = offset;
        PartitionId = partitionId;
    }
}
