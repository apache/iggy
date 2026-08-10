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

namespace Apache.Iggy.Contracts;

/// <summary>
///     Commit confirmation for one partition written by a send messages request.
/// </summary>
/// <remarks>
///     <see cref="BaseOffset" /> is the offset assigned to the first message of the batch in that
///     partition, bounded by two properties of the send path:
///     <list type="bullet">
///         <item>
///             Delivery is at-least-once. An earlier retry of the same batch may already have committed
///             at a lower offset, so the value never implies uniqueness.
///         </item>
///         <item>
///             A batch is confirmed once it is committed in memory, not once it is fsynced. A
///             crash-restart can stamp a later batch with an offset a client has already recorded.
///         </item>
///     </list>
/// </remarks>
public sealed class SendMessagesConfirmation
{
    /// <summary>
    ///     Stream identifier the batch was written to.
    /// </summary>
    public required uint StreamId { get; init; }

    /// <summary>
    ///     Topic identifier the batch was written to.
    /// </summary>
    public required uint TopicId { get; init; }

    /// <summary>
    ///     Partition the batch landed in.
    /// </summary>
    public required uint PartitionId { get; init; }

    /// <summary>
    ///     Offset assigned to the first message of the batch in that partition.
    /// </summary>
    public required ulong BaseOffset { get; init; }
}

/// <summary>
///     Result of a send messages request.
/// </summary>
/// <remarks>
///     An empty <see cref="Confirmations" /> list means the batch committed with no offsets to
///     report (e.g. a background send merged into a larger batch). The server currently reports a
///     single partition per request; the list shape lets a later multi-partition send grow the
///     count without an API change.
/// </remarks>
public sealed class SendMessagesResponse
{
    /// <summary>
    ///     One confirmation per partition the batch was committed to.
    /// </summary>
    public required IReadOnlyList<SendMessagesConfirmation> Confirmations { get; init; }

    internal static SendMessagesResponse Empty { get; } = new() { Confirmations = [] };
}
