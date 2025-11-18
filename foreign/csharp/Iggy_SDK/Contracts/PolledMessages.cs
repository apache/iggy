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
///     Response from the server containing a list of messages and the current offset.
/// </summary>
public sealed class PolledMessages
{
    /// <summary>
    ///     Partition identifier for the messages.
    /// </summary>
    public required int PartitionId { get; init; }

    /// <summary>
    ///     Current offset for the partition.
    /// </summary>
    public required ulong CurrentOffset { get; init; }

    /// <summary>
    ///     List of messages.
    /// </summary>
    public required IReadOnlyList<MessageResponse> Messages { get; set; }

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
