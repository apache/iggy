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
///     The position after one delivered message. A commit stores its offset, so it covers all earlier messages of the
///     partition. A checkpoint keeps the partition incarnation and owner that delivered the message, so a commit after
///     a change of the partition history fails. Only a read creates one.
/// </summary>
public sealed record Checkpoint
{
    internal Checkpoint(uint partitionId, ulong offset, PartitionContext context, ReaderStamp? stamp = null)
    {
        PartitionId = partitionId;
        Offset = offset;
        Context = context;
        Stamp = stamp;
    }

    /// <summary>The partition of the message.</summary>
    public uint PartitionId { get; }

    /// <summary>The offset of the message.</summary>
    public ulong Offset { get; }

    /// <summary>The partition history that delivered the message.</summary>
    internal PartitionContext Context { get; }

    /// <summary>The reader and the partition generation that delivered the message, null for a one-shot poll.</summary>
    internal ReaderStamp? Stamp { get; }
}

/// <summary>
///     The reader and the partition generation that delivered a message. A reader refuses a commit of a checkpoint
///     that another reader delivered, and skips one of an older generation.
/// </summary>
internal readonly record struct ReaderStamp(long Reader, ulong Generation);
