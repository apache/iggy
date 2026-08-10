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

using System.Buffers.Binary;

namespace Apache.Iggy.Vsr;

/// <summary>
///     Reply body of a <c>SyncConsumerGroup</c> request: the requesting member's current partition assignment
///     and the generation it belongs to. Mirrors
///     <c>core/binary_protocol/src/responses/consumer_groups/sync_consumer_group.rs</c>.
/// </summary>
/// <remarks>
///     Wire format: <c>[generation u64][partitions_count u32][partition_id u32]*</c>. The request body is the
///     plain <c>[stream][topic][group]</c> identifier triple every other group request already builds.
/// </remarks>
internal readonly record struct SyncConsumerGroupAssignment(ulong Generation, IReadOnlyList<uint> Partitions)
{
    internal static SyncConsumerGroupAssignment Decode(ReadOnlySpan<byte> body)
    {
        if (body.Length < 12)
        {
            throw Malformed();
        }

        var generation = BinaryPrimitives.ReadUInt64LittleEndian(body[..8]);
        var count = BinaryPrimitives.ReadUInt32LittleEndian(body.Slice(8, 4));
        if (body.Length < 12 + (long)count * 4)
        {
            throw Malformed();
        }

        var partitions = new uint[count];
        for (var i = 0; i < partitions.Length; i++)
        {
            partitions[i] = BinaryPrimitives.ReadUInt32LittleEndian(body.Slice(12 + i * 4, 4));
        }

        return new SyncConsumerGroupAssignment(generation, partitions);
    }

    private static Exception Malformed()
    {
        return VsrError.Exception(VsrError.INVALID_COMMAND,
            "Consumer group assignment reply is too short for the partition count it declares.");
    }
}
