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
using Apache.Iggy.Exceptions;
using Apache.Iggy.Vsr;

namespace Apache.Iggy.Tests.VsrTests;

public sealed class SyncConsumerGroupTests
{
    [Fact]
    public void Decode_ReadsGenerationAndPartitions()
    {
        var assignment = SyncConsumerGroupAssignment.Decode(Encode(7, [0, 2, 4]));

        Assert.Equal(7ul, assignment.Generation);
        Assert.Equal<uint[]>([0, 2, 4], [.. assignment.Partitions]);
    }

    [Fact]
    public void Decode_ReadsEmptyAssignment()
    {
        var assignment = SyncConsumerGroupAssignment.Decode(Encode(1, []));

        Assert.Equal(1ul, assignment.Generation);
        Assert.Empty(assignment.Partitions);
    }

    [Fact]
    public void Decode_IgnoresTrailingBytes()
    {
        var body = Encode(1, [3]).Concat(new byte[8]).ToArray();

        Assert.Equal<uint[]>([3], [.. SyncConsumerGroupAssignment.Decode(body).Partitions]);
    }

    [Fact]
    public void Decode_TruncatedBodyThrows()
    {
        var body = Encode(7, [1, 2]);
        for (var length = 0; length < body.Length; length++)
        {
            var truncated = body[..length];
            var error = Assert.Throws<IggyInvalidStatusCodeException>(() =>
                SyncConsumerGroupAssignment.Decode(truncated));

            Assert.Equal(VsrError.INVALID_COMMAND, error.StatusCode);
        }
    }

    /// <summary>
    ///     A count the body cannot back must fail rather than allocate for it: the value is attacker-reachable
    ///     through a corrupted frame.
    /// </summary>
    [Fact]
    public void Decode_ImplausiblePartitionCountThrows()
    {
        var body = new byte[12];
        BinaryPrimitives.WriteUInt32LittleEndian(body.AsSpan(8, 4), uint.MaxValue);

        var error = Assert.Throws<IggyInvalidStatusCodeException>(() => SyncConsumerGroupAssignment.Decode(body));

        Assert.Equal(VsrError.INVALID_COMMAND, error.StatusCode);
    }

    private static byte[] Encode(ulong generation, uint[] partitions)
    {
        var body = new byte[12 + partitions.Length * 4];
        BinaryPrimitives.WriteUInt64LittleEndian(body.AsSpan(0, 8), generation);
        BinaryPrimitives.WriteUInt32LittleEndian(body.AsSpan(8, 4), (uint)partitions.Length);
        for (var i = 0; i < partitions.Length; i++)
        {
            BinaryPrimitives.WriteUInt32LittleEndian(body.AsSpan(12 + i * 4, 4), partitions[i]);
        }

        return body;
    }
}
