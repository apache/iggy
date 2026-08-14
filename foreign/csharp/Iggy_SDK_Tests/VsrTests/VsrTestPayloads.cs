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
using System.Text;

namespace Apache.Iggy.Tests.VsrTests;

/// <summary>
///     Builders for the classic request bodies the VSR encoder peeks into, matching what
///     <c>TcpContracts</c> writes.
/// </summary>
internal static class VsrTestPayloads
{
    internal static byte[] NumericIdentifier(uint value)
    {
        var bytes = new byte[6];
        bytes[0] = 1;
        bytes[1] = 4;
        BinaryPrimitives.WriteUInt32LittleEndian(bytes.AsSpan(2), value);

        return bytes;
    }

    internal static byte[] NamedIdentifier(string value)
    {
        var name = Encoding.UTF8.GetBytes(value);
        var bytes = new byte[2 + name.Length];
        bytes[0] = 2;
        bytes[1] = (byte)name.Length;
        name.CopyTo(bytes, 2);

        return bytes;
    }

    internal static byte[] SendMessages(byte[] streamId, byte[] topicId, byte partitioningKind,
        byte[] partitioningValue, int messagesCount = 1)
    {
        var metadata = Concat(streamId, topicId,
            Concat([partitioningKind, (byte)partitioningValue.Length], partitioningValue),
            UInt32(messagesCount));

        return Concat(UInt32(metadata.Length), metadata);
    }

    internal static byte[] SendMessagesToPartition(uint streamId, uint topicId, uint partitionId)
    {
        return SendMessages(NumericIdentifier(streamId), NumericIdentifier(topicId), 2, UInt32((int)partitionId));
    }

    internal static byte[] ConsumerOffset(byte[] streamId, byte[] topicId, uint? partitionId)
    {
        var partition = new byte[5];
        if (partitionId.HasValue)
        {
            partition[0] = 1;
            BinaryPrimitives.WriteUInt32LittleEndian(partition.AsSpan(1), partitionId.Value);
        }

        return Concat([1], NumericIdentifier(1), streamId, topicId, partition);
    }

    internal static byte[] DeleteSegments(byte[] streamId, byte[] topicId, uint partitionId, uint segmentsCount = 1)
    {
        return Concat(streamId, topicId, UInt32((int)partitionId), UInt32((int)segmentsCount));
    }

    internal static byte[] UInt32(int value)
    {
        var bytes = new byte[4];
        BinaryPrimitives.WriteUInt32LittleEndian(bytes, (uint)value);

        return bytes;
    }

    internal static byte[] Concat(params byte[][] parts)
    {
        var result = new byte[parts.Sum(part => part.Length)];
        var position = 0;
        foreach (var part in parts)
        {
            part.CopyTo(result, position);
            position += part.Length;
        }

        return result;
    }
}
