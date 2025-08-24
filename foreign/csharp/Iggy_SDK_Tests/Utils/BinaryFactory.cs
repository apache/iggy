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
using Apache.Iggy.Contracts;
using Apache.Iggy.Extensions;

namespace Apache.Iggy.Tests.Utils;

internal sealed class BinaryFactory
{
    internal static byte[] CreatePersonalAccessTokensPayload(string name, uint expiry)
    {
        Span<byte> result = stackalloc byte[9 + name.Length];
        result[0] = (byte)name.Length;
        Encoding.UTF8.GetBytes(name, result[1..(1 + name.Length)]);
        BinaryPrimitives.WriteUInt32LittleEndian(result[(1 + name.Length)..], expiry);
        return result.ToArray();
    }

    internal static byte[] CreateOffsetPayload(int partitionId, ulong currentOffset, ulong offset)
    {
        var payload = new byte[20];
        BinaryPrimitives.WriteInt32LittleEndian(payload, partitionId);
        BinaryPrimitives.WriteUInt64LittleEndian(payload.AsSpan(4), currentOffset);
        BinaryPrimitives.WriteUInt64LittleEndian(payload.AsSpan(12), offset);
        return payload;
    }

    internal static byte[] CreateMessagePayload(ulong offset, ulong timestamp, int headersLength, uint checkSum,
        Guid guid, ReadOnlySpan<byte> payload)
    {
        var messageLength = payload.Length;
        var totalSize = 56 + payload.Length;
        Span<byte> payloadBuffer = new byte[totalSize].AsSpan();

        BinaryPrimitives.WriteUInt64LittleEndian(payloadBuffer[..8], checkSum);
        BinaryPrimitives.WriteUInt128LittleEndian(payloadBuffer[8..24], guid.ToUInt128());
        BinaryPrimitives.WriteUInt64LittleEndian(payloadBuffer[24..32], offset);
        BinaryPrimitives.WriteUInt64LittleEndian(payloadBuffer[32..+40], timestamp);
        BinaryPrimitives.WriteUInt64LittleEndian(payloadBuffer[40..48], timestamp);
        BinaryPrimitives.WriteInt32LittleEndian(payloadBuffer[48..52], headersLength);
        BinaryPrimitives.WriteInt32LittleEndian(payloadBuffer[52..56], payload.Length);

        payload.CopyTo(payloadBuffer[56..(56 + messageLength)]);

        return payloadBuffer.ToArray();
    }

    internal static byte[] CreateStreamPayload(uint id, int topicsCount, string name, ulong sizeBytes,
        ulong messagesCount, ulong createdAt)
    {
        var nameBytes = Encoding.UTF8.GetBytes(name);
        var totalSize = 4 + 4 + 8 + 8 + 1 + 8 + nameBytes.Length;
        var payload = new byte[totalSize];
        BinaryPrimitives.WriteUInt32LittleEndian(payload, id);
        BinaryPrimitives.WriteUInt64LittleEndian(payload.AsSpan(4), createdAt);
        BinaryPrimitives.WriteInt32LittleEndian(payload.AsSpan(12), topicsCount);
        BinaryPrimitives.WriteUInt64LittleEndian(payload.AsSpan(16), sizeBytes);
        BinaryPrimitives.WriteUInt64LittleEndian(payload.AsSpan(24), messagesCount);
        payload[32] = (byte)nameBytes.Length;
        nameBytes.CopyTo(payload.AsSpan(33));
        return payload;
    }

    internal static byte[] CreateTopicPayload(uint id, uint partitionsCount, uint messageExpiry, string name,
        ulong sizeBytes, ulong messagesCount, ulong createdAt, byte replicationFactor, ulong maxTopicSize,
        int compressionType)
    {
        var nameBytes = Encoding.UTF8.GetBytes(name);
        var totalSize = 4 + 8 + 4 + 8 + 1 + 8 + 8 + 8 + 1 + 1 + name.Length;

        var payload = new byte[totalSize];
        BinaryPrimitives.WriteUInt32LittleEndian(payload, id);
        BinaryPrimitives.WriteUInt64LittleEndian(payload.AsSpan(4), createdAt);
        BinaryPrimitives.WriteUInt32LittleEndian(payload.AsSpan(12), partitionsCount);
        BinaryPrimitives.WriteInt64LittleEndian(payload.AsSpan(16), messageExpiry);
        payload[24] = (byte)compressionType;
        BinaryPrimitives.WriteUInt64LittleEndian(payload.AsSpan(25), maxTopicSize);
        payload[33] = replicationFactor;
        BinaryPrimitives.WriteUInt64LittleEndian(payload.AsSpan(34), sizeBytes);
        BinaryPrimitives.WriteUInt64LittleEndian(payload.AsSpan(42), messagesCount);
        payload[50] = (byte)nameBytes.Length;
        nameBytes.CopyTo(payload.AsSpan(51));
        return payload;
    }

    internal static byte[] CreatePartitionPayload(int id, int segmentsCount, int currentOffset, ulong sizeBytes,
        ulong messagesCount)
    {
        var payload = new byte[16];
        BinaryPrimitives.WriteInt32LittleEndian(payload, id);
        BinaryPrimitives.WriteInt32LittleEndian(payload.AsSpan(4), segmentsCount);
        BinaryPrimitives.WriteInt32LittleEndian(payload.AsSpan(8), currentOffset);
        BinaryPrimitives.WriteUInt64LittleEndian(payload.AsSpan(12), sizeBytes);
        BinaryPrimitives.WriteUInt64LittleEndian(payload.AsSpan(16), messagesCount);
        return payload;
    }

    internal static byte[] CreateGroupPayload(uint id, uint membersCount, uint partitionsCount, string name,
        List<int>? partitionsOnMember = null)
    {
        var payload = new byte[13 + name.Length + (partitionsOnMember?.Count * 4 + 8 ?? 0)];
        BinaryPrimitives.WriteUInt32LittleEndian(payload, id);
        BinaryPrimitives.WriteUInt32LittleEndian(payload.AsSpan(4), partitionsCount);
        BinaryPrimitives.WriteUInt32LittleEndian(payload.AsSpan(8), membersCount);
        payload[12] = (byte)name.Length;
        var nameBytes = Encoding.UTF8.GetBytes(name);
        nameBytes.CopyTo(payload.AsSpan(13));
        if (partitionsOnMember is not null)
        {
            BinaryPrimitives.WriteInt32LittleEndian(payload.AsSpan(13 + name.Length), 30);
            BinaryPrimitives.WriteInt32LittleEndian(payload.AsSpan(17 + name.Length), partitionsOnMember.Count);
            for (var i = 0; i < partitionsOnMember.Count; i++)
            {
                BinaryPrimitives.WriteInt32LittleEndian(payload.AsSpan(21 + name.Length + i * 4),
                    partitionsOnMember[i]);
            }
        }

        return payload;
    }

    internal static byte[] CreateStatsPayload(StatsResponse stats)
    {
        var bytes = new byte[1024];
        BinaryPrimitives.WriteInt32LittleEndian(bytes.AsSpan(0, 4), stats.ProcessId);
        BinaryPrimitives.WriteSingleLittleEndian(bytes.AsSpan(4, 4), stats.CpuUsage);
        BinaryPrimitives.WriteSingleLittleEndian(bytes.AsSpan(8, 8), stats.TotalCpuUsage);
        BinaryPrimitives.WriteUInt64LittleEndian(bytes.AsSpan(12, 8), stats.MemoryUsage);
        BinaryPrimitives.WriteUInt64LittleEndian(bytes.AsSpan(20, 8), stats.TotalMemory);
        BinaryPrimitives.WriteUInt64LittleEndian(bytes.AsSpan(28, 8), stats.AvailableMemory);
        BinaryPrimitives.WriteUInt64LittleEndian(bytes.AsSpan(36, 8), stats.RunTime);
        BinaryPrimitives.WriteUInt64LittleEndian(bytes.AsSpan(44, 8),
            DateTimeOffsetUtils.ToUnixTimeMicroSeconds(stats.StartTime));
        BinaryPrimitives.WriteUInt64LittleEndian(bytes.AsSpan(52, 8), stats.ReadBytes);
        BinaryPrimitives.WriteUInt64LittleEndian(bytes.AsSpan(60, 8), stats.WrittenBytes);
        BinaryPrimitives.WriteUInt64LittleEndian(bytes.AsSpan(68, 8), stats.MessagesSizeBytes);
        BinaryPrimitives.WriteInt32LittleEndian(bytes.AsSpan(76, 4), stats.StreamsCount);
        BinaryPrimitives.WriteInt32LittleEndian(bytes.AsSpan(80, 4), stats.TopicsCount);
        BinaryPrimitives.WriteInt32LittleEndian(bytes.AsSpan(84, 4), stats.PartitionsCount);
        BinaryPrimitives.WriteInt32LittleEndian(bytes.AsSpan(88, 4), stats.SegmentsCount);
        BinaryPrimitives.WriteUInt64LittleEndian(bytes.AsSpan(92, 8), stats.MessagesCount);
        BinaryPrimitives.WriteInt32LittleEndian(bytes.AsSpan(100, 4), stats.ClientsCount);
        BinaryPrimitives.WriteInt32LittleEndian(bytes.AsSpan(104, 4), stats.ConsumerGroupsCount);

        // Convert string properties to bytes and set them in the byte array
        var hostnameBytes = Encoding.UTF8.GetBytes(stats.Hostname);
        BinaryPrimitives.WriteInt32LittleEndian(bytes.AsSpan(108, 4), hostnameBytes.Length);
        hostnameBytes.CopyTo(bytes, 112);

        var osNameBytes = Encoding.UTF8.GetBytes(stats.OsName);
        BinaryPrimitives.WriteInt32LittleEndian(bytes.AsSpan(112 + hostnameBytes.Length, 4), osNameBytes.Length);
        osNameBytes.CopyTo(bytes, 116 + hostnameBytes.Length);

        var osVersionBytes = Encoding.UTF8.GetBytes(stats.OsVersion);
        BinaryPrimitives.WriteInt32LittleEndian(bytes.AsSpan(116 + hostnameBytes.Length + osNameBytes.Length, 4),
            osVersionBytes.Length);
        osVersionBytes.CopyTo(bytes, 120 + hostnameBytes.Length + osNameBytes.Length);

        var kernelVersionBytes = Encoding.UTF8.GetBytes(stats.KernelVersion);
        BinaryPrimitives.WriteInt32LittleEndian(
            bytes.AsSpan(120 + hostnameBytes.Length + osNameBytes.Length + osVersionBytes.Length, 4),
            kernelVersionBytes.Length);
        kernelVersionBytes.CopyTo(bytes, 124 + hostnameBytes.Length + osNameBytes.Length + osVersionBytes.Length);

        return bytes;
    }
}