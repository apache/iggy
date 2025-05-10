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

namespace Iggy_SDK_Tests.Utils;

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

    internal static byte[] CreateMessagePayload(ulong offset, ulong timestamp, int headersLength, uint checkSum, Guid guid, ReadOnlySpan<byte> payload)
    {
        var messageLength = payload.Length;
        var totalSize = 45 + payload.Length;
        var payloadBuffer = new byte[totalSize];


        BinaryPrimitives.WriteUInt64LittleEndian(payloadBuffer, (ulong)offset);
        payloadBuffer.AsSpan()[8] = (byte)10;
        BinaryPrimitives.WriteUInt64LittleEndian(payloadBuffer.AsSpan(9), (ulong)timestamp);
        var idBytes = guid.ToByteArray();
        idBytes.CopyTo(payloadBuffer.AsSpan()[17..33]);
        BinaryPrimitives.WriteUInt32LittleEndian(payloadBuffer.AsSpan(33), (uint)checkSum);
        BinaryPrimitives.WriteUInt32LittleEndian(payloadBuffer.AsSpan(37), (uint)headersLength);
        BinaryPrimitives.WriteUInt32LittleEndian(payloadBuffer.AsSpan(41), (uint)messageLength);
        payload.CopyTo(payloadBuffer.AsSpan(45));
        return payloadBuffer;
    }
    internal static byte[] CreateStreamPayload(int id, int topicsCount, string name, ulong sizeBytes, ulong messagesCount, ulong createdAt)
    {
        var nameBytes = Encoding.UTF8.GetBytes(name);
        var totalSize = 4 + 4 + 8 + 8 + 1 + 8 + nameBytes.Length;
        var payload = new byte[totalSize];
        BinaryPrimitives.WriteInt32LittleEndian(payload, id);
        BinaryPrimitives.WriteUInt64LittleEndian(payload.AsSpan(4), createdAt);
        BinaryPrimitives.WriteInt32LittleEndian(payload.AsSpan(12), topicsCount);
        BinaryPrimitives.WriteUInt64LittleEndian(payload.AsSpan(16), sizeBytes);
        BinaryPrimitives.WriteUInt64LittleEndian(payload.AsSpan(24), messagesCount);
        payload[32] = (byte)nameBytes.Length;
        nameBytes.CopyTo(payload.AsSpan(33));
        return payload;
    }
    /*
    int id = BinaryPrimitives.ReadInt32LittleEndian(payload[position..(position + 4)]);
    ulong createdAt = BinaryPrimitives.ReadUInt64LittleEndian(payload[(position + 4)..(position + 12)]);
    int partitionsCount = BinaryPrimitives.ReadInt32LittleEndian(payload[(position + 12)..(position + 16)]);
    int messageExpiry = BinaryPrimitives.ReadInt32LittleEndian(payload[(position + 16)..(position + 20)]);
    ulong maxTopicSize = BinaryPrimitives.ReadUInt64LittleEndian(payload[(position + 20)..(position + 28)]);
    byte replicationFactor = payload[position + 28];
    ulong sizeBytes = BinaryPrimitives.ReadUInt64LittleEndian(payload[(position + 29)..(position + 37)]);
    ulong messagesCount = BinaryPrimitives.ReadUInt64LittleEndian(payload[(position + 37)..(position + 45)]);
    int nameLength = (int)payload[position + 45];
    string name = Encoding.UTF8.GetString(payload[(position + 46)..(position + 46 + nameLength)]);
    int readBytes = 4 + 8 + 4 + 4 + 8 + 8 + 8 + 1 + 1 + name.Length;
    */
    internal static byte[] CreateTopicPayload(int id, int partitionsCount, int messageExpiry, string name,
        ulong sizeBytes, ulong messagesCount, ulong createdAt, byte replicationFactor, ulong maxTopicSize)
    {
        var nameBytes = Encoding.UTF8.GetBytes(name);
        int totalSize = 4 + 8 + 4 + 4 + 8 + 8 + 8 + 1 + 1 + name.Length;        

        var payload = new byte[totalSize];
        BinaryPrimitives.WriteInt32LittleEndian(payload, id);
        BinaryPrimitives.WriteUInt64LittleEndian(payload.AsSpan(4), createdAt);
        BinaryPrimitives.WriteInt32LittleEndian(payload.AsSpan(12), partitionsCount);
        BinaryPrimitives.WriteInt32LittleEndian(payload.AsSpan(16), messageExpiry);
        BinaryPrimitives.WriteUInt64LittleEndian(payload.AsSpan(20), maxTopicSize);
        payload[28] = replicationFactor;
        BinaryPrimitives.WriteUInt64LittleEndian(payload.AsSpan(29), sizeBytes);
        BinaryPrimitives.WriteUInt64LittleEndian(payload.AsSpan(37), messagesCount);
        payload[45] = (byte)nameBytes.Length;
        nameBytes.CopyTo(payload.AsSpan(46));
        return payload;
    }

    internal static byte[] CreatePartitionPayload(int id, int segmentsCount, int currentOffset, ulong sizeBytes, ulong messagesCount)
    {
        var payload = new byte[16];
        BinaryPrimitives.WriteInt32LittleEndian(payload, id);
        BinaryPrimitives.WriteInt32LittleEndian(payload.AsSpan(4), segmentsCount);
        BinaryPrimitives.WriteInt32LittleEndian(payload.AsSpan(8), currentOffset);
        BinaryPrimitives.WriteUInt64LittleEndian(payload.AsSpan(12), sizeBytes);
        BinaryPrimitives.WriteUInt64LittleEndian(payload.AsSpan(16), messagesCount);
        return payload;
    }

    internal static byte[] CreateGroupPayload(int id, int membersCount, int partitionsCount, string name, List<int>? partitionsOnMember = null)
    {
        var payload = new byte[13 + name.Length + ((partitionsOnMember?.Count * 4) + 8 ?? 0)];
        BinaryPrimitives.WriteInt32LittleEndian(payload, id);
        BinaryPrimitives.WriteInt32LittleEndian(payload.AsSpan(4), partitionsCount);
        BinaryPrimitives.WriteInt32LittleEndian(payload.AsSpan(8), membersCount);
        payload[12] = (byte)name.Length;
        var nameBytes = Encoding.UTF8.GetBytes(name);
        nameBytes.CopyTo(payload.AsSpan(13 ));
        if (partitionsOnMember is not null)
        {
            BinaryPrimitives.WriteInt32LittleEndian(payload.AsSpan(13 + name.Length), 30);
            BinaryPrimitives.WriteInt32LittleEndian(payload.AsSpan(17 + name.Length), partitionsOnMember.Count);
            for (int i = 0; i < partitionsOnMember.Count; i++)
            {
                BinaryPrimitives.WriteInt32LittleEndian(payload.AsSpan(21 + name.Length + i * 4), partitionsOnMember[i]);
            }
        }
        return payload;
    }

    internal static byte[] CreateStatsPayload(Iggy_SDK.Contracts.Http.Stats stats)
    {
        byte[] bytes = new byte[1024];
        BinaryPrimitives.WriteInt32LittleEndian(bytes.AsSpan(0, 4), stats.ProcessId);
        BinaryPrimitives.WriteSingleLittleEndian(bytes.AsSpan(4, 4), stats.CpuUsage);
        BinaryPrimitives.WriteSingleLittleEndian(bytes.AsSpan(8, 8), stats.TotalCpuUsage);
        BinaryPrimitives.WriteUInt64LittleEndian(bytes.AsSpan(12, 8), stats.MemoryUsage);
        BinaryPrimitives.WriteUInt64LittleEndian(bytes.AsSpan(20, 8), stats.TotalMemory);
        BinaryPrimitives.WriteUInt64LittleEndian(bytes.AsSpan(28, 8), stats.AvailableMemory);
        BinaryPrimitives.WriteUInt64LittleEndian(bytes.AsSpan(36, 8), stats.RunTime);
        BinaryPrimitives.WriteUInt64LittleEndian(bytes.AsSpan(44, 8), (ulong)stats.StartTime.ToUnixTimeSeconds());
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
        byte[] hostnameBytes = Encoding.UTF8.GetBytes(stats.Hostname);
        BinaryPrimitives.WriteInt32LittleEndian(bytes.AsSpan(108, 4), hostnameBytes.Length);
        hostnameBytes.CopyTo(bytes, 112);

        byte[] osNameBytes = Encoding.UTF8.GetBytes(stats.OsName);
        BinaryPrimitives.WriteInt32LittleEndian(bytes.AsSpan(112 + hostnameBytes.Length, 4), osNameBytes.Length);
        osNameBytes.CopyTo(bytes, 116 + hostnameBytes.Length);

        byte[] osVersionBytes = Encoding.UTF8.GetBytes(stats.OsVersion);
        BinaryPrimitives.WriteInt32LittleEndian(bytes.AsSpan(116 + hostnameBytes.Length + osNameBytes.Length, 4), osVersionBytes.Length);
        osVersionBytes.CopyTo(bytes, 120 + hostnameBytes.Length + osNameBytes.Length);

        byte[] kernelVersionBytes = Encoding.UTF8.GetBytes(stats.KernelVersion);
        BinaryPrimitives.WriteInt32LittleEndian(bytes.AsSpan(120 + hostnameBytes.Length + osNameBytes.Length + osVersionBytes.Length, 4), kernelVersionBytes.Length);
        kernelVersionBytes.CopyTo(bytes, 124 + hostnameBytes.Length + osNameBytes.Length + osVersionBytes.Length);

        return bytes;
    }
}