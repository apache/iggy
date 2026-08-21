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

    internal static byte[] CreateMessageFrame(ulong checksum, Guid guid, uint offsetDelta, uint timestampDelta,
        ReadOnlySpan<byte> userHeaders, ReadOnlySpan<byte> payload)
    {
        Span<byte> frame = new byte[48 + payload.Length + userHeaders.Length].AsSpan();

        BinaryPrimitives.WriteUInt64LittleEndian(frame[..8], checksum);
        BinaryPrimitives.WriteUInt128LittleEndian(frame[8..24], guid.ToUInt128());
        BinaryPrimitives.WriteUInt32LittleEndian(frame[24..28], offsetDelta);
        BinaryPrimitives.WriteUInt32LittleEndian(frame[28..32], timestampDelta);
        BinaryPrimitives.WriteUInt32LittleEndian(frame[32..36], (uint)userHeaders.Length);
        BinaryPrimitives.WriteUInt32LittleEndian(frame[36..40], (uint)payload.Length);
        BinaryPrimitives.WriteUInt64LittleEndian(frame[40..48], 0); // reserved

        payload.CopyTo(frame[48..(48 + payload.Length)]);
        userHeaders.CopyTo(frame[(48 + payload.Length)..]);

        return frame.ToArray();
    }

    /// <summary>
    ///     One batch record: a 256-byte batch header followed by the given frames. The batch checksum is
    ///     left zero; the poll decoder does not verify it.
    /// </summary>
    internal static byte[] CreateBatchRecord(ulong baseOffset, ulong baseTimestamp, ulong originTimestamp,
        params byte[][] frames)
    {
        var blobLength = frames.Sum(frame => frame.Length);
        var record = new byte[256 + blobLength];
        Span<byte> header = record.AsSpan(0, 256);

        BinaryPrimitives.WriteUInt64LittleEndian(header[8..16], baseOffset);
        BinaryPrimitives.WriteUInt64LittleEndian(header[16..24], baseTimestamp);
        BinaryPrimitives.WriteUInt64LittleEndian(header[24..32], originTimestamp);
        BinaryPrimitives.WriteUInt64LittleEndian(header[32..40], (ulong)record.Length);
        BinaryPrimitives.WriteUInt32LittleEndian(header[48..52], (uint)frames.Length);

        var position = 256;
        foreach (var frame in frames)
        {
            frame.CopyTo(record.AsSpan(position));
            position += frame.Length;
        }

        return record;
    }

    internal static byte[] CreateStreamPayload(uint id, int topicsCount, string name, ulong sizeBytes,
        ulong messagesCount, ulong createdAt)
    {
        var nameBytes = Encoding.UTF8.GetBytes(name);
        var totalSize = 4 + 4 + 8 + 8 + 1 + 8 + nameBytes.Length + 4;
        var payload = new byte[totalSize];
        BinaryPrimitives.WriteUInt32LittleEndian(payload, id);
        BinaryPrimitives.WriteUInt64LittleEndian(payload.AsSpan(4), createdAt);
        BinaryPrimitives.WriteInt32LittleEndian(payload.AsSpan(12), topicsCount);
        BinaryPrimitives.WriteUInt64LittleEndian(payload.AsSpan(16), sizeBytes);
        BinaryPrimitives.WriteUInt64LittleEndian(payload.AsSpan(24), messagesCount);
        payload[32] = (byte)nameBytes.Length;
        nameBytes.CopyTo(payload.AsSpan(33));
        // Empty length-prefixed options block.
        BinaryPrimitives.WriteUInt32LittleEndian(payload.AsSpan(33 + nameBytes.Length), 0);
        return payload;
    }

    internal static byte[] CreateTopicPayload(uint id, uint partitionsCount, uint messageExpiry, string name,
        ulong sizeBytes, ulong messagesCount, ulong createdAt, ulong maxTopicSize, int compressionType,
        byte[]? options = null)
    {
        var nameBytes = Encoding.UTF8.GetBytes(name);
        var optionsBytes = options ?? [];
        var totalSize = 4 + 8 + 4 + 8 + 1 + 8 + 8 + 8 + 1 + nameBytes.Length + 4 + optionsBytes.Length + 4;

        var payload = new byte[totalSize];
        BinaryPrimitives.WriteUInt32LittleEndian(payload, id);
        BinaryPrimitives.WriteUInt64LittleEndian(payload.AsSpan(4), createdAt);
        BinaryPrimitives.WriteUInt32LittleEndian(payload.AsSpan(12), partitionsCount);
        BinaryPrimitives.WriteInt64LittleEndian(payload.AsSpan(16), messageExpiry);
        payload[24] = (byte)compressionType;
        BinaryPrimitives.WriteUInt64LittleEndian(payload.AsSpan(25), maxTopicSize);
        BinaryPrimitives.WriteUInt64LittleEndian(payload.AsSpan(33), sizeBytes);
        BinaryPrimitives.WriteUInt64LittleEndian(payload.AsSpan(41), messagesCount);
        payload[49] = (byte)nameBytes.Length;
        nameBytes.CopyTo(payload.AsSpan(50));
        // Length-prefixed explicit options block, then an empty derived one.
        var position = 50 + nameBytes.Length;
        BinaryPrimitives.WriteUInt32LittleEndian(payload.AsSpan(position), (uint)optionsBytes.Length);
        optionsBytes.CopyTo(payload.AsSpan(position + 4));
        BinaryPrimitives.WriteUInt32LittleEndian(payload.AsSpan(position + 4 + optionsBytes.Length), 0);
        return payload;
    }

    /// <summary>
    ///     One options entry: <c>[key_kind][key_len:u32][key][value_kind][value_len:u32][value]</c>. The kinds are
    ///     taken as raw wire codes so a test can encode a code this SDK has no name for.
    /// </summary>
    internal static byte[] CreateOptionEntry(byte keyKind, string key, byte valueKind, byte[] value)
    {
        var keyBytes = Encoding.UTF8.GetBytes(key);
        var entry = new byte[1 + 4 + keyBytes.Length + 1 + 4 + value.Length];
        entry[0] = keyKind;
        BinaryPrimitives.WriteUInt32LittleEndian(entry.AsSpan(1), (uint)keyBytes.Length);
        keyBytes.CopyTo(entry.AsSpan(5));

        var position = 5 + keyBytes.Length;
        entry[position] = valueKind;
        BinaryPrimitives.WriteUInt32LittleEndian(entry.AsSpan(position + 1), (uint)value.Length);
        value.CopyTo(entry.AsSpan(position + 5));
        return entry;
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
