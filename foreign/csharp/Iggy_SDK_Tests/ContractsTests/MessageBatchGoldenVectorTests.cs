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
using Apache.Iggy.Contracts.Tcp;
using Apache.Iggy.Extensions;
using Apache.Iggy.IggyClient.Implementations;
using Apache.Iggy.Kinds;
using Apache.Iggy.Messages;
using Apache.Iggy.Utils;

namespace Apache.Iggy.Tests.ContractsTests;

/// <summary>
///     Cross-SDK golden vectors for the canonical message batch, produced by the Rust encoder
///     (core/binary_protocol/src/requests/messages/send_messages.rs). These bytes are the
///     contract: a change to the batch layout has to break every SDK's copy of them together.
///
///     The produce vector is the SendMessages body for stream 1, topic 2, balanced partitioning,
///     and two messages: {id 7, origin timestamp 1000, payload "first-payload"} and
///     {id 8, origin timestamp 1050, payload "second-payload", user headers "user-header-bytes"}.
///     The poll vector serves that batch back stamped partition 3, base offset 100,
///     base timestamp 5000, current offset 101.
/// </summary>
public sealed class MessageBatchGoldenVectorTests
{
    private const string ProduceBodyFull =
        "12000000010401000000010402000000010002000000000000000000000000000000000000000000000000000000e803" +
        "0000000000008c01000000000000a91f38c86307267c0200000000000000000000000000000000000000000000000000" +
        "000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000" +
        "000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000" +
        "000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000" +
        "0000000000000000000000000000000000000000000000000000000000000000000000000000bfd2b9205a7596750700" +
        "00000000000000000000000000000000000000000000000000000d000000000000000000000066697273742d7061796c" +
        "6f6164d66b7e1c758eb7c0080000000000000000000000000000000100000032000000110000000e0000000000000000" +
        "0000007365636f6e642d7061796c6f6164757365722d6865616465722d6279746573";

    private const string ProduceBatchOnly =
        "000000000000000000000000000000000000000000000000e8030000000000008c01000000000000a91f38c86307267c" +
        "020000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000" +
        "000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000" +
        "000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000" +
        "000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000" +
        "00000000000000000000000000000000bfd2b9205a759675070000000000000000000000000000000000000000000000" +
        "000000000d000000000000000000000066697273742d7061796c6f6164d66b7e1c758eb7c00800000000000000000000" +
        "00000000000100000032000000110000000e00000000000000000000007365636f6e642d7061796c6f6164757365722d" +
        "6865616465722d6279746573";

    private const string PollBody =
        "03000000650000000000000002000000030000000000000064000000000000008813000000000000e803000000000000" +
        "8c01000000000000c96826b38a8feed20200000000000000000000000000000000000000000000000000000000000000" +
        "000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000" +
        "000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000" +
        "000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000" +
        "0000000000000000000000000000000000000000000000000000000000000000bfd2b9205a7596750700000000000000" +
        "00000000000000000000000000000000000000000d000000000000000000000066697273742d7061796c6f6164d66b7e" +
        "1c758eb7c0080000000000000000000000000000000100000032000000110000000e0000000000000000000000736563" +
        "6f6e642d7061796c6f6164757365722d6865616465722d6279746573";

    [Fact]
    public void CreateMessage_EncodesTheProduceGoldenVector()
    {
        var expected = Convert.FromHexString(ProduceBodyFull);
        var messages = GoldenMessages();
        var streamId = Identifier.Numeric(1);
        var topicId = Identifier.Numeric(2);
        var partitioning = Partitioning.None();

        var buffer = new byte[TcpMessageStreamHelpers.CalculateMessageBytesCount(messages, null)
                              + 2 + streamId.Length + 2 + topicId.Length + 2 + partitioning.Length + 4 + 4];
        var written = TcpContracts.CreateMessage(buffer, streamId, topicId, partitioning, messages);

        Assert.Equal(expected.Length, written);
        Assert.Equal(expected, buffer.AsSpan(0, written).ToArray());

        var metadataLength = BinaryPrimitives.ReadInt32LittleEndian(buffer.AsSpan(0, 4));
        Assert.Equal(Convert.FromHexString(ProduceBatchOnly),
            buffer.AsSpan(4 + metadataLength, written - 4 - metadataLength).ToArray());
    }

    [Fact]
    public void CreateMessage_MintsAZeroMessageIdBeforeEncoding()
    {
        var message = new Message(UInt128.Zero, "payload"u8.ToArray());
        var streamId = Identifier.Numeric(1);
        var topicId = Identifier.Numeric(2);
        var partitioning = Partitioning.None();

        var buffer = new byte[TcpMessageStreamHelpers.CalculateMessageBytesCount([message], null)
                              + 2 + streamId.Length + 2 + topicId.Length + 2 + partitioning.Length + 4 + 4];
        var written = TcpContracts.CreateMessage(buffer, streamId, topicId, partitioning, [message]);

        Assert.NotEqual(UInt128.Zero, message.Header.Id);
        var metadataLength = BinaryPrimitives.ReadInt32LittleEndian(buffer.AsSpan(0, 4));
        var frameStart = 4 + metadataLength + 256;
        var wireId = BinaryPrimitives.ReadUInt128LittleEndian(buffer.AsSpan(frameStart + 8, 16));
        Assert.Equal(message.Header.Id, wireId);
        Assert.Equal(buffer.Length, written);
    }

    [Fact]
    public void CreateMessage_EmptyBatch_Throws()
    {
        Assert.Throws<ArgumentException>(() => TcpContracts.CreateMessage(new byte[512], Identifier.Numeric(1),
            Identifier.Numeric(2), Partitioning.None(), ReadOnlySpan<Message>.Empty));
    }

    [Fact]
    public void CreateMessage_TimestampDeltaOverflow_Throws()
    {
        var messages = new[]
        {
            new Message(new UInt128(0, 1), "a"u8.ToArray()),
            new Message(new UInt128(0, 2), "b"u8.ToArray())
        };
        messages[1].Header = messages[1].Header with { OriginTimestamp = (ulong)uint.MaxValue + 1 };

        Assert.Throws<ArgumentException>(() => TcpContracts.CreateMessage(new byte[1024], Identifier.Numeric(1),
            Identifier.Numeric(2), Partitioning.None(), messages));
    }

    [Fact]
    public void MapRentedMessages_DecodesThePollGoldenVector()
    {
        var pollBody = Convert.FromHexString(PollBody);

        using var rental =
            Mappers.BinaryMapper.MapRentedMessages(pollBody, TcpMessageStream.EmptyMemoryOwner.Instance);

        Assert.Equal(3, rental.PartitionId);
        Assert.Equal(101ul, rental.CurrentOffset);
        Assert.Equal(2, rental.Messages.Count);

        var first = rental.Messages[0];
        Assert.Equal(100ul, first.Header.Offset);
        Assert.Equal(DateTimeOffsetUtils.FromUnixTimeMicroSeconds(5000), first.Header.Timestamp);
        Assert.Equal(1000ul, first.Header.OriginTimestamp);
        Assert.Equal(new UInt128(0, 7), first.Header.Id);
        Assert.Equal("first-payload"u8.ToArray(), first.Payload.ToArray());
        Assert.True(first.RawUserHeaders.IsEmpty);
        Assert.Equal(BinaryPrimitives.ReadUInt64LittleEndian(pollBody.AsSpan(16 + 256, 8)), first.Header.Checksum);

        var second = rental.Messages[1];
        Assert.Equal(101ul, second.Header.Offset);
        Assert.Equal(DateTimeOffsetUtils.FromUnixTimeMicroSeconds(5000), second.Header.Timestamp);
        Assert.Equal(1050ul, second.Header.OriginTimestamp);
        Assert.Equal(new UInt128(0, 8), second.Header.Id);
        Assert.Equal("second-payload"u8.ToArray(), second.Payload.ToArray());
        Assert.Equal("user-header-bytes"u8.ToArray(), second.RawUserHeaders.ToArray());
        Assert.Equal(
            BinaryPrimitives.ReadUInt64LittleEndian(pollBody.AsSpan(16 + 256 + 48 + "first-payload"u8.Length, 8)),
            second.Header.Checksum);
    }

    private static Message[] GoldenMessages()
    {
        var messages = new[]
        {
            new Message(new UInt128(0, 7), "first-payload"u8.ToArray()),
            new Message(new UInt128(0, 8), "second-payload"u8.ToArray())
        };
        messages[0].Header = messages[0].Header with { OriginTimestamp = 1000 };
        messages[1].Header = messages[1].Header with { OriginTimestamp = 1050 };
        messages[1].RawUserHeaders = "user-header-bytes"u8.ToArray();

        return messages;
    }
}
