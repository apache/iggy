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
using Apache.Iggy.Contracts;
using Apache.Iggy.Contracts.Tcp;
using Apache.Iggy.Exceptions;
using Apache.Iggy.Kinds;
using Apache.Iggy.Mappers;
using Apache.Iggy.Tests.ConsumerTests;
using Apache.Iggy.Vsr;

namespace Apache.Iggy.Tests.ContractsTests;

/// <summary>
///     Cross-SDK golden bodies of the fetch (code 109) from wire.md: consumer group 7, stream 1, topic "orders", a
///     five-second wait, a six-second request timeout, minimum count 1, count 100, a 1 MiB reply limit and auto
///     commit. The targets are (1, Offset 41) and (2, Next), each with a 512 KiB limit, incarnation 3 and owner 9.
///     The reply holds the message "hello" at offset 41 of partition 1, and refuses partition 2 as not primary.
/// </summary>
public sealed class FetchMessagesContractsTests
{
    private const string FetchBody =
        "0201040700000001040100000002066f7264657273404b4c0000000000808d5b0000000000010000006400000000001000" +
        "0102000000010000000129000000000000000000080003000000000000000900000000000000020000000500000000000000" +
        "000000080003000000000000000900000000000000";

    private const string FetchReplyBody =
        "020000000100000000000000290000000000000001000000030000000000000009000000000000000c000000000000002a00" +
        "0000000000003501000002000000590000000000000000000000000000000000000000000000000000000000000000000000" +
        "000000000000000000000000000000000100000000000000290000000000000064401e18240a060000401e18240a06003501" +
        "000000000000bd2d787d3cdb745c010000000000000000000000000000000000000000000000000000000000000000000000" +
        "0000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000" +
        "0000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000" +
        "0000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000" +
        "000000000000000000000000000000000000000000008e5ca59edbbdbfb90100000000000000000000000000000000000000" +
        "000000000000000005000000000000000000000068656c6c6f";

    private const uint TargetMaxBytes = 512 * 1024;
    private const int SecondSectionStatus = FetchedMessages.SECTIONS_COUNT_SIZE + FetchedMessages.SECTION_HEADER_SIZE + 4;
    private const int MessagesCountInHeader = 16;
    private const int RecordsLengthInHeader = 52;

    private static readonly PartitionContext Fence = new(3, 9, 0);

    private static readonly DeferredPollOptions Options = new()
    {
        MaxWait = TimeSpan.FromSeconds(5),
        RequestTimeout = TimeSpan.FromSeconds(6)
    };

    private static readonly (uint PartitionId, PollingStrategy Strategy)[] Targets =
    [
        (1, PollingStrategy.Offset(41).WithContext(Fence)),
        (2, PollingStrategy.Next().WithContext(Fence))
    ];

    [Fact]
    public void FetchMessages_EncodesTheFetchGoldenBody()
    {
        var body = TcpContracts.FetchMessages(Consumer.Group(7), Identifier.Numeric(1), Identifier.String("orders"),
            Options, 100, true, TargetMaxBytes, Targets);

        Assert.Equal(Convert.FromHexString(FetchBody), body);
    }

    [Fact]
    public void FetchBody_NamesItsCommitItsProgressAndItsFences()
    {
        var body = Convert.FromHexString(FetchBody);
        var offsetsOnly = TcpContracts.FetchMessages(Consumer.Group(7), Identifier.Numeric(1),
            Identifier.String("orders"), Options, 100, false, TargetMaxBytes, [(4, PollingStrategy.Offset(0))]);

        Assert.True(TcpContracts.FetchAutoCommits(body));
        Assert.True(TcpContracts.FetchMovesConsumer(body));
        Assert.Equal([(1u, 3ul), (2u, 3ul)], TcpContracts.FetchTargetFences(body));
        Assert.False(TcpContracts.FetchAutoCommits(offsetsOnly));
        Assert.False(TcpContracts.FetchMovesConsumer(offsetsOnly));
        Assert.Equal([(4u, 0ul)], TcpContracts.FetchTargetFences(offsetsOnly));
    }

    [Fact]
    public void MapFetchedMessages_DecodesTheFetchReplyGoldenBody()
    {
        var owner = Owner(Convert.FromHexString(FetchReplyBody));
        var reply = BinaryMapper.MapFetchedMessages(owner, null);

        Assert.Equal(2, reply.Sections.Count);
        var served = reply.Sections[0];
        Assert.Equal((1u, 0u, 41ul, 1u, 42ul, new PartitionContext(3, 9, 12)),
            (served.PartitionId, served.Status, served.CurrentOffset, served.MessagesCount, served.NextOffset,
                served.Context));
        var refused = reply.Sections[1];
        Assert.Equal((2u, (uint)VsrError.NOT_PRIMARY, 0, 0ul), (refused.PartitionId, refused.Status,
            refused.Records.Length, refused.NextOffset));

        var rental = served.DecodeRented();
        var message = Assert.Single(rental.Messages);
        Assert.Equal((41ul, new UInt128(0, 1)), (message.Header.Offset, message.Header.Id));
        Assert.Equal("hello"u8.ToArray(), message.Payload.ToArray());
        Assert.Equal((1u, 41ul, new PartitionContext(3, 9, 12)), (rental.PartitionId, rental.CurrentOffset,
            rental.Context));
        Assert.Equal(VsrError.NOT_PRIMARY, Assert.Throws<IggyInvalidStatusCodeException>(() => refused.Decode()).StatusCode);

        // The reply buffer stays rented while a rental of one of its sections lives.
        reply.Dispose();
        Assert.Equal(0, owner.DisposeCount);
        Assert.Equal("hello"u8.ToArray(), message.Payload.ToArray());
        rental.Dispose();
        Assert.Equal(1, owner.DisposeCount);
        Assert.Throws<ObjectDisposedException>(() => served.DecodeRented());
    }

    [Theory]
    [InlineData("short")]
    [InlineData("too many sections")]
    [InlineData("table past the body")]
    [InlineData("records of a refused section")]
    [InlineData("records past the body")]
    [InlineData("trailing bytes")]
    public void MapFetchedMessages_WithAMalformedReply_Throws(string malformation)
    {
        var golden = Convert.FromHexString(FetchReplyBody);
        var body = malformation switch
        {
            "short" => golden[..3],
            "too many sections" => Count(golden[..FetchedMessages.SECTIONS_COUNT_SIZE],
                TcpContracts.MAX_PARTITIONS_PER_REQUEST + 1),
            "table past the body" => golden[..FetchedMessages.HeadSize(1)],
            "records of a refused section" => WriteUInt32(
                [.. golden, 0], FetchedMessages.HeadSize(1) + RecordsLengthInHeader, 1),
            "records past the body" => WriteUInt32(golden,
                FetchedMessages.SECTIONS_COUNT_SIZE + RecordsLengthInHeader,
                (uint)(golden.Length - FetchedMessages.HeadSize(2) + 1)),
            _ => [.. golden, 0]
        };

        Assert.Throws<MalformedResponseException>(() => BinaryMapper.MapFetchedMessages(Owner(body), null));
    }

    [Fact]
    public void MapFetchedMessages_WithASectionThatReadNothing_DecodesNoMessages()
    {
        var golden = Convert.FromHexString(FetchReplyBody);
        var body = golden[..FetchedMessages.HeadSize(2)];
        WriteUInt32(body, FetchedMessages.SECTIONS_COUNT_SIZE + MessagesCountInHeader, 0);
        WriteUInt32(body, FetchedMessages.SECTIONS_COUNT_SIZE + RecordsLengthInHeader, 0);
        WriteUInt32(body, SecondSectionStatus, 0);

        using var reply = BinaryMapper.MapFetchedMessages(Owner(body), null);

        Assert.All(reply.Sections, section => Assert.Empty(section.Decode().Messages));
    }

    [Theory]
    [InlineData(0, 999_999, false)]
    [InlineData(0, 1_000_000, false)]
    [InlineData(0, 1_000_001, true)]
    [InlineData(5_000_000, 5_999_999, false)]
    [InlineData(5_000_000, 6_000_000, true)]
    public void ValidateFetch_KeepsTheReplyMarginAlsoWithoutAWait(long maxWaitUs, long requestTimeoutUs, bool valid)
    {
        var options = new DeferredPollOptions
        {
            MaxWait = TimeSpan.FromMicroseconds(maxWaitUs),
            RequestTimeout = TimeSpan.FromMicroseconds(requestTimeoutUs)
        };

        if (valid)
        {
            options.ValidateFetch(100);
        }
        else
        {
            Assert.Equal(VsrError.INVALID_COMMAND,
                Assert.Throws<IggyInvalidStatusCodeException>(() => options.ValidateFetch(100)).StatusCode);
        }
    }

    [Fact]
    public void ValidateFetch_WithLessThanOneMicrosecondOfHeadroom_RefusesTheRoundedWireTimeout()
    {
        var options = new DeferredPollOptions
        {
            MaxWait = TimeSpan.Zero,
            RequestTimeout = TimeSpan.FromSeconds(1) + TimeSpan.FromTicks(TimeSpan.TicksPerMicrosecond - 1)
        };

        Assert.Equal(VsrError.INVALID_COMMAND,
            Assert.Throws<IggyInvalidStatusCodeException>(() => options.ValidateFetch(1)).StatusCode);
        options.Validate(1);
    }

    private static RentedConsumerTests.TrackingMemoryOwner Owner(byte[] body)
    {
        var owner = new RentedConsumerTests.TrackingMemoryOwner(body.Length);
        body.CopyTo(owner.Memory);
        return owner;
    }

    private static byte[] Count(byte[] body, int count)
    {
        BinaryPrimitives.WriteUInt32LittleEndian(body, (uint)count);
        return body;
    }

    private static byte[] WriteUInt32(byte[] body, int position, uint value)
    {
        BinaryPrimitives.WriteUInt32LittleEndian(body.AsSpan(position), value);
        return body;
    }
}
