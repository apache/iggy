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

using Apache.Iggy.Contracts;
using Apache.Iggy.Contracts.Tcp;
using Apache.Iggy.Exceptions;
using Apache.Iggy.Kinds;
using Apache.Iggy.Vsr;

namespace Apache.Iggy.Tests.ContractsTests;

/// <summary>
///     Cross-SDK golden body of the deferred poll (code 106), produced by the
///     Rust encoder: consumer group 7, stream 1, topic "orders", partition 3, Next, count 100, auto commit, and the
///     default options (a one-second wait, minimum count 1, a 1 MiB reply limit, an eleven-second request
///     timeout).
/// </summary>
public sealed class DeferredPollContractsTests
{
    private const string PollBody =
        "0201040700000001040100000002066f726465727301030000000500000000000000006400000001";

    private const string DeferredPollBody =
        "0201040700000001040100000002066f72646572730103000000050000000000000000640000000140420f0000000000" +
        "0100000000001000c0d8a70000000000";

    private const uint Count = 100;
    private const uint Partition = 3;

    private static readonly Consumer Group = Consumer.Group(7);
    private static readonly Identifier Stream = Identifier.Numeric(1);
    private static readonly Identifier Topic = Identifier.String("orders");

    [Fact]
    public void GetMessages_WithoutOptions_EncodesThePollGoldenBody()
    {
        var expected = Convert.FromHexString(PollBody);
        var body = new byte[expected.Length];

        TcpContracts.GetMessages(body, Group, Stream, Topic, PollingStrategy.Next(), Count, true, Partition);

        Assert.Equal(expected, body);
    }

    [Fact]
    public void GetMessages_WithOptions_AppendsTheDeferredTailToThePollBody()
    {
        var expected = Convert.FromHexString(DeferredPollBody);
        var body = new byte[expected.Length];

        TcpContracts.GetMessages(body, Group, Stream, Topic, PollingStrategy.Next(), Count, true, Partition,
            new DeferredPollOptions());

        Assert.Equal(expected, body);
        Assert.Equal(Convert.FromHexString(PollBody), body[..^TcpContracts.DEFERRED_POLL_TAIL_SIZE]);
    }

    [Fact]
    public void DefaultOptions_MatchTheRustDefaultsAndPassValidation()
    {
        var options = new DeferredPollOptions();

        options.Validate(1);

        Assert.Equal(TimeSpan.FromSeconds(1), options.MaxWait);
        Assert.Equal(1u, options.MinCount);
        Assert.Equal(1024u * 1024, options.MaxBytes);
        Assert.Equal(TimeSpan.FromSeconds(11), options.RequestTimeout);
    }

    [Theory]
    [InlineData(1_000_000, 0u, 1_048_576u, 11_000_000, VsrError.INVALID_MESSAGES_COUNT)]
    [InlineData(1_000_000, Count + 1, 1_048_576u, 11_000_000, VsrError.INVALID_MESSAGES_COUNT)]
    [InlineData(-1, 1u, 1_048_576u, 11_000_000, VsrError.INVALID_COMMAND)]
    [InlineData(600_000_001, 1u, 1_048_576u, 630_000_000, VsrError.INVALID_COMMAND)]
    [InlineData(0, 1u, 1_048_576u, 0, VsrError.INVALID_COMMAND)]
    [InlineData(0, 1u, 1_048_576u, 630_000_001, VsrError.INVALID_COMMAND)]
    [InlineData(10_000_000, 1u, 1_048_576u, 10_999_999, VsrError.INVALID_COMMAND)]
    [InlineData(1_000_000, 1u, 343u, 11_000_000, VsrError.INVALID_SIZE_BYTES)]
    public void Validate_RefusesWhatTheServerRefuses(long maxWaitUs, uint minCount, uint maxBytes,
        long requestTimeoutUs, int status)
    {
        var options = new DeferredPollOptions
        {
            MaxWait = TimeSpan.FromMicroseconds(maxWaitUs),
            MinCount = minCount,
            MaxBytes = maxBytes,
            RequestTimeout = TimeSpan.FromMicroseconds(requestTimeoutUs)
        };

        var error = Assert.Throws<IggyInvalidStatusCodeException>(() => options.Validate(Count));

        Assert.Equal(status, error.StatusCode);
    }

    [Fact]
    public void Validate_AcceptsTheLimits()
    {
        new DeferredPollOptions { MinCount = Count, MaxBytes = 344 }.Validate(Count);
        new DeferredPollOptions { MaxWait = TimeSpan.Zero, RequestTimeout = TimeSpan.FromMicroseconds(1) }
            .Validate(Count);
        new DeferredPollOptions { MaxWait = TimeSpan.FromSeconds(600), RequestTimeout = TimeSpan.FromSeconds(630) }
            .Validate(Count);
        new DeferredPollOptions { MaxWait = TimeSpan.FromSeconds(10), RequestTimeout = TimeSpan.FromSeconds(11) }
            .Validate(Count);
    }

    [Fact]
    public void Remaining_ShrinksTheWaitAndTheTimeoutByTheSpentTime()
    {
        var options = new DeferredPollOptions { MaxWait = TimeSpan.FromSeconds(10), MinCount = 5, MaxBytes = 4096 };

        var remaining = options.Remaining(TimeSpan.FromSeconds(4));
        var spentWait = options.Remaining(TimeSpan.FromSeconds(10.5));

        Assert.Equal(options with { MaxWait = TimeSpan.FromSeconds(6), RequestTimeout = TimeSpan.FromSeconds(7) },
            remaining);
        Assert.Equal(TimeSpan.Zero, spentWait.MaxWait);
        Assert.Equal(TimeSpan.FromSeconds(0.5), spentWait.RequestTimeout);
        spentWait.Validate(5);
    }

    /// <summary>
    ///     A stopwatch measures parts of a microsecond, and the wire rounds the wait and the timeout down to whole ones
    ///     each. Spent time in whole microseconds keeps the gap that the options passed with.
    /// </summary>
    [Fact]
    public void Remaining_WithPartsOfAMicrosecond_KeepsTheReplyMargin()
    {
        var options = new DeferredPollOptions
        {
            MaxWait = TimeSpan.FromSeconds(10) + TimeSpan.FromTicks(5),
            RequestTimeout = TimeSpan.FromSeconds(11) + TimeSpan.FromTicks(1)
        };
        options.Validate(Count);

        var remaining = options.Remaining(TimeSpan.FromTicks(3));

        remaining.Validate(Count);
        Assert.Equal(options.RequestTimeoutUs - options.MaxWaitUs, remaining.RequestTimeoutUs - remaining.MaxWaitUs);
    }

    [Fact]
    public void Remaining_WithTheRequestTimeoutSpent_ReportsThatNothingWasSent()
    {
        var error = Assert.Throws<IggyInvalidStatusCodeException>(() =>
            new DeferredPollOptions().Remaining(TimeSpan.FromSeconds(11)));

        Assert.Equal(VsrError.TRANSIENT_NOT_ACCEPTED, error.StatusCode);
    }
}
