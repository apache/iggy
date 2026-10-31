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

using Apache.Iggy.Configuration;
using Apache.Iggy.Contracts;
using Apache.Iggy.Contracts.Tcp;
using Apache.Iggy.Enums;
using Apache.Iggy.Exceptions;
using Apache.Iggy.Vsr;

namespace Apache.Iggy.Tests.ContractsTests;

public sealed class PollOptionsTests
{
    [Fact]
    public void given_no_options_when_defaulted_should_answer_at_once_and_commit_nothing()
    {
        var options = new PollOptions();

        Assert.Equal(new PollOptions
        {
            Count = 1000,
            MaxWait = TimeSpan.Zero,
            MinCount = 1,
            MaxBytes = 1024 * 1024,
            Commit = CommitPolicy.Manual
        }, options);
        Assert.Equal(TimeSpan.FromSeconds(10), options.RequestTimeout);
        options.Validate();
    }

    [Fact]
    public void given_no_auto_commit_settings_when_defaulted_should_store_every_10000_messages_or_5_seconds_after_processing()
    {
        Assert.Equal(new AutoCommit
        {
            Interval = TimeSpan.FromSeconds(5),
            Every = CommitEvery.Messages(10_000),
            After = CommitAfter.Processing
        }, AutoCommit.Default);
        Assert.Equal(CommitMode.Auto, CommitPolicy.Auto().Mode);
        Assert.Equal(AutoCommit.Default, CommitPolicy.Auto().Settings);
    }

    [Fact]
    public void given_auto_commit_without_an_interval_or_an_event_when_validating_should_refuse_it()
    {
        static PollOptions Auto(TimeSpan? interval, CommitEvery? every) => new()
        {
            Commit = CommitPolicy.Auto(new AutoCommit { Interval = interval, Every = every, After = CommitAfter.Delivery })
        };

        var refused = Assert.Throws<IggyInvalidStatusCodeException>(() => Auto(null, null).Validate());

        Assert.Equal(VsrError.INVALID_CONFIGURATION, refused.StatusCode);
        Auto(TimeSpan.FromSeconds(1), null).Validate();
        Auto(null, CommitEvery.Batch).Validate();
    }

    [Fact]
    public void given_auto_commit_with_a_zero_interval_when_validating_should_refuse_it()
    {
        var options = new PollOptions
        {
            Commit = CommitPolicy.Auto(new AutoCommit { Interval = TimeSpan.Zero, Every = CommitEvery.Message })
        };

        var refused = Assert.Throws<IggyInvalidStatusCodeException>(() => options.Validate());

        Assert.Equal(VsrError.INVALID_CONFIGURATION, refused.StatusCode);
    }

    [Fact]
    public void given_a_wait_when_building_options_should_keep_the_other_defaults()
    {
        var options = PollOptions.Wait(TimeSpan.FromSeconds(3));

        Assert.Equal(new PollOptions { MaxWait = TimeSpan.FromSeconds(3) }, options);
        Assert.Equal(TimeSpan.FromSeconds(13), options.RequestTimeout);
    }

    [Theory]
    [InlineData(0u, 1u, 1024u * 1024, 0, VsrError.INVALID_MESSAGES_COUNT)]
    [InlineData(10u, 11u, 1024u * 1024, 0, VsrError.INVALID_MESSAGES_COUNT)]
    [InlineData(10u, 0u, 1024u * 1024, 0, VsrError.INVALID_MESSAGES_COUNT)]
    [InlineData(10u, 1u, 100u, 0, VsrError.INVALID_SIZE_BYTES)]
    [InlineData(10u, 1u, 1024u * 1024, 601, VsrError.INVALID_COMMAND)]
    public void given_invalid_options_when_validating_should_fail_like_the_wire_options(uint count, uint minCount,
        uint maxBytes, int maxWaitSeconds, int status)
    {
        var options = new PollOptions
        {
            Count = count,
            MinCount = minCount,
            MaxBytes = maxBytes,
            MaxWait = TimeSpan.FromSeconds(maxWaitSeconds)
        };

        var refused = Assert.Throws<IggyInvalidStatusCodeException>(() => options.Validate());

        Assert.Equal(status, refused.StatusCode);
    }

    [Fact]
    public void given_the_default_reply_frame_limit_when_a_read_returns_the_largest_message_should_hold_it()
    {
        Assert.Equal(64_156_564, TcpContracts.MAX_RECORD_REPLY_SIZE);
        Assert.True(new IggyClientConfigurator { BaseAddress = "127.0.0.1:8090", Protocol = Protocol.Tcp }.MaxResponseFrameSize
                    >= TcpContracts.MAX_RECORD_REPLY_SIZE);
    }

    [Fact]
    public void given_a_zero_count_of_messages_when_building_the_event_should_refuse_it()
    {
        Assert.Throws<ArgumentOutOfRangeException>(() => CommitEvery.Messages(0));
        Assert.Equal((CommitEveryKind.Messages, 3u), (CommitEvery.Messages(3).Kind, CommitEvery.Messages(3).Count));
        Assert.Equal(CommitEveryKind.Message, CommitEvery.Message.Kind);
        Assert.Equal(CommitEveryKind.Batch, CommitEvery.Batch.Kind);
    }
}
