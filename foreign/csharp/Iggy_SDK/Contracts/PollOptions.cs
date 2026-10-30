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

using Apache.Iggy.Exceptions;
using Apache.Iggy.Kinds;
using Apache.Iggy.Vsr;

namespace Apache.Iggy.Contracts;

/// <summary>
///     The limits of one read of one partition and who stores its offsets.
/// </summary>
/// <remarks>
///     A read waits until <see cref="MinCount" /> messages are readable or <see cref="MaxWait" /> passes, and returns
///     up to <see cref="Count" /> messages and <see cref="MaxBytes" /> bytes. A first message larger than
///     <see cref="MaxBytes" /> comes alone in its read. A <see cref="Kinds.PollingStrategy.Last" /> read of more than
///     one message that starts with such a message fails with status 80 (InvalidSizeBytes) instead, and stores
///     nothing.
/// </remarks>
public sealed record PollOptions
{
    private static readonly TimeSpan RequestTimeoutMargin = TimeSpan.FromSeconds(10);

    /// <summary>Most messages of one batch. 1000 by default.</summary>
    public uint Count { get; init; } = 1000;

    /// <summary>
    ///     How long the server waits for <see cref="MinCount" /> messages. Zero answers at once and is the default of
    ///     a poll. At most 600 seconds.
    /// </summary>
    public TimeSpan MaxWait { get; init; } = TimeSpan.Zero;

    /// <summary>How many messages make the server answer before <see cref="MaxWait" />. 1 to <see cref="Count" />, 1 by default.</summary>
    public uint MinCount { get; init; } = 1;

    /// <summary>The largest reply, in bytes. 1 MiB by default.</summary>
    public uint MaxBytes { get; init; } = 1024 * 1024;

    /// <summary>Who stores the offsets. <see cref="CommitPolicy.Manual" /> by default.</summary>
    public CommitPolicy Commit { get; init; } = CommitPolicy.Manual;

    /// <summary>The limit of the whole call: <see cref="MaxWait" /> plus 10 seconds.</summary>
    public TimeSpan RequestTimeout => MaxWait + RequestTimeoutMargin;

    /// <summary>The wire limits of a read with these options.</summary>
    internal DeferredPollOptions Wire => new()
    {
        MaxWait = MaxWait,
        MinCount = MinCount,
        MaxBytes = MaxBytes,
        RequestTimeout = RequestTimeout
    };

    /// <summary>The defaults with a wait of up to <paramref name="maxWait" /> for one message.</summary>
    public static PollOptions Wait(TimeSpan maxWait)
    {
        return new PollOptions { MaxWait = maxWait };
    }

    /// <summary>Applies the rules of the server and of the commit policy, so refused options are never sent.</summary>
    /// <exception cref="IggyInvalidStatusCodeException">
    ///     Status 4009 for a zero <see cref="Count" /> or a <see cref="MinCount" /> outside 1 to <see cref="Count" />, 3
    ///     for a <see cref="MaxWait" /> above 600 seconds, 80 for a <see cref="MaxBytes" /> below one message, and 2
    ///     (InvalidConfiguration) for <see cref="CommitPolicy.Auto" /> without an interval or an event, which would
    ///     store only on close, or with an interval that is not positive.
    /// </exception>
    internal void Validate()
    {
        ArgumentNullException.ThrowIfNull(Commit);
        if (Commit.Settings is { } auto && (auto.Interval is null && auto.Every is null
                                            || auto.Interval <= TimeSpan.Zero))
        {
            throw VsrError.Exception(VsrError.INVALID_CONFIGURATION,
                "An automatic commit needs a positive interval or an event.");
        }

        Wire.Validate(Count);
    }

    /// <summary>Applies <see cref="Validate" /> and the rules of a one-shot poll of <paramref name="partitionId" />.</summary>
    /// <exception cref="IggyInvalidStatusCodeException">
    ///     The status of <see cref="Validate" />, 2 for <see cref="CommitPolicy.Auto" />, which a one-shot poll has
    ///     nothing to count for, and 3 for a continuation of another partition.
    /// </exception>
    internal void ValidatePoll(uint partitionId, PollingStrategy strategy)
    {
        Validate();
        if (Commit.Mode == CommitMode.Auto)
        {
            throw VsrError.Exception(VsrError.INVALID_CONFIGURATION,
                "A one-shot poll has nothing to count, so it cannot commit automatically. Use a partition reader.");
        }

        strategy.ValidatePartition(partitionId);
    }
}
