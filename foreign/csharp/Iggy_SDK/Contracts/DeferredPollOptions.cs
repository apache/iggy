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
using Apache.Iggy.Messages;
using Apache.Iggy.Vsr;

namespace Apache.Iggy.Contracts;

/// <summary>
///     Readiness and reply limits of a deferred (long) poll, independent of the maximum message count. The server
///     replies once <see cref="MinCount" /> messages are readable, the reply reaches <see cref="MaxBytes" />, or
///     <see cref="MaxWait" /> passes. It then returns what it has, which can be fewer messages or none.
///     <see cref="RequestTimeout" /> bounds the whole call: routing, admission, the wait, the read and the reply.
/// </summary>
/// <remarks>
///     <see cref="MaxWait" /> is at most 600 seconds, and the server can shorten it to its own maximum.
///     <see cref="RequestTimeout" /> is 1 microsecond to 630 seconds, and a call that waits must leave one second
///     between its wait and its timeout for the final read and the reply. <see cref="MaxBytes" /> covers the binary
///     reply, its metadata and batch framing included, so it is at least 344 bytes: the reply header, one batch
///     header and one message header. A record wider than <see cref="MaxBytes" /> fails with status 80
///     (InvalidSizeBytes) and is not read.
/// </remarks>
internal sealed record DeferredPollOptions
{
    internal const ulong REPLY_MARGIN_US = 1_000_000;

    /// <summary>
    ///     Deferred polls and fetches one client keeps parked. The server holds at most as many per session
    ///     and shard, so a call over the limit waits in the client for a free slot instead of being refused.
    /// </summary>
    internal const int MAX_PARKED_REQUESTS = 64;

    private const ulong MaxWaitLimitUs = 600_000_000;
    private const ulong RequestTimeoutHeadroomUs = 30_000_000;
    private const ulong MaxRequestTimeoutUs = MaxWaitLimitUs + RequestTimeoutHeadroomUs;

    /// <summary>The longest <see cref="RequestTimeout" /> a deferred poll accepts.</summary>
    internal static readonly TimeSpan MaxRequestTimeout = FromMicros(MaxRequestTimeoutUs);

    /// <summary>The header of a poll reply: the partition, its current offset, the count and the context.</summary>
    internal const int POLL_REPLY_HEADER_SIZE =
        sizeof(uint) + sizeof(ulong) + sizeof(uint) + PartitionContext.ENCODED_SIZE;

    /// <summary>The smallest byte limit: a reply header, one batch header and one message header.</summary>
    internal const uint MIN_MAX_BYTES =
        POLL_REPLY_HEADER_SIZE + BatchWireFormat.BATCH_HEADER_SIZE + BatchWireFormat.FRAME_HEADER_SIZE;

    /// <summary>How long the server waits for <see cref="MinCount" /> messages. One second by default.</summary>
    public TimeSpan MaxWait { get; init; } = TimeSpan.FromSeconds(1);

    /// <summary>How many messages make the server reply before <see cref="MaxWait" />. One by default.</summary>
    public uint MinCount { get; init; } = 1;

    /// <summary>The largest binary reply, in bytes. 1 MiB by default.</summary>
    public uint MaxBytes { get; init; } = 1024 * 1024;

    /// <summary>The budget of the whole call. Eleven seconds by default.</summary>
    public TimeSpan RequestTimeout { get; init; } = TimeSpan.FromSeconds(11);

    internal ulong MaxWaitUs => Micros(MaxWait);

    internal ulong RequestTimeoutUs => Micros(RequestTimeout);

    /// <summary>Applies the rules the server applies to a deferred poll of <paramref name="count" /> messages.</summary>
    /// <exception cref="IggyInvalidStatusCodeException">
    ///     Status 4009 for a minimum count outside 1 to <paramref name="count" />, 3 for a wait or request timeout
    ///     out of range, 80 for a byte limit below one message.
    /// </exception>
    internal void Validate(uint count)
    {
        if (MinCount == 0 || MinCount > count)
        {
            throw VsrError.Exception(VsrError.INVALID_MESSAGES_COUNT,
                $"The minimum count must be 1 to the poll count {count}, got {MinCount}.");
        }

        ValidateBudget();
        if (MaxBytes < MIN_MAX_BYTES)
        {
            throw VsrError.Exception(VsrError.INVALID_SIZE_BYTES,
                $"The byte limit must be at least {MIN_MAX_BYTES}, got {MaxBytes}.");
        }
    }

    /// <summary>
    ///     Applies the rules of <see cref="Validate" /> and the one rule a fetch (code 109) adds: its request timeout
    ///     exceeds its wait by the reply margin also without a wait, with time left for client-side setup.
    /// </summary>
    /// <exception cref="IggyInvalidStatusCodeException">The status of <see cref="Validate" />, or 3.</exception>
    internal void ValidateFetch(uint count)
    {
        Validate(count);
        if (RequestTimeoutUs <= REPLY_MARGIN_US || RequestTimeoutUs < MaxWaitUs + REPLY_MARGIN_US)
        {
            throw Refused($"A fetch request timeout must exceed {REPLY_MARGIN_US} microseconds and cover its wait " +
                          "plus that reply margin.");
        }
    }

    /// <summary>The shared rules of the wait and the request timeout.</summary>
    /// <exception cref="IggyInvalidStatusCodeException">Status 3 naming the broken rule.</exception>
    internal void ValidateBudget()
    {
        // Compared as TimeSpan first: a negative span has no microsecond value on the wire.
        if (MaxWait < TimeSpan.Zero || MaxWait > FromMicros(MaxWaitLimitUs))
        {
            throw Refused($"The wait must be 0 to {MaxWaitLimitUs} microseconds.");
        }

        if (RequestTimeoutUs == 0 || RequestTimeout > FromMicros(MaxRequestTimeoutUs))
        {
            throw Refused($"The request timeout must be 1 to {MaxRequestTimeoutUs} microseconds.");
        }

        if (MaxWaitUs > 0 && RequestTimeoutUs < MaxWaitUs + REPLY_MARGIN_US)
        {
            throw Refused($"A request timeout must exceed its wait by {REPLY_MARGIN_US} microseconds.");
        }
    }

    /// <summary>
    ///     The options of a call that already spent <paramref name="elapsed" />. The wait and the request timeout
    ///     shrink by the same whole microseconds, so a wait keeps its reply margin on the wire.
    /// </summary>
    /// <remarks>
    ///     Callers measure <paramref name="elapsed" /> with a <see cref="System.Diagnostics.Stopwatch" />. The tick
    ///     count of <see cref="Environment.TickCount64" /> moves in steps of 1 to 16 ms, so it can count time that did
    ///     not pass and end an idle wait before its max wait.
    /// </remarks>
    /// <exception cref="IggyInvalidStatusCodeException">
    ///     Status 58 once the request timeout is spent: the request never left the client.
    /// </exception>
    internal DeferredPollOptions Remaining(TimeSpan elapsed)
    {
        elapsed = FromMicros(Micros(elapsed));
        var requestTimeout = RequestTimeout - elapsed;
        if (Micros(requestTimeout) == 0)
        {
            throw VsrError.Exception(VsrError.TRANSIENT_NOT_ACCEPTED,
                "The deferred call spent its request timeout before it was sent.");
        }

        return this with
        {
            MaxWait = MaxWait > elapsed ? MaxWait - elapsed : TimeSpan.Zero,
            RequestTimeout = requestTimeout
        };
    }

    internal static ulong Micros(TimeSpan span)
    {
        return span <= TimeSpan.Zero ? 0 : (ulong)(span.Ticks / TimeSpan.TicksPerMicrosecond);
    }

    private static TimeSpan FromMicros(ulong micros)
    {
        return TimeSpan.FromTicks((long)micros * TimeSpan.TicksPerMicrosecond);
    }

    private static IggyInvalidStatusCodeException Refused(string message)
    {
        return VsrError.Exception(VsrError.INVALID_COMMAND, $"Deferred poll options refused: {message}");
    }
}
