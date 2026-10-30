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

namespace Apache.Iggy.Contracts;

/// <summary>Who stores the offsets of the messages that a read returns.</summary>
public enum CommitMode
{
    /// <summary>Nothing is stored. The application commits checkpoints.</summary>
    Manual,

    /// <summary>
    ///     The server stores the offset of the last message of each read as part of the read: one round trip, at most
    ///     once.
    /// </summary>
    OnRead,

    /// <summary>The client stores the latest counted position, at least once with <see cref="CommitAfter.Processing" />.</summary>
    Auto
}

/// <summary>
///     When the offsets of the messages that a read returns are stored. A one-shot poll accepts
///     <see cref="Manual" /> and <see cref="OnRead" />. A partition reader and a consumer accept every policy.
/// </summary>
public sealed record CommitPolicy
{
    private CommitPolicy(CommitMode mode, AutoCommit? settings = null)
    {
        Mode = mode;
        Settings = settings;
    }

    /// <summary>Nothing is stored. The application commits checkpoints. The default.</summary>
    public static CommitPolicy Manual { get; } = new(CommitMode.Manual);

    /// <summary>The server stores the offset of the last message of each read as part of the read.</summary>
    public static CommitPolicy OnRead { get; } = new(CommitMode.OnRead);

    /// <summary>The mode of the policy.</summary>
    public CommitMode Mode { get; }

    /// <summary>When the client stores, set only for <see cref="CommitMode.Auto" />.</summary>
    public AutoCommit? Settings { get; }

    /// <summary>
    ///     The client stores the latest counted position as <paramref name="settings" /> say, by default every 10,000
    ///     messages or 5 seconds after processing. It also stores on close, on shutdown and before a rebalance takes a
    ///     partition away.
    /// </summary>
    public static CommitPolicy Auto(AutoCommit? settings = null)
    {
        return new CommitPolicy(CommitMode.Auto, settings ?? AutoCommit.Default);
    }
}

/// <summary>
///     When <see cref="CommitPolicy.Auto" /> stores. It needs <see cref="Interval" /> or <see cref="Every" />. A
///     store only moves forward, and a position of an older generation of its partition, after a seek or a new
///     assignment, is dropped.
/// </summary>
public sealed record AutoCommit
{
    /// <summary>Every 10,000 messages or 5 seconds, whichever comes first, after <see cref="CommitAfter.Processing" />.</summary>
    public static AutoCommit Default { get; } = new()
    {
        Interval = TimeSpan.FromSeconds(5),
        Every = CommitEvery.Messages(10_000)
    };

    /// <summary>Store the latest counted position at this interval, when it moved.</summary>
    public TimeSpan? Interval { get; init; }

    /// <summary>Also store it on this event.</summary>
    public CommitEvery? Every { get; init; }

    /// <summary>When a message counts.</summary>
    public CommitAfter After { get; init; } = CommitAfter.Processing;
}

/// <summary>The kind of a <see cref="CommitEvery" /> event.</summary>
public enum CommitEveryKind
{
    /// <summary>Each counted message, or each counted batch of a partition reader.</summary>
    Message,

    /// <summary>A number of counted messages of a partition since its last store.</summary>
    Messages,

    /// <summary>The last message of each batch.</summary>
    Batch
}

/// <summary>The event of <see cref="AutoCommit.Every" />.</summary>
public sealed record CommitEvery
{
    private CommitEvery(CommitEveryKind kind, uint count)
    {
        Kind = kind;
        Count = count;
    }

    /// <summary>Each counted message, or each counted batch of a partition reader.</summary>
    public static CommitEvery Message { get; } = new(CommitEveryKind.Message, 1);

    /// <summary>The last message of each batch.</summary>
    public static CommitEvery Batch { get; } = new(CommitEveryKind.Batch, 0);

    /// <summary>The kind of the event.</summary>
    public CommitEveryKind Kind { get; }

    /// <summary>The counted messages of a partition between stores, for <see cref="CommitEveryKind.Messages" />.</summary>
    public uint Count { get; }

    /// <summary>This many counted messages of a partition since its last store.</summary>
    /// <exception cref="ArgumentOutOfRangeException"><paramref name="count" /> is zero.</exception>
    public static CommitEvery Messages(uint count)
    {
        ArgumentOutOfRangeException.ThrowIfZero(count);
        return new CommitEvery(CommitEveryKind.Messages, count);
    }
}

/// <summary>When a message counts for <see cref="CommitPolicy.Auto" />.</summary>
public enum CommitAfter
{
    /// <summary>
    ///     Once the application asks for the next message or batch, returns from its handler with success, or closes
    ///     the reader. A handler that fails leaves its message uncounted.
    /// </summary>
    Processing,

    /// <summary>When the message is handed over.</summary>
    Delivery,

    /// <summary>
    ///     Only when the application marks its checkpoint, so messages processed concurrently count once they are
    ///     done.
    /// </summary>
    Marked
}
