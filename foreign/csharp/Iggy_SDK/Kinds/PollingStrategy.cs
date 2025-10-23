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

using Apache.Iggy.Enums;

namespace Apache.Iggy.Kinds;

/// <summary>
///     Represents a strategy for polling messages from a stream or topic.
///     Defines the starting point for message consumption.
/// </summary>
public readonly struct PollingStrategy
{
    /// <summary>
    ///     Gets the type of message polling strategy to use.
    /// </summary>
    public required MessagePolling Kind { get; init; }

    /// <summary>
    ///     Gets the value associated with the polling strategy.
    ///     For Offset: the message offset to start from.
    ///     For Timestamp: the Unix timestamp (in microseconds) to start from.
    ///     For First, Last, and Next: this value is 0.
    /// </summary>
    public required ulong Value { get; init; }

    /// <summary>
    ///     Creates a polling strategy that starts from a specific message offset.
    /// </summary>
    /// <param name="value">The message offset to start polling from.</param>
    /// <returns>A <see cref="PollingStrategy" /> configured for offset-based polling.</returns>
    public static PollingStrategy Offset(ulong value)
    {
        return new PollingStrategy
        {
            Kind = MessagePolling.Offset,
            Value = value
        };
    }

    /// <summary>
    ///     Creates a polling strategy that starts from a specific timestamp.
    /// </summary>
    /// <param name="value">The Unix timestamp (in microseconds) to start polling from.</param>
    /// <returns>A <see cref="PollingStrategy" /> configured for timestamp-based polling.</returns>
    public static PollingStrategy Timestamp(ulong value)
    {
        return new PollingStrategy
        {
            Kind = MessagePolling.Timestamp,
            Value = value
        };
    }

    /// <summary>
    ///     Creates a polling strategy that starts from the first available message.
    /// </summary>
    /// <returns>A <see cref="PollingStrategy" /> configured to poll from the first message.</returns>
    public static PollingStrategy First()
    {
        return new PollingStrategy
        {
            Kind = MessagePolling.First,
            Value = 0
        };
    }

    /// <summary>
    ///     Creates a polling strategy that starts from the last available message.
    /// </summary>
    /// <returns>A <see cref="PollingStrategy" /> configured to poll from the last message.</returns>
    public static PollingStrategy Last()
    {
        return new PollingStrategy
        {
            Kind = MessagePolling.Last,
            Value = 0
        };
    }

    /// <summary>
    ///     Creates a polling strategy that starts from the next available message after the current consumer offset.
    /// </summary>
    /// <returns>A <see cref="PollingStrategy" /> configured to poll from the next message.</returns>
    public static PollingStrategy Next()
    {
        return new PollingStrategy
        {
            Kind = MessagePolling.Next,
            Value = 0
        };
    }
}
