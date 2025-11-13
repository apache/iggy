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

namespace Apache.Iggy.Consumers;

/// <summary>
///     Represents a message received from the Iggy consumer with a deserialized payload of type T
/// </summary>
/// <typeparam name="T">The type of the deserialized message payload</typeparam>
public class ReceivedMessage<T> : ReceivedMessage
{
    /// <summary>
    ///     The deserialized message payload. Will be null if deserialization failed.
    /// </summary>
    public T? Data { get; init; }
}

/// <summary>
///     Represents a message received from the Iggy consumer
/// </summary>
public class ReceivedMessage
{
    /// <summary>
    ///     The underlying message response containing headers, payload, and user headers
    /// </summary>
    public required MessageResponse Message { get; init; }

    /// <summary>
    ///     The current offset of this message in the partition
    /// </summary>
    public required ulong CurrentOffset { get; init; }

    /// <summary>
    ///     The partition ID from which this message was consumed
    /// </summary>
    public uint PartitionId { get; init; }

    /// <summary>
    ///     The status of the message (Success, DecryptionFailed, DeserializationFailed)
    /// </summary>
    public MessageStatus Status { get; init; } = MessageStatus.Success;

    /// <summary>
    ///     The exception that occurred during processing, if any
    /// </summary>
    public Exception? Error { get; init; }
}
