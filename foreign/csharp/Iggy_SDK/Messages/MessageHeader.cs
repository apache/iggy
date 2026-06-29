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

using System.Text.Json.Serialization;
using Apache.Iggy.JsonConverters;

namespace Apache.Iggy.Messages;

/// <summary>
///     Header information for a message. An immutable value type, so a header cannot be aliased and
///     mutated across a re-send or a failure snapshot.
/// </summary>
public readonly record struct MessageHeader
{
    /// <summary>
    ///     Message checksum. Computed by the server on append and populated on poll; ignored on publish.
    ///     With a message encryptor configured, the server computes it over the stored ciphertext, so it
    ///     does not match the decrypted payload the client exposes.
    /// </summary>
    public ulong Checksum { get; init; }

    /// <summary>
    ///     Message identifier.
    /// </summary>
    public UInt128 Id { get; init; }

    /// <summary>
    ///     Message offset.
    /// </summary>
    public ulong Offset { get; init; }

    /// <summary>
    ///     Message timestamp.
    /// </summary>
    [JsonConverter(typeof(DateTimeOffsetConverter))]
    public DateTimeOffset Timestamp { get; init; }

    /// <summary>
    ///     Message origin timestamp.
    /// </summary>
    public ulong OriginTimestamp { get; init; }

    /// <summary>
    ///     Length of the user headers.
    /// </summary>
    public int UserHeadersLength { get; init; }

    /// <summary>
    ///     Length of the payload.
    /// </summary>
    public int PayloadLength { get; init; }

    /// <summary>
    ///     Reserved for future use.
    /// </summary>
    public ulong Reserved { get; init; }
}
