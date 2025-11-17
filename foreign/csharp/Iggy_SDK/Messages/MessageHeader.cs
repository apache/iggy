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
///     Header information for a message.
/// </summary>
public class MessageHeader
{
    /// <summary>
    ///     Message checksum.
    /// </summary>
    public ulong Checksum { get; set; }

    /// <summary>
    ///     Message identifier.
    /// </summary>
    public UInt128 Id { get; set; }

    /// <summary>
    ///     Message offset.
    /// </summary>
    public ulong Offset { get; set; }

    /// <summary>
    ///     Message timestamp.
    /// </summary>
    [JsonConverter(typeof(DateTimeOffsetConverter))]
    public DateTimeOffset Timestamp { get; set; }

    /// <summary>
    ///     Message origin timestamp.
    /// </summary>
    public ulong OriginTimestamp { get; set; }

    /// <summary>
    ///     Length of the user headers.
    /// </summary>
    public int UserHeadersLength { get; set; }

    /// <summary>
    ///     Length of the payload.
    /// </summary>
    public int PayloadLength { get; set; }
}
