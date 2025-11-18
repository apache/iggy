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

using System.Diagnostics.CodeAnalysis;
using System.IO.Hashing;
using System.Text.Json.Serialization;
using Apache.Iggy.Extensions;
using Apache.Iggy.Headers;
using Apache.Iggy.JsonConverters;

namespace Apache.Iggy.Messages;

/// <summary>
///     A message which can be sent to the server.
/// </summary>
[JsonConverter(typeof(MessageConverter))]
public class Message
{
    /// <summary>
    ///     Message header.
    /// </summary>
    public required MessageHeader Header { get; init; }

    /// <summary>
    ///     Message payload.
    /// </summary>
    public required byte[] Payload { get; set; }

    /// <summary>
    ///     User defined headers.
    /// </summary>
    public Dictionary<HeaderKey, HeaderValue>? UserHeaders { get; set; }

    /// <summary>
    ///     Default constructor.
    /// </summary>
    public Message()
    {
    }

    /// <summary>
    ///     Initializes a new instance of the <see cref="Message" /> class.
    /// </summary>
    /// <param name="id">Message ID.</param>
    /// <param name="payload">Message payload.</param>
    /// <param name="userHeaders">User defined headers.</param>
    [SetsRequiredMembers]
    public Message(Guid id, byte[] payload, Dictionary<HeaderKey, HeaderValue>? userHeaders = null)
    {
        Header = new MessageHeader
        {
            PayloadLength = payload.Length,
            Id = id.ToUInt128(),
            Checksum = CalculateChecksum(payload)
        };
        Payload = payload;
        UserHeaders = userHeaders;
    }

    /// <summary>
    ///     Initializes a new instance of the <see cref="Message" /> class.
    /// </summary>
    /// <param name="id">Message ID.</param>
    /// <param name="payload">Message payload.</param>
    /// <param name="userHeaders">User defined headers.</param>
    [SetsRequiredMembers]
    public Message(UInt128 id, byte[] payload, Dictionary<HeaderKey, HeaderValue>? userHeaders = null)
    {
        Header = new MessageHeader
        {
            PayloadLength = payload.Length,
            Id = id,
            Checksum = CalculateChecksum(payload)
        };
        Payload = payload;
        UserHeaders = userHeaders;
    }

    /// <summary>
    ///     Returns the size of the message in bytes.
    /// </summary>
    /// <returns></returns>
    public int GetSize()
    {
        //return 56 + Payload.Length + (UserHeaders?.Count ?? 0);
        return 56 + Payload.Length;
    }

    private ulong CalculateChecksum(byte[] bytes)
    {
        return BitConverter.ToUInt64(Crc64.Hash(bytes));
    }
}
