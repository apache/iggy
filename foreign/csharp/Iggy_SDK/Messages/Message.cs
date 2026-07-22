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
using System.Text.Json.Serialization;
using Apache.Iggy.Extensions;
using Apache.Iggy.Headers;
using Apache.Iggy.JsonConverters;

namespace Apache.Iggy.Messages;

/// <summary>
///     A message which can be sent to the server. A payload backed by caller-owned (e.g. pooled) memory
///     must stay alive until the send copies it to the wire: for a direct send, until the send call
///     completes; for a background/queued send, until <see cref="Publishers.IggyPublisher.WaitUntilAllSendsAsync" />
///     returns.
/// </summary>
[JsonConverter(typeof(MessageConverter))]
public class Message
{
    /// <summary>
    ///     Message header.
    /// </summary>
    public required MessageHeader Header { get; set; }

    /// <summary>
    ///     Message payload.
    /// </summary>
    public required ReadOnlyMemory<byte> Payload { get; set; }

    /// <summary>
    ///     User defined headers.
    /// </summary>
    public Dictionary<HeaderKey, HeaderValue>? UserHeaders { get; set; }

    /// <summary>
    ///     Pre-serialized (possibly encrypted) user headers bytes.
    ///     When non-empty, this takes precedence over <see cref="UserHeaders" /> during serialization.
    /// </summary>
    internal ReadOnlyMemory<byte> RawUserHeaders { get; set; }

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
    public Message(Guid id, ReadOnlyMemory<byte> payload, Dictionary<HeaderKey, HeaderValue>? userHeaders = null)
        : this(id.ToUInt128(), payload, userHeaders)
    {
    }

    /// <summary>
    ///     Initializes a new instance of the <see cref="Message" /> class.
    /// </summary>
    /// <param name="id">Message ID.</param>
    /// <param name="payload">Message payload.</param>
    /// <param name="userHeaders">User defined headers.</param>
    [SetsRequiredMembers]
    public Message(UInt128 id, ReadOnlyMemory<byte> payload, Dictionary<HeaderKey, HeaderValue>? userHeaders = null)
    {
        Header = new MessageHeader
        {
            PayloadLength = payload.Length,
            Id = id
        };
        Payload = payload;
        UserHeaders = userHeaders;
    }
}
