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

[JsonConverter(typeof(MessageConverter))]
public readonly struct Message
{
    public required MessageHeader Header { get; init; }
    public required byte[] Payload { get; init; }
    public Dictionary<HeaderKey, HeaderValue>? UserHeaders { get; init; }

    public Message()
    {
    }

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