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
using Apache.Iggy.Enums;

namespace Apache.Iggy.Contracts.Http;

internal sealed class CreateTopicRequest
{
    public required string Name { get; set; }
    public CompressionAlgorithm CompressionAlgorithm { get; set; } = CompressionAlgorithm.None;
    public ulong MessageExpiry { get; set; }
    public uint PartitionsCount { get; set; } = 1;
    public ulong MaxTopicSize { get; set; }

    /// <summary>
    ///     Option keys with no property of their own, as strings the server parses by the same rules a
    ///     config file value goes through. The REST body carries them this way; the binary transports
    ///     send a typed TLV block.
    /// </summary>
    public Dictionary<string, string> Options { get; set; } = new();

    public CreateTopicRequest()
    {
    }

    [SetsRequiredMembers]
    public CreateTopicRequest(string name,
        CompressionAlgorithm compressionAlgorithm,
        ulong messageExpiry,
        uint partitionsCount,
        ulong maxTopicSize,
        Dictionary<string, string>? options = null)
    {
        Name = name;
        CompressionAlgorithm = compressionAlgorithm;
        MessageExpiry = messageExpiry;
        PartitionsCount = partitionsCount;
        MaxTopicSize = maxTopicSize;
        Options = options ?? new Dictionary<string, string>();
    }
}
