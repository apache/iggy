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

using System.Text.Json;
using System.Text.Json.Serialization;

namespace Apache.Iggy.Shared;

public sealed class Envelope
{
    private readonly JsonSerializerOptions _jsonSerializerOptions = new();

    [JsonPropertyName("message_type")]
    public string MessageType { get; set; } = "";

    [JsonPropertyName("payload")]
    public string Payload { get; set; } = "";

    public Envelope()
    {
        _jsonSerializerOptions.PropertyNamingPolicy = JsonNamingPolicy.SnakeCaseLower;
        _jsonSerializerOptions.WriteIndented = true;
    }

    public Envelope New<T>(string messageType, T payload) where T : ISerializableMessage
    {
        return new Envelope
        {
            MessageType = messageType,
            Payload = JsonSerializer.Serialize(payload, _jsonSerializerOptions)
        };
    }
}