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
using Apache.Iggy.Headers;

namespace Apache.Iggy.JsonConverters;

/// <summary>
///     Writes raw option entries by name only, so a structured logger never records a value. This build cannot tell
///     which keys newer than it are secret, so every value is hidden. Requests never serialize through here.
/// </summary>
internal sealed class RedactedOptionsConverter : JsonConverter<IReadOnlyDictionary<string, HeaderValue>>
{
    private const string REDACTED = "<redacted>";

    public override IReadOnlyDictionary<string, HeaderValue>? Read(ref Utf8JsonReader reader, Type typeToConvert,
        JsonSerializerOptions options)
    {
        return JsonSerializer.Deserialize<Dictionary<string, HeaderValue>>(ref reader, options);
    }

    public override void Write(Utf8JsonWriter writer, IReadOnlyDictionary<string, HeaderValue> value,
        JsonSerializerOptions options)
    {
        writer.WriteStartObject();
        foreach (var name in value.Keys)
        {
            writer.WriteString(name, REDACTED);
        }

        writer.WriteEndObject();
    }
}
