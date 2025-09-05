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
using Apache.Iggy.Messages;

namespace Apache.Iggy.JsonConverters;

internal sealed class MessageConverter : JsonConverter<Message>
{
    public override Message Read(ref Utf8JsonReader reader, Type typeToConvert, JsonSerializerOptions options)
    {
        throw new NotImplementedException();
    }

    public override void Write(Utf8JsonWriter writer, Message value, JsonSerializerOptions options)
    {
        writer.WriteStartObject();

        WriteMessageId(writer, value.Header.Id);
        WritePayload(writer, value.Payload);
        WriteHeaders(writer, value.UserHeaders);

        writer.WriteEndObject();
    }

    private static void WriteMessageId(Utf8JsonWriter writer, UInt128 id)
    {
        writer.WritePropertyName("id");
        writer.WriteRawValue(id.ToString());
    }

    private static void WritePayload(Utf8JsonWriter writer, byte[] payload)
    {
        writer.WriteString("payload", Convert.ToBase64String(payload));
    }

    private static void WriteHeaders(Utf8JsonWriter writer, Dictionary<HeaderKey, HeaderValue>? userHeaders)
    {
        if (userHeaders is null)
        {
            writer.WriteNull("headers");
            return;
        }

        writer.WriteStartObject("headers");

        foreach (var (headerKey, headerValue) in userHeaders)
        {
            writer.WriteStartObject(headerKey.Value);
            writer.WriteString("kind", GetHeaderKindString(headerValue.Kind));
            writer.WriteBase64String("value", headerValue.Value);
            writer.WriteEndObject();
        }

        writer.WriteEndObject();
    }

    private static string GetHeaderKindString(HeaderKind kind)
    {
        return kind switch
        {
            HeaderKind.Bool => "bool",
            HeaderKind.Int32 => "int32",
            HeaderKind.Int64 => "int64",
            HeaderKind.Int128 => "int128",
            HeaderKind.Uint32 => "uint32",
            HeaderKind.Uint64 => "uint64",
            HeaderKind.Uint128 => "uint128",
            HeaderKind.Float => "float32",
            HeaderKind.Double => "float64",
            HeaderKind.String => "string",
            HeaderKind.Raw => "raw",
            _ => throw new ArgumentOutOfRangeException(nameof(kind), kind, "Invalid header kind")
        };
    }
}