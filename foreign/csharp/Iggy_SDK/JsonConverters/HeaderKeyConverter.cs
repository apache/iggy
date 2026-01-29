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

internal class HeaderKeyConverter : JsonConverter<HeaderKey>
{
    public override HeaderKey Read(ref Utf8JsonReader reader, Type typeToConvert, JsonSerializerOptions options)
    {
        if (reader.TokenType != JsonTokenType.StartObject)
        {
            throw new JsonException("Expected start of object for HeaderKey.");
        }

        HeaderKind? kind = null;
        byte[]? value = null;

        while (reader.Read())
        {
            if (reader.TokenType == JsonTokenType.EndObject)
            {
                break;
            }

            if (reader.TokenType != JsonTokenType.PropertyName)
            {
                throw new JsonException("Expected property name.");
            }

            var propertyName = reader.GetString();
            reader.Read();

            switch (propertyName)
            {
                case "kind":
                    var kindStr = reader.GetString();
                    kind = kindStr switch
                    {
                        "raw" => HeaderKind.Raw,
                        "string" => HeaderKind.String,
                        "bool" => HeaderKind.Bool,
                        "int8" => HeaderKind.Int8,
                        "int16" => HeaderKind.Int16,
                        "int32" => HeaderKind.Int32,
                        "int64" => HeaderKind.Int64,
                        "int128" => HeaderKind.Int128,
                        "uint8" => HeaderKind.Uint8,
                        "uint16" => HeaderKind.Uint16,
                        "uint32" => HeaderKind.Uint32,
                        "uint64" => HeaderKind.Uint64,
                        "uint128" => HeaderKind.Uint128,
                        "float32" => HeaderKind.Float,
                        "float64" => HeaderKind.Double,
                        _ => throw new JsonException($"Unknown header kind: {kindStr}")
                    };
                    break;
                case "value":
                    var base64 = reader.GetString();
                    value = base64 is not null ? Convert.FromBase64String(base64) : null;
                    break;
            }
        }

        if (kind is null || value is null)
        {
            throw new JsonException("HeaderKey must have both 'kind' and 'value' properties.");
        }

        return new HeaderKey { Kind = kind.Value, Value = value };
    }

    public override void Write(Utf8JsonWriter writer, HeaderKey value, JsonSerializerOptions options)
    {
        writer.WriteStartObject();
        writer.WriteString("kind", value.Kind switch
        {
            HeaderKind.Raw => "raw",
            HeaderKind.String => "string",
            HeaderKind.Bool => "bool",
            HeaderKind.Int8 => "int8",
            HeaderKind.Int16 => "int16",
            HeaderKind.Int32 => "int32",
            HeaderKind.Int64 => "int64",
            HeaderKind.Int128 => "int128",
            HeaderKind.Uint8 => "uint8",
            HeaderKind.Uint16 => "uint16",
            HeaderKind.Uint32 => "uint32",
            HeaderKind.Uint64 => "uint64",
            HeaderKind.Uint128 => "uint128",
            HeaderKind.Float => "float32",
            HeaderKind.Double => "float64",
            _ => throw new JsonException($"Unknown header kind: {value.Kind}")
        });
        writer.WriteString("value", Convert.ToBase64String(value.Value));
        writer.WriteEndObject();
    }

    public override HeaderKey ReadAsPropertyName(ref Utf8JsonReader reader, Type typeToConvert,
        JsonSerializerOptions options)
    {
        var keyStr = reader.GetString() ?? throw new JsonException("Header key cannot be null or empty.");
        return HeaderKey.FromString(keyStr);
    }

    public override void WriteAsPropertyName(Utf8JsonWriter writer, HeaderKey value, JsonSerializerOptions options)
    {
        writer.WritePropertyName(value.ToString());
    }
}
