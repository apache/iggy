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

internal class UserHeadersConverter : JsonConverter<Dictionary<HeaderKey, HeaderValue>?>
{
    public override Dictionary<HeaderKey, HeaderValue>? Read(ref Utf8JsonReader reader, Type typeToConvert, JsonSerializerOptions options)
    {
        if (reader.TokenType == JsonTokenType.Null)
        {
            return null;
        }

        if (reader.TokenType != JsonTokenType.StartArray)
        {
            throw new JsonException($"Expected start of array for headers but got {reader.TokenType}.");
        }

        var result = new Dictionary<HeaderKey, HeaderValue>();

        while (reader.Read())
        {
            if (reader.TokenType == JsonTokenType.EndArray)
            {
                return result.Count == 0 ? null : result;
            }

            if (reader.TokenType != JsonTokenType.StartObject)
            {
                throw new JsonException("Expected start of object for header entry.");
            }

            HeaderKey? key = null;
            HeaderValue? value = null;

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
                    case "key":
                        key = ReadHeaderKey(ref reader);
                        break;
                    case "value":
                        value = ReadHeaderValue(ref reader);
                        break;
                }
            }

            if (key is null || value is null)
            {
                throw new JsonException("Header entry must have both 'key' and 'value' properties.");
            }

            result[key.Value] = value.Value;
        }

        return result;
    }

    private static HeaderKey ReadHeaderKey(ref Utf8JsonReader reader)
    {
        if (reader.TokenType != JsonTokenType.StartObject)
        {
            throw new JsonException("Expected start of object for header key.");
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
                    kind = ParseHeaderKind(reader.GetString());
                    break;
                case "value":
                    var base64 = reader.GetString();
                    value = base64 is not null ? Convert.FromBase64String(base64) : null;
                    break;
            }
        }

        if (kind is null || value is null)
        {
            throw new JsonException("Header key must have both 'kind' and 'value' properties.");
        }

        return new HeaderKey { Kind = kind.Value, Value = value };
    }

    private static HeaderValue ReadHeaderValue(ref Utf8JsonReader reader)
    {
        if (reader.TokenType != JsonTokenType.StartObject)
        {
            throw new JsonException("Expected start of object for header value.");
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
                    kind = ParseHeaderKind(reader.GetString());
                    break;
                case "value":
                    var base64 = reader.GetString();
                    value = base64 is not null ? Convert.FromBase64String(base64) : null;
                    break;
            }
        }

        if (kind is null || value is null)
        {
            throw new JsonException("Header value must have both 'kind' and 'value' properties.");
        }

        return new HeaderValue { Kind = kind.Value, Value = value };
    }

    private static HeaderKind ParseHeaderKind(string? kindStr)
    {
        return kindStr switch
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
    }

    public override void Write(Utf8JsonWriter writer, Dictionary<HeaderKey, HeaderValue>? value, JsonSerializerOptions options)
    {
        if (value is null)
        {
            writer.WriteNullValue();
            return;
        }

        writer.WriteStartArray();

        foreach (var kvp in value)
        {
            writer.WriteStartObject();

            writer.WriteStartObject("key");
            writer.WriteString("kind", GetHeaderKindString(kvp.Key.Kind));
            writer.WriteBase64String("value", kvp.Key.Value);
            writer.WriteEndObject();

            writer.WriteStartObject("value");
            writer.WriteString("kind", GetHeaderKindString(kvp.Value.Kind));
            writer.WriteBase64String("value", kvp.Value.Value);
            writer.WriteEndObject();

            writer.WriteEndObject();
        }

        writer.WriteEndArray();
    }

    private static string GetHeaderKindString(HeaderKind kind)
    {
        return kind switch
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
            _ => throw new ArgumentOutOfRangeException(nameof(kind), kind, "Invalid header kind")
        };
    }
}
