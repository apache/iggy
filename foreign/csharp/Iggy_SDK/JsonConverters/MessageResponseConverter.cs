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
using Apache.Iggy.Contracts;
using Apache.Iggy.Headers;
using Apache.Iggy.Messages;

namespace Apache.Iggy.JsonConverters;

internal sealed class MessageResponseConverter : JsonConverter<MessageResponse>
{
    private static readonly UserHeadersConverter HeadersConverter = new();

    public override MessageResponse Read(ref Utf8JsonReader reader, Type typeToConvert, JsonSerializerOptions options)
    {
        if (reader.TokenType != JsonTokenType.StartObject)
        {
            throw new JsonException("Expected start of object for MessageResponse.");
        }

        MessageHeader? header = null;
        byte[]? payload = null;
        Dictionary<HeaderKey, HeaderValue>? userHeaders = null;
        byte[]? rawUserHeaders = null;

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
                case "header":
                    header = JsonSerializer.Deserialize<MessageHeader>(ref reader, options);
                    break;
                case "payload":
                    payload = reader.GetBytesFromBase64();
                    break;
                case "user_headers":
                    if (reader.TokenType == JsonTokenType.String)
                    {
                        rawUserHeaders = reader.GetBytesFromBase64();
                    }
                    else if (reader.TokenType == JsonTokenType.Null)
                    {
                        userHeaders = null;
                    }
                    else
                    {
                        userHeaders = HeadersConverter.Read(ref reader, typeof(Dictionary<HeaderKey, HeaderValue>), options);
                    }
                    break;
                default:
                    reader.Skip();
                    break;
            }
        }

        return new MessageResponse
        {
            Header = header ?? throw new JsonException("Missing 'header' field."),
            Payload = payload ?? throw new JsonException("Missing 'payload' field."),
            UserHeaders = userHeaders,
            RawUserHeaders = rawUserHeaders
        };
    }

    public override void Write(Utf8JsonWriter writer, MessageResponse value, JsonSerializerOptions options)
    {
        JsonSerializer.Serialize(writer, value, options);
    }
}
