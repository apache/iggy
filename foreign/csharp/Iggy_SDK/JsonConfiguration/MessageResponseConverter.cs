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

using Iggy_SDK.Contracts.Http;
using Iggy_SDK.Extensions;
using Iggy_SDK.Headers;
using System.Text.Json;
using System.Text.Json.Serialization;
using Iggy_SDK.Messages;

namespace Iggy_SDK.JsonConfiguration;

internal sealed class MessageResponseConverter : JsonConverter<PolledMessages>
{
    private readonly Func<byte[], byte[]>? _decryptor;
    public MessageResponseConverter(Func<byte[], byte[]>? decryptor)
    {
        _decryptor = decryptor;
    }

    public override PolledMessages Read(ref Utf8JsonReader reader, Type typeToConvert,
        JsonSerializerOptions options)
    {
        using var doc = JsonDocument.ParseValue(ref reader);

        var root = doc.RootElement;

        var partitionId = root.GetProperty(nameof(PolledMessages.PartitionId).ToSnakeCase()).GetInt32();
        var currentOffset = root.GetProperty(nameof(PolledMessages.CurrentOffset).ToSnakeCase()).GetUInt64();
        var messages = root.GetProperty(nameof(PolledMessages.Messages).ToSnakeCase());
        //var messagesCount = BinaryPrimitives.ReadUInt32LittleEndian(payload[12..16]);

        var messageResponses = new List<MessageResponse>();
        foreach (var element in messages.EnumerateArray())
        {
            var headerElement = element.GetProperty(nameof(Message.Header).ToSnakeCase());
            
            var checksum = headerElement.GetProperty(nameof(MessageHeader.Checksum).ToSnakeCase()).GetUInt64();
            var id = headerElement.GetProperty(nameof(MessageHeader.Id).ToSnakeCase()).GetUInt128();
            var offset = headerElement.GetProperty(nameof(MessageHeader.Offset).ToSnakeCase()).GetUInt64();
            var timestamp = headerElement.GetProperty(nameof(MessageHeader.Timestamp).ToSnakeCase()).GetUInt64();
            var originTimestamp = headerElement.GetProperty(nameof(MessageHeader.OriginTimestamp).ToSnakeCase()).GetUInt64();
            var headerLength = headerElement.GetProperty(nameof(MessageHeader.UserHeadersLength).ToSnakeCase()).GetInt32();
            var payloadLength = headerElement.GetProperty(nameof(MessageHeader.PayloadLength).ToSnakeCase()).GetInt32();
            
            element.TryGetProperty(nameof(MessageResponse.UserHeaders).ToSnakeCase(), out var headersElement);

            var payload = element.GetProperty(nameof(MessageResponse.Payload).ToSnakeCase()).GetBytesFromBase64();
            var headers = new Dictionary<HeaderKey, HeaderValue>();
            if (headersElement.ValueKind != JsonValueKind.Null && headersElement.ValueKind != JsonValueKind.Undefined)
            {
                var headersJsonArray = headersElement.EnumerateObject();
                foreach (var header in headersJsonArray)
                {
                    var headerKey = header.Name;
                    var headerKind = header.Value.GetProperty(nameof(HeaderValue.Kind).ToSnakeCase()).GetString();
                    var headerValue = header.Value.GetProperty(nameof(HeaderValue.Value).ToSnakeCase()).GetBytesFromBase64();
                    headers.Add(HeaderKey.New(headerKey), new HeaderValue
                    {
                        Kind = headerKind switch
                        {
                            "bool" => HeaderKind.Bool,
                            "int32" => HeaderKind.Int32,
                            "int64" => HeaderKind.Int64,
                            "int128" => HeaderKind.Int128,
                            "uint32" => HeaderKind.Uint32,
                            "uint64" => HeaderKind.Uint64,
                            "uint128" => HeaderKind.Uint128,
                            "float32" => HeaderKind.Float,
                            "float64" => HeaderKind.Double,
                            "string" => HeaderKind.String,
                            "raw" => HeaderKind.Raw,
                            /*
                            "raw" => Ok(HeaderKind::Raw),
                            "string" => Ok(HeaderKind::String),
                            "bool" => Ok(HeaderKind::Bool),
                            "int8" => Ok(HeaderKind::Int8),
                            "int16" => Ok(HeaderKind::Int16),
                            "int32" => Ok(HeaderKind::Int32),
                            "int64" => Ok(HeaderKind::Int64),
                            "int128" => Ok(HeaderKind::Int128),
                            "uint8" => Ok(HeaderKind::Uint8),
                            "uint16" => Ok(HeaderKind::Uint16),
                            "uint32" => Ok(HeaderKind::Uint32),
                            "uint64" => Ok(HeaderKind::Uint64),
                            "uint128" => Ok(HeaderKind::Uint128),
                            "float32" => Ok(HeaderKind::Float32),
                            "float64" => Ok(HeaderKind::Float64),
                            */
                            _ => throw new ArgumentOutOfRangeException()
                        },
                        Value = headerValue
                    });
                }
            }

            messageResponses.Add(new MessageResponse
            {
                Header = new MessageHeader()
                {
                    UserHeadersLength = headerLength,
                    PayloadLength = payloadLength,
                    Checksum = checksum,
                    Id = id,
                    Offset = offset,
                    Timestamp = DateTimeOffsetUtils.FromUnixTimeMicroSeconds(timestamp),
                    OriginTimestamp = originTimestamp
                },
                UserHeaders = headers.Any() ? headers : null,
                Payload = _decryptor is not null ? _decryptor(payload) : payload
            });
        }

        return new PolledMessages
        {
            Messages = messageResponses.AsReadOnly(),
            CurrentOffset = currentOffset,
            PartitionId = partitionId
        };
    }


    public override void Write(Utf8JsonWriter writer, PolledMessages value, JsonSerializerOptions options)
    {
        throw new NotImplementedException();
    }
}