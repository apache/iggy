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

internal sealed class MessageResponseGenericConverter<TMessage> : JsonConverter<PolledMessages<TMessage>>
{
    private readonly Func<byte[], TMessage> _serializer;
    private readonly Func<byte[], byte[]>? _decryptor;

    public MessageResponseGenericConverter(Func<byte[], TMessage> serializer, Func<byte[], byte[]>? decryptor)
    {
        _serializer = serializer;
        _decryptor = decryptor;
    }
    public override PolledMessages<TMessage>? Read(ref Utf8JsonReader reader, Type typeToConvert, JsonSerializerOptions options)
    {
        using var doc = JsonDocument.ParseValue(ref reader);

        var root = doc.RootElement;

        var partitionId = root.GetProperty(nameof(PolledMessages.PartitionId).ToSnakeCase()).GetInt32();
        var currentOffset = root.GetProperty(nameof(PolledMessages.CurrentOffset).ToSnakeCase()).GetUInt64();
        var messages = root.GetProperty(nameof(PolledMessages.Messages).ToSnakeCase());
        //var messagesCount = BinaryPrimitives.ReadUInt32LittleEndian(payload[12..16]);

        var messageResponses = new List<MessageResponse<TMessage>>();
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
                    //TODO - look into getting rid of this boxing 
                    var headerKey = header.Name;
                    var headerObj = header.Value.EnumerateObject();
                    var headerKind = headerObj.First().Value.GetString();
                    var headerValue = headerObj.Last().Value.GetBytesFromBase64();
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
                            _ => throw new ArgumentOutOfRangeException()
                        },
                        Value = headerValue
                    });
                }
            }
            messageResponses.Add(new MessageResponse<TMessage>
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
                Message = _decryptor is not null ? _serializer(_decryptor(payload)) : _serializer(payload)
            });
        }

        return new PolledMessages<TMessage>
        {
            Messages = messageResponses.AsReadOnly(),
            CurrentOffset = currentOffset,
            PartitionId = partitionId
        };
    }

    public override void Write(Utf8JsonWriter writer, PolledMessages<TMessage> value, JsonSerializerOptions options)
    {
        throw new NotImplementedException();
    }
}