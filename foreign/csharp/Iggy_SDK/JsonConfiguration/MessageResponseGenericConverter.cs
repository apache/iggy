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
using Iggy_SDK.Enums;
using Iggy_SDK.Extensions;
using Iggy_SDK.Headers;
using System.Text.Json;
using System.Text.Json.Serialization;

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
            var offset = element.GetProperty(nameof(MessageResponse.Offset).ToSnakeCase()).GetUInt64();
            var timestamp = element.GetProperty(nameof(MessageResponse.Timestamp).ToSnakeCase()).GetUInt64();
            var id = element.GetProperty(nameof(MessageResponse.Id).ToSnakeCase()).GetUInt128();
            var payload = element.GetProperty(nameof(MessageResponse.Payload).ToSnakeCase()).GetBytesFromBase64();
            var checksum = element.GetProperty(nameof(MessageResponse.Checksum).ToSnakeCase()).GetUInt32();
            var state = element.GetProperty(nameof(MessageResponse.State).ToSnakeCase()).GetString() switch
            {
                "available" => MessageState.Available,
                "unavailable" => MessageState.Unavailable,
                "poisoned" => MessageState.Poisoned,
                "marked_for_deletion" => MessageState.MarkedForDeletion,
                _ => throw new ArgumentOutOfRangeException()
            };
            var headersElement = element.GetProperty(nameof(MessageResponse.Headers).ToSnakeCase());

            var headers = new Dictionary<HeaderKey, HeaderValue>();
            if (headersElement.ValueKind != JsonValueKind.Null)
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
                Id = new Guid(id.GetBytesFromUInt128()),
                Offset = offset,
                Timestamp = timestamp,
                Checksum = checksum,
                Headers = headers.Count > 0 ? headers : null,
                State = state,
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