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

using System.ComponentModel;
using System.Text.Json;
using System.Text.Json.Serialization;
using Apache.Iggy.Contracts.Http;
using Apache.Iggy.Enums;
using Apache.Iggy.Extensions;

namespace Apache.Iggy.JsonConfiguration;

internal sealed class MessagesConverter : JsonConverter<MessageSendRequest>
{
    public override MessageSendRequest? Read(ref Utf8JsonReader reader, Type typeToConvert, JsonSerializerOptions options)
    {
        throw new NotImplementedException();
    }

    public override void Write(Utf8JsonWriter writer, MessageSendRequest value, JsonSerializerOptions options)
    {
        if (value.Messages.Any())
        {
            
            writer.WriteStartObject();
            writer.WriteStartObject("partitioning");

            writer.WriteString(nameof(MessageSendRequest.Partitioning.Kind).ToSnakeCase(), value: value.Partitioning.Kind switch
            {
                Partitioning.Balanced => "none",
                Partitioning.MessageKey => "entity_id",
                Partitioning.PartitionId => "partition_id",
                _ => throw new InvalidEnumArgumentException()
            });
            writer.WriteBase64String(nameof(MessageSendRequest.Partitioning.Value).ToSnakeCase(), value.Partitioning.Value);
            writer.WriteEndObject();

            writer.WriteStartArray("messages");
            foreach (var msg in value.Messages)
            {
                JsonSerializer.Serialize(writer, msg, JsonConverterFactory.HttpMessageOptions);
            }
            writer.WriteEndArray();

            writer.WriteEndObject();
            return;
        }
        writer.WriteStringValue("");
    }
}