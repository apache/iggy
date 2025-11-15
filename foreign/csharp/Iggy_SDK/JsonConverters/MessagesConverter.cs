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
using Apache.Iggy.Messages;

namespace Apache.Iggy.JsonConverters;

internal sealed class MessagesConverter : JsonConverter<MessageSendRequest>
{
    private static readonly Dictionary<Partitioning, string> PartitioningKindMapping = new()
    {
        { Partitioning.Balanced, "balanced" },
        { Partitioning.MessageKey, "messages_key" },
        { Partitioning.PartitionId, "partition_id" }
    };

    public override MessageSendRequest? Read(ref Utf8JsonReader reader, Type typeToConvert,
        JsonSerializerOptions options)
    {
        throw new NotImplementedException();
    }

    public override void Write(Utf8JsonWriter writer, MessageSendRequest value, JsonSerializerOptions options)
    {
        writer.WriteStartObject();

        WritePartitioning(writer, value.Partitioning);
        WriteMessages(writer, value.Messages, options);

        writer.WriteEndObject();
    }

    private static void WritePartitioning(Utf8JsonWriter writer, Kinds.Partitioning partitioning)
    {
        writer.WriteStartObject("partitioning");

        if (!PartitioningKindMapping.TryGetValue(partitioning.Kind, out var kindString))
        {
            throw new InvalidEnumArgumentException(nameof(partitioning.Kind), (int)partitioning.Kind,
                typeof(Partitioning));
        }

        writer.WriteString("kind", kindString);
        writer.WriteBase64String("value", partitioning.Value);

        writer.WriteEndObject();
    }

    private static void WriteMessages(Utf8JsonWriter writer, IList<Message> messages, JsonSerializerOptions options)
    {
        writer.WriteStartArray("messages");

        foreach (var message in messages)
        {
            JsonSerializer.Serialize(writer, message, options);
        }

        writer.WriteEndArray();
    }
}
