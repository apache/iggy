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
using Apache.Iggy.Contracts.Http;
using Apache.Iggy.Extensions;

namespace Apache.Iggy.JsonConfiguration;

internal sealed class CreateTopicConverter : JsonConverter<TopicRequest>
{
    public override TopicRequest? Read(ref Utf8JsonReader reader, Type typeToConvert, JsonSerializerOptions options)
    {
        throw new NotImplementedException();
    }

    public override void Write(Utf8JsonWriter writer, TopicRequest value, JsonSerializerOptions options)
    {
        writer.WriteStartObject();
        
        // If not provided, the Iggy server will generate one automatically
        if (value.TopicId is not null)
        {
            writer.WriteNumber(nameof(value.TopicId).ToSnakeCase(), (int)value.TopicId);
        }
        
        writer.WriteString(nameof(value.Name).ToSnakeCase(), value.Name);
        writer.WriteString(nameof(value.CompressionAlgorithm).ToSnakeCase(), value.CompressionAlgorithm.ToString());
        
        writer.WriteNumber(nameof(value.MessageExpiry).ToSnakeCase(), (int)value.MessageExpiry);
        writer.WriteNumber(nameof(value.PartitionsCount).ToSnakeCase(), value.PartitionsCount);
        writer.WriteNumber(nameof(value.MaxTopicSize).ToSnakeCase(), value.MaxTopicSize);
        writer.WriteNumber(nameof(value.ReplicationFactor).ToSnakeCase(), value.ReplicationFactor);
        writer.WriteEndObject();
    }
}