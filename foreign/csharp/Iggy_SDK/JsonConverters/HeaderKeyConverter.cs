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
        return HeaderKey.New(reader.GetString() ?? throw new JsonException("Header key cannot be null or empty."));
    }

    public override void Write(Utf8JsonWriter writer, HeaderKey value, JsonSerializerOptions options)
    {
        writer.WriteStringValue(value.Value);
    }

    public override HeaderKey ReadAsPropertyName(ref Utf8JsonReader reader, Type typeToConvert,
        JsonSerializerOptions options)
    {
        return HeaderKey.New(reader.GetString() ?? throw new JsonException("Header key cannot be null or empty."));
    }

    public override void WriteAsPropertyName(Utf8JsonWriter writer, HeaderKey value, JsonSerializerOptions options)
    {
        writer.WritePropertyName(value.Value);
    }
}