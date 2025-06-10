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
using Apache.Iggy.Contracts.Http.Auth;
using Apache.Iggy.Extensions;

namespace Apache.Iggy.JsonConfiguration;

public sealed class PersonalAccessTokenResponseConverter : JsonConverter<PersonalAccessTokenResponse>
{
    public override PersonalAccessTokenResponse? Read(ref Utf8JsonReader reader, Type typeToConvert, JsonSerializerOptions options)
    {
        using var doc = JsonDocument.ParseValue(ref reader);
        var root = doc.RootElement;

        var name = root.GetProperty(nameof(PersonalAccessTokenResponse.Name).ToSnakeCase()).GetString();
        root.TryGetProperty(nameof(PersonalAccessTokenResponse.ExpiryAt).ToSnakeCase(), out var expiryElement);
        DateTimeOffset? expiry = expiryElement.ValueKind switch
        {
            JsonValueKind.Null => null,
            JsonValueKind.Undefined => null,
            JsonValueKind.Number => DateTimeOffsetUtils.FromUnixTimeMicroSeconds(expiryElement.GetUInt64()).LocalDateTime,
            _ => throw new ArgumentOutOfRangeException()
        };
        return new PersonalAccessTokenResponse
        {
            Name = name!,
            ExpiryAt = expiry
        };
    }
    public override void Write(Utf8JsonWriter writer, PersonalAccessTokenResponse value, JsonSerializerOptions options)
    {
        throw new NotImplementedException();
    }
}