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

public sealed class AuthResponseConverter : JsonConverter<AuthResponse>
{
    public override AuthResponse? Read(ref Utf8JsonReader reader, Type typeToConvert, JsonSerializerOptions options)
    {
        using var doc = JsonDocument.ParseValue(ref reader);
        var root = doc.RootElement;
        
        var userId = root.GetProperty(nameof(AuthResponse.UserId).ToSnakeCase()).GetInt32();
        var accessToken = root.GetProperty(nameof(AuthResponse.AccessToken).ToSnakeCase());
        var token = accessToken.GetProperty(nameof(TokenInfo.Token).ToSnakeCase()).GetString();
        var accessTokenExpiry = accessToken.GetProperty(nameof(TokenInfo.Expiry).ToSnakeCase()).GetInt64();

        if(token is null)
        {
            throw new JsonException("Access token is null");
        }
        
        return new AuthResponse(
            userId,
            new TokenInfo(token, DateTimeOffset.FromUnixTimeSeconds(accessTokenExpiry).LocalDateTime)
        );
    }
    public override void Write(Utf8JsonWriter writer, AuthResponse value, JsonSerializerOptions options)
    {
        throw new NotImplementedException();
    }
}