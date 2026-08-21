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

/// <summary>
///     A resource's options split by provenance, the way the binary transports send them: what a
///     client set at creation, and what the server resolved from its own defaults.
/// </summary>
internal readonly record struct ResourceOptions(
    Dictionary<HeaderKey, HeaderValue> Explicit,
    Dictionary<HeaderKey, HeaderValue> Derived);

/// <summary>
///     Reads the one object a REST response renders both provenances into, keyed by option name,
///     each entry a readable value plus the flag saying who set it:
///     <c>{"segment_size":{"value":"1073741824","explicit":false}}</c>. The value arrives as the
///     string a config file or a create body would carry it in, so every value maps to
///     <see cref="HeaderKind.String" /> and the server's canonical kind for the key is not
///     recoverable from it.
/// </summary>
internal sealed class ResourceOptionsConverter : JsonConverter<ResourceOptions>
{
    public override ResourceOptions Read(ref Utf8JsonReader reader, Type typeToConvert,
        JsonSerializerOptions options)
    {
        var explicitOptions = new Dictionary<HeaderKey, HeaderValue>();
        var derivedOptions = new Dictionary<HeaderKey, HeaderValue>();

        if (reader.TokenType == JsonTokenType.Null)
        {
            return new ResourceOptions(explicitOptions, derivedOptions);
        }

        if (reader.TokenType != JsonTokenType.StartObject)
        {
            throw new JsonException($"Expected start of object for options but got {reader.TokenType}.");
        }

        while (reader.Read())
        {
            if (reader.TokenType == JsonTokenType.EndObject)
            {
                return new ResourceOptions(explicitOptions, derivedOptions);
            }

            if (reader.TokenType != JsonTokenType.PropertyName)
            {
                throw new JsonException($"Expected option name but got {reader.TokenType}.");
            }

            var key = reader.GetString();
            if (string.IsNullOrEmpty(key))
            {
                throw new JsonException("Option name must not be empty.");
            }

            reader.Read();
            var (value, isExplicit) = ReadOptionValue(ref reader, key);
            var target = isExplicit ? explicitOptions : derivedOptions;
            target[HeaderKey.FromString(key)] = value;
        }

        throw new JsonException("Unexpected end of options object.");
    }

    /// <summary>
    ///     Options travel out as the plain string map a create or update body takes, never in this
    ///     shape, so nothing serializes through here.
    /// </summary>
    public override void Write(Utf8JsonWriter writer, ResourceOptions value, JsonSerializerOptions options)
    {
        throw new NotSupportedException("Resource options are read from a response, not written to a request.");
    }

    private static (HeaderValue value, bool isExplicit) ReadOptionValue(ref Utf8JsonReader reader, string key)
    {
        if (reader.TokenType != JsonTokenType.StartObject)
        {
            throw new JsonException($"Expected start of object for option '{key}' but got {reader.TokenType}.");
        }

        string? value = null;
        bool? isExplicit = null;

        while (reader.Read())
        {
            if (reader.TokenType == JsonTokenType.EndObject)
            {
                break;
            }

            if (reader.TokenType != JsonTokenType.PropertyName)
            {
                throw new JsonException($"Expected property name for option '{key}'.");
            }

            var propertyName = reader.GetString();
            reader.Read();

            switch (propertyName)
            {
                case "value":
                    value = reader.GetString();
                    break;
                case "explicit":
                    isExplicit = reader.GetBoolean();
                    break;
                default:
                    reader.Skip();
                    break;
            }
        }

        if (value is null || isExplicit is null)
        {
            throw new JsonException($"Option '{key}' must have both 'value' and 'explicit' properties.");
        }

        return (HeaderValue.FromString(value), isExplicit.Value);
    }
}
