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

using System.Buffers.Binary;
using System.Globalization;
using System.Numerics;
using System.Text;
using System.Text.Json;
using System.Text.Json.Serialization;
using Apache.Iggy.Headers;

namespace Apache.Iggy.JsonConverters;

/// <summary>
///     A resource's options split by provenance, the way the binary transports send them: what a
///     client set, and what the server resolved from its defaults or computes on reads.
/// </summary>
internal readonly record struct ResourceOptions(
    Dictionary<HeaderKey, HeaderValue> Explicit,
    Dictionary<HeaderKey, HeaderValue> Derived);

/// <summary>
///     Reads the one object a REST response renders both provenances into, keyed by option name,
///     each entry a reversible text, its kind and the flag saying who set it:
///     <c>{"segment_size":{"value":"1073741824","kind":"uint64","explicit":false}}</c>. The text decodes back to the
///     exact value a binary transport carries, so a key reads with the same kind over either.
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
    ///     Options travel out as the plain value map a create or update body takes, never in this
    ///     shape, so nothing serializes through here.
    /// </summary>
    public override void Write(Utf8JsonWriter writer, ResourceOptions value, JsonSerializerOptions options)
    {
        throw new NotSupportedException("Resource options are read from a response, not written to a request.");
    }

    /// <summary>
    ///     The reversible text of a value for its kind: UTF-8 for strings, <c>true</c> or <c>false</c>, base-10
    ///     integers, standard padded base64 for raw bytes, and <c>0x</c> followed by the IEEE-754 bits in
    ///     fixed-width lowercase hex for floats.
    /// </summary>
    internal static string EncodeValue(HeaderValue value)
    {
        var bytes = value.Value;
        return value.Kind switch
        {
            HeaderKind.Raw => Convert.ToBase64String(bytes),
            HeaderKind.String => Encoding.UTF8.GetString(bytes),
            HeaderKind.Bool => bytes[0] switch
            {
                0 => "false",
                1 => "true",
                _ => throw new FormatException($"Bool option value {bytes[0]} is neither 0 nor 1.")
            },
            HeaderKind.Int8 => ((sbyte)bytes[0]).ToString(CultureInfo.InvariantCulture),
            HeaderKind.Int16 => BinaryPrimitives.ReadInt16LittleEndian(bytes).ToString(CultureInfo.InvariantCulture),
            HeaderKind.Int32 => BinaryPrimitives.ReadInt32LittleEndian(bytes).ToString(CultureInfo.InvariantCulture),
            HeaderKind.Int64 => BinaryPrimitives.ReadInt64LittleEndian(bytes).ToString(CultureInfo.InvariantCulture),
            HeaderKind.Int128 => BinaryPrimitives.ReadInt128LittleEndian(bytes).ToString(CultureInfo.InvariantCulture),
            HeaderKind.Uint8 => bytes[0].ToString(CultureInfo.InvariantCulture),
            HeaderKind.Uint16 => BinaryPrimitives.ReadUInt16LittleEndian(bytes).ToString(CultureInfo.InvariantCulture),
            HeaderKind.Uint32 => BinaryPrimitives.ReadUInt32LittleEndian(bytes).ToString(CultureInfo.InvariantCulture),
            HeaderKind.Uint64 => BinaryPrimitives.ReadUInt64LittleEndian(bytes).ToString(CultureInfo.InvariantCulture),
            HeaderKind.Uint128 => BinaryPrimitives.ReadUInt128LittleEndian(bytes).ToString(CultureInfo.InvariantCulture),
            HeaderKind.Float => "0x" + BinaryPrimitives.ReadUInt32LittleEndian(bytes).ToString("x8", CultureInfo.InvariantCulture),
            HeaderKind.Double => "0x" + BinaryPrimitives.ReadUInt64LittleEndian(bytes).ToString("x16", CultureInfo.InvariantCulture),
            _ => throw new FormatException($"Option value kind {value.Kind} has no text form.")
        };
    }

    /// <summary>
    ///     The value <paramref name="text" /> encodes for <paramref name="kind" />, the inverse of
    ///     <see cref="EncodeValue" />. Text that is not the canonical form of a value is refused, so one value has
    ///     one JSON form.
    /// </summary>
    internal static HeaderValue DecodeValue(HeaderKind kind, string text)
    {
        HeaderValue value;
        try
        {
            value = kind switch
            {
                HeaderKind.Raw => HeaderValue.FromBytes(Convert.FromBase64String(text)),
                HeaderKind.String => HeaderValue.FromString(text),
                HeaderKind.Bool => HeaderValue.FromBool(text switch
                {
                    "true" => true,
                    "false" => false,
                    _ => throw new FormatException("Bool option value must be true or false.")
                }),
                HeaderKind.Int8 => Integer<sbyte>(kind, text, (bytes, parsed) => bytes[0] = (byte)parsed),
                HeaderKind.Int16 => Integer<short>(kind, text, BinaryPrimitives.WriteInt16LittleEndian),
                HeaderKind.Int32 => Integer<int>(kind, text, BinaryPrimitives.WriteInt32LittleEndian),
                HeaderKind.Int64 => Integer<long>(kind, text, BinaryPrimitives.WriteInt64LittleEndian),
                HeaderKind.Int128 => Integer<Int128>(kind, text, BinaryPrimitives.WriteInt128LittleEndian),
                HeaderKind.Uint8 => Integer<byte>(kind, text, (bytes, parsed) => bytes[0] = parsed),
                HeaderKind.Uint16 => Integer<ushort>(kind, text, BinaryPrimitives.WriteUInt16LittleEndian),
                HeaderKind.Uint32 => Integer<uint>(kind, text, BinaryPrimitives.WriteUInt32LittleEndian),
                HeaderKind.Uint64 => Integer<ulong>(kind, text, BinaryPrimitives.WriteUInt64LittleEndian),
                HeaderKind.Uint128 => Integer<UInt128>(kind, text, BinaryPrimitives.WriteUInt128LittleEndian),
                HeaderKind.Float => FloatBits<uint>(kind, text, 8, BinaryPrimitives.WriteUInt32LittleEndian),
                HeaderKind.Double => FloatBits<ulong>(kind, text, 16, BinaryPrimitives.WriteUInt64LittleEndian),
                _ => throw new FormatException($"Option value kind {kind} has no text form.")
            };
        }
        catch (Exception error) when (error is FormatException or OverflowException or ArgumentException)
        {
            throw new JsonException($"Option value is not a valid {kind}.", error);
        }

        if (EncodeValue(value) != text)
        {
            throw new JsonException($"Option value is not the canonical text of a {kind}.");
        }

        return value;
    }

    private delegate void Writer<in T>(Span<byte> destination, T value);

    private static HeaderValue Integer<T>(HeaderKind kind, string text, Writer<T> write)
        where T : IBinaryInteger<T>
    {
        var parsed = T.Parse(text, NumberStyles.AllowLeadingSign, CultureInfo.InvariantCulture);
        var bytes = new byte[parsed.GetByteCount()];
        write(bytes, parsed);
        return new HeaderValue { Kind = kind, Value = bytes };
    }

    private static HeaderValue FloatBits<T>(HeaderKind kind, string text, int digits, Writer<T> write)
        where T : IBinaryInteger<T>
    {
        if (!text.StartsWith("0x", StringComparison.Ordinal) || text.Length != digits + 2)
        {
            throw new FormatException($"Float option value must be 0x followed by {digits} hex digits.");
        }

        var parsed = T.Parse(text.AsSpan(2), NumberStyles.AllowHexSpecifier, CultureInfo.InvariantCulture);
        var bytes = new byte[parsed.GetByteCount()];
        write(bytes, parsed);
        return new HeaderValue { Kind = kind, Value = bytes };
    }

    private static (HeaderValue value, bool isExplicit) ReadOptionValue(ref Utf8JsonReader reader, string key)
    {
        if (reader.TokenType != JsonTokenType.StartObject)
        {
            throw new JsonException($"Expected start of object for option '{key}' but got {reader.TokenType}.");
        }

        string? value = null;
        string? kind = null;
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
                case "kind":
                    kind = reader.GetString();
                    break;
                case "explicit":
                    isExplicit = reader.GetBoolean();
                    break;
                default:
                    reader.Skip();
                    break;
            }
        }

        if (value is null || kind is null || isExplicit is null)
        {
            throw new JsonException($"Option '{key}' must have 'value', 'kind' and 'explicit' properties.");
        }

        return (DecodeValue(UserHeadersConverter.ParseHeaderKind(kind), value), isExplicit.Value);
    }
}
