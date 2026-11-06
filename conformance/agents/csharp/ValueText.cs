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
using Apache.Iggy.Headers;

namespace Apache.Iggy.ConformanceAgent;

/// <summary>
///     The reversible text of an option or header value, as in core/common/fixtures/resource_options.json: UTF-8 for
///     strings, <c>true</c> or <c>false</c>, base-10 integers, padded standard base64 for raw bytes, and <c>0x</c> with
///     the IEEE-754 bits in fixed-width lowercase hex for floats. The SDK keeps its own codec internal.
/// </summary>
internal static class ValueText
{
    private static readonly Dictionary<string, HeaderKind> Kinds = Enum.GetValues<HeaderKind>().ToDictionary(KindName);

    internal static string KindName(HeaderKind kind)
    {
        return kind switch
        {
            HeaderKind.Float => "float32",
            HeaderKind.Double => "float64",
            _ when Enum.IsDefined(kind) => kind.ToString().ToLowerInvariant(),
            _ => throw new FormatException($"Unknown header kind {(int)kind}.")
        };
    }

    internal static string Encode(HeaderValue value)
    {
        var bytes = value.Value;
        if (Size(value.Kind) is { } size && bytes.Length != size)
        {
            throw new FormatException($"A {KindName(value.Kind)} value of {bytes.Length} bytes.");
        }

        return value.Kind switch
        {
            HeaderKind.Raw => Convert.ToBase64String(bytes),
            HeaderKind.String => Encoding.UTF8.GetString(bytes),
            HeaderKind.Bool => bytes[0] switch
            {
                0 => "false",
                1 => "true",
                var other => throw new FormatException($"A bool value of {other}.")
            },
            HeaderKind.Int8 => Text((sbyte)bytes[0]),
            HeaderKind.Int16 => Text(BinaryPrimitives.ReadInt16LittleEndian(bytes)),
            HeaderKind.Int32 => Text(BinaryPrimitives.ReadInt32LittleEndian(bytes)),
            HeaderKind.Int64 => Text(BinaryPrimitives.ReadInt64LittleEndian(bytes)),
            HeaderKind.Int128 => Text(BinaryPrimitives.ReadInt128LittleEndian(bytes)),
            HeaderKind.Uint8 => Text(bytes[0]),
            HeaderKind.Uint16 => Text(BinaryPrimitives.ReadUInt16LittleEndian(bytes)),
            HeaderKind.Uint32 => Text(BinaryPrimitives.ReadUInt32LittleEndian(bytes)),
            HeaderKind.Uint64 => Text(BinaryPrimitives.ReadUInt64LittleEndian(bytes)),
            HeaderKind.Uint128 => Text(BinaryPrimitives.ReadUInt128LittleEndian(bytes)),
            HeaderKind.Float => "0x" + BinaryPrimitives.ReadUInt32LittleEndian(bytes).ToString("x8", Invariant),
            HeaderKind.Double => "0x" + BinaryPrimitives.ReadUInt64LittleEndian(bytes).ToString("x16", Invariant),
            _ => throw new FormatException($"Unknown header kind {(int)value.Kind}.")
        };
    }

    /// <summary>The value that a text encodes for the kind named <paramref name="kindName" />.</summary>
    internal static HeaderValue Decode(string kindName, string text)
    {
        if (!Kinds.TryGetValue(kindName, out var kind))
        {
            throw new FormatException($"Unknown header kind {kindName}.");
        }

        var bytes = kind switch
        {
            HeaderKind.Raw => Convert.FromBase64String(text),
            HeaderKind.String => Encoding.UTF8.GetBytes(text),
            HeaderKind.Bool => [bool.Parse(text) ? (byte)1 : (byte)0],
            HeaderKind.Int8 => Bytes(sbyte.Parse(text, Invariant)),
            HeaderKind.Int16 => Bytes(short.Parse(text, Invariant)),
            HeaderKind.Int32 => Bytes(int.Parse(text, Invariant)),
            HeaderKind.Int64 => Bytes(long.Parse(text, Invariant)),
            HeaderKind.Int128 => Bytes(Int128.Parse(text, Invariant)),
            HeaderKind.Uint8 => Bytes(byte.Parse(text, Invariant)),
            HeaderKind.Uint16 => Bytes(ushort.Parse(text, Invariant)),
            HeaderKind.Uint32 => Bytes(uint.Parse(text, Invariant)),
            HeaderKind.Uint64 => Bytes(ulong.Parse(text, Invariant)),
            HeaderKind.Uint128 => Bytes(UInt128.Parse(text, Invariant)),
            HeaderKind.Float => Bytes(uint.Parse(FloatBits(text, 8), NumberStyles.AllowHexSpecifier, Invariant)),
            HeaderKind.Double => Bytes(ulong.Parse(FloatBits(text, 16), NumberStyles.AllowHexSpecifier, Invariant)),
            _ => throw new FormatException($"Unknown header kind {kindName}.")
        };
        return new HeaderValue { Kind = kind, Value = bytes };
    }

    private static CultureInfo Invariant => CultureInfo.InvariantCulture;

    private static int? Size(HeaderKind kind)
    {
        return kind switch
        {
            HeaderKind.Bool or HeaderKind.Int8 or HeaderKind.Uint8 => 1,
            HeaderKind.Int16 or HeaderKind.Uint16 => 2,
            HeaderKind.Int32 or HeaderKind.Uint32 or HeaderKind.Float => 4,
            HeaderKind.Int64 or HeaderKind.Uint64 or HeaderKind.Double => 8,
            HeaderKind.Int128 or HeaderKind.Uint128 => 16,
            _ => null
        };
    }

    private static string Text<T>(T value) where T : IBinaryInteger<T>
    {
        return value.ToString(null, Invariant);
    }

    private static byte[] Bytes<T>(T value) where T : IBinaryInteger<T>
    {
        var bytes = new byte[value.GetByteCount()];
        value.WriteLittleEndian(bytes);
        return bytes;
    }

    private static string FloatBits(string text, int digits)
    {
        return text.StartsWith("0x", StringComparison.Ordinal) && text.Length == digits + 2
            ? text[2..]
            : throw new FormatException($"A float value is 0x and {digits} hex digits, got {text}.");
    }
}
