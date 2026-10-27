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
using Apache.Iggy.Headers;
using Apache.Iggy.JsonConverters;

namespace Apache.Iggy.Contracts.Http;

/// <summary>
///     Option values as a REST body carries them: strings and booleans as themselves, unsigned integers as JSON
///     numbers, raw bytes as standard base64. A 64-bit value stays exact because the server reads it as a u64.
/// </summary>
internal static class OptionInputs
{
    /// <summary>Option values keyed by name, in ascending name order.</summary>
    internal static Dictionary<string, object> From(IReadOnlyDictionary<string, HeaderValue> options)
    {
        return options
            .OrderBy(option => option.Key, StringComparer.Ordinal)
            .ToDictionary(option => option.Key, option => Value(option.Value));
    }

    internal static object Value(HeaderValue value)
    {
        return value.Kind switch
        {
            HeaderKind.String => value.ToString(),
            HeaderKind.Bool => value.ToBool(),
            HeaderKind.Uint8 => (ulong)value.Value[0],
            HeaderKind.Uint16 => (ulong)BinaryPrimitives.ReadUInt16LittleEndian(value.Value),
            HeaderKind.Uint32 => (ulong)value.ToUInt32(),
            HeaderKind.Uint64 => value.ToUInt64(),
            HeaderKind.Raw => Convert.ToBase64String(value.Value),
            // The server parses any other kind from its text, the same form a config file carries.
            _ => ResourceOptionsConverter.EncodeValue(value)
        };
    }
}
