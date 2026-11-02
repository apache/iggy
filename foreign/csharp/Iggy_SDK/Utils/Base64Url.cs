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

using System.Diagnostics.CodeAnalysis;

namespace Apache.Iggy.Utils;

/// <summary>
///     Canonical unpadded Base64url, the text form of metadata change cursors and page tokens.
/// </summary>
internal static class Base64Url
{
    internal static string Encode(ReadOnlySpan<byte> bytes)
    {
        return Convert.ToBase64String(bytes).TrimEnd('=').Replace('+', '-').Replace('/', '_');
    }

    /// <summary>
    ///     Decodes <paramref name="text" /> only when it is the canonical encoding of its bytes: the Base64url
    ///     alphabet, no padding and zero trailing bits, so one value has exactly one text form.
    /// </summary>
    internal static bool TryDecode(ReadOnlySpan<char> text, [NotNullWhen(true)] out byte[]? bytes)
    {
        bytes = null;
        if (text.Length % 4 == 1)
        {
            return false;
        }

        var padding = (4 - text.Length % 4) % 4;
        var standard = new char[text.Length + padding];
        for (var index = 0; index < text.Length; index++)
        {
            var character = text[index];
            standard[index] = character switch
            {
                >= 'A' and <= 'Z' or >= 'a' and <= 'z' or >= '0' and <= '9' => character,
                '-' => '+',
                '_' => '/',
                _ => '\0'
            };
            if (standard[index] == '\0')
            {
                return false;
            }
        }

        standard.AsSpan(text.Length).Fill('=');
        var decoded = new byte[standard.Length / 4 * 3];
        if (!Convert.TryFromBase64Chars(standard, decoded, out var written))
        {
            return false;
        }

        decoded = decoded[..written];
        if (!Encode(decoded).AsSpan().SequenceEqual(text))
        {
            return false;
        }

        bytes = decoded;
        return true;
    }
}
