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
using System.Text.Json.Serialization;
using Apache.Iggy.JsonConverters;
using Apache.Iggy.Utils;

namespace Apache.Iggy.Contracts;

/// <summary>
///     The position after the last entry that a list page read passed. Send the <see cref="Page{T}.Next" /> of a
///     page back as the <see cref="PageQuery.Cursor" /> of the same query to read the next page.
/// </summary>
/// <remarks>
///     The text form is <c>pg1_</c> followed by unpadded Base64url of 1 to 255 bytes. Only the server reads the
///     bytes, so the client keeps the bytes of every cursor as they came and the server refuses one it cannot use.
/// </remarks>
[JsonConverter(typeof(PageCursorConverter))]
public sealed class PageCursor : IEquatable<PageCursor>
{
    /// <summary>Text prefix of a page cursor.</summary>
    public const string PREFIX = "pg1_";

    /// <summary>The length of a cursor on the wire is one byte.</summary>
    internal const int MAX_SIZE = byte.MaxValue;

    private const int MAX_TEXT_LENGTH = 4 + (MAX_SIZE + 2) / 3 * 4;

    private readonly byte[] _bytes;

    private PageCursor(byte[] bytes)
    {
        _bytes = bytes;
    }

    internal ReadOnlySpan<byte> Bytes => _bytes;

    /// <summary>
    ///     Parses the <c>pg1_</c> text form.
    /// </summary>
    /// <exception cref="FormatException">The text is not a canonical page cursor.</exception>
    public static PageCursor Parse(string text)
    {
        return TryParse(text, out var cursor)
            ? cursor
            : throw new FormatException("The text is not a canonical page cursor.");
    }

    /// <summary>
    ///     Parses the <c>pg1_</c> text form, refusing a wrong prefix, padding, the standard Base64 alphabet,
    ///     non-canonical encodings, and a cursor of no bytes or of more than 255 bytes.
    /// </summary>
    public static bool TryParse([NotNullWhen(true)] string? text, [NotNullWhen(true)] out PageCursor? cursor)
    {
        cursor = null;
        if (text is not { Length: <= MAX_TEXT_LENGTH } || !text.StartsWith(PREFIX, StringComparison.Ordinal)
            || !Base64Url.TryDecode(text.AsSpan(PREFIX.Length), out var bytes) || bytes.Length is 0 or > MAX_SIZE)
        {
            return false;
        }

        cursor = new PageCursor(bytes);
        return true;
    }

    /// <summary>The cursor of the next field of a reply, which holds 1 to 255 bytes.</summary>
    internal static PageCursor FromBytes(ReadOnlySpan<byte> bytes)
    {
        return new PageCursor(bytes.ToArray());
    }

    /// <summary>The <c>pg1_</c> text form.</summary>
    public override string ToString()
    {
        return PREFIX + Base64Url.Encode(_bytes);
    }

    /// <inheritdoc />
    public bool Equals(PageCursor? other)
    {
        return other is not null && _bytes.AsSpan().SequenceEqual(other._bytes);
    }

    /// <inheritdoc />
    public override bool Equals(object? obj)
    {
        return obj is PageCursor other && Equals(other);
    }

    /// <inheritdoc />
    public override int GetHashCode()
    {
        var hash = new HashCode();
        hash.AddBytes(_bytes);
        return hash.ToHashCode();
    }
}
