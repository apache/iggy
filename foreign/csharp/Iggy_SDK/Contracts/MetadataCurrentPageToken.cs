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
using System.Diagnostics.CodeAnalysis;
using System.Text;
using Apache.Iggy.Enums;
using Apache.Iggy.Utils;

namespace Apache.Iggy.Contracts;

/// <summary>
///     Opaque continuation of a current state scan. Send it back unchanged with the same groups through
///     <see cref="MetadataChangeStart.Current" />.
/// </summary>
/// <remarks>
///     The text form is <c>cs1_</c> followed by unpadded Base64url of
///     <c>[anchor:44][required_head:u64][groups_mask:u8][group:u8][key_len:u16][last_key]</c>, little-endian.
/// </remarks>
public sealed class MetadataCurrentPageToken : IEquatable<MetadataCurrentPageToken>
{
    /// <summary>Text prefix of a current state page token.</summary>
    public const string PREFIX = "cs1_";

    /// <summary>Longest key: a user id and a personal access token name.</summary>
    internal const int MAX_KEY_SIZE = 4 + 1 + WireName.MAX_LENGTH;

    internal const int MAX_SIZE = FIXED_SIZE + MAX_KEY_SIZE;

    private const int FIXED_SIZE = MetadataChangeCursor.SIZE + 8 + 1 + 1 + 2;
    private const int MAX_TEXT_LENGTH = 4 + (MAX_SIZE + 2) / 3 * 4;

    private static readonly UTF8Encoding StrictUtf8 = new(false, true);

    private readonly byte[] _payload;

    private MetadataCurrentPageToken(byte[] payload)
    {
        _payload = payload;
        Anchor = MetadataChangeCursor.Read(payload);
        RequiredHead = BinaryPrimitives.ReadUInt64LittleEndian(payload.AsSpan(MetadataChangeCursor.SIZE, 8));
        GroupsMask = payload[MetadataChangeCursor.SIZE + 8];
        LastGroup = (MetadataChangeGroup)payload[MetadataChangeCursor.SIZE + 9];
    }

    /// <summary>The anchor captured before the scan's first page.</summary>
    public MetadataChangeCursor Anchor { get; }

    /// <summary>The highest head any page of the scan reported so far.</summary>
    public ulong RequiredHead { get; }

    /// <summary>The groups the scan was started with, in code order.</summary>
    public IReadOnlyList<MetadataChangeGroup> Groups => MetadataChangeGroups.FromMask(GroupsMask);

    /// <summary>The group of the last item returned.</summary>
    public MetadataChangeGroup LastGroup { get; }

    /// <summary>The key of the last item returned, in the page token key layout.</summary>
    public ReadOnlyMemory<byte> LastKey => _payload.AsMemory(FIXED_SIZE);

    internal byte GroupsMask { get; }

    internal ReadOnlySpan<byte> Payload => _payload;

    /// <summary>
    ///     Parses the <c>cs1_</c> text form.
    /// </summary>
    /// <exception cref="FormatException">The text is not a canonical page token.</exception>
    public static MetadataCurrentPageToken Parse(string text)
    {
        return TryParse(text, out var token)
            ? token
            : throw new FormatException("The text is not a canonical metadata current page token.");
    }

    /// <summary>
    ///     Parses the <c>cs1_</c> text form, refusing a wrong prefix, padding, non-canonical encodings and a payload
    ///     that breaks the token's own rules.
    /// </summary>
    public static bool TryParse([NotNullWhen(true)] string? text, [NotNullWhen(true)] out MetadataCurrentPageToken? token)
    {
        token = null;
        return text is { Length: <= MAX_TEXT_LENGTH } && text.StartsWith(PREFIX, StringComparison.Ordinal)
               && Base64Url.TryDecode(text.AsSpan(PREFIX.Length), out var bytes) && TryDecode(bytes, out token);
    }

    /// <summary>
    ///     Decodes a token payload, requiring it to be exactly one token: a canonical anchor, a known group selected
    ///     by the mask, a head at or above the anchor and a key of the exact width that group uses.
    /// </summary>
    internal static bool TryDecode(ReadOnlySpan<byte> payload, [NotNullWhen(true)] out MetadataCurrentPageToken? token)
    {
        token = null;
        if (payload.Length < FIXED_SIZE)
        {
            return false;
        }

        var keyLength = BinaryPrimitives.ReadUInt16LittleEndian(payload[(FIXED_SIZE - 2)..FIXED_SIZE]);
        if (keyLength > MAX_KEY_SIZE || payload.Length != FIXED_SIZE + keyLength)
        {
            return false;
        }

        var anchor = MetadataChangeCursor.Read(payload);
        var requiredHead = BinaryPrimitives.ReadUInt64LittleEndian(payload[MetadataChangeCursor.SIZE..]);
        var mask = payload[MetadataChangeCursor.SIZE + 8];
        var group = (MetadataChangeGroup)payload[MetadataChangeCursor.SIZE + 9];
        if (!anchor.IsCanonical || requiredHead < anchor.Op || !MetadataChangeGroups.IsDefined(group)
            || (mask & MetadataChangeGroups.Bit(group)) == 0 || !KeyFitsGroup(group, payload[FIXED_SIZE..]))
        {
            return false;
        }

        token = new MetadataCurrentPageToken(payload.ToArray());
        return true;
    }

    /// <summary>The <c>cs1_</c> text form.</summary>
    public override string ToString()
    {
        return PREFIX + Base64Url.Encode(_payload);
    }

    /// <inheritdoc />
    public bool Equals(MetadataCurrentPageToken? other)
    {
        return other is not null && _payload.AsSpan().SequenceEqual(other._payload);
    }

    /// <inheritdoc />
    public override bool Equals(object? obj)
    {
        return obj is MetadataCurrentPageToken other && Equals(other);
    }

    /// <inheritdoc />
    public override int GetHashCode()
    {
        var hash = new HashCode();
        hash.AddBytes(_payload);
        return hash.ToHashCode();
    }

    private static bool KeyFitsGroup(MetadataChangeGroup group, ReadOnlySpan<byte> key)
    {
        return group switch
        {
            MetadataChangeGroup.Streams or MetadataChangeGroup.Users => key.Length == 4,
            MetadataChangeGroup.Topics or MetadataChangeGroup.Partitions or MetadataChangeGroup.Sessions =>
                key.Length == 8,
            MetadataChangeGroup.ConsumerGroups => key.Length == 12,
            MetadataChangeGroup.ConsumerGroupMembers => key.Length == 16,
            MetadataChangeGroup.PersonalAccessTokens =>
                key.Length > 5 && key.Length == 5 + key[4] && IsUtf8(key[5..]),
            _ => false
        };
    }

    private static bool IsUtf8(ReadOnlySpan<byte> bytes)
    {
        try
        {
            StrictUtf8.GetCharCount(bytes);
            return true;
        }
        catch (DecoderFallbackException)
        {
            return false;
        }
    }
}
