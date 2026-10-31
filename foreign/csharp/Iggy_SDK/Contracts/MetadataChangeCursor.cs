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
using Apache.Iggy.Utils;

namespace Apache.Iggy.Contracts;

/// <summary>
///     Position of the last metadata change record a caller consumed or scanned. Send the cursor an answer returns
///     in <see cref="MetadataChanges.Next" /> back to read the records after it.
/// </summary>
/// <remarks>
///     The text form is <c>mc1_</c> followed by unpadded Base64url of
///     <c>[log_id:u128][op:u64][checksum:u128][ordinal:u32]</c>, little-endian. Op 0 with a zero checksum and ordinal
///     names the beginning of the history <see cref="LogId" /> identifies.
/// </remarks>
/// <param name="LogId">Prepare checksum of op 1, the root bootstrap of this history.</param>
/// <param name="Op">The metadata operation of the record.</param>
/// <param name="Checksum">Prepare checksum of <paramref name="Op" />.</param>
/// <param name="Ordinal">Position of the record within <paramref name="Op" />.</param>
public readonly record struct MetadataChangeCursor(UInt128 LogId, ulong Op, UInt128 Checksum, uint Ordinal)
{
    /// <summary>Text prefix of a change cursor.</summary>
    public const string PREFIX = "mc1_";

    internal const int SIZE = 44;

    private const int TEXT_LENGTH = 63;

    /// <summary>Whether the cursor names the beginning of its history.</summary>
    public bool IsZero => Op == 0;

    /// <summary>Op 0 carries no checksum and no ordinal. A cursor that does is refused.</summary>
    public bool IsCanonical => Op != 0 || (Checksum == UInt128.Zero && Ordinal == 0);

    /// <summary>
    ///     Parses the <c>mc1_</c> text form.
    /// </summary>
    /// <exception cref="FormatException">The text is not a canonical change cursor.</exception>
    public static MetadataChangeCursor Parse(string text)
    {
        return TryParse(text, out var cursor)
            ? cursor
            : throw new FormatException("The text is not a canonical metadata change cursor.");
    }

    /// <summary>
    ///     Parses the <c>mc1_</c> text form, refusing a wrong prefix or length, padding, the standard Base64
    ///     alphabet, non-canonical encodings and a non-canonical cursor.
    /// </summary>
    public static bool TryParse([NotNullWhen(true)] string? text, out MetadataChangeCursor cursor)
    {
        cursor = default;
        if (text is not { Length: TEXT_LENGTH } || !text.StartsWith(PREFIX, StringComparison.Ordinal)
            || !Base64Url.TryDecode(text.AsSpan(PREFIX.Length), out var bytes) || bytes.Length != SIZE)
        {
            return false;
        }

        cursor = Read(bytes);
        return cursor.IsCanonical;
    }

    /// <summary>The <c>mc1_</c> text form.</summary>
    public override string ToString()
    {
        Span<byte> bytes = stackalloc byte[SIZE];
        WriteTo(bytes);
        return PREFIX + Base64Url.Encode(bytes);
    }

    internal void WriteTo(Span<byte> destination)
    {
        BinaryPrimitives.WriteUInt128LittleEndian(destination[..16], LogId);
        BinaryPrimitives.WriteUInt64LittleEndian(destination[16..24], Op);
        BinaryPrimitives.WriteUInt128LittleEndian(destination[24..40], Checksum);
        BinaryPrimitives.WriteUInt32LittleEndian(destination[40..44], Ordinal);
    }

    internal static MetadataChangeCursor Read(ReadOnlySpan<byte> source)
    {
        return new MetadataChangeCursor(BinaryPrimitives.ReadUInt128LittleEndian(source[..16]),
            BinaryPrimitives.ReadUInt64LittleEndian(source[16..24]),
            BinaryPrimitives.ReadUInt128LittleEndian(source[24..40]),
            BinaryPrimitives.ReadUInt32LittleEndian(source[40..44]));
    }
}
