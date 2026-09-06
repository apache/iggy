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
using System.Text;
using Apache.Iggy.Enums;
using Apache.Iggy.Utils;

namespace Apache.Iggy;

/// <summary>
///     A unique identifier for a resource.
/// </summary>
public readonly struct Identifier : IEquatable<Identifier>
{
    /// <summary>
    ///     Identifier kind.
    /// </summary>
    public required IdKind Kind { get; init; }

    /// <summary>
    ///     Identifier length in bytes, always derived from <see cref="Value" />.
    ///     The initializer is kept for compatibility and its value is ignored.
    /// </summary>
    public int Length
    {
        get => _value.Length;
        init { }
    }

    /// <summary>
    ///     Copy of the identifier value as bytes, at most 255 of them.
    /// </summary>
    /// <exception cref="ArgumentOutOfRangeException">Thrown when the value is longer than 255 bytes.</exception>
    public required byte[] Value
    {
        get => _value.ToArray();
        init
        {
            ArgumentNullException.ThrowIfNull(value);
            ArgumentOutOfRangeException.ThrowIfGreaterThan(value.Length, WireName.MAX_LENGTH, nameof(Value));
            _value = value.ToArray();
        }
    }

    /// <summary>
    ///     Read-only view of the value bytes for serialization, without the defensive copy of <see cref="Value" />.
    /// </summary>
    internal ReadOnlySpan<byte> Bytes => _value;

    private readonly byte[] _value;

    /// <summary>
    ///     Creates a numeric identifier from a value.
    /// </summary>
    /// <param name="value">Identifier value</param>
    /// <returns></returns>
    public static Identifier Numeric(int value)
    {
        ArgumentOutOfRangeException.ThrowIfNegative(value);
        return Numeric((uint)value);
    }

    /// <summary>
    ///     Creates a numeric identifier from a value.
    /// </summary>
    /// <param name="value">Identifier value</param>
    /// <returns></returns>
    public static Identifier Numeric(uint value)
    {
        var bytes = new byte[4];
        BinaryPrimitives.WriteUInt32LittleEndian(bytes, value);

        return new Identifier
        {
            Kind = IdKind.Numeric,
            Value = bytes
        };
    }

    /// <summary>
    ///     Creates a string identifier from a value.
    /// </summary>
    /// <param name="value">Identifier value</param>
    /// <returns></returns>
    /// <exception cref="ArgumentException">Thrown when the value is too long or too short.</exception>
    public static Identifier String(string value)
    {
        var bytes = Encoding.UTF8.GetBytes(value);
        WireName.Validate(bytes.Length, nameof(value));

        return new Identifier
        {
            Kind = IdKind.String,
            Value = bytes
        };
    }

    /// <inheritdoc />
    public override string ToString()
    {
        return Kind switch
        {
            IdKind.Numeric => BitConverter.ToInt32(_value).ToString(),
            IdKind.String => Encoding.UTF8.GetString(_value),
            _ => throw new ArgumentOutOfRangeException()
        };
    }

    /// <summary>
    ///     Gets the numeric value of the identifier.
    /// </summary>
    /// <returns>Unsigned integer identifier value.</returns>
    /// <exception cref="InvalidOperationException">Thrown when the identifier is not numeric.</exception>
    public uint GetUInt32()
    {
        if (Kind != IdKind.Numeric)
        {
            throw new InvalidOperationException("Identifier is not numeric");
        }

        return BinaryPrimitives.ReadUInt32LittleEndian(_value);
    }

    /// <summary>
    ///     Gets the string value of the identifier.
    /// </summary>
    /// <returns>String identifier value.</returns>
    /// <exception cref="InvalidOperationException">Thrown when the identifier is not string.</exception>
    public string GetString()
    {
        if (Kind != IdKind.String)
        {
            throw new InvalidOperationException("Identifier is not string");
        }

        return Encoding.UTF8.GetString(_value);
    }

    /// <summary>
    ///     Determines whether the current identifier is equal to another identifier.
    /// </summary>
    /// <param name="other">The identifier to compare with the current identifier.</param>
    /// <returns>True if the current identifier is equal to the other identifier; otherwise, false.</returns>
    public bool Equals(Identifier other)
    {
        return Kind == other.Kind && Bytes.SequenceEqual(other.Bytes);
    }

    /// <inheritdoc />
    public override bool Equals(object? obj)
    {
        return obj is Identifier other && Equals(other);
    }

    /// <inheritdoc />
    public override int GetHashCode()
    {
        var hash = new HashCode();
        hash.Add(Kind);
        hash.AddBytes(_value);
        return hash.ToHashCode();
    }
}
