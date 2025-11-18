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
    ///     Identifier length in bytes.
    /// </summary>
    public required int Length { get; init; }

    /// <summary>
    ///     Identifier value as bytes.
    /// </summary>
    public required byte[] Value { get; init; }

    /// <summary>
    ///     Creates a numeric identifier from a value.
    /// </summary>
    /// <param name="value">Identifier value</param>
    /// <returns></returns>
    public static Identifier Numeric(int value)
    {
        var bytes = new byte[4];
        BinaryPrimitives.WriteInt32LittleEndian(bytes, value);

        return new Identifier
        {
            Kind = IdKind.Numeric,
            Length = 4,
            Value = bytes
        };
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
            Length = 4,
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
        if (value.Length is 0 or > 255)
        {
            throw new ArgumentException("Value has incorrect size, must be between 1 and 255", nameof(value));
        }

        return new Identifier
        {
            Kind = IdKind.String,
            Length = value.Length,
            Value = Encoding.UTF8.GetBytes(value)
        };
    }

    /// <inheritdoc />
    public override string ToString()
    {
        return Kind switch
        {
            IdKind.Numeric => BitConverter.ToInt32(Value).ToString(),
            IdKind.String => Encoding.UTF8.GetString(Value),
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

        return BinaryPrimitives.ReadUInt32LittleEndian(Value);
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

        return Encoding.UTF8.GetString(Value);
    }

    /// <summary>
    ///     Determines whether the current identifier is equal to another identifier.
    /// </summary>
    /// <param name="other">The identifier to compare with the current identifier.</param>
    /// <returns>True if the current identifier is equal to the other identifier; otherwise, false.</returns>
    public bool Equals(Identifier other)
    {
        return Kind == other.Kind && Value.Equals(other.Value);
    }

    /// <inheritdoc />
    public override bool Equals(object? obj)
    {
        return obj is Identifier other && Equals(other);
    }

    /// <inheritdoc />
    public override int GetHashCode()
    {
        return HashCode.Combine((int)Kind, Value);
    }
}
