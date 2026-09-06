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
using Apache.Iggy.Utils;

namespace Apache.Iggy.Kinds;

/// <summary>
///     Used to specify to which partition the messages should be sent.
/// </summary>
public readonly struct Partitioning
{
    /// <summary>
    ///     Partitioning strategy.
    /// </summary>
    public required Enums.Partitioning Kind { get; init; }

    /// <summary>
    ///     Length of the partitioning value in bytes, always derived from <see cref="Value" />.
    ///     The initializer is kept for compatibility and its value is ignored.
    /// </summary>
    public int Length
    {
        get => _value.Length;
        init { }
    }

    /// <summary>
    ///     Copy of the partitioning value as bytes, at most 255 of them.
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
    ///     Creates a partitioning strategy that use default partitioning (balanced).
    /// </summary>
    /// <returns>Partitioning instance</returns>
    public static Partitioning None()
    {
        return new Partitioning
        {
            Kind = Enums.Partitioning.Balanced,
            Value = []
        };
    }

    /// <summary>
    ///     Creates a partitioning strategy that use a specific partition id.
    /// </summary>
    /// <param name="value">Partition id</param>
    /// <returns>Partitioning instance</returns>
    public static Partitioning PartitionId(int value)
    {
        ArgumentOutOfRangeException.ThrowIfNegative(value);
        return PartitionId((uint)value);
    }

    /// <summary>
    ///     Creates a partitioning strategy that use a specific partition id.
    /// </summary>
    /// <param name="value">Partition id</param>
    /// <returns>Partitioning instance</returns>
    public static Partitioning PartitionId(uint value)
    {
        var bytes = new byte[4];
        BinaryPrimitives.WriteUInt32LittleEndian(bytes, value);

        return new Partitioning
        {
            Kind = Enums.Partitioning.PartitionId,
            Value = bytes
        };
    }

    /// <summary>
    ///     Creates a partitioning strategy that use message key as partitioning value.
    /// </summary>
    /// <param name="value">>Message key as string</param>
    /// <returns>Partitioning instance</returns>
    /// <exception cref="ArgumentException">Thrown when the value size is incorrect</exception>
    public static Partitioning EntityIdString(string value)
    {
        var bytes = Encoding.UTF8.GetBytes(value);
        WireName.Validate(bytes.Length, nameof(value));

        return new Partitioning
        {
            Kind = Enums.Partitioning.MessageKey,
            Value = bytes
        };
    }

    /// <summary>
    ///     Creates a partitioning strategy that use message key as partitioning value.
    /// </summary>
    /// <param name="value">>Message key as byte array</param>
    /// <returns>Partitioning instance</returns>
    /// <exception cref="ArgumentException">Thrown when the value size is incorrect</exception>
    public static Partitioning EntityIdBytes(byte[] value)
    {
        if (value.Length is 0 or > 255)
        {
            throw new ArgumentException("Value has incorrect size, must be between 1 and 255", nameof(value));
        }

        return new Partitioning
        {
            Kind = Enums.Partitioning.MessageKey,
            Value = value
        };
    }

    /// <summary>
    ///     Creates a partitioning strategy that use message key as partitioning value.
    /// </summary>
    /// <param name="value">>Message key as int</param>
    /// <returns>>Partitioning instance</returns>
    public static Partitioning EntityIdInt(int value)
    {
        Span<byte> bytes = stackalloc byte[4];
        BinaryPrimitives.WriteInt32LittleEndian(bytes, value);
        return new Partitioning
        {
            Kind = Enums.Partitioning.MessageKey,
            Value = bytes.ToArray()
        };
    }

    /// <summary>
    ///     Creates a partitioning strategy that use message key as partitioning value.
    /// </summary>
    /// <param name="value">>>Message key as ulong</param>
    /// <returns>>Partitioning instance</returns>
    public static Partitioning EntityIdUlong(ulong value)
    {
        Span<byte> bytes = stackalloc byte[8];
        BinaryPrimitives.WriteUInt64LittleEndian(bytes, value);
        return new Partitioning
        {
            Kind = Enums.Partitioning.MessageKey,
            Value = bytes.ToArray()
        };
    }

    /// <summary>
    ///     Creates a partitioning strategy that use message key as partitioning value.
    /// </summary>
    /// <param name="value">>Message key as Guid</param>
    /// <returns>>Partitioning instance</returns>
    public static Partitioning EntityIdGuid(Guid value)
    {
        var bytes = value.ToByteArray();
        return new Partitioning
        {
            Kind = Enums.Partitioning.MessageKey,
            Value = bytes
        };
    }
}
