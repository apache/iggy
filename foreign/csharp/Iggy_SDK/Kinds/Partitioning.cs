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
    ///     Length of the partitioning value.
    /// </summary>
    public required int Length { get; init; }

    /// <summary>
    ///     Partitioning value as bytes.
    /// </summary>
    public required byte[] Value { get; init; }

    /// <summary>
    ///     Creates a partitioning strategy that use default partitioning (balanced).
    /// </summary>
    /// <returns>Partitioning instance</returns>
    public static Partitioning None()
    {
        return new Partitioning
        {
            Kind = Enums.Partitioning.Balanced,
            Length = 0,
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
        var bytes = new byte[4];
        BinaryPrimitives.WriteInt32LittleEndian(bytes, value);

        return new Partitioning
        {
            Kind = Enums.Partitioning.PartitionId,
            Length = 4,
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
        if (value.Length is 0 or > 255)
        {
            throw new ArgumentException("Value has incorrect size, must be between 1 and 255", nameof(value));
        }

        return new Partitioning
        {
            Kind = Enums.Partitioning.MessageKey,
            Length = value.Length,
            Value = Encoding.UTF8.GetBytes(value)
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
            Length = value.Length,
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
            Length = 4,
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
            Length = 8,
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
            Length = 16,
            Value = bytes
        };
    }
}
