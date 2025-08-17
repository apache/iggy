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

public readonly struct Partitioning
{
    public required Enums.Partitioning Kind { get; init; }
    public required int Length { get; init; }
    public required byte[] Value { get; init; }

    public static Partitioning None()
    {
        return new Partitioning
        {
            Kind = Enums.Partitioning.Balanced,
            Length = 0,
            Value = []
        };
    }

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