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

public readonly struct Identifier : IEquatable<Identifier>
{
    public required IdKind Kind { get; init; }
    public required int Length { get; init; }
    public required byte[] Value { get; init; }

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

    public override string ToString()
    {
        return Kind switch
        {
            IdKind.Numeric => BitConverter.ToInt32(Value).ToString(),
            IdKind.String => Encoding.UTF8.GetString(Value),
            _ => throw new ArgumentOutOfRangeException()
        };
    }

    public bool Equals(Identifier other)
    {
        return Kind == other.Kind && Value.Equals(other.Value);
    }

    public override bool Equals(object? obj)
    {
        return obj is Identifier other && Equals(other);
    }

    public override int GetHashCode()
    {
        return HashCode.Combine((int)Kind, Value);
    }
}