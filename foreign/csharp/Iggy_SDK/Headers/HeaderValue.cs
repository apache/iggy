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
using System.Globalization;
using System.Text;
using Apache.Iggy.Extensions;

namespace Apache.Iggy.Headers;

public readonly struct HeaderValue
{
    public required HeaderKind Kind { get; init; }
    public required byte[] Value { get; init; }

    public static HeaderValue FromBytes(byte[] value)
    {
        return new HeaderValue
        {
            Kind = HeaderKind.Raw,
            Value = value
        };
    }

    public static HeaderValue FromString(string value)
    {
        if (value.Length is 0 or > 255)
        {
            throw new ArgumentException("Value has incorrect size, must be between 1 and 255", nameof(value));
        }

        return new HeaderValue
        {
            Kind = HeaderKind.String,
            Value = Encoding.UTF8.GetBytes(value)
        };
    }

    public static HeaderValue FromBool(bool value)
    {
        return new HeaderValue
        {
            Kind = HeaderKind.Bool,
            Value = BitConverter.GetBytes(value)
        };
    }

    public static HeaderValue FromInt32(int value)
    {
        var bytes = new byte[4];
        BinaryPrimitives.WriteInt32LittleEndian(bytes, value);
        return new HeaderValue
        {
            Kind = HeaderKind.Int32,
            Value = bytes
        };
    }

    public static HeaderValue FromInt64(long value)
    {
        var bytes = new byte[8];
        BinaryPrimitives.WriteInt64LittleEndian(bytes, value);
        return new HeaderValue
        {
            Kind = HeaderKind.Int64,
            Value = bytes
        };
    }

    public static HeaderValue FromInt128(Int128 value)
    {
        return new HeaderValue
        {
            Kind = HeaderKind.Int128,
            Value = value.GetBytesFromInt128()
        };
    }

    public static HeaderValue FromGuid(Guid value)
    {
        return new HeaderValue
        {
            Kind = HeaderKind.Uint128,
            Value = value.GetBytesFromGuid()
        };
    }

    public static HeaderValue FromUInt32(uint value)
    {
        var bytes = new byte[4];
        BinaryPrimitives.WriteUInt32LittleEndian(bytes, value);
        return new HeaderValue
        {
            Kind = HeaderKind.Uint32,
            Value = bytes
        };
    }

    public static HeaderValue FromUInt64(ulong value)
    {
        var bytes = new byte[8];
        BinaryPrimitives.WriteUInt64LittleEndian(bytes, value);
        return new HeaderValue
        {
            Kind = HeaderKind.Uint64,
            Value = bytes
        };
    }

    public static HeaderValue FromUInt128(UInt128 value)
    {
        return new HeaderValue
        {
            Kind = HeaderKind.Uint128,
            Value = value.GetBytesFromUInt128()
        };
    }

    public static HeaderValue FromFloat(float value)
    {
        var bytes = new byte[4];
        BinaryPrimitives.TryWriteSingleLittleEndian(bytes, value);
        return new HeaderValue
        {
            Kind = HeaderKind.Float,
            Value = bytes
        };
    }

    public static HeaderValue FromDouble(double value)
    {
        var bytes = new byte[8];
        BinaryPrimitives.TryWriteDoubleLittleEndian(bytes, value);
        return new HeaderValue
        {
            Kind = HeaderKind.Double,
            Value = bytes
        };
    }

    public byte[] ToBytes()
    {
        if (Kind is not HeaderKind.Raw)
        {
            throw new InvalidOperationException("Can't convert header");
        }

        return Value;
    }

    public new string ToString()
    {
        return Kind switch
        {
            HeaderKind.Raw => Value.ToString(),
            HeaderKind.String => Encoding.UTF8.GetString(Value),
            HeaderKind.Bool => Value[0].ToString(CultureInfo.InvariantCulture),
            HeaderKind.Int32 => BinaryPrimitives.ReadInt32LittleEndian(Value).ToString(),
            HeaderKind.Int64 => BinaryPrimitives.ReadInt64LittleEndian(Value).ToString(),
            HeaderKind.Int128 => Value.ToInt128().ToString(),
            HeaderKind.Uint32 => BinaryPrimitives.ReadUInt32LittleEndian(Value).ToString(),
            HeaderKind.Uint64 => BinaryPrimitives.ReadUInt64LittleEndian(Value).ToString(),
            HeaderKind.Uint128 => Value.ToUInt128().ToString(),
            HeaderKind.Float => BinaryPrimitives.ReadSingleLittleEndian(Value).ToString(),
            HeaderKind.Double => BinaryPrimitives.ReadDoubleLittleEndian(Value).ToString(),
            _ => throw new InvalidOperationException("Can't convert header")
        } ?? throw new InvalidOperationException();
    }

    public bool ToBool()
    {
        if (Kind is not HeaderKind.Bool)
        {
            throw new InvalidOperationException("Can't convert header");
        }

        return BitConverter.ToBoolean(Value, 0);
    }

    public int ToInt32()
    {
        if (Kind is not HeaderKind.Int32)
        {
            throw new InvalidOperationException("Can't convert header");
        }

        return BitConverter.ToInt32(Value, 0);
    }

    public long ToInt64()
    {
        if (Kind is not HeaderKind.Int64)
        {
            throw new InvalidOperationException("Can't convert header");
        }

        return BitConverter.ToInt64(Value, 0);
    }

    public Int128 ToInt128()
    {
        if (Kind is not HeaderKind.Int128)
        {
            throw new InvalidOperationException("Can't convert header");
        }

        return Value.ToInt128();
    }

    public Guid ToGuid()
    {
        if (Kind is not HeaderKind.Uint128)
        {
            throw new InvalidOperationException("Can't convert header");
        }

        return new Guid(Value);
    }

    public uint ToUInt32()
    {
        if (Kind is not HeaderKind.Uint32)
        {
            throw new InvalidOperationException("Can't convert header");
        }

        return BitConverter.ToUInt32(Value);
    }

    public ulong ToUInt64()
    {
        if (Kind is not HeaderKind.Uint64)
        {
            throw new InvalidOperationException("Can't convert header");
        }

        return BitConverter.ToUInt64(Value);
    }

    public UInt128 ToUInt128()
    {
        if (Kind is not HeaderKind.Uint128)
        {
            throw new InvalidOperationException("Can't convert header");
        }

        return Value.ToUInt128();
    }

    public float ToFloat()
    {
        if (Kind is not HeaderKind.Float)
        {
            throw new InvalidOperationException("Can't convert header");
        }

        return BitConverter.ToSingle(Value);
    }

    public double ToDouble()
    {
        if (Kind is not HeaderKind.Double)
        {
            throw new InvalidOperationException("Can't convert header");
        }

        return BitConverter.ToDouble(Value);
    }
}