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

/// <summary>
///     A header value.
/// </summary>
public readonly struct HeaderValue
{
    /// <summary>
    ///     Header kind.
    /// </summary>
    public required HeaderKind Kind { get; init; }

    /// <summary>
    ///     Header value.
    /// </summary>
    public required byte[] Value { get; init; }

    /// <summary>
    ///     Creates a header value from raw bytes.
    /// </summary>
    /// <param name="value">Raw bytes</param>
    /// <returns></returns>
    public static HeaderValue FromBytes(byte[] value)
    {
        return new HeaderValue
        {
            Kind = HeaderKind.Raw,
            Value = value
        };
    }

    /// <summary>
    ///     Creates a header value from a string.
    /// </summary>
    /// <param name="value">Header value</param>
    /// <returns></returns>
    /// <exception cref="ArgumentException"></exception>
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

    /// <summary>
    ///     Creates a header value from a boolean.
    /// </summary>
    /// <param name="value">Header value</param>
    /// <returns></returns>
    public static HeaderValue FromBool(bool value)
    {
        return new HeaderValue
        {
            Kind = HeaderKind.Bool,
            Value = BitConverter.GetBytes(value)
        };
    }

    /// <summary>
    ///     Creates a header value from an integer.
    /// </summary>
    /// <param name="value">Header value</param>
    /// <returns></returns>
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

    /// <summary>
    ///     Creates a header value from a long.
    /// </summary>
    /// <param name="value">Header value</param>
    /// <returns></returns>
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

    /// <summary>
    ///     Creates a header value from an Int128.
    /// </summary>
    /// <param name="value">Header value</param>
    /// <returns></returns>
    public static HeaderValue FromInt128(Int128 value)
    {
        return new HeaderValue
        {
            Kind = HeaderKind.Int128,
            Value = value.GetBytesFromInt128()
        };
    }

    /// <summary>
    ///     Creates a header value from a Guid.
    /// </summary>
    /// <param name="value">Header value</param>
    /// <returns></returns>
    public static HeaderValue FromGuid(Guid value)
    {
        return new HeaderValue
        {
            Kind = HeaderKind.Uint128,
            Value = value.GetBytesFromGuid()
        };
    }

    /// <summary>
    ///     Creates a header value from a uint.
    /// </summary>
    /// <param name="value">Header value</param>
    /// <returns></returns>
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

    /// <summary>
    ///     Creates a header value from a ulong.
    /// </summary>
    /// <param name="value">Header value</param>
    /// <returns></returns>
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

    /// <summary>
    ///     Creates a header value from a UInt128.
    /// </summary>
    /// <param name="value">Header value</param>
    /// <returns></returns>
    public static HeaderValue FromUInt128(UInt128 value)
    {
        return new HeaderValue
        {
            Kind = HeaderKind.Uint128,
            Value = value.GetBytesFromUInt128()
        };
    }

    /// <summary>
    ///     Creates a header value from a float.
    /// </summary>
    /// <param name="value">Header value</param>
    /// <returns></returns>
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

    /// <summary>
    ///     Creates a header value from a double.
    /// </summary>
    /// <param name="value">Header value</param>
    /// <returns></returns>
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

    /// <summary>
    ///     Converts the header value to raw bytes.
    /// </summary>
    /// <returns>Header values as bytes</returns>
    /// <exception cref="InvalidOperationException"></exception>
    public byte[] ToBytes()
    {
        if (Kind is not HeaderKind.Raw)
        {
            throw new InvalidOperationException("Can't convert header");
        }

        return Value;
    }

    /// <summary>
    ///     Converts the header value to a string.
    /// </summary>
    /// <returns>String representation of header value</returns>
    /// <exception cref="InvalidOperationException"></exception>
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

    /// <summary>
    ///     Converts the header value to a boolean.
    /// </summary>
    /// <returns>Boolean representation of the header value.</returns>
    /// <exception cref="InvalidOperationException">Thrown when the header kind is not Bool.</exception>
    public bool ToBool()
    {
        if (Kind is not HeaderKind.Bool)
        {
            throw new InvalidOperationException("Can't convert header");
        }

        return BitConverter.ToBoolean(Value, 0);
    }

    /// <summary>
    ///     Converts the header value to a 32-bit integer.
    /// </summary>
    /// <returns>The 32-bit integer representation of the header value.</returns>
    /// <exception cref="InvalidOperationException">Thrown when the header kind is not <see cref="HeaderKind.Int32" />.</exception>
    public int ToInt32()
    {
        if (Kind is not HeaderKind.Int32)
        {
            throw new InvalidOperationException("Can't convert header");
        }

        return BitConverter.ToInt32(Value, 0);
    }

    /// <summary>
    ///     Converts the header value to a 64-bit signed integer.
    /// </summary>
    /// <returns>A 64-bit signed integer representation of the header value.</returns>
    /// <exception cref="InvalidOperationException">Thrown when the header kind is not <see cref="HeaderKind.Int64" />.</exception>
    public long ToInt64()
    {
        if (Kind is not HeaderKind.Int64)
        {
            throw new InvalidOperationException("Can't convert header");
        }

        return BitConverter.ToInt64(Value, 0);
    }

    /// <summary>
    ///     Converts the header value to a 128-bit integer.
    /// </summary>
    /// <returns>The 128-bit integer representation of the header value.</returns>
    /// <exception cref="InvalidOperationException">Thrown if the header kind is not Int128.</exception>
    public Int128 ToInt128()
    {
        if (Kind is not HeaderKind.Int128)
        {
            throw new InvalidOperationException("Can't convert header");
        }

        return Value.ToInt128();
    }

    /// <summary>
    ///     Converts the header value to a GUID.
    /// </summary>
    /// <returns>A <see cref="Guid" /> representation of the header value.</returns>
    /// <exception cref="InvalidOperationException">
    ///     Thrown when the header value's kind is not <see cref="HeaderKind.Uint128" />.
    /// </exception>
    public Guid ToGuid()
    {
        if (Kind is not HeaderKind.Uint128)
        {
            throw new InvalidOperationException("Can't convert header");
        }

        return new Guid(Value);
    }

    /// <summary>
    ///     Converts the current header value to an unsigned 32-bit integer.
    /// </summary>
    /// <returns>An unsigned 32-bit integer representation of the header value.</returns>
    /// <exception cref="InvalidOperationException">Thrown when the header kind is not <c>HeaderKind.Uint32</c>.</exception>
    public uint ToUInt32()
    {
        if (Kind is not HeaderKind.Uint32)
        {
            throw new InvalidOperationException("Can't convert header");
        }

        return BitConverter.ToUInt32(Value);
    }

    /// <summary>
    ///     Converts the header value to a 64-bit unsigned integer.
    /// </summary>
    /// <returns>A 64-bit unsigned integer representation of the header value.</returns>
    /// <exception cref="InvalidOperationException">Thrown when the header kind is not Uint64.</exception>
    public ulong ToUInt64()
    {
        if (Kind is not HeaderKind.Uint64)
        {
            throw new InvalidOperationException("Can't convert header");
        }

        return BitConverter.ToUInt64(Value);
    }

    /// <summary>
    ///     Converts the header value to a UInt128 type.
    /// </summary>
    /// <returns>The UInt128 representation of the header value.</returns>
    /// <exception cref="InvalidOperationException">Thrown when the header kind is not Uint128.</exception>
    public UInt128 ToUInt128()
    {
        if (Kind is not HeaderKind.Uint128)
        {
            throw new InvalidOperationException("Can't convert header");
        }

        return Value.ToUInt128();
    }

    /// <summary>
    ///     Converts the header value to a float.
    /// </summary>
    /// <returns>A float representation of the header value.</returns>
    /// <exception cref="InvalidOperationException">Thrown when the header kind is not <c>HeaderKind.Float</c>.</exception>
    public float ToFloat()
    {
        if (Kind is not HeaderKind.Float)
        {
            throw new InvalidOperationException("Can't convert header");
        }

        return BitConverter.ToSingle(Value);
    }

    /// <summary>
    ///     Converts the header value to a double-precision floating-point number.
    /// </summary>
    /// <returns>The double-precision floating-point number represented by the header value.</returns>
    /// <exception cref="InvalidOperationException">Thrown when the header kind is not compatible with a double conversion.</exception>
    public double ToDouble()
    {
        if (Kind is not HeaderKind.Double)
        {
            throw new InvalidOperationException("Can't convert header");
        }

        return BitConverter.ToDouble(Value);
    }
}
