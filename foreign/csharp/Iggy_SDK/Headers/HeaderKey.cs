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

using System.Text.Json.Serialization;
using Apache.Iggy.JsonConverters;

namespace Apache.Iggy.Headers;

/// <summary>
///     A key for a header.
/// </summary>
[JsonConverter(typeof(HeaderKeyConverter))]
public readonly struct HeaderKey : IEquatable<HeaderKey>
{
    /// <summary>
    ///     Header key value.
    /// </summary>
    public required string Value { get; init; }

    /// <summary>
    ///     Creates a new header key from a string.
    /// </summary>
    /// <param name="val">Key value</param>
    /// <returns></returns>
    /// <exception cref="ArgumentException"></exception>
    public static HeaderKey New(string val)
    {
        return new HeaderKey
        {
            Value = val.Length is 0 or > 255
                ? throw new ArgumentException("Value has incorrect size, must be between 1 and 255", nameof(val))
                : val
        };
    }

    /// <inheritdoc />
    public override string ToString()
    {
        return Value;
    }

    /// <inheritdoc />
    public bool Equals(HeaderKey other)
    {
        return StringComparer.Ordinal.Equals(Value, other.Value);
    }

    /// <inheritdoc />
    public override bool Equals(object? obj)
    {
        return obj is HeaderKey other && Equals(other);
    }

    /// <inheritdoc />
    public override int GetHashCode()
    {
        return StringComparer.Ordinal.GetHashCode(Value);
    }

    /// <summary>
    ///     Determines whether two specified <see cref="HeaderKey" /> objects are equal.
    /// </summary>
    /// <param name="left">The first <see cref="HeaderKey" /> to compare.</param>
    /// <param name="right">The second <see cref="HeaderKey" /> to compare.</param>
    /// <returns>True if the two <see cref="HeaderKey" /> objects are equal; otherwise, false.</returns>
    public static bool operator ==(HeaderKey left, HeaderKey right)
    {
        return left.Equals(right);
    }

    /// <summary>
    ///     Determines whether two specified <see cref="HeaderKey" /> objects are not equal.
    /// </summary>
    /// <param name="left">The first <see cref="HeaderKey" /> to compare.</param>
    /// <param name="right">The second <see cref="HeaderKey" /> to compare.</param>
    /// <returns>True if the two <see cref="HeaderKey" /> objects are not equal; otherwise, false.</returns>
    public static bool operator !=(HeaderKey left, HeaderKey right)
    {
        return !left.Equals(right);
    }
}
