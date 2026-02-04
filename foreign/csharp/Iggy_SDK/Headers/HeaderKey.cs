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

using System.Text;

namespace Apache.Iggy.Headers;

/// <summary>
/// Represents a message header key with a kind and binary value.
/// </summary>
public readonly struct HeaderKey : IEquatable<HeaderKey>
{
    /// <summary>
    /// The kind of the header key.
    /// </summary>
    public required HeaderKind Kind { get; init; }

    /// <summary>
    /// The binary value of the header key.
    /// </summary>
    public required byte[] Value { get; init; }

    /// <summary>
    /// Creates a HeaderKey from a string value.
    /// </summary>
    /// <param name="val">The string value (must be 1-255 characters).</param>
    /// <returns>A new HeaderKey with String kind.</returns>
    /// <exception cref="ArgumentException">Thrown when value length is invalid.</exception>
    public static HeaderKey FromString(string val)
    {
        if (val.Length is 0 or > 255)
        {
            throw new ArgumentException("Value has incorrect size, must be between 1 and 255", nameof(val));
        }

        return new HeaderKey
        {
            Kind = HeaderKind.String,
            Value = Encoding.UTF8.GetBytes(val)
        };
    }

    /// <summary>
    /// Gets the value as a UTF-8 string.
    /// </summary>
    /// <returns>The string value.</returns>
    /// <exception cref="InvalidOperationException">Thrown when kind is not String.</exception>
    public string AsString()
    {
        if (Kind is not HeaderKind.String)
        {
            throw new InvalidOperationException("HeaderKey is not of String kind");
        }

        return Encoding.UTF8.GetString(Value);
    }

    /// <inheritdoc />
    public override string ToString()
    {
        return Kind == HeaderKind.String ? Encoding.UTF8.GetString(Value) : Convert.ToBase64String(Value);
    }

    /// <inheritdoc />
    public bool Equals(HeaderKey other)
    {
        return Kind == other.Kind && Value.SequenceEqual(other.Value);
    }

    /// <inheritdoc />
    public override bool Equals(object? obj)
    {
        return obj is HeaderKey other && Equals(other);
    }

    /// <inheritdoc />
    public override int GetHashCode()
    {
        var hash = new HashCode();
        hash.Add(Kind);
        foreach (var b in Value)
        {
            hash.Add(b);
        }

        return hash.ToHashCode();
    }

    /// <summary>
    /// Determines whether two HeaderKey instances are equal.
    /// </summary>
    public static bool operator ==(HeaderKey left, HeaderKey right)
    {
        return left.Equals(right);
    }

    /// <summary>
    /// Determines whether two HeaderKey instances are not equal.
    /// </summary>
    public static bool operator !=(HeaderKey left, HeaderKey right)
    {
        return !left.Equals(right);
    }
}
