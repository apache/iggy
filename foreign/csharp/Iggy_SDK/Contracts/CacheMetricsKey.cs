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

namespace Apache.Iggy.Contracts;

/// <summary>
///     Key for cache metrics.
/// </summary>
public struct CacheMetricsKey : IEquatable<CacheMetricsKey>
{
    /// <summary>
    ///     Stream identifier.
    /// </summary>
    public uint StreamId { get; set; }

    /// <summary>
    ///     Topic identifier.
    /// </summary>
    public uint TopicId { get; set; }

    /// <summary>
    ///     Partition identifier.
    /// </summary>
    public uint PartitionId { get; set; }

    /// <inheritdoc />
    public bool Equals(CacheMetricsKey other)
    {
        return StreamId == other.StreamId && TopicId == other.TopicId && PartitionId == other.PartitionId;
    }

    /// <inheritdoc />
    public override bool Equals(object? obj)
    {
        return obj is CacheMetricsKey other && Equals(other);
    }

    /// <inheritdoc />
    public override int GetHashCode()
    {
        return HashCode.Combine(StreamId, TopicId, PartitionId);
    }

    /// <summary>
    ///     Compares two <see cref="CacheMetricsKey" /> instances for equality.
    /// </summary>
    /// <param name="left">The first <see cref="CacheMetricsKey" /> instance to compare.</param>
    /// <param name="right">The second <see cref="CacheMetricsKey" /> instance to compare.</param>
    /// <returns>True if the two instances represent the same value; otherwise, false.</returns>
    public static bool operator ==(CacheMetricsKey left, CacheMetricsKey right)
    {
        return left.Equals(right);
    }

    /// <summary>
    ///     Compares two <see cref="CacheMetricsKey" /> instances for inequality.
    /// </summary>
    /// <param name="left">The first <see cref="CacheMetricsKey" /> instance to compare.</param>
    /// <param name="right">The second <see cref="CacheMetricsKey" /> instance to compare.</param>
    /// <returns>True if the two instances do not represent the same value; otherwise, false.</returns>
    public static bool operator !=(CacheMetricsKey left, CacheMetricsKey right)
    {
        return !(left == right);
    }
}
