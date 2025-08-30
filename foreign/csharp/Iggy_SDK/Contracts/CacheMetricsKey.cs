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

public struct CacheMetricsKey : IEquatable<CacheMetricsKey>
{
    public uint StreamId { get; set; }
    public uint TopicId { get; set; }
    public uint PartitionId { get; set; }

    public bool Equals(CacheMetricsKey other)
    {
        return StreamId == other.StreamId && TopicId == other.TopicId && PartitionId == other.PartitionId;
    }

    public override bool Equals(object? obj)
    {
        return obj is CacheMetricsKey other && Equals(other);
    }

    public override int GetHashCode()
    {
        return HashCode.Combine(StreamId, TopicId, PartitionId);
    }

    public static bool operator ==(CacheMetricsKey left, CacheMetricsKey right)
    {
        return left.Equals(right);
    }

    public static bool operator !=(CacheMetricsKey left, CacheMetricsKey right)
    {
        return !(left == right);
    }
}