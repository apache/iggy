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

namespace Apache.Iggy.IggyClient;

/// <summary>
///     Defines methods for managing segments within partitions in an Iggy client.
///     Segments are the underlying storage units that hold messages within a partition.
/// </summary>
public interface IIggySegment
{
    /// <summary>
    ///     Deletes the last N segments from a partition.
    /// </summary>
    /// <remarks>
    ///     For example, given a partition with 5 segments, if you delete 2 segments,
    ///     the partition will have 3 segments left (from 1 to 3).
    ///     Authentication is required, and the permission to manage the segments.
    /// </remarks>
    /// <param name="streamId">The identifier of the stream containing the topic (numeric ID or name).</param>
    /// <param name="topicId">The identifier of the topic containing the partition (numeric ID or name).</param>
    /// <param name="partitionId">The unique partition ID.</param>
    /// <param name="segmentsCount">The number of segments to delete.</param>
    /// <param name="token">The cancellation token to cancel the operation.</param>
    /// <returns>A task representing the asynchronous operation.</returns>
    Task DeleteSegmentsAsync(Identifier streamId, Identifier topicId, uint partitionId, uint segmentsCount,
        CancellationToken token = default);
}
