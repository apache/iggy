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

using Apache.Iggy.Contracts;
using Apache.Iggy.Enums;

namespace Apache.Iggy.IggyClient;

public interface IIggyTopic
{
    Task<IReadOnlyList<TopicResponse>> GetTopicsAsync(Identifier streamId, CancellationToken token = default);
    Task<TopicResponse?> GetTopicByIdAsync(Identifier streamId, Identifier topicId, CancellationToken token = default);

    Task<TopicResponse?> CreateTopicAsync(Identifier streamId, string name, uint partitionsCount,
        CompressionAlgorithm compressionAlgorithm = CompressionAlgorithm.None, uint? topicId = null,
        byte? replicationFactor = null, ulong messageExpiry = 0, ulong maxTopicSize = 0,
        CancellationToken token = default);

    Task UpdateTopicAsync(Identifier streamId, Identifier topicId, string name,
        CompressionAlgorithm compressionAlgorithm = CompressionAlgorithm.None, ulong maxTopicSize = 0,
        ulong messageExpiry = 0, byte? replicationFactor = null, CancellationToken token = default);

    Task DeleteTopicAsync(Identifier streamId, Identifier topicId, CancellationToken token = default);
    Task PurgeTopicAsync(Identifier streamId, Identifier topicId, CancellationToken token = default);
}