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

using Iggy_SDK;
using Iggy_SDK.Contracts.Http;
using Iggy_SDK.Kinds;

namespace Iggy_SDK_Tests.Utils.Offset;

internal static class OffsetFactory
{
    internal static OffsetRequest CreateOffsetRequest()
    {
        return new OffsetRequest
        {
            Consumer = Consumer.New(1),
            TopicId = Identifier.Numeric(Random.Shared.Next(1, 10)),
            PartitionId = Random.Shared.Next(1, 10),
            StreamId = Identifier.Numeric(Random.Shared.Next(1, 10)),
        };
    }
    internal static OffsetRequest CreateOffsetRequest(int streamId, int topicId, int partitionId, int consumerId)
    {
        return new OffsetRequest
        {
            Consumer = Consumer.New(consumerId),
            TopicId = Identifier.Numeric(topicId),
            PartitionId = partitionId,
            StreamId = Identifier.Numeric(streamId),
        };
    }

    internal static OffsetResponse CreateOffsetResponse()
    {
        return new OffsetResponse
        {
            CurrentOffset = (ulong)Random.Shared.Next(420, 69420),
            PartitionId = Random.Shared.Next(1, 10),
            StoredOffset = (ulong)Random.Shared.Next(69, 420),
        };
    }

    internal static StoreOffsetRequest CreateOffsetContract()
    {
        var streamId = Identifier.Numeric(1);
        var topicId = Identifier.Numeric(1);
        return new StoreOffsetRequest
        {
            StreamId = streamId,
            TopicId = topicId,
            Consumer = Consumer.New(1),
            Offset = (ulong)Random.Shared.Next(1, 10),
            PartitionId = Random.Shared.Next(1, 10),
        };
    }
    internal static StoreOffsetRequest CreateOffsetContract(int streamId, int topicId, int consumerId, ulong offset,
        int partitionId)
    {
        var stream = Identifier.Numeric(streamId);
        var topic = Identifier.Numeric(topicId);
        return new StoreOffsetRequest
        {
            StreamId = stream,
            TopicId = topic,
            Consumer = Consumer.New(consumerId),
            Offset = offset,
            PartitionId = partitionId
        };
    }
}