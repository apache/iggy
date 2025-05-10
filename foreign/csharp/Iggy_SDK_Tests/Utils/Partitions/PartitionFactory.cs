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

namespace Iggy_SDK_Tests.Utils.Partitions;

public static class PartitionFactory
{
    public static CreatePartitionsRequest CreatePartitionsRequest()
    {
        var streamId = Identifier.Numeric(1);
        var topicId = Identifier.Numeric(1);
        return new CreatePartitionsRequest
        {
            StreamId = streamId,
            TopicId = topicId,
            PartitionsCount = Random.Shared.Next(1, 69)
        };
    }
    public static CreatePartitionsRequest CreatePartitionsRequest(int streamId, int topicId)
    {
        var stream = Identifier.Numeric(streamId);
        var topic = Identifier.Numeric(topicId);
        return new CreatePartitionsRequest
        {
            StreamId = stream,
            TopicId = topic,
            PartitionsCount = Random.Shared.Next(1, 69)
        };
    }
    public static DeletePartitionsRequest CreateDeletePartitionsRequest()
    {
        var streamId = Identifier.Numeric(1);
        var topicId = Identifier.Numeric(1);
        return new DeletePartitionsRequest
        {
            StreamId = streamId,
            TopicId = topicId,
            PartitionsCount = Random.Shared.Next(1, 69)
        };
    }
    public static DeletePartitionsRequest CreateDeletePartitionsRequest(int streamId, int topicId, int count)
    {
        var stream = Identifier.Numeric(streamId);
        var topic = Identifier.Numeric(topicId);
        return new DeletePartitionsRequest
        {
            StreamId = stream,
            TopicId = topic,
            PartitionsCount = count
        };
    }
}