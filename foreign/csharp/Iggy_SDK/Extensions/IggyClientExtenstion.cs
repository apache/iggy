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

using Apache.Iggy.Consumers;
using Apache.Iggy.IggyClient;
using Apache.Iggy.Kinds;
using Apache.Iggy.Publishers;

namespace Apache.Iggy.Extensions;

public static class IggyClientExtenstion
{
    public static IggyConsumerBuilder CreateConsumerBuilder(this IIggyClient client, Identifier streamId,
        Identifier topicId, Consumer consumer)
    {
        return IggyConsumerBuilder.Create(client, streamId, topicId, consumer);
    }

    public static IggyConsumerBuilder CreateConsumerBuilder<T>(this IIggyClient client, Identifier streamId,
        Identifier topicId, Consumer consumer, IDeserializer<T> deserializer) where T : IDeserializer<T>
    {
        return IggyConsumerBuilder.Create(client, streamId, topicId, consumer);
    }

    public static IggyPublisherBuilder CreatePublisherBuilder(this IIggyClient client, Identifier streamId,
        Identifier topicId)
    {
        return IggyPublisherBuilder.Create(client, streamId, topicId);
    }
}
