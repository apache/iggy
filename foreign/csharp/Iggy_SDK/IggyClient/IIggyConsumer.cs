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
using Apache.Iggy.Kinds;

namespace Apache.Iggy.IggyClient;

public interface IIggyConsumer
{
    Task<PolledMessages> PollMessagesAsync(Identifier streamId, Identifier topicId, uint? partitionId,
        Consumer consumer, PollingStrategy pollingStrategy, int count, bool autoCommit,
        Func<byte[], byte[]>? decryptor = null, CancellationToken token = default)
    {
        return PollMessagesAsync(new MessageFetchRequest
        {
            AutoCommit = autoCommit,
            Consumer = consumer,
            Count = count,
            PartitionId = partitionId,
            PollingStrategy = pollingStrategy,
            StreamId = streamId,
            TopicId = topicId
        }, decryptor, token);
    }

    Task<PolledMessages> PollMessagesAsync(MessageFetchRequest request, Func<byte[], byte[]>? decryptor = null,
        CancellationToken token = default);

    Task<PolledMessages<TMessage>> PollMessagesAsync<TMessage>(MessageFetchRequest request,
        Func<byte[], TMessage> deserializer, Func<byte[], byte[]>? decryptor = null, CancellationToken token = default);

    IAsyncEnumerable<MessageResponse<TMessage>> PollMessagesAsync<TMessage>(PollMessagesRequest request,
        Func<byte[], TMessage> deserializer, Func<byte[], byte[]>? decryptor = null,
        CancellationToken token = default);
}