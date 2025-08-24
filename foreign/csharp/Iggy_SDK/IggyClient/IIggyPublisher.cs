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
using Apache.Iggy.Headers;
using Apache.Iggy.Kinds;
using Apache.Iggy.Messages;

namespace Apache.Iggy.IggyClient;

public interface IIggyPublisher
{
    Task SendMessagesAsync(Identifier streamId, Identifier topicId, Partitioning partitioning, Message[] messages,
        Func<byte[], byte[]>? encryptor = null, CancellationToken token = default)
    {
        return SendMessagesAsync(new MessageSendRequest
        {
            Messages = messages,
            Partitioning = partitioning,
            StreamId = streamId,
            TopicId = topicId
        }, encryptor, token);
    }

    Task SendMessagesAsync(MessageSendRequest request, Func<byte[], byte[]>? encryptor = null,
        CancellationToken token = default);

    Task SendMessagesAsync<TMessage>(MessageSendRequest<TMessage> request, Func<TMessage, byte[]> serializer,
        Func<byte[], byte[]>? encryptor = null, Dictionary<HeaderKey, HeaderValue>? headers = null,
        CancellationToken token = default);

    Task FlushUnsavedBufferAsync(Identifier streamId, Identifier topicId, uint partitionId, bool fsync,
        CancellationToken token = default);
}