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

using Apache.Iggy.Contracts.Http;
using Apache.Iggy.Kinds;
using Apache.Iggy.Tests.E2ETests.Fixtures.Models;
using Apache.Iggy.Tests.Utils.DummyObj;
using Apache.Iggy.Tests.Utils.Messages;
using Apache.Iggy.Tests.Utils.Streams;
using Apache.Iggy.Tests.Utils.Topics;

namespace Apache.Iggy.Tests.E2ETests.Fixtures.Bootstraps;

public class PollMessagesFixtureBootstrap : IIggyBootstrap
{
    private static readonly StreamRequest StreamRequest = StreamFactory.CreateStreamRequest();
    private static readonly TopicRequest TopicRequest = TopicFactory.CreateTopicRequest();
    private static readonly TopicRequest HeadersTopicRequest = TopicFactory.CreateTopicRequest();
    public const int MessageCount = 100;

    public static readonly int StreamId = (int)StreamRequest.StreamId!;
    public static readonly int TopicId = (int)TopicRequest.TopicId!;
    public static readonly int HeadersTopicId = (int)HeadersTopicRequest.TopicId!;

    public const int PartitionId = 1;
    public const int HeadersCount = 3;

    public async Task BootstrapResourcesAsync(IggyClientModel httpClient, IggyClientModel tcpClient)
    {
        await tcpClient.Client.CreateStreamAsync(StreamRequest);
        await tcpClient.Client.CreateTopicAsync(Identifier.Numeric((int)StreamRequest.StreamId!), TopicRequest);
        await tcpClient.Client.CreateTopicAsync(Identifier.Numeric((int)StreamRequest.StreamId), HeadersTopicRequest);
        await tcpClient.Client.SendMessagesAsync(new MessageSendRequest<DummyMessage>
            {
                Messages = MessageFactory.GenerateDummyMessages(MessageCount),
                Partitioning = Partitioning.PartitionId(PartitionId),
                StreamId = Identifier.Numeric(StreamId),
                TopicId = Identifier.Numeric(TopicId)
            },
            MessageFactory.Serializer,
            headers: MessageFactory.GenerateMessageHeaders(HeadersCount));

        await httpClient.Client.CreateStreamAsync(StreamRequest);
        await httpClient.Client.CreateTopicAsync(Identifier.Numeric((int)StreamRequest.StreamId!), TopicRequest);
        await httpClient.Client.CreateTopicAsync(Identifier.Numeric((int)StreamRequest.StreamId), HeadersTopicRequest);
        await httpClient.Client.SendMessagesAsync(new MessageSendRequest<DummyMessage>
            {
                Messages = MessageFactory.GenerateDummyMessages(MessageCount),
                Partitioning = Partitioning.PartitionId(PartitionId),
                StreamId = Identifier.Numeric(StreamId),
                TopicId = Identifier.Numeric(TopicId)
            },
            MessageFactory.Serializer,
            headers: MessageFactory.GenerateMessageHeaders(HeadersCount));
    }
}