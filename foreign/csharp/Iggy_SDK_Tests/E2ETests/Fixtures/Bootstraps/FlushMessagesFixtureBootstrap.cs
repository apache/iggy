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
using Apache.Iggy.Tests.E2ETests.Fixtures.Models;
using Apache.Iggy.Tests.Utils.Messages;
using Apache.Iggy.Tests.Utils.Streams;
using Apache.Iggy.Tests.Utils.Topics;

namespace Apache.Iggy.Tests.E2ETests.Fixtures.Bootstraps;

public class FlushMessagesFixtureBootstrap : IIggyBootstrap
{
    private static readonly StreamRequest StreamRequest = StreamFactory.CreateStreamRequest();
    private static readonly StreamRequest NonExistingStreamRequest = StreamFactory.CreateStreamRequest();
    private static readonly TopicRequest NonExistingTopicRequest = TopicFactory.CreateTopicRequest(3000);
    private static readonly TopicRequest TopicRequest = TopicFactory.CreateTopicRequest();
    private static readonly TopicRequest HeadersTopicRequest = TopicFactory.CreateTopicRequest();

    public static readonly int StreamId = (int)StreamRequest.StreamId!;
    public static readonly int TopicId = (int)TopicRequest.TopicId!;
    public static readonly int HeadersTopicId = (int)HeadersTopicRequest.TopicId!;

    public static readonly int InvalidStreamId = (int)NonExistingStreamRequest.StreamId!;
    public static readonly int InvalidTopicId = (int)NonExistingTopicRequest.TopicId!;
    public const int PartitionId = 1;

    public async Task BootstrapResourcesAsync(IggyClientModel httpClient, IggyClientModel tcpClient)
    {
        await tcpClient.Client.CreateStreamAsync(StreamRequest);
        await tcpClient.Client.CreateTopicAsync(Identifier.Numeric((int)StreamRequest.StreamId!), TopicRequest);

        var request = MessageFactory.CreateMessageSendRequest(
            (int)StreamRequest.StreamId, (int)TopicRequest.TopicId!, PartitionId,
            MessageFactory.GenerateMessages(20));
        await tcpClient.Client.SendMessagesAsync(request);
        await Task.Delay(1000);
        
        await httpClient.Client.CreateStreamAsync(StreamRequest);
        await httpClient.Client.CreateTopicAsync(Identifier.Numeric((int)StreamRequest.StreamId!), TopicRequest);

        await httpClient.Client.SendMessagesAsync(request);
        await Task.Delay(1000);
    }
}