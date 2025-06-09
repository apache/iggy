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

using Iggy_SDK_Tests.E2ETests.Fixtures.Models;
using Iggy_SDK_Tests.Utils.Streams;
using Iggy_SDK_Tests.Utils.Topics;
using Iggy_SDK.Contracts.Http;

namespace Iggy_SDK_Tests.E2ETests.Fixtures.Bootstraps;

public class TopicsFixtureBootstrap : IIggyBootstrap
{
    public static readonly StreamRequest StreamRequest = StreamFactory.CreateStreamRequest();
    public static readonly TopicRequest TopicRequest = TopicFactory.CreateTopicRequest();
    public static readonly TopicRequest TopicRequestSecond = TopicFactory.CreateTopicRequest();
    public static readonly UpdateTopicRequest UpdateTopicRequest = TopicFactory.CreateUpdateTopicRequest();
    
    
    public async Task BootstrapResourcesAsync(IggyClientModel httpClient, IggyClientModel tcpClient)
    {
        await tcpClient.Client.CreateStreamAsync(StreamRequest);
        await httpClient.Client.CreateStreamAsync(StreamRequest);
    }
}