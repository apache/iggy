// // Licensed to the Apache Software Foundation (ASF) under one
// // or more contributor license agreements.  See the NOTICE file
// // distributed with this work for additional information
// // regarding copyright ownership.  The ASF licenses this file
// // to you under the Apache License, Version 2.0 (the
// // "License"); you may not use this file except in compliance
// // with the License.  You may obtain a copy of the License at
// //
// //   http://www.apache.org/licenses/LICENSE-2.0
// //
// // Unless required by applicable law or agreed to in writing,
// // software distributed under the License is distributed on an
// // "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// // KIND, either express or implied.  See the License for the
// // specific language governing permissions and limitations
// // under the License.

using Apache.Iggy.Contracts.Http;
using Apache.Iggy.Tests.Integrations.Helpers;

namespace Apache.Iggy.Tests.Integrations.Fixtures;

public class PartitionsFixture : IggyServerFixture
{
    public readonly StreamRequest StreamRequest = StreamFactory.CreateStream();
    public readonly TopicRequest TopicRequest = TopicFactory.CreateTopic();

    public override async Task InitializeAsync()
    {
        await base.InitializeAsync();

        foreach (var client in Clients.Values)
        {
            await client.CreateStreamAsync(StreamRequest);
            await client.CreateTopicAsync(Identifier.Numeric(StreamRequest.StreamId!.Value), TopicRequest);
        }
    }
}