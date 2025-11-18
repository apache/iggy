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

using System.Runtime.InteropServices;
using System.Text;
using Apache.Iggy;
using Apache.Iggy.Configuration;
using Apache.Iggy.Factory;
using Apache.Iggy.Kinds;
using Iggy_SDK.Examples.Basic.Consumer;
using Iggy_SDK.Examples.Shared;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.Logging;

var loggerFactory = LoggerFactory.Create(b => { b.AddConsole(); });
var logger = loggerFactory.CreateLogger<Program>();

var configuration = new ConfigurationBuilder().AddJsonFile("appsettings.json").Build();
var settings = configuration.Get<Settings>() ?? new Settings();

logger.LogInformation(
    "Basic consumer has started, selected protocol: {Protocol}",
    settings.Protocol
);

var client = IggyClientFactory.CreateClient(new IggyClientConfigurator()
{
    BaseAddress = settings.BaseAddress,
    Protocol = settings.Protocol,
    LoggerFactory = loggerFactory
});

await client.ConnectAsync();
await client.LoginUser(settings.Username, settings.Password);

logger.LogInformation("Basic consumer has logged on successfully");

var streamId = Identifier.String(settings.StreamName);
var topicId = Identifier.String(settings.TopicName);
var partitionId = 0u;
var consumerId = 1;

await ExampleHelpers.EnsureStreamExists(client, streamId, settings.StreamName);
await ExampleHelpers.EnsureTopicExists(
    client,
    streamId,
    topicId,
    settings.TopicName,
    settings.PartitionsCount
);

var consumedBatches = 0;
while (true)
{
    if (settings.MessageBatchesLimit > 0 && consumedBatches == settings.MessageBatchesLimit)
    {
        logger.LogInformation("Consumed {ConsumedBatches} batches of messages, exiting.", consumedBatches);
        break;
    }

    var response = await client.PollMessagesAsync(
        streamId,
        topicId,
        partitionId,
        Consumer.New(consumerId),
        PollingStrategy.Next(),
        10,
        true
    );

    foreach (var message in response.Messages)
        logger.LogInformation(
            "Handling message at offset: {Offset}, payload: {Payload}...",
            message.Header.Offset,
            Encoding.UTF8.GetString(message.Payload)
        );

    consumedBatches++;
}
