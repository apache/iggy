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

using System.Text;
using Apache.Iggy;
using Apache.Iggy.Configuration;
using Apache.Iggy.Contracts;
using Apache.Iggy.Factory;
using Apache.Iggy.Kinds;
using Apache.Iggy.Messages;
using Iggy_SDK.Examples.Basic.Producer;
using Iggy_SDK.Examples.Shared;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.Logging;

var loggerFactory = LoggerFactory.Create(b => { b.AddConsole(); });
var logger = loggerFactory.CreateLogger<Program>();
;
var configuration = new ConfigurationBuilder().AddJsonFile("appsettings.json").Build();
var settings = configuration.Get<Settings>() ?? new Settings();

logger.LogInformation(
    "Basic producer has started, selected protocol: {Protocol}",
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

logger.LogInformation("Basic producer has logged on successfully");

var streamId = Identifier.String(settings.StreamName);
var topicId = Identifier.String(settings.TopicName);

await ExampleHelpers.EnsureStreamExists(client, streamId, settings.StreamName);
await ExampleHelpers.EnsureTopicExists(
    client,
    streamId,
    topicId,
    settings.TopicName,
    settings.PartitionsCount
);

var sentBatches = 0;
var currentId = 0;
while (true)
{
    if (settings.MessageBatchesLimit > 0 && sentBatches == settings.MessageBatchesLimit)
    {
        logger.LogInformation("Sent {SentBatches} batches of messages, exiting.", sentBatches);
        break;
    }

    var payloads = Enumerable
        .Range(currentId, settings.MessagesPerBatch)
        .Aggregate(new List<string>(), (list, next) =>
        {
            list.Add($"message-{next}");
            return list;
        });

    var messages = payloads.Select(payload => new Message(Guid.NewGuid(), Encoding.UTF8.GetBytes(payload))).ToList();

    await client.SendMessagesAsync(streamId, topicId, Partitioning.None(), messages);

    currentId += settings.MessagesPerBatch;
    sentBatches++;
    logger.LogInformation("Sent messages: {Messages}", payloads);

    await Task.Delay(settings.Interval);
}
