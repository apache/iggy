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

using System.Text.Json;
using Apache.Iggy;
using Apache.Iggy.Configuration;
using Apache.Iggy.Factory;
using Apache.Iggy.Kinds;
using Apache.Iggy.Messages;
using Iggy_SDK.Examples.Shared;
using Iggy_SDK.Examples.SinkDataProducer.Producer;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.Logging;

string[] sources = ["browser", "mobile", "desktop", "email", "network", "other"];
string[] states = ["active", "inactive", "blocked", "deleted", "unknown"];
string[] domains = ["gmail.com", "yahoo.com", "hotmail.com", "outlook.com", "aol.com"];
const string alphanumeric = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789";

// Snake case keeps the field names identical to the other SDKs' producers, so one
// sink connector config works for all of them.
var jsonOptions = new JsonSerializerOptions { PropertyNamingPolicy = JsonNamingPolicy.SnakeCaseLower };

using var loggerFactory = LoggerFactory.Create(builder => { builder.AddConsole(); });
var logger = loggerFactory.CreateLogger<Program>();

var configuration = new ConfigurationBuilder().AddJsonFile("appsettings.json").Build();
var settings = configuration.Get<Settings>() ?? new Settings();

logger.LogInformation("Starting data producer...");
logger.LogInformation("Stream: {Stream}, Topic: {Topic}", settings.StreamName, settings.TopicName);

using var client = IggyClientFactory.CreateClient(new IggyClientConfigurator()
{
    BaseAddress = settings.BaseAddress,
    Protocol = settings.Protocol,
    LoggerFactory = loggerFactory
});

await client.ConnectAsync();
await client.LoginUserAsync(settings.Username, settings.Password);

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

var totalMessages = 0;
var sentBatches = 0;
while (true)
{
    if (settings.MessageBatchesLimit > 0 && sentBatches == settings.MessageBatchesLimit)
    {
        logger.LogInformation("Sent {SentBatches} batches of messages, exiting.", sentBatches);
        break;
    }

    var recordsCount = Random.Shared.Next(settings.MinRecordsPerBatch, settings.MaxRecordsPerBatch);
    var messages = new List<Message>(recordsCount);
    for (var i = 0; i < recordsCount; i++)
    {
        var payload = JsonSerializer.SerializeToUtf8Bytes(RandomRecord(), jsonOptions);
        messages.Add(new Message(Guid.NewGuid(), payload));
    }

    await client.SendMessagesAsync(streamId, topicId, Partitioning.None(), messages);
    logger.LogInformation("Sent {RecordsCount} messages", recordsCount);
    totalMessages += recordsCount;
    sentBatches++;
}

logger.LogInformation("Reached maximum batches count");
logger.LogInformation("Produced {TotalMessages} messages.", totalMessages);

UserRecord RandomRecord()
{
    return new UserRecord(
        $"user_{Random.Shared.Next(1, 100)}",
        Random.Shared.Next(1, 5),
        $"{RandomAlphanumeric(Random.Shared.Next(3, 20))}@{domains[Random.Shared.Next(domains.Length)]}",
        sources[Random.Shared.Next(sources.Length)],
        states[Random.Shared.Next(states.Length)],
        RandomAlphanumeric(Random.Shared.Next(10, 100)),
        DateTimeOffset.UtcNow - TimeSpan.FromDays(Random.Shared.Next(1000))
    );
}

string RandomAlphanumeric(int length)
{
    return new string(Random.Shared.GetItems(alphanumeric.AsSpan(), length));
}

internal sealed record UserRecord(
    string UserId,
    int UserType,
    string Email,
    string Source,
    string State,
    string Message,
    DateTimeOffset CreatedAt
);
