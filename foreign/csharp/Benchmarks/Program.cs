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

using Apache.Iggy;
using Apache.Iggy.Benchmarks;
using Apache.Iggy.Configuration;
using Apache.Iggy.Enums;
using Apache.Iggy.Factory;
using Apache.Iggy.IggyClient;
using Microsoft.Extensions.Logging;

const int messagesCount = 1000;
const int messagesBatch = 1000;
const int messageSize = 1000;
const int producerCount = 3;
const uint startingStreamId = 100;
const int topicId = 1;
Dictionary<int, IIggyClient> clients = new();
var loggerFactory = LoggerFactory.Create(builder =>
{
    builder
        .AddFilter("Iggy_SDK.MessageStream.Implementations;", LogLevel.Trace)
        .AddConsole();
});

for (var i = 0; i < producerCount; i++)
{
    var bus = IggyClientFactory.CreateClient(new IggyClientConfigurator()
    {
        BaseAddress = "127.0.0.1:8090",
        Protocol = Protocol.Tcp,
        LoggerFactory = loggerFactory,
#if OS_LINUX
        ReceiveBufferSize = Int32.MaxValue,
        SendBufferSize = Int32.MaxValue,
#elif OS_WINDOWS
        ReceiveBufferSize = int.MaxValue,
        SendBufferSize = int.MaxValue,
#elif OS_MAC
		ReceiveBufferSize = 7280*1024,
		SendBufferSize = 7280*1024,
#endif
    });

    await bus.LoginUser("iggy", "iggy");
    clients[i] = bus;
}

try
{
    for (uint i = 0; i < producerCount; i++)
    {
        await clients[0].CreateStreamAsync($"Test bench stream_{i}");

        await clients[0].CreateTopicAsync(Identifier.Numeric(startingStreamId + i),
            name: $"Test bench topic_{i}",
            compressionAlgorithm: CompressionAlgorithm.None,
            messageExpiry: 0,
            maxTopicSize: 2_000_000_000,
            replicationFactor: 3,
            partitionsCount: 1);
    }
}
catch
{
    Console.WriteLine("Failed to create streams, they already exist.");
}

List<Task> tasks = new();

for (var i = 0; i < producerCount; i++)
{
    tasks.Add(SendMessage.Create(clients[i], i, producerCount, messagesBatch, messagesCount, messageSize,
        Identifier.Numeric(startingStreamId + (uint)i),
        Identifier.Numeric(topicId)));
}

await Task.WhenAll(tasks);

try
{
    for (uint i = 0; i < producerCount; i++)
    {
        await clients[0].DeleteStreamAsync(Identifier.Numeric(startingStreamId + i));
    }
}
catch
{
    Console.WriteLine("Failed to delete streams");
}
