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
using Apache.Iggy.Configuration;
using Apache.Iggy.Enums;
using Apache.Iggy.Extensions;
using Apache.Iggy.Factory;
using Iggy_SDK.Examples.NewSdk.Producer;
using Microsoft.Extensions.Logging;

var loggerFactory = LoggerFactory.Create(b => { b.AddConsole(); });
ILogger<Program> logger = loggerFactory.CreateLogger<Program>();

var client = IggyClientFactory.CreateClient(new IggyClientConfigurator
{
    BaseAddress = Utils.GetTcpServerAddr(args, logger),
    Protocol = Protocol.Tcp,
    LoggerFactory = loggerFactory
});

await client.ConnectAsync();
await client.LoginUser("iggy", "iggy");

var publisher = client.CreatePublisherBuilder(Identifier.String("new-sdk-stream"), Identifier.String("new-sdk-topic"))
    .CreateStreamIfNotExists("new-sdk-stream")
    .CreateTopicIfNotExists("new-sdk-topic", 4)
    .WithBackgroundSending(batchSize: 5, flushInterval: TimeSpan.FromSeconds(1))
    .WithLogger(loggerFactory)
    .Build();

await publisher.InitAsync();
await Utils.ProduceMessages(publisher, logger);
await publisher.WaitUntilAllSends();
