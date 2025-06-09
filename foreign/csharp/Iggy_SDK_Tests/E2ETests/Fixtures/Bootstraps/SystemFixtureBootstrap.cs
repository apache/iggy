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
using Iggy_SDK.Contracts.Http.Auth;
using Iggy_SDK.Enums;
using Iggy_SDK.Factory;
using Microsoft.Extensions.Logging.Abstractions;

namespace Iggy_SDK_Tests.E2ETests.Fixtures.Bootstraps;

public class SystemFixtureBootstrap : IIggyBootstrap
{
    private const int FRESH_CLIENTS_COUNT = 6;
    
    public const int TotalClientsCount = FRESH_CLIENTS_COUNT + 1;
    
    public async Task BootstrapResourcesAsync(IggyClientModel httpClient, IggyClientModel tcpClient)
    {
        for (int i = 0; i < FRESH_CLIENTS_COUNT; i++)
        {
            var client = MessageStreamFactory.CreateMessageStream(options =>
            {
                options.BaseAdress = $"127.0.0.1:{tcpClient.TcpPort}";
                options.Protocol = Protocol.Tcp;
                options.MessageBatchingSettings = x =>
                {
                    x.MaxMessagesPerBatch = 1000;
                    x.Interval = TimeSpan.FromMilliseconds(100);
                };
            }, NullLoggerFactory.Instance);
            await client.LoginUser(new LoginUserRequest
            {
                Password = "iggy",
                Username = "iggy"
            });
        }

        for (int i = 0; i < TotalClientsCount; i++)
        {
            var client = MessageStreamFactory.CreateMessageStream(options =>
            {
                options.BaseAdress = $"127.0.0.1:{httpClient.TcpPort}";
                options.Protocol = Protocol.Tcp;
                options.MessageBatchingSettings = x =>
                {
                    x.MaxMessagesPerBatch = 1000;
                    x.Interval = TimeSpan.FromMilliseconds(100);
                };
            }, NullLoggerFactory.Instance);
            await client.LoginUser(new LoginUserRequest
            {
                Password = "iggy",
                Username = "iggy"
            });
        }
    }
}