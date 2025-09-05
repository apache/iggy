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

using Apache.Iggy.Enums;
using Apache.Iggy.IggyClient;

namespace Apache.Iggy.Tests.Integrations.Fixtures;

public class SystemFixture : IggyServerFixture
{
    internal readonly int TotalClientsCount = 10;

    private List<IIggyClient> AdditionalClients { get; } = new();

    public override async Task InitializeAsync()
    {
        await base.InitializeAsync();

        await CreateClientsAsync();
    }

    private async Task CreateClientsAsync()
    {
        for (var i = 0; i < TotalClientsCount; i++)
        {
            await Clients[Protocol.Http].CreateUser($"iggy{i}", "iggy", UserStatus.Active);

            var client = CreateClient(Protocol.Tcp, Protocol.Http);
            AdditionalClients.Add(client);
            var login = await client.LoginUser($"iggy{i}", "iggy");

            if (login!.UserId == 0)
            {
                throw new Exception("Failed to login user 'iggy'.");
            }

            await client.PingAsync();
        }

        // One client less for tcp due to a default client
        for (var i = 0; i < TotalClientsCount - 1; i++)
        {
            await Clients[Protocol.Tcp].CreateUser($"iggy{i}", "iggy", UserStatus.Active);

            var client = CreateClient(Protocol.Tcp, Protocol.Tcp);
            AdditionalClients.Add(client);
            var login = await client.LoginUser($"iggy{i}", "iggy");
            if (login!.UserId == 0)
            {
                throw new Exception("Failed to login user 'iggy'.");
            }

            await client.PingAsync();
        }
    }
}