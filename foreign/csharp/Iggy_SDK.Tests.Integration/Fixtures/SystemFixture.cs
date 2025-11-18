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
using TUnit.Core.Interfaces;

namespace Apache.Iggy.Tests.Integrations.Fixtures;

public class SystemFixture : IAsyncInitializer
{
    internal readonly string StreamId = "SystemStream";
    internal readonly int TotalClientsCount = 10;

    private List<IIggyClient> AdditionalClients { get; } = new();

    [ClassDataSource<IggyServerFixture>(Shared = SharedType.PerAssembly)]
    public required IggyServerFixture IggyServerFixture { get; init; }

    public Dictionary<Protocol, IIggyClient> Clients { get; set; } = new();

    public async Task InitializeAsync()
    {
        await CreateClientsAsync();
    }

    private async Task CreateClientsAsync()
    {
        Clients = await IggyServerFixture.CreateClients();
        for (var i = 0; i < TotalClientsCount; i++)
        {
            var userName = $"iggy_{Protocol.Http}_{i}";
            await Clients[Protocol.Http].CreateUser(userName, "iggy", UserStatus.Active);

            var client = await IggyServerFixture.CreateClient(Protocol.Tcp, Protocol.Http);
            AdditionalClients.Add(client);
            var login = await client.LoginUser(userName, "iggy");

            if (login!.UserId == 0)
            {
                throw new Exception("Failed to login user 'iggy'.");
            }

            await client.PingAsync();
        }

        // One client less for tcp due to a default client
        for (var i = 0; i < TotalClientsCount - 1; i++)
        {
            var userName = $"iggy_{Protocol.Tcp}_{i}";
            await Clients[Protocol.Tcp].CreateUser(userName, "iggy", UserStatus.Active);

            var client = await IggyServerFixture.CreateClient(Protocol.Tcp, Protocol.Tcp);
            AdditionalClients.Add(client);
            var login = await client.LoginUser(userName, "iggy");
            if (login!.UserId == 0)
            {
                throw new Exception("Failed to login user 'iggy'.");
            }

            await client.PingAsync();
        }
    }
}
