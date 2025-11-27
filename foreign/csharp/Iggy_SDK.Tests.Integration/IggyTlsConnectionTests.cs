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

using Apache.Iggy.Configuration;
using Apache.Iggy.Enums;
using Apache.Iggy.Exceptions;
using Apache.Iggy.Factory;
using Apache.Iggy.Tests.Integrations.Fixtures;
using Shouldly;

namespace Apache.Iggy.Tests.Integrations;

public class IggyTlsConnectionTests
{
    [ClassDataSource<IggyTlsServerFixture>(Shared = SharedType.PerAssembly)]
    public required IggyTlsServerFixture Fixture { get; init; }

    [Test]
    public async Task Connect_WithTls_Should_Connect_Successfully()
    {
        using var client = IggyClientFactory.CreateClient(new IggyClientConfigurator
        {
            BaseAddress = Fixture.GetIggyAddress(Protocol.Tcp),
            Protocol = Protocol.Tcp,
            ReconnectionSettings = new ReconnectionSettings { Enabled = false },
            AutoLoginSettings = new AutoLoginSettings
            {
                Enabled = true,
                Username = "iggy",
                Password = "iggy"
            },
            TlsSettings = new TlsSettings
            {
                Enabled = true,
                Hostname = "localhost",
                CertificatePath = "Certs/iggy_cert.pem"
            }
        });

        await client.ConnectAsync();
        var loginResult = await client.LoginUser("iggy", "iggy");

        loginResult.ShouldNotBeNull();
    }

    [Test]
    public async Task Connect_WithoutTls_Should_Throw_WhenTlsIsRequired()
    {
        using var client = IggyClientFactory.CreateClient(new IggyClientConfigurator
        {
            BaseAddress = Fixture.GetIggyAddress(Protocol.Tcp),
            Protocol = Protocol.Tcp,
            ReconnectionSettings = new ReconnectionSettings { Enabled = false }
        });

        await client.ConnectAsync();
        await Should.ThrowAsync<IggyZeroBytesException>(client.LoginUser("iggy", "iggy"));
    }

    [Test]
    public async Task Connect_WithTls_CA_Should_Connect_Successfully()
    {
        using var client = IggyClientFactory.CreateClient(new IggyClientConfigurator
        {
            BaseAddress = Fixture.GetIggyAddress(Protocol.Tcp),
            Protocol = Protocol.Tcp,
            ReconnectionSettings = new ReconnectionSettings { Enabled = false },
            AutoLoginSettings = new AutoLoginSettings
            {
                Enabled = true,
                Username = "iggy",
                Password = "iggy"
            },
            TlsSettings = new TlsSettings
            {
                Enabled = true,
                Hostname = "localhost",
                CertificatePath = "Certs/iggy_ca_cert.pem"
            }
        });

        await client.ConnectAsync();
        var loginResult = await client.LoginUser("iggy", "iggy");

        loginResult.ShouldNotBeNull();
    }
}
