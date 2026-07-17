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

using Apache.Iggy.Enums;
using Apache.Iggy.Exceptions;
using Apache.Iggy.Tests.Integrations.Attributes;
using Apache.Iggy.Tests.Integrations.Fixtures;
using Shouldly;

namespace Apache.Iggy.Tests.Integrations;

public class RawCommandTests
{
    [ClassDataSource<IggyServerFixture>(Shared = SharedType.PerAssembly)]
    public required IggyServerFixture Fixture { get; init; }

    [Test]
    [SkipHttp]
    [MethodDataSource<IggyServerFixture>(nameof(IggyServerFixture.ProtocolData))]
    public async Task SendRawWithResponse_Tcp_ShouldReturnRawPayloads(Protocol protocol)
    {
        var client = await Fixture.CreateAuthenticatedClient(protocol);

        var pingResponse = await client.SendRawWithResponseAsync(1, []);
        var statsResponse = await client.SendRawWithResponseAsync(10, []);

        pingResponse.ShouldBeEmpty();
        statsResponse.ShouldNotBeEmpty();
    }

    [Test]
    [SkipHttp]
    [MethodDataSource<IggyServerFixture>(nameof(IggyServerFixture.ProtocolData))]
    public async Task SendRawWithResponse_Tcp_ShouldRejectSessionControlCodes(Protocol protocol)
    {
        var client = await Fixture.CreateAuthenticatedClient(protocol);

        foreach (var code in new uint[] { 38, 39, 40, 44, 45 })
        {
            var exception = await Should.ThrowAsync<IggyInvalidStatusCodeException>(
                () => client.SendRawWithResponseAsync(code, []));
            exception.StatusCode.ShouldBe(3);
        }
    }

    [Test]
    [SkipHttp]
    [MethodDataSource<IggyServerFixture>(nameof(IggyServerFixture.ProtocolData))]
    public async Task SendRawWithResponse_Tcp_ShouldPropagateServerError(Protocol protocol)
    {
        var client = await Fixture.CreateAuthenticatedClient(protocol);

        var exception = await Should.ThrowAsync<IggyInvalidStatusCodeException>(
            () => client.SendRawWithResponseAsync(60_000, []));

        exception.StatusCode.ShouldBe(3);
    }

    [Test]
    [SkipTcp]
    [MethodDataSource<IggyServerFixture>(nameof(IggyServerFixture.ProtocolData))]
    public async Task SendRawWithResponse_Http_ShouldThrowFeatureUnavailable(Protocol protocol)
    {
        var client = await Fixture.CreateAuthenticatedClient(protocol);

        await Should.ThrowAsync<FeatureUnavailableException>(
            () => client.SendRawWithResponseAsync(1, []));
    }
}
