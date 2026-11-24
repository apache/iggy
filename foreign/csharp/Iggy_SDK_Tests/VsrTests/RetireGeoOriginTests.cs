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

using System.Collections.Concurrent;
using Apache.Iggy.Configuration;
using Apache.Iggy.Contracts.Tcp;
using Apache.Iggy.Enums;
using Apache.Iggy.IggyClient.Implementations;
using Apache.Iggy.Vsr;
using Microsoft.Extensions.Logging.Abstractions;
using static Apache.Iggy.Tests.VsrTests.MockFrames;

namespace Apache.Iggy.Tests.VsrTests;

/// <summary>RetireGeoOrigin wire format: <c>[origin_len:u8][origin:N]</c>, a replicated metadata operation.</summary>
public sealed class RetireGeoOriginTests
{
    [Fact]
    public void given_an_origin_when_encoding_should_write_it_as_one_length_prefixed_name()
    {
        Assert.Equal([4, (byte)'d', (byte)'c', (byte)'-', (byte)'a'], TcpContracts.RetireGeoOrigin("dc-a"));
    }

    [Fact]
    public async Task given_an_origin_when_retiring_should_send_it_as_a_metadata_operation()
    {
        var retires = new ConcurrentQueue<MockRequest>();
        using var node = new MockNode();
        node.Serve(request =>
        {
            if (request.Operation == (byte)VsrOperation.RetireGeoOrigin)
            {
                retires.Enqueue(request);
            }

            return request.Code == GET_CLUSTER_METADATA_CODE ? StandaloneRoster(node.Port) : Answer(request);
        });
        using var client = new TcpMessageStream(new IggyClientConfigurator
        {
            BaseAddress = $"127.0.0.1:{node.Port}",
            Protocol = Protocol.Tcp
        }, NullLoggerFactory.Instance);
        await client.ConnectAsync(TestContext.Current.CancellationToken);
        await client.LoginUserAsync("user", "secret", TestContext.Current.CancellationToken);

        await client.RetireGeoOriginAsync("dc-a", TestContext.Current.CancellationToken);

        var retire = Assert.Single(retires);
        Assert.Equal(TcpContracts.RetireGeoOrigin("dc-a"), retire.Body);
    }
}
