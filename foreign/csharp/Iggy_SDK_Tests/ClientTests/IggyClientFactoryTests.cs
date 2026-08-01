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
using Apache.Iggy.Factory;

namespace Apache.Iggy.Tests.ClientTests;

public sealed class IggyClientFactoryTests
{
    [Fact]
    public void CreateClient_DefaultsToClassicWireProtocol()
    {
        var options = new IggyClientConfigurator
        {
            BaseAddress = "127.0.0.1:8090",
            Protocol = Protocol.Tcp
        };

        Assert.Equal(WireProtocol.Classic, options.WireProtocol);
        Assert.Equal(64 * 1024 * 1024, options.MaxResponseFrameSize);

        using var client = IggyClientFactory.CreateClient(options) as IDisposable;
        Assert.NotNull(client);
    }

    [Fact]
    public void CreateClient_AllowsVsrOverTcp()
    {
        var options = new IggyClientConfigurator
        {
            BaseAddress = "127.0.0.1:8090",
            Protocol = Protocol.Tcp,
            WireProtocol = WireProtocol.Vsr
        };

        using var client = IggyClientFactory.CreateClient(options) as IDisposable;
        Assert.NotNull(client);
    }

    [Fact]
    public void CreateClient_RejectsVsrOverHttp()
    {
        var options = new IggyClientConfigurator
        {
            BaseAddress = "http://127.0.0.1:3000",
            Protocol = Protocol.Http,
            WireProtocol = WireProtocol.Vsr
        };

        var exception = Assert.Throws<ArgumentException>(() => IggyClientFactory.CreateClient(options));
        Assert.Contains("WireProtocol.Vsr requires Protocol.Tcp", exception.Message);
    }

    [Fact]
    public void CreateClient_RejectsMaxResponseFrameSizeBelowHeader()
    {
        var options = new IggyClientConfigurator
        {
            BaseAddress = "127.0.0.1:8090",
            Protocol = Protocol.Tcp,
            WireProtocol = WireProtocol.Vsr,
            MaxResponseFrameSize = 255
        };

        Assert.Throws<ArgumentOutOfRangeException>(() => IggyClientFactory.CreateClient(options));
    }

    /// <summary>
    ///     Only the VSR reader bounds the buffer it rents for a peer-announced length, so a classic client is
    ///     not failed over a value that never reaches its path.
    /// </summary>
    [Fact]
    public void CreateClient_AcceptsMaxResponseFrameSizeBelowHeaderUnderClassic()
    {
        var options = new IggyClientConfigurator
        {
            BaseAddress = "127.0.0.1:8090",
            Protocol = Protocol.Tcp,
            MaxResponseFrameSize = 1
        };

        Assert.NotNull(IggyClientFactory.CreateClient(options));
    }

    [Fact]
    public void CreateClient_AcceptsMaxResponseFrameSizeUnderHttp()
    {
        var options = new IggyClientConfigurator
        {
            BaseAddress = "http://127.0.0.1:3000",
            Protocol = Protocol.Http,
            MaxResponseFrameSize = 1
        };

        using var client = IggyClientFactory.CreateClient(options) as IDisposable;
        Assert.NotNull(client);
    }
}
