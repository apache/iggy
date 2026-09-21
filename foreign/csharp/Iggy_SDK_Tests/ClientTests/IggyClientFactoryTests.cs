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
    public void CreateClient_CreatesTcpClient()
    {
        var options = new IggyClientConfigurator
        {
            BaseAddress = "127.0.0.1:8090",
            Protocol = Protocol.Tcp
        };

        Assert.Equal(64 * 1024 * 1024, options.MaxResponseFrameSize);
        Assert.Null(options.ReceiveBufferSize);
        Assert.Null(options.SendBufferSize);

        using var client = IggyClientFactory.CreateClient(options) as IDisposable;
        Assert.NotNull(client);
    }

    [Theory]
    [InlineData(0, null)]
    [InlineData(-1, null)]
    [InlineData(null, 0)]
    [InlineData(null, -1)]
    public void CreateClient_RejectsNonPositiveSocketBufferSize(int? receiveBufferSize, int? sendBufferSize)
    {
        var options = new IggyClientConfigurator
        {
            BaseAddress = "127.0.0.1:8090",
            Protocol = Protocol.Tcp,
            ReceiveBufferSize = receiveBufferSize,
            SendBufferSize = sendBufferSize
        };

        Assert.Throws<ArgumentOutOfRangeException>(() => IggyClientFactory.CreateClient(options));
    }

    [Fact]
    public void CreateClient_RejectsMaxResponseFrameSizeBelowHeader()
    {
        var options = new IggyClientConfigurator
        {
            BaseAddress = "127.0.0.1:8090",
            Protocol = Protocol.Tcp,
            MaxResponseFrameSize = 255
        };

        Assert.Throws<ArgumentOutOfRangeException>(() => IggyClientFactory.CreateClient(options));
    }

    [Theory]
    [InlineData(0)]
    [InlineData(-1000)]
    [InlineData(0.5)]
    [InlineData(uint.MaxValue)]
    public void CreateClient_RejectsHeartbeatIntervalOutsideTimerBounds(double milliseconds)
    {
        var options = new IggyClientConfigurator
        {
            BaseAddress = "127.0.0.1:8090",
            Protocol = Protocol.Tcp,
            HeartbeatInterval = TimeSpan.FromMilliseconds(milliseconds)
        };

        Assert.Throws<ArgumentOutOfRangeException>(() => IggyClientFactory.CreateClient(options));
    }

    [Fact]
    public void CreateClient_TcpClientDisposesTwice()
    {
        var options = new IggyClientConfigurator
        {
            BaseAddress = "127.0.0.1:8090",
            Protocol = Protocol.Tcp
        };

        var client = (IDisposable)IggyClientFactory.CreateClient(options);

        client.Dispose();
        client.Dispose();
    }

    [Fact]
    public void Defaults_KeepIdleSessionsAlive()
    {
        var options = new IggyClientConfigurator
        {
            BaseAddress = "127.0.0.1:8090",
            Protocol = Protocol.Tcp
        };

        Assert.Equal(TimeSpan.FromSeconds(5), options.HeartbeatInterval);
        Assert.True(options.ReconnectionSettings.Enabled);
        Assert.Equal(0, options.ReconnectionSettings.MaxRetries);
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
