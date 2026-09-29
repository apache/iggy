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

public sealed class ConnectionStringTests
{
    [Theory]
    [InlineData("iggy://user:secret@127.0.0.1:1234")]
    [InlineData("iggy+tcp://user:secret@127.0.0.1:1234")]
    public void FromConnectionString_WithoutOptions_AppliesDefaults(string connectionString)
    {
        var config = IggyClientConfigurator.FromConnectionString(connectionString);

        Assert.Equal("127.0.0.1:1234", config.BaseAddress);
        Assert.Equal(Protocol.Tcp, config.Protocol);
        Assert.True(config.AutoLoginSettings.Enabled);
        Assert.Equal("user", config.AutoLoginSettings.Username);
        Assert.Equal("secret", config.AutoLoginSettings.Password);
        Assert.Empty(config.AutoLoginSettings.PersonalAccessToken);

        Assert.False(config.TlsSettings.Enabled);
        Assert.Equal("127.0.0.1", config.TlsSettings.Hostname);
        Assert.Empty(config.TlsSettings.CertificatePath);
        Assert.Equal(TimeSpan.FromSeconds(5), config.HeartbeatInterval);

        Assert.True(config.ReconnectionSettings.Enabled);
        Assert.Equal(0, config.ReconnectionSettings.MaxRetries);
        Assert.Equal(TimeSpan.FromSeconds(1), config.ReconnectionSettings.InitialDelay);
        Assert.False(config.ReconnectionSettings.UseExponentialBackoff);
    }

    [Theory]
    [InlineData("iggy://iggypat-1234567890abcdef@127.0.0.1:1234")]
    [InlineData("iggy+tcp://iggypat-1234567890abcdef@localhost:1234")]
    public void FromConnectionString_WithPersonalAccessToken_SignsInWithToken(string connectionString)
    {
        var config = IggyClientConfigurator.FromConnectionString(connectionString);

        Assert.True(config.AutoLoginSettings.Enabled);
        Assert.Equal("iggypat-1234567890abcdef", config.AutoLoginSettings.PersonalAccessToken);
        Assert.Empty(config.AutoLoginSettings.Username);
        Assert.Empty(config.AutoLoginSettings.Password);
        Assert.Equal(0, config.ReconnectionSettings.MaxRetries);
        Assert.Equal(TimeSpan.FromSeconds(5), config.HeartbeatInterval);
    }

    [Theory]
    [InlineData("reconnection_retries=3&heartbeat_interval=10s", 3)]
    [InlineData("heartbeat_interval=10s&reconnection_retries=10", 10)]
    public void FromConnectionString_WithRetriesAndHeartbeat_MapsBoth(string options, int retries)
    {
        var config = IggyClientConfigurator.FromConnectionString($"iggy+tcp://user:secret@127.0.0.1:1234?{options}");

        Assert.Equal("127.0.0.1:1234", config.BaseAddress);
        Assert.Equal("user", config.AutoLoginSettings.Username);
        Assert.Equal("secret", config.AutoLoginSettings.Password);
        Assert.False(config.TlsSettings.Enabled);
        Assert.Equal(TimeSpan.FromSeconds(10), config.HeartbeatInterval);
        Assert.True(config.ReconnectionSettings.Enabled);
        Assert.Equal(retries, config.ReconnectionSettings.MaxRetries);
        Assert.Equal(TimeSpan.FromSeconds(1), config.ReconnectionSettings.InitialDelay);
    }

    [Fact]
    public void FromConnectionString_WithReconnectionOptions_MapsIntervalAndRetries()
    {
        var config = IggyClientConfigurator.FromConnectionString(
            "iggy+tcp://iggy:secret@localhost:8090?reconnection_retries=3&reconnection_interval=5s&heartbeat_interval=10s");

        Assert.True(config.ReconnectionSettings.Enabled);
        Assert.Equal(3, config.ReconnectionSettings.MaxRetries);
        Assert.Equal(TimeSpan.FromSeconds(5), config.ReconnectionSettings.InitialDelay);
        Assert.False(config.ReconnectionSettings.UseExponentialBackoff);
        Assert.Equal(TimeSpan.FromSeconds(10), config.HeartbeatInterval);
    }

    [Fact]
    public void FromConnectionString_WithPartialReconnectionOptions_KeepsTheOtherDefault()
    {
        var retriesOnly = IggyClientConfigurator.FromConnectionString(
            "iggy://iggy:secret@localhost:8090?reconnection_retries=3");
        var intervalOnly = IggyClientConfigurator.FromConnectionString(
            "iggy://iggy:secret@localhost:8090?reconnection_interval=5s");

        Assert.Equal(3, retriesOnly.ReconnectionSettings.MaxRetries);
        Assert.Equal(TimeSpan.FromSeconds(1), retriesOnly.ReconnectionSettings.InitialDelay);
        Assert.Equal(0, intervalOnly.ReconnectionSettings.MaxRetries);
        Assert.Equal(TimeSpan.FromSeconds(5), intervalOnly.ReconnectionSettings.InitialDelay);
    }

    [Fact]
    public void FromConnectionString_WithTlsOptions_MapsTlsSettings()
    {
        var config = IggyClientConfigurator.FromConnectionString(
            "iggy://iggy:secret@localhost:8090?tls=true&tls_domain=iggy.apache.org&tls_ca_file=/does/not/exist.pem");

        Assert.True(config.TlsSettings.Enabled);
        Assert.Equal("iggy.apache.org", config.TlsSettings.Hostname);
        // Stored as a path: the file is read when the TLS stream is created, not at parse time.
        Assert.Equal("/does/not/exist.pem", config.TlsSettings.CertificatePath);
        Assert.Equal("localhost:8090", config.BaseAddress);
    }

    [Theory]
    [InlineData("tls=false")]
    [InlineData("nodelay=true")]
    [InlineData("nodelay=false")]
    public void FromConnectionString_WithBooleanOptions_Accepts(string options)
    {
        var config = IggyClientConfigurator.FromConnectionString($"iggy://iggy:secret@localhost:8090?{options}");

        Assert.False(config.TlsSettings.Enabled);
    }

    [Fact]
    public void FromConnectionString_WithUnlimitedRetries_KeepsRetriesUnlimited()
    {
        var config = IggyClientConfigurator.FromConnectionString(
            "iggy://iggy:secret@localhost:8090?reconnection_retries=unlimited");

        Assert.True(config.ReconnectionSettings.Enabled);
        Assert.Equal(0, config.ReconnectionSettings.MaxRetries);
    }

    [Fact]
    public void FromConnectionString_WithZeroRetries_DisablesReconnection()
    {
        var config = IggyClientConfigurator.FromConnectionString(
            "iggy://iggy:secret@localhost:8090?reconnection_retries=0");

        Assert.False(config.ReconnectionSettings.Enabled);
    }

    [Theory]
    [InlineData("0", "5", true, 5)]
    [InlineData("3", "unlimited", true, 0)]
    [InlineData("3", "0", false, 0)]
    [InlineData("3", "4294967295", true, 0)]
    public void FromConnectionString_WithRepeatedRetries_UsesTheLastValue(string first, string last, bool enabled,
        int maxRetries)
    {
        var config = IggyClientConfigurator.FromConnectionString(
            $"iggy://iggy:secret@localhost:8090?reconnection_retries={first}&reconnection_retries={last}");

        Assert.Equal(enabled, config.ReconnectionSettings.Enabled);
        Assert.Equal(maxRetries, config.ReconnectionSettings.MaxRetries);
    }

    [Theory]
    [InlineData("2147483647", int.MaxValue)]
    [InlineData("2147483648", 0)]
    [InlineData("4294967295", 0)]
    public void FromConnectionString_WithRetriesUpToU32Max_Accepts(string retries, int expected)
    {
        var config = IggyClientConfigurator.FromConnectionString(
            $"iggy://iggy:secret@localhost:8090?reconnection_retries={retries}");

        Assert.True(config.ReconnectionSettings.Enabled);
        Assert.Equal(expected, config.ReconnectionSettings.MaxRetries);
    }

    [Theory]
    [InlineData("4294967296")]
    [InlineData("99999999999999")]
    [InlineData("three")]
    [InlineData("-1")]
    [InlineData("+3")]
    [InlineData("")]
    public void FromConnectionString_WithInvalidRetries_Throws(string retries)
    {
        Assert.Throws<FormatException>(() => IggyClientConfigurator.FromConnectionString(
            $"iggy://iggy:secret@localhost:8090?reconnection_retries={retries}"));
    }

    [Theory]
    [InlineData("0")]
    [InlineData("0ms")]
    [InlineData("500us")]
    [InlineData("none")]
    [InlineData("50d")]
    public void FromConnectionString_WithReconnectionIntervalOutOfRange_Throws(string interval)
    {
        var exception = Assert.Throws<FormatException>(() => IggyClientConfigurator.FromConnectionString(
            $"iggy://iggy:secret@localhost:8090?reconnection_interval={interval}"));

        Assert.Contains("must be between 1 millisecond", exception.Message);
    }

    [Fact]
    public void FromConnectionString_WithReconnectionIntervalAtTimerCeiling_Accepts()
    {
        var config = IggyClientConfigurator.FromConnectionString(
            "iggy://iggy:secret@localhost:8090?reconnection_interval=49d");

        Assert.Equal(TimeSpan.FromDays(49), config.ReconnectionSettings.InitialDelay);
    }

    [Fact]
    public void FromConnectionString_WithNegativeReconnectionInterval_Throws()
    {
        Assert.Throws<FormatException>(() => IggyClientConfigurator.FromConnectionString(
            "iggy://iggy:secret@localhost:8090?reconnection_interval=-1s"));
    }

    [Theory]
    [InlineData("none")]
    [InlineData("0")]
    [InlineData("0ms")]
    [InlineData("500us")]
    [InlineData("50d")]
    public void FromConnectionString_WithHeartbeatIntervalOutOfRange_Throws(string interval)
    {
        Assert.Throws<FormatException>(() => IggyClientConfigurator.FromConnectionString(
            $"iggy+tcp://user:secret@127.0.0.1:1234?heartbeat_interval={interval}"));
    }

    [Theory]
    [InlineData("0")]
    [InlineData("10s")]
    [InlineData("100000y")]
    public void FromConnectionString_WithReestablishAfter_ValidatesAndIgnoresIt(string value)
    {
        var config = IggyClientConfigurator.FromConnectionString(
            $"iggy+tcp://user:secret@127.0.0.1:1234?reestablish_after={value}");

        Assert.True(config.ReconnectionSettings.Enabled);
        Assert.Equal(0, config.ReconnectionSettings.MaxRetries);
        Assert.Equal(TimeSpan.FromSeconds(1), config.ReconnectionSettings.InitialDelay);
        Assert.Equal(TimeSpan.FromSeconds(1), config.ReconnectionSettings.WaitAfterReconnect);
    }

    [Theory]
    [InlineData("reconnection_interval=1m30s", 90_000)]
    [InlineData("reconnection_interval=250ms", 250)]
    [InlineData("reconnection_interval=1.5s", 1500)]
    public void FromConnectionString_WithDurationSyntax_ParsesInterval(string options, int milliseconds)
    {
        var config = IggyClientConfigurator.FromConnectionString($"iggy://iggy:secret@localhost:8090?{options}");

        Assert.Equal(TimeSpan.FromMilliseconds(milliseconds), config.ReconnectionSettings.InitialDelay);
    }

    [Fact]
    public void FromConnectionString_WithTlsAndNoDomain_UsesTheServerHost()
    {
        var config = IggyClientConfigurator.FromConnectionString(
            "iggy://iggy:secret@[::1]:8090?tls=true&tls_ca_file=/ca.pem");

        Assert.Equal("::1", config.TlsSettings.Hostname);
    }

    [Theory]
    [InlineData("tls_domain=")]
    [InlineData("tls_domain=iggy.apache.org&tls_domain=")]
    public void FromConnectionString_WithEmptyTlsDomain_UsesTheServerHost(string options)
    {
        var config = IggyClientConfigurator.FromConnectionString(
            $"iggy://iggy:secret@localhost:8090?tls=true&tls_ca_file=/ca.pem&{options}");

        Assert.Equal("localhost", config.TlsSettings.Hostname);
    }

    [Fact]
    public void FromConnectionString_WithTlsAndNoCaFile_Throws()
    {
        var exception = Assert.Throws<FormatException>(() => IggyClientConfigurator.FromConnectionString(
            "iggy://iggy:hunter2@localhost:8090?tls=true&tls_domain=iggy.apache.org"));

        Assert.Contains("tls_ca_file", exception.Message);
    }

    [Fact]
    public void FromConnectionString_WithSchemeTailHoldingCredentials_DoesNotEchoThem()
    {
        var exception = Assert.Throws<FormatException>(() => IggyClientConfigurator.FromConnectionString(
            "iggy+user:pw@host:8090?x=a://b"));

        Assert.DoesNotContain("pw", exception.Message);
    }

    [Fact]
    public void FromConnectionString_WithBracketedIpv6_KeepsTheBracketsForTheDialer()
    {
        var config = IggyClientConfigurator.FromConnectionString("iggy://iggy:secret@[::1]:8090");

        Assert.Equal("[::1]:8090", config.BaseAddress);
    }

    [Theory]
    [InlineData("iggy+quic://iggy:secret@localhost:8090")]
    [InlineData("iggy+ws://iggy:secret@localhost:8090")]
    [InlineData("iggy+http://iggy:secret@localhost:3000")]
    [InlineData("iggy+http://iggypat-1234567890abcdef@localhost:3000")]
    public void FromConnectionString_WithUnsupportedTransport_Throws(string connectionString)
    {
        var exception = Assert.Throws<FormatException>(
            () => IggyClientConfigurator.FromConnectionString(connectionString));

        Assert.Contains("Unsupported transport", exception.Message);
    }

    [Theory]
    [InlineData("IGGY://iggy:secret@localhost:8090")]
    [InlineData("iggy+TCP://iggy:secret@localhost:8090")]
    [InlineData("Iggy+Tcp://iggy:secret@localhost:8090")]
    public void FromConnectionString_WithUppercaseScheme_ThrowsCaseSensitiveError(string connectionString)
    {
        var exception = Assert.Throws<FormatException>(
            () => IggyClientConfigurator.FromConnectionString(connectionString));

        Assert.Contains("case-sensitive", exception.Message);
    }

    [Theory]
    [InlineData("")]
    [InlineData("iggy")]
    [InlineData("iggy://")]
    [InlineData("invalid+tcp://user:secret@127.0.0.1:1234")]
    [InlineData("tcp://user:secret@127.0.0.1:1234")]
    [InlineData("iggy://:secret@127.0.0.1:1234")]
    [InlineData("iggy+tcp://:secret@127.0.0.1:1234")]
    [InlineData("iggy://user:@127.0.0.1:1234")]
    [InlineData("iggy+tcp://user:@127.0.0.1:1234")]
    [InlineData("iggy://@127.0.0.1:1234")]
    [InlineData("iggy://user:secret:extra@127.0.0.1:1234")]
    [InlineData("iggy://user:secret@127.0.0.1:1234@other")]
    [InlineData("iggy://user:secret@:1234")]
    [InlineData("iggy+tcp://user:secret@:1234")]
    [InlineData("iggy://user:secret@127.0.0.1:")]
    [InlineData("iggy+tcp://user:secret@127.0.0.1:")]
    [InlineData("iggy://user:secret@127.0.0.1")]
    [InlineData("iggy://user:secret@localhost:port")]
    [InlineData("iggy://user:secret@localhost:70000")]
    [InlineData("iggy://user:secret@localhost:-1")]
    [InlineData("iggy://user:secret@localhost:+80")]
    [InlineData("iggy://user:secret@localhost:8090\n")]
    [InlineData("iggy://user:secret@local host:8090")]
    [InlineData("iggy://user:secret@local\nhost:8090")]
    [InlineData("iggy://user:secret@[::1 ]:8090")]
    [InlineData("iggy://user:secret@[::1:8090")]
    [InlineData("iggy://user:secret@[]:8090")]
    [InlineData("iggy://user:secret@[::1]x:8090")]
    [InlineData("iggy://user:secret@2001:db8::1:8090")]
    [InlineData("iggy://user:secret@host:8090:9090")]
    [InlineData("iggy://user:secret@127.0.0.1:1234?invalid_option=invalid")]
    [InlineData("iggy+tcp://user:secret@127.0.0.1:?invalid_option=invalid")]
    [InlineData("iggy://user:secret@localhost:8090?unknown=value")]
    [InlineData("iggy://user:secret@localhost:8090?tls=maybe")]
    [InlineData("iggy://user:secret@localhost:8090?tls=TRUE")]
    [InlineData("iggy://user:secret@localhost:8090?nodelay=1")]
    [InlineData("iggy://user:secret@localhost:8090?")]
    [InlineData("iggy://user:secret@localhost:8090?&")]
    [InlineData("iggy://user:secret@localhost:8090?tls")]
    [InlineData("iggy://user:secret@localhost:8090?tls=true=false")]
    [InlineData("iggy://user:secret@localhost:8090?tls=true?nodelay=true")]
    [InlineData("iggy://user:secret@localhost:8090?reestablish_after=garbage")]
    [InlineData("iggy://user:secret@localhost:8090?heartbeat_interval=5")]
    [InlineData("iggy://iggy://user:secret@localhost:8090")]
    public void FromConnectionString_WithMalformedValue_Throws(string connectionString)
    {
        Assert.Throws<FormatException>(() => IggyClientConfigurator.FromConnectionString(connectionString));
    }

    [Fact]
    public void FromConnectionString_WithNull_Throws()
    {
        Assert.Throws<ArgumentNullException>(() => IggyClientConfigurator.FromConnectionString(null!));
    }

    [Theory]
    [InlineData("iggy://iggy:hunter2@localhost")]
    [InlineData("iggy+tcp://iggypat-1234567890abcdef@localhost")]
    [InlineData("iggy://iggy:hunter2@localhost:8090?unknown=value")]
    [InlineData("iggy://iggy:hunter2@localhost:8090?tls=maybe")]
    [InlineData("iggy://iggy:hunter2@localhost:8090?reconnection_retries=three")]
    [InlineData("iggy://iggy:hunter2@localhost:8090?reconnection_interval=0")]
    [InlineData("iggy://iggy:hunter2@localhost:8090?heartbeat_interval=garbage")]
    [InlineData("iggy://iggy:hunter2@localhost:70000")]
    [InlineData("iggy+quic://iggy:hunter2@localhost:8090")]
    public void FromConnectionString_ErrorMessages_NeverContainCredentials(string connectionString)
    {
        var exception = Assert.Throws<FormatException>(
            () => IggyClientConfigurator.FromConnectionString(connectionString));

        Assert.DoesNotContain("hunter2", exception.Message);
        Assert.DoesNotContain("iggypat-1234567890abcdef", exception.Message);
        Assert.DoesNotContain("hunter2", exception.InnerException?.Message ?? string.Empty);
    }

    [Fact]
    public void FromConnectionString_ReturnsAConfiguratorTheCallerCanStillAdjust()
    {
        var config = IggyClientConfigurator.FromConnectionString("iggy://iggy:secret@localhost:8090");
        config.ReceiveBufferSize = 4096;

        Assert.Equal(4096, config.ReceiveBufferSize);
        Assert.Equal(64 * 1024 * 1024, config.MaxResponseFrameSize);
        Assert.Null(config.MessageEncryptor);
    }

    [Fact]
    public void CreateClient_FromConnectionString_CreatesTcpClient()
    {
        using var client = IggyClientFactory.CreateClient("iggy://iggy:secret@127.0.0.1:8090") as IDisposable;

        Assert.NotNull(client);
    }

    [Fact]
    public void CreateClient_FromMalformedConnectionString_Throws()
    {
        Assert.Throws<FormatException>(() => IggyClientFactory.CreateClient("iggy://iggy:secret@127.0.0.1"));
    }

    [Theory]
    [InlineData("500us")]
    [InlineData("50d")]
    public void CreateClient_FromConnectionString_ValidatesHeartbeatRange(string interval)
    {
        Assert.Throws<FormatException>(() => IggyClientFactory.CreateClient(
            $"iggy://iggy:secret@127.0.0.1:8090?heartbeat_interval={interval}"));
    }
}
