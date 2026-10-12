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

using Apache.Iggy.Encryption;
using Apache.Iggy.Enums;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Logging.Abstractions;

namespace Apache.Iggy.Configuration;

/// <summary>
///     Configuration for the Iggy client
/// </summary>
public sealed class IggyClientConfigurator
{
    // The bounds Task.Delay and PeriodicTimer accept, which the reconnect loop and heartbeat run on. Below the
    // minimum the delay truncates to zero and redials in a hot loop.
    internal static readonly TimeSpan MinInterval = TimeSpan.FromMilliseconds(1);
    internal static readonly TimeSpan MaxInterval = TimeSpan.FromMilliseconds(uint.MaxValue - 1);

    /// <summary>
    ///     The base address of the Iggy server.
    /// </summary>
    public required string BaseAddress { get; set; }

    /// <summary>
    ///     The transport protocol to use.
    /// </summary>
    public required Protocol Protocol { get; set; }

    /// <summary>
    ///     The largest response frame accepted over <see cref="Enums.Protocol.Tcp" />, in bytes.
    ///     Default is 64 MiB, minimum is the 256-byte header.
    /// </summary>
    public int MaxResponseFrameSize { get; set; } = 64 * 1024 * 1024;

    /// <summary>
    ///     The size of the receive buffer in bytes. When null, the size is not set on the socket, so the
    ///     operating system default is used. On Linux, this keeps TCP auto-tuning enabled. The initial size
    ///     is the middle value in <c>/proc/sys/net/ipv4/tcp_rmem</c>.
    /// </summary>
    public int? ReceiveBufferSize { get; set; } = null;

    /// <summary>
    ///     The size of the send buffer in bytes. When null, the size is not set on the socket, so the
    ///     operating system default is used. On Linux, this keeps TCP auto-tuning enabled. The initial size
    ///     is the middle value in <c>/proc/sys/net/ipv4/tcp_wmem</c>.
    /// </summary>
    public int? SendBufferSize { get; set; } = null;

    /// <summary>
    ///     Interval between the pings the <see cref="Enums.Protocol.Tcp" /> client sends on its own while
    ///     connected, so an idle session survives the server's heartbeat verification and consumer-group
    ///     assignments stay fresh. Default is 5 seconds; must be between 1 millisecond and about 49 days.
    ///     Init-only: the client validates the interval once and hands it to a timer that would fault on an
    ///     out-of-range value.
    /// </summary>
    public TimeSpan HeartbeatInterval { get; init; } = TimeSpan.FromSeconds(5);

    /// <summary>
    ///     TLS settings
    /// </summary>
    public TlsSettings TlsSettings { get; set; } = new();

    /// <summary>
    ///     Reconnection settings
    /// </summary>
    public ReconnectionSettings ReconnectionSettings { get; set; } = new();

    /// <summary>
    ///     Auto-login settings used after successful connection.
    /// </summary>
    public AutoLoginSettings AutoLoginSettings { get; set; } = new();

    /// <summary>
    ///     The logger factory to use.
    /// </summary>
    public ILoggerFactory LoggerFactory { get; set; } = NullLoggerFactory.Instance;

    /// <summary>
    ///     Optional message encryptor. When set, the client encrypts message payloads and user headers on send
    ///     and decrypts them on poll. Applies to every message on the connection, so topics mixing encrypted
    ///     and plaintext messages are not supported. The caller owns the encryptor and must dispose it
    ///     (e.g. <see cref="AesMessageEncryptor" />) after the client is done with it.
    /// </summary>
    public IMessageEncryptor? MessageEncryptor { get; set; }

    /// <summary>
    ///     Opt in to <c>autoCommit: true</c> on the raw poll methods while an encryptor is configured.
    ///     Off by default: the server commits the offset before the client decrypts, so a decryption failure
    ///     would silently skip the batch. Does not affect <see cref="Consumers.IggyConsumer" /> commit modes.
    /// </summary>
    public bool AllowAutoCommitWithEncryptor { get; set; }

    /// <summary>
    ///     Creates a TCP configuration from a connection string in the format shared by every Iggy SDK:
    ///     <c>iggy://username:password@host:port?options</c>, or <c>iggy://token@host:port</c> to sign in with a
    ///     personal access token. The <c>iggy+tcp://</c> scheme is accepted as well. The credentials become the
    ///     <see cref="AutoLoginSettings" />; the remaining settings can still be changed on the returned instance, except
    ///     <see cref="HeartbeatInterval" />, which is init-only: <c>heartbeat_interval</c> in the string is the only
    ///     way to set it.
    /// </summary>
    /// <remarks>
    ///     Credentials are taken literally: they are not percent-decoded and must not contain <c>@</c> or <c>:</c>.
    ///     Supported options: <c>tls</c>, <c>tls_domain</c>, <c>tls_ca_file</c>, <c>reconnection_retries</c>
    ///     (a count or <c>unlimited</c>), <c>reconnection_interval</c>, <c>heartbeat_interval</c>,
    ///     <c>reestablish_after</c> and <c>nodelay</c>. Durations look like <c>500ms</c>, <c>5s</c> or <c>1m30s</c>.
    ///     Reconnection defaults to unlimited retries every second, and after each reconnect the client still waits
    ///     <see cref="ReconnectionSettings.WaitAfterReconnect" /> (1 second by default). <c>reestablish_after</c> is
    ///     validated but ignored, since <c>reconnection_interval</c> paces every redial. <c>nodelay</c> is validated
    ///     but ignored too: sockets are always opened with <c>NoDelay</c>, so <c>nodelay=false</c> does not turn
    ///     Nagle back on. <c>tls=true</c> requires
    ///     <c>tls_ca_file</c>, and one <c>tls_domain</c> covers every node the client dials.
    ///     <c>reconnection_retries=0</c> turns reconnection off entirely, so the client does not reconnect after a
    ///     lost connection either.
    /// </remarks>
    /// <param name="connectionString">The connection string.</param>
    /// <exception cref="ArgumentNullException">Thrown when <paramref name="connectionString" /> is null.</exception>
    /// <exception cref="FormatException">
    ///     Thrown when the connection string is malformed, names a transport other than TCP or holds an unknown or
    ///     invalid option. The message never contains the connection string itself.
    /// </exception>
    public static IggyClientConfigurator FromConnectionString(string connectionString)
    {
        return ConnectionString.Parse(connectionString);
    }
}
